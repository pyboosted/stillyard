use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OptionalExtension, params};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    AttemptId, Blocker, ContainmentId, DaemonSnapshot, Estimate, InvocationId, JobId, JobOutcome,
    JobReceipt, JobSnapshot, JobSpec, JobState, LogChunk, LogStream, RecoveryResult, SubmissionId,
    SubmissionState,
};

pub(crate) struct StorePaths {
    pub(crate) root: PathBuf,
    pub(crate) database: PathBuf,
    pub(crate) logs: PathBuf,
    pub(crate) lock: PathBuf,
}

impl StorePaths {
    pub(crate) fn new(root: PathBuf) -> Self {
        Self {
            database: root.join("stillyard.sqlite3"),
            logs: root.join("logs"),
            lock: root.join("daemon.lock"),
            root,
        }
    }

    pub(crate) fn ensure(&self) -> StoreResult<()> {
        std::fs::create_dir_all(&self.root)?;
        std::fs::create_dir_all(&self.logs)?;
        Ok(())
    }

    pub(crate) fn stdout_path(&self, job_id: JobId) -> PathBuf {
        self.logs.join(job_id.to_string()).join("stdout.bin")
    }

    pub(crate) fn stderr_path(&self, job_id: JobId) -> PathBuf {
        self.logs.join(job_id.to_string()).join("stderr.bin")
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum StoreError {
    #[error("SQLite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("corrupt durable id: {0}")]
    Id(#[from] uuid::Error),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("idempotency conflict")]
    IdempotencyConflict,
    #[error("invalid specification: {0}")]
    InvalidSpec(String),
    #[error("invalid durable state: {0}")]
    InvalidState(String),
}

pub(crate) type StoreResult<T> = std::result::Result<T, StoreError>;

pub(crate) struct SubmitResult {
    pub(crate) receipt: JobReceipt,
    pub(crate) should_schedule: bool,
}

#[derive(Clone)]
pub(crate) struct PreparedJob {
    pub(crate) job_id: JobId,
    pub(crate) attempt_id: AttemptId,
    pub(crate) invocation_id: InvocationId,
    pub(crate) containment_id: ContainmentId,
    pub(crate) spec: JobSpec,
    pub(crate) stdout_path: PathBuf,
    pub(crate) stderr_path: PathBuf,
}

pub(crate) struct Store {
    connection: Connection,
    pub(crate) paths: StorePaths,
    store_uuid: Uuid,
}

impl Store {
    pub(crate) fn open(paths: StorePaths) -> StoreResult<Self> {
        paths.ensure()?;
        let connection = Connection::open(&paths.database)?;
        connection.execute_batch(
            "PRAGMA foreign_keys = ON;
             PRAGMA journal_mode = WAL;
             PRAGMA synchronous = FULL;
             PRAGMA busy_timeout = 5000;",
        )?;
        migrate(&connection)?;
        let store_uuid = match connection
            .query_row(
                "SELECT value FROM meta WHERE key = 'store_uuid'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?
        {
            Some(value) => Uuid::parse_str(&value)?,
            None => {
                let value = Uuid::now_v7();
                connection.execute(
                    "INSERT INTO meta(key, value) VALUES ('store_uuid', ?1)",
                    [value.to_string()],
                )?;
                value
            }
        };
        let mut store = Self {
            connection,
            paths,
            store_uuid,
        };
        store.recover_interrupted()?;
        store.resume_received()?;
        Ok(store)
    }

    pub(crate) fn submit(
        &mut self,
        idempotency_key: Uuid,
        claimed_payload_hash: &str,
        spec: &JobSpec,
    ) -> StoreResult<SubmitResult> {
        spec.validate()
            .map_err(|error| StoreError::InvalidSpec(error.to_string()))?;
        let payload_hash = normalized_payload_hash(spec)?;
        if claimed_payload_hash != payload_hash {
            return Err(StoreError::InvalidSpec(
                "payload hash does not match the normalized specification".into(),
            ));
        }
        let key = idempotency_key.to_string();
        if let Some((submission_id, stored_hash, state, job_id, spec_json)) = self
            .connection
            .query_row(
                "SELECT id, payload_hash, state, job_id, spec_json
                 FROM submissions WHERE scope = 'unmanaged' AND idempotency_key = ?1",
                [&key],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                },
            )
            .optional()?
        {
            if stored_hash != payload_hash {
                return Err(StoreError::IdempotencyConflict);
            }
            if state == "accepted" {
                let job_id = job_id.ok_or_else(|| {
                    StoreError::InvalidState("accepted submission has no job".into())
                })?;
                return Ok(SubmitResult {
                    receipt: self.receipt(
                        SubmissionId(Uuid::parse_str(&submission_id)?),
                        JobId(Uuid::parse_str(&job_id)?),
                    )?,
                    should_schedule: false,
                });
            }
            if state == "received" {
                let durable_spec = serde_json::from_str(&spec_json)?;
                return self.accept_received(
                    SubmissionId(Uuid::parse_str(&submission_id)?),
                    &durable_spec,
                );
            }
            return Err(StoreError::InvalidState(format!(
                "terminal submission state {state} cannot be replaced"
            )));
        }

        let submission_id = SubmissionId::new();
        let received = self.connection.transaction()?;
        received.execute(
            "INSERT INTO submissions(
                id, scope, idempotency_key, payload_hash, state, spec_json, created_ms
             ) VALUES (?1, 'unmanaged', ?2, ?3, 'received', ?4, ?5)",
            params![
                submission_id.to_string(),
                key,
                payload_hash,
                serde_json::to_string(spec)?,
                now_millis(),
            ],
        )?;
        received.commit()?;
        self.accept_received(submission_id, spec)
    }

    pub(crate) fn recover_submission(
        &self,
        idempotency_key: Uuid,
        payload_hash: &str,
    ) -> StoreResult<RecoveryResult> {
        let row = self
            .connection
            .query_row(
                "SELECT id, payload_hash, state, job_id
                 FROM submissions WHERE scope = 'unmanaged' AND idempotency_key = ?1",
                [idempotency_key.to_string()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                },
            )
            .optional()?;
        let Some((submission_id, stored_hash, state, job_id)) = row else {
            // An unmanaged caller has no retained parent Attempt proving absence.
            return Ok(RecoveryResult::Unknown);
        };
        if stored_hash != payload_hash {
            return Ok(RecoveryResult::Conflict);
        }
        let submission_id = SubmissionId(Uuid::parse_str(&submission_id)?);
        match state.as_str() {
            "received" => Ok(RecoveryResult::Received { submission_id }),
            "accepted" => {
                let job_id = job_id.ok_or_else(|| {
                    StoreError::InvalidState("accepted submission has no job".into())
                })?;
                Ok(RecoveryResult::Accepted(self.receipt(
                    submission_id,
                    JobId(Uuid::parse_str(&job_id)?),
                )?))
            }
            "rejected" => Ok(RecoveryResult::Rejected {
                code: "rejected".into(),
                detail: "submission was rejected".into(),
            }),
            other => Err(StoreError::InvalidState(format!(
                "unknown submission state {other}"
            ))),
        }
    }

    fn accept_received(
        &mut self,
        submission_id: SubmissionId,
        spec: &JobSpec,
    ) -> StoreResult<SubmitResult> {
        let job_id = JobId::new();
        let accepted_ms = now_millis();
        let transaction = self.connection.transaction()?;
        let state: String = transaction.query_row(
            "SELECT state FROM submissions WHERE id = ?1",
            [submission_id.to_string()],
            |row| row.get(0),
        )?;
        if state == "accepted" {
            let existing: String = transaction.query_row(
                "SELECT job_id FROM submissions WHERE id = ?1",
                [submission_id.to_string()],
                |row| row.get(0),
            )?;
            transaction.commit()?;
            return Ok(SubmitResult {
                receipt: self.receipt(submission_id, JobId(Uuid::parse_str(&existing)?))?,
                should_schedule: false,
            });
        }
        if state != "received" {
            return Err(StoreError::InvalidState(format!(
                "submission {submission_id} is terminal in state {state}"
            )));
        }
        transaction.execute(
            "INSERT INTO jobs(id, submission_id, state, spec_json, accepted_ms)
             VALUES (?1, ?2, 'pending', ?3, ?4)",
            params![
                job_id.to_string(),
                submission_id.to_string(),
                serde_json::to_string(spec)?,
                accepted_ms,
            ],
        )?;
        transaction.execute(
            "UPDATE submissions SET state = 'accepted', job_id = ?2 WHERE id = ?1",
            params![submission_id.to_string(), job_id.to_string()],
        )?;
        transaction.commit()?;
        Ok(SubmitResult {
            receipt: self.receipt(submission_id, job_id)?,
            should_schedule: true,
        })
    }

    pub(crate) fn receipt(
        &self,
        submission_id: SubmissionId,
        job_id: JobId,
    ) -> StoreResult<JobReceipt> {
        let state: String = self.connection.query_row(
            "SELECT state FROM jobs WHERE id = ?1",
            [job_id.to_string()],
            |row| row.get(0),
        )?;
        let queue_rank = if state == "pending" {
            Some(self.connection.query_row(
                "SELECT COUNT(*) FROM jobs
                 WHERE state = 'pending' AND accepted_ms <= (
                     SELECT accepted_ms FROM jobs WHERE id = ?1
                 )",
                [job_id.to_string()],
                |row| row.get::<_, u64>(0),
            )?)
        } else {
            None
        };
        Ok(JobReceipt {
            submission_id,
            job_id,
            submission_state: SubmissionState::Accepted,
            job_state: parse_job_state(&state)?,
            blockers: Vec::new(),
            queue_rank,
            estimate: Estimate::unknown("runtime calibration is not available yet"),
        })
    }

    pub(crate) fn prepare_job(&mut self, job_id: JobId) -> StoreResult<Option<PreparedJob>> {
        let attempt_id = AttemptId::new();
        let invocation_id = InvocationId::new();
        let containment_id = ContainmentId::new();
        let lease_id = Uuid::now_v7();
        let transaction = self.connection.transaction()?;
        let row = transaction
            .query_row(
                "SELECT spec_json FROM jobs WHERE id = ?1 AND state = 'pending'",
                [job_id.to_string()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let Some(spec_json) = row else {
            transaction.rollback()?;
            return Ok(None);
        };
        let spec = serde_json::from_str(&spec_json)?;
        let log_directory = self.paths.logs.join(job_id.to_string());
        std::fs::create_dir_all(&log_directory)?;
        transaction.execute(
            "UPDATE jobs SET state = 'active', attempt_id = ?2, invocation_id = ?3,
                containment_id = ?4 WHERE id = ?1 AND state = 'pending'",
            params![
                job_id.to_string(),
                attempt_id.to_string(),
                invocation_id.to_string(),
                containment_id.to_string(),
            ],
        )?;
        transaction.execute(
            "INSERT INTO attempts(id, job_id, state, attempt_index)
             VALUES (?1, ?2, 'starting', 1)",
            params![attempt_id.to_string(), job_id.to_string()],
        )?;
        transaction.execute(
            "INSERT INTO invocations(id, attempt_id, role, state)
             VALUES (?1, ?2, 'primary', 'prepared')",
            params![invocation_id.to_string(), attempt_id.to_string()],
        )?;
        transaction.execute(
            "INSERT INTO containments(id, invocation_id, state)
             VALUES (?1, ?2, 'creating')",
            params![containment_id.to_string(), invocation_id.to_string()],
        )?;
        transaction.execute(
            "INSERT INTO leases(id, attempt_id, state, claims_json)
             VALUES (?1, ?2, 'granted', ?3)",
            params![lease_id.to_string(), attempt_id.to_string(), "{}",],
        )?;
        transaction.commit()?;
        Ok(Some(PreparedJob {
            job_id,
            attempt_id,
            invocation_id,
            containment_id,
            spec,
            stdout_path: self.paths.stdout_path(job_id),
            stderr_path: self.paths.stderr_path(job_id),
        }))
    }

    pub(crate) fn mark_started(
        &mut self,
        job: &PreparedJob,
        root_pid: u32,
        executable_hash: &str,
    ) -> StoreResult<()> {
        let transaction = self.connection.transaction()?;
        let state: String = transaction.query_row(
            "SELECT state FROM jobs WHERE id = ?1",
            [job.job_id.to_string()],
            |row| row.get(0),
        )?;
        if state != "active" {
            return Err(StoreError::InvalidState(format!(
                "job {} cannot start from {state}",
                job.job_id
            )));
        }
        transaction.execute(
            "UPDATE invocations SET state = 'started', root_pid = ?2,
                executable_hash = ?3, started_ms = ?4 WHERE id = ?1",
            params![
                job.invocation_id.to_string(),
                root_pid,
                executable_hash,
                now_millis(),
            ],
        )?;
        transaction.execute(
            "UPDATE containments SET state = 'live' WHERE id = ?1",
            [job.containment_id.to_string()],
        )?;
        transaction.execute(
            "UPDATE attempts SET state = 'running' WHERE id = ?1",
            [job.attempt_id.to_string()],
        )?;
        transaction.execute(
            "UPDATE jobs SET started_ms = ?2 WHERE id = ?1",
            params![job.job_id.to_string(), now_millis()],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn commit_log_offset(
        &mut self,
        job_id: JobId,
        stream: LogStream,
        offset: u64,
    ) -> StoreResult<()> {
        let column = match stream {
            LogStream::Stdout => "stdout_len",
            LogStream::Stderr => "stderr_len",
        };
        self.connection.execute(
            &format!("UPDATE jobs SET {column} = ?2 WHERE id = ?1"),
            params![job_id.to_string(), offset],
        )?;
        Ok(())
    }

    pub(crate) fn mark_root_exited(
        &mut self,
        job: &PreparedJob,
        exit_code: i32,
    ) -> StoreResult<()> {
        let transaction = self.connection.transaction()?;
        let state: String = transaction.query_row(
            "SELECT state FROM jobs WHERE id = ?1",
            [job.job_id.to_string()],
            |row| row.get(0),
        )?;
        if state != "active" {
            return Err(StoreError::InvalidState(format!(
                "job {} cannot record root exit from {state}",
                job.job_id
            )));
        }
        transaction.execute(
            "UPDATE invocations SET root_exit_code = ?2 WHERE id = ?1 AND state = 'started'",
            params![job.invocation_id.to_string(), exit_code],
        )?;
        transaction.execute(
            "UPDATE jobs SET root_exit_code = ?2 WHERE id = ?1 AND state = 'active'",
            params![job.job_id.to_string(), exit_code],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn mark_finished(
        &mut self,
        job: &PreparedJob,
        exit_code: Option<i32>,
        outcome: JobOutcome,
        verdict: &str,
    ) -> StoreResult<()> {
        let transaction = self.connection.transaction()?;
        let state: String = transaction.query_row(
            "SELECT state FROM jobs WHERE id = ?1",
            [job.job_id.to_string()],
            |row| row.get(0),
        )?;
        if state != "active" {
            return Err(StoreError::InvalidState(format!(
                "job {} cannot settle from {state}",
                job.job_id
            )));
        }
        transaction.execute(
            "UPDATE invocations SET state = 'resolved', root_exit_code = ?2,
                finished_ms = ?3 WHERE id = ?1",
            params![job.invocation_id.to_string(), exit_code, now_millis()],
        )?;
        transaction.execute(
            "UPDATE containments SET state = 'empty' WHERE id = ?1",
            [job.containment_id.to_string()],
        )?;
        transaction.execute(
            "UPDATE attempts SET state = 'settled', verdict = ?2 WHERE id = ?1",
            params![job.attempt_id.to_string(), verdict],
        )?;
        transaction.execute(
            "UPDATE leases SET state = 'released' WHERE attempt_id = ?1",
            [job.attempt_id.to_string()],
        )?;
        transaction.execute(
            "UPDATE jobs SET state = 'final', outcome = ?2, root_exit_code = ?3,
                finished_ms = ?4 WHERE id = ?1",
            params![
                job.job_id.to_string(),
                outcome_string(outcome),
                exit_code,
                now_millis(),
            ],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn mark_uncertain(
        &mut self,
        job: &PreparedJob,
        exit_code: Option<i32>,
        verdict: &str,
    ) -> StoreResult<()> {
        let transaction = self.connection.transaction()?;
        let state: String = transaction.query_row(
            "SELECT state FROM jobs WHERE id = ?1",
            [job.job_id.to_string()],
            |row| row.get(0),
        )?;
        if state != "active" {
            return Err(StoreError::InvalidState(format!(
                "job {} cannot become uncertain from {state}",
                job.job_id
            )));
        }
        transaction.execute(
            "UPDATE invocations SET state = 'resolved', root_exit_code = COALESCE(?2, root_exit_code),
                finished_ms = ?3 WHERE id = ?1 AND state IN ('prepared', 'started')",
            params![job.invocation_id.to_string(), exit_code, now_millis()],
        )?;
        transaction.execute(
            "UPDATE containments SET state = 'uncertain'
             WHERE id = ?1 AND state IN ('creating', 'live')",
            [job.containment_id.to_string()],
        )?;
        transaction.execute(
            "UPDATE attempts SET state = 'settled', verdict = ?2
             WHERE id = ?1 AND state != 'settled'",
            params![job.attempt_id.to_string(), verdict],
        )?;
        // An uncertain Containment deliberately keeps its Lease granted.
        transaction.execute(
            "UPDATE jobs SET state = 'final', outcome = 'interrupted',
                root_exit_code = COALESCE(?2, root_exit_code), finished_ms = ?3
             WHERE id = ?1 AND state = 'active'",
            params![job.job_id.to_string(), exit_code, now_millis()],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn status(&self, job_id: JobId) -> StoreResult<JobSnapshot> {
        self.connection
            .query_row(
                "SELECT submission_id, state, outcome, attempt_id, invocation_id,
                    containment_id, root_exit_code, accepted_ms, started_ms, finished_ms,
                    spec_json
                 FROM jobs WHERE id = ?1",
                [job_id.to_string()],
                |row| {
                    let submission_id: String = row.get(0)?;
                    let state: String = row.get(1)?;
                    let outcome: Option<String> = row.get(2)?;
                    let attempt_id: Option<String> = row.get(3)?;
                    let invocation_id: Option<String> = row.get(4)?;
                    let containment_id: Option<String> = row.get(5)?;
                    let spec_json: String = row.get(10)?;
                    Ok((
                        submission_id,
                        state,
                        outcome,
                        attempt_id,
                        invocation_id,
                        containment_id,
                        row.get::<_, Option<i32>>(6)?,
                        row.get::<_, i64>(7)?,
                        row.get::<_, Option<i64>>(8)?,
                        row.get::<_, Option<i64>>(9)?,
                        spec_json,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(job_id.to_string()))
            .and_then(
                |(
                    submission_id,
                    state,
                    outcome,
                    attempt_id,
                    invocation_id,
                    containment_id,
                    root_exit_code,
                    accepted_ms,
                    started_ms,
                    finished_ms,
                    spec_json,
                )| {
                    Ok(JobSnapshot {
                        job_id,
                        submission_id: SubmissionId(Uuid::parse_str(&submission_id)?),
                        state: parse_job_state(&state)?,
                        outcome: outcome.map(|value| parse_outcome(&value)).transpose()?,
                        attempt_id: attempt_id
                            .map(|value| Uuid::parse_str(&value).map(AttemptId))
                            .transpose()?,
                        invocation_id: invocation_id
                            .map(|value| Uuid::parse_str(&value).map(InvocationId))
                            .transpose()?,
                        containment_id: containment_id
                            .map(|value| Uuid::parse_str(&value).map(ContainmentId))
                            .transpose()?,
                        root_exit_code,
                        accepted_unix_millis: accepted_ms,
                        started_unix_millis: started_ms,
                        finished_unix_millis: finished_ms,
                        spec: serde_json::from_str(&spec_json)?,
                        blockers: Vec::<Blocker>::new(),
                    })
                },
            )
    }

    pub(crate) fn logs(
        &self,
        job_id: JobId,
        stream: LogStream,
        offset: u64,
        limit: u32,
    ) -> StoreResult<LogChunk> {
        let (committed, state, containment): (u64, String, String) = match stream {
            LogStream::Stdout => self.connection.query_row(
                "SELECT stdout_len, state, COALESCE((
                    SELECT state FROM containments WHERE id = jobs.containment_id
                 ), 'empty') FROM jobs WHERE id = ?1",
                [job_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?,
            LogStream::Stderr => self.connection.query_row(
                "SELECT stderr_len, state, COALESCE((
                    SELECT state FROM containments WHERE id = jobs.containment_id
                 ), 'empty') FROM jobs WHERE id = ?1",
                [job_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?,
        };
        let path = match stream {
            LogStream::Stdout => self.paths.stdout_path(job_id),
            LogStream::Stderr => self.paths.stderr_path(job_id),
        };
        if offset > committed {
            return Ok(LogChunk {
                job_id,
                stream,
                offset,
                bytes: Vec::new(),
                next_offset: committed,
                eof: state == "final" && containment != "uncertain",
                gap: Some(format!(
                    "requested offset {offset} exceeds committed offset {committed}"
                )),
            });
        }
        let available = committed.saturating_sub(offset);
        let length = available.min(u64::from(limit.min(1024 * 1024))) as usize;
        let mut bytes = vec![0_u8; length];
        if length > 0 {
            let read = File::open(&path).and_then(|mut file| {
                file.seek(SeekFrom::Start(offset))?;
                file.read_exact(&mut bytes)
            });
            if let Err(error) = read {
                return Ok(LogChunk {
                    job_id,
                    stream,
                    offset,
                    bytes: Vec::new(),
                    next_offset: offset,
                    eof: false,
                    gap: Some(format!(
                        "committed range {offset}..{} is unavailable: {error}",
                        offset + length as u64
                    )),
                });
            }
        }
        let next_offset = offset + bytes.len() as u64;
        Ok(LogChunk {
            job_id,
            stream,
            offset,
            bytes,
            next_offset,
            eof: state == "final" && containment != "uncertain" && next_offset == committed,
            gap: None,
        })
    }

    pub(crate) fn daemon_status(&self) -> StoreResult<DaemonSnapshot> {
        let queued_jobs = self.connection.query_row(
            "SELECT COUNT(*) FROM jobs WHERE state = 'pending'",
            [],
            |row| row.get(0),
        )?;
        let running_jobs = self.connection.query_row(
            "SELECT COUNT(*) FROM jobs WHERE state = 'active'",
            [],
            |row| row.get(0),
        )?;
        Ok(DaemonSnapshot {
            store_uuid: self.store_uuid,
            version: env!("CARGO_PKG_VERSION").to_owned(),
            pid: std::process::id(),
            queued_jobs,
            running_jobs,
        })
    }

    pub(crate) fn pending_jobs(&self) -> StoreResult<Vec<JobId>> {
        let mut statement = self
            .connection
            .prepare("SELECT id FROM jobs WHERE state = 'pending' ORDER BY accepted_ms")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        let mut jobs = Vec::new();
        for row in rows {
            jobs.push(JobId(Uuid::parse_str(&row?)?));
        }
        Ok(jobs)
    }

    fn recover_interrupted(&mut self) -> StoreResult<()> {
        let live_roots = {
            let mut statement = self.connection.prepare(
                "SELECT containments.id, attempts.id, invocations.root_pid
                 FROM containments
                 JOIN invocations ON invocations.id = containments.invocation_id
                 JOIN attempts ON attempts.id = invocations.attempt_id
                 WHERE containments.state = 'live'",
            )?;
            let rows = statement.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<u32>>(2)?,
                ))
            })?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        let proven_empty: Vec<_> = live_roots
            .into_iter()
            .filter(|(_, _, root_pid)| root_pid.is_some_and(root_disappeared_bounded))
            .collect();
        let finished = now_millis();
        let transaction = self.connection.transaction()?;
        transaction.execute(
            "UPDATE invocations SET state = 'resolved', finished_ms = ?1
             WHERE state = 'prepared'",
            [finished],
        )?;
        transaction.execute(
            "UPDATE containments SET state = 'empty' WHERE state = 'creating'",
            [],
        )?;
        transaction.execute(
            "UPDATE attempts SET state = 'settled', verdict = 'start_failed'
             WHERE state = 'starting'",
            [],
        )?;
        transaction.execute(
            "UPDATE leases SET state = 'released' WHERE state = 'granted' AND attempt_id IN (
                SELECT id FROM attempts WHERE verdict = 'start_failed'
             )",
            [],
        )?;
        transaction.execute(
            "UPDATE jobs SET state = 'final', outcome = 'failed', finished_ms = ?1
             WHERE state = 'active' AND invocation_id IN (
                SELECT id FROM invocations WHERE root_pid IS NULL
             )",
            [finished],
        )?;
        transaction.execute(
            "UPDATE invocations SET state = 'resolved', finished_ms = ?1
             WHERE state = 'started'",
            [finished],
        )?;
        transaction.execute(
            "UPDATE containments SET state = 'uncertain' WHERE state = 'live'",
            [],
        )?;
        transaction.execute(
            "UPDATE attempts SET state = 'settled', verdict = 'interrupted'
             WHERE state != 'settled'",
            [],
        )?;
        transaction.execute(
            "UPDATE jobs SET state = 'final', outcome = 'interrupted', finished_ms = ?1
             WHERE state = 'active'",
            [finished],
        )?;
        for (containment_id, attempt_id, _) in proven_empty {
            transaction.execute(
                "UPDATE containments SET state = 'empty'
                 WHERE id = ?1 AND state = 'uncertain'",
                [containment_id],
            )?;
            transaction.execute(
                "UPDATE leases SET state = 'released'
                 WHERE attempt_id = ?1 AND state = 'granted'",
                [attempt_id],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    fn resume_received(&mut self) -> StoreResult<()> {
        let received = {
            let mut statement = self.connection.prepare(
                "SELECT id, spec_json FROM submissions WHERE state = 'received' ORDER BY created_ms",
            )?;
            let rows = statement.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        for (submission_id, spec_json) in received {
            let spec = serde_json::from_str(&spec_json)?;
            self.accept_received(SubmissionId(Uuid::parse_str(&submission_id)?), &spec)?;
        }
        Ok(())
    }
}

pub(crate) fn normalized_payload_hash(spec: &JobSpec) -> StoreResult<String> {
    let normalized = serde_json::to_vec(spec)?;
    Ok(format!("{:x}", Sha256::digest(normalized)))
}

#[cfg(windows)]
fn root_disappeared_bounded(pid: u32) -> bool {
    use windows_sys::Win32::Foundation::{CloseHandle, WAIT_OBJECT_0};
    use windows_sys::Win32::Storage::FileSystem::SYNCHRONIZE;
    use windows_sys::Win32::System::Threading::{
        OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, WaitForSingleObject,
    };

    // SAFETY: the access is observational and pid came from the durable root record.
    let process = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION | SYNCHRONIZE, 0, pid) };
    if process.is_null() {
        return std::io::Error::last_os_error().raw_os_error() == Some(87);
    }
    // SAFETY: process is a live waitable handle. Five seconds is the bounded recovery proof.
    let gone = unsafe { WaitForSingleObject(process, 5_000) } == WAIT_OBJECT_0;
    // SAFETY: this function owns the process handle.
    unsafe { CloseHandle(process) };
    gone
}

#[cfg(not(windows))]
fn root_disappeared_bounded(_pid: u32) -> bool {
    false
}

pub(crate) fn open_lock(path: &Path) -> StoreResult<File> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    Ok(OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?)
}

fn migrate(connection: &Connection) -> StoreResult<()> {
    let version: u32 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    match version {
        1 => return validate_schema(connection),
        0 => {}
        unsupported => {
            return Err(StoreError::InvalidState(format!(
                "unsupported SQLite schema version {unsupported}; expected 1"
            )));
        }
    }
    connection.execute_batch(
        "BEGIN IMMEDIATE;
         CREATE TABLE IF NOT EXISTS meta(
             key TEXT PRIMARY KEY,
             value TEXT NOT NULL
         );
         CREATE TABLE IF NOT EXISTS submissions(
             id TEXT PRIMARY KEY,
             scope TEXT NOT NULL,
             idempotency_key TEXT NOT NULL,
             payload_hash TEXT NOT NULL,
             state TEXT NOT NULL,
             spec_json TEXT NOT NULL,
             job_id TEXT,
             created_ms INTEGER NOT NULL,
             UNIQUE(scope, idempotency_key)
         );
         CREATE TABLE IF NOT EXISTS batches(
             id TEXT PRIMARY KEY,
             state TEXT NOT NULL
         );
         CREATE TABLE IF NOT EXISTS jobs(
             id TEXT PRIMARY KEY,
             submission_id TEXT NOT NULL REFERENCES submissions(id),
             state TEXT NOT NULL,
             outcome TEXT,
             spec_json TEXT NOT NULL,
             attempt_id TEXT,
             invocation_id TEXT,
             containment_id TEXT,
             root_exit_code INTEGER,
             accepted_ms INTEGER NOT NULL,
             started_ms INTEGER,
             finished_ms INTEGER,
             stdout_len INTEGER NOT NULL DEFAULT 0,
             stderr_len INTEGER NOT NULL DEFAULT 0
         );
         CREATE TABLE IF NOT EXISTS attempts(
             id TEXT PRIMARY KEY,
             job_id TEXT NOT NULL REFERENCES jobs(id),
             state TEXT NOT NULL,
             attempt_index INTEGER NOT NULL,
             verdict TEXT
         );
         CREATE TABLE IF NOT EXISTS invocations(
             id TEXT PRIMARY KEY,
             attempt_id TEXT NOT NULL REFERENCES attempts(id),
             role TEXT NOT NULL,
             state TEXT NOT NULL,
             root_pid INTEGER,
             root_exit_code INTEGER,
             executable_hash TEXT,
             started_ms INTEGER,
             finished_ms INTEGER
         );
         CREATE TABLE IF NOT EXISTS containments(
             id TEXT PRIMARY KEY,
             invocation_id TEXT NOT NULL REFERENCES invocations(id),
             state TEXT NOT NULL
         );
         CREATE TABLE IF NOT EXISTS conditions(
             id TEXT PRIMARY KEY,
             job_id TEXT NOT NULL REFERENCES jobs(id),
             state TEXT NOT NULL,
             spec_json TEXT NOT NULL
         );
         CREATE TABLE IF NOT EXISTS observations(
             id TEXT PRIMARY KEY,
             condition_id TEXT NOT NULL REFERENCES conditions(id),
             observed_ms INTEGER NOT NULL,
             value_json TEXT NOT NULL
         );
         CREATE TABLE IF NOT EXISTS leases(
             id TEXT PRIMARY KEY,
             attempt_id TEXT NOT NULL REFERENCES attempts(id),
             state TEXT NOT NULL,
             claims_json TEXT NOT NULL
         );
         PRAGMA user_version = 1;
         COMMIT;",
    )?;
    validate_schema(connection)
}

fn validate_schema(connection: &Connection) -> StoreResult<()> {
    for table in [
        "meta",
        "submissions",
        "batches",
        "jobs",
        "attempts",
        "invocations",
        "containments",
        "conditions",
        "observations",
        "leases",
    ] {
        let exists: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
            [table],
            |row| row.get(0),
        )?;
        if !exists {
            return Err(StoreError::InvalidState(format!(
                "schema version 1 is missing table {table}; refusing reconstruction"
            )));
        }
    }
    Ok(())
}

fn parse_job_state(state: &str) -> StoreResult<JobState> {
    match state {
        "pending" => Ok(JobState::Pending),
        "active" => Ok(JobState::Active),
        "finalizing" => Ok(JobState::Finalizing),
        "final" => Ok(JobState::Final),
        other => Err(StoreError::InvalidState(format!(
            "unknown job state {other}"
        ))),
    }
}

fn parse_outcome(outcome: &str) -> StoreResult<JobOutcome> {
    match outcome {
        "succeeded" => Ok(JobOutcome::Succeeded),
        "failed" => Ok(JobOutcome::Failed),
        "timed_out" => Ok(JobOutcome::TimedOut),
        "interrupted" => Ok(JobOutcome::Interrupted),
        "canceled" => Ok(JobOutcome::Canceled),
        "skipped" => Ok(JobOutcome::Skipped),
        other => Err(StoreError::InvalidState(format!(
            "unknown job outcome {other}"
        ))),
    }
}

fn outcome_string(outcome: JobOutcome) -> &'static str {
    match outcome {
        JobOutcome::Succeeded => "succeeded",
        JobOutcome::Failed => "failed",
        JobOutcome::TimedOut => "timed_out",
        JobOutcome::Interrupted => "interrupted",
        JobOutcome::Canceled => "canceled",
        JobOutcome::Skipped => "skipped",
    }
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EnvironmentSpec, ResourceClaims, RetryPolicy, SPEC_VERSION, StdinSpec};

    fn spec(root: &Path) -> JobSpec {
        JobSpec {
            spec_version: SPEC_VERSION,
            executable: root.join("tool.exe"),
            args: Vec::new(),
            working_directory: root.to_path_buf(),
            stdin: StdinSpec::Eof,
            environment: EnvironmentSpec::default(),
            resources: ResourceClaims::default(),
            conditions: Vec::new(),
            retry: RetryPolicy::default(),
            labels: Vec::new(),
            expected_duration_seconds: None,
            timeout_seconds: None,
            quiet: None,
            artifacts: Vec::new(),
        }
    }

    #[test]
    fn duplicate_key_returns_one_job() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
        let key = Uuid::now_v7();
        let spec = spec(temp.path());
        let hash = normalized_payload_hash(&spec).unwrap();
        let first = store.submit(key, &hash, &spec).unwrap();
        let second = store.submit(key, &hash, &spec).unwrap();
        assert_eq!(first.receipt.job_id, second.receipt.job_id);
        assert!(first.should_schedule);
        assert!(!second.should_schedule);
    }

    #[test]
    fn same_key_different_payload_conflicts() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
        let key = Uuid::now_v7();
        let first = spec(temp.path());
        let first_hash = normalized_payload_hash(&first).unwrap();
        store.submit(key, &first_hash, &first).unwrap();
        let mut second = first.clone();
        second.args.push("different".into());
        let second_hash = normalized_payload_hash(&second).unwrap();
        assert!(matches!(
            store.submit(key, &second_hash, &second),
            Err(StoreError::IdempotencyConflict)
        ));
    }

    #[test]
    fn recovery_never_creates_work_and_distinguishes_conflict() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
        let key = Uuid::now_v7();
        assert_eq!(
            store.recover_submission(key, "hash").unwrap(),
            RecoveryResult::Unknown
        );
        let spec = spec(temp.path());
        let hash = normalized_payload_hash(&spec).unwrap();
        let submitted = store.submit(key, &hash, &spec).unwrap();
        assert_eq!(
            store.recover_submission(key, "other").unwrap(),
            RecoveryResult::Conflict
        );
        match store.recover_submission(key, &hash).unwrap() {
            RecoveryResult::Accepted(receipt) => {
                assert_eq!(receipt.job_id, submitted.receipt.job_id);
            }
            recovery => panic!("unexpected recovery: {recovery:?}"),
        }
        assert_eq!(store.pending_jobs().unwrap().len(), 1);
    }

    #[test]
    fn restart_interrupts_active_job_without_requeueing_it() {
        let temp = tempfile::tempdir().unwrap();
        let paths = StorePaths::new(temp.path().to_path_buf());
        let job_id = {
            let mut store = Store::open(paths).unwrap();
            let spec = spec(temp.path());
            let hash = normalized_payload_hash(&spec).unwrap();
            let submitted = store.submit(Uuid::now_v7(), &hash, &spec).unwrap();
            let prepared = store
                .prepare_job(submitted.receipt.job_id)
                .unwrap()
                .unwrap();
            store
                .mark_started(&prepared, std::process::id(), "exe-hash")
                .unwrap();
            prepared.job_id
        };
        let store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
        let snapshot = store.status(job_id).unwrap();
        assert_eq!(snapshot.state, JobState::Final);
        assert_eq!(snapshot.outcome, Some(JobOutcome::Interrupted));
        assert!(store.pending_jobs().unwrap().is_empty());
        let (containment, lease): (String, String) = store
            .connection
            .query_row(
                "SELECT containments.state, leases.state
                 FROM containments
                 JOIN invocations ON invocations.id = containments.invocation_id
                 JOIN attempts ON attempts.id = invocations.attempt_id
                 JOIN leases ON leases.attempt_id = attempts.id
                 WHERE attempts.job_id = ?1",
                [job_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(containment, "uncertain");
        assert_eq!(lease, "granted");
    }

    #[cfg(windows)]
    #[test]
    fn restart_releases_lease_after_recorded_root_is_gone() {
        let temp = tempfile::tempdir().unwrap();
        let paths = StorePaths::new(temp.path().to_path_buf());
        let job_id = {
            let mut store = Store::open(paths).unwrap();
            let job_spec = spec(temp.path());
            let hash = normalized_payload_hash(&job_spec).unwrap();
            let submitted = store.submit(Uuid::now_v7(), &hash, &job_spec).unwrap();
            let prepared = store
                .prepare_job(submitted.receipt.job_id)
                .unwrap()
                .unwrap();
            store.mark_started(&prepared, u32::MAX, "exe-hash").unwrap();
            prepared.job_id
        };
        let store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
        let (containment, lease): (String, String) = store
            .connection
            .query_row(
                "SELECT containments.state, leases.state
                 FROM containments
                 JOIN invocations ON invocations.id = containments.invocation_id
                 JOIN attempts ON attempts.id = invocations.attempt_id
                 JOIN leases ON leases.attempt_id = attempts.id
                 WHERE attempts.job_id = ?1",
                [job_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(containment, "empty");
        assert_eq!(lease, "released");
    }

    #[test]
    fn restart_before_root_settles_start_failed_and_releases_lease() {
        let temp = tempfile::tempdir().unwrap();
        let paths = StorePaths::new(temp.path().to_path_buf());
        let job_id = {
            let mut store = Store::open(paths).unwrap();
            let job_spec = spec(temp.path());
            let hash = normalized_payload_hash(&job_spec).unwrap();
            let submitted = store.submit(Uuid::now_v7(), &hash, &job_spec).unwrap();
            store
                .prepare_job(submitted.receipt.job_id)
                .unwrap()
                .unwrap()
                .job_id
        };
        let store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
        let snapshot = store.status(job_id).unwrap();
        assert_eq!(snapshot.outcome, Some(JobOutcome::Failed));
        let (verdict, containment, lease): (String, String, String) = store
            .connection
            .query_row(
                "SELECT attempts.verdict, containments.state, leases.state
                 FROM attempts
                 JOIN invocations ON invocations.attempt_id = attempts.id
                 JOIN containments ON containments.invocation_id = invocations.id
                 JOIN leases ON leases.attempt_id = attempts.id
                 WHERE attempts.job_id = ?1",
                [job_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(verdict, "start_failed");
        assert_eq!(containment, "empty");
        assert_eq!(lease, "released");
    }

    #[test]
    fn uncertain_settlement_retains_lease_and_is_terminal() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
        let job_spec = spec(temp.path());
        let hash = normalized_payload_hash(&job_spec).unwrap();
        let submitted = store.submit(Uuid::now_v7(), &hash, &job_spec).unwrap();
        let prepared = store
            .prepare_job(submitted.receipt.job_id)
            .unwrap()
            .unwrap();
        store.mark_started(&prepared, 1234, "exe-hash").unwrap();
        store
            .mark_uncertain(&prepared, None, "interrupted")
            .unwrap();
        let (containment, lease): (String, String) = store
            .connection
            .query_row(
                "SELECT containments.state, leases.state
                 FROM containments
                 JOIN invocations ON invocations.id = containments.invocation_id
                 JOIN attempts ON attempts.id = invocations.attempt_id
                 JOIN leases ON leases.attempt_id = attempts.id
                 WHERE attempts.job_id = ?1",
                [prepared.job_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(containment, "uncertain");
        assert_eq!(lease, "granted");
        assert!(matches!(
            store.mark_finished(&prepared, None, JobOutcome::Failed, "start_failed"),
            Err(StoreError::InvalidState(_))
        ));
    }

    #[test]
    fn logs_publish_only_flushed_committed_prefix() {
        use std::io::Write;

        let temp = tempfile::tempdir().unwrap();
        let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
        let spec = spec(temp.path());
        let hash = normalized_payload_hash(&spec).unwrap();
        let submitted = store.submit(Uuid::now_v7(), &hash, &spec).unwrap();
        let prepared = store
            .prepare_job(submitted.receipt.job_id)
            .unwrap()
            .unwrap();
        let mut output = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&prepared.stdout_path)
            .unwrap();
        output.write_all(b"committed-tail").unwrap();
        output.sync_data().unwrap();
        store
            .commit_log_offset(prepared.job_id, LogStream::Stdout, 9)
            .unwrap();
        let chunk = store
            .logs(prepared.job_id, LogStream::Stdout, 0, 1024)
            .unwrap();
        assert_eq!(chunk.bytes, b"committed");
        assert_eq!(chunk.next_offset, 9);

        drop(output);
        std::fs::remove_file(&prepared.stdout_path).unwrap();
        let gap = store
            .logs(prepared.job_id, LogStream::Stdout, 0, 1024)
            .unwrap();
        assert!(gap.gap.is_some());
        assert!(gap.bytes.is_empty());
    }

    #[test]
    fn startup_resumes_durable_received_submission() {
        let temp = tempfile::tempdir().unwrap();
        let paths = StorePaths::new(temp.path().to_path_buf());
        let job_spec = spec(temp.path());
        let hash = normalized_payload_hash(&job_spec).unwrap();
        let key = Uuid::now_v7();
        let submission_id = SubmissionId::new();
        {
            let store = Store::open(paths).unwrap();
            store
                .connection
                .execute(
                    "INSERT INTO submissions(
                        id, scope, idempotency_key, payload_hash, state, spec_json, created_ms
                     ) VALUES (?1, 'unmanaged', ?2, ?3, 'received', ?4, ?5)",
                    params![
                        submission_id.to_string(),
                        key.to_string(),
                        hash,
                        serde_json::to_string(&job_spec).unwrap(),
                        now_millis(),
                    ],
                )
                .unwrap();
        }
        let store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
        assert!(matches!(
            store.recover_submission(key, &hash).unwrap(),
            RecoveryResult::Accepted(_)
        ));
        assert_eq!(store.pending_jobs().unwrap().len(), 1);
    }

    #[test]
    fn unsupported_or_damaged_schema_is_not_recreated() {
        let future = tempfile::tempdir().unwrap();
        let future_paths = StorePaths::new(future.path().to_path_buf());
        future_paths.ensure().unwrap();
        let connection = Connection::open(&future_paths.database).unwrap();
        connection
            .execute_batch("PRAGMA user_version = 2;")
            .unwrap();
        drop(connection);
        assert!(matches!(
            Store::open(future_paths),
            Err(StoreError::InvalidState(message)) if message.contains("unsupported SQLite schema")
        ));

        let damaged = tempfile::tempdir().unwrap();
        let damaged_paths = StorePaths::new(damaged.path().to_path_buf());
        let store = Store::open(damaged_paths).unwrap();
        store.connection.execute("DROP TABLE batches", []).unwrap();
        drop(store);
        assert!(matches!(
            Store::open(StorePaths::new(damaged.path().to_path_buf())),
            Err(StoreError::InvalidState(message)) if message.contains("missing table batches")
        ));
    }
}
