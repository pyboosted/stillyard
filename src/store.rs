use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OptionalExtension, params};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::resources::ResolvedClaims;
use crate::{
    AttemptId, BatchId, BatchJobReceipt, BatchReceipt, BatchSpec, Blocker, ContainmentId,
    DaemonSnapshot, Estimate, InvocationId, JobId, JobOutcome, JobReceipt, JobSnapshot, JobSpec,
    JobState, LogChunk, LogStream, RecoveryResult, ResourceCapacities, SubmissionId,
    SubmissionState,
};

pub(crate) struct StorePaths {
    pub(crate) root: PathBuf,
    pub(crate) database: PathBuf,
    pub(crate) logs: PathBuf,
    pub(crate) lock: PathBuf,
    pub(crate) config: PathBuf,
}

impl StorePaths {
    pub(crate) fn new(root: PathBuf) -> Self {
        Self {
            database: root.join("stillyard.sqlite3"),
            logs: root.join("logs"),
            lock: root.join("daemon.lock"),
            config: root.join("config.json"),
            root,
        }
    }

    pub(crate) fn ensure(&self) -> StoreResult<()> {
        std::fs::create_dir_all(&self.root)?;
        crate::filesystem::require_fixed_local_ntfs(&self.root)?;
        std::fs::create_dir_all(&self.logs)?;
        Ok(())
    }

    pub(crate) fn stdout_path(&self, job_id: JobId) -> PathBuf {
        self.logs
            .join(job_id.entity_uuid().to_string())
            .join("stdout.bin")
    }

    pub(crate) fn stderr_path(&self, job_id: JobId) -> PathBuf {
        self.logs
            .join(job_id.entity_uuid().to_string())
            .join("stderr.bin")
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

pub(crate) struct BatchSubmitResult {
    pub(crate) receipt: BatchReceipt,
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
    capacities: ResourceCapacities,
}

impl Store {
    pub(crate) fn open(paths: StorePaths) -> StoreResult<Self> {
        let capacities = load_capacities(&paths.config)?;
        Self::open_with_capacities(paths, capacities)
    }

    fn open_with_capacities(
        paths: StorePaths,
        capacities: ResourceCapacities,
    ) -> StoreResult<Self> {
        paths.ensure()?;
        let database_existed = paths.database.exists();
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
            Some(value) => Uuid::parse_str(&value).map_err(|_| {
                StoreError::InvalidState(
                    "existing store has an invalid store_uuid; move the complete store directory aside before recovery"
                        .into(),
                )
            })?,
            None => {
                if database_existed {
                    return Err(StoreError::InvalidState(
                        "existing store has no store_uuid; move the complete store directory aside before recovery"
                            .into(),
                    ));
                }
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
            capacities,
        };
        store.recover_interrupted()?;
        store.resume_received()?;
        Ok(store)
    }

    fn local_id(&self, id: JobId) -> StoreResult<String> {
        if id.store_uuid() != self.store_uuid {
            return Err(StoreError::NotFound(format!(
                "foreign durable ID from store {}",
                id.store_uuid()
            )));
        }
        Ok(id.entity_uuid().to_string())
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
        if let Some((submission_id, stored_hash, state, job_id, spec_json, kind)) = self
            .connection
            .query_row(
                "SELECT id, payload_hash, state, job_id, spec_json, kind
                 FROM submissions WHERE scope = 'unmanaged' AND idempotency_key = ?1",
                [&key],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                    ))
                },
            )
            .optional()?
        {
            if stored_hash != payload_hash || kind != "job" {
                return Err(StoreError::IdempotencyConflict);
            }
            if state == "accepted" {
                let job_id = job_id.ok_or_else(|| {
                    StoreError::InvalidState("accepted submission has no job".into())
                })?;
                return Ok(SubmitResult {
                    receipt: self.receipt(
                        SubmissionId::from_parts(self.store_uuid, Uuid::parse_str(&submission_id)?),
                        JobId::from_parts(self.store_uuid, Uuid::parse_str(&job_id)?),
                    )?,
                    should_schedule: false,
                });
            }
            if state == "received" {
                let durable_spec = serde_json::from_str(&spec_json)?;
                return self.accept_received(
                    SubmissionId::from_parts(self.store_uuid, Uuid::parse_str(&submission_id)?),
                    &durable_spec,
                );
            }
            return Err(StoreError::InvalidState(format!(
                "terminal submission state {state} cannot be replaced"
            )));
        }

        let submission_id = SubmissionId::new(self.store_uuid);
        let received = self.connection.transaction()?;
        received.execute(
            "INSERT INTO submissions(
                id, scope, idempotency_key, payload_hash, state, spec_json, kind, created_ms
             ) VALUES (?1, 'unmanaged', ?2, ?3, 'received', ?4, 'job', ?5)",
            params![
                submission_id.entity_uuid().to_string(),
                key,
                payload_hash,
                serde_json::to_string(spec)?,
                now_millis(),
            ],
        )?;
        received.commit()?;
        self.accept_received(submission_id, spec)
    }

    pub(crate) fn submit_batch(
        &mut self,
        idempotency_key: Uuid,
        claimed_payload_hash: &str,
        spec: &BatchSpec,
    ) -> StoreResult<BatchSubmitResult> {
        spec.validate()
            .map_err(|error| StoreError::InvalidSpec(error.to_string()))?;
        let payload_hash = normalized_batch_payload_hash(spec)?;
        if claimed_payload_hash != payload_hash {
            return Err(StoreError::InvalidSpec(
                "payload hash does not match the normalized specification".into(),
            ));
        }
        let key = idempotency_key.to_string();
        if let Some((submission, stored_hash, state, batch, spec_json, kind)) = self
            .connection
            .query_row(
                "SELECT id, payload_hash, state, batch_id, spec_json, kind
                 FROM submissions WHERE scope = 'unmanaged' AND idempotency_key = ?1",
                [&key],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                    ))
                },
            )
            .optional()?
        {
            if stored_hash != payload_hash || kind != "batch" {
                return Err(StoreError::IdempotencyConflict);
            }
            let submission_id =
                SubmissionId::from_parts(self.store_uuid, Uuid::parse_str(&submission)?);
            if state == "accepted" {
                let batch_id = batch.ok_or_else(|| {
                    StoreError::InvalidState("accepted batch submission has no batch".into())
                })?;
                return Ok(BatchSubmitResult {
                    receipt: self.batch_receipt(
                        submission_id,
                        BatchId::from_parts(self.store_uuid, Uuid::parse_str(&batch_id)?),
                    )?,
                    should_schedule: false,
                });
            }
            if state == "received" {
                let durable: BatchSpec = serde_json::from_str(&spec_json)?;
                return self.accept_received_batch(submission_id, &durable);
            }
            return Err(StoreError::InvalidState(format!(
                "terminal submission state {state} cannot be replaced"
            )));
        }

        let submission_id = SubmissionId::new(self.store_uuid);
        let received = self.connection.transaction()?;
        received.execute(
            "INSERT INTO submissions(
                id, scope, idempotency_key, payload_hash, state, spec_json, kind, created_ms
             ) VALUES (?1, 'unmanaged', ?2, ?3, 'received', ?4, 'batch', ?5)",
            params![
                submission_id.entity_uuid().to_string(),
                key,
                payload_hash,
                serde_json::to_string(spec)?,
                now_millis(),
            ],
        )?;
        received.commit()?;
        self.accept_received_batch(submission_id, spec)
    }

    fn accept_received_batch(
        &mut self,
        submission_id: SubmissionId,
        spec: &BatchSpec,
    ) -> StoreResult<BatchSubmitResult> {
        let batch_id = BatchId::new(self.store_uuid);
        let accepted_ms = now_millis();
        let jobs: StoreResult<Vec<_>> = spec
            .jobs
            .iter()
            .map(|member| {
                Ok((
                    JobId::new(self.store_uuid),
                    ResolvedClaims::resolve(&member.spec.resources)
                        .map_err(|error| StoreError::InvalidSpec(error.to_string()))?,
                ))
            })
            .collect();
        let jobs = match jobs {
            Ok(jobs) => jobs,
            Err(error) => {
                self.reject_received(submission_id)?;
                return Err(error);
            }
        };
        let names: std::collections::HashMap<_, _> = spec
            .jobs
            .iter()
            .zip(&jobs)
            .map(|(member, (job_id, _))| (member.name.as_str(), *job_id))
            .collect();
        let transaction = self.connection.transaction()?;
        let state: String = transaction.query_row(
            "SELECT state FROM submissions WHERE id = ?1",
            [submission_id.entity_uuid().to_string()],
            |row| row.get(0),
        )?;
        if state == "accepted" {
            let existing: String = transaction.query_row(
                "SELECT batch_id FROM submissions WHERE id = ?1",
                [submission_id.entity_uuid().to_string()],
                |row| row.get(0),
            )?;
            transaction.commit()?;
            return Ok(BatchSubmitResult {
                receipt: self.batch_receipt(
                    submission_id,
                    BatchId::from_parts(self.store_uuid, Uuid::parse_str(&existing)?),
                )?,
                should_schedule: false,
            });
        }
        if state != "received" {
            return Err(StoreError::InvalidState(format!(
                "submission {submission_id} is terminal in state {state}"
            )));
        }
        transaction.execute(
            "INSERT INTO batches(id, state, submission_id, accepted_ms)
             VALUES (?1, 'retained', ?2, ?3)",
            params![
                batch_id.entity_uuid().to_string(),
                submission_id.entity_uuid().to_string(),
                accepted_ms,
            ],
        )?;
        for (index, (member, (job_id, claims))) in spec.jobs.iter().zip(&jobs).enumerate() {
            transaction.execute(
                "INSERT INTO jobs(
                    id, submission_id, batch_id, batch_member, batch_index, state,
                    spec_json, claims_json, accepted_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5, 'pending', ?6, ?7, ?8)",
                params![
                    job_id.entity_uuid().to_string(),
                    submission_id.entity_uuid().to_string(),
                    batch_id.entity_uuid().to_string(),
                    member.name,
                    index as u64,
                    serde_json::to_string(&member.spec)?,
                    serde_json::to_string(claims)?,
                    accepted_ms,
                ],
            )?;
        }
        for (member, (successor, _)) in spec.jobs.iter().zip(&jobs) {
            for dependency in &member.dependencies {
                let predecessor = names[dependency.job.as_str()];
                transaction.execute(
                    "INSERT INTO dependencies(predecessor_id, successor_id, kind)
                     VALUES (?1, ?2, ?3)",
                    params![
                        predecessor.entity_uuid().to_string(),
                        successor.entity_uuid().to_string(),
                        dependency_kind(dependency.on),
                    ],
                )?;
            }
        }
        transaction.execute(
            "UPDATE submissions SET state = 'accepted', batch_id = ?2 WHERE id = ?1",
            params![
                submission_id.entity_uuid().to_string(),
                batch_id.entity_uuid().to_string()
            ],
        )?;
        transaction.commit()?;
        Ok(BatchSubmitResult {
            receipt: self.batch_receipt(submission_id, batch_id)?,
            should_schedule: true,
        })
    }

    fn batch_receipt(
        &self,
        submission_id: SubmissionId,
        batch_id: BatchId,
    ) -> StoreResult<BatchReceipt> {
        if batch_id.store_uuid() != self.store_uuid {
            return Err(StoreError::NotFound(batch_id.to_string()));
        }
        let mut statement = self.connection.prepare(
            "SELECT id, batch_member FROM jobs WHERE batch_id = ?1 ORDER BY batch_index",
        )?;
        let rows = statement.query_map([batch_id.entity_uuid().to_string()], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut jobs = Vec::new();
        for row in rows {
            let (job, name) = row?;
            let job_id = JobId::from_parts(self.store_uuid, Uuid::parse_str(&job)?);
            jobs.push(BatchJobReceipt {
                name,
                receipt: self.receipt(submission_id, job_id)?,
            });
        }
        if jobs.is_empty() {
            return Err(StoreError::InvalidState(format!(
                "retained batch {batch_id} has no members"
            )));
        }
        Ok(BatchReceipt {
            submission_id,
            batch_id,
            submission_state: SubmissionState::Accepted,
            jobs,
        })
    }

    fn reject_received(&mut self, submission_id: SubmissionId) -> StoreResult<()> {
        self.connection.execute(
            "UPDATE submissions SET state = 'rejected'
             WHERE id = ?1 AND state = 'received'",
            [submission_id.entity_uuid().to_string()],
        )?;
        Ok(())
    }

    pub(crate) fn recover_submission(
        &self,
        idempotency_key: Uuid,
        payload_hash: &str,
    ) -> StoreResult<RecoveryResult> {
        let row = self
            .connection
            .query_row(
                "SELECT id, payload_hash, state, job_id, batch_id, kind
                 FROM submissions WHERE scope = 'unmanaged' AND idempotency_key = ?1",
                [idempotency_key.to_string()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, String>(5)?,
                    ))
                },
            )
            .optional()?;
        let Some((submission_id, stored_hash, state, job_id, batch_id, kind)) = row else {
            // An unmanaged caller has no retained parent Attempt proving absence.
            return Ok(RecoveryResult::Unknown);
        };
        if stored_hash != payload_hash {
            return Ok(RecoveryResult::Conflict);
        }
        let submission_id =
            SubmissionId::from_parts(self.store_uuid, Uuid::parse_str(&submission_id)?);
        match state.as_str() {
            "received" => Ok(RecoveryResult::Received { submission_id }),
            "accepted" => {
                if kind == "batch" {
                    let batch_id = batch_id.ok_or_else(|| {
                        StoreError::InvalidState("accepted batch submission has no batch".into())
                    })?;
                    Ok(RecoveryResult::AcceptedBatch(self.batch_receipt(
                        submission_id,
                        BatchId::from_parts(self.store_uuid, Uuid::parse_str(&batch_id)?),
                    )?))
                } else {
                    let job_id = job_id.ok_or_else(|| {
                        StoreError::InvalidState("accepted submission has no job".into())
                    })?;
                    Ok(RecoveryResult::Accepted(self.receipt(
                        submission_id,
                        JobId::from_parts(self.store_uuid, Uuid::parse_str(&job_id)?),
                    )?))
                }
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
        let job_id = JobId::new(self.store_uuid);
        let claims = match ResolvedClaims::resolve(&spec.resources) {
            Ok(claims) => claims,
            Err(error) => {
                self.reject_received(submission_id)?;
                return Err(StoreError::InvalidSpec(error.to_string()));
            }
        };
        let accepted_ms = now_millis();
        let transaction = self.connection.transaction()?;
        let state: String = transaction.query_row(
            "SELECT state FROM submissions WHERE id = ?1",
            [submission_id.entity_uuid().to_string()],
            |row| row.get(0),
        )?;
        if state == "accepted" {
            let existing: String = transaction.query_row(
                "SELECT job_id FROM submissions WHERE id = ?1",
                [submission_id.entity_uuid().to_string()],
                |row| row.get(0),
            )?;
            transaction.commit()?;
            return Ok(SubmitResult {
                receipt: self.receipt(
                    submission_id,
                    JobId::from_parts(self.store_uuid, Uuid::parse_str(&existing)?),
                )?,
                should_schedule: false,
            });
        }
        if state != "received" {
            return Err(StoreError::InvalidState(format!(
                "submission {submission_id} is terminal in state {state}"
            )));
        }
        transaction.execute(
            "INSERT INTO jobs(id, submission_id, state, spec_json, claims_json, accepted_ms)
             VALUES (?1, ?2, 'pending', ?3, ?4, ?5)",
            params![
                job_id.entity_uuid().to_string(),
                submission_id.entity_uuid().to_string(),
                serde_json::to_string(spec)?,
                serde_json::to_string(&claims)?,
                accepted_ms,
            ],
        )?;
        transaction.execute(
            "UPDATE submissions SET state = 'accepted', job_id = ?2 WHERE id = ?1",
            params![
                submission_id.entity_uuid().to_string(),
                job_id.entity_uuid().to_string()
            ],
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
            [self.local_id(job_id)?],
            |row| row.get(0),
        )?;
        let queue_rank = if state == "pending" {
            Some(self.connection.query_row(
                "SELECT COUNT(*) FROM jobs
                 WHERE state = 'pending' AND rowid <= (
                     SELECT rowid FROM jobs WHERE id = ?1
                 )",
                [job_id.entity_uuid().to_string()],
                |row| row.get::<_, u64>(0),
            )?)
        } else {
            None
        };
        let blockers = if state == "pending" {
            self.blockers_for_job(job_id)?
        } else {
            Vec::new()
        };
        let estimate = self.estimate_for_job(job_id, &blockers)?;
        Ok(JobReceipt {
            submission_id,
            job_id,
            submission_state: SubmissionState::Accepted,
            job_state: parse_job_state(&state)?,
            blockers,
            queue_rank,
            estimate,
        })
    }

    fn blockers_for_job(&self, job_id: JobId) -> StoreResult<Vec<Blocker>> {
        let job_key = self.local_id(job_id)?;
        let mut blockers = self.dependency_blockers(&job_key)?.0;
        let claims: String = self.connection.query_row(
            "SELECT claims_json FROM jobs WHERE id = ?1",
            [&job_key],
            |row| row.get(0),
        )?;
        let claims: ResolvedClaims = serde_json::from_str(&claims)?;
        blockers.extend(claims.blockers(
            &self.capacities,
            &self.active_and_reserved_claims_before(&job_key)?,
        ));
        Ok(blockers)
    }

    fn active_and_reserved_claims_before(&self, job_key: &str) -> StoreResult<Vec<ResolvedClaims>> {
        let mut granted = self.active_claims()?;
        let mut statement = self.connection.prepare(
            "SELECT id, claims_json FROM jobs
             WHERE state = 'pending' AND rowid < (SELECT rowid FROM jobs WHERE id = ?1)
             ORDER BY accepted_ms, rowid",
        )?;
        let rows = statement.query_map([job_key], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        for row in rows {
            let (candidate, claims) = row?;
            let (dependencies, impossible) = self.dependency_blockers(&candidate)?;
            if impossible || !dependencies.is_empty() {
                continue;
            }
            let claims: ResolvedClaims = serde_json::from_str(&claims)?;
            if claims.blockers(&self.capacities, &granted).is_empty() {
                granted.push(claims);
            }
        }
        Ok(granted)
    }

    fn dependency_blockers(&self, job_key: &str) -> StoreResult<(Vec<Blocker>, bool)> {
        let mut statement = self.connection.prepare(
            "SELECT dependencies.kind, jobs.state, jobs.outcome, jobs.batch_member
             FROM dependencies JOIN jobs ON jobs.id = dependencies.predecessor_id
             WHERE dependencies.successor_id = ?1 ORDER BY jobs.batch_index",
        )?;
        let rows = statement.query_map([job_key], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })?;
        let mut blockers = Vec::new();
        let mut impossible = false;
        for row in rows {
            let (kind, state, outcome, name) = row?;
            let label = name.unwrap_or_else(|| "predecessor".into());
            if state != "final" {
                blockers.push(Blocker {
                    code: "dependency_pending".into(),
                    detail: label,
                });
                continue;
            }
            let satisfied = match kind.as_str() {
                "success" => outcome.as_deref() == Some("succeeded"),
                "failure" => outcome.as_deref() == Some("failed"),
                "terminal" => true,
                other => {
                    return Err(StoreError::InvalidState(format!(
                        "unknown dependency kind {other}"
                    )));
                }
            };
            if !satisfied {
                impossible = true;
                blockers.push(Blocker {
                    code: "dependency_impossible".into(),
                    detail: format!("{label} finalized as {}", outcome.unwrap_or_default()),
                });
            }
        }
        Ok((blockers, impossible))
    }

    fn active_claims(&self) -> StoreResult<Vec<ResolvedClaims>> {
        let mut statement = self
            .connection
            .prepare("SELECT claims_json FROM leases WHERE state = 'granted'")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
    }

    fn estimate_for_job(&self, job_id: JobId, blockers: &[Blocker]) -> StoreResult<Estimate> {
        if blockers.is_empty() {
            return Ok(Estimate {
                confidence: crate::EstimateConfidence::Estimated,
                start_in_millis: Some(0),
                assumptions: vec!["currently admissible".into()],
            });
        }
        if blockers.iter().any(|blocker| {
            blocker.code == "resource_capacity" || blocker.code == "dependency_impossible"
        }) {
            return Ok(Estimate::unknown(
                "a configured-capacity or impossible-dependency blocker has no time estimate",
            ));
        }
        let mut statement = self.connection.prepare(
            "SELECT accepted_ms, started_ms, spec_json FROM jobs
             WHERE id != ?1 AND (
                 state = 'active' OR (
                     state = 'pending' AND rowid < (SELECT rowid FROM jobs WHERE id = ?1)
                 )
             ) ORDER BY accepted_ms, rowid",
        )?;
        let rows = statement.query_map([self.local_id(job_id)?], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<i64>>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        let now = now_millis();
        let mut estimate = 0_u64;
        let mut saw_job = false;
        for row in rows {
            let (accepted, started, json) = row?;
            saw_job = true;
            let spec: JobSpec = serde_json::from_str(&json)?;
            let Some(seconds) = spec.expected_duration_seconds else {
                return Ok(Estimate::unknown(
                    "at least one running or earlier queued job has no declared duration",
                ));
            };
            let elapsed = started
                .map(|began| now.saturating_sub(began) as u64)
                .unwrap_or(0);
            let _ = accepted;
            estimate =
                estimate.saturating_add(seconds.saturating_mul(1000).saturating_sub(elapsed));
        }
        if saw_job {
            Ok(Estimate {
                confidence: crate::EstimateConfidence::Estimated,
                start_in_millis: Some(estimate),
                assumptions: vec![
                    "conservative FIFO estimate from declared durations of running and earlier queued jobs; orthogonal work may start sooner".into(),
                ],
            })
        } else {
            Ok(Estimate::unknown(
                "blocked work has no sufficient declared running duration",
            ))
        }
    }

    pub(crate) fn prepare_next_job(&mut self) -> StoreResult<Option<PreparedJob>> {
        for job_id in self.pending_jobs()? {
            if let Some(job) = self.prepare_job(job_id)? {
                return Ok(Some(job));
            }
        }
        Ok(None)
    }

    pub(crate) fn prepare_job(&mut self, job_id: JobId) -> StoreResult<Option<PreparedJob>> {
        let job_key = self.local_id(job_id)?;
        let capacities = self.capacities.clone();
        let attempt_id = AttemptId::new(self.store_uuid);
        let invocation_id = InvocationId::new(self.store_uuid);
        let containment_id = ContainmentId::new(self.store_uuid);
        let lease_id = Uuid::now_v7();
        let transaction = self.connection.transaction()?;
        let row = transaction
            .query_row(
                "SELECT spec_json, claims_json FROM jobs WHERE id = ?1 AND state = 'pending'",
                [job_key],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        let Some((spec_json, claims_json)) = row else {
            transaction.rollback()?;
            return Ok(None);
        };
        let (dependency_blockers, impossible) = dependency_blockers_tx(&transaction, job_id)?;
        if impossible {
            transaction.execute(
                "UPDATE jobs SET state = 'final', outcome = 'skipped', finished_ms = ?2
                 WHERE id = ?1 AND state = 'pending'",
                params![job_id.entity_uuid().to_string(), now_millis()],
            )?;
            transaction.commit()?;
            return Ok(None);
        }
        if !dependency_blockers.is_empty() {
            transaction.rollback()?;
            return Ok(None);
        }
        let claims: ResolvedClaims = serde_json::from_str(&claims_json)?;
        let active = active_claims_tx(&transaction)?;
        if !claims.blockers(&capacities, &active).is_empty() {
            transaction.rollback()?;
            return Ok(None);
        }
        let spec = serde_json::from_str(&spec_json)?;
        let log_directory = self.paths.logs.join(job_id.entity_uuid().to_string());
        std::fs::create_dir_all(&log_directory)?;
        transaction.execute(
            "UPDATE jobs SET state = 'active', attempt_id = ?2, invocation_id = ?3,
                containment_id = ?4 WHERE id = ?1 AND state = 'pending'",
            params![
                job_id.entity_uuid().to_string(),
                attempt_id.entity_uuid().to_string(),
                invocation_id.entity_uuid().to_string(),
                containment_id.entity_uuid().to_string(),
            ],
        )?;
        transaction.execute(
            "INSERT INTO attempts(id, job_id, state, attempt_index)
             VALUES (?1, ?2, 'starting', 1)",
            params![
                attempt_id.entity_uuid().to_string(),
                job_id.entity_uuid().to_string()
            ],
        )?;
        transaction.execute(
            "INSERT INTO invocations(id, attempt_id, role, state)
             VALUES (?1, ?2, 'primary', 'prepared')",
            params![
                invocation_id.entity_uuid().to_string(),
                attempt_id.entity_uuid().to_string()
            ],
        )?;
        transaction.execute(
            "INSERT INTO containments(id, invocation_id, state)
             VALUES (?1, ?2, 'creating')",
            params![
                containment_id.entity_uuid().to_string(),
                invocation_id.entity_uuid().to_string()
            ],
        )?;
        transaction.execute(
            "INSERT INTO leases(id, attempt_id, state, claims_json)
             VALUES (?1, ?2, 'granted', ?3)",
            params![
                lease_id.to_string(),
                attempt_id.entity_uuid().to_string(),
                claims_json,
            ],
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
            [job.job_id.entity_uuid().to_string()],
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
                job.invocation_id.entity_uuid().to_string(),
                root_pid,
                executable_hash,
                now_millis(),
            ],
        )?;
        transaction.execute(
            "UPDATE containments SET state = 'live' WHERE id = ?1",
            [job.containment_id.entity_uuid().to_string()],
        )?;
        transaction.execute(
            "UPDATE attempts SET state = 'running' WHERE id = ?1",
            [job.attempt_id.entity_uuid().to_string()],
        )?;
        transaction.execute(
            "UPDATE jobs SET started_ms = ?2 WHERE id = ?1",
            params![job.job_id.entity_uuid().to_string(), now_millis()],
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
            params![self.local_id(job_id)?, offset],
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
            [job.job_id.entity_uuid().to_string()],
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
            params![job.invocation_id.entity_uuid().to_string(), exit_code],
        )?;
        transaction.execute(
            "UPDATE jobs SET root_exit_code = ?2 WHERE id = ?1 AND state = 'active'",
            params![job.job_id.entity_uuid().to_string(), exit_code],
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
            [job.job_id.entity_uuid().to_string()],
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
            params![
                job.invocation_id.entity_uuid().to_string(),
                exit_code,
                now_millis()
            ],
        )?;
        transaction.execute(
            "UPDATE containments SET state = 'empty' WHERE id = ?1",
            [job.containment_id.entity_uuid().to_string()],
        )?;
        transaction.execute(
            "UPDATE attempts SET state = 'settled', verdict = ?2 WHERE id = ?1",
            params![job.attempt_id.entity_uuid().to_string(), verdict],
        )?;
        transaction.execute(
            "UPDATE leases SET state = 'released' WHERE attempt_id = ?1",
            [job.attempt_id.entity_uuid().to_string()],
        )?;
        transaction.execute(
            "UPDATE jobs SET state = 'final', outcome = ?2, root_exit_code = ?3,
                finished_ms = ?4 WHERE id = ?1",
            params![
                job.job_id.entity_uuid().to_string(),
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
            [job.job_id.entity_uuid().to_string()],
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
            params![
                job.invocation_id.entity_uuid().to_string(),
                exit_code,
                now_millis()
            ],
        )?;
        transaction.execute(
            "UPDATE containments SET state = 'uncertain'
             WHERE id = ?1 AND state IN ('creating', 'live')",
            [job.containment_id.entity_uuid().to_string()],
        )?;
        transaction.execute(
            "UPDATE attempts SET state = 'settled', verdict = ?2
             WHERE id = ?1 AND state != 'settled'",
            params![job.attempt_id.entity_uuid().to_string(), verdict],
        )?;
        // An uncertain Containment deliberately keeps its Lease granted.
        transaction.execute(
            "UPDATE jobs SET state = 'final', outcome = 'interrupted',
                root_exit_code = COALESCE(?2, root_exit_code), finished_ms = ?3
             WHERE id = ?1 AND state = 'active'",
            params![
                job.job_id.entity_uuid().to_string(),
                exit_code,
                now_millis()
            ],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn status(&self, job_id: JobId) -> StoreResult<JobSnapshot> {
        self.connection
            .query_row(
                "SELECT submission_id, state, outcome, attempt_id, invocation_id,
                    containment_id, root_exit_code, accepted_ms, started_ms, finished_ms,
                    spec_json, batch_id, batch_member
                 FROM jobs WHERE id = ?1",
                [self.local_id(job_id)?],
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
                        row.get::<_, Option<String>>(11)?,
                        row.get::<_, Option<String>>(12)?,
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
                    batch_id,
                    batch_member,
                )| {
                    let parsed_state = parse_job_state(&state)?;
                    Ok(JobSnapshot {
                        job_id,
                        submission_id: SubmissionId::from_parts(
                            self.store_uuid,
                            Uuid::parse_str(&submission_id)?,
                        ),
                        batch_id: batch_id
                            .map(|value| {
                                Uuid::parse_str(&value)
                                    .map(|uuid| BatchId::from_parts(self.store_uuid, uuid))
                            })
                            .transpose()?,
                        batch_member,
                        state: parsed_state,
                        outcome: outcome.map(|value| parse_outcome(&value)).transpose()?,
                        attempt_id: attempt_id
                            .map(|value| {
                                Uuid::parse_str(&value)
                                    .map(|uuid| AttemptId::from_parts(self.store_uuid, uuid))
                            })
                            .transpose()?,
                        invocation_id: invocation_id
                            .map(|value| {
                                Uuid::parse_str(&value)
                                    .map(|uuid| InvocationId::from_parts(self.store_uuid, uuid))
                            })
                            .transpose()?,
                        containment_id: containment_id
                            .map(|value| {
                                Uuid::parse_str(&value)
                                    .map(|uuid| ContainmentId::from_parts(self.store_uuid, uuid))
                            })
                            .transpose()?,
                        root_exit_code,
                        accepted_unix_millis: accepted_ms,
                        started_unix_millis: started_ms,
                        finished_unix_millis: finished_ms,
                        spec: serde_json::from_str(&spec_json)?,
                        blockers: if parsed_state == JobState::Pending {
                            self.blockers_for_job(job_id)?
                        } else {
                            Vec::new()
                        },
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
                [self.local_id(job_id)?],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?,
            LogStream::Stderr => self.connection.query_row(
                "SELECT stderr_len, state, COALESCE((
                    SELECT state FROM containments WHERE id = jobs.containment_id
                 ), 'empty') FROM jobs WHERE id = ?1",
                [self.local_id(job_id)?],
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
            store_path: self.paths.root.clone(),
            config_path: self.paths.config.clone(),
            capacities: self.capacities.clone(),
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
            jobs.push(JobId::from_parts(self.store_uuid, Uuid::parse_str(&row?)?));
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
                "SELECT id, spec_json, kind FROM submissions
                 WHERE state = 'received' ORDER BY created_ms",
            )?;
            let rows = statement.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        for (submission_id, spec_json, kind) in received {
            let submission_id =
                SubmissionId::from_parts(self.store_uuid, Uuid::parse_str(&submission_id)?);
            let result = if kind == "batch" {
                self.accept_received_batch(submission_id, &serde_json::from_str(&spec_json)?)
                    .map(|_| ())
            } else {
                self.accept_received(submission_id, &serde_json::from_str(&spec_json)?)
                    .map(|_| ())
            };
            match result {
                Ok(()) | Err(StoreError::InvalidSpec(_)) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
}

fn dependency_blockers_tx(
    transaction: &rusqlite::Transaction<'_>,
    job_id: JobId,
) -> StoreResult<(Vec<Blocker>, bool)> {
    let mut statement = transaction.prepare(
        "SELECT dependencies.kind, jobs.state, jobs.outcome, jobs.batch_member
         FROM dependencies JOIN jobs ON jobs.id = dependencies.predecessor_id
         WHERE dependencies.successor_id = ?1 ORDER BY jobs.batch_index",
    )?;
    let rows = statement.query_map([job_id.entity_uuid().to_string()], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<String>>(3)?,
        ))
    })?;
    let mut blockers = Vec::new();
    let mut impossible = false;
    for row in rows {
        let (kind, state, outcome, name) = row?;
        if state != "final" {
            blockers.push(Blocker {
                code: "dependency_pending".into(),
                detail: name.unwrap_or_else(|| "predecessor".into()),
            });
            continue;
        }
        let satisfied = match kind.as_str() {
            "success" => outcome.as_deref() == Some("succeeded"),
            "failure" => outcome.as_deref() == Some("failed"),
            "terminal" => true,
            other => {
                return Err(StoreError::InvalidState(format!(
                    "unknown dependency kind {other}"
                )));
            }
        };
        impossible |= !satisfied;
    }
    Ok((blockers, impossible))
}

fn active_claims_tx(transaction: &rusqlite::Transaction<'_>) -> StoreResult<Vec<ResolvedClaims>> {
    let mut statement =
        transaction.prepare("SELECT claims_json FROM leases WHERE state = 'granted'")?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
}

pub(crate) fn normalized_payload_hash(spec: &JobSpec) -> StoreResult<String> {
    let normalized = serde_json::to_vec(spec)?;
    Ok(format!("{:x}", Sha256::digest(normalized)))
}

pub(crate) fn normalized_batch_payload_hash(spec: &BatchSpec) -> StoreResult<String> {
    let normalized = serde_json::to_vec(spec)?;
    Ok(format!("{:x}", Sha256::digest(normalized)))
}

fn dependency_kind(kind: crate::DependencyKind) -> &'static str {
    match kind {
        crate::DependencyKind::Success => "success",
        crate::DependencyKind::Failure => "failure",
        crate::DependencyKind::Terminal => "terminal",
    }
}

fn load_capacities(path: &Path) -> StoreResult<ResourceCapacities> {
    match File::open(path) {
        Ok(file) => {
            let capacities: ResourceCapacities = serde_json::from_reader(file)?;
            capacities
                .validate()
                .map_err(|error| StoreError::InvalidSpec(error.to_string()))?;
            Ok(capacities)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(ResourceCapacities::default())
        }
        Err(error) => Err(error.into()),
    }
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
        2 => return validate_schema(connection),
        1 => {
            connection.execute_batch(
                "BEGIN IMMEDIATE;
                 ALTER TABLE submissions ADD COLUMN kind TEXT NOT NULL DEFAULT 'job';
                 ALTER TABLE submissions ADD COLUMN batch_id TEXT;
                 ALTER TABLE batches ADD COLUMN submission_id TEXT REFERENCES submissions(id);
                 ALTER TABLE batches ADD COLUMN accepted_ms INTEGER;
                 ALTER TABLE jobs ADD COLUMN batch_id TEXT REFERENCES batches(id);
                 ALTER TABLE jobs ADD COLUMN batch_member TEXT;
                 ALTER TABLE jobs ADD COLUMN batch_index INTEGER;
                 ALTER TABLE jobs ADD COLUMN claims_json TEXT NOT NULL DEFAULT '{}';
                 CREATE TABLE dependencies(
                     predecessor_id TEXT NOT NULL REFERENCES jobs(id),
                     successor_id TEXT NOT NULL REFERENCES jobs(id),
                     kind TEXT NOT NULL,
                     PRIMARY KEY(predecessor_id, successor_id, kind)
                 );
                 PRAGMA user_version = 2;
                 COMMIT;",
            )?;
            return validate_schema(connection);
        }
        0 => {}
        unsupported => {
            return Err(StoreError::InvalidState(format!(
                "unsupported SQLite schema version {unsupported}; expected 2"
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
             kind TEXT NOT NULL DEFAULT 'job',
             batch_id TEXT,
             created_ms INTEGER NOT NULL,
             UNIQUE(scope, idempotency_key)
         );
         CREATE TABLE IF NOT EXISTS batches(
             id TEXT PRIMARY KEY,
             state TEXT NOT NULL,
             submission_id TEXT NOT NULL REFERENCES submissions(id),
             accepted_ms INTEGER NOT NULL
         );
         CREATE TABLE IF NOT EXISTS jobs(
             id TEXT PRIMARY KEY,
             submission_id TEXT NOT NULL REFERENCES submissions(id),
             batch_id TEXT REFERENCES batches(id),
             batch_member TEXT,
             batch_index INTEGER,
             state TEXT NOT NULL,
             outcome TEXT,
             spec_json TEXT NOT NULL,
             claims_json TEXT NOT NULL DEFAULT '{}',
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
         CREATE TABLE IF NOT EXISTS dependencies(
             predecessor_id TEXT NOT NULL REFERENCES jobs(id),
             successor_id TEXT NOT NULL REFERENCES jobs(id),
             kind TEXT NOT NULL,
             PRIMARY KEY(predecessor_id, successor_id, kind)
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
         PRAGMA user_version = 2;
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
        "dependencies",
    ] {
        let exists: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
            [table],
            |row| row.get(0),
        )?;
        if !exists {
            return Err(StoreError::InvalidState(format!(
                "schema version 2 is missing table {table}; refusing reconstruction"
            )));
        }
    }
    for (table, columns) in [
        ("submissions", &["kind", "batch_id"] as &[_]),
        ("batches", &["submission_id", "accepted_ms"] as &[_]),
        (
            "jobs",
            &["batch_id", "batch_member", "batch_index", "claims_json"] as &[_],
        ),
        (
            "dependencies",
            &["predecessor_id", "successor_id", "kind"] as &[_],
        ),
    ] {
        let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
        let present: std::collections::HashSet<String> = statement
            .query_map([], |row| row.get(1))?
            .collect::<std::result::Result<_, _>>()?;
        for column in columns {
            if !present.contains(*column) {
                return Err(StoreError::InvalidState(format!(
                    "schema version 2 table {table} is missing column {column}; refusing reconstruction"
                )));
            }
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
    use crate::{
        BatchMember, DependencyKind, DependencySpec, EnvironmentSpec, EstimateConfidence,
        ResourceClaims, RetryPolicy, SPEC_VERSION, StdinSpec,
    };

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

    fn capacities() -> ResourceCapacities {
        ResourceCapacities {
            cpu_units: 4,
            ram_mb: 16_384,
            cargo_slots: 1,
            gpu_slots: 1,
            custom: [("review_slots".into(), 2)].into(),
        }
    }

    fn member(name: &str, spec: JobSpec, dependencies: Vec<DependencySpec>) -> BatchMember {
        BatchMember {
            name: name.into(),
            spec,
            dependencies,
        }
    }

    #[test]
    fn increment_2a_a03_batch_is_atomic_and_dependencies_use_final_outcomes() {
        let temp = tempfile::tempdir().unwrap();
        let paths = StorePaths::new(temp.path().to_path_buf());
        let mut store = Store::open_with_capacities(paths, capacities()).unwrap();
        let mut invalid = BatchSpec {
            spec_version: SPEC_VERSION,
            jobs: vec![member(
                "only",
                spec(temp.path()),
                vec![DependencySpec {
                    job: "missing".into(),
                    on: DependencyKind::Success,
                }],
            )],
        };
        let hash = normalized_batch_payload_hash(&invalid).unwrap();
        assert!(matches!(
            store.submit_batch(Uuid::now_v7(), &hash, &invalid),
            Err(StoreError::InvalidSpec(_))
        ));
        let count: u64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM jobs", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0, "invalid atomic batch must create no members");

        invalid.jobs = vec![
            member("root", spec(temp.path()), vec![]),
            member(
                "successor",
                spec(temp.path()),
                vec![DependencySpec {
                    job: "root".into(),
                    on: DependencyKind::Success,
                }],
            ),
            member(
                "finally",
                spec(temp.path()),
                vec![DependencySpec {
                    job: "root".into(),
                    on: DependencyKind::Terminal,
                }],
            ),
        ];
        let hash = normalized_batch_payload_hash(&invalid).unwrap();
        let receipt = store
            .submit_batch(Uuid::now_v7(), &hash, &invalid)
            .unwrap()
            .receipt;
        assert_eq!(receipt.jobs.len(), 3);
        assert_eq!(
            receipt.jobs[1].receipt.blockers[0].code,
            "dependency_pending"
        );
        let root = store.prepare_next_job().unwrap().unwrap();
        assert_eq!(root.job_id, receipt.jobs[0].receipt.job_id);
        store
            .mark_finished(&root, Some(1), JobOutcome::Failed, "process_failed")
            .unwrap();
        let finally = store.prepare_next_job().unwrap().unwrap();
        assert_eq!(finally.job_id, receipt.jobs[2].receipt.job_id);
        let skipped = store.status(receipt.jobs[1].receipt.job_id).unwrap();
        assert_eq!(skipped.outcome, Some(JobOutcome::Skipped));
    }

    #[test]
    fn increment_2a_a04_complete_leases_serialize_conflicts_but_allow_orthogonal_work() {
        let temp = tempfile::tempdir().unwrap();
        let mut store =
            Store::open_with_capacities(StorePaths::new(temp.path().to_path_buf()), capacities())
                .unwrap();
        let mut cpu = spec(temp.path());
        cpu.resources.cpu_units = Some(3);
        cpu.resources.ram_mb = Some(8_000);
        cpu.expected_duration_seconds = Some(30);
        let mut blocked = spec(temp.path());
        blocked.resources.cpu_units = Some(2);
        blocked.resources.ram_mb = Some(1_000);
        let mut gpu = spec(temp.path());
        gpu.resources.gpu_slots = Some(1);
        let batch = BatchSpec {
            spec_version: SPEC_VERSION,
            jobs: vec![
                member("cpu", cpu, vec![]),
                member("blocked", blocked, vec![]),
                member("gpu", gpu, vec![]),
            ],
        };
        let hash = normalized_batch_payload_hash(&batch).unwrap();
        let receipt = store
            .submit_batch(Uuid::now_v7(), &hash, &batch)
            .unwrap()
            .receipt;
        assert!(
            receipt.jobs[1]
                .receipt
                .blockers
                .iter()
                .any(|blocker| blocker.code == "resource_busy"),
            "receipt must account for an earlier compatible queue reservation"
        );
        assert!(receipt.jobs[2].receipt.blockers.is_empty());
        let cpu = store.prepare_next_job().unwrap().unwrap();
        assert_eq!(cpu.job_id, receipt.jobs[0].receipt.job_id);
        let gpu = store.prepare_next_job().unwrap().unwrap();
        assert_eq!(
            gpu.job_id, receipt.jobs[2].receipt.job_id,
            "a partially fitting CPU claim must not reserve RAM or block orthogonal GPU work"
        );
        let blocked = store.status(receipt.jobs[1].receipt.job_id).unwrap();
        assert!(
            blocked
                .blockers
                .iter()
                .any(|item| item.code == "resource_busy")
        );
        store
            .mark_finished(&cpu, Some(0), JobOutcome::Succeeded, "succeeded")
            .unwrap();
        let admitted = store.prepare_next_job().unwrap().unwrap();
        assert_eq!(admitted.job_id, receipt.jobs[1].receipt.job_id);
    }

    #[test]
    fn increment_2a_a06_receipt_reports_rank_blocker_and_honest_estimate() {
        let temp = tempfile::tempdir().unwrap();
        let mut store =
            Store::open_with_capacities(StorePaths::new(temp.path().to_path_buf()), capacities())
                .unwrap();
        let mut first = spec(temp.path());
        first.resources.cargo_slots = Some(1);
        first.expected_duration_seconds = Some(60);
        let hash = normalized_payload_hash(&first).unwrap();
        let first = store.submit(Uuid::now_v7(), &hash, &first).unwrap();
        let running = store.prepare_next_job().unwrap().unwrap();
        assert_eq!(running.job_id, first.receipt.job_id);

        let mut waiting = spec(temp.path());
        waiting.resources.cargo_slots = Some(1);
        let hash = normalized_payload_hash(&waiting).unwrap();
        let waiting = store.submit(Uuid::now_v7(), &hash, &waiting).unwrap();
        assert_eq!(waiting.receipt.queue_rank, Some(1));
        assert!(waiting.receipt.blockers.iter().any(|blocker| {
            blocker.code == "resource_busy" && blocker.detail.contains("cargo_slots")
        }));
        assert_eq!(
            waiting.receipt.estimate.confidence,
            EstimateConfidence::Estimated
        );
        assert!(waiting.receipt.estimate.start_in_millis.is_some());
    }

    #[test]
    fn increment_2a_a04_missing_path_fence_identity_survives_later_creation() {
        let temp = tempfile::tempdir().unwrap();
        let mut store =
            Store::open_with_capacities(StorePaths::new(temp.path().to_path_buf()), capacities())
                .unwrap();
        let fenced = temp.path().join("future-slot");
        let mut first = spec(temp.path());
        first.resources.exclusive_fences = vec![fenced.to_string_lossy().into_owned()];
        let second = first.clone();
        let first_hash = normalized_payload_hash(&first).unwrap();
        let first = store
            .submit(Uuid::now_v7(), &first_hash, &first)
            .unwrap()
            .receipt;
        let second_hash = normalized_payload_hash(&second).unwrap();
        let second = store
            .submit(Uuid::now_v7(), &second_hash, &second)
            .unwrap()
            .receipt;
        std::fs::create_dir(&fenced).unwrap();
        let admitted = store.prepare_next_job().unwrap().unwrap();
        assert_eq!(admitted.job_id, first.job_id);
        let snapshot = store.status(second.job_id).unwrap();
        assert!(
            snapshot
                .blockers
                .iter()
                .any(|blocker| blocker.code == "path_fence_busy")
        );
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
    fn foreign_store_id_rejects_even_if_entity_uuid_collides() {
        let first_dir = tempfile::tempdir().unwrap();
        let second_dir = tempfile::tempdir().unwrap();
        let mut first = Store::open(StorePaths::new(first_dir.path().to_path_buf())).unwrap();
        let job_spec = spec(first_dir.path());
        let hash = normalized_payload_hash(&job_spec).unwrap();
        let receipt = first
            .submit(Uuid::now_v7(), &hash, &job_spec)
            .unwrap()
            .receipt;
        let second = Store::open(StorePaths::new(second_dir.path().to_path_buf())).unwrap();
        let foreign = JobId::from_parts(second.store_uuid, receipt.job_id.entity_uuid());
        assert!(matches!(
            first.status(foreign),
            Err(StoreError::NotFound(message)) if message.contains("foreign durable ID")
        ));
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
                [job_id.entity_uuid().to_string()],
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
                [job_id.entity_uuid().to_string()],
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
                [job_id.entity_uuid().to_string()],
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
                [prepared.job_id.entity_uuid().to_string()],
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
        {
            let store = Store::open(paths).unwrap();
            let submission_id = SubmissionId::new(store.store_uuid);
            store
                .connection
                .execute(
                    "INSERT INTO submissions(
                        id, scope, idempotency_key, payload_hash, state, spec_json, created_ms
                     ) VALUES (?1, 'unmanaged', ?2, ?3, 'received', ?4, ?5)",
                    params![
                        submission_id.entity_uuid().to_string(),
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
            .execute_batch("PRAGMA user_version = 3;")
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

    #[test]
    fn schema_v1_migrates_transactionally_to_v2() {
        let temp = tempfile::tempdir().unwrap();
        let paths = StorePaths::new(temp.path().to_path_buf());
        paths.ensure().unwrap();
        let connection = Connection::open(&paths.database).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
                 INSERT INTO meta(key, value) VALUES ('store_uuid', '00000000-0000-0000-0000-000000000001');
                 CREATE TABLE submissions(
                    id TEXT PRIMARY KEY, scope TEXT NOT NULL, idempotency_key TEXT NOT NULL,
                    payload_hash TEXT NOT NULL, state TEXT NOT NULL, spec_json TEXT NOT NULL,
                    job_id TEXT, created_ms INTEGER NOT NULL, UNIQUE(scope, idempotency_key));
                 CREATE TABLE batches(id TEXT PRIMARY KEY, state TEXT NOT NULL);
                 CREATE TABLE jobs(
                    id TEXT PRIMARY KEY, submission_id TEXT NOT NULL REFERENCES submissions(id),
                    state TEXT NOT NULL, outcome TEXT, spec_json TEXT NOT NULL, attempt_id TEXT,
                    invocation_id TEXT, containment_id TEXT, root_exit_code INTEGER,
                    accepted_ms INTEGER NOT NULL, started_ms INTEGER, finished_ms INTEGER,
                    stdout_len INTEGER NOT NULL DEFAULT 0, stderr_len INTEGER NOT NULL DEFAULT 0);
                 CREATE TABLE attempts(
                    id TEXT PRIMARY KEY, job_id TEXT NOT NULL REFERENCES jobs(id), state TEXT NOT NULL,
                    attempt_index INTEGER NOT NULL, verdict TEXT);
                 CREATE TABLE invocations(
                    id TEXT PRIMARY KEY, attempt_id TEXT NOT NULL REFERENCES attempts(id),
                    role TEXT NOT NULL, state TEXT NOT NULL, root_pid INTEGER, root_exit_code INTEGER,
                    executable_hash TEXT, started_ms INTEGER, finished_ms INTEGER);
                 CREATE TABLE containments(
                    id TEXT PRIMARY KEY, invocation_id TEXT NOT NULL REFERENCES invocations(id),
                    state TEXT NOT NULL);
                 CREATE TABLE conditions(
                    id TEXT PRIMARY KEY, job_id TEXT NOT NULL REFERENCES jobs(id),
                    state TEXT NOT NULL, spec_json TEXT NOT NULL);
                 CREATE TABLE observations(
                    id TEXT PRIMARY KEY, condition_id TEXT NOT NULL REFERENCES conditions(id),
                    observed_ms INTEGER NOT NULL, value_json TEXT NOT NULL);
                 CREATE TABLE leases(
                    id TEXT PRIMARY KEY, attempt_id TEXT NOT NULL REFERENCES attempts(id),
                    state TEXT NOT NULL, claims_json TEXT NOT NULL);
                 PRAGMA user_version = 1;",
            )
            .unwrap();
        drop(connection);

        let store = Store::open(paths).unwrap();
        let version: u32 = store
            .connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, 2);
        let columns: Vec<String> = store
            .connection
            .prepare("PRAGMA table_info(jobs)")
            .unwrap()
            .query_map([], |row| row.get(1))
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        assert!(columns.iter().any(|column| column == "claims_json"));
        assert!(columns.iter().any(|column| column == "batch_index"));
    }

    #[test]
    fn existing_store_identity_is_never_reconstructed() {
        let temp = tempfile::tempdir().unwrap();
        let paths = StorePaths::new(temp.path().to_path_buf());
        let store = Store::open(paths).unwrap();
        store
            .connection
            .execute("DELETE FROM meta WHERE key = 'store_uuid'", [])
            .unwrap();
        drop(store);
        assert!(matches!(
            Store::open(StorePaths::new(temp.path().to_path_buf())),
            Err(StoreError::InvalidState(message)) if message.contains("complete store directory aside")
        ));
    }
}
