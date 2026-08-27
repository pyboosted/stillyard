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

// Pre-stable Stillyard intentionally has no migration chain. Change this opaque epoch whenever
// the current schema changes; daemon startup will replace the whole SQLite database.
const STORE_SCHEMA_EPOCH: &str = "stillyard-alpha-2026-08-27";

#[derive(Clone)]
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
    #[error("submission rejected: {0}")]
    Rejected(String),
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

pub(crate) struct PrepareNext {
    pub(crate) job: Option<PreparedJob>,
    pub(crate) state_changed: bool,
}

enum PrepareJob {
    Ready(Box<PreparedJob>),
    Blocked,
    Skipped,
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
        let database_existed = paths.database.try_exists()?;
        if !database_existed {
            // A crash may have left sidecars without a main database file.
            reset_database_files(&paths)?;
            return Self::open_fresh(paths, capacities);
        }

        let connection = match Connection::open(&paths.database) {
            Ok(connection) => connection,
            Err(error) if is_database_corruption(&error) => {
                reset_database_files(&paths)?;
                return Self::open_fresh(paths, capacities);
            }
            Err(error) => return Err(error.into()),
        };
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        if !schema_is_current(&connection)? {
            drop(connection);
            reset_database_files(&paths)?;
            return Self::open_fresh(paths, capacities);
        }

        match Self::finish_open(connection, paths.clone(), capacities.clone()) {
            Ok(store) => Ok(store),
            Err(StoreError::Sqlite(error)) if is_database_corruption(&error) => {
                reset_database_files(&paths)?;
                Self::open_fresh(paths, capacities)
            }
            Err(error) => Err(error),
        }
    }

    fn open_fresh(paths: StorePaths, capacities: ResourceCapacities) -> StoreResult<Self> {
        let connection = Connection::open(&paths.database)?;
        configure_database(&connection)?;
        create_current_schema(&connection, Uuid::now_v7())?;
        Self::finish_open(connection, paths, capacities)
    }

    fn finish_open(
        connection: Connection,
        paths: StorePaths,
        capacities: ResourceCapacities,
    ) -> StoreResult<Self> {
        configure_database(&connection)?;
        let store_uuid = current_store_uuid(&connection)?;
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
            if state == "rejected" {
                return Err(StoreError::Rejected(
                    "the retained submission decision is rejected".into(),
                ));
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
            if state == "rejected" {
                return Err(StoreError::Rejected(
                    "the retained submission decision is rejected".into(),
                ));
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
                return Err(StoreError::Rejected(error.to_string()));
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
                let predecessor = names.get(dependency.job.as_str()).copied().ok_or_else(|| {
                    StoreError::InvalidState(format!(
                        "retained batch member {} has unknown predecessor {}",
                        member.name, dependency.job
                    ))
                })?;
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
                return Err(StoreError::Rejected(error.to_string()));
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
        if blockers
            .iter()
            .any(|blocker| blocker.code == "dependency_pending")
        {
            return Ok(Estimate::unknown(
                "dependency completion is not estimated without walking its full predecessor closure",
            ));
        }
        let claims_json: String = self.connection.query_row(
            "SELECT claims_json FROM jobs WHERE id = ?1",
            [self.local_id(job_id)?],
            |row| row.get(0),
        )?;
        let claims: ResolvedClaims = serde_json::from_str(&claims_json)?;
        let retained = self.settled_granted_claims()?;
        if !claims.blockers(&self.capacities, &retained).is_empty() {
            return Ok(Estimate::unknown(
                "a retained Lease from an uncertain Containment has no automatic release estimate",
            ));
        }
        let mut statement = self.connection.prepare(
            "SELECT accepted_ms, started_ms, spec_json, state FROM jobs
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
                row.get::<_, String>(3)?,
            ))
        })?;
        let now = now_millis();
        let mut estimate = 0_u64;
        let mut saw_job = false;
        for row in rows {
            let (accepted, started, json, state) = row?;
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
            if state == "active" && elapsed >= seconds.saturating_mul(1000) {
                return Ok(Estimate::unknown(
                    "a running job has exceeded its declared duration",
                ));
            }
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

    fn settled_granted_claims(&self) -> StoreResult<Vec<ResolvedClaims>> {
        let mut statement = self.connection.prepare(
            "SELECT leases.claims_json FROM leases
             JOIN attempts ON attempts.id = leases.attempt_id
             WHERE leases.state = 'granted' AND attempts.state = 'settled'",
        )?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
    }

    #[cfg(test)]
    pub(crate) fn prepare_next_job(&mut self) -> StoreResult<Option<PreparedJob>> {
        Ok(self.prepare_next_job_with_progress()?.job)
    }

    pub(crate) fn prepare_next_job_with_progress(&mut self) -> StoreResult<PrepareNext> {
        let mut state_changed = false;
        loop {
            let mut skipped_in_pass = false;
            for job_id in self.pending_jobs()? {
                match self.prepare_job_inner(job_id)? {
                    PrepareJob::Ready(job) => {
                        return Ok(PrepareNext {
                            job: Some(*job),
                            state_changed,
                        });
                    }
                    PrepareJob::Blocked => {}
                    PrepareJob::Skipped => {
                        skipped_in_pass = true;
                        state_changed = true;
                    }
                }
            }
            if !skipped_in_pass {
                return Ok(PrepareNext {
                    job: None,
                    state_changed,
                });
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn prepare_job(&mut self, job_id: JobId) -> StoreResult<Option<PreparedJob>> {
        Ok(match self.prepare_job_inner(job_id)? {
            PrepareJob::Ready(job) => Some(*job),
            PrepareJob::Blocked | PrepareJob::Skipped => None,
        })
    }

    fn prepare_job_inner(&mut self, job_id: JobId) -> StoreResult<PrepareJob> {
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
            return Ok(PrepareJob::Blocked);
        };
        let (dependency_blockers, impossible) = dependency_blockers_tx(&transaction, job_id)?;
        if impossible {
            transaction.execute(
                "UPDATE jobs SET state = 'final', outcome = 'skipped', finished_ms = ?2
                 WHERE id = ?1 AND state = 'pending'",
                params![job_id.entity_uuid().to_string(), now_millis()],
            )?;
            transaction.commit()?;
            return Ok(PrepareJob::Skipped);
        }
        if !dependency_blockers.is_empty() {
            transaction.rollback()?;
            return Ok(PrepareJob::Blocked);
        }
        let claims: ResolvedClaims = serde_json::from_str(&claims_json)?;
        let active = active_claims_tx(&transaction)?;
        if !claims.blockers(&capacities, &active).is_empty() {
            transaction.rollback()?;
            return Ok(PrepareJob::Blocked);
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
        Ok(PrepareJob::Ready(Box::new(PreparedJob {
            job_id,
            attempt_id,
            invocation_id,
            containment_id,
            spec,
            stdout_path: self.paths.stdout_path(job_id),
            stderr_path: self.paths.stderr_path(job_id),
        })))
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
            .prepare("SELECT id FROM jobs WHERE state = 'pending' ORDER BY accepted_ms, rowid")?;
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
                match serde_json::from_str(&spec_json) {
                    Ok(spec) => self.accept_received_batch(submission_id, &spec).map(|_| ()),
                    Err(error) => {
                        self.reject_received(submission_id)?;
                        Err(StoreError::Rejected(format!(
                            "retained BatchSpec cannot be decoded: {error}"
                        )))
                    }
                }
            } else {
                match serde_json::from_str(&spec_json) {
                    Ok(spec) => self.accept_received(submission_id, &spec).map(|_| ()),
                    Err(error) => {
                        self.reject_received(submission_id)?;
                        Err(StoreError::Rejected(format!(
                            "retained JobSpec cannot be decoded: {error}"
                        )))
                    }
                }
            };
            match result {
                Ok(()) | Err(StoreError::InvalidSpec(_) | StoreError::Rejected(_)) => {}
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

fn configure_database(connection: &Connection) -> StoreResult<()> {
    connection.execute_batch(
        "PRAGMA foreign_keys = ON;
         PRAGMA journal_mode = WAL;
         PRAGMA synchronous = FULL;
         PRAGMA busy_timeout = 5000;",
    )?;
    Ok(())
}

fn schema_is_current(connection: &Connection) -> StoreResult<bool> {
    let meta_exists = match connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'meta')",
        [],
        |row| row.get::<_, bool>(0),
    ) {
        Ok(exists) => exists,
        Err(error) => return schema_probe_error(error.into()),
    };
    if !meta_exists {
        return Ok(false);
    }
    let meta_columns = match table_columns(connection, "meta") {
        Ok(columns) => columns,
        Err(error) => return schema_probe_error(error),
    };
    if !["key", "value"]
        .iter()
        .all(|column| meta_columns.contains(*column))
    {
        return Ok(false);
    }

    let epoch = match connection
        .query_row(
            "SELECT value FROM meta WHERE key = 'schema_epoch'",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()
    {
        Ok(epoch) => epoch,
        Err(error) => return schema_probe_error(error.into()),
    };
    if epoch.as_deref() != Some(STORE_SCHEMA_EPOCH) {
        return Ok(false);
    }

    match current_store_uuid(connection) {
        Ok(_) => {}
        Err(error) => return schema_probe_error(error),
    }
    match validate_schema(connection) {
        Ok(()) => Ok(true),
        Err(error) => schema_probe_error(error),
    }
}

fn schema_probe_error(error: StoreError) -> StoreResult<bool> {
    match error {
        StoreError::InvalidState(_) => Ok(false),
        StoreError::Sqlite(ref sqlite) if is_database_corruption(sqlite) => Ok(false),
        other => Err(other),
    }
}

fn is_database_corruption(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error {
                code: rusqlite::ErrorCode::DatabaseCorrupt | rusqlite::ErrorCode::NotADatabase,
                ..
            },
            _
        )
    )
}

fn table_columns(
    connection: &Connection,
    table: &str,
) -> StoreResult<std::collections::HashSet<String>> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    statement
        .query_map([], |row| row.get(1))?
        .collect::<std::result::Result<_, _>>()
        .map_err(Into::into)
}

fn current_store_uuid(connection: &Connection) -> StoreResult<Uuid> {
    let value = connection
        .query_row(
            "SELECT value FROM meta WHERE key = 'store_uuid'",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .ok_or_else(|| StoreError::InvalidState("current store has no store_uuid".into()))?;
    Uuid::parse_str(&value)
        .map_err(|_| StoreError::InvalidState("current store has an invalid store_uuid".into()))
}

fn reset_database_files(paths: &StorePaths) -> StoreResult<()> {
    for path in [
        sqlite_sidecar_path(&paths.database, "-wal"),
        sqlite_sidecar_path(&paths.database, "-shm"),
        paths.database.clone(),
    ] {
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn sqlite_sidecar_path(database: &Path, suffix: &str) -> PathBuf {
    let mut path = database.as_os_str().to_os_string();
    path.push(suffix);
    PathBuf::from(path)
}

fn create_current_schema(connection: &Connection, store_uuid: Uuid) -> StoreResult<()> {
    connection.execute_batch(&format!(
        "BEGIN IMMEDIATE;
         CREATE TABLE meta(
             key TEXT PRIMARY KEY,
             value TEXT NOT NULL
         );
         CREATE TABLE submissions(
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
         CREATE TABLE batches(
             id TEXT PRIMARY KEY,
             state TEXT NOT NULL,
             submission_id TEXT REFERENCES submissions(id),
             accepted_ms INTEGER
         );
         CREATE TABLE jobs(
             id TEXT PRIMARY KEY,
             submission_id TEXT NOT NULL REFERENCES submissions(id),
             batch_id TEXT REFERENCES batches(id),
             batch_member TEXT,
             batch_index INTEGER,
             state TEXT NOT NULL,
             outcome TEXT,
             spec_json TEXT NOT NULL,
             claims_json TEXT NOT NULL DEFAULT '{{}}',
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
         CREATE TABLE dependencies(
             predecessor_id TEXT NOT NULL REFERENCES jobs(id),
             successor_id TEXT NOT NULL REFERENCES jobs(id),
             kind TEXT NOT NULL,
             PRIMARY KEY(predecessor_id, successor_id, kind)
         );
         CREATE TABLE attempts(
             id TEXT PRIMARY KEY,
             job_id TEXT NOT NULL REFERENCES jobs(id),
             state TEXT NOT NULL,
             attempt_index INTEGER NOT NULL,
             verdict TEXT
         );
         CREATE TABLE invocations(
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
         CREATE TABLE containments(
             id TEXT PRIMARY KEY,
             invocation_id TEXT NOT NULL REFERENCES invocations(id),
             state TEXT NOT NULL
         );
         CREATE TABLE conditions(
             id TEXT PRIMARY KEY,
             job_id TEXT NOT NULL REFERENCES jobs(id),
             state TEXT NOT NULL,
             spec_json TEXT NOT NULL
         );
         CREATE TABLE observations(
             id TEXT PRIMARY KEY,
             condition_id TEXT NOT NULL REFERENCES conditions(id),
             observed_ms INTEGER NOT NULL,
             value_json TEXT NOT NULL
         );
         CREATE TABLE leases(
             id TEXT PRIMARY KEY,
             attempt_id TEXT NOT NULL REFERENCES attempts(id),
             state TEXT NOT NULL,
             claims_json TEXT NOT NULL
         );
         INSERT INTO meta(key, value) VALUES ('store_uuid', '{store_uuid}');
         INSERT INTO meta(key, value) VALUES ('schema_epoch', '{STORE_SCHEMA_EPOCH}');
         COMMIT;"
    ))?;
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
                "current schema is missing table {table}"
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
        let present = table_columns(connection, table)?;
        for column in columns {
            if !present.contains(*column) {
                return Err(StoreError::InvalidState(format!(
                    "current schema table {table} is missing column {column}"
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
    fn increment_2a_a03_reverse_order_skip_closure_reaches_terminal_state() {
        let temp = tempfile::tempdir().unwrap();
        let mut store =
            Store::open_with_capacities(StorePaths::new(temp.path().to_path_buf()), capacities())
                .unwrap();
        let batch = BatchSpec {
            spec_version: SPEC_VERSION,
            jobs: vec![
                member(
                    "c",
                    spec(temp.path()),
                    vec![DependencySpec {
                        job: "b".into(),
                        on: DependencyKind::Success,
                    }],
                ),
                member(
                    "b",
                    spec(temp.path()),
                    vec![DependencySpec {
                        job: "a".into(),
                        on: DependencyKind::Success,
                    }],
                ),
                member("a", spec(temp.path()), vec![]),
            ],
        };
        let hash = normalized_batch_payload_hash(&batch).unwrap();
        let receipt = store
            .submit_batch(Uuid::now_v7(), &hash, &batch)
            .unwrap()
            .receipt;
        let root = store.prepare_next_job().unwrap().unwrap();
        assert_eq!(root.job_id, receipt.jobs[2].receipt.job_id);
        store
            .mark_finished(&root, Some(1), JobOutcome::Failed, "process_failed")
            .unwrap();

        let progress = store.prepare_next_job_with_progress().unwrap();
        assert!(progress.job.is_none());
        assert!(
            progress.state_changed,
            "skip-only passes must notify waiters"
        );
        assert_eq!(
            store
                .status(receipt.jobs[1].receipt.job_id)
                .unwrap()
                .outcome,
            Some(JobOutcome::Skipped)
        );
        assert_eq!(
            store
                .status(receipt.jobs[0].receipt.job_id)
                .unwrap()
                .outcome,
            Some(JobOutcome::Skipped)
        );
    }

    #[test]
    fn increment_2a_a03_sqlite_failure_rolls_back_every_batch_member() {
        let temp = tempfile::tempdir().unwrap();
        let mut store =
            Store::open_with_capacities(StorePaths::new(temp.path().to_path_buf()), capacities())
                .unwrap();
        store
            .connection
            .execute_batch(
                "CREATE TRIGGER fail_second_batch_member
                 BEFORE INSERT ON jobs WHEN NEW.batch_member = 'second'
                 BEGIN SELECT RAISE(ABORT, 'forced batch fault'); END;",
            )
            .unwrap();
        let batch = BatchSpec {
            spec_version: SPEC_VERSION,
            jobs: vec![
                member("first", spec(temp.path()), vec![]),
                member("second", spec(temp.path()), vec![]),
            ],
        };
        let key = Uuid::now_v7();
        let hash = normalized_batch_payload_hash(&batch).unwrap();
        assert!(matches!(
            store.submit_batch(key, &hash, &batch),
            Err(StoreError::Sqlite(_))
        ));
        for table in ["batches", "jobs", "dependencies"] {
            let count: u64 = store
                .connection
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(count, 0, "{table} must roll back atomically");
        }
        let state: String = store
            .connection
            .query_row(
                "SELECT state FROM submissions WHERE idempotency_key = ?1",
                [key.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(state, "received");

        store
            .connection
            .execute_batch("DROP TRIGGER fail_second_batch_member")
            .unwrap();
        store.resume_received().unwrap();
        let recovered = store.recover_submission(key, &hash).unwrap();
        assert!(matches!(recovered, RecoveryResult::AcceptedBatch(_)));
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
        let mut ram = spec(temp.path());
        ram.resources.ram_mb = Some(8_000);
        let batch = BatchSpec {
            spec_version: SPEC_VERSION,
            jobs: vec![
                member("cpu", cpu, vec![]),
                member("blocked", blocked, vec![]),
                member("gpu", gpu, vec![]),
                member("ram", ram, vec![]),
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
        assert!(
            receipt.jobs[3].receipt.blockers.is_empty(),
            "a non-fitting earlier claim must not reserve only its RAM portion"
        );
        let cpu = store.prepare_next_job().unwrap().unwrap();
        assert_eq!(cpu.job_id, receipt.jobs[0].receipt.job_id);
        let gpu = store.prepare_next_job().unwrap().unwrap();
        assert_eq!(
            gpu.job_id, receipt.jobs[2].receipt.job_id,
            "a partially fitting CPU claim must not reserve RAM or block orthogonal GPU work"
        );
        let ram = store.prepare_next_job().unwrap().unwrap();
        assert_eq!(ram.job_id, receipt.jobs[3].receipt.job_id);
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
        let fence_spec = first.clone();
        let first_hash = normalized_payload_hash(&first).unwrap();
        let first = store
            .submit(Uuid::now_v7(), &first_hash, &first)
            .unwrap()
            .receipt;
        let second_hash = normalized_payload_hash(&fence_spec).unwrap();
        let second = store
            .submit(Uuid::now_v7(), &second_hash, &fence_spec)
            .unwrap()
            .receipt;
        std::fs::create_dir(&fenced).unwrap();
        let admitted = store.prepare_next_job().unwrap().unwrap();
        assert_eq!(admitted.job_id, first.job_id);
        let after_creation_hash = normalized_payload_hash(&fence_spec).unwrap();
        let after_creation = store
            .submit(Uuid::now_v7(), &after_creation_hash, &fence_spec)
            .unwrap()
            .receipt;
        let snapshot = store.status(second.job_id).unwrap();
        assert!(
            snapshot
                .blockers
                .iter()
                .any(|blocker| blocker.code == "path_fence_busy")
        );
        assert!(
            store
                .status(after_creation.job_id)
                .unwrap()
                .blockers
                .iter()
                .any(|blocker| blocker.code == "path_fence_busy"),
            "creating the leaf between acceptances must not evade the incumbent fence"
        );
    }

    #[test]
    fn increment_2a_a06_dependency_outside_fifo_prefix_is_unknown() {
        let temp = tempfile::tempdir().unwrap();
        let mut store =
            Store::open_with_capacities(StorePaths::new(temp.path().to_path_buf()), capacities())
                .unwrap();
        let mut short = spec(temp.path());
        short.expected_duration_seconds = Some(5);
        let mut long = spec(temp.path());
        long.expected_duration_seconds = Some(3_600);
        let batch = BatchSpec {
            spec_version: SPEC_VERSION,
            jobs: vec![
                member("short", short, vec![]),
                member(
                    "dependent",
                    spec(temp.path()),
                    vec![DependencySpec {
                        job: "long".into(),
                        on: DependencyKind::Success,
                    }],
                ),
                member("long", long, vec![]),
            ],
        };
        let hash = normalized_batch_payload_hash(&batch).unwrap();
        let receipt = store
            .submit_batch(Uuid::now_v7(), &hash, &batch)
            .unwrap()
            .receipt;
        assert_eq!(
            receipt.jobs[1].receipt.estimate.confidence,
            EstimateConfidence::Unknown
        );
        assert_eq!(receipt.jobs[1].receipt.estimate.start_in_millis, None);
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
    fn rejected_idempotency_decision_replays_as_rejected() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
        let key = Uuid::now_v7();
        let spec = spec(temp.path());
        let hash = normalized_payload_hash(&spec).unwrap();
        store
            .connection
            .execute(
                "INSERT INTO submissions(
                    id, scope, idempotency_key, payload_hash, state, spec_json, kind, created_ms
                 ) VALUES (?1, 'unmanaged', ?2, ?3, 'rejected', ?4, 'job', ?5)",
                params![
                    Uuid::now_v7().to_string(),
                    key.to_string(),
                    hash,
                    serde_json::to_string(&spec).unwrap(),
                    now_millis(),
                ],
            )
            .unwrap();

        assert!(matches!(
            store.submit(key, &normalized_payload_hash(&spec).unwrap(), &spec),
            Err(StoreError::Rejected(_))
        ));
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
    fn schema_epoch_mismatch_resets_database_and_preserves_other_files() {
        let temp = tempfile::tempdir().unwrap();
        let paths = StorePaths::new(temp.path().to_path_buf());
        let mut old_store = Store::open_with_capacities(paths, capacities()).unwrap();
        let old_uuid = old_store.store_uuid;
        let job_spec = spec(temp.path());
        let hash = normalized_payload_hash(&job_spec).unwrap();
        let accepted_key = Uuid::now_v7();
        let old_job_id = old_store
            .submit(accepted_key, &hash, &job_spec)
            .unwrap()
            .receipt
            .job_id;
        let received_key = Uuid::now_v7();
        old_store
            .connection
            .execute(
                "INSERT INTO submissions(
                    id, scope, idempotency_key, payload_hash, state, spec_json, kind, created_ms
                 ) VALUES (?1, 'unmanaged', ?2, ?3, 'received', ?4, 'job', ?5)",
                params![
                    Uuid::now_v7().to_string(),
                    received_key.to_string(),
                    hash,
                    serde_json::to_string(&job_spec).unwrap(),
                    now_millis(),
                ],
            )
            .unwrap();
        old_store
            .connection
            .execute_batch(
                "CREATE TABLE obsolete_rows(value TEXT NOT NULL);
                 INSERT INTO obsolete_rows(value) VALUES ('must not survive');
                 UPDATE meta SET value = 'obsolete-alpha-schema' WHERE key = 'schema_epoch';",
            )
            .unwrap();
        drop(old_store);

        let paths = StorePaths::new(temp.path().to_path_buf());
        let log_marker = paths.logs.join("orphaned.log");
        std::fs::write(&log_marker, b"preserve me").unwrap();
        std::fs::write(&paths.config, serde_json::to_vec(&capacities()).unwrap()).unwrap();
        let store = Store::open(paths).unwrap();
        assert_ne!(store.store_uuid, old_uuid);
        assert_eq!(std::fs::read(&log_marker).unwrap(), b"preserve me");
        assert_eq!(
            std::fs::read(&store.paths.config).unwrap(),
            serde_json::to_vec(&capacities()).unwrap()
        );
        let epoch: String = store
            .connection
            .query_row(
                "SELECT value FROM meta WHERE key = 'schema_epoch'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(epoch, STORE_SCHEMA_EPOCH);
        let obsolete_exists: bool = store
            .connection
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM sqlite_master
                    WHERE type = 'table' AND name = 'obsolete_rows'
                 )",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!obsolete_exists, "reset must not import old rows or tables");
        for table in ["jobs", "submissions"] {
            let count: u64 = store
                .connection
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(count, 0, "reset must not import old {table}");
        }
        assert!(store.pending_jobs().unwrap().is_empty());
        assert!(matches!(
            store.recover_submission(accepted_key, &hash).unwrap(),
            RecoveryResult::Unknown
        ));
        assert!(matches!(
            store.recover_submission(received_key, &hash).unwrap(),
            RecoveryResult::Unknown
        ));
        assert!(matches!(
            store.status(old_job_id),
            Err(StoreError::NotFound(message)) if message.contains("foreign durable ID")
        ));
    }

    #[test]
    fn damaged_schema_and_identity_each_reset_the_whole_database() {
        let temp = tempfile::tempdir().unwrap();
        let paths = StorePaths::new(temp.path().to_path_buf());
        let store = Store::open(paths).unwrap();
        let first_uuid = store.store_uuid;
        store.connection.execute("DROP TABLE batches", []).unwrap();
        drop(store);

        let store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
        assert_ne!(store.store_uuid, first_uuid);
        let second_uuid = store.store_uuid;
        store
            .connection
            .execute("DELETE FROM meta WHERE key = 'store_uuid'", [])
            .unwrap();
        drop(store);

        let store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
        assert_ne!(store.store_uuid, second_uuid);
    }

    #[test]
    fn corrupt_or_empty_database_is_replaced_with_current_schema() {
        let corrupt = tempfile::tempdir().unwrap();
        let corrupt_paths = StorePaths::new(corrupt.path().to_path_buf());
        corrupt_paths.ensure().unwrap();
        std::fs::write(&corrupt_paths.database, b"not a sqlite database").unwrap();
        let corrupt_store = Store::open(corrupt_paths).unwrap();
        assert!(schema_is_current(&corrupt_store.connection).unwrap());

        let temp = tempfile::tempdir().unwrap();
        let paths = StorePaths::new(temp.path().to_path_buf());
        paths.ensure().unwrap();
        File::create(&paths.database).unwrap();

        let store = Store::open(paths).unwrap();
        let stored: String = store
            .connection
            .query_row(
                "SELECT value FROM meta WHERE key = 'store_uuid'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(Uuid::parse_str(&stored).unwrap(), store.store_uuid);
    }

    #[test]
    fn corruption_discovered_during_recovery_resets_once() {
        use std::io::Write as _;

        let temp = tempfile::tempdir().unwrap();
        let paths = StorePaths::new(temp.path().to_path_buf());
        let mut store = Store::open(paths).unwrap();
        let old_uuid = store.store_uuid;
        let job_spec = spec(temp.path());
        let hash = normalized_payload_hash(&job_spec).unwrap();
        store.submit(Uuid::now_v7(), &hash, &job_spec).unwrap();
        drop(store);

        let paths = StorePaths::new(temp.path().to_path_buf());
        let connection = Connection::open(&paths.database).unwrap();
        let page_size: u64 = connection
            .query_row("PRAGMA page_size", [], |row| row.get(0))
            .unwrap();
        let jobs_root_page: u64 = connection
            .query_row(
                "SELECT rootpage FROM sqlite_master WHERE type = 'table' AND name = 'jobs'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        drop(connection);
        let mut database = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&paths.database)
            .unwrap();
        database
            .seek(SeekFrom::Start((jobs_root_page - 1) * page_size))
            .unwrap();
        database.write_all(&[0xff; 128]).unwrap();
        database.sync_all().unwrap();
        drop(database);

        let reopened = Store::open(paths).unwrap();
        assert_ne!(reopened.store_uuid, old_uuid);
        assert!(reopened.pending_jobs().unwrap().is_empty());
    }

    #[test]
    fn only_corruption_errors_authorize_destructive_reset() {
        fn sqlite_error(code: i32) -> StoreError {
            StoreError::Sqlite(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(code),
                None,
            ))
        }

        assert!(matches!(
            schema_probe_error(sqlite_error(rusqlite::ffi::SQLITE_BUSY)),
            Err(StoreError::Sqlite(_))
        ));
        assert!(matches!(
            schema_probe_error(sqlite_error(rusqlite::ffi::SQLITE_IOERR)),
            Err(StoreError::Sqlite(_))
        ));
        assert!(!schema_probe_error(sqlite_error(rusqlite::ffi::SQLITE_CORRUPT)).unwrap());
        assert!(!schema_probe_error(sqlite_error(rusqlite::ffi::SQLITE_NOTADB)).unwrap());
    }

    #[test]
    fn current_schema_reopens_without_changing_store_identity() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
        let store_uuid = store.store_uuid;
        drop(store);

        let reopened = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
        assert_eq!(reopened.store_uuid, store_uuid);
    }
}
