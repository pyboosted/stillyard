use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::identity::{StartupIdentity, probe_startup_identity};
use crate::payload::{MAX_CANCEL_JOBS, MAX_STDIN_BYTES};
use crate::protocol::StagedInputRef;
use crate::resources::ResolvedClaims;
use crate::{
    AttemptId, AttemptSnapshot, AttemptVerdict, BatchId, BatchJobReceipt, BatchReceipt, BatchSpec,
    Blocker, BootId, ClearContainmentResult, ClearanceOrigin, ContainmentId,
    ContainmentIncidentCursor, ContainmentIncidentSnapshot, ContainmentResolution,
    ContainmentResolutionAudit, ContainmentSnapshot, ContainmentState, DaemonSnapshot,
    DoctorBoundary, DoctorCheck, DoctorCheckStatus, DoctorHostSnapshot, DoctorIncidentPage,
    DoctorOverallStatus, DoctorSnapshot, DoctorStoreSnapshot, Estimate, EventCursor, EventGap,
    ExitClassification, ForcedClearanceAudit, HostConfig, HostId, InvocationId, InvocationRole,
    InvocationSnapshot, InvocationState, JobId, JobListCursor, JobListPage, JobOutcome, JobReceipt,
    JobSelector, JobSnapshot, JobSpec, JobState, JobSummary, LogChunk, LogStream,
    MAX_OBSERVATION_PAGE, ManagedParent, ObservationFrame, ProcessIdentity, ReconciliationResult,
    RecoveryResult, ResourceCapacities, SchedulerEvent, SchedulerEventKind, StdinSpec,
    SubmissionId, SubmissionState,
};

// Pre-stable Stillyard intentionally has no migration chain. Change this opaque epoch whenever
// the current schema changes; daemon startup will replace the whole SQLite database.
const STORE_SCHEMA_EPOCH: &str = "stillyard-operational-diagnostics-r1-2026-08-28";
const MAX_EVENT_ROWS: u64 = 16_384;
const SNAPSHOT_DIAGNOSTIC_BUDGET_BYTES: usize = 64 * 1024;
const MAX_UPLOAD_CHUNK_BYTES: usize = 256 * 1024;

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct UploadMetadata {
    sha256: String,
    length: u64,
}

#[derive(Clone)]
pub(crate) struct StorePaths {
    pub(crate) root: PathBuf,
    pub(crate) database: PathBuf,
    pub(crate) logs: PathBuf,
    pub(crate) uploads: PathBuf,
    pub(crate) blobs: PathBuf,
    pub(crate) lock: PathBuf,
    pub(crate) config: PathBuf,
}

impl StorePaths {
    pub(crate) fn new(root: PathBuf) -> Self {
        Self {
            database: root.join("stillyard.sqlite3"),
            logs: root.join("logs"),
            uploads: root.join("staging").join("uploads"),
            blobs: root.join("staging").join("blobs"),
            lock: root.join("daemon.lock"),
            config: root.join("config.json"),
            root,
        }
    }

    pub(crate) fn ensure(&self) -> StoreResult<()> {
        std::fs::create_dir_all(&self.root)?;
        crate::filesystem::require_fixed_local_ntfs(&self.root)?;
        std::fs::create_dir_all(&self.logs)?;
        std::fs::create_dir_all(&self.uploads)?;
        std::fs::create_dir_all(&self.blobs)?;
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

    pub(crate) fn blob_path(&self, hash: &str) -> PathBuf {
        self.blobs.join(format!("{hash}.stdin"))
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
    #[error("operation rejected ({code}): {detail}")]
    OperationRejected { code: String, detail: String },
    #[error("managed wait blocked by an ancestor: {0}")]
    BlockedByAncestor(String),
    #[error("managed wait rejected ({code}): {detail}")]
    ManagedWaitRejected { code: String, detail: String },
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SubmissionScope {
    Unmanaged,
    Managed(ManagedParent),
}

impl SubmissionScope {
    fn key(self) -> String {
        match self {
            Self::Unmanaged => "unmanaged".into(),
            Self::Managed(parent) => format!(
                "managed:{}:{}",
                parent.job_id.entity_uuid(),
                parent.attempt_id.entity_uuid()
            ),
        }
    }

    fn parent(self) -> Option<ManagedParent> {
        match self {
            Self::Unmanaged => None,
            Self::Managed(parent) => Some(parent),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ManagedCandidate {
    pub(crate) parent: ManagedParent,
    pub(crate) parent_job_id: Option<JobId>,
    pub(crate) submissions_enabled: bool,
    pub(crate) current: bool,
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
    pub(crate) stdin: Option<StagedInputRef>,
    pub(crate) stdin_path: Option<PathBuf>,
    pub(crate) role: InvocationRole,
    pub(crate) attempt_deadline_unix_millis: Option<i64>,
    pub(crate) host_id: Option<HostId>,
    pub(crate) boot_id: Option<BootId>,
}

#[derive(Clone, Debug)]
pub(crate) struct ReconciliationCandidate {
    pub(crate) containment_id: ContainmentId,
    pub(crate) invocation_id: InvocationId,
    pub(crate) attempt_id: AttemptId,
    pub(crate) version: u64,
    pub(crate) host_id: Option<HostId>,
    pub(crate) boot_id: Option<BootId>,
    pub(crate) daemon_generation: Option<Uuid>,
    pub(crate) root_pid_recorded: bool,
    pub(crate) root_identity: Option<ProcessIdentity>,
    pub(crate) prior_daemon_identity: Option<ProcessIdentity>,
    pub(crate) incident_sequence: u64,
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
    daemon_generation: Uuid,
    capacities: ResourceCapacities,
    profiles: std::collections::BTreeMap<String, crate::EnvironmentProfile>,
    impact_incompatibilities: std::collections::BTreeMap<String, Vec<String>>,
    config_sha256: String,
    startup_identity: StartupIdentity,
    bound_host_id: Option<HostId>,
}

impl Store {
    pub(crate) fn store_uuid(&self) -> Uuid {
        self.store_uuid
    }

    pub(crate) fn set_change_notifier(&mut self, notifier: std::sync::Arc<dyn Fn() + Send + Sync>) {
        // SQLite invokes update hooks before the surrounding transaction commits. This is only a
        // wake hint: every reader rechecks durable state, and the daemon's single Store mutex keeps
        // it from observing the connection until the writer commits and releases that mutex.
        self.connection.update_hook(Some(
            move |_action: rusqlite::hooks::Action, _database: &str, table: &str, _row_id: i64| {
                if table == "events" {
                    notifier();
                }
            },
        ));
    }

    pub(crate) fn managed_containment_candidates(&self) -> StoreResult<Vec<ManagedCandidate>> {
        let current_generation = self.daemon_generation.to_string();
        let mut statement = self.connection.prepare(
            "SELECT jobs.id, attempts.id, invocations.id, jobs.spec_json, jobs.parent_job_id,
                    jobs.state, jobs.attempt_id, jobs.invocation_id, attempts.state,
                    invocations.state, invocations.root_pid, invocations.root_exit_code,
                    invocations.daemon_generation, containments.state
             FROM invocations
             JOIN attempts ON attempts.id = invocations.attempt_id
             JOIN jobs ON jobs.id = attempts.job_id
             JOIN containments ON containments.invocation_id = invocations.id
             WHERE invocations.role = 'primary'
               AND invocations.daemon_generation = ?1
               AND containments.state = 'live'",
        )?;
        let rows = statement.query_map([&current_generation], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, Option<String>>(7)?,
                row.get::<_, String>(8)?,
                row.get::<_, String>(9)?,
                row.get::<_, Option<u32>>(10)?,
                row.get::<_, Option<i32>>(11)?,
                row.get::<_, Option<String>>(12)?,
                row.get::<_, String>(13)?,
            ))
        })?;
        let mut candidates = Vec::new();
        for row in rows {
            let (
                job,
                attempt,
                invocation,
                spec_json,
                parent_job,
                job_state,
                job_attempt,
                job_invocation,
                attempt_state,
                invocation_state,
                root_pid,
                root_exit_code,
                daemon_generation,
                containment_state,
            ) = row?;
            let spec: JobSpec = serde_json::from_str(&spec_json)?;
            let current = job_state == "active"
                && job_attempt.as_deref() == Some(attempt.as_str())
                && job_invocation.as_deref() == Some(invocation.as_str())
                && attempt_state == "running"
                && invocation_state == "started"
                && root_pid.is_some()
                && root_exit_code.is_none()
                && daemon_generation.as_deref() == Some(current_generation.as_str())
                && containment_state == "live";
            candidates.push(ManagedCandidate {
                parent: ManagedParent {
                    job_id: JobId::from_parts(self.store_uuid, Uuid::parse_str(&job)?),
                    attempt_id: AttemptId::from_parts(self.store_uuid, Uuid::parse_str(&attempt)?),
                    invocation_id: InvocationId::from_parts(
                        self.store_uuid,
                        Uuid::parse_str(&invocation)?,
                    ),
                },
                parent_job_id: parent_job
                    .map(|job| Uuid::parse_str(&job))
                    .transpose()?
                    .map(|job| JobId::from_parts(self.store_uuid, job)),
                submissions_enabled: spec.allow_child_submissions,
                current,
            });
        }
        Ok(candidates)
    }

    pub(crate) fn open(paths: StorePaths) -> StoreResult<Self> {
        let config = load_host_config(&paths.config)?;
        Self::open_with_config(paths, config, probe_startup_identity())
    }

    #[cfg(test)]
    pub(crate) fn open_with_capacities(
        paths: StorePaths,
        capacities: ResourceCapacities,
    ) -> StoreResult<Self> {
        Self::open_with_config(
            paths,
            HostConfig {
                resources: capacities,
                profiles: Default::default(),
                impact_incompatibilities: Default::default(),
            },
            probe_startup_identity(),
        )
    }

    fn open_with_config(
        paths: StorePaths,
        config: HostConfig,
        startup_identity: StartupIdentity,
    ) -> StoreResult<Self> {
        paths.ensure()?;
        let database_existed = paths.database.try_exists()?;
        if !database_existed {
            // A crash may have left sidecars without a main database file.
            reset_database_files(&paths)?;
            return Self::open_fresh(paths, config, startup_identity);
        }

        let connection = match Connection::open(&paths.database) {
            Ok(connection) => connection,
            Err(error) if is_database_corruption(&error) => {
                reset_database_files(&paths)?;
                return Self::open_fresh(paths, config, startup_identity);
            }
            Err(error) => return Err(error.into()),
        };
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        if !schema_is_current(&connection)? {
            drop(connection);
            reset_database_files(&paths)?;
            return Self::open_fresh(paths, config, startup_identity);
        }

        if !host_binding_is_acceptable(&connection, startup_identity.host_id.as_ref())? {
            drop(connection);
            reset_database_files(&paths)?;
            return Self::open_fresh(paths, config, startup_identity);
        }
        bind_unbound_store(&connection, startup_identity.host_id.as_ref())?;

        match Self::finish_open(
            connection,
            paths.clone(),
            config.clone(),
            startup_identity.clone(),
        ) {
            Ok(store) => Ok(store),
            Err(StoreError::Sqlite(error)) if is_database_corruption(&error) => {
                reset_database_files(&paths)?;
                Self::open_fresh(paths, config, startup_identity)
            }
            Err(error) => Err(error),
        }
    }

    fn open_fresh(
        paths: StorePaths,
        config: HostConfig,
        startup_identity: StartupIdentity,
    ) -> StoreResult<Self> {
        let connection = Connection::open(&paths.database)?;
        configure_database(&connection)?;
        create_current_schema(
            &connection,
            Uuid::now_v7(),
            startup_identity.host_id.as_ref(),
        )?;
        Self::finish_open(connection, paths, config, startup_identity)
    }

    fn finish_open(
        connection: Connection,
        paths: StorePaths,
        config: HostConfig,
        startup_identity: StartupIdentity,
    ) -> StoreResult<Self> {
        configure_database(&connection)?;
        let store_uuid = current_store_uuid(&connection)?;
        let config_sha256 = format!("{:x}", Sha256::digest(serde_json::to_vec(&config)?));
        let bound_host_id = meta_value(&connection, "bound_host_id")?.map(HostId);
        let daemon_generation = Uuid::now_v7();
        if let Some(process_identity) = &startup_identity.daemon_process {
            connection.execute(
                "INSERT INTO daemon_generations(generation, process_identity_json, started_ms)
                 VALUES (?1, ?2, ?3)",
                params![
                    daemon_generation.to_string(),
                    serde_json::to_string(process_identity)?,
                    now_millis(),
                ],
            )?;
        }
        let mut store = Self {
            connection,
            paths,
            store_uuid,
            daemon_generation,
            capacities: config.resources,
            profiles: config.profiles,
            impact_incompatibilities: config.impact_incompatibilities,
            config_sha256,
            startup_identity,
            bound_host_id,
        };
        store.recover_interrupted()?;
        store.resume_received()?;
        store.collect_abandoned_staging()?;
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

    fn local_containment_id(&self, id: ContainmentId) -> StoreResult<String> {
        if id.store_uuid() != self.store_uuid {
            return Err(StoreError::OperationRejected {
                code: "containment_foreign".into(),
                detail: format!("containment belongs to store {}", id.store_uuid()),
            });
        }
        Ok(id.entity_uuid().to_string())
    }
}

mod admission;
mod database;
mod input;
mod lifecycle;
mod observation;
mod reconciliation;
mod recovery;
mod schedule;
mod values;

use admission::*;
use database::{
    bind_unbound_store, configure_database, create_current_schema, current_store_uuid,
    host_binding_is_acceptable, is_database_corruption, load_host_config, meta_value,
    reset_database_files, schema_is_current,
};
use input::{validate_batch_input_shape, validate_input_shape};
use values::*;

#[cfg(windows)]
pub(crate) use database::open_lock;
pub(crate) use input::{
    normalized_batch_payload_hash_with_inputs, normalized_payload_hash_with_input,
};
pub(crate) use reconciliation::ReconciliationObservations;

#[cfg(test)]
use database::schema_probe_error;
#[cfg(test)]
use input::make_file_writable;
#[cfg(test)]
pub(crate) use input::{normalized_batch_payload_hash, normalized_payload_hash};

#[cfg(test)]
mod tests;
