use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::identity::{StartupIdentity, probe_startup_identity};
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
const MAX_CANCEL_JOBS: usize = 16;
const SNAPSHOT_DIAGNOSTIC_BUDGET_BYTES: usize = 64 * 1024;
const MAX_STDIN_BYTES: u64 = 64 * 1024 * 1024;
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
    reconciliation_observations:
        std::collections::BTreeMap<ContainmentId, (i64, ReconciliationResult)>,
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
            reconciliation_observations: std::collections::BTreeMap::new(),
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

    pub(crate) fn stage_begin(
        &self,
        upload_id: Uuid,
        expected_sha256: &str,
        expected_length: u64,
    ) -> StoreResult<u64> {
        validate_input_ref(&StagedInputRef {
            sha256: expected_sha256.to_owned(),
            length: expected_length,
        })?;
        let metadata_path = self.upload_metadata_path(upload_id);
        let partial_path = self.upload_partial_path(upload_id);
        let expected = UploadMetadata {
            sha256: expected_sha256.to_owned(),
            length: expected_length,
        };
        let metadata_exists = metadata_path.try_exists()?;
        let partial_exists = partial_path.try_exists()?;
        if metadata_exists && partial_exists {
            let actual: UploadMetadata = serde_json::from_reader(File::open(&metadata_path)?)?;
            if actual.sha256 != expected.sha256 || actual.length != expected.length {
                return Err(StoreError::InvalidSpec(
                    "upload ID was reused with different stdin metadata".into(),
                ));
            }
            let offset = std::fs::metadata(&partial_path)?.len();
            if offset > expected_length {
                return Err(StoreError::InvalidState(
                    "partial stdin upload exceeds its declared length".into(),
                ));
            }
            return Ok(offset);
        }
        if metadata_exists || partial_exists {
            remove_file_allow_readonly(&metadata_path)?;
            remove_file_allow_readonly(&partial_path)?;
        }
        let mut metadata = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&metadata_path)?;
        let initialized = (|| -> StoreResult<()> {
            serde_json::to_writer(&mut metadata, &expected)?;
            metadata.write_all(b"\n")?;
            metadata.sync_all()?;
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&partial_path)?
                .sync_all()?;
            Ok(())
        })();
        if let Err(error) = initialized {
            drop(metadata);
            let _ = std::fs::remove_file(&partial_path);
            let _ = std::fs::remove_file(&metadata_path);
            return Err(error);
        }
        Ok(0)
    }

    pub(crate) fn stage_chunk(
        &self,
        upload_id: Uuid,
        offset: u64,
        bytes: &[u8],
    ) -> StoreResult<u64> {
        if bytes.is_empty() || bytes.len() > MAX_UPLOAD_CHUNK_BYTES {
            return Err(StoreError::InvalidSpec(format!(
                "stdin upload chunks must contain 1..={MAX_UPLOAD_CHUNK_BYTES} bytes"
            )));
        }
        let metadata: UploadMetadata =
            serde_json::from_reader(File::open(self.upload_metadata_path(upload_id))?)?;
        let partial_path = self.upload_partial_path(upload_id);
        let current = std::fs::metadata(&partial_path)?.len();
        if current != offset {
            return Err(StoreError::InvalidState(format!(
                "stdin upload offset mismatch: expected {current}, received {offset}"
            )));
        }
        let next = current.saturating_add(bytes.len() as u64);
        if next > metadata.length {
            return Err(StoreError::InvalidSpec(
                "stdin upload exceeds its declared length".into(),
            ));
        }
        let mut partial = OpenOptions::new().append(true).open(partial_path)?;
        partial.write_all(bytes)?;
        Ok(next)
    }

    pub(crate) fn stage_commit(&self, upload_id: Uuid) -> StoreResult<StagedInputRef> {
        let metadata_path = self.upload_metadata_path(upload_id);
        let metadata: UploadMetadata = serde_json::from_reader(File::open(&metadata_path)?)?;
        let input = StagedInputRef {
            sha256: metadata.sha256,
            length: metadata.length,
        };
        validate_input_ref(&input)?;
        let partial_path = self.upload_partial_path(upload_id);
        let blob_path = self.paths.blob_path(&input.sha256);
        if !partial_path.try_exists()? && blob_path.try_exists()? {
            verify_file(&blob_path, &input)?;
            set_file_readonly(&blob_path)?;
            std::fs::remove_file(metadata_path)?;
            return Ok(input);
        }
        verify_file(&partial_path, &input)?;
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(&partial_path)?
            .sync_all()?;
        if blob_path.try_exists()? {
            verify_file(&blob_path, &input)?;
            set_file_readonly(&blob_path)?;
            remove_file_allow_readonly(&partial_path)?;
        } else {
            std::fs::rename(&partial_path, &blob_path).map_err(|error| {
                StoreError::InvalidState(format!(
                    "cannot publish staged stdin {}: {error}",
                    blob_path.display()
                ))
            })?;
            if let Err(error) = set_file_readonly(&blob_path) {
                let _ = std::fs::rename(&blob_path, &partial_path);
                return Err(error);
            }
        }
        std::fs::remove_file(&metadata_path).map_err(|error| {
            StoreError::InvalidState(format!(
                "cannot finalize staged stdin metadata {}: {error}",
                metadata_path.display()
            ))
        })?;
        Ok(input)
    }

    fn upload_metadata_path(&self, upload_id: Uuid) -> PathBuf {
        self.paths.uploads.join(format!("{upload_id}.json"))
    }

    fn upload_partial_path(&self, upload_id: Uuid) -> PathBuf {
        self.paths.uploads.join(format!("{upload_id}.partial"))
    }

    fn collect_abandoned_staging(&self) -> StoreResult<()> {
        for entry in std::fs::read_dir(&self.paths.uploads)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                remove_file_allow_readonly(&entry.path())?;
            }
        }
        let mut referenced = std::collections::HashSet::new();
        let mut statement = self
            .connection
            .prepare("SELECT DISTINCT stdin_hash FROM jobs WHERE stdin_hash IS NOT NULL")?;
        for hash in statement.query_map([], |row| row.get::<_, String>(0))? {
            referenced.insert(hash?);
        }
        for entry in std::fs::read_dir(&self.paths.blobs)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            let hash = name.strip_suffix(".stdin").unwrap_or_default();
            if !referenced.contains(hash) {
                remove_file_allow_readonly(&entry.path())?;
            }
        }
        Ok(())
    }

    fn verify_staged_input(
        &self,
        spec: &JobSpec,
        stdin: Option<&StagedInputRef>,
    ) -> StoreResult<()> {
        validate_input_shape(spec, stdin)?;
        if let Some(stdin) = stdin {
            verify_file(&self.paths.blob_path(&stdin.sha256), stdin)?;
        }
        Ok(())
    }

    fn verify_staged_batch_inputs(
        &self,
        spec: &BatchSpec,
        stdins: &std::collections::BTreeMap<String, StagedInputRef>,
    ) -> StoreResult<()> {
        validate_batch_input_shape(spec, stdins)?;
        for stdin in stdins.values() {
            verify_file(&self.paths.blob_path(&stdin.sha256), stdin)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn submit(
        &mut self,
        idempotency_key: Uuid,
        claimed_payload_hash: &str,
        spec: &JobSpec,
    ) -> StoreResult<SubmitResult> {
        self.submit_with_stdin(idempotency_key, claimed_payload_hash, spec, None)
    }

    #[cfg(test)]
    pub(crate) fn submit_with_stdin(
        &mut self,
        idempotency_key: Uuid,
        claimed_payload_hash: &str,
        spec: &JobSpec,
        stdin: Option<&StagedInputRef>,
    ) -> StoreResult<SubmitResult> {
        self.submit_with_stdin_scoped(
            SubmissionScope::Unmanaged,
            idempotency_key,
            claimed_payload_hash,
            spec,
            stdin,
        )
    }

    #[cfg(test)]
    pub(crate) fn submit_with_stdin_scoped(
        &mut self,
        scope: SubmissionScope,
        idempotency_key: Uuid,
        claimed_payload_hash: &str,
        spec: &JobSpec,
        stdin: Option<&StagedInputRef>,
    ) -> StoreResult<SubmitResult> {
        self.submit_with_stdin_scoped_for_wait(
            scope,
            idempotency_key,
            claimed_payload_hash,
            spec,
            stdin,
            false,
        )
    }

    pub(crate) fn submit_with_stdin_scoped_for_wait(
        &mut self,
        scope: SubmissionScope,
        idempotency_key: Uuid,
        claimed_payload_hash: &str,
        spec: &JobSpec,
        stdin: Option<&StagedInputRef>,
        wait_for_completion: bool,
    ) -> StoreResult<SubmitResult> {
        spec.validate()
            .map_err(|error| StoreError::InvalidSpec(error.to_string()))?;
        validate_input_shape(spec, stdin)?;
        let payload_hash = normalized_payload_hash_with_input(spec, stdin)?;
        if claimed_payload_hash != payload_hash {
            return Err(StoreError::InvalidSpec(
                "payload hash does not match the normalized specification".into(),
            ));
        }
        let key = idempotency_key.to_string();
        let scope_key = scope.key();
        if let Some((
            submission_id,
            stored_hash,
            state,
            job_id,
            spec_json,
            stdin_json,
            kind,
            durable_wait,
            reject_code,
            reject_detail,
        )) = self
            .connection
            .query_row(
                "SELECT id, payload_hash, state, job_id, spec_json, stdin_json, kind, wait_intent,
                        reject_code, reject_detail
                 FROM submissions WHERE scope = ?1 AND idempotency_key = ?2",
                params![scope_key, key],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, bool>(7)?,
                        row.get::<_, Option<String>>(8)?,
                        row.get::<_, Option<String>>(9)?,
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
                let result = SubmitResult {
                    receipt: self.receipt(
                        SubmissionId::from_parts(self.store_uuid, Uuid::parse_str(&submission_id)?),
                        JobId::from_parts(self.store_uuid, Uuid::parse_str(&job_id)?),
                    )?,
                    should_schedule: false,
                };
                return Ok(result);
            }
            if state == "received" {
                let wait_for_completion = durable_wait || wait_for_completion;
                if wait_for_completion && !durable_wait {
                    self.connection.execute(
                        "UPDATE submissions SET wait_intent = 1 WHERE id = ?1 AND state = 'received'",
                        [&submission_id],
                    )?;
                }
                let durable_spec = serde_json::from_str(&spec_json)?;
                let durable_stdin = stdin_json
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()?;
                return self.accept_received(
                    SubmissionId::from_parts(self.store_uuid, Uuid::parse_str(&submission_id)?),
                    &durable_spec,
                    durable_stdin.as_ref(),
                    scope,
                    wait_for_completion,
                );
            }
            if state == "rejected" {
                return Err(retained_rejection(reject_code, reject_detail));
            }
            return Err(StoreError::InvalidState(format!(
                "terminal submission state {state} cannot be replaced"
            )));
        }

        self.verify_staged_input(spec, stdin)?;
        let submission_id = SubmissionId::new(self.store_uuid);
        let received = self.connection.transaction()?;
        validate_current_parent(&received, self.store_uuid, self.daemon_generation, scope)?;
        let parent = scope.parent();
        received.execute(
            "INSERT INTO submissions(
                id, scope, idempotency_key, payload_hash, state, spec_json, stdin_json, kind,
                parent_job_id, parent_attempt_id, parent_invocation_id, wait_intent, created_ms
             ) VALUES (?1, ?2, ?3, ?4, 'received', ?5, ?6, 'job', ?7, ?8, ?9, ?10, ?11)",
            params![
                submission_id.entity_uuid().to_string(),
                scope_key,
                key,
                payload_hash,
                serde_json::to_string(spec)?,
                stdin.map(serde_json::to_string).transpose()?,
                parent.map(|parent| parent.job_id.entity_uuid().to_string()),
                parent.map(|parent| parent.attempt_id.entity_uuid().to_string()),
                parent.map(|parent| parent.invocation_id.entity_uuid().to_string()),
                wait_for_completion,
                now_millis(),
            ],
        )?;
        received.commit()?;
        self.accept_received(submission_id, spec, stdin, scope, wait_for_completion)
    }

    #[cfg(test)]
    pub(crate) fn submit_batch(
        &mut self,
        idempotency_key: Uuid,
        claimed_payload_hash: &str,
        spec: &BatchSpec,
    ) -> StoreResult<BatchSubmitResult> {
        self.submit_batch_with_stdins(
            idempotency_key,
            claimed_payload_hash,
            spec,
            &Default::default(),
        )
    }

    #[cfg(test)]
    pub(crate) fn submit_batch_with_stdins(
        &mut self,
        idempotency_key: Uuid,
        claimed_payload_hash: &str,
        spec: &BatchSpec,
        stdins: &std::collections::BTreeMap<String, StagedInputRef>,
    ) -> StoreResult<BatchSubmitResult> {
        self.submit_batch_with_stdins_scoped(
            SubmissionScope::Unmanaged,
            idempotency_key,
            claimed_payload_hash,
            spec,
            stdins,
        )
    }

    #[cfg(test)]
    pub(crate) fn submit_batch_with_stdins_scoped(
        &mut self,
        scope: SubmissionScope,
        idempotency_key: Uuid,
        claimed_payload_hash: &str,
        spec: &BatchSpec,
        stdins: &std::collections::BTreeMap<String, StagedInputRef>,
    ) -> StoreResult<BatchSubmitResult> {
        self.submit_batch_with_stdins_scoped_for_wait(
            scope,
            idempotency_key,
            claimed_payload_hash,
            spec,
            stdins,
            false,
        )
    }

    pub(crate) fn submit_batch_with_stdins_scoped_for_wait(
        &mut self,
        scope: SubmissionScope,
        idempotency_key: Uuid,
        claimed_payload_hash: &str,
        spec: &BatchSpec,
        stdins: &std::collections::BTreeMap<String, StagedInputRef>,
        wait_for_completion: bool,
    ) -> StoreResult<BatchSubmitResult> {
        spec.validate()
            .map_err(|error| StoreError::InvalidSpec(error.to_string()))?;
        validate_batch_input_shape(spec, stdins)?;
        let payload_hash = normalized_batch_payload_hash_with_inputs(spec, stdins)?;
        if claimed_payload_hash != payload_hash {
            return Err(StoreError::InvalidSpec(
                "payload hash does not match the normalized specification".into(),
            ));
        }
        let key = idempotency_key.to_string();
        let scope_key = scope.key();
        if let Some((
            submission,
            stored_hash,
            state,
            batch,
            spec_json,
            stdin_json,
            kind,
            durable_wait,
            reject_code,
            reject_detail,
        )) = self
            .connection
            .query_row(
                "SELECT id, payload_hash, state, batch_id, spec_json, stdin_json, kind, wait_intent,
                        reject_code, reject_detail
                 FROM submissions WHERE scope = ?1 AND idempotency_key = ?2",
                params![scope_key, key],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, bool>(7)?,
                        row.get::<_, Option<String>>(8)?,
                        row.get::<_, Option<String>>(9)?,
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
                let result = BatchSubmitResult {
                    receipt: self.batch_receipt(
                        submission_id,
                        BatchId::from_parts(self.store_uuid, Uuid::parse_str(&batch_id)?),
                    )?,
                    should_schedule: false,
                };
                return Ok(result);
            }
            if state == "received" {
                let wait_for_completion = durable_wait || wait_for_completion;
                if wait_for_completion && !durable_wait {
                    self.connection.execute(
                        "UPDATE submissions SET wait_intent = 1 WHERE id = ?1 AND state = 'received'",
                        [&submission],
                    )?;
                }
                let durable: BatchSpec = serde_json::from_str(&spec_json)?;
                let durable_stdins = stdin_json
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()?
                    .unwrap_or_default();
                return self.accept_received_batch(
                    submission_id,
                    &durable,
                    &durable_stdins,
                    scope,
                    wait_for_completion,
                );
            }
            if state == "rejected" {
                return Err(retained_rejection(reject_code, reject_detail));
            }
            return Err(StoreError::InvalidState(format!(
                "terminal submission state {state} cannot be replaced"
            )));
        }

        self.verify_staged_batch_inputs(spec, stdins)?;
        let submission_id = SubmissionId::new(self.store_uuid);
        let received = self.connection.transaction()?;
        validate_current_parent(&received, self.store_uuid, self.daemon_generation, scope)?;
        let parent = scope.parent();
        received.execute(
            "INSERT INTO submissions(
                id, scope, idempotency_key, payload_hash, state, spec_json, stdin_json, kind,
                parent_job_id, parent_attempt_id, parent_invocation_id, wait_intent, created_ms
             ) VALUES (?1, ?2, ?3, ?4, 'received', ?5, ?6, 'batch', ?7, ?8, ?9, ?10, ?11)",
            params![
                submission_id.entity_uuid().to_string(),
                scope_key,
                key,
                payload_hash,
                serde_json::to_string(spec)?,
                serde_json::to_string(stdins)?,
                parent.map(|parent| parent.job_id.entity_uuid().to_string()),
                parent.map(|parent| parent.attempt_id.entity_uuid().to_string()),
                parent.map(|parent| parent.invocation_id.entity_uuid().to_string()),
                wait_for_completion,
                now_millis(),
            ],
        )?;
        received.commit()?;
        self.accept_received_batch(submission_id, spec, stdins, scope, wait_for_completion)
    }

    fn accept_received_batch(
        &mut self,
        submission_id: SubmissionId,
        spec: &BatchSpec,
        stdins: &std::collections::BTreeMap<String, StagedInputRef>,
        scope: SubmissionScope,
        wait_for_completion: bool,
    ) -> StoreResult<BatchSubmitResult> {
        if let Err(error) = self.verify_staged_batch_inputs(spec, stdins) {
            self.reject_received_with(submission_id, "rejected", &error.to_string())?;
            return Err(StoreError::Rejected(error.to_string()));
        }
        let batch_id = BatchId::new(self.store_uuid);
        let accepted_ms = now_millis();
        let jobs: StoreResult<Vec<_>> = spec
            .jobs
            .iter()
            .map(|member| {
                let mut accepted_spec = member.spec.clone();
                accepted_spec.environment =
                    crate::spec::expand_environment(&member.spec.environment, &self.profiles)
                        .map_err(|error| StoreError::InvalidSpec(error.to_string()))?;
                Ok((
                    JobId::new(self.store_uuid),
                    ResolvedClaims::resolve(&member.spec.resources)
                        .map_err(|error| StoreError::InvalidSpec(error.to_string()))?,
                    accepted_spec,
                    stdins.get(&member.name).cloned(),
                ))
            })
            .collect();
        let jobs = match jobs {
            Ok(jobs) => jobs,
            Err(error) => {
                self.reject_received_for_error(submission_id, &error)?;
                return Err(StoreError::Rejected(error.to_string()));
            }
        };
        let names: std::collections::HashMap<_, _> = spec
            .jobs
            .iter()
            .zip(&jobs)
            .map(|(member, (job_id, _, _, _))| (member.name.as_str(), *job_id))
            .collect();
        let store_uuid = self.store_uuid;
        let daemon_generation = self.daemon_generation;
        let capacities = self.capacities.clone();
        let impact_incompatibilities = self.impact_incompatibilities.clone();
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
        if let Err(error) =
            validate_current_parent(&transaction, self.store_uuid, self.daemon_generation, scope)
        {
            drop(transaction);
            self.reject_received_for_error(submission_id, &error)?;
            return Err(StoreError::Rejected(error.to_string()));
        }
        let parent = scope.parent();
        transaction.execute(
            "INSERT INTO batches(id, state, submission_id, accepted_ms)
             VALUES (?1, 'retained', ?2, ?3)",
            params![
                batch_id.entity_uuid().to_string(),
                submission_id.entity_uuid().to_string(),
                accepted_ms,
            ],
        )?;
        for (index, (member, (job_id, claims, accepted_spec, stdin))) in
            spec.jobs.iter().zip(&jobs).enumerate()
        {
            transaction.execute(
                "INSERT INTO jobs(
                    id, submission_id, batch_id, batch_member, batch_index, state,
                    spec_json, claims_json, stdin_hash, stdin_len,
                    parent_job_id, parent_attempt_id, parent_invocation_id, accepted_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5, 'pending', ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                params![
                    job_id.entity_uuid().to_string(),
                    submission_id.entity_uuid().to_string(),
                    batch_id.entity_uuid().to_string(),
                    member.name,
                    index as u64,
                    serde_json::to_string(accepted_spec)?,
                    serde_json::to_string(claims)?,
                    stdin.as_ref().map(|stdin| stdin.sha256.as_str()),
                    stdin.as_ref().map(|stdin| stdin.length),
                    parent.map(|parent| parent.job_id.entity_uuid().to_string()),
                    parent.map(|parent| parent.attempt_id.entity_uuid().to_string()),
                    parent.map(|parent| parent.invocation_id.entity_uuid().to_string()),
                    accepted_ms,
                ],
            )?;
        }
        for (member, (successor, _, _, _)) in spec.jobs.iter().zip(&jobs) {
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
        if wait_for_completion {
            let targets = jobs
                .iter()
                .map(|(job_id, _, _, _)| *job_id)
                .collect::<Vec<_>>();
            if let Err(error) = validate_managed_wait_targets(
                &transaction,
                store_uuid,
                daemon_generation,
                &capacities,
                &impact_incompatibilities,
                scope,
                &targets,
            ) {
                drop(transaction);
                self.reject_received_for_error(submission_id, &error)?;
                return Err(error);
            }
        }
        transaction.execute(
            "UPDATE submissions SET state = 'accepted', batch_id = ?2,
                daemon_generation = ?3 WHERE id = ?1",
            params![
                submission_id.entity_uuid().to_string(),
                batch_id.entity_uuid().to_string(),
                daemon_generation.to_string()
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
            daemon_generation: self.accepting_daemon_generation(submission_id)?,
        })
    }

    fn reject_received(&mut self, submission_id: SubmissionId) -> StoreResult<()> {
        self.reject_received_with(
            submission_id,
            "rejected",
            "the retained submission decision is rejected",
        )
    }

    fn reject_received_with(
        &mut self,
        submission_id: SubmissionId,
        code: &str,
        detail: &str,
    ) -> StoreResult<()> {
        self.connection.execute(
            "UPDATE submissions
             SET state = 'rejected', reject_code = ?2, reject_detail = ?3
             WHERE id = ?1 AND state = 'received'",
            params![submission_id.entity_uuid().to_string(), code, detail],
        )?;
        Ok(())
    }

    fn reject_received_for_error(
        &mut self,
        submission_id: SubmissionId,
        error: &StoreError,
    ) -> StoreResult<()> {
        let (code, detail) = rejection_decision(error);
        self.reject_received_with(submission_id, &code, &detail)
    }

    #[cfg(test)]
    pub(crate) fn recover_submission(
        &self,
        idempotency_key: Uuid,
        payload_hash: &str,
    ) -> StoreResult<RecoveryResult> {
        self.recover_submission_scoped(SubmissionScope::Unmanaged, idempotency_key, payload_hash)
    }

    pub(crate) fn recover_submission_scoped(
        &self,
        scope: SubmissionScope,
        idempotency_key: Uuid,
        payload_hash: &str,
    ) -> StoreResult<RecoveryResult> {
        let scope_key = scope.key();
        let row = self
            .connection
            .query_row(
                "SELECT id, payload_hash, state, job_id, batch_id, kind,
                        reject_code, reject_detail
                 FROM submissions WHERE scope = ?1 AND idempotency_key = ?2",
                params![scope_key, idempotency_key.to_string()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, Option<String>>(7)?,
                    ))
                },
            )
            .optional()?;
        let Some((
            submission_id,
            stored_hash,
            state,
            job_id,
            batch_id,
            kind,
            reject_code,
            reject_detail,
        )) = row
        else {
            return match scope {
                SubmissionScope::Unmanaged => Ok(RecoveryResult::Unknown),
                SubmissionScope::Managed(_) => {
                    match validate_current_parent(
                        &self.connection,
                        self.store_uuid,
                        self.daemon_generation,
                        scope,
                    ) {
                        Ok(()) => Ok(RecoveryResult::NotReceived),
                        Err(StoreError::Rejected(_)) => Ok(RecoveryResult::Unknown),
                        Err(error) => Err(error),
                    }
                }
            };
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
                code: reject_code.unwrap_or_else(|| "rejected".into()),
                detail: reject_detail.unwrap_or_else(|| "submission was rejected".into()),
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
        stdin: Option<&StagedInputRef>,
        scope: SubmissionScope,
        wait_for_completion: bool,
    ) -> StoreResult<SubmitResult> {
        if let Err(error) = self.verify_staged_input(spec, stdin) {
            self.reject_received_with(submission_id, "rejected", &error.to_string())?;
            return Err(StoreError::Rejected(error.to_string()));
        }
        let mut accepted_spec = spec.clone();
        accepted_spec.environment =
            match crate::spec::expand_environment(&spec.environment, &self.profiles) {
                Ok(environment) => environment,
                Err(error) => {
                    self.reject_received_with(submission_id, "rejected", &error.to_string())?;
                    return Err(StoreError::Rejected(error.to_string()));
                }
            };
        let job_id = JobId::new(self.store_uuid);
        let claims = match ResolvedClaims::resolve(&spec.resources) {
            Ok(claims) => claims,
            Err(error) => {
                self.reject_received_with(submission_id, "rejected", &error.to_string())?;
                return Err(StoreError::Rejected(error.to_string()));
            }
        };
        let accepted_ms = now_millis();
        let store_uuid = self.store_uuid;
        let daemon_generation = self.daemon_generation;
        let capacities = self.capacities.clone();
        let impact_incompatibilities = self.impact_incompatibilities.clone();
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
        if let Err(error) =
            validate_current_parent(&transaction, self.store_uuid, self.daemon_generation, scope)
        {
            drop(transaction);
            self.reject_received_for_error(submission_id, &error)?;
            return Err(StoreError::Rejected(error.to_string()));
        }
        let parent = scope.parent();
        transaction.execute(
            "INSERT INTO jobs(
                id, submission_id, state, spec_json, claims_json, stdin_hash, stdin_len,
                parent_job_id, parent_attempt_id, parent_invocation_id, accepted_ms
             ) VALUES (?1, ?2, 'pending', ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                job_id.entity_uuid().to_string(),
                submission_id.entity_uuid().to_string(),
                serde_json::to_string(&accepted_spec)?,
                serde_json::to_string(&claims)?,
                stdin.map(|stdin| stdin.sha256.as_str()),
                stdin.map(|stdin| stdin.length),
                parent.map(|parent| parent.job_id.entity_uuid().to_string()),
                parent.map(|parent| parent.attempt_id.entity_uuid().to_string()),
                parent.map(|parent| parent.invocation_id.entity_uuid().to_string()),
                accepted_ms,
            ],
        )?;
        if wait_for_completion {
            if let Err(error) = validate_managed_wait_targets(
                &transaction,
                store_uuid,
                daemon_generation,
                &capacities,
                &impact_incompatibilities,
                scope,
                &[job_id],
            ) {
                drop(transaction);
                self.reject_received_for_error(submission_id, &error)?;
                return Err(error);
            }
        }
        transaction.execute(
            "UPDATE submissions SET state = 'accepted', job_id = ?2,
                daemon_generation = ?3 WHERE id = ?1",
            params![
                submission_id.entity_uuid().to_string(),
                job_id.entity_uuid().to_string(),
                daemon_generation.to_string()
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
        let parent = self.parent_for_job(job_id)?;
        Ok(JobReceipt {
            submission_id,
            job_id,
            submission_state: SubmissionState::Accepted,
            job_state: parse_job_state(&state)?,
            blockers,
            queue_rank,
            estimate,
            parent,
            daemon_generation: self.accepting_daemon_generation(submission_id)?,
        })
    }

    fn accepting_daemon_generation(&self, submission_id: SubmissionId) -> StoreResult<Uuid> {
        let value: String = self.connection.query_row(
            "SELECT daemon_generation FROM submissions WHERE id = ?1 AND state = 'accepted'",
            [submission_id.entity_uuid().to_string()],
            |row| row.get(0),
        )?;
        Ok(Uuid::parse_str(&value)?)
    }

    fn parent_for_job(&self, job_id: JobId) -> StoreResult<Option<ManagedParent>> {
        let row = self.connection.query_row(
            "SELECT parent_job_id, parent_attempt_id, parent_invocation_id
             FROM jobs WHERE id = ?1",
            [self.local_id(job_id)?],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            },
        )?;
        managed_parent_from_columns(self.store_uuid, row)
    }

    fn blockers_for_job(&self, job_id: JobId) -> StoreResult<Vec<Blocker>> {
        let job_key = self.local_id(job_id)?;
        let mut blockers = self.dependency_blockers(&job_key)?.0;
        if !self.startup_identity.capable() {
            blockers.push(Blocker {
                code: "host_capability_unavailable".into(),
                detail: self.startup_identity.failures.join("; "),
            });
        }
        let retry_not_before: Option<i64> = self.connection.query_row(
            "SELECT retry_not_before_ms FROM jobs WHERE id = ?1",
            [&job_key],
            |row| row.get(0),
        )?;
        if retry_not_before.is_some_and(|instant| instant > now_millis()) {
            blockers.push(Blocker {
                code: "retry_backoff".into(),
                detail: format!("retry_not_before_unix_millis={}", retry_not_before.unwrap()),
            });
        }
        let claims: String = self.connection.query_row(
            "SELECT claims_json FROM jobs WHERE id = ?1",
            [&job_key],
            |row| row.get(0),
        )?;
        let claims: ResolvedClaims = serde_json::from_str(&claims)?;
        blockers.extend(claims.blockers(
            &self.capacities,
            &self.active_and_reserved_claims_before(&job_key)?,
            &self.impact_incompatibilities,
        ));
        Ok(blockers)
    }

    fn active_and_reserved_claims_before(&self, job_key: &str) -> StoreResult<Vec<ResolvedClaims>> {
        let mut granted = self.active_claims()?;
        let mut statement = self.connection.prepare(
            "SELECT id, claims_json, retry_not_before_ms FROM jobs
             WHERE state = 'pending' AND rowid < (SELECT rowid FROM jobs WHERE id = ?1)
             ORDER BY accepted_ms, rowid",
        )?;
        let rows = statement.query_map([job_key], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<i64>>(2)?,
            ))
        })?;
        for row in rows {
            let (candidate, claims, retry_not_before) = row?;
            if retry_not_before.is_some_and(|instant| instant > now_millis()) {
                continue;
            }
            let (dependencies, impossible) = self.dependency_blockers(&candidate)?;
            if impossible || !dependencies.is_empty() {
                continue;
            }
            let claims: ResolvedClaims = serde_json::from_str(&claims)?;
            if claims
                .blockers(&self.capacities, &granted, &self.impact_incompatibilities)
                .is_empty()
            {
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
        if !claims
            .blockers(&self.capacities, &retained, &self.impact_incompatibilities)
            .is_empty()
        {
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
        if !self.startup_identity.capable() {
            return Ok(PrepareJob::Blocked);
        }
        let capacities = self.capacities.clone();
        let impact_incompatibilities = self.impact_incompatibilities.clone();
        let attempt_id = AttemptId::new(self.store_uuid);
        let invocation_id = InvocationId::new(self.store_uuid);
        let containment_id = ContainmentId::new(self.store_uuid);
        let lease_id = Uuid::now_v7();
        let transaction = self.connection.transaction()?;
        let row = transaction
            .query_row(
                "SELECT spec_json, claims_json, stdin_hash, stdin_len
                 FROM jobs WHERE id = ?1 AND state = 'pending'
                   AND COALESCE(retry_not_before_ms, 0) <= ?2",
                params![job_key, now_millis()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<u64>>(3)?,
                    ))
                },
            )
            .optional()?;
        let Some((spec_json, claims_json, stdin_hash, stdin_len)) = row else {
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
        if !claims
            .blockers(&capacities, &active, &impact_incompatibilities)
            .is_empty()
        {
            transaction.rollback()?;
            return Ok(PrepareJob::Blocked);
        }
        let spec = serde_json::from_str(&spec_json)?;
        let stdin = match (stdin_hash, stdin_len) {
            (Some(sha256), Some(length)) => Some(StagedInputRef { sha256, length }),
            (None, None) => None,
            _ => {
                return Err(StoreError::InvalidState(
                    "job has a partial staged stdin reference".into(),
                ));
            }
        };
        validate_input_shape(&spec, stdin.as_ref())?;
        let log_directory = self.paths.logs.join(job_id.entity_uuid().to_string());
        std::fs::create_dir_all(&log_directory)?;
        let attempt_index: u32 = transaction.query_row(
            "SELECT COALESCE(MAX(attempt_index), 0) + 1 FROM attempts WHERE job_id = ?1",
            [job_id.entity_uuid().to_string()],
            |row| row.get(0),
        )?;
        transaction.execute(
            "UPDATE jobs SET state = 'active', attempt_id = ?2, invocation_id = ?3,
                containment_id = ?4, stdout_len = 0, stderr_len = 0,
                retry_not_before_ms = NULL WHERE id = ?1 AND state = 'pending'",
            params![
                job_id.entity_uuid().to_string(),
                attempt_id.entity_uuid().to_string(),
                invocation_id.entity_uuid().to_string(),
                containment_id.entity_uuid().to_string(),
            ],
        )?;
        let attempt_started = now_millis();
        let attempt_deadline = spec.timeout_seconds.map(|seconds| {
            attempt_started
                .saturating_add(i64::try_from(seconds.saturating_mul(1000)).unwrap_or(i64::MAX))
        });
        transaction.execute(
            "INSERT INTO attempts(id, job_id, state, attempt_index, started_ms, deadline_ms)
             VALUES (?1, ?2, 'starting', ?3, ?4, ?5)",
            params![
                attempt_id.entity_uuid().to_string(),
                job_id.entity_uuid().to_string(),
                attempt_index,
                attempt_started,
                attempt_deadline,
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
            "INSERT INTO containments(
                id, invocation_id, state, host_id, boot_id, daemon_generation, strength, version
             ) VALUES (?1, ?2, 'creating', ?3, ?4, ?5, 'windows_job_object', 1)",
            params![
                containment_id.entity_uuid().to_string(),
                invocation_id.entity_uuid().to_string(),
                self.startup_identity.host_id.as_ref().map(|value| &value.0),
                self.startup_identity.boot_id.as_ref().map(|value| &value.0),
                self.daemon_generation.to_string(),
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
            stdin_path: stdin
                .as_ref()
                .map(|stdin| self.paths.blob_path(&stdin.sha256)),
            stdin,
            role: InvocationRole::Primary,
            attempt_deadline_unix_millis: attempt_deadline,
            host_id: self.startup_identity.host_id.clone(),
            boot_id: self.startup_identity.boot_id.clone(),
        })))
    }

    pub(crate) fn prepare_postcondition(
        &mut self,
        primary: &PreparedJob,
        index: usize,
    ) -> StoreResult<PreparedJob> {
        let postcondition =
            primary.spec.postconditions.get(index).ok_or_else(|| {
                StoreError::InvalidState("postcondition index out of range".into())
            })?;
        let invocation_id = InvocationId::new(self.store_uuid);
        let containment_id = ContainmentId::new(self.store_uuid);
        let transaction = self.connection.transaction()?;
        let current: (String, String, Option<i64>) = transaction.query_row(
            "SELECT jobs.state, jobs.attempt_id, attempts.deadline_ms
             FROM jobs JOIN attempts ON attempts.id = jobs.attempt_id WHERE jobs.id = ?1",
            [primary.job_id.entity_uuid().to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        if current.0 != "active" || current.1 != primary.attempt_id.entity_uuid().to_string() {
            return Err(StoreError::InvalidState(
                "postcondition requires the current active Attempt".into(),
            ));
        }
        transaction.execute(
            "INSERT INTO invocations(id, attempt_id, role, role_index, state)
             VALUES (?1, ?2, 'postcondition', ?3, 'prepared')",
            params![
                invocation_id.entity_uuid().to_string(),
                primary.attempt_id.entity_uuid().to_string(),
                u32::try_from(index + 1)
                    .map_err(|_| StoreError::InvalidState("too many postconditions".into()))?,
            ],
        )?;
        transaction.execute(
            "INSERT INTO containments(
                id, invocation_id, state, host_id, boot_id, daemon_generation, strength, version
             ) VALUES (?1, ?2, 'creating', ?3, ?4, ?5, 'windows_job_object', 1)",
            params![
                containment_id.entity_uuid().to_string(),
                invocation_id.entity_uuid().to_string(),
                self.startup_identity.host_id.as_ref().map(|value| &value.0),
                self.startup_identity.boot_id.as_ref().map(|value| &value.0),
                self.daemon_generation.to_string(),
            ],
        )?;
        transaction.execute(
            "UPDATE jobs SET invocation_id = ?2, containment_id = ?3 WHERE id = ?1",
            params![
                primary.job_id.entity_uuid().to_string(),
                invocation_id.entity_uuid().to_string(),
                containment_id.entity_uuid().to_string(),
            ],
        )?;
        transaction.commit()?;
        let mut spec = primary.spec.clone();
        spec.executable = postcondition.executable.clone();
        spec.args = postcondition.args.clone();
        if let Some(working_directory) = &postcondition.working_directory {
            spec.working_directory = working_directory.clone();
        }
        spec.stdin = StdinSpec::Eof;
        spec.postconditions.clear();
        spec.allow_child_submissions = false;
        let log_directory = self
            .paths
            .logs
            .join(primary.job_id.entity_uuid().to_string());
        Ok(PreparedJob {
            job_id: primary.job_id,
            attempt_id: primary.attempt_id,
            invocation_id,
            containment_id,
            spec,
            stdout_path: log_directory.join(format!("{invocation_id}.stdout")),
            stderr_path: log_directory.join(format!("{invocation_id}.stderr")),
            stdin: None,
            stdin_path: None,
            role: InvocationRole::Postcondition,
            attempt_deadline_unix_millis: current.2,
            host_id: self.startup_identity.host_id.clone(),
            boot_id: self.startup_identity.boot_id.clone(),
        })
    }

    #[cfg(test)]
    pub(crate) fn mark_started(
        &mut self,
        job: &PreparedJob,
        root_pid: u32,
        executable_hash: &str,
    ) -> StoreResult<()> {
        self.mark_started_with_identity(job, root_pid, executable_hash, None)
    }

    pub(crate) fn mark_started_with_identity(
        &mut self,
        job: &PreparedJob,
        root_pid: u32,
        executable_hash: &str,
        root_identity: Option<&ProcessIdentity>,
    ) -> StoreResult<()> {
        let (root_host_id, root_boot_id, root_creation_filetime_100ns) = match root_identity {
            Some(ProcessIdentity::Windows {
                host_id,
                boot_id,
                pid,
                creation_filetime_100ns,
            }) if *pid == root_pid => (
                Some(host_id.0.as_str()),
                Some(boot_id.0.as_str()),
                Some(i64::try_from(*creation_filetime_100ns).map_err(|_| {
                    StoreError::InvalidState(
                        "process creation identity exceeds SQLite range".into(),
                    )
                })?),
            ),
            Some(ProcessIdentity::Windows { pid, .. }) => {
                return Err(StoreError::InvalidState(format!(
                    "process identity PID {pid} does not match created PID {root_pid}"
                )));
            }
            Some(ProcessIdentity::Unknown { .. }) => {
                return Err(StoreError::InvalidState(
                    "unknown process identity cannot authorize native containment".into(),
                ));
            }
            None => (None, None, None),
        };
        let started = now_millis();
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
                executable_hash = ?3, started_ms = ?4, daemon_generation = ?5,
                root_host_id = ?6, root_boot_id = ?7,
                root_creation_filetime_100ns = ?8 WHERE id = ?1",
            params![
                job.invocation_id.entity_uuid().to_string(),
                root_pid,
                executable_hash,
                started,
                self.daemon_generation.to_string(),
                root_host_id,
                root_boot_id,
                root_creation_filetime_100ns,
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
            "UPDATE jobs SET started_ms = COALESCE(started_ms, ?2) WHERE id = ?1",
            params![job.job_id.entity_uuid().to_string(), started],
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
            "UPDATE invocations SET state = 'exited', root_exit_code = ?2
             WHERE id = ?1 AND state = 'started'",
            params![job.invocation_id.entity_uuid().to_string(), exit_code],
        )?;
        if job.role == InvocationRole::Primary {
            transaction.execute(
                "UPDATE jobs SET root_exit_code = ?2 WHERE id = ?1 AND state = 'active'",
                params![job.job_id.entity_uuid().to_string(), exit_code],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn mark_invocation_resolved(
        &mut self,
        job: &PreparedJob,
        exit_code: Option<i32>,
        classification: Option<ExitClassification>,
    ) -> StoreResult<()> {
        // Diagnostic observations must never prevent the authoritative lifecycle transition.
        // Preserve the read failure in-band while still resolving the Invocation and Lease.
        let stdout_tail = read_diagnostic_tail(&job.stdout_path)
            .unwrap_or_else(|error| format!("[stillyard stdout tail unavailable: {error}]"));
        let stderr_tail = read_diagnostic_tail(&job.stderr_path)
            .unwrap_or_else(|error| format!("[stillyard stderr tail unavailable: {error}]"));
        let transaction = self.connection.transaction()?;
        transaction.execute(
            "UPDATE invocations SET state = 'resolved', root_exit_code = COALESCE(?2, root_exit_code),
                exit_classification = ?3, finished_ms = ?4, stdout_tail = ?5, stderr_tail = ?6
             WHERE id = ?1",
            params![
                job.invocation_id.entity_uuid().to_string(),
                exit_code,
                classification.map(exit_classification_string),
                now_millis(),
                stdout_tail,
                stderr_tail,
            ],
        )?;
        transaction.execute(
            "UPDATE containments SET state = 'empty' WHERE id = ?1",
            [job.containment_id.entity_uuid().to_string()],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn settle_attempt(
        &mut self,
        job: &PreparedJob,
        verdict: AttemptVerdict,
    ) -> StoreResult<bool> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (state, spec_json, attempt_index, cancel_requested): (String, String, u32, bool) =
            transaction.query_row(
                "SELECT jobs.state, jobs.spec_json, attempts.attempt_index,
                        jobs.cancel_requested != 0
                 FROM jobs JOIN attempts ON attempts.id = jobs.attempt_id
                 WHERE jobs.id = ?1 AND attempts.id = ?2",
                params![
                    job.job_id.entity_uuid().to_string(),
                    job.attempt_id.entity_uuid().to_string(),
                ],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )?;
        if state != "active" {
            return Err(StoreError::InvalidState(format!(
                "job {} cannot settle Attempt from {state}",
                job.job_id
            )));
        }
        let spec: JobSpec = serde_json::from_str(&spec_json)?;
        let verdict_text = verdict.as_str();
        let retry = !cancel_requested
            && attempt_index < spec.retry.max_attempts
            && spec
                .retry
                .retryable
                .iter()
                .any(|value| value == verdict_text);
        transaction.execute(
            "UPDATE attempts SET state = 'settled', verdict = ?2, finished_ms = ?3 WHERE id = ?1",
            params![
                job.attempt_id.entity_uuid().to_string(),
                verdict_text,
                now_millis()
            ],
        )?;
        release_attempt_lease_if_safe(&transaction, &job.attempt_id.entity_uuid().to_string())?;
        if retry {
            let not_before = now_millis().saturating_add(
                i64::try_from(spec.retry.backoff_seconds.saturating_mul(1000)).unwrap_or(i64::MAX),
            );
            transaction.execute(
                "UPDATE jobs SET state = 'pending', attempt_id = NULL, invocation_id = NULL,
                    containment_id = NULL, root_exit_code = NULL,
                    retry_not_before_ms = ?2, cancel_requested = 0
                 WHERE id = ?1",
                params![job.job_id.entity_uuid().to_string(), not_before],
            )?;
        } else {
            let effective_verdict = if cancel_requested {
                AttemptVerdict::Canceled
            } else {
                verdict
            };
            if effective_verdict != verdict {
                transaction.execute(
                    "UPDATE attempts SET verdict = ?2 WHERE id = ?1",
                    params![
                        job.attempt_id.entity_uuid().to_string(),
                        effective_verdict.as_str()
                    ],
                )?;
            }
            transaction.execute(
                "UPDATE jobs SET state = 'final', outcome = ?2, finished_ms = ?3,
                    retry_not_before_ms = NULL WHERE id = ?1",
                params![
                    job.job_id.entity_uuid().to_string(),
                    outcome_string(outcome_for_verdict(effective_verdict)),
                    now_millis(),
                ],
            )?;
        }
        transaction.commit()?;
        Ok(retry)
    }

    pub(crate) fn cancel_requested(&self, job_id: JobId) -> StoreResult<bool> {
        self.connection
            .query_row(
                "SELECT cancel_requested != 0 FROM jobs WHERE id = ?1",
                [self.local_id(job_id)?],
                |row| row.get(0),
            )
            .map_err(Into::into)
    }

    pub(crate) fn cancel_jobs(&mut self, job_ids: &[JobId]) -> StoreResult<Vec<JobSnapshot>> {
        if job_ids.is_empty() {
            return Err(StoreError::InvalidSpec(
                "cancel requires at least one explicit Job ID".into(),
            ));
        }
        if job_ids.len() > MAX_CANCEL_JOBS {
            return Err(StoreError::InvalidSpec(format!(
                "cancel accepts at most {MAX_CANCEL_JOBS} Job IDs per request"
            )));
        }
        let local_ids = job_ids
            .iter()
            .map(|job_id| self.local_id(*job_id))
            .collect::<StoreResult<Vec<_>>>()?;
        let transaction = self.connection.transaction()?;
        for local_id in &local_ids {
            let state = transaction
                .query_row("SELECT state FROM jobs WHERE id = ?1", [local_id], |row| {
                    row.get::<_, String>(0)
                })
                .optional()?
                .ok_or_else(|| StoreError::NotFound(local_id.clone()))?;
            match state.as_str() {
                "pending" => {
                    transaction.execute(
                        "UPDATE jobs SET state = 'final', outcome = 'canceled',
                            cancel_requested = 1, retry_not_before_ms = NULL, finished_ms = ?2
                         WHERE id = ?1",
                        params![local_id, now_millis()],
                    )?;
                }
                "active" => {
                    transaction.execute(
                        "UPDATE jobs SET cancel_requested = 1 WHERE id = ?1",
                        [local_id],
                    )?;
                }
                "final" => {}
                other => {
                    return Err(StoreError::InvalidState(format!(
                        "cannot cancel Job in state {other}"
                    )));
                }
            }
        }
        transaction.commit()?;
        job_ids.iter().map(|job_id| self.status(*job_id)).collect()
    }

    pub(crate) fn mark_finished(
        &mut self,
        job: &PreparedJob,
        exit_code: Option<i32>,
        outcome: JobOutcome,
        verdict: &str,
    ) -> StoreResult<()> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
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
            "UPDATE attempts SET state = 'settled', verdict = ?2, finished_ms = ?3 WHERE id = ?1",
            params![
                job.attempt_id.entity_uuid().to_string(),
                verdict,
                now_millis()
            ],
        )?;
        release_attempt_lease_if_safe(&transaction, &job.attempt_id.entity_uuid().to_string())?;
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
        let incident_sequence: u64 = transaction.query_row(
            "SELECT COALESCE(MAX(incident_sequence), 0) + 1 FROM containments",
            [],
            |row| row.get(0),
        )?;
        let spec_json: String = transaction.query_row(
            "SELECT jobs.spec_json FROM jobs
             JOIN attempts ON attempts.job_id = jobs.id
             JOIN invocations ON invocations.attempt_id = attempts.id
             WHERE invocations.id = ?1",
            [job.invocation_id.entity_uuid().to_string()],
            |row| row.get(0),
        )?;
        let retained_claims_json =
            serde_json::to_string(&serde_json::from_str::<JobSpec>(&spec_json)?.resources)?;
        let opened = now_millis();
        transaction.execute(
            "UPDATE invocations SET state = 'resolved', root_exit_code = COALESCE(?2, root_exit_code),
                finished_ms = ?3 WHERE id = ?1 AND state IN ('prepared', 'started', 'exited')",
            params![
                job.invocation_id.entity_uuid().to_string(),
                exit_code,
                now_millis()
            ],
        )?;
        transaction.execute(
            "UPDATE containments SET state = 'uncertain', version = version + 1,
                incident_sequence = COALESCE(incident_sequence, ?2),
                reason_code = COALESCE(reason_code, ?3),
                detail = COALESCE(detail, ?4), opened_ms = COALESCE(opened_ms, ?5),
                retained_claims_json = COALESCE(retained_claims_json, ?6)
             WHERE id = ?1 AND state IN ('creating', 'live')",
            params![
                job.containment_id.entity_uuid().to_string(),
                incident_sequence,
                verdict,
                "cleanup could not be proven within the bounded runner wait",
                opened,
                retained_claims_json,
            ],
        )?;
        transaction.execute(
            "UPDATE attempts SET state = 'settled', verdict = ?2, finished_ms = ?3
             WHERE id = ?1 AND state != 'settled'",
            params![
                job.attempt_id.entity_uuid().to_string(),
                verdict,
                now_millis()
            ],
        )?;
        // An uncertain Containment deliberately keeps its Lease granted.
        transaction.execute(
            "UPDATE jobs SET state = 'final', outcome = 'interrupted',
                root_exit_code = COALESCE(?2, root_exit_code), finished_ms = ?3
             WHERE id = ?1 AND state = 'active'",
            params![
                job.job_id.entity_uuid().to_string(),
                (job.role == InvocationRole::Primary)
                    .then_some(exit_code)
                    .flatten(),
                now_millis()
            ],
        )?;
        transaction.commit()?;
        Ok(())
    }
}

mod admission;
mod database;
mod input;
mod observation;
mod reconciliation;
mod recovery;
mod values;

use admission::*;
use database::{
    bind_unbound_store, configure_database, create_current_schema, current_store_uuid,
    host_binding_is_acceptable, is_database_corruption, load_host_config, meta_value,
    reset_database_files, schema_is_current,
};
use input::{
    remove_file_allow_readonly, set_file_readonly, validate_batch_input_shape, validate_input_ref,
    validate_input_shape, verify_file,
};
use values::*;

pub(crate) use database::open_lock;
pub(crate) use input::{
    normalized_batch_payload_hash_with_inputs, normalized_payload_hash_with_input,
};

#[cfg(test)]
use database::schema_probe_error;
#[cfg(test)]
use input::make_file_writable;
#[cfg(test)]
pub(crate) use input::{normalized_batch_payload_hash, normalized_payload_hash};

#[cfg(test)]
mod tests;
