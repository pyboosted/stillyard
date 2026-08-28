use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OptionalExtension, params};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::protocol::StagedInputRef;
use crate::resources::ResolvedClaims;
use crate::{
    AttemptId, AttemptSnapshot, AttemptVerdict, BatchId, BatchJobReceipt, BatchReceipt, BatchSpec,
    Blocker, ContainmentId, ContainmentSnapshot, ContainmentState, DaemonSnapshot, Estimate,
    EventCursor, EventGap, ExitClassification, HostConfig, InvocationId, InvocationRole,
    InvocationSnapshot, InvocationState, JobId, JobListCursor, JobListPage, JobOutcome, JobReceipt,
    JobSelector, JobSnapshot, JobSpec, JobState, JobSummary, LogChunk, LogStream,
    MAX_OBSERVATION_PAGE, ManagedParent, ObservationFrame, RecoveryResult, ResourceCapacities,
    SchedulerEvent, SchedulerEventKind, StdinSpec, SubmissionId, SubmissionState,
};

// Pre-stable Stillyard intentionally has no migration chain. Change this opaque epoch whenever
// the current schema changes; daemon startup will replace the whole SQLite database.
const STORE_SCHEMA_EPOCH: &str = "stillyard-alpha-live-observation-2026-08-28";
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
    change_notifier: Option<std::sync::Arc<dyn Fn() + Send + Sync>>,
}

impl Store {
    pub(crate) fn store_uuid(&self) -> Uuid {
        self.store_uuid
    }

    pub(crate) fn set_change_notifier(&mut self, notifier: std::sync::Arc<dyn Fn() + Send + Sync>) {
        let hook = std::sync::Arc::clone(&notifier);
        self.connection.update_hook(Some(
            move |_action: rusqlite::hooks::Action, _database: &str, table: &str, _row_id: i64| {
                if table == "events" {
                    hook();
                }
            },
        ));
        self.change_notifier = Some(notifier);
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
        Self::open_with_config(paths, config)
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
        )
    }

    fn open_with_config(paths: StorePaths, config: HostConfig) -> StoreResult<Self> {
        paths.ensure()?;
        let database_existed = paths.database.try_exists()?;
        if !database_existed {
            // A crash may have left sidecars without a main database file.
            reset_database_files(&paths)?;
            return Self::open_fresh(paths, config);
        }

        let connection = match Connection::open(&paths.database) {
            Ok(connection) => connection,
            Err(error) if is_database_corruption(&error) => {
                reset_database_files(&paths)?;
                return Self::open_fresh(paths, config);
            }
            Err(error) => return Err(error.into()),
        };
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        if !schema_is_current(&connection)? {
            drop(connection);
            reset_database_files(&paths)?;
            return Self::open_fresh(paths, config);
        }

        match Self::finish_open(connection, paths.clone(), config.clone()) {
            Ok(store) => Ok(store),
            Err(StoreError::Sqlite(error)) if is_database_corruption(&error) => {
                reset_database_files(&paths)?;
                Self::open_fresh(paths, config)
            }
            Err(error) => Err(error),
        }
    }

    fn open_fresh(paths: StorePaths, config: HostConfig) -> StoreResult<Self> {
        let connection = Connection::open(&paths.database)?;
        configure_database(&connection)?;
        create_current_schema(&connection, Uuid::now_v7())?;
        Self::finish_open(connection, paths, config)
    }

    fn finish_open(
        connection: Connection,
        paths: StorePaths,
        config: HostConfig,
    ) -> StoreResult<Self> {
        configure_database(&connection)?;
        let store_uuid = current_store_uuid(&connection)?;
        let config_sha256 = format!("{:x}", Sha256::digest(serde_json::to_vec(&config)?));
        let mut store = Self {
            connection,
            paths,
            store_uuid,
            daemon_generation: Uuid::now_v7(),
            capacities: config.resources,
            profiles: config.profiles,
            impact_incompatibilities: config.impact_incompatibilities,
            config_sha256,
            change_notifier: None,
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
            stdin_path: stdin
                .as_ref()
                .map(|stdin| self.paths.blob_path(&stdin.sha256)),
            stdin,
            role: InvocationRole::Primary,
            attempt_deadline_unix_millis: attempt_deadline,
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
            "INSERT INTO containments(id, invocation_id, state) VALUES (?1, ?2, 'creating')",
            params![
                containment_id.entity_uuid().to_string(),
                invocation_id.entity_uuid().to_string(),
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
        })
    }

    pub(crate) fn mark_started(
        &mut self,
        job: &PreparedJob,
        root_pid: u32,
        executable_hash: &str,
    ) -> StoreResult<()> {
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
                executable_hash = ?3, started_ms = ?4, daemon_generation = ?5 WHERE id = ?1",
            params![
                job.invocation_id.entity_uuid().to_string(),
                root_pid,
                executable_hash,
                started,
                self.daemon_generation.to_string(),
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
        let transaction = self.connection.transaction()?;
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
        transaction.execute(
            "UPDATE leases SET state = 'released' WHERE attempt_id = ?1",
            [job.attempt_id.entity_uuid().to_string()],
        )?;
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
            "UPDATE attempts SET state = 'settled', verdict = ?2, finished_ms = ?3 WHERE id = ?1",
            params![
                job.attempt_id.entity_uuid().to_string(),
                verdict,
                now_millis()
            ],
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
                finished_ms = ?3 WHERE id = ?1 AND state IN ('prepared', 'started', 'exited')",
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

    pub(crate) fn status(&self, job_id: JobId) -> StoreResult<JobSnapshot> {
        self.connection
            .query_row(
                "SELECT submission_id, state, outcome, attempt_id, invocation_id,
                    containment_id, root_exit_code, accepted_ms, started_ms, finished_ms,
                    spec_json, batch_id, batch_member,
                    parent_job_id, parent_attempt_id, parent_invocation_id,
                    cancel_requested != 0
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
                        row.get::<_, Option<String>>(13)?,
                        row.get::<_, Option<String>>(14)?,
                        row.get::<_, Option<String>>(15)?,
                        row.get::<_, bool>(16)?,
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
                    parent_job,
                    parent_attempt,
                    parent_invocation,
                    cancel_requested,
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
                        cancel_requested,
                        accepted_unix_millis: accepted_ms,
                        started_unix_millis: started_ms,
                        finished_unix_millis: finished_ms,
                        spec: serde_json::from_str(&spec_json)?,
                        parent: managed_parent_from_columns(
                            self.store_uuid,
                            (parent_job, parent_attempt, parent_invocation),
                        )?,
                        blockers: if parsed_state == JobState::Pending {
                            self.blockers_for_job(job_id)?
                        } else {
                            Vec::new()
                        },
                        attempts: self.attempt_snapshots(job_id)?,
                        daemon_generation: self.daemon_generation,
                    })
                },
            )
    }

    pub(crate) fn list_jobs(
        &self,
        selector: &JobSelector,
        cursor: Option<JobListCursor>,
        limit: u32,
    ) -> StoreResult<JobListPage> {
        self.validate_selector(selector)?;
        let limit = usize::try_from(limit.clamp(1, MAX_OBSERVATION_PAGE)).unwrap_or(1);
        if let Some(cursor) = cursor {
            if cursor.store_uuid != self.store_uuid || cursor.job_id.store_uuid() != self.store_uuid
            {
                return Err(StoreError::Rejected(
                    "list cursor belongs to a different store".into(),
                ));
            }
            let valid = self.connection.query_row(
                "SELECT EXISTS(SELECT 1 FROM jobs WHERE id = ?1 AND accepted_ms = ?2)",
                params![
                    cursor.job_id.entity_uuid().to_string(),
                    cursor.accepted_unix_millis
                ],
                |row| row.get::<_, bool>(0),
            )?;
            if !valid {
                return Err(StoreError::Rejected("invalid list cursor".into()));
            }
        }

        let mut scan = cursor;
        let mut selected = Vec::with_capacity(limit + 1);
        let mut exhausted = false;
        while selected.len() <= limit && !exhausted {
            let rows = self.scan_job_rows(scan, MAX_OBSERVATION_PAGE)?;
            exhausted = rows.len() < usize::try_from(MAX_OBSERVATION_PAGE).unwrap();
            if let Some(last) = rows.last() {
                scan = Some(JobListCursor {
                    store_uuid: self.store_uuid,
                    accepted_unix_millis: last.1,
                    job_id: last.0,
                });
            }
            for row in rows {
                if self.row_matches_selector(row.0, row.3.as_deref(), &row.4, selector)? {
                    selected.push(row);
                    if selected.len() > limit {
                        break;
                    }
                }
            }
        }
        let has_more = selected.len() > limit;
        if has_more {
            selected.pop();
        }
        let next_cursor = if has_more {
            let last = selected.last().expect("a positive page limit has one row");
            Some(JobListCursor {
                store_uuid: self.store_uuid,
                accepted_unix_millis: last.1,
                job_id: last.0,
            })
        } else {
            None
        };
        let jobs = selected
            .into_iter()
            .map(|(job_id, _, _, _, _)| self.job_summary(job_id))
            .collect::<StoreResult<Vec<_>>>()?;
        Ok(JobListPage {
            jobs,
            next_cursor,
            event_cursor: self.event_head()?,
        })
    }

    pub(crate) fn observe(
        &self,
        selector: &JobSelector,
        cursor: Option<EventCursor>,
        limit: u32,
    ) -> StoreResult<ObservationFrame> {
        self.validate_selector(selector)?;
        let head = self.event_head()?;
        let requested = cursor.unwrap_or(EventCursor {
            store_uuid: self.store_uuid,
            sequence: 0,
        });
        if requested.store_uuid != self.store_uuid {
            let snapshot = self.list_jobs(selector, None, MAX_OBSERVATION_PAGE)?;
            return Ok(ObservationFrame::Gap {
                gap: EventGap {
                    requested,
                    oldest_available: self.oldest_event_cursor(head.sequence)?,
                },
                snapshot,
                cursor: head,
            });
        }
        if requested.sequence > head.sequence {
            return Err(StoreError::Rejected(
                "event cursor is ahead of durable history".into(),
            ));
        }
        let oldest = self.oldest_event_cursor(head.sequence)?;
        if requested.sequence.saturating_add(1) < oldest.sequence {
            let snapshot = self.list_jobs(selector, None, MAX_OBSERVATION_PAGE)?;
            return Ok(ObservationFrame::Gap {
                gap: EventGap {
                    requested,
                    oldest_available: oldest,
                },
                snapshot,
                cursor: head,
            });
        }

        let wanted = usize::try_from(limit.clamp(1, MAX_OBSERVATION_PAGE)).unwrap_or(1);
        let mut events = Vec::with_capacity(wanted);
        let mut scanned = requested.sequence;
        while events.len() < wanted && scanned < head.sequence {
            let mut statement = self.connection.prepare(
                "SELECT sequence, kind, job_id, batch_id, committed_ms FROM events
                 WHERE sequence > ?1 ORDER BY sequence LIMIT ?2",
            )?;
            let rows = statement
                .query_map(params![scanned, MAX_OBSERVATION_PAGE], |row| {
                    Ok((
                        row.get::<_, u64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, i64>(4)?,
                    ))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            if rows.is_empty() {
                scanned = head.sequence;
                break;
            }
            for (sequence, kind, job, batch, committed) in rows {
                scanned = sequence;
                let job_id = JobId::from_parts(self.store_uuid, Uuid::parse_str(&job)?);
                let spec_json: String = self.connection.query_row(
                    "SELECT spec_json FROM jobs WHERE id = ?1",
                    [&job],
                    |row| row.get(0),
                )?;
                if !self.row_matches_selector(job_id, batch.as_deref(), &spec_json, selector)? {
                    continue;
                }
                events.push(SchedulerEvent {
                    cursor: EventCursor {
                        store_uuid: self.store_uuid,
                        sequence,
                    },
                    kind: parse_scheduler_event_kind(&kind)?,
                    job_id,
                    batch_id: batch
                        .map(|value| {
                            Uuid::parse_str(&value)
                                .map(|uuid| BatchId::from_parts(self.store_uuid, uuid))
                        })
                        .transpose()?,
                    committed_unix_millis: committed,
                });
                if events.len() == wanted {
                    break;
                }
            }
        }
        Ok(ObservationFrame::Events {
            events,
            cursor: EventCursor {
                store_uuid: self.store_uuid,
                sequence: scanned,
            },
        })
    }

    fn event_head(&self) -> StoreResult<EventCursor> {
        let sequence = self.connection.query_row(
            "SELECT COALESCE(MAX(sequence), 0) FROM events",
            [],
            |row| row.get(0),
        )?;
        Ok(EventCursor {
            store_uuid: self.store_uuid,
            sequence,
        })
    }

    fn oldest_event_cursor(&self, head: u64) -> StoreResult<EventCursor> {
        let sequence = self.connection.query_row(
            "SELECT COALESCE(MIN(sequence), ?1 + 1) FROM events",
            [head],
            |row| row.get(0),
        )?;
        Ok(EventCursor {
            store_uuid: self.store_uuid,
            sequence,
        })
    }

    fn validate_selector(&self, selector: &JobSelector) -> StoreResult<()> {
        match selector {
            JobSelector::All => Ok(()),
            JobSelector::Jobs { job_ids } => {
                if job_ids.is_empty() || job_ids.len() > crate::MAX_WAIT_STREAM_JOBS {
                    return Err(StoreError::Rejected(
                        "explicit Job selector must contain 1..=1024 IDs".into(),
                    ));
                }
                let mut seen = std::collections::BTreeSet::new();
                for job_id in job_ids {
                    if job_id.store_uuid() != self.store_uuid || !seen.insert(*job_id) {
                        return Err(StoreError::Rejected(
                            "explicit Job selector contains a foreign or duplicate ID".into(),
                        ));
                    }
                    let exists = self.connection.query_row(
                        "SELECT EXISTS(SELECT 1 FROM jobs WHERE id = ?1)",
                        [job_id.entity_uuid().to_string()],
                        |row| row.get::<_, bool>(0),
                    )?;
                    if !exists {
                        return Err(StoreError::NotFound(job_id.to_string()));
                    }
                }
                Ok(())
            }
            JobSelector::Batch { batch_id } => {
                if batch_id.store_uuid() != self.store_uuid {
                    return Err(StoreError::Rejected(
                        "Batch selector belongs to a different store".into(),
                    ));
                }
                let exists = self.connection.query_row(
                    "SELECT EXISTS(SELECT 1 FROM batches WHERE id = ?1)",
                    [batch_id.entity_uuid().to_string()],
                    |row| row.get::<_, bool>(0),
                )?;
                if exists {
                    Ok(())
                } else {
                    Err(StoreError::NotFound(batch_id.to_string()))
                }
            }
            JobSelector::Labels { labels } => {
                if labels.is_empty() || labels.len() > 32 {
                    return Err(StoreError::Rejected(
                        "label selector must contain 1..=32 labels".into(),
                    ));
                }
                let mut seen = std::collections::BTreeSet::new();
                for label in labels {
                    if label.key.is_empty()
                        || label.value.is_empty()
                        || label.key.contains(['\0', '='])
                        || label.value.contains('\0')
                        || !seen.insert((label.key.as_str(), label.value.as_str()))
                    {
                        return Err(StoreError::Rejected(
                            "label selector contains an invalid or duplicate label".into(),
                        ));
                    }
                }
                Ok(())
            }
        }
    }

    fn row_matches_selector(
        &self,
        job_id: JobId,
        batch_id: Option<&str>,
        spec_json: &str,
        selector: &JobSelector,
    ) -> StoreResult<bool> {
        Ok(match selector {
            JobSelector::All => true,
            JobSelector::Jobs { job_ids } => job_ids.contains(&job_id),
            JobSelector::Batch { batch_id: selected } => batch_id
                .map(Uuid::parse_str)
                .transpose()?
                .is_some_and(|batch| batch == selected.entity_uuid()),
            JobSelector::Labels { labels } => {
                let spec: JobSpec = serde_json::from_str(spec_json)?;
                labels.iter().all(|label| spec.labels.contains(label))
            }
        })
    }

    #[allow(clippy::type_complexity)]
    fn scan_job_rows(
        &self,
        cursor: Option<JobListCursor>,
        limit: u32,
    ) -> StoreResult<Vec<(JobId, i64, String, Option<String>, String)>> {
        let sql = if cursor.is_some() {
            "SELECT id, accepted_ms, state, batch_id, spec_json FROM jobs
             WHERE accepted_ms < ?1 OR (accepted_ms = ?1 AND id < ?2)
             ORDER BY accepted_ms DESC, id DESC LIMIT ?3"
        } else {
            "SELECT id, accepted_ms, state, batch_id, spec_json FROM jobs
             ORDER BY accepted_ms DESC, id DESC LIMIT ?1"
        };
        let mut statement = self.connection.prepare(sql)?;
        let map = |row: &rusqlite::Row<'_>| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, String>(4)?,
            ))
        };
        let rows = if let Some(cursor) = cursor {
            statement
                .query_map(
                    params![
                        cursor.accepted_unix_millis,
                        cursor.job_id.entity_uuid().to_string(),
                        limit
                    ],
                    map,
                )?
                .collect::<std::result::Result<Vec<_>, _>>()?
        } else {
            statement
                .query_map([limit], map)?
                .collect::<std::result::Result<Vec<_>, _>>()?
        };
        rows.into_iter()
            .map(|(job, accepted, state, batch, spec)| {
                Ok((
                    JobId::from_parts(self.store_uuid, Uuid::parse_str(&job)?),
                    accepted,
                    state,
                    batch,
                    spec,
                ))
            })
            .collect()
    }

    fn job_summary(&self, job_id: JobId) -> StoreResult<JobSummary> {
        let (
            state,
            outcome,
            accepted,
            started,
            finished,
            spec_json,
            batch,
            batch_member,
            attempt,
            invocation,
            stdout,
            stderr,
        ) = self.connection.query_row(
            "SELECT state, outcome, accepted_ms, started_ms, finished_ms, spec_json,
                    batch_id, batch_member, attempt_id, invocation_id, stdout_len, stderr_len
             FROM jobs WHERE id = ?1",
            [self.local_id(job_id)?],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, Option<String>>(8)?,
                    row.get::<_, Option<String>>(9)?,
                    row.get::<_, u64>(10)?,
                    row.get::<_, u64>(11)?,
                ))
            },
        )?;
        let state = parse_job_state(&state)?;
        let blockers = if state == JobState::Pending {
            self.blockers_for_job(job_id)?
        } else {
            Vec::new()
        };
        let queue_rank = if state == JobState::Pending {
            Some(self.connection.query_row(
                "SELECT COUNT(*) FROM jobs WHERE state = 'pending' AND rowid <=
                    (SELECT rowid FROM jobs WHERE id = ?1)",
                [job_id.entity_uuid().to_string()],
                |row| row.get(0),
            )?)
        } else {
            None
        };
        let estimate = if state == JobState::Pending {
            self.estimate_for_job(job_id, &blockers)?
        } else {
            Estimate::unknown("Job is no longer pending")
        };
        let spec: JobSpec = serde_json::from_str(&spec_json)?;
        Ok(JobSummary {
            job_id,
            batch_id: batch
                .map(|value| {
                    Uuid::parse_str(&value).map(|uuid| BatchId::from_parts(self.store_uuid, uuid))
                })
                .transpose()?,
            batch_member,
            parent: self.parent_for_job(job_id)?,
            state,
            outcome: outcome.map(|value| parse_outcome(&value)).transpose()?,
            accepted_unix_millis: accepted,
            started_unix_millis: started,
            finished_unix_millis: finished,
            queue_rank,
            estimate,
            claims: spec.resources,
            blocker: blockers.into_iter().next(),
            attempt_id: attempt
                .map(|value| {
                    Uuid::parse_str(&value).map(|uuid| AttemptId::from_parts(self.store_uuid, uuid))
                })
                .transpose()?,
            invocation_id: invocation
                .map(|value| {
                    Uuid::parse_str(&value)
                        .map(|uuid| InvocationId::from_parts(self.store_uuid, uuid))
                })
                .transpose()?,
            stdout_committed: stdout,
            stderr_committed: stderr,
        })
    }

    fn attempt_snapshots(&self, job_id: JobId) -> StoreResult<Vec<AttemptSnapshot>> {
        let mut statement = self.connection.prepare(
            "SELECT attempts.id, attempts.attempt_index, attempts.verdict,
                    attempts.started_ms, attempts.deadline_ms, attempts.finished_ms,
                    invocations.id, invocations.role, invocations.role_index, invocations.state,
                    invocations.root_pid, invocations.root_exit_code,
                    invocations.exit_classification, invocations.executable_hash,
                    invocations.daemon_generation, invocations.started_ms,
                    invocations.finished_ms, invocations.stdout_tail, invocations.stderr_tail,
                    containments.id, containments.state
             FROM attempts
             LEFT JOIN invocations ON invocations.attempt_id = attempts.id
             LEFT JOIN containments ON containments.invocation_id = invocations.id
             WHERE attempts.job_id = ?1
             ORDER BY attempts.attempt_index, invocations.role_index, invocations.rowid",
        )?;
        let rows = statement.query_map([self.local_id(job_id)?], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, u32>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, Option<i64>>(4)?,
                row.get::<_, Option<i64>>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, Option<String>>(7)?,
                row.get::<_, Option<u32>>(8)?,
                row.get::<_, Option<String>>(9)?,
                row.get::<_, Option<u32>>(10)?,
                row.get::<_, Option<i32>>(11)?,
                row.get::<_, Option<String>>(12)?,
                row.get::<_, Option<String>>(13)?,
                row.get::<_, Option<String>>(14)?,
                row.get::<_, Option<i64>>(15)?,
                row.get::<_, Option<i64>>(16)?,
                row.get::<_, Option<String>>(17)?,
                row.get::<_, Option<String>>(18)?,
                row.get::<_, Option<String>>(19)?,
                row.get::<_, Option<String>>(20)?,
            ))
        })?;
        let mut attempts = Vec::<AttemptSnapshot>::new();
        for row in rows {
            let (
                attempt,
                attempt_index,
                verdict,
                attempt_started,
                attempt_deadline,
                attempt_finished,
                invocation,
                role,
                role_index,
                invocation_state,
                root_pid,
                root_exit_code,
                exit_classification,
                executable_hash,
                daemon_generation,
                started,
                finished,
                stdout_tail,
                stderr_tail,
                containment,
                containment_state,
            ) = row?;
            if attempts
                .last()
                .is_none_or(|current| current.attempt_id.entity_uuid().to_string() != attempt)
            {
                attempts.push(AttemptSnapshot {
                    attempt_id: AttemptId::from_parts(self.store_uuid, Uuid::parse_str(&attempt)?),
                    attempt_index,
                    verdict: verdict.as_deref().map(parse_attempt_verdict).transpose()?,
                    started_unix_millis: attempt_started,
                    deadline_unix_millis: attempt_deadline,
                    finished_unix_millis: attempt_finished,
                    invocations: Vec::new(),
                });
            }
            let (
                Some(invocation),
                Some(role),
                Some(role_index),
                Some(invocation_state),
                Some(containment),
                Some(containment_state),
            ) = (
                invocation,
                role,
                role_index,
                invocation_state,
                containment,
                containment_state,
            )
            else {
                continue;
            };
            let containment_id =
                ContainmentId::from_parts(self.store_uuid, Uuid::parse_str(&containment)?);
            let containment_state = parse_containment_state(&containment_state)?;
            attempts
                .last_mut()
                .expect("attempt inserted above")
                .invocations
                .push(InvocationSnapshot {
                    invocation_id: InvocationId::from_parts(
                        self.store_uuid,
                        Uuid::parse_str(&invocation)?,
                    ),
                    role: parse_invocation_role(&role)?,
                    role_index,
                    state: parse_invocation_state(&invocation_state)?,
                    root_pid,
                    root_exit_code,
                    exit_classification: exit_classification
                        .as_deref()
                        .map(parse_exit_classification)
                        .transpose()?,
                    executable_hash,
                    daemon_generation: daemon_generation
                        .map(|value| Uuid::parse_str(&value))
                        .transpose()?,
                    started_unix_millis: started,
                    finished_unix_millis: finished,
                    containment: ContainmentSnapshot {
                        containment_id,
                        state: containment_state,
                        strength: if cfg!(windows) {
                            "windows_job_object".into()
                        } else {
                            "unsupported".into()
                        },
                        incident_id: (containment_state == ContainmentState::Uncertain)
                            .then_some(containment_id),
                    },
                    stdout_tail: stdout_tail.unwrap_or_default(),
                    stderr_tail: stderr_tail.unwrap_or_default(),
                });
        }
        bound_snapshot_diagnostics(&mut attempts);
        Ok(attempts)
    }

    pub(crate) fn validate_managed_wait(
        &self,
        scope: SubmissionScope,
        targets: &[JobId],
    ) -> StoreResult<()> {
        validate_managed_wait_targets(
            &self.connection,
            self.store_uuid,
            self.daemon_generation,
            &self.capacities,
            &self.impact_incompatibilities,
            scope,
            targets,
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
            daemon_generation: self.daemon_generation,
            version: env!("CARGO_PKG_VERSION").to_owned(),
            pid: std::process::id(),
            store_path: self.paths.root.clone(),
            config_path: self.paths.config.clone(),
            capacities: self.capacities.clone(),
            profile_names: self.profiles.keys().cloned().collect(),
            config_sha256: self.config_sha256.clone(),
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

    pub(crate) fn next_retry_delay(
        &self,
        scheduling_pass_started: i64,
    ) -> StoreResult<Option<std::time::Duration>> {
        let now = now_millis();
        let next: Option<i64> = self.connection.query_row(
            "SELECT MIN(retry_not_before_ms) FROM jobs
             WHERE state = 'pending' AND retry_not_before_ms > ?1",
            [scheduling_pass_started],
            |row| row.get(0),
        )?;
        Ok(next.map(|instant| {
            std::time::Duration::from_millis(
                u64::try_from(instant.saturating_sub(now)).unwrap_or(0),
            )
        }))
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
            "UPDATE attempts SET state = 'settled', verdict = 'start_failed', finished_ms = ?1
             WHERE state = 'starting'",
            [finished],
        )?;
        transaction.execute(
            "UPDATE leases SET state = 'released' WHERE state = 'granted' AND attempt_id IN (
                SELECT id FROM attempts WHERE verdict = 'start_failed'
             )",
            [],
        )?;
        transaction.execute(
            "UPDATE invocations SET state = 'resolved', finished_ms = ?1
             WHERE state IN ('started', 'exited')",
            [finished],
        )?;
        transaction.execute(
            "UPDATE containments SET state = 'uncertain' WHERE state = 'live'",
            [],
        )?;
        transaction.execute(
            "UPDATE attempts SET state = 'settled', verdict = 'interrupted', finished_ms = ?1
             WHERE state != 'settled'",
            [finished],
        )?;
        transaction.execute(
            "UPDATE jobs SET state = 'final',
                outcome = CASE
                    WHEN attempt_id IN (
                        SELECT id FROM attempts WHERE verdict = 'start_failed'
                    ) THEN 'failed'
                    ELSE 'interrupted'
                END,
                finished_ms = ?1
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
        transaction.execute(
            "UPDATE leases SET state = 'released'
             WHERE state = 'granted' AND attempt_id NOT IN (
                SELECT DISTINCT invocations.attempt_id
                FROM invocations
                JOIN containments ON containments.invocation_id = invocations.id
                WHERE containments.state IN ('creating', 'live', 'uncertain')
             )",
            [],
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn resume_received(&mut self) -> StoreResult<()> {
        let received = {
            let mut statement = self.connection.prepare(
                "SELECT id, spec_json, stdin_json, kind,
                        parent_job_id, parent_attempt_id, parent_invocation_id, wait_intent
                 FROM submissions
                 WHERE state = 'received' ORDER BY created_ms",
            )?;
            let rows = statement.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, bool>(7)?,
                ))
            })?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        for (
            submission_id,
            spec_json,
            stdin_json,
            kind,
            parent_job,
            parent_attempt,
            parent_invocation,
            wait_for_completion,
        ) in received
        {
            let submission_id =
                SubmissionId::from_parts(self.store_uuid, Uuid::parse_str(&submission_id)?);
            let scope = managed_parent_from_columns(
                self.store_uuid,
                (parent_job, parent_attempt, parent_invocation),
            )?
            .map_or(SubmissionScope::Unmanaged, SubmissionScope::Managed);
            let result = if kind == "batch" {
                match (
                    serde_json::from_str(&spec_json),
                    stdin_json.as_deref().map(serde_json::from_str).transpose(),
                ) {
                    (Ok(spec), Ok(stdins)) => self
                        .accept_received_batch(
                            submission_id,
                            &spec,
                            &stdins.unwrap_or_default(),
                            scope,
                            wait_for_completion,
                        )
                        .map(|_| ()),
                    (Err(error), _) | (_, Err(error)) => {
                        self.reject_received(submission_id)?;
                        Err(StoreError::Rejected(format!(
                            "retained BatchSpec cannot be decoded: {error}"
                        )))
                    }
                }
            } else {
                match (
                    serde_json::from_str(&spec_json),
                    stdin_json.as_deref().map(serde_json::from_str).transpose(),
                ) {
                    (Ok(spec), Ok(stdin)) => self
                        .accept_received(
                            submission_id,
                            &spec,
                            stdin.as_ref(),
                            scope,
                            wait_for_completion,
                        )
                        .map(|_| ()),
                    (Err(error), _) | (_, Err(error)) => {
                        self.reject_received(submission_id)?;
                        Err(StoreError::Rejected(format!(
                            "retained JobSpec cannot be decoded: {error}"
                        )))
                    }
                }
            };
            match result {
                Ok(())
                | Err(
                    StoreError::InvalidSpec(_)
                    | StoreError::Rejected(_)
                    | StoreError::BlockedByAncestor(_)
                    | StoreError::ManagedWaitRejected { .. },
                ) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
}

fn rejection_decision(error: &StoreError) -> (String, String) {
    match error {
        StoreError::BlockedByAncestor(detail) => ("blocked_by_ancestor".into(), detail.clone()),
        StoreError::ManagedWaitRejected { code, detail } => (code.clone(), detail.clone()),
        _ => ("rejected".into(), error.to_string()),
    }
}

fn retained_rejection(code: Option<String>, detail: Option<String>) -> StoreError {
    let code = code.unwrap_or_else(|| "rejected".into());
    let detail = detail.unwrap_or_else(|| "the retained submission decision is rejected".into());
    match code.as_str() {
        "blocked_by_ancestor" => StoreError::BlockedByAncestor(detail),
        "resource_capacity" => StoreError::ManagedWaitRejected { code, detail },
        _ => StoreError::Rejected(detail),
    }
}

fn validate_current_parent(
    connection: &Connection,
    store_uuid: Uuid,
    daemon_generation: Uuid,
    scope: SubmissionScope,
) -> StoreResult<()> {
    let SubmissionScope::Managed(parent) = scope else {
        return Ok(());
    };
    if parent.job_id.store_uuid() != store_uuid
        || parent.attempt_id.store_uuid() != store_uuid
        || parent.invocation_id.store_uuid() != store_uuid
    {
        return Err(StoreError::Rejected(
            "managed parent belongs to a foreign store".into(),
        ));
    }
    let spec_json = connection
        .query_row(
            "SELECT jobs.spec_json
             FROM jobs
             JOIN attempts ON attempts.id = jobs.attempt_id
             JOIN invocations ON invocations.id = jobs.invocation_id
             JOIN containments ON containments.invocation_id = invocations.id
             WHERE jobs.id = ?1
               AND attempts.id = ?2
               AND invocations.id = ?3
               AND jobs.state = 'active'
               AND attempts.state = 'running'
               AND invocations.state = 'started'
               AND invocations.role = 'primary'
               AND invocations.root_pid IS NOT NULL
               AND invocations.root_exit_code IS NULL
               AND invocations.daemon_generation = ?4
               AND containments.state = 'live'",
            params![
                parent.job_id.entity_uuid().to_string(),
                parent.attempt_id.entity_uuid().to_string(),
                parent.invocation_id.entity_uuid().to_string(),
                daemon_generation.to_string(),
            ],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    let Some(spec_json) = spec_json else {
        return Err(StoreError::Rejected(
            "managed parent is no longer the live current primary Invocation".into(),
        ));
    };
    let spec: JobSpec = serde_json::from_str(&spec_json)?;
    if !spec.allow_child_submissions {
        return Err(StoreError::Rejected(
            "managed parent does not allow child submissions".into(),
        ));
    }
    Ok(())
}

fn validate_managed_wait_targets(
    connection: &Connection,
    store_uuid: Uuid,
    daemon_generation: Uuid,
    capacities: &ResourceCapacities,
    impact_incompatibilities: &std::collections::BTreeMap<String, Vec<String>>,
    scope: SubmissionScope,
    targets: &[JobId],
) -> StoreResult<()> {
    let SubmissionScope::Managed(parent) = scope else {
        return Ok(());
    };
    if targets.is_empty() {
        return Err(StoreError::Rejected(
            "managed wait requires at least one target".into(),
        ));
    }
    validate_current_parent(connection, store_uuid, daemon_generation, scope)?;
    let ancestor_claims = managed_ancestor_claims(connection, store_uuid, parent)?;
    let mut pending = std::collections::VecDeque::from_iter(targets.iter().copied());
    let mut visited = std::collections::HashSet::new();
    let mut waited_claims = Vec::new();
    while let Some(job_id) = pending.pop_front() {
        if job_id.store_uuid() != store_uuid {
            return Err(StoreError::Rejected(format!(
                "managed wait target {job_id} belongs to a foreign store"
            )));
        }
        let job_key = job_id.entity_uuid().to_string();
        if !visited.insert(job_key.clone()) {
            continue;
        }
        if job_id == parent.job_id {
            return Err(StoreError::BlockedByAncestor(
                "the dependency closure reaches the waiting Job itself".into(),
            ));
        }
        if !job_descends_from(connection, store_uuid, job_id, parent.job_id)? {
            return Err(StoreError::Rejected(format!(
                "managed wait target {job_id} is not an authenticated descendant of {}",
                parent.job_id
            )));
        }
        let (state, claims_json, display_name) = connection
            .query_row(
                "SELECT state, claims_json, COALESCE(batch_member, id) FROM jobs WHERE id = ?1",
                [&job_key],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(job_id.to_string()))?;
        if state == "final" {
            continue;
        }
        let claims: ResolvedClaims = serde_json::from_str(&claims_json)?;
        waited_claims.push((job_id, display_name, claims));
        let mut statement = connection.prepare(
            "SELECT dependencies.predecessor_id
             FROM dependencies
             JOIN jobs ON jobs.id = dependencies.predecessor_id
             WHERE dependencies.successor_id = ?1 AND jobs.state != 'final'
             ORDER BY jobs.accepted_ms, jobs.rowid",
        )?;
        let predecessors = statement
            .query_map([&job_key], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        for predecessor in predecessors {
            pending.push_back(JobId::from_parts(
                store_uuid,
                Uuid::parse_str(&predecessor)?,
            ));
        }
    }
    for (job_id, display_name, claims) in waited_claims {
        let blockers =
            claims.ancestor_blockers(capacities, &ancestor_claims, impact_incompatibilities);
        if !blockers.is_empty() {
            let detail = format!(
                "target {display_name} ({job_id}): {}",
                blockers
                    .iter()
                    .map(|blocker| blocker.detail.as_str())
                    .collect::<Vec<_>>()
                    .join("; ")
            );
            if blockers
                .iter()
                .any(|blocker| blocker.code == "resource_capacity")
            {
                return Err(StoreError::ManagedWaitRejected {
                    code: "resource_capacity".into(),
                    detail,
                });
            }
            return Err(StoreError::BlockedByAncestor(detail));
        }
    }
    Ok(())
}

fn job_descends_from(
    connection: &Connection,
    store_uuid: Uuid,
    job_id: JobId,
    ancestor_id: JobId,
) -> StoreResult<bool> {
    let mut current = job_id;
    let mut visited = std::collections::HashSet::new();
    loop {
        if !visited.insert(current.entity_uuid()) {
            return Err(StoreError::InvalidState(
                "managed parent graph contains a cycle".into(),
            ));
        }
        let columns = connection
            .query_row(
                "SELECT parent_job_id, parent_attempt_id, parent_invocation_id
                 FROM jobs WHERE id = ?1",
                [current.entity_uuid().to_string()],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(current.to_string()))?;
        let Some(parent) = managed_parent_from_columns(store_uuid, columns)? else {
            return Ok(false);
        };
        if parent.job_id == ancestor_id {
            return Ok(true);
        }
        current = parent.job_id;
    }
}

fn managed_ancestor_claims(
    connection: &Connection,
    store_uuid: Uuid,
    parent: ManagedParent,
) -> StoreResult<Vec<ResolvedClaims>> {
    let mut current = Some(parent);
    let mut visited = std::collections::HashSet::new();
    let mut claims = Vec::new();
    while let Some(ancestor) = current {
        if !visited.insert((
            ancestor.job_id.entity_uuid(),
            ancestor.attempt_id.entity_uuid(),
        )) {
            return Err(StoreError::InvalidState(
                "managed ancestor graph contains a cycle".into(),
            ));
        }
        let lease = connection
            .query_row(
                "SELECT leases.state, leases.claims_json
                 FROM leases
                 JOIN attempts ON attempts.id = leases.attempt_id
                 WHERE attempts.id = ?1 AND attempts.job_id = ?2",
                params![
                    ancestor.attempt_id.entity_uuid().to_string(),
                    ancestor.job_id.entity_uuid().to_string(),
                ],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?
            .ok_or_else(|| {
                StoreError::InvalidState(format!(
                    "managed ancestor {} has no Lease for Attempt {}",
                    ancestor.job_id, ancestor.attempt_id
                ))
            })?;
        match lease.0.as_str() {
            "granted" => claims.push(serde_json::from_str(&lease.1)?),
            "released" => {}
            other => {
                return Err(StoreError::InvalidState(format!(
                    "managed ancestor Lease has unknown state {other}"
                )));
            }
        }
        let columns = connection.query_row(
            "SELECT parent_job_id, parent_attempt_id, parent_invocation_id
             FROM jobs WHERE id = ?1",
            [ancestor.job_id.entity_uuid().to_string()],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            },
        )?;
        current = managed_parent_from_columns(store_uuid, columns)?;
    }
    Ok(claims)
}

fn managed_parent_from_columns(
    store_uuid: Uuid,
    columns: (Option<String>, Option<String>, Option<String>),
) -> StoreResult<Option<ManagedParent>> {
    match columns {
        (None, None, None) => Ok(None),
        (Some(job), Some(attempt), Some(invocation)) => Ok(Some(ManagedParent {
            job_id: JobId::from_parts(store_uuid, Uuid::parse_str(&job)?),
            attempt_id: AttemptId::from_parts(store_uuid, Uuid::parse_str(&attempt)?),
            invocation_id: InvocationId::from_parts(store_uuid, Uuid::parse_str(&invocation)?),
        })),
        _ => Err(StoreError::InvalidState(
            "managed parent columns are only partially populated".into(),
        )),
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

#[cfg(test)]
pub(crate) fn normalized_payload_hash(spec: &JobSpec) -> StoreResult<String> {
    normalized_payload_hash_with_input(spec, None)
}

pub(crate) fn normalized_payload_hash_with_input(
    spec: &JobSpec,
    stdin: Option<&StagedInputRef>,
) -> StoreResult<String> {
    let normalized = serde_json::to_vec(&(spec, stdin))?;
    Ok(format!("{:x}", Sha256::digest(normalized)))
}

#[cfg(test)]
pub(crate) fn normalized_batch_payload_hash(spec: &BatchSpec) -> StoreResult<String> {
    normalized_batch_payload_hash_with_inputs(spec, &Default::default())
}

pub(crate) fn normalized_batch_payload_hash_with_inputs(
    spec: &BatchSpec,
    stdins: &std::collections::BTreeMap<String, StagedInputRef>,
) -> StoreResult<String> {
    let normalized = serde_json::to_vec(&(spec, stdins))?;
    Ok(format!("{:x}", Sha256::digest(normalized)))
}

fn validate_input_ref(input: &StagedInputRef) -> StoreResult<()> {
    if input.length > MAX_STDIN_BYTES
        || input.sha256.len() != 64
        || !input
            .sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(StoreError::InvalidSpec(format!(
            "staged stdin must be at most {MAX_STDIN_BYTES} bytes with a lowercase SHA-256"
        )));
    }
    Ok(())
}

fn validate_input_shape(spec: &JobSpec, stdin: Option<&StagedInputRef>) -> StoreResult<()> {
    match (&spec.stdin, stdin) {
        (StdinSpec::Eof, None) => Ok(()),
        (StdinSpec::File { .. }, Some(stdin)) => validate_input_ref(stdin),
        (StdinSpec::Eof, Some(_)) => Err(StoreError::InvalidSpec(
            "EOF stdin must not carry a staged input".into(),
        )),
        (StdinSpec::File { .. }, None) => Err(StoreError::InvalidSpec(
            "file stdin requires one committed staged input".into(),
        )),
    }
}

fn validate_batch_input_shape(
    spec: &BatchSpec,
    stdins: &std::collections::BTreeMap<String, StagedInputRef>,
) -> StoreResult<()> {
    let expected: std::collections::BTreeSet<_> = spec
        .jobs
        .iter()
        .filter(|member| matches!(member.spec.stdin, StdinSpec::File { .. }))
        .map(|member| member.name.as_str())
        .collect();
    if expected.len() != stdins.len() || !stdins.keys().all(|name| expected.contains(name.as_str()))
    {
        return Err(StoreError::InvalidSpec(
            "Batch staged stdin mapping must exactly match file-stdin members".into(),
        ));
    }
    for stdin in stdins.values() {
        validate_input_ref(stdin)?;
    }
    Ok(())
}

fn verify_file(path: &Path, input: &StagedInputRef) -> StoreResult<()> {
    validate_input_ref(input)?;
    let mut file = File::open(path)?;
    if file.metadata()?.len() != input.length {
        return Err(StoreError::InvalidSpec(
            "staged stdin length does not match its reference".into(),
        ));
    }
    let mut hash = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hash.update(&buffer[..read]);
    }
    if format!("{:x}", hash.finalize()) != input.sha256 {
        return Err(StoreError::InvalidSpec(
            "staged stdin hash does not match its reference".into(),
        ));
    }
    Ok(())
}

fn remove_file_allow_readonly(path: &Path) -> StoreResult<()> {
    if !path.try_exists()? {
        return Ok(());
    }
    let mut permissions = std::fs::metadata(path)?.permissions();
    if permissions.readonly() {
        make_file_writable(path, &mut permissions)?;
    }
    std::fs::remove_file(path)?;
    Ok(())
}

#[cfg(windows)]
fn make_file_writable(path: &Path, permissions: &mut std::fs::Permissions) -> StoreResult<()> {
    permissions.set_readonly(false);
    std::fs::set_permissions(path, permissions.clone())?;
    Ok(())
}

#[cfg(unix)]
fn make_file_writable(path: &Path, permissions: &mut std::fs::Permissions) -> StoreResult<()> {
    use std::os::unix::fs::PermissionsExt;

    permissions.set_mode(permissions.mode() | 0o200);
    std::fs::set_permissions(path, permissions.clone())?;
    Ok(())
}

fn set_file_readonly(path: &Path) -> StoreResult<()> {
    let mut permissions = std::fs::metadata(path)?.permissions();
    if !permissions.readonly() {
        permissions.set_readonly(true);
        std::fs::set_permissions(path, permissions).map_err(|error| {
            StoreError::InvalidState(format!(
                "cannot make staged stdin immutable at {}: {error}",
                path.display()
            ))
        })?;
    }
    Ok(())
}

fn dependency_kind(kind: crate::DependencyKind) -> &'static str {
    match kind {
        crate::DependencyKind::Success => "success",
        crate::DependencyKind::Failure => "failure",
        crate::DependencyKind::Terminal => "terminal",
    }
}

fn load_host_config(path: &Path) -> StoreResult<HostConfig> {
    match File::open(path) {
        Ok(file) => {
            let config: HostConfig = serde_json::from_reader(file)?;
            config
                .validate()
                .map_err(|error| StoreError::InvalidSpec(error.to_string()))?;
            Ok(config)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(HostConfig::default()),
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
             stdin_json TEXT,
             job_id TEXT,
             kind TEXT NOT NULL DEFAULT 'job',
             batch_id TEXT,
             parent_job_id TEXT,
             parent_attempt_id TEXT,
             parent_invocation_id TEXT,
             wait_intent INTEGER NOT NULL DEFAULT 0,
             reject_code TEXT,
             reject_detail TEXT,
             daemon_generation TEXT,
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
             stdin_hash TEXT,
             stdin_len INTEGER,
             attempt_id TEXT,
             invocation_id TEXT,
             containment_id TEXT,
             root_exit_code INTEGER,
             accepted_ms INTEGER NOT NULL,
             started_ms INTEGER,
             finished_ms INTEGER,
             stdout_len INTEGER NOT NULL DEFAULT 0,
             stderr_len INTEGER NOT NULL DEFAULT 0,
             cancel_requested INTEGER NOT NULL DEFAULT 0,
             retry_not_before_ms INTEGER,
             parent_job_id TEXT,
             parent_attempt_id TEXT,
             parent_invocation_id TEXT
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
             verdict TEXT,
             started_ms INTEGER NOT NULL,
             deadline_ms INTEGER,
             finished_ms INTEGER,
             UNIQUE(job_id, attempt_index)
         );
         CREATE TABLE invocations(
             id TEXT PRIMARY KEY,
             attempt_id TEXT NOT NULL REFERENCES attempts(id),
             role TEXT NOT NULL,
             role_index INTEGER NOT NULL DEFAULT 0,
             state TEXT NOT NULL,
             root_pid INTEGER,
             root_exit_code INTEGER,
             executable_hash TEXT,
             daemon_generation TEXT,
             started_ms INTEGER,
             finished_ms INTEGER,
             exit_classification TEXT,
             stdout_tail TEXT NOT NULL DEFAULT '',
             stderr_tail TEXT NOT NULL DEFAULT '',
             UNIQUE(attempt_id, role_index)
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
         CREATE TABLE events(
             sequence INTEGER PRIMARY KEY AUTOINCREMENT,
             kind TEXT NOT NULL,
             job_id TEXT NOT NULL REFERENCES jobs(id),
             batch_id TEXT REFERENCES batches(id),
             committed_ms INTEGER NOT NULL
         );
         CREATE INDEX events_job_sequence ON events(job_id, sequence);
         CREATE TRIGGER events_prune AFTER INSERT ON events BEGIN
             DELETE FROM events WHERE sequence <= NEW.sequence - {MAX_EVENT_ROWS};
         END;
         CREATE TRIGGER jobs_event_insert AFTER INSERT ON jobs BEGIN
             INSERT INTO events(kind, job_id, batch_id, committed_ms)
             VALUES ('job_changed', NEW.id, NEW.batch_id,
                 CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER));
         END;
         CREATE TRIGGER jobs_event_update AFTER UPDATE ON jobs BEGIN
             INSERT INTO events(kind, job_id, batch_id, committed_ms)
             VALUES ('job_changed', NEW.id, NEW.batch_id,
                 CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER));
         END;
         CREATE TRIGGER logs_event_update AFTER UPDATE OF stdout_len, stderr_len ON jobs
         WHEN OLD.stdout_len != NEW.stdout_len OR OLD.stderr_len != NEW.stderr_len BEGIN
             INSERT INTO events(kind, job_id, batch_id, committed_ms)
             VALUES ('log_committed', NEW.id, NEW.batch_id,
                 CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER));
         END;
         CREATE TRIGGER attempts_event_insert AFTER INSERT ON attempts BEGIN
             INSERT INTO events(kind, job_id, batch_id, committed_ms)
             SELECT 'attempt_changed', NEW.job_id, jobs.batch_id,
                 CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)
             FROM jobs WHERE jobs.id = NEW.job_id;
         END;
         CREATE TRIGGER attempts_event_update AFTER UPDATE ON attempts BEGIN
             INSERT INTO events(kind, job_id, batch_id, committed_ms)
             SELECT 'attempt_changed', NEW.job_id, jobs.batch_id,
                 CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)
             FROM jobs WHERE jobs.id = NEW.job_id;
         END;
         CREATE TRIGGER invocations_event_insert AFTER INSERT ON invocations BEGIN
             INSERT INTO events(kind, job_id, batch_id, committed_ms)
             SELECT 'invocation_changed', attempts.job_id, jobs.batch_id,
                 CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)
             FROM attempts JOIN jobs ON jobs.id = attempts.job_id
             WHERE attempts.id = NEW.attempt_id;
         END;
         CREATE TRIGGER invocations_event_update AFTER UPDATE ON invocations BEGIN
             INSERT INTO events(kind, job_id, batch_id, committed_ms)
             SELECT 'invocation_changed', attempts.job_id, jobs.batch_id,
                 CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)
             FROM attempts JOIN jobs ON jobs.id = attempts.job_id
             WHERE attempts.id = NEW.attempt_id;
         END;
         CREATE TRIGGER containments_event_insert AFTER INSERT ON containments BEGIN
             INSERT INTO events(kind, job_id, batch_id, committed_ms)
             SELECT 'containment_changed', attempts.job_id, jobs.batch_id,
                 CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)
             FROM invocations
             JOIN attempts ON attempts.id = invocations.attempt_id
             JOIN jobs ON jobs.id = attempts.job_id
             WHERE invocations.id = NEW.invocation_id;
         END;
         CREATE TRIGGER containments_event_update AFTER UPDATE ON containments BEGIN
             INSERT INTO events(kind, job_id, batch_id, committed_ms)
             SELECT 'containment_changed', attempts.job_id, jobs.batch_id,
                 CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)
             FROM invocations
             JOIN attempts ON attempts.id = invocations.attempt_id
             JOIN jobs ON jobs.id = attempts.job_id
             WHERE invocations.id = NEW.invocation_id;
         END;
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
        "events",
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
        (
            "submissions",
            &[
                "kind",
                "batch_id",
                "stdin_json",
                "parent_job_id",
                "parent_attempt_id",
                "parent_invocation_id",
                "wait_intent",
                "reject_code",
                "reject_detail",
                "daemon_generation",
            ] as &[_],
        ),
        ("batches", &["submission_id", "accepted_ms"] as &[_]),
        (
            "jobs",
            &[
                "batch_id",
                "batch_member",
                "batch_index",
                "claims_json",
                "stdin_hash",
                "stdin_len",
                "cancel_requested",
                "retry_not_before_ms",
                "parent_job_id",
                "parent_attempt_id",
                "parent_invocation_id",
            ] as &[_],
        ),
        (
            "dependencies",
            &["predecessor_id", "successor_id", "kind"] as &[_],
        ),
        (
            "attempts",
            &["attempt_index", "started_ms", "deadline_ms", "finished_ms"] as &[_],
        ),
        (
            "invocations",
            &[
                "role_index",
                "daemon_generation",
                "exit_classification",
                "stdout_tail",
                "stderr_tail",
            ] as &[_],
        ),
        (
            "events",
            &["sequence", "kind", "job_id", "batch_id", "committed_ms"] as &[_],
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
    for trigger in [
        "events_prune",
        "jobs_event_insert",
        "jobs_event_update",
        "logs_event_update",
        "attempts_event_insert",
        "attempts_event_update",
        "invocations_event_insert",
        "invocations_event_update",
        "containments_event_insert",
        "containments_event_update",
    ] {
        let exists = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'trigger' AND name = ?1)",
            [trigger],
            |row| row.get::<_, bool>(0),
        )?;
        if !exists {
            return Err(StoreError::InvalidState(format!(
                "current schema is missing trigger {trigger}"
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

fn parse_attempt_verdict(verdict: &str) -> StoreResult<AttemptVerdict> {
    match verdict {
        "succeeded" => Ok(AttemptVerdict::Succeeded),
        "process_failed" => Ok(AttemptVerdict::ProcessFailed),
        "start_failed" => Ok(AttemptVerdict::StartFailed),
        "timed_out" => Ok(AttemptVerdict::TimedOut),
        "interrupted" => Ok(AttemptVerdict::Interrupted),
        "safety_failed" => Ok(AttemptVerdict::SafetyFailed),
        "postcondition_retryable" => Ok(AttemptVerdict::PostconditionRetryable),
        "postcondition_failed" => Ok(AttemptVerdict::PostconditionFailed),
        "canceled" => Ok(AttemptVerdict::Canceled),
        other => Err(StoreError::InvalidState(format!(
            "unknown Attempt verdict {other}"
        ))),
    }
}

fn parse_invocation_role(role: &str) -> StoreResult<InvocationRole> {
    match role {
        "primary" => Ok(InvocationRole::Primary),
        "postcondition" => Ok(InvocationRole::Postcondition),
        other => Err(StoreError::InvalidState(format!(
            "unknown Invocation role {other}"
        ))),
    }
}

fn parse_invocation_state(state: &str) -> StoreResult<InvocationState> {
    match state {
        "prepared" => Ok(InvocationState::Prepared),
        "started" => Ok(InvocationState::Started),
        "exited" => Ok(InvocationState::Exited),
        "resolved" => Ok(InvocationState::Resolved),
        other => Err(StoreError::InvalidState(format!(
            "unknown Invocation state {other}"
        ))),
    }
}

fn parse_containment_state(state: &str) -> StoreResult<ContainmentState> {
    match state {
        "creating" => Ok(ContainmentState::Creating),
        "live" => Ok(ContainmentState::Live),
        "empty" => Ok(ContainmentState::Empty),
        "uncertain" => Ok(ContainmentState::Uncertain),
        other => Err(StoreError::InvalidState(format!(
            "unknown Containment state {other}"
        ))),
    }
}

fn parse_exit_classification(value: &str) -> StoreResult<ExitClassification> {
    match value {
        "accepted" => Ok(ExitClassification::Accepted),
        "retryable" => Ok(ExitClassification::Retryable),
        "failed" => Ok(ExitClassification::Failed),
        other => Err(StoreError::InvalidState(format!(
            "unknown exit classification {other}"
        ))),
    }
}

fn parse_scheduler_event_kind(value: &str) -> StoreResult<SchedulerEventKind> {
    match value {
        "job_changed" => Ok(SchedulerEventKind::JobChanged),
        "log_committed" => Ok(SchedulerEventKind::LogCommitted),
        "attempt_changed" => Ok(SchedulerEventKind::AttemptChanged),
        "invocation_changed" => Ok(SchedulerEventKind::InvocationChanged),
        "containment_changed" => Ok(SchedulerEventKind::ContainmentChanged),
        other => Err(StoreError::InvalidState(format!(
            "unknown scheduler event kind {other}"
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

fn outcome_for_verdict(verdict: AttemptVerdict) -> JobOutcome {
    match verdict {
        AttemptVerdict::Succeeded => JobOutcome::Succeeded,
        AttemptVerdict::TimedOut => JobOutcome::TimedOut,
        AttemptVerdict::Interrupted => JobOutcome::Interrupted,
        AttemptVerdict::Canceled => JobOutcome::Canceled,
        AttemptVerdict::ProcessFailed
        | AttemptVerdict::StartFailed
        | AttemptVerdict::SafetyFailed
        | AttemptVerdict::PostconditionRetryable
        | AttemptVerdict::PostconditionFailed => JobOutcome::Failed,
    }
}

fn exit_classification_string(classification: ExitClassification) -> &'static str {
    match classification {
        ExitClassification::Accepted => "accepted",
        ExitClassification::Retryable => "retryable",
        ExitClassification::Failed => "failed",
    }
}

fn read_diagnostic_tail(path: &Path) -> StoreResult<String> {
    const LIMIT: u64 = 16 * 1024;
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(String::new()),
        Err(error) => return Err(error.into()),
    };
    let length = file.metadata()?.len();
    file.seek(SeekFrom::Start(length.saturating_sub(LIMIT)))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

fn bound_snapshot_diagnostics(attempts: &mut [AttemptSnapshot]) {
    let mut remaining = SNAPSHOT_DIAGNOSTIC_BUDGET_BYTES;
    for attempt in attempts.iter_mut().rev() {
        for invocation in attempt.invocations.iter_mut().rev() {
            keep_tail_within_budget(&mut invocation.stderr_tail, &mut remaining);
            keep_tail_within_budget(&mut invocation.stdout_tail, &mut remaining);
        }
    }
}

fn keep_tail_within_budget(value: &mut String, remaining: &mut usize) {
    if value.len() <= *remaining {
        *remaining -= value.len();
        return;
    }
    if *remaining == 0 {
        value.clear();
        return;
    }
    let mut start = value.len() - *remaining;
    while !value.is_char_boundary(start) {
        start += 1;
    }
    *value = value[start..].to_owned();
    *remaining = 0;
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
        BatchMember, DependencyKind, DependencySpec, EnvironmentProfile, EnvironmentSpec,
        EstimateConfidence, PostconditionSpec, ResourceClaims, RetryPolicy, SPEC_VERSION,
        StdinSpec,
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
            postconditions: Vec::new(),
            labels: Vec::new(),
            expected_duration_seconds: None,
            timeout_seconds: None,
            quiet: None,
            artifacts: Vec::new(),
            allow_child_submissions: false,
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

    fn stage_bytes(store: &Store, bytes: &[u8]) -> StagedInputRef {
        let input = StagedInputRef {
            sha256: format!("{:x}", Sha256::digest(bytes)),
            length: bytes.len() as u64,
        };
        let upload_id = Uuid::now_v7();
        assert_eq!(
            store
                .stage_begin(upload_id, &input.sha256, input.length)
                .unwrap(),
            0
        );
        let mut offset = 0_u64;
        for chunk in bytes.chunks(17_003) {
            offset = store.stage_chunk(upload_id, offset, chunk).unwrap();
        }
        assert_eq!(store.stage_commit(upload_id).unwrap(), input);
        input
    }

    #[test]
    fn increment_2b_staged_stdin_is_pre_received_immutable_and_idempotent() {
        let temp = tempfile::tempdir().unwrap();
        let paths = StorePaths::new(temp.path().to_path_buf());
        let mut store = Store::open_with_capacities(paths, capacities()).unwrap();
        let bytes = (0..90_000)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let upload_id = Uuid::now_v7();
        let input = StagedInputRef {
            sha256: format!("{:x}", Sha256::digest(&bytes)),
            length: bytes.len() as u64,
        };
        store
            .stage_begin(upload_id, &input.sha256, input.length)
            .unwrap();
        let midpoint = 41_000;
        assert_eq!(
            store.stage_chunk(upload_id, 0, &bytes[..midpoint]).unwrap(),
            midpoint as u64
        );
        let submissions: u64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM submissions", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            submissions, 0,
            "partial upload must not create a Submission"
        );
        store
            .stage_chunk(upload_id, midpoint as u64, &bytes[midpoint..])
            .unwrap();
        assert_eq!(store.stage_commit(upload_id).unwrap(), input);

        let source = temp.path().join("client-source.bin");
        let mut job = spec(temp.path());
        job.stdin = StdinSpec::File { path: source };
        let key = Uuid::now_v7();
        let hash = normalized_payload_hash_with_input(&job, Some(&input)).unwrap();
        let accepted = store
            .submit_with_stdin(key, &hash, &job, Some(&input))
            .unwrap();
        let prepared = store.prepare_job(accepted.receipt.job_id).unwrap().unwrap();
        assert_eq!(prepared.stdin, Some(input.clone()));
        assert_eq!(
            std::fs::read(prepared.stdin_path.as_ref().unwrap()).unwrap(),
            bytes
        );
        assert!(
            std::fs::metadata(prepared.stdin_path.unwrap())
                .unwrap()
                .permissions()
                .readonly(),
            "published stdin blob must be immutable to ordinary writers"
        );

        let changed = stage_bytes(&store, b"different immutable input");
        let changed_hash = normalized_payload_hash_with_input(&job, Some(&changed)).unwrap();
        assert!(matches!(
            store.submit_with_stdin(key, &changed_hash, &job, Some(&changed)),
            Err(StoreError::IdempotencyConflict)
        ));
    }

    #[test]
    fn increment_2b_corrupt_staged_input_rejects_before_received() {
        let temp = tempfile::tempdir().unwrap();
        let mut store =
            Store::open_with_capacities(StorePaths::new(temp.path().to_path_buf()), capacities())
                .unwrap();
        let input = stage_bytes(&store, b"trusted");
        let blob = store.paths.blob_path(&input.sha256);
        let mut permissions = std::fs::metadata(&blob).unwrap().permissions();
        make_file_writable(&blob, &mut permissions).unwrap();
        std::fs::write(&blob, b"altered").unwrap();
        let mut job = spec(temp.path());
        job.stdin = StdinSpec::File {
            path: temp.path().join("client-source.bin"),
        };
        let hash = normalized_payload_hash_with_input(&job, Some(&input)).unwrap();
        assert!(matches!(
            store.submit_with_stdin(Uuid::now_v7(), &hash, &job, Some(&input)),
            Err(StoreError::InvalidSpec(_))
        ));
        let submissions: u64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM submissions", [], |row| row.get(0))
            .unwrap();
        assert_eq!(submissions, 0);
    }

    #[test]
    fn increment_2b_restart_collects_partial_upload_without_submission() {
        let temp = tempfile::tempdir().unwrap();
        let paths = StorePaths::new(temp.path().to_path_buf());
        let key = Uuid::now_v7();
        {
            let store = Store::open_with_capacities(paths.clone(), capacities()).unwrap();
            let bytes = b"never committed";
            let hash = format!("{:x}", Sha256::digest(bytes));
            let upload_id = Uuid::now_v7();
            store
                .stage_begin(upload_id, &hash, bytes.len() as u64)
                .unwrap();
            store.stage_chunk(upload_id, 0, bytes).unwrap();
            assert!(matches!(
                store.recover_submission(key, &hash).unwrap(),
                RecoveryResult::Unknown
            ));
        }
        let store = Store::open_with_capacities(paths, capacities()).unwrap();
        assert_eq!(std::fs::read_dir(&store.paths.uploads).unwrap().count(), 0);
        assert_eq!(std::fs::read_dir(&store.paths.blobs).unwrap().count(), 0);
        let submissions: u64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM submissions", [], |row| row.get(0))
            .unwrap();
        assert_eq!(submissions, 0);
    }

    #[test]
    fn increment_2b_partial_batch_input_map_is_atomic() {
        let temp = tempfile::tempdir().unwrap();
        let mut store =
            Store::open_with_capacities(StorePaths::new(temp.path().to_path_buf()), capacities())
                .unwrap();
        let mut first = spec(temp.path());
        first.stdin = StdinSpec::File {
            path: temp.path().join("first.in"),
        };
        let mut second = spec(temp.path());
        second.stdin = StdinSpec::File {
            path: temp.path().join("second.in"),
        };
        let batch = BatchSpec {
            spec_version: SPEC_VERSION,
            jobs: vec![
                member("first", first, vec![]),
                member("second", second, vec![]),
            ],
        };
        let stdins = [("first".to_owned(), stage_bytes(&store, b"first"))].into();
        let hash = normalized_batch_payload_hash_with_inputs(&batch, &stdins).unwrap();
        assert!(matches!(
            store.submit_batch_with_stdins(Uuid::now_v7(), &hash, &batch, &stdins),
            Err(StoreError::InvalidSpec(_))
        ));
        for table in ["submissions", "batches", "jobs"] {
            let count: u64 = store
                .connection
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(count, 0, "partial Batch stdin must create no {table}");
        }
    }

    #[test]
    fn increment_2b_received_batch_revalidates_staged_inputs_before_acceptance() {
        let temp = tempfile::tempdir().unwrap();
        let mut store =
            Store::open_with_capacities(StorePaths::new(temp.path().to_path_buf()), capacities())
                .unwrap();
        let mut job = spec(temp.path());
        job.stdin = StdinSpec::File {
            path: temp.path().join("member.in"),
        };
        let batch = BatchSpec {
            spec_version: SPEC_VERSION,
            jobs: vec![member("member", job, vec![])],
        };
        let input = stage_bytes(&store, b"trusted");
        let stdins = [("member".to_owned(), input.clone())].into();
        let hash = normalized_batch_payload_hash_with_inputs(&batch, &stdins).unwrap();
        let key = Uuid::now_v7();
        let submission_id = SubmissionId::new(store.store_uuid);
        store
            .connection
            .execute(
                "INSERT INTO submissions(
                    id, scope, idempotency_key, payload_hash, state, spec_json, stdin_json,
                    kind, created_ms
                 ) VALUES (?1, 'unmanaged', ?2, ?3, 'received', ?4, ?5, 'batch', ?6)",
                params![
                    submission_id.entity_uuid().to_string(),
                    key.to_string(),
                    hash,
                    serde_json::to_string(&batch).unwrap(),
                    serde_json::to_string(&stdins).unwrap(),
                    now_millis(),
                ],
            )
            .unwrap();
        let blob = store.paths.blob_path(&input.sha256);
        let mut permissions = std::fs::metadata(&blob).unwrap().permissions();
        make_file_writable(&blob, &mut permissions).unwrap();
        std::fs::write(&blob, b"altered").unwrap();

        store.resume_received().unwrap();
        assert!(matches!(
            store.recover_submission(key, &hash).unwrap(),
            RecoveryResult::Rejected { .. }
        ));
        let jobs: u64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM jobs", [], |row| row.get(0))
            .unwrap();
        assert_eq!(jobs, 0, "corrupt staged Batch input must never create Jobs");
    }

    #[test]
    fn increment_2b_profile_expands_at_acceptance_and_enforces_precedence() {
        let temp = tempfile::tempdir().unwrap();
        let paths = StorePaths::new(temp.path().to_path_buf());
        let config = HostConfig {
            resources: capacities(),
            impact_incompatibilities: Default::default(),
            profiles: [(
                "codex".to_owned(),
                EnvironmentProfile {
                    set: [("PATH".to_owned(), r"C:\Tools".to_owned())].into(),
                    unset: vec!["ANTHROPIC_API_KEY".into()],
                    locked_set: [("CODEX_HOME".to_owned(), r"C:\Accounts\codex2".to_owned())]
                        .into(),
                    locked_unset: vec!["XAI_API_KEY".into()],
                },
            )]
            .into(),
        };
        std::fs::write(&paths.config, serde_json::to_vec(&config).unwrap()).unwrap();
        let mut store = Store::open(paths).unwrap();
        let mut job = spec(temp.path());
        job.environment.profile = Some("codex".into());
        job.environment
            .set
            .insert("ANTHROPIC_API_KEY".into(), "must-not-leak".into());
        job.environment.set.insert("ROUND".into(), "2".into());
        let hash = normalized_payload_hash(&job).unwrap();
        let accepted = store.submit(Uuid::now_v7(), &hash, &job).unwrap();
        let effective = store
            .status(accepted.receipt.job_id)
            .unwrap()
            .spec
            .environment;
        assert_eq!(effective.profile.as_deref(), Some("codex"));
        assert_eq!(effective.set.get("PATH").unwrap(), r"C:\Tools");
        assert_eq!(
            effective.set.get("CODEX_HOME").unwrap(),
            r"C:\Accounts\codex2"
        );
        assert_eq!(effective.set.get("ROUND").unwrap(), "2");
        assert!(!effective.set.contains_key("ANTHROPIC_API_KEY"));
        assert!(
            effective
                .unset
                .iter()
                .any(|name| name == "ANTHROPIC_API_KEY")
        );
        assert!(effective.unset.iter().any(|name| name == "XAI_API_KEY"));

        let mut override_locked = job;
        override_locked
            .environment
            .set
            .insert("CODEX_HOME".into(), "wrong".into());
        let hash = normalized_payload_hash(&override_locked).unwrap();
        assert!(matches!(
            store.submit(Uuid::now_v7(), &hash, &override_locked),
            Err(StoreError::Rejected(_))
        ));
        let jobs: u64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM jobs", [], |row| row.get(0))
            .unwrap();
        assert_eq!(jobs, 1, "locked override must never create a Job");
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
            store.mark_root_exited(&prepared, 0).unwrap();
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
        assert_eq!(
            store.status(job_id).unwrap().attempts[0].invocations[0].state,
            InvocationState::Resolved
        );
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
    fn restart_during_prepared_postcondition_interrupts_consistently_and_releases_lease() {
        let temp = tempfile::tempdir().unwrap();
        let paths = StorePaths::new(temp.path().to_path_buf());
        let job_id = {
            let mut store = Store::open_with_capacities(paths, capacities()).unwrap();
            let mut job_spec = spec(temp.path());
            job_spec.resources.cargo_slots = Some(1);
            job_spec.postconditions.push(PostconditionSpec {
                executable: temp.path().join("validate.exe"),
                args: Vec::new(),
                working_directory: None,
                accepted_exit_codes: vec![0],
                retryable_exit_codes: Vec::new(),
            });
            let hash = normalized_payload_hash(&job_spec).unwrap();
            let receipt = store
                .submit(Uuid::now_v7(), &hash, &job_spec)
                .unwrap()
                .receipt;
            let primary = store.prepare_job(receipt.job_id).unwrap().unwrap();
            store
                .mark_started(&primary, u32::MAX, "primary-hash")
                .unwrap();
            store.mark_root_exited(&primary, 0).unwrap();
            store
                .mark_invocation_resolved(&primary, Some(0), None)
                .unwrap();
            store.prepare_postcondition(&primary, 0).unwrap();
            receipt.job_id
        };

        let store =
            Store::open_with_capacities(StorePaths::new(temp.path().to_path_buf()), capacities())
                .unwrap();
        let snapshot = store.status(job_id).unwrap();
        assert_eq!(snapshot.outcome, Some(JobOutcome::Interrupted));
        assert_eq!(
            snapshot.attempts[0].verdict,
            Some(AttemptVerdict::Interrupted)
        );
        assert_eq!(snapshot.attempts[0].invocations.len(), 2);
        assert_eq!(
            snapshot.attempts[0].invocations[1].state,
            InvocationState::Resolved
        );
        assert_eq!(
            snapshot.attempts[0].invocations[1].containment.state,
            ContainmentState::Empty
        );
        let lease: String = store
            .connection
            .query_row(
                "SELECT leases.state FROM leases JOIN attempts ON attempts.id = leases.attempt_id
                 WHERE attempts.job_id = ?1",
                [job_id.entity_uuid().to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(lease, "released");
    }

    #[test]
    fn restart_after_resolved_postcondition_releases_empty_attempt_lease() {
        let temp = tempfile::tempdir().unwrap();
        let paths = StorePaths::new(temp.path().to_path_buf());
        let job_id = {
            let mut store = Store::open_with_capacities(paths, capacities()).unwrap();
            let mut job_spec = spec(temp.path());
            job_spec.resources.cargo_slots = Some(1);
            job_spec.postconditions.push(PostconditionSpec {
                executable: temp.path().join("validate.exe"),
                args: Vec::new(),
                working_directory: None,
                accepted_exit_codes: vec![0],
                retryable_exit_codes: Vec::new(),
            });
            let hash = normalized_payload_hash(&job_spec).unwrap();
            let receipt = store
                .submit(Uuid::now_v7(), &hash, &job_spec)
                .unwrap()
                .receipt;
            let primary = store.prepare_job(receipt.job_id).unwrap().unwrap();
            store
                .mark_started(&primary, u32::MAX, "primary-hash")
                .unwrap();
            store.mark_root_exited(&primary, 0).unwrap();
            store
                .mark_invocation_resolved(&primary, Some(0), None)
                .unwrap();
            let validator = store.prepare_postcondition(&primary, 0).unwrap();
            store
                .mark_started(&validator, u32::MAX, "validator-hash")
                .unwrap();
            store.mark_root_exited(&validator, 0).unwrap();
            store
                .mark_invocation_resolved(&validator, Some(0), Some(ExitClassification::Accepted))
                .unwrap();
            receipt.job_id
        };

        let store =
            Store::open_with_capacities(StorePaths::new(temp.path().to_path_buf()), capacities())
                .unwrap();
        let snapshot = store.status(job_id).unwrap();
        assert_eq!(snapshot.outcome, Some(JobOutcome::Interrupted));
        assert_eq!(
            snapshot.attempts[0].verdict,
            Some(AttemptVerdict::Interrupted)
        );
        let lease: String = store
            .connection
            .query_row(
                "SELECT leases.state FROM leases JOIN attempts ON attempts.id = leases.attempt_id
                 WHERE attempts.job_id = ?1",
                [job_id.entity_uuid().to_string()],
                |row| row.get(0),
            )
            .unwrap();
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
        assert_eq!(store.managed_containment_candidates().unwrap().len(), 1);
        store
            .mark_uncertain(&prepared, None, "interrupted")
            .unwrap();
        assert!(store.managed_containment_candidates().unwrap().is_empty());
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
    fn diagnostic_tail_io_failure_cannot_block_invocation_resolution() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
        let job_spec = spec(temp.path());
        let hash = normalized_payload_hash(&job_spec).unwrap();
        let receipt = store
            .submit(Uuid::now_v7(), &hash, &job_spec)
            .unwrap()
            .receipt;
        let prepared = store.prepare_job(receipt.job_id).unwrap().unwrap();
        std::fs::create_dir(&prepared.stdout_path).unwrap();

        store
            .mark_invocation_resolved(&prepared, Some(0), None)
            .unwrap();
        let snapshot = store.status(receipt.job_id).unwrap();
        assert_eq!(
            snapshot.attempts[0].invocations[0].state,
            InvocationState::Resolved
        );
        assert!(
            snapshot.attempts[0].invocations[0]
                .stdout_tail
                .contains("tail unavailable")
        );
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
        let config = HostConfig {
            resources: capacities(),
            profiles: Default::default(),
            impact_incompatibilities: Default::default(),
        };
        std::fs::write(&paths.config, serde_json::to_vec(&config).unwrap()).unwrap();
        let store = Store::open(paths).unwrap();
        assert_ne!(store.store_uuid, old_uuid);
        assert_eq!(std::fs::read(&log_marker).unwrap(), b"preserve me");
        assert_eq!(
            std::fs::read(&store.paths.config).unwrap(),
            serde_json::to_vec(&config).unwrap()
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

    fn start_managed_parent(store: &mut Store, root: &Path, enabled: bool) -> PreparedJob {
        let mut parent_spec = spec(root);
        parent_spec.allow_child_submissions = enabled;
        start_managed_parent_with_spec(store, parent_spec)
    }

    fn start_managed_parent_with_spec(store: &mut Store, parent_spec: JobSpec) -> PreparedJob {
        let hash = normalized_payload_hash(&parent_spec).unwrap();
        let receipt = store
            .submit(Uuid::now_v7(), &hash, &parent_spec)
            .unwrap()
            .receipt;
        let prepared = store.prepare_job(receipt.job_id).unwrap().unwrap();
        store.mark_started(&prepared, 4242, "parent-image").unwrap();
        prepared
    }

    fn start_managed_child(
        store: &mut Store,
        parent: SubmissionScope,
        child_spec: &JobSpec,
    ) -> PreparedJob {
        let hash = normalized_payload_hash(child_spec).unwrap();
        let receipt = store
            .submit_with_stdin_scoped(parent, Uuid::now_v7(), &hash, child_spec, None)
            .unwrap()
            .receipt;
        let prepared = store.prepare_job(receipt.job_id).unwrap().unwrap();
        store.mark_started(&prepared, 4343, "child-image").unwrap();
        prepared
    }

    fn scope_for(prepared: &PreparedJob) -> SubmissionScope {
        SubmissionScope::Managed(ManagedParent {
            job_id: prepared.job_id,
            attempt_id: prepared.attempt_id,
            invocation_id: prepared.invocation_id,
        })
    }

    #[test]
    fn managed_not_received_is_provable_only_for_the_live_current_parent() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
        let parent = start_managed_parent(&mut store, temp.path(), true);
        let scope = scope_for(&parent);
        let key = Uuid::now_v7();

        assert_eq!(
            store
                .recover_submission_scoped(scope, key, "child-payload")
                .unwrap(),
            RecoveryResult::NotReceived
        );

        store
            .mark_finished(&parent, Some(0), JobOutcome::Succeeded, "succeeded")
            .unwrap();
        assert_eq!(
            store
                .recover_submission_scoped(scope, key, "child-payload")
                .unwrap(),
            RecoveryResult::Unknown
        );
    }

    #[test]
    fn managed_exact_replay_is_idempotent_and_commits_parentage() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
        let parent = start_managed_parent(&mut store, temp.path(), true);
        let scope = scope_for(&parent);
        let child = spec(temp.path());
        let hash = normalized_payload_hash(&child).unwrap();
        let key = Uuid::now_v7();

        assert_eq!(
            store.recover_submission_scoped(scope, key, &hash).unwrap(),
            RecoveryResult::NotReceived
        );
        let first = store
            .submit_with_stdin_scoped(scope, key, &hash, &child, None)
            .unwrap();
        let replay = store
            .submit_with_stdin_scoped(scope, key, &hash, &child, None)
            .unwrap();
        assert_eq!(first.receipt.job_id, replay.receipt.job_id);
        assert_eq!(first.receipt.parent, scope.parent());
        assert_eq!(
            store.status(first.receipt.job_id).unwrap().parent,
            scope.parent()
        );
        assert!(matches!(
            store
                .recover_submission_scoped(scope, key, &hash)
                .unwrap(),
            RecoveryResult::Accepted(receipt) if receipt.job_id == first.receipt.job_id
        ));

        let mut changed = child.clone();
        changed.args.push("different".into());
        let changed_hash = normalized_payload_hash(&changed).unwrap();
        assert!(matches!(
            store.submit_with_stdin_scoped(scope, key, &changed_hash, &changed, None),
            Err(StoreError::IdempotencyConflict)
        ));
    }

    #[test]
    fn managed_combined_wait_rejects_ancestor_scalar_but_detached_submit_survives() {
        let temp = tempfile::tempdir().unwrap();
        let paths = StorePaths::new(temp.path().to_path_buf());
        let mut store = Store::open_with_capacities(paths, capacities()).unwrap();
        let mut parent_spec = spec(temp.path());
        parent_spec.allow_child_submissions = true;
        parent_spec.resources.cargo_slots = Some(1);
        let parent = start_managed_parent_with_spec(&mut store, parent_spec);
        let scope = scope_for(&parent);
        let mut child = spec(temp.path());
        child.resources.cargo_slots = Some(1);
        let hash = normalized_payload_hash(&child).unwrap();
        let wait_key = Uuid::now_v7();

        assert!(matches!(
            store.submit_with_stdin_scoped_for_wait(scope, wait_key, &hash, &child, None, true,),
            Err(StoreError::BlockedByAncestor(_))
        ));
        let (state, wait_intent): (String, bool) = store
            .connection
            .query_row(
                "SELECT state, wait_intent FROM submissions WHERE idempotency_key = ?1",
                [wait_key.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(state, "rejected");
        assert!(wait_intent);
        assert!(matches!(
            store.submit_with_stdin_scoped_for_wait(
                scope,
                wait_key,
                &hash,
                &child,
                None,
                true,
            ),
            Err(StoreError::BlockedByAncestor(detail)) if detail.contains("cargo_slots")
        ));
        assert!(matches!(
            store.recover_submission_scoped(scope, wait_key, &hash).unwrap(),
            RecoveryResult::Rejected { code, detail }
                if code == "blocked_by_ancestor" && detail.contains("cargo_slots")
        ));
        let jobs: u64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM jobs", [], |row| row.get(0))
            .unwrap();
        assert_eq!(jobs, 1, "unsafe combined wait must create no child Job");

        let detached_key = Uuid::now_v7();
        let detached = store
            .submit_with_stdin_scoped_for_wait(scope, detached_key, &hash, &child, None, false)
            .unwrap();
        assert!(matches!(
            store.validate_managed_wait(scope, &[detached.receipt.job_id]),
            Err(StoreError::BlockedByAncestor(_))
        ));
        assert!(
            detached
                .receipt
                .blockers
                .iter()
                .any(|blocker| blocker.code == "resource_busy")
        );
        let replay = store
            .submit_with_stdin_scoped_for_wait(scope, detached_key, &hash, &child, None, true)
            .unwrap();
        assert_eq!(replay.receipt.job_id, detached.receipt.job_id);
        assert!(!replay.should_schedule);
    }

    #[test]
    fn managed_wait_rejects_a_claim_that_exceeds_host_capacity() {
        let temp = tempfile::tempdir().unwrap();
        let paths = StorePaths::new(temp.path().to_path_buf());
        let mut store = Store::open_with_capacities(paths, capacities()).unwrap();
        let mut parent_spec = spec(temp.path());
        parent_spec.allow_child_submissions = true;
        let parent = start_managed_parent_with_spec(&mut store, parent_spec);
        let scope = scope_for(&parent);
        let mut child = spec(temp.path());
        child.resources.cargo_slots = Some(2);
        let hash = normalized_payload_hash(&child).unwrap();

        assert!(matches!(
            store.submit_with_stdin_scoped_for_wait(
                scope,
                Uuid::now_v7(),
                &hash,
                &child,
                None,
                true,
            ),
            Err(StoreError::ManagedWaitRejected { code, detail })
                if code == "resource_capacity" && detail.contains("configured capacity 1")
        ));
        let children: u64 = store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM jobs WHERE parent_job_id = ?1",
                [parent.job_id.entity_uuid().to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(children, 0);
    }

    #[test]
    fn received_wait_intent_survives_resume_and_cannot_accept_an_unsafe_child() {
        let temp = tempfile::tempdir().unwrap();
        let paths = StorePaths::new(temp.path().to_path_buf());
        let mut store = Store::open_with_capacities(paths, capacities()).unwrap();
        let mut parent_spec = spec(temp.path());
        parent_spec.allow_child_submissions = true;
        parent_spec.resources.cargo_slots = Some(1);
        let parent = start_managed_parent_with_spec(&mut store, parent_spec);
        let scope = scope_for(&parent);
        let mut child = spec(temp.path());
        child.resources.cargo_slots = Some(1);
        let hash = normalized_payload_hash(&child).unwrap();
        let key = Uuid::now_v7();
        let submission_id = SubmissionId::new(store.store_uuid);
        let managed = scope.parent().unwrap();
        store
            .connection
            .execute(
                "INSERT INTO submissions(
                    id, scope, idempotency_key, payload_hash, state, spec_json, kind,
                    parent_job_id, parent_attempt_id, parent_invocation_id, wait_intent, created_ms
                 ) VALUES (?1, ?2, ?3, ?4, 'received', ?5, 'job', ?6, ?7, ?8, 1, ?9)",
                params![
                    submission_id.entity_uuid().to_string(),
                    scope.key(),
                    key.to_string(),
                    hash,
                    serde_json::to_string(&child).unwrap(),
                    managed.job_id.entity_uuid().to_string(),
                    managed.attempt_id.entity_uuid().to_string(),
                    managed.invocation_id.entity_uuid().to_string(),
                    now_millis(),
                ],
            )
            .unwrap();

        store.resume_received().unwrap();
        assert!(matches!(
            store.recover_submission_scoped(scope, key, &hash).unwrap(),
            RecoveryResult::Rejected { .. }
        ));
        let children: u64 = store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM jobs WHERE parent_job_id = ?1",
                [managed.job_id.entity_uuid().to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(children, 0);
    }

    #[test]
    fn managed_wait_allows_orthogonal_child_and_checks_the_full_ancestor_chain() {
        let temp = tempfile::tempdir().unwrap();
        let paths = StorePaths::new(temp.path().to_path_buf());
        let mut store = Store::open_with_capacities(paths, capacities()).unwrap();
        let mut grandparent_spec = spec(temp.path());
        grandparent_spec.allow_child_submissions = true;
        grandparent_spec.resources.cargo_slots = Some(1);
        let grandparent = start_managed_parent_with_spec(&mut store, grandparent_spec);

        let mut waiter_spec = spec(temp.path());
        waiter_spec.allow_child_submissions = true;
        let waiter = start_managed_child(&mut store, scope_for(&grandparent), &waiter_spec);
        let waiter_scope = scope_for(&waiter);
        let mut orthogonal = spec(temp.path());
        orthogonal.resources.gpu_slots = Some(1);
        let orthogonal_hash = normalized_payload_hash(&orthogonal).unwrap();
        let accepted = store
            .submit_with_stdin_scoped_for_wait(
                waiter_scope,
                Uuid::now_v7(),
                &orthogonal_hash,
                &orthogonal,
                None,
                true,
            )
            .unwrap();
        store
            .validate_managed_wait(waiter_scope, &[accepted.receipt.job_id])
            .unwrap();

        let mut conflicting = spec(temp.path());
        conflicting.resources.cargo_slots = Some(1);
        let conflicting_hash = normalized_payload_hash(&conflicting).unwrap();
        assert!(matches!(
            store.submit_with_stdin_scoped_for_wait(
                waiter_scope,
                Uuid::now_v7(),
                &conflicting_hash,
                &conflicting,
                None,
                true,
            ),
            Err(StoreError::BlockedByAncestor(detail)) if detail.contains("cargo_slots")
        ));
    }

    #[test]
    fn managed_wait_walks_unfinished_predecessors_and_rejects_self_or_foreign_targets() {
        let temp = tempfile::tempdir().unwrap();
        let paths = StorePaths::new(temp.path().to_path_buf());
        let mut store = Store::open_with_capacities(paths, capacities()).unwrap();
        let mut parent_spec = spec(temp.path());
        parent_spec.allow_child_submissions = true;
        parent_spec.resources.cargo_slots = Some(1);
        let parent = start_managed_parent_with_spec(&mut store, parent_spec);
        let scope = scope_for(&parent);

        let mut predecessor = spec(temp.path());
        predecessor.resources.cargo_slots = Some(1);
        let mut successor = spec(temp.path());
        successor.resources.gpu_slots = Some(1);
        let batch = BatchSpec {
            spec_version: SPEC_VERSION,
            jobs: vec![
                member("predecessor", predecessor, Vec::new()),
                member(
                    "successor",
                    successor,
                    vec![DependencySpec {
                        job: "predecessor".into(),
                        on: DependencyKind::Terminal,
                    }],
                ),
            ],
        };
        let hash = normalized_batch_payload_hash(&batch).unwrap();
        let receipt = store
            .submit_batch_with_stdins_scoped(
                scope,
                Uuid::now_v7(),
                &hash,
                &batch,
                &Default::default(),
            )
            .unwrap()
            .receipt;
        let successor_id = receipt
            .jobs
            .iter()
            .find(|member| member.name == "successor")
            .unwrap()
            .receipt
            .job_id;
        assert!(matches!(
            store.validate_managed_wait(scope, &[successor_id]),
            Err(StoreError::BlockedByAncestor(detail)) if detail.contains("predecessor") && detail.contains("cargo_slots")
        ));

        let foreign_spec = spec(temp.path());
        let foreign_hash = normalized_payload_hash(&foreign_spec).unwrap();
        let foreign = store
            .submit(Uuid::now_v7(), &foreign_hash, &foreign_spec)
            .unwrap();
        assert!(matches!(
            store.validate_managed_wait(scope, &[foreign.receipt.job_id]),
            Err(StoreError::Rejected(_))
        ));

        let direct_child = receipt.jobs[0].receipt.job_id;
        store
            .connection
            .execute(
                "INSERT INTO dependencies(predecessor_id, successor_id, kind)
                 VALUES (?1, ?2, 'terminal')",
                params![
                    parent.job_id.entity_uuid().to_string(),
                    direct_child.entity_uuid().to_string(),
                ],
            )
            .unwrap();
        assert!(matches!(
            store.validate_managed_wait(scope, &[direct_child]),
            Err(StoreError::BlockedByAncestor(detail)) if detail.contains("waiting Job itself")
        ));
    }

    #[test]
    fn managed_batch_wait_rejects_atomically_when_one_member_conflicts() {
        let temp = tempfile::tempdir().unwrap();
        let paths = StorePaths::new(temp.path().to_path_buf());
        let mut store = Store::open_with_capacities(paths, capacities()).unwrap();
        let mut parent_spec = spec(temp.path());
        parent_spec.allow_child_submissions = true;
        parent_spec.resources.cargo_slots = Some(1);
        let parent = start_managed_parent_with_spec(&mut store, parent_spec);
        let scope = scope_for(&parent);
        let mut safe = spec(temp.path());
        safe.resources.gpu_slots = Some(1);
        let mut blocked = spec(temp.path());
        blocked.resources.cargo_slots = Some(1);
        let batch = BatchSpec {
            spec_version: SPEC_VERSION,
            jobs: vec![
                member("safe", safe, Vec::new()),
                member("blocked", blocked, Vec::new()),
            ],
        };
        let hash = normalized_batch_payload_hash(&batch).unwrap();
        let key = Uuid::now_v7();
        assert!(matches!(
            store.submit_batch_with_stdins_scoped_for_wait(
                scope,
                key,
                &hash,
                &batch,
                &Default::default(),
                true,
            ),
            Err(StoreError::BlockedByAncestor(_))
        ));
        let child_jobs: u64 = store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM jobs WHERE parent_job_id = ?1",
                [parent.job_id.entity_uuid().to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(child_jobs, 0);
        let batches: u64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM batches", [], |row| row.get(0))
            .unwrap();
        assert_eq!(batches, 0);
        let state: String = store
            .connection
            .query_row(
                "SELECT state FROM submissions WHERE idempotency_key = ?1",
                [key.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(state, "rejected");
    }

    #[test]
    fn managed_acceptance_rechecks_parent_and_disabled_primary_never_proves_absence() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
        let disabled = start_managed_parent(&mut store, temp.path(), false);
        let disabled_scope = scope_for(&disabled);
        let child = spec(temp.path());
        let hash = normalized_payload_hash(&child).unwrap();
        assert_eq!(
            store
                .recover_submission_scoped(disabled_scope, Uuid::now_v7(), &hash)
                .unwrap(),
            RecoveryResult::Unknown
        );
        assert!(matches!(
            store.submit_with_stdin_scoped(disabled_scope, Uuid::now_v7(), &hash, &child, None,),
            Err(StoreError::Rejected(_))
        ));

        let enabled = start_managed_parent(&mut store, temp.path(), true);
        let enabled_scope = scope_for(&enabled);
        store.mark_root_exited(&enabled, 0).unwrap();
        assert_eq!(
            store
                .recover_submission_scoped(enabled_scope, Uuid::now_v7(), &hash)
                .unwrap(),
            RecoveryResult::Unknown,
            "a live descendant cannot submit after the primary root exited"
        );
        store
            .mark_finished(&enabled, Some(0), JobOutcome::Succeeded, "succeeded")
            .unwrap();
        let key = Uuid::now_v7();
        assert!(matches!(
            store.submit_with_stdin_scoped(enabled_scope, key, &hash, &child, None),
            Err(StoreError::Rejected(_))
        ));
        let retained: u64 = store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM submissions WHERE idempotency_key = ?1",
                [key.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(retained, 0, "late child rejection must create no work");
    }

    #[test]
    fn restart_rejects_managed_received_work_from_the_previous_daemon_generation() {
        let temp = tempfile::tempdir().unwrap();
        let paths = StorePaths::new(temp.path().to_path_buf());
        let mut store = Store::open(paths.clone()).unwrap();
        let previous_generation = store.daemon_generation;
        let parent = start_managed_parent(&mut store, temp.path(), true);
        let scope = scope_for(&parent);
        let child = spec(temp.path());
        let hash = normalized_payload_hash(&child).unwrap();
        let key = Uuid::now_v7();
        let submission_id = SubmissionId::new(store.store_uuid);
        store
            .connection
            .execute(
                "INSERT INTO submissions(
                    id, scope, idempotency_key, payload_hash, state, spec_json, kind,
                    parent_job_id, parent_attempt_id, parent_invocation_id, created_ms
                 ) VALUES (?1, ?2, ?3, ?4, 'received', ?5, 'job', ?6, ?7, ?8, ?9)",
                params![
                    submission_id.entity_uuid().to_string(),
                    scope.key(),
                    key.to_string(),
                    hash,
                    serde_json::to_string(&child).unwrap(),
                    parent.job_id.entity_uuid().to_string(),
                    parent.attempt_id.entity_uuid().to_string(),
                    parent.invocation_id.entity_uuid().to_string(),
                    now_millis(),
                ],
            )
            .unwrap();
        drop(store);

        let reopened = Store::open(paths).unwrap();
        assert_ne!(reopened.daemon_generation, previous_generation);
        let state: String = reopened
            .connection
            .query_row(
                "SELECT state FROM submissions WHERE id = ?1",
                [submission_id.entity_uuid().to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(state, "rejected");
        let child_jobs: u64 = reopened
            .connection
            .query_row(
                "SELECT COUNT(*) FROM jobs WHERE submission_id = ?1",
                [submission_id.entity_uuid().to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(child_jobs, 0, "restart must not accept a late child");
    }

    #[test]
    fn alpha6_postcondition_retry_keeps_one_job_and_exposes_ordered_attempts() {
        let temp = tempfile::tempdir().unwrap();
        let mut store =
            Store::open_with_capacities(StorePaths::new(temp.path().to_path_buf()), capacities())
                .unwrap();
        let mut job = spec(temp.path());
        job.resources.cargo_slots = Some(1);
        job.retry = RetryPolicy {
            max_attempts: 2,
            backoff_seconds: 0,
            retryable: vec!["postcondition_retryable".into()],
        };
        job.postconditions.push(PostconditionSpec {
            executable: temp.path().join("validate.exe"),
            args: vec!["--result".into()],
            working_directory: None,
            accepted_exit_codes: vec![0],
            retryable_exit_codes: vec![10],
        });
        let hash = normalized_payload_hash(&job).unwrap();
        let receipt = store.submit(Uuid::now_v7(), &hash, &job).unwrap().receipt;
        let first = store.prepare_job(receipt.job_id).unwrap().unwrap();
        store
            .mark_invocation_resolved(&first, Some(0), None)
            .unwrap();

        let mut contender = spec(temp.path());
        contender.resources.cargo_slots = Some(1);
        let contender_hash = normalized_payload_hash(&contender).unwrap();
        let contender = store
            .submit(Uuid::now_v7(), &contender_hash, &contender)
            .unwrap()
            .receipt;
        assert!(
            contender
                .blockers
                .iter()
                .any(|blocker| blocker.code == "resource_busy"),
            "primary cleanup must not release the Attempt Lease before postconditions"
        );

        let validator = store.prepare_postcondition(&first, 0).unwrap();
        assert!(
            store
                .status(contender.job_id)
                .unwrap()
                .blockers
                .iter()
                .any(|blocker| blocker.code == "resource_busy"),
            "preparing a postcondition must retain the complete Attempt Lease"
        );
        store
            .mark_invocation_resolved(&validator, Some(10), Some(ExitClassification::Retryable))
            .unwrap();
        assert!(
            store
                .status(contender.job_id)
                .unwrap()
                .blockers
                .iter()
                .any(|blocker| blocker.code == "resource_busy"),
            "resolving the validator must not release the Lease before Attempt settlement"
        );
        assert!(
            store
                .settle_attempt(&first, AttemptVerdict::PostconditionRetryable)
                .unwrap()
        );
        let between = store.status(receipt.job_id).unwrap();
        assert_eq!(between.state, JobState::Pending);
        assert_eq!(between.attempts.len(), 1);
        assert_eq!(
            between.attempts[0].verdict,
            Some(AttemptVerdict::PostconditionRetryable)
        );
        assert_eq!(between.attempts[0].invocations.len(), 2);
        assert_eq!(
            between.attempts[0].invocations[1].exit_classification,
            Some(ExitClassification::Retryable)
        );

        let second = store.prepare_job(receipt.job_id).unwrap().unwrap();
        assert_ne!(first.attempt_id, second.attempt_id);
        store
            .mark_invocation_resolved(&second, Some(0), None)
            .unwrap();
        let validator = store.prepare_postcondition(&second, 0).unwrap();
        store
            .mark_invocation_resolved(&validator, Some(0), Some(ExitClassification::Accepted))
            .unwrap();
        assert!(
            !store
                .settle_attempt(&second, AttemptVerdict::Succeeded)
                .unwrap()
        );
        let final_snapshot = store.status(receipt.job_id).unwrap();
        assert_eq!(final_snapshot.outcome, Some(JobOutcome::Succeeded));
        assert_eq!(
            final_snapshot
                .attempts
                .iter()
                .map(|attempt| attempt.attempt_index)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn alpha6_plain_cancel_covers_queued_active_and_backoff_without_successors() {
        let temp = tempfile::tempdir().unwrap();
        let mut store =
            Store::open_with_capacities(StorePaths::new(temp.path().to_path_buf()), capacities())
                .unwrap();
        let batch = BatchSpec {
            spec_version: SPEC_VERSION,
            jobs: vec![
                member("reviewer", spec(temp.path()), vec![]),
                member(
                    "collect",
                    spec(temp.path()),
                    vec![DependencySpec {
                        job: "reviewer".into(),
                        on: DependencyKind::Terminal,
                    }],
                ),
            ],
        };
        let hash = normalized_batch_payload_hash(&batch).unwrap();
        let receipt = store
            .submit_batch(Uuid::now_v7(), &hash, &batch)
            .unwrap()
            .receipt;
        let reviewer = receipt.jobs[0].receipt.job_id;
        let collect = receipt.jobs[1].receipt.job_id;
        assert!(matches!(
            store.cancel_jobs(&vec![reviewer; MAX_CANCEL_JOBS + 1]),
            Err(StoreError::InvalidSpec(_))
        ));
        let canceled = store.cancel_jobs(&[reviewer]).unwrap();
        assert_eq!(canceled[0].outcome, Some(JobOutcome::Canceled));
        assert!(canceled[0].cancel_requested);
        let prepared_collect = store.prepare_next_job().unwrap().unwrap();
        assert_eq!(
            prepared_collect.job_id, collect,
            "plain cancel must not select collect"
        );

        let mut active_spec = spec(temp.path());
        active_spec.retry = RetryPolicy {
            max_attempts: 2,
            backoff_seconds: 60,
            retryable: vec!["process_failed".into()],
        };
        let hash = normalized_payload_hash(&active_spec).unwrap();
        let active = store
            .submit(Uuid::now_v7(), &hash, &active_spec)
            .unwrap()
            .receipt
            .job_id;
        let prepared = store.prepare_job(active).unwrap().unwrap();
        let canceling = store.cancel_jobs(&[active]).unwrap();
        assert_eq!(canceling[0].state, JobState::Active);
        assert!(canceling[0].cancel_requested);
        assert!(store.cancel_requested(active).unwrap());
        store
            .mark_invocation_resolved(&prepared, Some(1), None)
            .unwrap();
        assert!(
            !store
                .settle_attempt(&prepared, AttemptVerdict::ProcessFailed)
                .unwrap()
        );
        let active = store.status(active).unwrap();
        assert_eq!(active.outcome, Some(JobOutcome::Canceled));
        assert_eq!(active.attempts[0].verdict, Some(AttemptVerdict::Canceled));
    }

    #[test]
    fn alpha6_expired_blocked_retry_does_not_spin_and_backoff_cancel_is_terminal() {
        let temp = tempfile::tempdir().unwrap();
        let mut store =
            Store::open_with_capacities(StorePaths::new(temp.path().to_path_buf()), capacities())
                .unwrap();
        let mut retry_spec = spec(temp.path());
        retry_spec.resources.cargo_slots = Some(1);
        retry_spec.retry = RetryPolicy {
            max_attempts: 2,
            backoff_seconds: 60,
            retryable: vec!["process_failed".into()],
        };
        let hash = normalized_payload_hash(&retry_spec).unwrap();
        let retry_job = store
            .submit(Uuid::now_v7(), &hash, &retry_spec)
            .unwrap()
            .receipt
            .job_id;
        let first = store.prepare_job(retry_job).unwrap().unwrap();
        store
            .mark_invocation_resolved(&first, Some(1), None)
            .unwrap();
        assert!(
            store
                .settle_attempt(&first, AttemptVerdict::ProcessFailed)
                .unwrap()
        );

        let mut holder_spec = spec(temp.path());
        holder_spec.resources.cargo_slots = Some(1);
        let hash = normalized_payload_hash(&holder_spec).unwrap();
        let holder = store
            .submit(Uuid::now_v7(), &hash, &holder_spec)
            .unwrap()
            .receipt
            .job_id;
        store.prepare_job(holder).unwrap().unwrap();
        let boundary = now_millis();
        store
            .connection
            .execute(
                "UPDATE jobs SET retry_not_before_ms = ?2 WHERE id = ?1",
                params![retry_job.entity_uuid().to_string(), boundary],
            )
            .unwrap();
        assert!(
            store.next_retry_delay(boundary - 1).unwrap().is_some(),
            "a retry that expires during a scheduling pass needs one immediate rescan"
        );
        store
            .connection
            .execute(
                "UPDATE jobs SET retry_not_before_ms = ?2 WHERE id = ?1",
                params![retry_job.entity_uuid().to_string(), now_millis() - 1],
            )
            .unwrap();

        assert!(store.prepare_job(retry_job).unwrap().is_none());
        let after_expiry = now_millis();
        assert_eq!(
            store.next_retry_delay(after_expiry).unwrap(),
            None,
            "an expired retry blocked on a Lease must wait for the Lease-release wakeup"
        );
        let canceled = store.cancel_jobs(&[retry_job]).unwrap();
        assert_eq!(canceled[0].outcome, Some(JobOutcome::Canceled));
        assert_eq!(canceled[0].attempts.len(), 1);
        assert!(store.prepare_job(retry_job).unwrap().is_none());
    }

    #[test]
    fn alpha6_impact_rules_block_admission_and_ancestor_waits_symmetrically() {
        let temp = tempfile::tempdir().unwrap();
        let config = HostConfig {
            resources: capacities(),
            profiles: Default::default(),
            impact_incompatibilities: [(
                "measurement".into(),
                vec!["cpu_heavy".into(), "gpu_heavy".into()],
            )]
            .into(),
        };
        let mut store =
            Store::open_with_config(StorePaths::new(temp.path().to_path_buf()), config).unwrap();
        let mut cpu = spec(temp.path());
        cpu.resources.impacts = vec!["cpu_heavy".into()];
        let hash = normalized_payload_hash(&cpu).unwrap();
        let cpu = store.submit(Uuid::now_v7(), &hash, &cpu).unwrap().receipt;
        store.prepare_job(cpu.job_id).unwrap().unwrap();

        let mut measurement = spec(temp.path());
        measurement.resources.impacts = vec!["measurement".into()];
        let hash = normalized_payload_hash(&measurement).unwrap();
        let measurement = store
            .submit(Uuid::now_v7(), &hash, &measurement)
            .unwrap()
            .receipt;
        assert!(
            measurement
                .blockers
                .iter()
                .any(|blocker| blocker.code == "impact_busy")
        );
        let daemon = store.daemon_status().unwrap();
        assert_eq!(daemon.daemon_generation, cpu.daemon_generation);
        assert!(!daemon.config_sha256.is_empty());
    }

    #[test]
    fn alpha6_receipt_preserves_accepting_generation_across_daemon_restart() {
        let temp = tempfile::tempdir().unwrap();
        let paths = StorePaths::new(temp.path().to_path_buf());
        let key = Uuid::now_v7();
        let job = spec(temp.path());
        let hash = normalized_payload_hash(&job).unwrap();
        let accepted_generation = {
            let mut store = Store::open(paths.clone()).unwrap();
            store
                .submit(key, &hash, &job)
                .unwrap()
                .receipt
                .daemon_generation
        };
        let mut reopened = Store::open(paths).unwrap();
        assert_ne!(reopened.daemon_generation, accepted_generation);
        let replay = reopened.submit(key, &hash, &job).unwrap();
        assert!(!replay.should_schedule);
        assert_eq!(replay.receipt.daemon_generation, accepted_generation);
    }

    #[test]
    fn alpha6_root_exit_is_visible_before_containment_resolution() {
        let temp = tempfile::tempdir().unwrap();
        let mut store =
            Store::open_with_capacities(StorePaths::new(temp.path().to_path_buf()), capacities())
                .unwrap();
        let job = spec(temp.path());
        let hash = normalized_payload_hash(&job).unwrap();
        let receipt = store.submit(Uuid::now_v7(), &hash, &job).unwrap().receipt;
        let prepared = store.prepare_job(receipt.job_id).unwrap().unwrap();
        store.mark_started(&prepared, 42, "test-hash").unwrap();
        store.mark_root_exited(&prepared, 0).unwrap();

        let snapshot = store.status(receipt.job_id).unwrap();
        let invocation = &snapshot.attempts[0].invocations[0];
        assert_eq!(invocation.state, InvocationState::Exited);
        assert_eq!(invocation.root_exit_code, Some(0));
        assert_eq!(invocation.containment.state, ContainmentState::Live);
    }

    #[test]
    fn snapshot_diagnostic_budget_keeps_newest_utf8_suffixes() {
        let mut newest = "new".to_owned();
        let mut older = "éolder".to_owned();
        let mut remaining = 5;
        keep_tail_within_budget(&mut newest, &mut remaining);
        keep_tail_within_budget(&mut older, &mut remaining);

        assert_eq!(newest, "new");
        assert_eq!(older, "er");
        assert_eq!(remaining, 0);
    }

    #[test]
    fn alpha7_list_events_and_cursor_are_one_public_observation_path() {
        let temp = tempfile::tempdir().unwrap();
        let mut store =
            Store::open_with_capacities(StorePaths::new(temp.path().to_path_buf()), capacities())
                .unwrap();
        let mut job = spec(temp.path());
        job.labels.push(crate::Label {
            key: "round".into(),
            value: "seven".into(),
        });
        job.resources.cargo_slots = Some(1);
        let hash = normalized_payload_hash(&job).unwrap();
        let receipt = store.submit(Uuid::now_v7(), &hash, &job).unwrap().receipt;

        let page = store
            .list_jobs(
                &JobSelector::Labels {
                    labels: job.labels.clone(),
                },
                None,
                10,
            )
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].job_id, receipt.job_id);
        assert_eq!(page.jobs[0].queue_rank, Some(1));
        assert_eq!(page.jobs[0].claims.cargo_slots, Some(1));
        assert!(page.event_cursor.sequence > 0);

        let frame = store
            .observe(
                &JobSelector::Jobs {
                    job_ids: vec![receipt.job_id],
                },
                Some(EventCursor {
                    store_uuid: store.store_uuid,
                    sequence: 0,
                }),
                100,
            )
            .unwrap();
        let ObservationFrame::Events { events, cursor } = frame else {
            panic!("fresh retained history must not produce Gap");
        };
        assert!(!events.is_empty());
        assert_eq!(events.last().unwrap().cursor, cursor);
        assert!(events.iter().all(|event| event.job_id == receipt.job_id));

        let replaced = store
            .observe(
                &JobSelector::All,
                Some(EventCursor {
                    store_uuid: Uuid::now_v7(),
                    sequence: cursor.sequence,
                }),
                10,
            )
            .unwrap();
        assert!(matches!(replaced, ObservationFrame::Gap { .. }));

        let before = store.event_head().unwrap();
        let transaction = store.connection.transaction().unwrap();
        transaction
            .execute(
                "UPDATE jobs SET stdout_len = 99 WHERE id = ?1",
                [receipt.job_id.entity_uuid().to_string()],
            )
            .unwrap();
        drop(transaction);
        assert_eq!(store.event_head().unwrap(), before);
        assert_eq!(
            store.job_summary(receipt.job_id).unwrap().stdout_committed,
            0
        );
    }

    #[test]
    fn alpha7_event_ring_reports_gap_and_resynchronizes() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
        let job = spec(temp.path());
        let hash = normalized_payload_hash(&job).unwrap();
        let receipt = store.submit(Uuid::now_v7(), &hash, &job).unwrap().receipt;
        let transaction = store.connection.transaction().unwrap();
        for offset in 1..=(MAX_EVENT_ROWS + 8) {
            transaction
                .execute(
                    "UPDATE jobs SET stdout_len = ?2 WHERE id = ?1",
                    params![receipt.job_id.entity_uuid().to_string(), offset],
                )
                .unwrap();
        }
        transaction.commit().unwrap();
        let retained: u64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(retained, MAX_EVENT_ROWS);

        let frame = store
            .observe(
                &JobSelector::All,
                Some(EventCursor {
                    store_uuid: store.store_uuid,
                    sequence: 0,
                }),
                16,
            )
            .unwrap();
        let ObservationFrame::Gap {
            gap,
            snapshot,
            cursor,
        } = frame
        else {
            panic!("expired cursor must produce Gap");
        };
        assert!(gap.oldest_available.sequence > 1);
        assert_eq!(snapshot.jobs[0].job_id, receipt.job_id);
        assert_eq!(snapshot.jobs[0].stdout_committed, MAX_EVENT_ROWS + 8);
        assert_eq!(snapshot.event_cursor, cursor);
    }

    #[test]
    fn alpha7_list_cursor_is_stable_and_store_scoped() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
        for _ in 0..3 {
            let job = spec(temp.path());
            let hash = normalized_payload_hash(&job).unwrap();
            store.submit(Uuid::now_v7(), &hash, &job).unwrap();
        }
        let first = store.list_jobs(&JobSelector::All, None, 1).unwrap();
        store
            .connection
            .execute(
                "UPDATE jobs SET state = 'active' WHERE id = ?1",
                [first.jobs[0].job_id.entity_uuid().to_string()],
            )
            .unwrap();
        let second = store
            .list_jobs(&JobSelector::All, first.next_cursor, 1)
            .unwrap();
        assert_ne!(first.jobs[0].job_id, second.jobs[0].job_id);
        let mut foreign = first.next_cursor.unwrap();
        foreign.store_uuid = Uuid::now_v7();
        assert!(matches!(
            store.list_jobs(&JobSelector::All, Some(foreign), 1),
            Err(StoreError::Rejected(_))
        ));
    }
}
