use std::collections::BTreeMap;
#[cfg(windows)]
use std::ffi::OsStr;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use fs2::FileExt;
use sha2::{Digest, Sha256};

use crate::instance::{default_endpoint, default_instance, endpoints_equal, validate_endpoint};
use crate::payload::{MAX_CANCEL_JOBS, MAX_STDIN_BYTES, batch_hash, job_hash};
use crate::protocol::{PROTOCOL_VERSION, Request, Response, StagedInputRef, error_code};
#[cfg(windows)]
use crate::protocol::{read_frame, write_frame};
use crate::{
    BatchReceipt, BatchSpec, CancellationToken, ClearContainmentResult, CompleteDoctorSnapshot,
    ContainmentId, ContainmentIncidentCursor, DaemonSnapshot, DoctorSnapshot, EnsureOptions,
    EnsureOutcome, EnsuredBatch, EnsuredJob, Error, EventCursor, JobChildrenCursor,
    JobChildrenPage, JobId, JobListCursor, JobListPage, JobReceipt, JobSelector, JobSnapshot,
    JobSpec, JobTreePage, JobTreeRootCursor, JobTreeSelector, LogChunk, LogStream,
    MAX_COMPLETE_DOCTOR_BYTES, MAX_COMPLETE_DOCTOR_INCIDENTS, MAX_DOCTOR_PAGE,
    MAX_OBSERVATION_PAGE, ManagedParent, ObservationFrame, PendingReason, RecoveryResult,
    RejectReason, Result, SubmissionContext, SubmissionRef, SubmitOptions, TreeObservationFrame,
    WaitOutcome, WaitStreamItem,
};

const RESULT_FILE_VERSION: u32 = 5;

#[derive(Clone, Debug)]
pub struct ClientBuilder {
    endpoint: Option<String>,
    auto_start: bool,
    daemon_executable: Option<PathBuf>,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self {
            endpoint: None,
            auto_start: true,
            daemon_executable: None,
        }
    }
}

impl ClientBuilder {
    #[must_use]
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    #[must_use]
    pub fn auto_start(mut self, auto_start: bool) -> Self {
        self.auto_start = auto_start;
        self
    }

    /// Selects the daemon binary used for auto-start and server-image authentication.
    #[must_use]
    pub fn daemon_executable(mut self, executable: impl Into<PathBuf>) -> Self {
        self.daemon_executable = Some(executable.into());
        self
    }

    pub fn connect(
        self,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<Client> {
        let endpoint_explicit = self.endpoint.is_some();
        let managed_endpoint = std::env::var("STILLYARD_ENDPOINT").ok();
        let (endpoint, connect_only) =
            select_client_endpoint(self.endpoint, managed_endpoint.clone())?;
        validate_endpoint(&endpoint)?;
        let claimed_parent = claimed_managed_parent_for_endpoint(
            &endpoint,
            managed_endpoint.as_deref(),
            managed_environment_coordinates(),
        )?;
        let daemon_executable = self
            .daemon_executable
            .map(Ok)
            .unwrap_or_else(default_daemon_executable)?;
        let client = Client {
            endpoint,
            daemon_executable,
            claimed_parent,
            endpoint_explicit,
        };
        match client.ping(deadline, cancellation) {
            Ok(()) => Ok(client),
            Err(Error::Unavailable(detail)) if self.auto_start && connect_only => {
                Err(Error::Unavailable(format!(
                    "auto-start is unavailable for an explicit endpoint; connection failed: {detail}"
                )))
            }
            Err(Error::Unavailable(_)) if self.auto_start => {
                if std::env::var_os("STILLYARD_JOB_ID").is_some()
                    || std::env::var_os("STILLYARD_ROLE").is_some()
                {
                    return Err(Error::Unavailable(
                        "a managed child may not auto-start the daemon".into(),
                    ));
                }
                let default_instance = default_instance()?;
                let mut daemon = start_daemon(
                    &client.daemon_executable,
                    &default_instance.store_path,
                    &default_instance.endpoint,
                )?;
                let startup_deadline = deadline.min(Instant::now() + Duration::from_secs(10));
                let mut child_exit = None;
                loop {
                    if let Err(error) = check_wait(startup_deadline, cancellation) {
                        return Err(match error {
                            Error::DeadlineElapsed => Error::Unavailable(match child_exit {
                                Some(status) => format!(
                                    "daemon did not become ready within 10 seconds; spawned candidate exited with {status}"
                                ),
                                None => "daemon did not become ready within 10 seconds".into(),
                            }),
                            other => other,
                        });
                    }
                    if child_exit.is_none() {
                        child_exit = daemon.try_wait()?;
                    }
                    match client.ping(startup_deadline, cancellation) {
                        Ok(()) => return Ok(client),
                        Err(Error::Unavailable(_)) => std::thread::sleep(Duration::from_millis(25)),
                        Err(error) => return Err(error),
                    }
                }
            }
            Err(error) => Err(error),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Client {
    endpoint: String,
    daemon_executable: PathBuf,
    claimed_parent: Option<ManagedParent>,
    endpoint_explicit: bool,
}

pub struct ObservationStream {
    client: Client,
    selector: JobSelector,
    cursor: Option<EventCursor>,
    deadline: Instant,
    cancellation: Option<CancellationToken>,
    finished: bool,
}

pub struct WaitStream {
    client: Client,
    jobs: Vec<JobId>,
    settled: std::collections::BTreeSet<JobId>,
    outcomes: Vec<crate::JobOutcome>,
    pending: std::collections::VecDeque<WaitStreamItem>,
    cursor: EventCursor,
    any: bool,
    aggregate_emitted: bool,
    finished: bool,
    deadline: Instant,
    cancellation: Option<CancellationToken>,
}

pub struct LogFollower {
    client: Client,
    job_id: JobId,
    stream: LogStream,
    offset: u64,
    cursor: EventCursor,
    deadline: Instant,
    cancellation: Option<CancellationToken>,
    finished: bool,
}

#[derive(Clone, Copy, Debug)]
struct StreamProgress {
    eof: bool,
    caught_up: bool,
}

impl Client {
    #[must_use]
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    pub fn connect(deadline: Instant, cancellation: Option<&CancellationToken>) -> Result<Self> {
        Self::builder().connect(deadline, cancellation)
    }

    pub fn submit(
        &self,
        spec: JobSpec,
        options: &SubmitOptions,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<JobReceipt> {
        self.submit_inner(spec, options, false, None, deadline, cancellation)
    }

    fn submit_inner(
        &self,
        spec: JobSpec,
        options: &SubmitOptions,
        result_file_prepared: bool,
        expected_payload_hash: Option<&str>,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<JobReceipt> {
        spec.validate()?;
        let stdin = inspect_stdin(&spec.stdin)?;
        let payload_hash = job_hash(&spec, stdin.as_ref().map(|(input, _)| input))?;
        if expected_payload_hash.is_some_and(|expected| expected != payload_hash) {
            return Err(Error::InvalidSpec(
                "normalized Job payload changed after the ensure receipt was claimed".into(),
            ));
        }
        let context = self.submission_context(deadline, cancellation)?;
        if !result_file_prepared {
            if let Some(path) = &options.result_file {
                prepare_result_file(
                    path,
                    options,
                    &payload_hash,
                    &self.endpoint,
                    context,
                    deadline,
                    cancellation,
                )?;
            }
        }
        let stdin = match stdin {
            Some((input, path)) => {
                Some(self.upload_stdin(&path, &input, deadline, cancellation)?)
            }
            None => None,
        };
        let response = self.request(
            Request::Submit {
                idempotency_key: options.idempotency_key,
                payload_hash: payload_hash.clone(),
                spec: Box::new(spec),
                stdin,
                expected_store_uuid: Some(context.store_uuid),
                expected_parent: context.parent,
                wait_for_completion: options.wait_for_completion,
            },
            deadline,
            cancellation,
        )?;
        match response {
            Response::Submitted(receipt) => {
                if receipt.parent != context.parent {
                    return Err(Error::Protocol(
                        "daemon returned a receipt for a different managed parent".into(),
                    ));
                }
                if let Some(path) = &options.result_file {
                    persist_result_receipt(
                        path,
                        &ResultFileRecord {
                            version: RESULT_FILE_VERSION,
                            idempotency_key: options.idempotency_key,
                            payload_hash: payload_hash.clone(),
                            endpoint: self.endpoint.clone(),
                            store_uuid: context.store_uuid,
                            parent: context.parent,
                            receipt: None,
                        },
                        RecoveryResult::Accepted(receipt.clone()),
                        deadline,
                        cancellation,
                    )?;
                }
                Ok(receipt)
            }
            response => {
                if let Some(path) = &options.result_file {
                    persist_submit_decision(
                        path,
                        &ResultFileRecord {
                            version: RESULT_FILE_VERSION,
                            idempotency_key: options.idempotency_key,
                            payload_hash: payload_hash.clone(),
                            endpoint: self.endpoint.clone(),
                            store_uuid: context.store_uuid,
                            parent: context.parent,
                            receipt: None,
                        },
                        &response,
                        deadline,
                        cancellation,
                    )?;
                }
                response_error(response)
            }
        }
    }

    pub fn submit_batch(
        &self,
        spec: BatchSpec,
        options: &SubmitOptions,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<BatchReceipt> {
        self.submit_batch_inner(spec, options, false, None, deadline, cancellation)
    }

    fn submit_batch_inner(
        &self,
        spec: BatchSpec,
        options: &SubmitOptions,
        result_file_prepared: bool,
        expected_payload_hash: Option<&str>,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<BatchReceipt> {
        spec.validate()?;
        let mut inspected = Vec::new();
        let mut input_refs = BTreeMap::new();
        for member in &spec.jobs {
            if let Some((input, path)) = inspect_stdin(&member.spec.stdin)? {
                input_refs.insert(member.name.clone(), input.clone());
                inspected.push((member.name.clone(), input, path));
            }
        }
        let payload_hash = batch_hash(&spec, &input_refs)?;
        if expected_payload_hash.is_some_and(|expected| expected != payload_hash) {
            return Err(Error::InvalidSpec(
                "normalized Batch payload changed after the ensure receipt was claimed".into(),
            ));
        }
        let context = self.submission_context(deadline, cancellation)?;
        if !result_file_prepared {
            if let Some(path) = &options.result_file {
                prepare_result_file(
                    path,
                    options,
                    &payload_hash,
                    &self.endpoint,
                    context,
                    deadline,
                    cancellation,
                )?;
            }
        }
        let mut stdins = BTreeMap::new();
        for (name, input, path) in inspected {
            stdins.insert(
                name,
                self.upload_stdin(&path, &input, deadline, cancellation)?,
            );
        }
        let response = self.request(
            Request::SubmitBatch {
                idempotency_key: options.idempotency_key,
                payload_hash: payload_hash.clone(),
                spec: Box::new(spec),
                stdins,
                expected_store_uuid: Some(context.store_uuid),
                expected_parent: context.parent,
                wait_for_completion: options.wait_for_completion,
            },
            deadline,
            cancellation,
        )?;
        match response {
            Response::BatchSubmitted(receipt) => {
                if receipt
                    .jobs
                    .iter()
                    .any(|member| member.receipt.parent != context.parent)
                {
                    return Err(Error::Protocol(
                        "daemon returned a Batch receipt for a different managed parent".into(),
                    ));
                }
                if let Some(path) = &options.result_file {
                    persist_result_receipt(
                        path,
                        &ResultFileRecord {
                            version: RESULT_FILE_VERSION,
                            idempotency_key: options.idempotency_key,
                            payload_hash: payload_hash.clone(),
                            endpoint: self.endpoint.clone(),
                            store_uuid: context.store_uuid,
                            parent: context.parent,
                            receipt: None,
                        },
                        RecoveryResult::AcceptedBatch(receipt.clone()),
                        deadline,
                        cancellation,
                    )?;
                }
                Ok(receipt)
            }
            response => {
                if let Some(path) = &options.result_file {
                    persist_submit_decision(
                        path,
                        &ResultFileRecord {
                            version: RESULT_FILE_VERSION,
                            idempotency_key: options.idempotency_key,
                            payload_hash: payload_hash.clone(),
                            endpoint: self.endpoint.clone(),
                            store_uuid: context.store_uuid,
                            parent: context.parent,
                            receipt: None,
                        },
                        &response,
                        deadline,
                        cancellation,
                    )?;
                }
                response_error(response)
            }
        }
    }

    /// Atomically ensures one Job for the normalized payload and idempotency key.
    ///
    /// An existing result file is recovered first. Only an authenticated managed
    /// `not_received` decision permits exact replay; `unknown` always fails closed.
    pub fn ensure_job(
        &self,
        spec: JobSpec,
        options: &EnsureOptions,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<EnsureOutcome<EnsuredJob>> {
        if !self.endpoint_explicit {
            return Err(Error::InvalidSpec(
                "ensure_job requires an explicitly selected ClientBuilder endpoint".into(),
            ));
        }
        spec.validate()?;
        let operation_lock = match options
            .result_file
            .as_deref()
            .map(|path| lock_ensure_operation(path, deadline, cancellation))
            .transpose()
        {
            Ok(lock) => lock,
            Err(Error::DeadlineElapsed | Error::Canceled | Error::Unavailable(_)) => {
                return Ok(EnsureOutcome::Unknown);
            }
            Err(error) => return Err(error),
        };
        let inspected = inspect_stdin(&spec.stdin)?;
        let payload_hash = job_hash(&spec, inspected.as_ref().map(|(input, _)| input))?;
        let submit_options = ensure_submit_options(options);
        let result_file_context = if options.result_file.is_some() {
            match self.submission_context(deadline, cancellation) {
                Ok(context) => Some(context),
                Err(Error::DeadlineElapsed | Error::Canceled | Error::Unavailable(_)) => {
                    return Ok(EnsureOutcome::Unknown);
                }
                Err(
                    Error::Rejected { code, detail } | Error::ManagedWaitRejected { code, detail },
                ) => {
                    return Ok(EnsureOutcome::Rejected(RejectReason::new(code, detail)));
                }
                Err(error) => return Err(error),
            }
        } else {
            None
        };
        let result_file_state = if let Some(path) = &options.result_file {
            match prepare_ensure_result_file(
                path,
                options.idempotency_key,
                &payload_hash,
                &self.endpoint,
                result_file_context.expect("result file context was resolved"),
                deadline,
                cancellation,
            ) {
                Ok(state) => Some(state),
                Err(Error::DeadlineElapsed | Error::Canceled | Error::Unavailable(_)) => {
                    return Ok(EnsureOutcome::Unknown);
                }
                Err(error) => return Err(error),
            }
        } else {
            None
        };
        if matches!(
            &result_file_state,
            Some(EnsureResultFileState::PayloadMismatch { .. })
        ) {
            let context = result_file_context.expect("result file context was resolved");
            return match self.recover_submission_with_store(
                options.idempotency_key,
                payload_hash,
                context.parent,
                deadline,
                cancellation,
            ) {
                Ok((
                    store_uuid,
                    RecoveryResult::Conflict {
                        existing_payload_hash,
                        requested_payload_hash,
                    },
                )) if store_uuid == context.store_uuid => Ok(EnsureOutcome::Conflict {
                    existing_payload_hash,
                    requested_payload_hash,
                }),
                Ok(_) | Err(Error::DeadlineElapsed | Error::Canceled | Error::Unavailable(_)) => {
                    Ok(EnsureOutcome::Unknown)
                }
                Err(
                    Error::Rejected { code, detail } | Error::ManagedWaitRejected { code, detail },
                ) => Ok(EnsureOutcome::Rejected(RejectReason::new(code, detail))),
                Err(error) => Err(error),
            };
        }

        if matches!(&result_file_state, Some(EnsureResultFileState::Existing)) {
            let recovery = match self.recover_result_file(
                options
                    .result_file
                    .as_deref()
                    .expect("existing result file has a path"),
                deadline,
                cancellation,
            ) {
                Ok(recovery) => recovery,
                Err(Error::DeadlineElapsed | Error::Canceled | Error::Unavailable(_)) => {
                    return Ok(EnsureOutcome::Unknown);
                }
                Err(
                    Error::Rejected { code, detail } | Error::ManagedWaitRejected { code, detail },
                ) => {
                    return Ok(EnsureOutcome::Rejected(RejectReason::new(code, detail)));
                }
                Err(error) => return Err(error),
            };
            match recovery {
                RecoveryResult::NotReceived
                    if result_file_context.is_some_and(|context| context.parent.is_some()) => {}
                RecoveryResult::NotReceived => {
                    drop(operation_lock);
                    return Ok(EnsureOutcome::Unknown);
                }
                other => {
                    drop(operation_lock);
                    return self.ensure_job_from_recovery(other, options, deadline, cancellation);
                }
            }
        }

        let result_file_prepared = matches!(&result_file_state, Some(EnsureResultFileState::Fresh));
        let submitted = self.submit_inner(
            spec,
            &submit_options,
            result_file_prepared,
            Some(&payload_hash),
            deadline,
            cancellation,
        );
        drop(operation_lock);
        match submitted {
            Ok(receipt) => self.finish_ensured_job(receipt, options, deadline, cancellation),
            Err(Error::IdempotencyConflict {
                existing_payload_hash,
                requested_payload_hash,
            }) => Ok(EnsureOutcome::Conflict {
                existing_payload_hash,
                requested_payload_hash,
            }),
            Err(Error::Rejected { code, detail })
            | Err(Error::ManagedWaitRejected { code, detail }) => {
                Ok(EnsureOutcome::Rejected(RejectReason::new(code, detail)))
            }
            Err(Error::DeadlineElapsed | Error::Canceled | Error::Unavailable(_)) => {
                Ok(EnsureOutcome::Unknown)
            }
            Err(error) => Err(error),
        }
    }

    /// Atomically ensures one all-or-nothing Batch for the normalized payload and key.
    pub fn ensure_batch(
        &self,
        spec: BatchSpec,
        options: &EnsureOptions,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<EnsureOutcome<EnsuredBatch>> {
        if !self.endpoint_explicit {
            return Err(Error::InvalidSpec(
                "ensure_batch requires an explicitly selected ClientBuilder endpoint".into(),
            ));
        }
        spec.validate()?;
        let operation_lock = match options
            .result_file
            .as_deref()
            .map(|path| lock_ensure_operation(path, deadline, cancellation))
            .transpose()
        {
            Ok(lock) => lock,
            Err(Error::DeadlineElapsed | Error::Canceled | Error::Unavailable(_)) => {
                return Ok(EnsureOutcome::Unknown);
            }
            Err(error) => return Err(error),
        };
        let mut input_refs = BTreeMap::new();
        for member in &spec.jobs {
            if let Some((input, _)) = inspect_stdin(&member.spec.stdin)? {
                input_refs.insert(member.name.clone(), input);
            }
        }
        let payload_hash = batch_hash(&spec, &input_refs)?;
        let submit_options = ensure_submit_options(options);
        let result_file_context = if options.result_file.is_some() {
            match self.submission_context(deadline, cancellation) {
                Ok(context) => Some(context),
                Err(Error::DeadlineElapsed | Error::Canceled | Error::Unavailable(_)) => {
                    return Ok(EnsureOutcome::Unknown);
                }
                Err(
                    Error::Rejected { code, detail } | Error::ManagedWaitRejected { code, detail },
                ) => {
                    return Ok(EnsureOutcome::Rejected(RejectReason::new(code, detail)));
                }
                Err(error) => return Err(error),
            }
        } else {
            None
        };
        let result_file_state = if let Some(path) = &options.result_file {
            match prepare_ensure_result_file(
                path,
                options.idempotency_key,
                &payload_hash,
                &self.endpoint,
                result_file_context.expect("result file context was resolved"),
                deadline,
                cancellation,
            ) {
                Ok(state) => Some(state),
                Err(Error::DeadlineElapsed | Error::Canceled | Error::Unavailable(_)) => {
                    return Ok(EnsureOutcome::Unknown);
                }
                Err(error) => return Err(error),
            }
        } else {
            None
        };
        if matches!(
            &result_file_state,
            Some(EnsureResultFileState::PayloadMismatch { .. })
        ) {
            let context = result_file_context.expect("result file context was resolved");
            return match self.recover_submission_with_store(
                options.idempotency_key,
                payload_hash,
                context.parent,
                deadline,
                cancellation,
            ) {
                Ok((
                    store_uuid,
                    RecoveryResult::Conflict {
                        existing_payload_hash,
                        requested_payload_hash,
                    },
                )) if store_uuid == context.store_uuid => Ok(EnsureOutcome::Conflict {
                    existing_payload_hash,
                    requested_payload_hash,
                }),
                Ok(_) | Err(Error::DeadlineElapsed | Error::Canceled | Error::Unavailable(_)) => {
                    Ok(EnsureOutcome::Unknown)
                }
                Err(
                    Error::Rejected { code, detail } | Error::ManagedWaitRejected { code, detail },
                ) => Ok(EnsureOutcome::Rejected(RejectReason::new(code, detail))),
                Err(error) => Err(error),
            };
        }

        if matches!(&result_file_state, Some(EnsureResultFileState::Existing)) {
            let recovery = match self.recover_result_file(
                options
                    .result_file
                    .as_deref()
                    .expect("existing result file has a path"),
                deadline,
                cancellation,
            ) {
                Ok(recovery) => recovery,
                Err(Error::DeadlineElapsed | Error::Canceled | Error::Unavailable(_)) => {
                    return Ok(EnsureOutcome::Unknown);
                }
                Err(
                    Error::Rejected { code, detail } | Error::ManagedWaitRejected { code, detail },
                ) => {
                    return Ok(EnsureOutcome::Rejected(RejectReason::new(code, detail)));
                }
                Err(error) => return Err(error),
            };
            match recovery {
                RecoveryResult::NotReceived
                    if result_file_context.is_some_and(|context| context.parent.is_some()) => {}
                RecoveryResult::NotReceived => {
                    drop(operation_lock);
                    return Ok(EnsureOutcome::Unknown);
                }
                other => {
                    drop(operation_lock);
                    return self.ensure_batch_from_recovery(other, options, deadline, cancellation);
                }
            }
        }

        let result_file_prepared = matches!(&result_file_state, Some(EnsureResultFileState::Fresh));
        let submitted = self.submit_batch_inner(
            spec,
            &submit_options,
            result_file_prepared,
            Some(&payload_hash),
            deadline,
            cancellation,
        );
        drop(operation_lock);
        match submitted {
            Ok(receipt) => self.finish_ensured_batch(receipt, options, deadline, cancellation),
            Err(Error::IdempotencyConflict {
                existing_payload_hash,
                requested_payload_hash,
            }) => Ok(EnsureOutcome::Conflict {
                existing_payload_hash,
                requested_payload_hash,
            }),
            Err(Error::Rejected { code, detail })
            | Err(Error::ManagedWaitRejected { code, detail }) => {
                Ok(EnsureOutcome::Rejected(RejectReason::new(code, detail)))
            }
            Err(Error::DeadlineElapsed | Error::Canceled | Error::Unavailable(_)) => {
                Ok(EnsureOutcome::Unknown)
            }
            Err(error) => Err(error),
        }
    }

    fn ensure_job_from_recovery(
        &self,
        mut recovery: RecoveryResult,
        options: &EnsureOptions,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<EnsureOutcome<EnsuredJob>> {
        while matches!(recovery, RecoveryResult::Received { .. }) && options.wait_for_completion {
            if Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(Duration::from_millis(25));
            recovery = match self.recover_result_file(
                options
                    .result_file
                    .as_deref()
                    .expect("recovery requires result file"),
                deadline,
                cancellation,
            ) {
                Ok(recovery) => recovery,
                Err(Error::DeadlineElapsed | Error::Canceled) => break,
                Err(Error::Unavailable(_)) => return Ok(EnsureOutcome::Unknown),
                Err(error) => return Err(error),
            };
        }
        match recovery {
            RecoveryResult::Received { submission_id } => {
                Ok(EnsureOutcome::Pending(SubmissionRef::new(submission_id)))
            }
            RecoveryResult::Accepted(receipt) => {
                self.finish_ensured_job(receipt, options, deadline, cancellation)
            }
            RecoveryResult::AcceptedBatch(_) => Err(Error::Protocol(
                "idempotency key belongs to a Batch, not a Job".into(),
            )),
            RecoveryResult::Rejected { code, detail } => {
                Ok(EnsureOutcome::Rejected(RejectReason::new(code, detail)))
            }
            RecoveryResult::Conflict {
                existing_payload_hash,
                requested_payload_hash,
            } => Ok(EnsureOutcome::Conflict {
                existing_payload_hash,
                requested_payload_hash,
            }),
            RecoveryResult::Unknown | RecoveryResult::NotReceived => Ok(EnsureOutcome::Unknown),
        }
    }

    fn ensure_batch_from_recovery(
        &self,
        mut recovery: RecoveryResult,
        options: &EnsureOptions,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<EnsureOutcome<EnsuredBatch>> {
        while matches!(recovery, RecoveryResult::Received { .. }) && options.wait_for_completion {
            if Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(Duration::from_millis(25));
            recovery = match self.recover_result_file(
                options
                    .result_file
                    .as_deref()
                    .expect("recovery requires result file"),
                deadline,
                cancellation,
            ) {
                Ok(recovery) => recovery,
                Err(Error::DeadlineElapsed | Error::Canceled) => break,
                Err(Error::Unavailable(_)) => return Ok(EnsureOutcome::Unknown),
                Err(error) => return Err(error),
            };
        }
        match recovery {
            RecoveryResult::Received { submission_id } => {
                Ok(EnsureOutcome::Pending(SubmissionRef::new(submission_id)))
            }
            RecoveryResult::AcceptedBatch(receipt) => {
                self.finish_ensured_batch(receipt, options, deadline, cancellation)
            }
            RecoveryResult::Accepted(_) => Err(Error::Protocol(
                "idempotency key belongs to a Job, not a Batch".into(),
            )),
            RecoveryResult::Rejected { code, detail } => {
                Ok(EnsureOutcome::Rejected(RejectReason::new(code, detail)))
            }
            RecoveryResult::Conflict {
                existing_payload_hash,
                requested_payload_hash,
            } => Ok(EnsureOutcome::Conflict {
                existing_payload_hash,
                requested_payload_hash,
            }),
            RecoveryResult::Unknown | RecoveryResult::NotReceived => Ok(EnsureOutcome::Unknown),
        }
    }

    fn finish_ensured_job(
        &self,
        receipt: JobReceipt,
        options: &EnsureOptions,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<EnsureOutcome<EnsuredJob>> {
        if receipt.job_state == crate::JobState::Final {
            return Ok(
                match self.wait_outcome(receipt.job_id, deadline, cancellation) {
                    WaitOutcome::Final { snapshot, .. } => {
                        EnsureOutcome::Final(EnsuredJob::new(receipt).with_snapshot(*snapshot))
                    }
                    WaitOutcome::Pending { .. } | WaitOutcome::Unavailable { .. } => {
                        EnsureOutcome::Pending(submission_ref_for_job(&receipt))
                    }
                    WaitOutcome::GapOrUnknown { .. } => EnsureOutcome::Unknown,
                },
            );
        }
        if !options.wait_for_completion {
            return Ok(EnsureOutcome::Accepted(EnsuredJob::new(receipt)));
        }
        match self.wait_outcome(receipt.job_id, deadline, cancellation) {
            WaitOutcome::Final { snapshot, .. } => Ok(EnsureOutcome::Final(
                EnsuredJob::new(receipt).with_snapshot(*snapshot),
            )),
            WaitOutcome::Pending { .. } => {
                Ok(EnsureOutcome::Pending(submission_ref_for_job(&receipt)))
            }
            WaitOutcome::Unavailable { .. } => {
                Ok(EnsureOutcome::Pending(submission_ref_for_job(&receipt)))
            }
            WaitOutcome::GapOrUnknown { .. } => Ok(EnsureOutcome::Unknown),
        }
    }

    fn finish_ensured_batch(
        &self,
        receipt: BatchReceipt,
        options: &EnsureOptions,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<EnsureOutcome<EnsuredBatch>> {
        if !options.wait_for_completion {
            let all_final = receipt
                .jobs
                .iter()
                .all(|member| member.receipt.job_state == crate::JobState::Final);
            if !all_final {
                return Ok(EnsureOutcome::Accepted(EnsuredBatch::new(receipt)));
            }
        }
        let mut snapshots = Vec::with_capacity(receipt.jobs.len());
        for member in &receipt.jobs {
            match self.wait_outcome(member.receipt.job_id, deadline, cancellation) {
                WaitOutcome::Final { snapshot, .. } => snapshots.push(*snapshot),
                WaitOutcome::Pending { .. } => {
                    return Ok(EnsureOutcome::Pending(submission_ref_for_batch(&receipt)));
                }
                WaitOutcome::Unavailable { .. } => {
                    return Ok(EnsureOutcome::Pending(submission_ref_for_batch(&receipt)));
                }
                WaitOutcome::GapOrUnknown { .. } => return Ok(EnsureOutcome::Unknown),
            }
        }
        Ok(EnsureOutcome::Final(
            EnsuredBatch::new(receipt).with_snapshots(snapshots),
        ))
    }

    fn upload_stdin(
        &self,
        path: &Path,
        expected: &StagedInputRef,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<StagedInputRef> {
        const CHUNK_BYTES: usize = 256 * 1024;

        let upload_id = uuid::Uuid::now_v7();
        let mut offset = match self.request(
            Request::StageBegin {
                upload_id,
                expected_sha256: expected.sha256.clone(),
                expected_length: expected.length,
            },
            deadline,
            cancellation,
        )? {
            Response::StageReady { next_offset } => next_offset,
            response => return response_error(response),
        };
        let mut input = std::fs::File::open(path)?;
        input.seek(SeekFrom::Start(offset))?;
        let mut buffer = vec![0_u8; CHUNK_BYTES];
        while offset < expected.length {
            check_wait(deadline, cancellation)?;
            let remaining = usize::try_from(expected.length - offset)
                .unwrap_or(usize::MAX)
                .min(buffer.len());
            let read = input.read(&mut buffer[..remaining])?;
            if read == 0 {
                return Err(Error::InvalidSpec(format!(
                    "stdin file changed or was truncated during upload: {}",
                    path.display()
                )));
            }
            offset = match self.request(
                Request::StageChunk {
                    upload_id,
                    offset,
                    bytes: buffer[..read].to_vec(),
                },
                deadline,
                cancellation,
            )? {
                Response::StageReady { next_offset } => next_offset,
                response => return response_error(response),
            };
        }
        if input.read(&mut [0_u8; 1])? != 0 {
            return Err(Error::InvalidSpec(format!(
                "stdin file grew during upload: {}",
                path.display()
            )));
        }
        match self.request(Request::StageCommit { upload_id }, deadline, cancellation)? {
            Response::StageCommitted { input } if input == *expected => Ok(input),
            Response::StageCommitted { .. } => Err(Error::Protocol(
                "daemon committed different stdin metadata".into(),
            )),
            response => response_error(response),
        }
    }

    pub fn recover_submission(
        &self,
        idempotency_key: uuid::Uuid,
        payload_hash: impl Into<String>,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<RecoveryResult> {
        let context = self.submission_context(deadline, cancellation)?;
        self.recover_submission_with_store(
            idempotency_key,
            payload_hash.into(),
            context.parent,
            deadline,
            cancellation,
        )
        .map(|(_, recovery)| recovery)
    }

    fn recover_submission_with_store(
        &self,
        idempotency_key: uuid::Uuid,
        payload_hash: String,
        expected_parent: Option<ManagedParent>,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<(uuid::Uuid, RecoveryResult)> {
        match self.request(
            Request::Recover {
                idempotency_key,
                payload_hash,
                expected_parent,
            },
            deadline,
            cancellation,
        )? {
            Response::Recovered {
                store_uuid,
                recovery,
            } => Ok((store_uuid, recovery)),
            response => response_error(response),
        }
    }

    pub fn recover_result_file(
        &self,
        path: &Path,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<RecoveryResult> {
        let record: ResultFileRecord = serde_json::from_reader(std::fs::File::open(path)?)?;
        if record.version != RESULT_FILE_VERSION {
            return Err(Error::Protocol(format!(
                "unsupported result-file version {}",
                record.version
            )));
        }
        if !endpoints_equal(&record.endpoint, &self.endpoint) {
            return Err(Error::Protocol(format!(
                "result file belongs to endpoint {:?}, connected to {:?}",
                record.endpoint, self.endpoint
            )));
        }
        let context = self.submission_context(deadline, cancellation)?;
        if context.store_uuid != record.store_uuid {
            return Err(Error::Protocol(format!(
                "result file belongs to store {}, connected to {}",
                record.store_uuid, context.store_uuid
            )));
        }
        if context.parent != record.parent {
            return Err(Error::Protocol(
                "result file managed parent does not match the current authenticated caller".into(),
            ));
        }
        let (store_uuid, recovery) = self.recover_submission_with_store(
            record.idempotency_key,
            record.payload_hash.clone(),
            record.parent,
            deadline,
            cancellation,
        )?;
        if store_uuid != record.store_uuid {
            return Err(Error::Protocol(format!(
                "result file belongs to store {}, connected to {}",
                record.store_uuid, store_uuid
            )));
        }
        persist_recovery(path, &record, &recovery, deadline, cancellation)?;
        Ok(recovery)
    }

    /// Returns the store and server-authenticated managed parent for this client process.
    ///
    /// `parent` is `None` only when the daemon can establish that the caller is not in a managed
    /// Stillyard Containment. Peer-inspection failures, ambiguous containment, and mismatched
    /// inherited coordinates are returned as errors rather than downgraded to an unmanaged
    /// context.
    pub fn submission_context(
        &self,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<SubmissionContext> {
        match self.request(
            Request::SubmissionContext {
                claimed_parent: self.claimed_parent,
            },
            deadline,
            cancellation,
        )? {
            Response::SubmissionContext(context) => Ok(context),
            response => response_error(response),
        }
    }

    pub fn status(
        &self,
        job_id: JobId,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<JobSnapshot> {
        match self.request(Request::Status { job_id }, deadline, cancellation)? {
            Response::Snapshot(snapshot) => Ok(*snapshot),
            response => response_error(response),
        }
    }

    pub fn list(
        &self,
        selector: JobSelector,
        cursor: Option<JobListCursor>,
        limit: u32,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<JobListPage> {
        match self.request(
            Request::List {
                selector,
                cursor,
                limit: limit.clamp(1, MAX_OBSERVATION_PAGE),
            },
            deadline,
            cancellation,
        )? {
            Response::Listed(page) => Ok(page),
            response => response_error(response),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn tree(
        &self,
        selector: JobSelector,
        root_cursor: Option<JobTreeRootCursor>,
        root_limit: u32,
        node_limit: u32,
        max_depth: Option<u32>,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<JobTreePage> {
        match self.request(
            Request::Tree {
                selector,
                root_cursor,
                root_limit,
                node_limit,
                max_depth,
            },
            deadline,
            cancellation,
        )? {
            Response::Tree(page) => Ok(page),
            response => response_error(response),
        }
    }

    pub fn tree_for_job(
        &self,
        job_id: JobId,
        node_limit: u32,
        max_depth: Option<u32>,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<JobTreePage> {
        match self.request(
            Request::TreeForJob {
                job_id,
                node_limit,
                max_depth,
            },
            deadline,
            cancellation,
        )? {
            Response::Tree(page) => Ok(page),
            response => response_error(response),
        }
    }

    pub fn tree_children(
        &self,
        cursor: JobChildrenCursor,
        node_limit: u32,
        additional_depth: Option<u32>,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<JobChildrenPage> {
        match self.request(
            Request::TreeChildren {
                cursor,
                node_limit,
                additional_depth,
            },
            deadline,
            cancellation,
        )? {
            Response::TreeChildren(page) => Ok(page),
            response => response_error(response),
        }
    }

    pub fn observe(
        &self,
        selector: JobSelector,
        cursor: Option<EventCursor>,
        limit: u32,
        max_wait: Duration,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<ObservationFrame> {
        self.observe_inner(
            selector,
            cursor,
            limit,
            max_wait,
            false,
            deadline,
            cancellation,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn observe_trees(
        &self,
        selector: JobTreeSelector,
        cursor: Option<EventCursor>,
        event_limit: u32,
        root_limit: u32,
        node_limit: u32,
        max_depth: Option<u32>,
        max_wait: Duration,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<TreeObservationFrame> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match self.request(
            Request::ObserveTrees {
                selector,
                cursor,
                event_limit,
                root_limit,
                node_limit,
                max_depth,
                max_wait_millis: max_wait
                    .min(Duration::from_secs(60))
                    .min(remaining)
                    .as_millis()
                    .try_into()
                    .unwrap_or(60_000),
            },
            deadline,
            cancellation,
        )? {
            Response::TreesObserved(frame) => Ok(frame),
            response => response_error(response),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn observe_inner(
        &self,
        selector: JobSelector,
        cursor: Option<EventCursor>,
        limit: u32,
        max_wait: Duration,
        managed_wait: bool,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<ObservationFrame> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match self.request(
            Request::Observe {
                selector,
                cursor,
                limit: limit.clamp(1, MAX_OBSERVATION_PAGE),
                max_wait_millis: max_wait
                    .min(Duration::from_secs(60))
                    .min(remaining)
                    .as_millis()
                    .try_into()
                    .unwrap_or(60_000),
                managed_wait,
            },
            deadline,
            cancellation,
        )? {
            Response::Observed(frame) => Ok(frame),
            response => response_error(response),
        }
    }

    pub fn observation_stream(
        &self,
        selector: JobSelector,
        cursor: Option<EventCursor>,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> ObservationStream {
        ObservationStream {
            client: self.clone(),
            selector,
            cursor,
            deadline,
            cancellation: cancellation.cloned(),
            finished: false,
        }
    }

    pub fn wait_stream(
        &self,
        selector: JobSelector,
        any: bool,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<WaitStream> {
        let page = self.list(
            selector,
            None,
            crate::MAX_WAIT_STREAM_JOBS as u32,
            deadline,
            cancellation,
        )?;
        if page.next_cursor.is_some() {
            return Err(Error::InvalidSpec(format!(
                "wait stream membership exceeds {} Jobs",
                crate::MAX_WAIT_STREAM_JOBS
            )));
        }
        let jobs = page.jobs.iter().map(|job| job.job_id).collect::<Vec<_>>();
        if !jobs.is_empty() {
            self.observe_inner(
                JobSelector::Jobs {
                    job_ids: jobs.clone(),
                },
                Some(page.event_cursor),
                1,
                Duration::ZERO,
                true,
                deadline,
                cancellation,
            )?;
        }
        let mut stream = WaitStream {
            client: self.clone(),
            jobs,
            settled: Default::default(),
            outcomes: Vec::new(),
            pending: Default::default(),
            cursor: page.event_cursor,
            any,
            aggregate_emitted: false,
            finished: false,
            deadline,
            cancellation: cancellation.cloned(),
        };
        stream.refresh_settlements(None)?;
        Ok(stream)
    }

    pub fn follow_logs(
        &self,
        job_id: JobId,
        stream: LogStream,
        offset: u64,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<LogFollower> {
        let page = self.list(
            JobSelector::Jobs {
                job_ids: vec![job_id],
            },
            None,
            1,
            deadline,
            cancellation,
        )?;
        Ok(LogFollower {
            client: self.clone(),
            job_id,
            stream,
            offset,
            cursor: page.event_cursor,
            deadline,
            cancellation: cancellation.cloned(),
            finished: false,
        })
    }

    pub fn cancel(
        &self,
        job_ids: &[JobId],
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<Vec<JobSnapshot>> {
        if job_ids.is_empty() {
            return Err(Error::InvalidSpec(
                "cancel requires at least one explicit Job ID".into(),
            ));
        }
        if job_ids.len() > MAX_CANCEL_JOBS {
            return Err(Error::InvalidSpec(format!(
                "cancel accepts at most {MAX_CANCEL_JOBS} Job IDs per request"
            )));
        }
        match self.request(
            Request::Cancel {
                job_ids: job_ids.to_vec(),
            },
            deadline,
            cancellation,
        )? {
            Response::Canceled { snapshots } => Ok(snapshots),
            response => response_error(response),
        }
    }

    pub fn wait(
        &self,
        job_id: JobId,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<JobSnapshot> {
        match self.wait_outcome(job_id, deadline, cancellation) {
            WaitOutcome::Final { snapshot, .. } => Ok(*snapshot),
            WaitOutcome::Pending {
                reason: PendingReason::ClientCanceled,
            } => Err(Error::Canceled),
            WaitOutcome::Pending { .. } => Err(Error::DeadlineElapsed),
            WaitOutcome::Unavailable { detail } => Err(Error::Unavailable(detail)),
            WaitOutcome::GapOrUnknown { detail } => Err(Error::Protocol(detail)),
        }
    }

    /// Waits without conflating a client deadline with any primary process exit code.
    #[must_use]
    pub fn wait_outcome(
        &self,
        job_id: JobId,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> WaitOutcome {
        loop {
            let response = match self.request(
                Request::Wait {
                    job_id,
                    max_wait_millis: 1_000,
                    claimed_parent: self.claimed_parent,
                },
                deadline,
                cancellation,
            ) {
                Ok(response) => response,
                Err(Error::DeadlineElapsed) => {
                    return WaitOutcome::Pending {
                        reason: PendingReason::ClientDeadline,
                    };
                }
                Err(Error::Canceled) => {
                    return WaitOutcome::Pending {
                        reason: PendingReason::ClientCanceled,
                    };
                }
                Err(Error::Unavailable(detail)) => {
                    return WaitOutcome::Unavailable { detail };
                }
                Err(error) => {
                    return WaitOutcome::GapOrUnknown {
                        detail: error.to_string(),
                    };
                }
            };
            match response {
                Response::Snapshot(snapshot) if snapshot.is_final() => {
                    let root_exit_code = snapshot.root_exit_code;
                    return WaitOutcome::Final {
                        snapshot,
                        root_exit_code,
                    };
                }
                Response::Snapshot(_) => continue,
                response => {
                    let detail = response_error::<()>(response)
                        .expect_err("non-snapshot wait response is an error")
                        .to_string();
                    return WaitOutcome::GapOrUnknown { detail };
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn wait_with_passthrough(
        &self,
        job_id: JobId,
        stdout_offset: &mut u64,
        stderr_offset: &mut u64,
        stdout: &mut impl Write,
        stderr: &mut impl Write,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<JobSnapshot> {
        match self.wait_with_passthrough_outcome(
            job_id,
            stdout_offset,
            stderr_offset,
            stdout,
            stderr,
            deadline,
            cancellation,
        )? {
            WaitOutcome::Final { snapshot, .. } => Ok(*snapshot),
            WaitOutcome::Pending {
                reason: PendingReason::ClientCanceled,
            } => Err(Error::Canceled),
            WaitOutcome::Pending { .. } => Err(Error::DeadlineElapsed),
            WaitOutcome::Unavailable { detail } => Err(Error::Unavailable(detail)),
            WaitOutcome::GapOrUnknown { detail } => Err(Error::Protocol(detail)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn wait_with_passthrough_outcome(
        &self,
        job_id: JobId,
        stdout_offset: &mut u64,
        stderr_offset: &mut u64,
        stdout: &mut impl Write,
        stderr: &mut impl Write,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<WaitOutcome> {
        loop {
            let snapshot = match self.request(
                Request::Wait {
                    job_id,
                    max_wait_millis: 250,
                    claimed_parent: self.claimed_parent,
                },
                deadline,
                cancellation,
            ) {
                Err(Error::DeadlineElapsed) => {
                    return Ok(WaitOutcome::Pending {
                        reason: PendingReason::ClientDeadline,
                    });
                }
                Err(Error::Canceled) => {
                    return Ok(WaitOutcome::Pending {
                        reason: PendingReason::ClientCanceled,
                    });
                }
                Err(Error::Unavailable(detail)) => {
                    return Ok(WaitOutcome::Unavailable { detail });
                }
                Err(error) => {
                    return Ok(WaitOutcome::GapOrUnknown {
                        detail: error.to_string(),
                    });
                }
                Ok(response) => match response {
                    Response::Snapshot(snapshot) => *snapshot,
                    response => {
                        return Ok(WaitOutcome::GapOrUnknown {
                            detail: response_error::<()>(response)
                                .expect_err("non-snapshot wait response is an error")
                                .to_string(),
                        });
                    }
                },
            };
            let stdout_progress = match self.passthrough_stream(
                job_id,
                LogStream::Stdout,
                stdout_offset,
                stdout,
                deadline,
                cancellation,
            ) {
                Ok(progress) => progress,
                Err(error) => match typed_wait_error(&error) {
                    Some(outcome) => return Ok(outcome),
                    None => return Err(error),
                },
            };
            let stderr_progress = match self.passthrough_stream(
                job_id,
                LogStream::Stderr,
                stderr_offset,
                stderr,
                deadline,
                cancellation,
            ) {
                Ok(progress) => progress,
                Err(error) => match typed_wait_error(&error) {
                    Some(outcome) => return Ok(outcome),
                    None => return Err(error),
                },
            };
            if passthrough_is_complete(&snapshot, stdout_progress, stderr_progress) {
                stdout.flush()?;
                stderr.flush()?;
                let root_exit_code = snapshot.root_exit_code;
                return Ok(WaitOutcome::Final {
                    snapshot: Box::new(snapshot),
                    root_exit_code,
                });
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn passthrough_stream(
        &self,
        job_id: JobId,
        stream: LogStream,
        offset: &mut u64,
        output: &mut impl Write,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<StreamProgress> {
        let chunk = self.logs(job_id, stream, *offset, 1024 * 1024, deadline, cancellation)?;
        if let Some(gap) = chunk.gap {
            return Err(Error::Protocol(format!(
                "canonical {stream:?} log gap at offset {}: {gap}",
                *offset
            )));
        }
        let caught_up = chunk.bytes.is_empty() || chunk.eof;
        if !chunk.bytes.is_empty() {
            output.write_all(&chunk.bytes)?;
            output.flush()?;
            *offset = chunk.next_offset;
        }
        Ok(StreamProgress {
            eof: chunk.eof,
            caught_up,
        })
    }

    pub fn submit_and_wait(
        &self,
        spec: JobSpec,
        options: &SubmitOptions,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<JobSnapshot> {
        let receipt = self.submit(
            spec,
            &options.clone().with_wait_for_completion(),
            deadline,
            cancellation,
        )?;
        self.wait(receipt.job_id, deadline, cancellation)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn logs(
        &self,
        job_id: JobId,
        stream: LogStream,
        offset: u64,
        limit: u32,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<LogChunk> {
        match self.request(
            Request::Logs {
                job_id,
                stream,
                offset,
                limit,
            },
            deadline,
            cancellation,
        )? {
            Response::Logs(chunk) => Ok(chunk),
            response => response_error(response),
        }
    }

    pub fn daemon_status(
        &self,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<DaemonSnapshot> {
        match self.request(Request::DaemonStatus {}, deadline, cancellation)? {
            Response::DaemonStatus(status) => Ok(status),
            response => response_error(response),
        }
    }

    pub fn doctor(
        &self,
        cursor: Option<ContainmentIncidentCursor>,
        limit: Option<u32>,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<DoctorSnapshot> {
        match self.request(Request::Doctor { cursor, limit }, deadline, cancellation)? {
            Response::Doctor(snapshot) => Ok(*snapshot),
            response => response_error(response),
        }
    }

    /// Collects every incident from one snapshot-consistent, bounded doctor inventory.
    ///
    /// The traversal uses one monotonic deadline and fails instead of returning a partial result
    /// when [`crate::MAX_COMPLETE_DOCTOR_INCIDENTS`] or [`crate::MAX_COMPLETE_DOCTOR_BYTES`] is
    /// exceeded. Continuations expire after [`crate::DOCTOR_SNAPSHOT_TTL_SECONDS`].
    pub fn doctor_complete(
        &self,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<CompleteDoctorSnapshot> {
        check_wait(deadline, cancellation)?;
        let first = self.doctor(None, Some(MAX_DOCTOR_PAGE), deadline, cancellation)?;
        let total_unresolved = first.incidents.total_unresolved;
        if total_unresolved > MAX_COMPLETE_DOCTOR_INCIDENTS {
            return Err(Error::DoctorIncidentLimit {
                limit: MAX_COMPLETE_DOCTOR_INCIDENTS,
            });
        }

        let store_uuid = first.store.store_uuid;
        let mut cursor = first.incidents.next_cursor;
        let snapshot_uuid = cursor.map(|cursor| cursor.snapshot_uuid);
        let mut incidents = Vec::with_capacity(total_unresolved as usize);
        let mut identities = std::collections::BTreeSet::new();
        let mut prior_order = None;
        let mut serialized_bytes = 0_u64;
        append_complete_doctor_page(
            &mut incidents,
            &mut identities,
            &mut prior_order,
            &mut serialized_bytes,
            first.incidents.incidents.iter().cloned(),
        )?;
        validate_doctor_page_cursor(first.incidents.truncated, cursor)?;

        while let Some(next) = cursor {
            check_wait(deadline, cancellation)?;
            if next.store_uuid != store_uuid || Some(next.snapshot_uuid) != snapshot_uuid {
                return Err(Error::Protocol(
                    "doctor continuation changed snapshot identity".into(),
                ));
            }
            let page = self.doctor(Some(next), Some(MAX_DOCTOR_PAGE), deadline, cancellation)?;
            if page.store.store_uuid != store_uuid
                || page.incidents.total_unresolved != total_unresolved
            {
                return Err(Error::Protocol(
                    "doctor continuation changed store or snapshot total".into(),
                ));
            }
            cursor = page.incidents.next_cursor;
            validate_doctor_page_cursor(page.incidents.truncated, cursor)?;
            append_complete_doctor_page(
                &mut incidents,
                &mut identities,
                &mut prior_order,
                &mut serialized_bytes,
                page.incidents.incidents,
            )?;
            if incidents.len() as u64 > total_unresolved {
                return Err(Error::Protocol(
                    "doctor continuation exceeded its snapshot total".into(),
                ));
            }
        }
        if incidents.len() as u64 != total_unresolved {
            return Err(Error::Protocol(format!(
                "doctor snapshot declared {total_unresolved} incidents but returned {}",
                incidents.len()
            )));
        }
        check_wait(deadline, cancellation)?;

        Ok(CompleteDoctorSnapshot {
            schema_version: first.schema_version,
            observed_unix_millis: first.observed_unix_millis,
            overall: first.overall,
            daemon: first.daemon,
            host: first.host,
            store: first.store,
            checks: first.checks,
            coverage: first.coverage,
            total_unresolved,
            incidents,
            boundaries: first.boundaries,
        })
    }

    pub fn force_clear_containment(
        &self,
        containment_id: ContainmentId,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<ClearContainmentResult> {
        match self.request(
            Request::ForceClearContainment { containment_id },
            deadline,
            cancellation,
        )? {
            Response::ContainmentCleared(result) => Ok(result),
            response => response_error(response),
        }
    }

    fn ping(&self, deadline: Instant, cancellation: Option<&CancellationToken>) -> Result<()> {
        match self.request(Request::Ping {}, deadline, cancellation)? {
            Response::Pong { protocol_version } if protocol_version == PROTOCOL_VERSION => Ok(()),
            Response::Pong { protocol_version } => Err(Error::Protocol(format!(
                "daemon protocol {protocol_version}, client protocol {PROTOCOL_VERSION}"
            ))),
            response => response_error(response),
        }
    }

    fn request(
        &self,
        request: Request,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<Response> {
        check_wait(deadline, cancellation)?;
        let endpoint = self.endpoint.clone();
        let daemon_executable = self.daemon_executable.clone();
        let (sender, receiver) = mpsc::sync_channel(1);
        std::thread::spawn(move || {
            let result = transport_request(&endpoint, &daemon_executable, &request, deadline);
            let _ = sender.send(result);
        });
        if cancellation.is_none() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            return match receiver.recv_timeout(remaining) {
                Ok(result) => result,
                Err(mpsc::RecvTimeoutError::Timeout) => Err(Error::DeadlineElapsed),
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    Err(Error::Unavailable("transport worker stopped".into()))
                }
            };
        }
        loop {
            check_wait(deadline, cancellation)?;
            let remaining = deadline.saturating_duration_since(Instant::now());
            match receiver.recv_timeout(remaining.min(Duration::from_millis(25))) {
                Ok(result) => return result,
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(Error::Unavailable("transport worker stopped".into()));
                }
            }
        }
    }
}

impl Iterator for ObservationStream {
    type Item = Result<ObservationFrame>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        loop {
            let requested = self.cursor;
            let frame = match self.client.observe(
                self.selector.clone(),
                self.cursor,
                MAX_OBSERVATION_PAGE,
                Duration::from_secs(30),
                self.deadline,
                self.cancellation.as_ref(),
            ) {
                Ok(frame) => frame,
                Err(error) => {
                    self.finished = true;
                    return Some(Err(error));
                }
            };
            self.cursor = Some(frame.cursor());
            if matches!(&frame, ObservationFrame::Gap { .. })
                || matches!(&frame, ObservationFrame::Events { events, .. } if !events.is_empty())
                || requested != self.cursor
            {
                return Some(Ok(frame));
            }
            if let Err(error) = check_wait(self.deadline, self.cancellation.as_ref()) {
                self.finished = true;
                return Some(Err(error));
            }
        }
    }
}

impl WaitStream {
    fn refresh_settlements(
        &mut self,
        candidates: Option<&std::collections::BTreeSet<JobId>>,
    ) -> Result<()> {
        for job_id in self.jobs.iter().copied() {
            if self.settled.contains(&job_id)
                || candidates.is_some_and(|candidates| !candidates.contains(&job_id))
            {
                continue;
            }
            let snapshot = self
                .client
                .status(job_id, self.deadline, self.cancellation.as_ref())?;
            if snapshot.is_final() {
                self.settled.insert(job_id);
                if let Some(outcome) = snapshot.outcome {
                    self.outcomes.push(outcome);
                }
                self.pending.push_back(WaitStreamItem::Settlement {
                    snapshot: Box::new(snapshot),
                });
                if self.any {
                    break;
                }
            }
        }
        if !self.aggregate_emitted
            && (self.jobs.is_empty()
                || (self.any && !self.settled.is_empty())
                || self.settled.len() == self.jobs.len())
        {
            let outcome = worst_wait_outcome(self.outcomes.iter().copied());
            self.pending
                .push_back(WaitStreamItem::Aggregate { outcome });
            self.aggregate_emitted = true;
        }
        Ok(())
    }
}

impl Iterator for WaitStream {
    type Item = Result<WaitStreamItem>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        loop {
            if let Some(item) = self.pending.pop_front() {
                return Some(Ok(item));
            }
            if self.aggregate_emitted {
                self.finished = true;
                return None;
            }
            let frame = match self.client.observe_inner(
                JobSelector::Jobs {
                    job_ids: self.jobs.clone(),
                },
                Some(self.cursor),
                MAX_OBSERVATION_PAGE,
                Duration::from_secs(30),
                true,
                self.deadline,
                self.cancellation.as_ref(),
            ) {
                Ok(frame) => frame,
                Err(error) => {
                    self.finished = true;
                    return Some(Err(error));
                }
            };
            self.cursor = frame.cursor();
            let candidates = match &frame {
                ObservationFrame::Gap { .. } => None,
                ObservationFrame::Events { events, .. } if !events.is_empty() => Some(
                    events
                        .iter()
                        .map(|event| event.job_id)
                        .collect::<std::collections::BTreeSet<_>>(),
                ),
                ObservationFrame::Events { .. } => continue,
            };
            if let Err(error) = self.refresh_settlements(candidates.as_ref()) {
                self.finished = true;
                return Some(Err(error));
            }
        }
    }
}

impl Iterator for LogFollower {
    type Item = Result<LogChunk>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        loop {
            let chunk = match self.client.logs(
                self.job_id,
                self.stream,
                self.offset,
                1024 * 1024,
                self.deadline,
                self.cancellation.as_ref(),
            ) {
                Ok(chunk) => chunk,
                Err(error) => {
                    self.finished = true;
                    return Some(Err(error));
                }
            };
            if chunk.gap.is_some() || !chunk.bytes.is_empty() || chunk.eof {
                let unrecoverable_gap = chunk.gap.is_some() && chunk.next_offset == self.offset;
                self.offset = chunk.next_offset;
                self.finished = chunk.eof || unrecoverable_gap;
                return Some(Ok(chunk));
            }
            let frame = match self.client.observe(
                JobSelector::Jobs {
                    job_ids: vec![self.job_id],
                },
                Some(self.cursor),
                MAX_OBSERVATION_PAGE,
                Duration::from_secs(30),
                self.deadline,
                self.cancellation.as_ref(),
            ) {
                Ok(frame) => frame,
                Err(error) => {
                    self.finished = true;
                    return Some(Err(error));
                }
            };
            self.cursor = frame.cursor();
            let changed = matches!(&frame, ObservationFrame::Gap { .. })
                || matches!(&frame, ObservationFrame::Events { events, .. } if !events.is_empty());
            if !changed {
                continue;
            }
        }
    }
}

fn worst_wait_outcome(
    outcomes: impl Iterator<Item = crate::JobOutcome>,
) -> Option<crate::JobOutcome> {
    outcomes.max_by_key(|outcome| match outcome {
        crate::JobOutcome::Succeeded => 0,
        crate::JobOutcome::Skipped => 1,
        crate::JobOutcome::Canceled => 2,
        crate::JobOutcome::Interrupted => 3,
        crate::JobOutcome::TimedOut => 4,
        crate::JobOutcome::Failed => 5,
    })
}

fn passthrough_is_complete(
    snapshot: &JobSnapshot,
    stdout: StreamProgress,
    stderr: StreamProgress,
) -> bool {
    passthrough_state_is_complete(snapshot.is_final(), snapshot.outcome, stdout, stderr)
}

fn typed_wait_error(error: &Error) -> Option<WaitOutcome> {
    match error {
        Error::DeadlineElapsed => Some(WaitOutcome::Pending {
            reason: PendingReason::ClientDeadline,
        }),
        Error::Canceled => Some(WaitOutcome::Pending {
            reason: PendingReason::ClientCanceled,
        }),
        Error::Unavailable(detail) => Some(WaitOutcome::Unavailable {
            detail: detail.clone(),
        }),
        Error::NotFound { detail } | Error::Protocol(detail) => Some(WaitOutcome::GapOrUnknown {
            detail: detail.clone(),
        }),
        _ => None,
    }
}

fn passthrough_state_is_complete(
    is_final: bool,
    outcome: Option<crate::JobOutcome>,
    stdout: StreamProgress,
    stderr: StreamProgress,
) -> bool {
    if !is_final {
        return false;
    }
    if stdout.eof && stderr.eof {
        return true;
    }
    outcome == Some(crate::JobOutcome::Interrupted) && stdout.caught_up && stderr.caught_up
}

fn inspect_stdin(stdin: &crate::StdinSpec) -> Result<Option<(StagedInputRef, PathBuf)>> {
    let crate::StdinSpec::File { path } = stdin else {
        return Ok(None);
    };
    let mut file = std::fs::File::open(path)?;
    let length = file.metadata()?.len();
    if length > MAX_STDIN_BYTES {
        return Err(Error::InvalidSpec(format!(
            "stdin file exceeds the {MAX_STDIN_BYTES}-byte input limit"
        )));
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
    Ok(Some((
        StagedInputRef {
            sha256: format!("{:x}", hash.finalize()),
            length,
        },
        path.clone(),
    )))
}

fn response_error<T>(response: Response) -> Result<T> {
    match response {
        Response::Conflict {
            existing_payload_hash,
            requested_payload_hash,
        } => Err(Error::IdempotencyConflict {
            existing_payload_hash,
            requested_payload_hash,
        }),
        Response::SubmissionRejected { code, message } => Err(Error::Rejected {
            code,
            detail: message,
        }),
        Response::Error { code, message } if code == error_code::INVALID_SPEC => {
            Err(Error::InvalidSpec(message))
        }
        Response::Error { code, message } if code == error_code::NOT_FOUND => {
            Err(Error::NotFound { detail: message })
        }
        Response::Error { code, message }
            if code == error_code::BLOCKED_BY_ANCESTOR || code == error_code::RESOURCE_CAPACITY =>
        {
            Err(Error::ManagedWaitRejected {
                code,
                detail: message,
            })
        }
        Response::Error { code, message }
            if code == error_code::IDEMPOTENCY_CONFLICT
                || code == error_code::REJECTED
                || code.starts_with("child_")
                || code.starts_with("containment_") =>
        {
            Err(Error::Rejected {
                code,
                detail: message,
            })
        }
        Response::Error { code, message } if code == error_code::TREE_CURSOR_STALE => {
            Err(Error::ViewStale { detail: message })
        }
        Response::Error { code, message } if code == error_code::DOCTOR_CURSOR_STALE => {
            Err(Error::ViewStale { detail: message })
        }
        Response::Error { code, .. } if code == error_code::DOCTOR_INCIDENT_LIMIT => {
            Err(Error::DoctorIncidentLimit {
                limit: crate::MAX_COMPLETE_DOCTOR_INCIDENTS,
            })
        }
        Response::Error { code, .. } if code == error_code::DOCTOR_MEMORY_LIMIT => {
            Err(Error::DoctorMemoryLimit {
                limit_bytes: crate::MAX_COMPLETE_DOCTOR_BYTES,
            })
        }
        Response::Error { code, .. } if code == error_code::DOCTOR_SNAPSHOT_CAPACITY => {
            Err(Error::DoctorSnapshotCapacity)
        }
        Response::Error { code, message } if code == error_code::TREE_SCAN_LIMIT => {
            Err(Error::ViewUnavailable { detail: message })
        }
        Response::Error { code, message } => Err(Error::Protocol(format!("{code}: {message}"))),
        _ => Err(Error::Protocol("unexpected response variant".into())),
    }
}

fn validate_doctor_page_cursor(
    truncated: bool,
    cursor: Option<ContainmentIncidentCursor>,
) -> Result<()> {
    if truncated != cursor.is_some() {
        return Err(Error::Protocol(
            "doctor page truncation and continuation cursor disagree".into(),
        ));
    }
    Ok(())
}

fn append_complete_doctor_page(
    incidents: &mut Vec<crate::ContainmentIncidentSnapshot>,
    identities: &mut std::collections::BTreeSet<ContainmentId>,
    prior_order: &mut Option<(u64, ContainmentId)>,
    serialized_bytes: &mut u64,
    page: impl IntoIterator<Item = crate::ContainmentIncidentSnapshot>,
) -> Result<()> {
    for incident in page {
        let order = (incident.incident_sequence, incident.incident_id);
        if prior_order.is_some_and(|prior| prior >= order) {
            return Err(Error::Protocol(
                "doctor snapshot incidents are duplicated or out of order".into(),
            ));
        }
        if !identities.insert(incident.incident_id) {
            return Err(Error::Protocol(
                "doctor snapshot returned an incident more than once".into(),
            ));
        }
        *serialized_bytes = serialized_bytes
            .checked_add(serde_json::to_vec(&incident)?.len() as u64)
            .ok_or(Error::DoctorMemoryLimit {
                limit_bytes: MAX_COMPLETE_DOCTOR_BYTES,
            })?;
        if *serialized_bytes > MAX_COMPLETE_DOCTOR_BYTES {
            return Err(Error::DoctorMemoryLimit {
                limit_bytes: MAX_COMPLETE_DOCTOR_BYTES,
            });
        }
        *prior_order = Some(order);
        incidents.push(incident);
    }
    if incidents.len() as u64 > MAX_COMPLETE_DOCTOR_INCIDENTS {
        return Err(Error::DoctorIncidentLimit {
            limit: MAX_COMPLETE_DOCTOR_INCIDENTS,
        });
    }
    Ok(())
}

fn check_wait(deadline: Instant, cancellation: Option<&CancellationToken>) -> Result<()> {
    if cancellation.is_some_and(CancellationToken::is_canceled) {
        return Err(Error::Canceled);
    }
    if Instant::now() >= deadline {
        return Err(Error::DeadlineElapsed);
    }
    Ok(())
}

#[cfg(windows)]
fn transport_request(
    endpoint: &str,
    daemon_executable: &Path,
    request: &Request,
    deadline: Instant,
) -> Result<Response> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Foundation::ERROR_PIPE_BUSY;
    use windows_sys::Win32::System::Pipes::WaitNamedPipeW;

    let mut pipe = loop {
        match OpenOptions::new().read(true).write(true).open(endpoint) {
            Ok(pipe) => break pipe,
            Err(error) if error.raw_os_error() == Some(ERROR_PIPE_BUSY as i32) => {
                if Instant::now() >= deadline {
                    return Err(Error::DeadlineElapsed);
                }
                let remaining = deadline.saturating_duration_since(Instant::now());
                let timeout = remaining.min(Duration::from_secs(1)).as_millis().max(1) as u32;
                let endpoint: Vec<u16> = OsStr::new(endpoint)
                    .encode_wide()
                    .chain(std::iter::once(0))
                    .collect();
                // SAFETY: endpoint is NUL-terminated and remains alive for the call.
                unsafe { WaitNamedPipeW(endpoint.as_ptr(), timeout) };
            }
            Err(error) => return Err(Error::Unavailable(error.to_string())),
        }
    };
    verify_pipe_server(&pipe, daemon_executable)?;
    write_frame(&mut pipe, request)?;
    read_frame(&mut pipe).map_err(Error::from)
}

#[cfg(windows)]
fn verify_pipe_server(pipe: &std::fs::File, daemon_executable: &Path) -> Result<()> {
    use std::os::windows::io::AsRawHandle;
    use std::path::PathBuf;
    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
    use windows_sys::Win32::System::Pipes::GetNamedPipeServerProcessId;
    use windows_sys::Win32::System::Threading::{
        OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, QueryFullProcessImageNameW,
    };

    let mut pid = 0_u32;
    // SAFETY: pipe owns a live named-pipe handle and pid is writable.
    if unsafe { GetNamedPipeServerProcessId(pipe.as_raw_handle() as HANDLE, &mut pid) } == 0 {
        return Err(Error::Protocol(format!(
            "cannot identify named-pipe server: {}",
            std::io::Error::last_os_error()
        )));
    }
    // SAFETY: the requested access is read-only and pid came from the kernel for this pipe.
    let process = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
    if process.is_null() {
        return Err(Error::Protocol(format!(
            "cannot inspect named-pipe server process {pid}: {}",
            std::io::Error::last_os_error()
        )));
    }
    struct ProcessHandle(HANDLE);
    impl Drop for ProcessHandle {
        fn drop(&mut self) {
            // SAFETY: this guard owns a valid process handle.
            unsafe { CloseHandle(self.0) };
        }
    }
    let process = ProcessHandle(process);
    let mut image = vec![0_u16; 32_768];
    let mut length = image.len() as u32;
    // SAFETY: process is live and the output buffer/length are writable.
    if unsafe { QueryFullProcessImageNameW(process.0, 0, image.as_mut_ptr(), &mut length) } == 0 {
        return Err(Error::Protocol(format!(
            "cannot inspect named-pipe server image: {}",
            std::io::Error::last_os_error()
        )));
    }
    image.truncate(length as usize);
    let server = std::fs::canonicalize(PathBuf::from(String::from_utf16_lossy(&image))).map_err(
        |error| Error::Protocol(format!("cannot resolve named-pipe server image: {error}")),
    )?;
    let expected = std::fs::canonicalize(daemon_executable).map_err(|error| {
        Error::Protocol(format!(
            "cannot resolve expected daemon executable {}: {error}",
            daemon_executable.display()
        ))
    })?;
    if server != expected {
        return Err(Error::Protocol(format!(
            "named-pipe server image mismatch: expected {}, found {}",
            expected.display(),
            server.display()
        )));
    }
    Ok(())
}

#[cfg(not(windows))]
fn transport_request(
    _endpoint: &str,
    _daemon_executable: &Path,
    _request: &Request,
    _deadline: Instant,
) -> Result<Response> {
    Err(Error::UnsupportedPlatform(std::env::consts::OS))
}

#[cfg(windows)]
fn start_daemon(
    executable: &Path,
    store_root: &Path,
    endpoint: &str,
) -> Result<std::process::Child> {
    use std::os::windows::process::CommandExt;
    use std::process::{Command, Stdio};

    const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
    const DETACHED_PROCESS: u32 = 0x0000_0008;
    const CREATE_NO_WINDOW: u32 = 0x0800_0000;
    const CREATE_BREAKAWAY_FROM_JOB: u32 = 0x0100_0000;

    std::fs::create_dir_all(store_root)?;
    Command::new(executable)
        .args(["daemon", "--background-child", "--store"])
        .arg(store_root)
        .args(["--endpoint", endpoint])
        .env_remove("STILLYARD_STORE")
        .env_remove("STILLYARD_ENDPOINT")
        .current_dir(store_root)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .creation_flags(
            CREATE_NEW_PROCESS_GROUP
                | DETACHED_PROCESS
                | CREATE_NO_WINDOW
                | CREATE_BREAKAWAY_FROM_JOB,
        )
        .spawn()
        .map_err(|error| Error::Unavailable(format!("cannot start daemon: {error}")))
}

#[cfg(not(windows))]
fn start_daemon(
    _executable: &Path,
    _store_root: &Path,
    _endpoint: &str,
) -> Result<std::process::Child> {
    Err(Error::UnsupportedPlatform(std::env::consts::OS))
}

fn default_daemon_executable() -> Result<PathBuf> {
    let current = std::env::current_exe()?;
    let is_stillyard = current
        .file_stem()
        .is_some_and(|name| name.eq_ignore_ascii_case("stillyard"));
    if is_stillyard {
        return Ok(current);
    }
    let filename = if cfg!(windows) {
        "stillyard.exe"
    } else {
        "stillyard"
    };
    current
        .parent()
        .map(|parent| parent.join(filename))
        .ok_or_else(|| Error::Unavailable("cannot resolve sibling stillyard daemon".into()))
}

fn managed_environment_coordinates() -> (Option<String>, Option<String>, Option<String>) {
    (
        std::env::var("STILLYARD_JOB_ID").ok(),
        std::env::var("STILLYARD_ATTEMPT").ok(),
        std::env::var("STILLYARD_INVOCATION_ID").ok(),
    )
}

fn select_client_endpoint(
    explicit: Option<String>,
    inherited: Option<String>,
) -> Result<(String, bool)> {
    match (explicit, inherited) {
        (Some(endpoint), _) | (None, Some(endpoint)) => Ok((endpoint, true)),
        (None, None) => Ok((default_endpoint()?, false)),
    }
}

fn claimed_managed_parent_for_endpoint(
    selected_endpoint: &str,
    inherited_endpoint: Option<&str>,
    coordinates: (Option<String>, Option<String>, Option<String>),
) -> Result<Option<ManagedParent>> {
    let (job, attempt, invocation) = coordinates;
    if job.is_none() && attempt.is_none() && invocation.is_none() {
        return Ok(None);
    }
    let inherited_endpoint = inherited_endpoint.ok_or_else(|| {
        Error::Protocol("managed environment has no STILLYARD_ENDPOINT coordinate".into())
    })?;
    validate_endpoint(inherited_endpoint)?;
    let (Some(job), Some(attempt), Some(invocation)) = (job, attempt, invocation) else {
        return Err(Error::Protocol(
            "managed environment has incomplete Job/Attempt/Invocation coordinates".into(),
        ));
    };
    if !endpoints_equal(selected_endpoint, inherited_endpoint) {
        return Ok(None);
    }
    let parent = ManagedParent {
        job_id: job
            .parse()
            .map_err(|_| Error::Protocol("invalid STILLYARD_JOB_ID".into()))?,
        attempt_id: attempt
            .parse()
            .map_err(|_| Error::Protocol("invalid STILLYARD_ATTEMPT".into()))?,
        invocation_id: invocation
            .parse()
            .map_err(|_| Error::Protocol("invalid STILLYARD_INVOCATION_ID".into()))?,
    };
    if parent.job_id.store_uuid() != parent.attempt_id.store_uuid()
        || parent.job_id.store_uuid() != parent.invocation_id.store_uuid()
    {
        return Err(Error::Protocol(
            "managed environment coordinates belong to different stores".into(),
        ));
    }
    Ok(Some(parent))
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum EnsureResultFileState {
    Fresh,
    Existing,
    PayloadMismatch { existing_payload_hash: String },
}

fn ensure_submit_options(options: &EnsureOptions) -> SubmitOptions {
    SubmitOptions {
        idempotency_key: options.idempotency_key,
        result_file: options.result_file.clone(),
        wait_for_completion: options.wait_for_completion,
    }
}

fn submission_ref_for_job(receipt: &JobReceipt) -> SubmissionRef {
    SubmissionRef::new(receipt.submission_id).with_job_ids([receipt.job_id])
}

fn submission_ref_for_batch(receipt: &BatchReceipt) -> SubmissionRef {
    SubmissionRef::new(receipt.submission_id)
        .with_job_ids(receipt.jobs.iter().map(|member| member.receipt.job_id))
        .with_batch_id(receipt.batch_id)
}

fn prepare_ensure_result_file(
    path: &Path,
    idempotency_key: uuid::Uuid,
    payload_hash: &str,
    endpoint: &str,
    context: SubmissionContext,
    deadline: Instant,
    cancellation: Option<&CancellationToken>,
) -> Result<EnsureResultFileState> {
    let proposed = ResultFileRecord {
        version: RESULT_FILE_VERSION,
        idempotency_key,
        payload_hash: payload_hash.to_owned(),
        endpoint: endpoint.to_owned(),
        store_uuid: context.store_uuid,
        parent: context.parent,
        receipt: None,
    };
    with_result_file_lock(path, deadline, cancellation, || {
        match std::fs::File::open(path) {
            Ok(file) => {
                let existing: ResultFileRecord = serde_json::from_reader(file)?;
                validate_result_file_operation(&existing, &proposed).map_err(|detail| {
                    Error::InvalidSpec(format!(
                        "result file {} belongs to a different ensure operation: {detail}",
                        path.display()
                    ))
                })?;
                if existing.payload_hash != proposed.payload_hash {
                    return Ok(EnsureResultFileState::PayloadMismatch {
                        existing_payload_hash: existing.payload_hash,
                    });
                }
                Ok(EnsureResultFileState::Existing)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match write_json_new_atomically(path, &proposed) {
                    Ok(()) => Ok(EnsureResultFileState::Fresh),
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                        let existing: ResultFileRecord =
                            serde_json::from_reader(std::fs::File::open(path)?)?;
                        validate_result_file_operation(&existing, &proposed).map_err(|detail| {
                            Error::InvalidSpec(format!(
                                "result file {} changed during atomic ensure: {detail}",
                                path.display()
                            ))
                        })?;
                        if existing.payload_hash != proposed.payload_hash {
                            return Ok(EnsureResultFileState::PayloadMismatch {
                                existing_payload_hash: existing.payload_hash,
                            });
                        }
                        Ok(EnsureResultFileState::Existing)
                    }
                    Err(error) => Err(Error::Io(error)),
                }
            }
            Err(error) => Err(Error::Io(error)),
        }
    })
}

fn lock_ensure_operation(
    path: &Path,
    deadline: Instant,
    cancellation: Option<&CancellationToken>,
) -> Result<std::fs::File> {
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => std::env::current_dir()?,
    };
    std::fs::create_dir_all(&parent)?;
    crate::filesystem::require_fixed_local_ntfs(&parent)?;
    let mut lock_name = path.as_os_str().to_os_string();
    lock_name.push(".ensure.lock");
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(PathBuf::from(lock_name))?;
    loop {
        check_wait(deadline, cancellation)?;
        match lock.try_lock_exclusive() {
            Ok(()) => return Ok(lock),
            Err(error) if lock_is_contended(&error) => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => return Err(Error::Io(error)),
        }
    }
}

fn validate_result_file_operation(
    current: &ResultFileRecord,
    expected: &ResultFileRecord,
) -> std::result::Result<(), &'static str> {
    if current.version != RESULT_FILE_VERSION || expected.version != RESULT_FILE_VERSION {
        return Err("unsupported result-file version");
    }
    if current.idempotency_key != expected.idempotency_key
        || !endpoints_equal(&current.endpoint, &expected.endpoint)
        || current.store_uuid != expected.store_uuid
        || current.parent != expected.parent
    {
        return Err("identity, key, endpoint, store, or managed parent differs");
    }
    Ok(())
}

fn lock_is_contended(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::WouldBlock
        || (cfg!(windows) && error.raw_os_error() == Some(33))
}

fn prepare_result_file(
    path: &Path,
    options: &SubmitOptions,
    payload_hash: &str,
    endpoint: &str,
    context: SubmissionContext,
    deadline: Instant,
    cancellation: Option<&CancellationToken>,
) -> Result<()> {
    let record = ResultFileRecord {
        version: RESULT_FILE_VERSION,
        idempotency_key: options.idempotency_key,
        payload_hash: payload_hash.to_owned(),
        endpoint: endpoint.to_owned(),
        store_uuid: context.store_uuid,
        parent: context.parent,
        receipt: None,
    };
    with_result_file_lock(
        path,
        deadline,
        cancellation,
        || match write_json_new_atomically(path, &record) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let existing: ResultFileRecord =
                    serde_json::from_reader(std::fs::File::open(path)?)?;
                validate_managed_resubmit(&existing, &record).map_err(|detail| {
                    Error::InvalidSpec(format!(
                        "result file {} cannot authorize managed resubmission: {detail}",
                        path.display()
                    ))
                })
            }
            Err(error) => Err(Error::Io(error)),
        },
    )
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ResultFileRecord {
    version: u32,
    idempotency_key: uuid::Uuid,
    payload_hash: String,
    endpoint: String,
    store_uuid: uuid::Uuid,
    parent: Option<ManagedParent>,
    receipt: Option<RecoveryResult>,
}

fn validate_managed_resubmit(
    existing: &ResultFileRecord,
    proposed: &ResultFileRecord,
) -> std::result::Result<(), &'static str> {
    if existing.version != RESULT_FILE_VERSION {
        return Err("unsupported result-file version");
    }
    if existing.idempotency_key != proposed.idempotency_key
        || existing.payload_hash != proposed.payload_hash
        || !endpoints_equal(&existing.endpoint, &proposed.endpoint)
        || existing.store_uuid != proposed.store_uuid
        || existing.parent != proposed.parent
    {
        return Err("identity, key, or normalized payload differs");
    }
    if proposed.parent.is_none() {
        return Err("an unmanaged submission can never reuse a result file");
    }
    if existing.receipt != Some(RecoveryResult::NotReceived) {
        return Err("the latest durable recovery result is not not_received");
    }
    Ok(())
}

fn persist_recovery(
    path: &Path,
    record: &ResultFileRecord,
    recovery: &RecoveryResult,
    deadline: Instant,
    cancellation: Option<&CancellationToken>,
) -> Result<()> {
    if matches!(recovery, RecoveryResult::Unknown) {
        return Ok(());
    }
    persist_result_receipt(path, record, recovery.clone(), deadline, cancellation)
}

fn persist_submit_decision(
    path: &Path,
    expected: &ResultFileRecord,
    response: &Response,
    deadline: Instant,
    cancellation: Option<&CancellationToken>,
) -> Result<()> {
    let decision = match response {
        Response::Conflict {
            existing_payload_hash,
            requested_payload_hash,
        } => Some(RecoveryResult::Conflict {
            existing_payload_hash: existing_payload_hash.clone(),
            requested_payload_hash: requested_payload_hash.clone(),
        }),
        Response::SubmissionRejected { code, message } => Some(RecoveryResult::Rejected {
            code: code.clone(),
            detail: message.clone(),
        }),
        _ => None,
    };
    if let Some(decision) = decision {
        persist_result_receipt(path, expected, decision, deadline, cancellation)?;
    }
    Ok(())
}

fn persist_result_receipt(
    path: &Path,
    expected: &ResultFileRecord,
    receipt: RecoveryResult,
    deadline: Instant,
    cancellation: Option<&CancellationToken>,
) -> Result<()> {
    with_result_file_lock(path, deadline, cancellation, || {
        let mut current: ResultFileRecord = serde_json::from_reader(std::fs::File::open(path)?)?;
        validate_result_file_identity(&current, expected).map_err(|detail| {
            Error::Protocol(format!(
                "result file {} changed identity while updating it: {detail}",
                path.display()
            ))
        })?;
        if current
            .receipt
            .as_ref()
            .is_some_and(|existing| same_terminal_decision(existing, &receipt))
        {
            // Accepted receipts contain live queue/state estimates. The durable decision is the
            // Submission/Job identity, so a refresh of those volatile fields is a no-op rather
            // than a competing terminal decision or a reason to churn the receipt file.
            return Ok(());
        }
        let may_advance = match current.receipt.as_ref() {
            None => true,
            Some(RecoveryResult::NotReceived) => !matches!(receipt, RecoveryResult::Unknown),
            Some(RecoveryResult::Received { .. }) => matches!(
                receipt,
                RecoveryResult::Received { .. }
                    | RecoveryResult::Accepted(_)
                    | RecoveryResult::AcceptedBatch(_)
                    | RecoveryResult::Rejected { .. }
                    | RecoveryResult::Conflict { .. }
            ),
            Some(existing) => existing == &receipt,
        };
        if !may_advance {
            return Err(Error::Protocol(
                "result-file update would regress or replace its durable decision".into(),
            ));
        }
        if current.receipt.as_ref() != Some(&receipt) {
            current.receipt = Some(receipt);
            write_json_atomically(path, &current)?;
        }
        Ok(())
    })
}

fn same_terminal_decision(existing: &RecoveryResult, proposed: &RecoveryResult) -> bool {
    match (existing, proposed) {
        (RecoveryResult::Accepted(existing), RecoveryResult::Accepted(proposed)) => {
            existing.submission_id == proposed.submission_id
                && existing.job_id == proposed.job_id
                && existing.parent == proposed.parent
        }
        (RecoveryResult::AcceptedBatch(existing), RecoveryResult::AcceptedBatch(proposed)) => {
            existing.submission_id == proposed.submission_id
                && existing.batch_id == proposed.batch_id
                && existing.jobs.len() == proposed.jobs.len()
                && existing
                    .jobs
                    .iter()
                    .zip(&proposed.jobs)
                    .all(|(existing, proposed)| {
                        existing.name == proposed.name
                            && existing.receipt.submission_id == proposed.receipt.submission_id
                            && existing.receipt.job_id == proposed.receipt.job_id
                            && existing.receipt.parent == proposed.receipt.parent
                    })
        }
        (RecoveryResult::Rejected { .. }, RecoveryResult::Rejected { .. })
        | (RecoveryResult::Conflict { .. }, RecoveryResult::Conflict { .. }) => {
            existing == proposed
        }
        _ => false,
    }
}

fn validate_result_file_identity(
    current: &ResultFileRecord,
    expected: &ResultFileRecord,
) -> std::result::Result<(), &'static str> {
    if current.version != RESULT_FILE_VERSION || expected.version != RESULT_FILE_VERSION {
        return Err("unsupported result-file version");
    }
    if current.idempotency_key != expected.idempotency_key
        || current.payload_hash != expected.payload_hash
        || !endpoints_equal(&current.endpoint, &expected.endpoint)
        || current.store_uuid != expected.store_uuid
        || current.parent != expected.parent
    {
        return Err("identity, key, or normalized payload differs");
    }
    Ok(())
}

fn with_result_file_lock<T>(
    path: &Path,
    deadline: Instant,
    cancellation: Option<&CancellationToken>,
    action: impl FnOnce() -> Result<T>,
) -> Result<T> {
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => std::env::current_dir()?,
    };
    std::fs::create_dir_all(&parent)?;
    crate::filesystem::require_fixed_local_ntfs(&parent)?;
    let mut lock_name = path.as_os_str().to_os_string();
    lock_name.push(".lock");
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(PathBuf::from(lock_name))?;
    loop {
        check_wait(deadline, cancellation)?;
        match lock.try_lock_exclusive() {
            Ok(()) => break,
            Err(error) if lock_is_contended(&error) => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => return Err(Error::Io(error)),
        }
    }
    let result = action();
    let unlock = FileExt::unlock(&lock).map_err(Error::Io);
    match (result, unlock) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Ok(value), Ok(())) => Ok(value),
    }
}

#[cfg(test)]
fn write_result_file(
    path: &Path,
    idempotency_key: uuid::Uuid,
    payload_hash: &str,
    endpoint: &str,
    context: SubmissionContext,
    receipt: Option<RecoveryResult>,
) -> Result<()> {
    write_json_atomically(
        path,
        &ResultFileRecord {
            version: RESULT_FILE_VERSION,
            idempotency_key,
            payload_hash: payload_hash.to_owned(),
            endpoint: endpoint.to_owned(),
            store_uuid: context.store_uuid,
            parent: context.parent,
            receipt,
        },
    )
}

fn write_json_atomically(path: &Path, value: &impl serde::Serialize) -> Result<()> {
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => std::env::current_dir()?,
    };
    std::fs::create_dir_all(&parent)?;
    crate::filesystem::require_fixed_local_ntfs(&parent)?;
    let temp = parent.join(format!(".stillyard-result-{}.tmp", uuid::Uuid::now_v7()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    drop(file);
    let result = replace_file_atomically(&temp, path);
    if result.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    result
}

fn write_json_new_atomically(path: &Path, value: &impl serde::Serialize) -> std::io::Result<()> {
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => std::env::current_dir()?,
    };
    std::fs::create_dir_all(&parent)?;
    crate::filesystem::require_fixed_local_ntfs(&parent).map_err(std::io::Error::other)?;
    let temp = parent.join(format!(".stillyard-result-{}.tmp", uuid::Uuid::now_v7()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        serde_json::to_writer_pretty(&mut file, value).map_err(std::io::Error::other)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        drop(file);
        // Creating a hard link is an atomic create-if-absent operation on the required local
        // NTFS volume. Removing the temporary name cannot invalidate the published receipt.
        std::fs::hard_link(&temp, path)?;
        let _ = std::fs::remove_file(&temp);
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    result
}

#[cfg(windows)]
fn replace_file_atomically(source: &Path, destination: &Path) -> Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
    };

    let source: Vec<u16> = source
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    let destination: Vec<u16> = destination
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    // SAFETY: both strings are NUL-terminated and remain alive for the call.
    let moved = unsafe {
        MoveFileExW(
            source.as_ptr(),
            destination.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if moved == 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

#[cfg(not(windows))]
fn replace_file_atomically(source: &Path, destination: &Path) -> Result<()> {
    std::fs::rename(source, destination)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn response_errors_preserve_known_wire_rejections() {
        for code in [
            error_code::IDEMPOTENCY_CONFLICT,
            error_code::REJECTED,
            "child_claim_not_permitted",
            "containment_identity_unavailable",
        ] {
            assert!(matches!(
                response_error::<()>(Response::Error {
                    code: code.into(),
                    message: "detail".into(),
                }),
                Err(Error::Rejected {
                    code: observed,
                    detail,
                }) if observed == code && detail == "detail"
            ));
        }
        assert!(matches!(
            response_error::<()>(Response::Error {
                code: error_code::NOT_FOUND.into(),
                message: "not found: missing".into(),
            }),
            Err(Error::NotFound { detail }) if detail == "not found: missing"
        ));
        assert!(matches!(
            response_error::<()>(Response::Error {
                code: "unknown_code".into(),
                message: "detail".into(),
            }),
            Err(Error::Protocol(detail)) if detail == "unknown_code: detail"
        ));
        assert!(matches!(
            response_error::<()>(Response::Error {
                code: error_code::DOCTOR_CURSOR_STALE.into(),
                message: "expired".into(),
            }),
            Err(Error::ViewStale { detail }) if detail == "expired"
        ));
        assert!(matches!(
            response_error::<()>(Response::Error {
                code: error_code::DOCTOR_INCIDENT_LIMIT.into(),
                message: "too many".into(),
            }),
            Err(Error::DoctorIncidentLimit { limit })
                if limit == MAX_COMPLETE_DOCTOR_INCIDENTS
        ));
        assert!(matches!(
            response_error::<()>(Response::Error {
                code: error_code::DOCTOR_MEMORY_LIMIT.into(),
                message: "too large".into(),
            }),
            Err(Error::DoctorMemoryLimit { limit_bytes })
                if limit_bytes == MAX_COMPLETE_DOCTOR_BYTES
        ));
        assert!(matches!(
            response_error::<()>(Response::Error {
                code: error_code::DOCTOR_SNAPSHOT_CAPACITY.into(),
                message: "busy".into(),
            }),
            Err(Error::DoctorSnapshotCapacity)
        ));
    }

    #[test]
    fn doctor_complete_distinguishes_deadline_and_cancellation_before_connecting() {
        let client = Client {
            endpoint: "unused".into(),
            daemon_executable: PathBuf::from("unused"),
            claimed_parent: None,
            endpoint_explicit: true,
        };
        assert!(matches!(
            client.doctor_complete(Instant::now(), None),
            Err(Error::DeadlineElapsed)
        ));
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        assert!(matches!(
            client.doctor_complete(Instant::now() + Duration::from_secs(1), Some(&cancellation)),
            Err(Error::Canceled)
        ));
    }

    #[test]
    fn wait_aggregate_uses_the_worst_terminal_outcome() {
        use crate::JobOutcome::{Canceled, Failed, Interrupted, Skipped, Succeeded, TimedOut};

        assert_eq!(worst_wait_outcome([Succeeded].into_iter()), Some(Succeeded));
        assert_eq!(
            worst_wait_outcome([Succeeded, Skipped, Canceled, Interrupted, TimedOut].into_iter()),
            Some(TimedOut)
        );
        assert_eq!(
            worst_wait_outcome([Succeeded, Failed, TimedOut].into_iter()),
            Some(Failed)
        );
        assert_eq!(worst_wait_outcome([].into_iter()), None);
    }

    fn managed_parent(store: uuid::Uuid) -> ManagedParent {
        ManagedParent {
            job_id: crate::JobId::from_parts(store, uuid::Uuid::now_v7()),
            attempt_id: crate::AttemptId::from_parts(store, uuid::Uuid::now_v7()),
            invocation_id: crate::InvocationId::from_parts(store, uuid::Uuid::now_v7()),
        }
    }

    fn coordinates(parent: ManagedParent) -> (Option<String>, Option<String>, Option<String>) {
        (
            Some(parent.job_id.to_string()),
            Some(parent.attempt_id.to_string()),
            Some(parent.invocation_id.to_string()),
        )
    }

    #[test]
    fn managed_coordinates_are_scoped_to_the_inherited_endpoint() {
        let parent = managed_parent(uuid::Uuid::now_v7());
        let inherited = if cfg!(windows) {
            r"\\.\pipe\stillyard-parent"
        } else {
            "/tmp/stillyard-parent.sock"
        };
        let isolated = if cfg!(windows) {
            r"\\.\pipe\stillyard-isolated"
        } else {
            "/tmp/stillyard-isolated.sock"
        };
        assert_eq!(
            claimed_managed_parent_for_endpoint(inherited, Some(inherited), coordinates(parent),)
                .unwrap(),
            Some(parent)
        );
        assert_eq!(
            claimed_managed_parent_for_endpoint(isolated, Some(inherited), coordinates(parent),)
                .unwrap(),
            None
        );
    }

    #[test]
    fn explicit_and_inherited_client_endpoints_are_connect_only() {
        let (selected, connect_only) =
            select_client_endpoint(Some("explicit".into()), Some("inherited".into())).unwrap();
        assert_eq!(selected, "explicit");
        assert!(connect_only);

        let (selected, connect_only) =
            select_client_endpoint(None, Some("inherited".into())).unwrap();
        assert_eq!(selected, "inherited");
        assert!(connect_only);
    }

    #[test]
    fn unmanaged_client_selects_the_public_default_endpoint() {
        let expected = default_instance().unwrap();
        let (selected, connect_only) = select_client_endpoint(None, None).unwrap();
        assert_eq!(selected, expected.endpoint);
        assert!(!connect_only);
    }

    #[test]
    fn incomplete_managed_environment_fails_even_for_another_endpoint() {
        let inherited = if cfg!(windows) {
            r"\\.\pipe\stillyard-parent"
        } else {
            "/tmp/stillyard-parent.sock"
        };
        let isolated = if cfg!(windows) {
            r"\\.\pipe\stillyard-isolated"
        } else {
            "/tmp/stillyard-isolated.sock"
        };
        assert!(matches!(
            claimed_managed_parent_for_endpoint(
                isolated,
                Some(inherited),
                (Some("job".into()), None, None),
            ),
            Err(Error::Protocol(_))
        ));
    }

    #[cfg(windows)]
    #[test]
    fn explicit_windows_endpoints_are_local_single_component_pipe_names() {
        assert!(validate_endpoint(r"\\.\pipe\moot-test-123").is_ok());
        for invalid in [
            r"\\server\pipe\moot-test-123".to_owned(),
            r"\\.\pipe\nested\name".to_owned(),
            r"\\.\pipe\".to_owned(),
            format!(r"\\.\pipe\{}", "x".repeat(248)),
            r"\\.\pipe\stillyard-демон".to_owned(),
        ] {
            assert!(matches!(
                validate_endpoint(&invalid),
                Err(Error::InvalidSpec(_))
            ));
        }
    }

    #[test]
    fn result_file_fresh_create_is_atomic_and_never_overwrites() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("operation.result.json");
        let first = SubmitOptions::new(uuid::Uuid::now_v7());
        let store_uuid = uuid::Uuid::now_v7();
        let context = SubmissionContext {
            store_uuid,
            parent: None,
        };
        prepare_result_file(
            &path,
            &first,
            "first-hash",
            "pipe-a",
            context,
            Instant::now() + Duration::from_secs(1),
            None,
        )
        .unwrap();
        let before = std::fs::read(&path).unwrap();

        let second = SubmitOptions::new(uuid::Uuid::now_v7());
        assert!(matches!(
            prepare_result_file(
                &path,
                &second,
                "second-hash",
                "pipe-b",
                SubmissionContext {
                    store_uuid: uuid::Uuid::now_v7(),
                    parent: None,
                },
                Instant::now() + Duration::from_secs(1),
                None,
            ),
            Err(Error::InvalidSpec(_))
        ));
        assert_eq!(std::fs::read(&path).unwrap(), before);

        write_result_file(
            &path,
            first.idempotency_key,
            "first-hash",
            "pipe-a",
            context,
            Some(RecoveryResult::NotReceived),
        )
        .unwrap();
        let record: ResultFileRecord =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(record.idempotency_key, first.idempotency_key);
        assert_eq!(record.payload_hash, "first-hash");
        assert_eq!(record.endpoint, "pipe-a");
        assert_eq!(record.store_uuid, store_uuid);
        assert_eq!(record.receipt, Some(RecoveryResult::NotReceived));
    }

    #[test]
    fn ensure_result_file_claim_is_atomic_and_reports_payload_conflict_without_overwrite() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("ensure.result.json");
        let key = uuid::Uuid::now_v7();
        let context = SubmissionContext {
            store_uuid: uuid::Uuid::now_v7(),
            parent: None,
        };
        assert_eq!(
            prepare_ensure_result_file(
                &path,
                key,
                "first",
                "pipe-a",
                context,
                Instant::now() + Duration::from_secs(1),
                None,
            )
            .unwrap(),
            EnsureResultFileState::Fresh
        );
        let first = std::fs::read(&path).unwrap();
        assert_eq!(
            prepare_ensure_result_file(
                &path,
                key,
                "first",
                "pipe-a",
                context,
                Instant::now() + Duration::from_secs(1),
                None,
            )
            .unwrap(),
            EnsureResultFileState::Existing
        );
        assert_eq!(std::fs::read(&path).unwrap(), first);
        assert_eq!(
            prepare_ensure_result_file(
                &path,
                key,
                "second",
                "pipe-a",
                context,
                Instant::now() + Duration::from_secs(1),
                None,
            )
            .unwrap(),
            EnsureResultFileState::PayloadMismatch {
                existing_payload_hash: "first".into(),
            }
        );
        assert_eq!(std::fs::read(&path).unwrap(), first);
        assert!(matches!(
            prepare_ensure_result_file(
                &path,
                uuid::Uuid::now_v7(),
                "first",
                "pipe-a",
                context,
                Instant::now() + Duration::from_secs(1),
                None,
            ),
            Err(Error::InvalidSpec(_))
        ));
        assert_eq!(std::fs::read(&path).unwrap(), first);
    }

    #[test]
    fn ensure_result_file_lock_obeys_the_client_deadline() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("contended.result.json");
        let mut lock_name = path.as_os_str().to_os_string();
        lock_name.push(".lock");
        let ready = temp.path().join("lock-ready");
        let mut holder = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "client::tests::result_file_lock_holder_helper",
                "--nocapture",
            ])
            .env("STILLYARD_TEST_LOCK_PATH", PathBuf::from(lock_name))
            .env("STILLYARD_TEST_LOCK_READY", &ready)
            .spawn()
            .unwrap();
        let ready_deadline = Instant::now() + Duration::from_secs(5);
        while !ready.exists() {
            assert!(
                Instant::now() < ready_deadline,
                "result-file lock helper did not become ready"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        let context = SubmissionContext {
            store_uuid: uuid::Uuid::now_v7(),
            parent: None,
        };
        assert!(matches!(
            prepare_ensure_result_file(
                &path,
                uuid::Uuid::now_v7(),
                "payload",
                "pipe-a",
                context,
                Instant::now() + Duration::from_millis(20),
                None,
            ),
            Err(Error::DeadlineElapsed)
        ));
        let _ = holder.kill();
        let _ = holder.wait();
    }

    #[test]
    #[ignore = "launched as a cross-process result-file lock holder"]
    fn result_file_lock_holder_helper() {
        let lock_path = PathBuf::from(std::env::var_os("STILLYARD_TEST_LOCK_PATH").unwrap());
        let ready = PathBuf::from(std::env::var_os("STILLYARD_TEST_LOCK_READY").unwrap());
        let held = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)
            .unwrap();
        held.lock_exclusive().unwrap();
        std::fs::write(ready, b"ready").unwrap();
        std::thread::sleep(Duration::from_secs(30));
    }

    #[test]
    fn unknown_recovery_preserves_the_last_durable_receipt() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("operation.result.json");
        let record = ResultFileRecord {
            version: RESULT_FILE_VERSION,
            idempotency_key: uuid::Uuid::now_v7(),
            payload_hash: "payload".into(),
            endpoint: "pipe-a".into(),
            store_uuid: uuid::Uuid::now_v7(),
            parent: None,
            receipt: Some(RecoveryResult::Conflict {
                existing_payload_hash: "existing".into(),
                requested_payload_hash: "requested".into(),
            }),
        };
        write_json_atomically(&path, &record).unwrap();
        let before = std::fs::read(&path).unwrap();
        persist_recovery(
            &path,
            &record,
            &RecoveryResult::Unknown,
            Instant::now() + Duration::from_secs(1),
            None,
        )
        .unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), before);
        assert!(matches!(
            persist_recovery(
                &path,
                &record,
                &RecoveryResult::NotReceived,
                Instant::now() + Duration::from_secs(1),
                None,
            ),
            Err(Error::Protocol(_))
        ));
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }

    #[test]
    fn ensure_unknown_is_a_typed_fail_closed_outcome_without_transport_replay() {
        let temp = tempfile::tempdir().unwrap();
        let client = Client {
            endpoint: "unused".into(),
            daemon_executable: temp.path().join("must-not-run.exe"),
            claimed_parent: None,
            endpoint_explicit: true,
        };
        let options = EnsureOptions::new(uuid::Uuid::now_v7())
            .with_result_file(temp.path().join("unknown.result.json"));
        assert_eq!(
            client
                .ensure_job_from_recovery(
                    RecoveryResult::Unknown,
                    &options,
                    Instant::now() + Duration::from_secs(1),
                    None,
                )
                .unwrap(),
            EnsureOutcome::Unknown
        );
        assert!(!client.daemon_executable.exists());
    }

    #[test]
    fn stale_recovery_cannot_overwrite_a_concurrent_accepted_receipt() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("operation.result.json");
        let store_uuid = uuid::Uuid::now_v7();
        let submission_id = crate::SubmissionId::from_parts(store_uuid, uuid::Uuid::now_v7());
        let stale = ResultFileRecord {
            version: RESULT_FILE_VERSION,
            idempotency_key: uuid::Uuid::now_v7(),
            payload_hash: "payload".into(),
            endpoint: "pipe-a".into(),
            store_uuid,
            parent: None,
            receipt: Some(RecoveryResult::NotReceived),
        };
        write_json_atomically(&path, &stale).unwrap();
        let accepted = RecoveryResult::Accepted(JobReceipt {
            submission_id,
            job_id: crate::JobId::from_parts(store_uuid, uuid::Uuid::now_v7()),
            submission_state: crate::SubmissionState::Accepted,
            job_state: crate::JobState::Pending,
            blockers: Vec::new(),
            queue_rank: Some(1),
            estimate: crate::Estimate::unknown("test"),
            parent: None,
            managed_policy_admission: None,
            gpu_provenance: None,
            admission: None,
            daemon_generation: uuid::Uuid::nil(),
        });
        persist_result_receipt(
            &path,
            &stale,
            accepted.clone(),
            Instant::now() + Duration::from_secs(1),
            None,
        )
        .unwrap();

        assert!(matches!(
            persist_recovery(
                &path,
                &stale,
                &RecoveryResult::Received { submission_id },
                Instant::now() + Duration::from_secs(1),
                None,
            ),
            Err(Error::Protocol(_))
        ));
        let durable: ResultFileRecord =
            serde_json::from_reader(std::fs::File::open(&path).unwrap()).unwrap();
        assert_eq!(durable.receipt, Some(accepted));
    }

    #[test]
    fn accepted_refresh_uses_stable_identity_and_keeps_the_durable_receipt() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("operation.result.json");
        let store_uuid = uuid::Uuid::now_v7();
        let submission_id = crate::SubmissionId::from_parts(store_uuid, uuid::Uuid::now_v7());
        let job_id = crate::JobId::from_parts(store_uuid, uuid::Uuid::now_v7());
        let record = ResultFileRecord {
            version: RESULT_FILE_VERSION,
            idempotency_key: uuid::Uuid::now_v7(),
            payload_hash: "payload".into(),
            endpoint: "pipe-a".into(),
            store_uuid,
            parent: None,
            receipt: Some(RecoveryResult::Accepted(JobReceipt {
                submission_id,
                job_id,
                submission_state: crate::SubmissionState::Accepted,
                job_state: crate::JobState::Pending,
                blockers: Vec::new(),
                queue_rank: Some(1),
                estimate: crate::Estimate::unknown("pending"),
                parent: None,
                managed_policy_admission: None,
                gpu_provenance: None,
                admission: None,
                daemon_generation: uuid::Uuid::nil(),
            })),
        };
        write_json_atomically(&path, &record).unwrap();
        let before = std::fs::read(&path).unwrap();
        let refreshed = RecoveryResult::Accepted(JobReceipt {
            submission_id,
            job_id,
            submission_state: crate::SubmissionState::Accepted,
            job_state: crate::JobState::Final,
            blockers: Vec::new(),
            queue_rank: None,
            estimate: crate::Estimate::unknown("final"),
            parent: None,
            managed_policy_admission: None,
            gpu_provenance: None,
            admission: None,
            daemon_generation: uuid::Uuid::nil(),
        });
        persist_result_receipt(
            &path,
            &record,
            refreshed,
            Instant::now() + Duration::from_secs(1),
            None,
        )
        .unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), before);

        let foreign = RecoveryResult::Accepted(JobReceipt {
            submission_id,
            job_id: crate::JobId::from_parts(store_uuid, uuid::Uuid::now_v7()),
            submission_state: crate::SubmissionState::Accepted,
            job_state: crate::JobState::Final,
            blockers: Vec::new(),
            queue_rank: None,
            estimate: crate::Estimate::unknown("foreign"),
            parent: None,
            managed_policy_admission: None,
            gpu_provenance: None,
            admission: None,
            daemon_generation: uuid::Uuid::nil(),
        });
        assert!(matches!(
            persist_result_receipt(
                &path,
                &record,
                foreign,
                Instant::now() + Duration::from_secs(1),
                None,
            ),
            Err(Error::Protocol(_))
        ));
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }

    #[test]
    fn accepted_batch_refresh_pins_the_complete_member_identity() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("batch.result.json");
        let store_uuid = uuid::Uuid::now_v7();
        let submission_id = crate::SubmissionId::from_parts(store_uuid, uuid::Uuid::now_v7());
        let batch_id = crate::BatchId::from_parts(store_uuid, uuid::Uuid::now_v7());
        let receipt = |job_id, rank| JobReceipt {
            submission_id,
            job_id,
            submission_state: crate::SubmissionState::Accepted,
            job_state: crate::JobState::Pending,
            blockers: Vec::new(),
            queue_rank: Some(rank),
            estimate: crate::Estimate::unknown("pending"),
            parent: None,
            managed_policy_admission: None,
            gpu_provenance: None,
            admission: None,
            daemon_generation: uuid::Uuid::nil(),
        };
        let durable_batch = BatchReceipt {
            submission_id,
            batch_id,
            submission_state: crate::SubmissionState::Accepted,
            jobs: vec![
                crate::BatchJobReceipt {
                    name: "first".into(),
                    receipt: receipt(
                        crate::JobId::from_parts(store_uuid, uuid::Uuid::now_v7()),
                        1,
                    ),
                },
                crate::BatchJobReceipt {
                    name: "second".into(),
                    receipt: receipt(
                        crate::JobId::from_parts(store_uuid, uuid::Uuid::now_v7()),
                        2,
                    ),
                },
            ],
            daemon_generation: uuid::Uuid::nil(),
        };
        let record = ResultFileRecord {
            version: RESULT_FILE_VERSION,
            idempotency_key: uuid::Uuid::now_v7(),
            payload_hash: "batch-payload".into(),
            endpoint: "pipe-a".into(),
            store_uuid,
            parent: None,
            receipt: Some(RecoveryResult::AcceptedBatch(durable_batch.clone())),
        };
        write_json_atomically(&path, &record).unwrap();
        let before = std::fs::read(&path).unwrap();

        let mut refreshed = durable_batch.clone();
        for member in &mut refreshed.jobs {
            member.receipt.job_state = crate::JobState::Final;
            member.receipt.queue_rank = None;
            member.receipt.estimate = crate::Estimate::unknown("final");
        }
        persist_result_receipt(
            &path,
            &record,
            RecoveryResult::AcceptedBatch(refreshed),
            Instant::now() + Duration::from_secs(1),
            None,
        )
        .unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), before);

        let mut foreign_batch = durable_batch.clone();
        foreign_batch.batch_id = crate::BatchId::from_parts(store_uuid, uuid::Uuid::now_v7());
        let mut truncated = durable_batch.clone();
        truncated.jobs.pop();
        let mut foreign_member = durable_batch;
        foreign_member.jobs[1].receipt.job_id =
            crate::JobId::from_parts(store_uuid, uuid::Uuid::now_v7());
        for mutant in [foreign_batch, truncated, foreign_member] {
            assert!(matches!(
                persist_result_receipt(
                    &path,
                    &record,
                    RecoveryResult::AcceptedBatch(mutant),
                    Instant::now() + Duration::from_secs(1),
                    None,
                ),
                Err(Error::Protocol(_))
            ));
            assert_eq!(std::fs::read(&path).unwrap(), before);
        }
    }

    #[test]
    fn only_exact_managed_not_received_receipt_authorizes_result_file_reuse() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("managed.result.json");
        let store_uuid = uuid::Uuid::now_v7();
        let parent = managed_parent(store_uuid);
        let options = SubmitOptions::new(uuid::Uuid::now_v7());
        let record = ResultFileRecord {
            version: RESULT_FILE_VERSION,
            idempotency_key: options.idempotency_key,
            payload_hash: "payload".into(),
            endpoint: "pipe-a".into(),
            store_uuid,
            parent: Some(parent),
            receipt: Some(RecoveryResult::NotReceived),
        };
        write_json_atomically(&path, &record).unwrap();
        let before = std::fs::read(&path).unwrap();
        prepare_result_file(
            &path,
            &options,
            "payload",
            "pipe-a",
            SubmissionContext {
                store_uuid,
                parent: Some(parent),
            },
            Instant::now() + Duration::from_secs(1),
            None,
        )
        .unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), before);

        let mut unknown = record;
        unknown.receipt = Some(RecoveryResult::Unknown);
        write_json_atomically(&path, &unknown).unwrap();
        assert!(matches!(
            prepare_result_file(
                &path,
                &options,
                "payload",
                "pipe-a",
                SubmissionContext {
                    store_uuid,
                    parent: Some(parent),
                },
                Instant::now() + Duration::from_secs(1),
                None,
            ),
            Err(Error::InvalidSpec(_))
        ));

        unknown.receipt = Some(RecoveryResult::NotReceived);
        write_json_atomically(&path, &unknown).unwrap();
        assert!(matches!(
            prepare_result_file(
                &path,
                &options,
                "payload",
                "pipe-a",
                SubmissionContext {
                    store_uuid,
                    parent: Some(managed_parent(store_uuid)),
                },
                Instant::now() + Duration::from_secs(1),
                None,
            ),
            Err(Error::InvalidSpec(_))
        ));

        persist_submit_decision(
            &path,
            &unknown,
            &Response::Conflict {
                existing_payload_hash: "existing".into(),
                requested_payload_hash: "payload".into(),
            },
            Instant::now() + Duration::from_secs(1),
            None,
        )
        .unwrap();
        let conflicted: ResultFileRecord =
            serde_json::from_reader(std::fs::File::open(&path).unwrap()).unwrap();
        assert_eq!(
            conflicted.receipt,
            Some(RecoveryResult::Conflict {
                existing_payload_hash: "existing".into(),
                requested_payload_hash: "payload".into(),
            })
        );

        unknown.receipt = None;
        write_json_atomically(&path, &unknown).unwrap();
        persist_submit_decision(
            &path,
            &unknown,
            &Response::SubmissionRejected {
                code: error_code::REJECTED.into(),
                message: "resolved policy fence is unavailable".into(),
            },
            Instant::now() + Duration::from_secs(1),
            None,
        )
        .unwrap();
        let rejected: ResultFileRecord =
            serde_json::from_reader(std::fs::File::open(&path).unwrap()).unwrap();
        assert_eq!(
            rejected.receipt,
            Some(RecoveryResult::Rejected {
                code: error_code::REJECTED.into(),
                detail: "resolved policy fence is unavailable".into(),
            })
        );

        unknown.receipt = None;
        write_json_atomically(&path, &unknown).unwrap();
        persist_submit_decision(
            &path,
            &unknown,
            &Response::SubmissionRejected {
                code: "blocked_by_ancestor".into(),
                message: "cargo_slots retained by parent".into(),
            },
            Instant::now() + Duration::from_secs(1),
            None,
        )
        .unwrap();
        let rejected: ResultFileRecord =
            serde_json::from_reader(std::fs::File::open(&path).unwrap()).unwrap();
        assert_eq!(
            rejected.receipt,
            Some(RecoveryResult::Rejected {
                code: "blocked_by_ancestor".into(),
                detail: "cargo_slots retained by parent".into(),
            })
        );

        unknown.receipt = None;
        write_json_atomically(&path, &unknown).unwrap();
        persist_submit_decision(
            &path,
            &unknown,
            &Response::SubmissionRejected {
                code: "child_claim_not_permitted".into(),
                message: "cargo_slots=2 exceeds 1".into(),
            },
            Instant::now() + Duration::from_secs(1),
            None,
        )
        .unwrap();
        let rejected: ResultFileRecord =
            serde_json::from_reader(std::fs::File::open(&path).unwrap()).unwrap();
        assert_eq!(
            rejected.receipt,
            Some(RecoveryResult::Rejected {
                code: "child_claim_not_permitted".into(),
                detail: "cargo_slots=2 exceeds 1".into(),
            })
        );
    }

    #[test]
    fn every_result_file_identity_discriminant_blocks_replay() {
        let store_uuid = uuid::Uuid::now_v7();
        let parent = managed_parent(store_uuid);
        let baseline = ResultFileRecord {
            version: RESULT_FILE_VERSION,
            idempotency_key: uuid::Uuid::now_v7(),
            payload_hash: "payload".into(),
            endpoint: "pipe-a".into(),
            store_uuid,
            parent: Some(parent),
            receipt: Some(RecoveryResult::NotReceived),
        };
        for changed in [
            ResultFileRecord {
                idempotency_key: uuid::Uuid::now_v7(),
                ..baseline.clone()
            },
            ResultFileRecord {
                payload_hash: "changed".into(),
                ..baseline.clone()
            },
            ResultFileRecord {
                endpoint: "pipe-b".into(),
                ..baseline.clone()
            },
            ResultFileRecord {
                store_uuid: uuid::Uuid::now_v7(),
                ..baseline.clone()
            },
        ] {
            assert!(validate_managed_resubmit(&baseline, &changed).is_err());
        }
    }

    #[test]
    fn recovery_rejects_a_foreign_endpoint_without_touching_the_receipt() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("operation.result.json");
        let record = ResultFileRecord {
            version: RESULT_FILE_VERSION,
            idempotency_key: uuid::Uuid::now_v7(),
            payload_hash: "payload".into(),
            endpoint: "pipe-a".into(),
            store_uuid: uuid::Uuid::now_v7(),
            parent: None,
            receipt: Some(RecoveryResult::Conflict {
                existing_payload_hash: "existing".into(),
                requested_payload_hash: "requested".into(),
            }),
        };
        write_json_atomically(&path, &record).unwrap();
        let before = std::fs::read(&path).unwrap();
        let client = Client {
            endpoint: "pipe-b".into(),
            daemon_executable: temp.path().join("unused.exe"),
            claimed_parent: None,
            endpoint_explicit: true,
        };
        assert!(matches!(
            client.recover_result_file(&path, Instant::now() + Duration::from_secs(1), None,),
            Err(Error::Protocol(_))
        ));
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }

    #[cfg(windows)]
    #[test]
    fn recovery_accepts_a_case_variant_of_the_same_windows_endpoint() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("operation.result.json");
        let endpoint = format!(r"\\.\pipe\stillyard-result-{}", uuid::Uuid::now_v7());
        let record = ResultFileRecord {
            version: RESULT_FILE_VERSION,
            idempotency_key: uuid::Uuid::now_v7(),
            payload_hash: "payload".into(),
            endpoint: endpoint.to_ascii_uppercase(),
            store_uuid: uuid::Uuid::now_v7(),
            parent: None,
            receipt: None,
        };
        write_json_atomically(&path, &record).unwrap();
        let client = Client {
            endpoint,
            daemon_executable: temp.path().join("unused.exe"),
            claimed_parent: None,
            endpoint_explicit: true,
        };
        assert!(matches!(
            client.recover_result_file(&path, Instant::now() + Duration::from_secs(1), None),
            Err(Error::Unavailable(_))
        ));
    }

    #[test]
    fn recover_missing_result_file_does_not_create_it() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("missing.result.json");
        let client = Client {
            endpoint: "unused".into(),
            daemon_executable: temp.path().join("unused.exe"),
            claimed_parent: None,
            endpoint_explicit: true,
        };
        assert!(
            client
                .recover_result_file(&path, Instant::now() + Duration::from_secs(1), None)
                .is_err()
        );
        assert!(!path.exists());
    }

    #[test]
    fn interrupted_passthrough_stops_at_a_quiescent_unclaimed_prefix() {
        let caught_up = StreamProgress {
            eof: false,
            caught_up: true,
        };
        assert!(passthrough_state_is_complete(
            true,
            Some(crate::JobOutcome::Interrupted),
            caught_up,
            caught_up,
        ));
        assert!(!passthrough_state_is_complete(
            true,
            Some(crate::JobOutcome::Failed),
            caught_up,
            caught_up,
        ));
        assert!(!passthrough_state_is_complete(
            false,
            Some(crate::JobOutcome::Interrupted),
            caught_up,
            caught_up,
        ));
    }
}
