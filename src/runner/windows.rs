use std::collections::BTreeMap;
use std::ffi::{OsStr, c_void};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem::{size_of, zeroed};
use std::os::windows::ffi::OsStrExt;
use std::os::windows::fs::OpenOptionsExt;
use std::os::windows::io::{AsRawHandle, FromRawHandle, RawHandle};
use std::path::{Path, PathBuf};
use std::ptr::{null, null_mut};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};
use windows_sys::Win32::Foundation::{
    CloseHandle, GENERIC_READ, HANDLE, HANDLE_FLAG_INHERIT, INVALID_HANDLE_VALUE,
    SetHandleInformation, WAIT_OBJECT_0, WAIT_TIMEOUT,
};
use windows_sys::Win32::Security::SECURITY_ATTRIBUTES;
use windows_sys::Win32::Storage::FileSystem::{
    CreateFileW, FILE_ATTRIBUTE_NORMAL, FILE_ATTRIBUTE_REPARSE_POINT, FILE_SHARE_READ,
    FILE_SHARE_WRITE, GetFileAttributesW, INVALID_FILE_ATTRIBUTES, OPEN_EXISTING,
};
use windows_sys::Win32::System::IO::{CreateIoCompletionPort, GetQueuedCompletionStatus};
use windows_sys::Win32::System::JobObjects::{
    CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE, JOBOBJECT_ASSOCIATE_COMPLETION_PORT,
    JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
    JobObjectAssociateCompletionPortInformation, JobObjectBasicAccountingInformation,
    JobObjectExtendedLimitInformation, QueryInformationJobObject, SetInformationJobObject,
    TerminateJobObject,
};
use windows_sys::Win32::System::Pipes::CreatePipe;
use windows_sys::Win32::System::Threading::{
    CREATE_NO_WINDOW, CREATE_SUSPENDED, CREATE_UNICODE_ENVIRONMENT, CreateProcessW,
    DeleteProcThreadAttributeList, EXTENDED_STARTUPINFO_PRESENT, GetExitCodeProcess,
    InitializeProcThreadAttributeList, PROC_THREAD_ATTRIBUTE_HANDLE_LIST,
    PROC_THREAD_ATTRIBUTE_JOB_LIST, PROCESS_INFORMATION, QueryFullProcessImageNameW, ResumeThread,
    STARTF_USESTDHANDLES, STARTUPINFOEXW, UpdateProcThreadAttribute, WaitForSingleObject,
};

use crate::store::{PreparedJob, Store, StoreError};
use crate::{
    AttemptVerdict, ExitClassification, InvocationRole, InvocationVerdict, LogStream,
    TerminationReason,
};

#[cfg(test)]
thread_local! {
    static FORCE_PRESTART_FAILURE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static FORCE_IMAGE_MISMATCH: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static FORCE_WAIT_JOB_EMPTY_FAILURE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static FORCE_RESUME_FAILURE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static FORCE_STOPPED_ROOT_WAIT_FAILURE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

struct OwnedHandle(HANDLE);

// SAFETY: OwnedHandle has exclusive ownership and Windows kernel handles may be transferred
// between threads. Drop still closes the value exactly once on the receiving thread.
unsafe impl Send for OwnedHandle {}

impl OwnedHandle {
    fn new(handle: HANDLE) -> std::io::Result<Self> {
        if handle.is_null() || handle == INVALID_HANDLE_VALUE {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(Self(handle))
        }
    }

    fn raw(&self) -> HANDLE {
        self.0
    }

    fn into_raw(mut self) -> HANDLE {
        let handle = self.0;
        self.0 = null_mut();
        handle
    }
}

impl Drop for OwnedHandle {
    fn drop(&mut self) {
        if !self.0.is_null() && self.0 != INVALID_HANDLE_VALUE {
            // SAFETY: the handle is owned and closed exactly once.
            unsafe { CloseHandle(self.0) };
        }
    }
}

struct AttributeList {
    storage: Vec<u8>,
    initialized: bool,
    job: Box<HANDLE>,
    inherited_handles: Box<[HANDLE; 3]>,
}

impl AttributeList {
    fn with_job_and_handles(job: HANDLE, inherited_handles: [HANDLE; 3]) -> std::io::Result<Self> {
        let mut bytes = 0_usize;
        // SAFETY: the documented sizing call accepts a null list and writes the required size.
        unsafe { InitializeProcThreadAttributeList(null_mut(), 2, 0, &mut bytes) };
        if bytes == 0 {
            return Err(std::io::Error::last_os_error());
        }
        let mut list = Self {
            storage: vec![0_u8; bytes],
            initialized: false,
            job: Box::new(job),
            inherited_handles: Box::new(inherited_handles),
        };
        // SAFETY: storage is writable and has the exact size returned by the sizing call.
        if unsafe { InitializeProcThreadAttributeList(list.raw(), 2, 0, &mut bytes) } == 0 {
            return Err(std::io::Error::last_os_error());
        }
        list.initialized = true;
        // SAFETY: the list is initialized and `job` remains alive through CreateProcessW.
        if unsafe {
            UpdateProcThreadAttribute(
                list.raw(),
                0,
                PROC_THREAD_ATTRIBUTE_JOB_LIST as usize,
                (list.job.as_ref() as *const HANDLE).cast::<c_void>(),
                size_of::<HANDLE>(),
                null_mut(),
                null(),
            )
        } == 0
        {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: the handles are inheritable and the boxed array remains alive through
        // CreateProcessW. The explicit list prevents unrelated daemon handles from leaking.
        if unsafe {
            UpdateProcThreadAttribute(
                list.raw(),
                0,
                PROC_THREAD_ATTRIBUTE_HANDLE_LIST as usize,
                list.inherited_handles.as_ptr().cast::<c_void>(),
                size_of::<[HANDLE; 3]>(),
                null_mut(),
                null(),
            )
        } == 0
        {
            return Err(std::io::Error::last_os_error());
        }
        Ok(list)
    }

    fn raw(&mut self) -> *mut c_void {
        self.storage.as_mut_ptr().cast()
    }
}

impl Drop for AttributeList {
    fn drop(&mut self) {
        if self.initialized {
            // SAFETY: a nonempty list was initialized before construction completed.
            unsafe { DeleteProcThreadAttributeList(self.raw()) };
        }
    }
}

#[derive(Default)]
struct RunProgress {
    user_code_released: bool,
    durable_release_authorized: bool,
    release_authorized: bool,
    pre_release_replanned: bool,
    cleanup_proven: bool,
    uncertainty_persisted: bool,
    exit_code: Option<i32>,
    timed_out: bool,
    canceled: bool,
    never_run_reason: Option<String>,
}

enum GuardedRelease {
    Resumed {
        runtime_deadline_unix_millis: Option<i64>,
    },
    Deferred {
        reason: String,
    },
}

struct PendingStop {
    verdict: AttemptVerdict,
    reason: String,
}

type RunResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub(super) fn run_with_wake(
    job: &PreparedJob,
    store: &Arc<Mutex<Store>>,
    endpoint: &str,
    live_containments: &super::LiveContainments,
    host_observation: &crate::host_observation::HostObservationService,
    reconciliation_wake: &super::ReconciliationWake,
) {
    if job.role == InvocationRole::Probe {
        run_probe_with_wake(
            job,
            store,
            endpoint,
            live_containments,
            host_observation,
            reconciliation_wake,
        );
        return;
    }
    let mut progress = RunProgress {
        cleanup_proven: true,
        ..RunProgress::default()
    };
    let primary = match run_inner(
        job,
        store,
        endpoint,
        live_containments,
        host_observation,
        &mut progress,
        reconciliation_wake,
    ) {
        Ok(result) => result,
        Err(error) => {
            finish_failed_invocation(job, store, live_containments, &progress, false);
            report_runner_error(job, error.as_ref());
            return;
        }
    };
    if progress.pre_release_replanned {
        return;
    }
    let mut verdict = if progress.canceled {
        AttemptVerdict::Canceled
    } else if primary.1 {
        AttemptVerdict::TimedOut
    } else if primary.0 == 0 {
        AttemptVerdict::Succeeded
    } else {
        AttemptVerdict::ProcessFailed
    };
    if let Ok(mut locked) = store.lock() {
        match locked
            .mark_invocation_resolved(job, Some(primary.0 as i32), None)
            .and_then(|()| record_primary_result(&mut locked, job, verdict).map(|_| ()))
        {
            Ok(()) => live_containments.clear(job.invocation_id),
            Err(error) => {
                drop(locked);
                finish_completed_invocation(
                    job,
                    store,
                    live_containments,
                    Some(primary.0 as i32),
                    None,
                    verdict,
                );
                report_runner_error(job, &error);
                return;
            }
        }
    } else {
        return;
    }

    if !matches!(verdict, AttemptVerdict::Canceled | AttemptVerdict::TimedOut) {
        for index in 0..job.spec.postconditions.len() {
            match pending_stop_verdict(job, store, false) {
                Ok(Some(stop)) => {
                    verdict = stop.verdict;
                    break;
                }
                Ok(None) => {}
                Err(error) => {
                    verdict = AttemptVerdict::Interrupted;
                    report_runner_error(job, error.as_ref());
                    break;
                }
            }
            let postcondition = match store
                .lock()
                .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))
                .and_then(|mut locked| locked.prepare_postcondition(job, index))
            {
                Ok(postcondition) => postcondition,
                Err(error) => {
                    verdict = AttemptVerdict::PostconditionFailed;
                    report_runner_error(job, &error);
                    break;
                }
            };
            let mut post_progress = RunProgress {
                cleanup_proven: true,
                ..RunProgress::default()
            };
            let result = match run_inner(
                &postcondition,
                store,
                endpoint,
                live_containments,
                host_observation,
                &mut post_progress,
                reconciliation_wake,
            ) {
                Ok(result) => result,
                Err(error) => {
                    finish_failed_invocation(
                        &postcondition,
                        store,
                        live_containments,
                        &post_progress,
                        true,
                    );
                    report_runner_error(&postcondition, error.as_ref());
                    return;
                }
            };
            if post_progress.canceled || result.1 {
                let stop = if post_progress.canceled {
                    AttemptVerdict::Canceled
                } else {
                    AttemptVerdict::TimedOut
                };
                if let Ok(mut locked) = store.lock() {
                    match locked.mark_invocation_resolved(
                        &postcondition,
                        Some(result.0 as i32),
                        None,
                    ) {
                        Ok(()) => live_containments.clear(postcondition.invocation_id),
                        Err(error) => {
                            drop(locked);
                            finish_completed_invocation(
                                &postcondition,
                                store,
                                live_containments,
                                Some(result.0 as i32),
                                None,
                                stop,
                            );
                            report_runner_error(&postcondition, &error);
                            return;
                        }
                    }
                } else {
                    return;
                }
                verdict = stop;
                break;
            }
            let definition = &job.spec.postconditions[index];
            let classification = if definition.accepted_exit_codes.contains(&(result.0 as i32)) {
                ExitClassification::Accepted
            } else if definition.retryable_exit_codes.contains(&(result.0 as i32)) {
                ExitClassification::Retryable
            } else {
                ExitClassification::Failed
            };
            let classified_verdict = match classification {
                ExitClassification::Accepted => verdict,
                ExitClassification::Retryable => AttemptVerdict::PostconditionRetryable,
                ExitClassification::Failed => AttemptVerdict::PostconditionFailed,
            };
            if let Ok(mut locked) = store.lock() {
                match locked.mark_invocation_resolved(
                    &postcondition,
                    Some(result.0 as i32),
                    Some(classification),
                ) {
                    Ok(()) => live_containments.clear(postcondition.invocation_id),
                    Err(error) => {
                        drop(locked);
                        finish_completed_invocation(
                            &postcondition,
                            store,
                            live_containments,
                            Some(result.0 as i32),
                            Some(classification),
                            classified_verdict,
                        );
                        report_runner_error(&postcondition, &error);
                        return;
                    }
                }
            } else {
                return;
            }
            verdict = classified_verdict;
            if classification != ExitClassification::Accepted {
                break;
            }
        }
    }
    if let Ok(mut locked) = store.lock() {
        if let Err(error) = locked.settle_attempt(job, verdict) {
            report_runner_error(job, &error);
        }
    }
}

fn run_probe_with_wake(
    job: &PreparedJob,
    store: &Arc<Mutex<Store>>,
    endpoint: &str,
    live_containments: &super::LiveContainments,
    host_observation: &crate::host_observation::HostObservationService,
    reconciliation_wake: &super::ReconciliationWake,
) {
    let mut progress = RunProgress {
        cleanup_proven: true,
        ..RunProgress::default()
    };
    match run_inner(
        job,
        store,
        endpoint,
        live_containments,
        host_observation,
        &mut progress,
        reconciliation_wake,
    ) {
        Ok((exit_code, timed_out)) => {
            let settled = store
                .lock()
                .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))
                .and_then(|mut locked| locked.settle_probe(job, Some(exit_code as i32), timed_out));
            match settled {
                Ok(()) => live_containments.clear(job.invocation_id),
                Err(error) => report_runner_error(job, &error),
            }
        }
        Err(error) => {
            if progress.cleanup_proven {
                let settled = store
                    .lock()
                    .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))
                    .and_then(|mut locked| {
                        locked.settle_probe(job, progress.exit_code, progress.timed_out)
                    });
                match settled {
                    Ok(()) => live_containments.clear(job.invocation_id),
                    Err(settle_error) => report_runner_error(job, &settle_error),
                }
            } else if !progress.uncertainty_persisted {
                if let Ok(mut locked) = store.lock() {
                    if let Err(persist_error) = locked.mark_probe_uncertain(job, progress.exit_code)
                    {
                        report_runner_error(job, &persist_error);
                    }
                }
            }
            report_runner_error(job, error.as_ref());
        }
    }
}

#[cfg(test)]
fn run(job: &PreparedJob, store: &Arc<Mutex<Store>>, endpoint: &str) {
    let wake: super::ReconciliationWake = Arc::new(|| {});
    let observation = crate::host_observation::HostObservationService::new(Default::default());
    run_with_wake(
        job,
        store,
        endpoint,
        &super::LiveContainments::default(),
        &observation,
        &wake,
    );
}

fn pending_stop_verdict(
    job: &PreparedJob,
    store: &Arc<Mutex<Store>>,
    preserve_readiness_terminal: bool,
) -> RunResult<Option<PendingStop>> {
    let mut locked = store
        .lock()
        .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?;
    if preserve_readiness_terminal {
        if let Some(reason) = locked.pre_resume_defer_reason(job.job_id)? {
            let verdict = if reason.contains("outcome=failed") {
                AttemptVerdict::SafetyFailed
            } else {
                AttemptVerdict::Canceled
            };
            return Ok(Some(PendingStop { verdict, reason }));
        }
    } else if locked.invocation_stop_requested(job.job_id)? {
        return Ok(Some(PendingStop {
            verdict: AttemptVerdict::Canceled,
            reason: "cancel_requested".into(),
        }));
    }
    drop(locked);
    let now: i64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX);
    Ok(job
        .attempt_deadline_unix_millis
        .is_some_and(|deadline| deadline <= now)
        .then_some(PendingStop {
            verdict: AttemptVerdict::TimedOut,
            reason: "attempt_timeout".into(),
        }))
}

fn finish_completed_invocation(
    job: &PreparedJob,
    store: &Arc<Mutex<Store>>,
    live_containments: &super::LiveContainments,
    exit_code: Option<i32>,
    classification: Option<ExitClassification>,
    verdict: AttemptVerdict,
) {
    if let Ok(mut locked) = store.lock() {
        if locked
            .mark_invocation_resolved(job, exit_code, classification)
            .is_ok()
            && (job.role != InvocationRole::Primary
                || record_primary_result(&mut locked, job, verdict).is_ok())
        {
            live_containments.clear(job.invocation_id);
            if let Err(error) = locked.settle_attempt(job, verdict) {
                report_runner_error(job, &error);
            }
        }
    }
}

fn failed_run_verdict(progress: &RunProgress) -> AttemptVerdict {
    if progress.canceled {
        AttemptVerdict::Canceled
    } else if progress.timed_out {
        AttemptVerdict::TimedOut
    } else if progress.user_code_released {
        AttemptVerdict::Interrupted
    } else {
        AttemptVerdict::StartFailed
    }
}

#[cfg(test)]
fn failed_run_classification(progress: &RunProgress) -> (crate::JobOutcome, &'static str) {
    let verdict = failed_run_verdict(progress);
    let outcome = match verdict {
        AttemptVerdict::TimedOut => crate::JobOutcome::TimedOut,
        AttemptVerdict::Interrupted => crate::JobOutcome::Interrupted,
        AttemptVerdict::Canceled => crate::JobOutcome::Canceled,
        _ => crate::JobOutcome::Failed,
    };
    (outcome, verdict.as_str())
}

fn finish_failed_invocation(
    job: &PreparedJob,
    store: &Arc<Mutex<Store>>,
    live_containments: &super::LiveContainments,
    progress: &RunProgress,
    postcondition: bool,
) {
    if let Ok(mut locked) = store.lock() {
        if progress.cleanup_proven {
            if let Some(reason) = progress
                .never_run_reason
                .as_deref()
                .filter(|_| !progress.user_code_released)
            {
                match locked.replan_never_run(job, reason) {
                    Ok(()) => live_containments.clear(job.invocation_id),
                    Err(error) => report_runner_error(job, &error),
                }
                return;
            }
            let failed_verdict = failed_run_verdict(progress);
            let verdict = if postcondition && failed_verdict == AttemptVerdict::StartFailed {
                AttemptVerdict::PostconditionFailed
            } else {
                failed_verdict
            };
            let classification = (verdict == AttemptVerdict::PostconditionFailed)
                .then_some(ExitClassification::Failed);
            if locked
                .mark_invocation_resolved(job, progress.exit_code, classification)
                .is_ok()
                && (job.role != InvocationRole::Primary
                    || record_primary_result(&mut locked, job, verdict).is_ok())
            {
                live_containments.clear(job.invocation_id);
                if let Err(error) = locked.settle_attempt(job, verdict) {
                    report_runner_error(job, &error);
                }
            }
        } else {
            // The unproven path transfers its still-owned Job Object to the reconciler before
            // returning. Never remove that authority merely because the outer settlement
            // observes or retries the durable uncertain transition.
            if !progress.uncertainty_persisted {
                if let Err(error) = persist_uncertain_cleanup(&mut locked, job, progress) {
                    report_runner_error(job, &error);
                }
            }
        }
    }
}

fn persist_uncertain_cleanup(
    store: &mut Store,
    job: &PreparedJob,
    progress: &RunProgress,
) -> crate::store::StoreResult<()> {
    if job.role == InvocationRole::Probe {
        store.mark_probe_uncertain(job, progress.exit_code)
    } else if (job.spec.quiet.is_some() || !job.spec.conditions.is_empty())
        && !progress.release_authorized
    {
        store.mark_pre_release_cleanup_uncertain(job, progress.exit_code)
    } else {
        store.mark_uncertain(job, progress.exit_code, "interrupted")
    }
}

fn record_primary_result(
    store: &mut Store,
    job: &PreparedJob,
    verdict: AttemptVerdict,
) -> crate::store::StoreResult<crate::PrimaryInvocationResult> {
    let (invocation_verdict, termination) = match verdict {
        AttemptVerdict::Succeeded => (InvocationVerdict::Succeeded, TerminationReason::Exited),
        AttemptVerdict::ProcessFailed => {
            (InvocationVerdict::ProcessFailed, TerminationReason::Exited)
        }
        AttemptVerdict::StartFailed => (
            InvocationVerdict::StartFailed,
            TerminationReason::StartFailed,
        ),
        AttemptVerdict::TimedOut => (InvocationVerdict::TimedOut, TerminationReason::Timeout),
        AttemptVerdict::Interrupted => {
            (InvocationVerdict::Interrupted, TerminationReason::Interrupt)
        }
        AttemptVerdict::SafetyFailed => (
            InvocationVerdict::SafetyFailed,
            TerminationReason::SafetyFailure,
        ),
        AttemptVerdict::Canceled => (InvocationVerdict::Canceled, TerminationReason::Cancel),
        AttemptVerdict::PostconditionRetryable | AttemptVerdict::PostconditionFailed => {
            return Err(StoreError::InvalidState(
                "postcondition verdict cannot define the primary Invocation result".into(),
            ));
        }
    };
    store.record_primary_result(job, invocation_verdict, termination)
}

fn authorize_condition_and_resume(
    job: &PreparedJob,
    store: &Arc<Mutex<Store>>,
    host_observation: &crate::host_observation::HostObservationService,
    thread_handle: HANDLE,
    observation: Option<crate::host_observation::ObservationMoment<'_>>,
    progress: &mut RunProgress,
) -> RunResult<GuardedRelease> {
    let mut locked = store
        .lock()
        .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?;
    Ok(
        match locked.authorize_condition_release(job, observation)? {
            crate::store::ReleaseAuthorization::Authorized {
                runtime_deadline_unix_millis,
                evidence_expires_monotonic_millis,
            } => {
                progress.durable_release_authorized = true;
                if let Some(reason) = locked.pre_resume_defer_reason(job.job_id)? {
                    GuardedRelease::Deferred { reason }
                } else {
                    let (wall_now, monotonic_now) = crate::host_observation::observation_clock()?;
                    let discontinuity = observation.is_some_and(|observation| {
                        let wall_delta = wall_now
                            .checked_sub(observation.sample.captured_unix_millis)
                            .and_then(|delta| u64::try_from(delta).ok());
                        let monotonic_delta =
                            monotonic_now.checked_sub(observation.sample.captured_monotonic_millis);
                        wall_delta
                            .zip(monotonic_delta)
                            .is_none_or(|(wall, monotonic)| {
                                wall.abs_diff(monotonic)
                                    > host_observation.release_discontinuity_limit_millis()
                            })
                    });
                    if monotonic_now >= evidence_expires_monotonic_millis || discontinuity {
                        GuardedRelease::Deferred {
                        reason: "Condition/host release evidence expired or the clock changed before resume".into(),
                    }
                    } else {
                        #[cfg(test)]
                        let force_resume_failure = FORCE_RESUME_FAILURE.replace(false);
                        #[cfg(not(test))]
                        let force_resume_failure = false;
                        // SAFETY: the primary remains suspended in its complete Job Object; the
                        // Store mutex orders cancellation/deadline commits after this release point.
                        if force_resume_failure
                            || unsafe { ResumeThread(thread_handle) } == u32::MAX
                        {
                            return Err(std::io::Error::last_os_error().into());
                        }
                        progress.release_authorized = true;
                        GuardedRelease::Resumed {
                            runtime_deadline_unix_millis,
                        }
                    }
                }
            }
            crate::store::ReleaseAuthorization::Deferred { reason } => {
                GuardedRelease::Deferred { reason }
            }
        },
    )
}

fn report_runner_error(job: &PreparedJob, error: &dyn std::error::Error) {
    use std::io::Write as _;
    let _ = writeln!(
        std::io::stderr(),
        "stillyard runner for {} failed: {error}",
        job.job_id
    );
}

fn run_inner(
    job: &PreparedJob,
    store: &Arc<Mutex<Store>>,
    endpoint: &str,
    live_containments: &super::LiveContainments,
    host_observation: &crate::host_observation::HostObservationService,
    progress: &mut RunProgress,
    reconciliation_wake: &super::ReconciliationWake,
) -> RunResult<(u32, bool)> {
    validate_paths(&job.spec.executable, &job.spec.working_directory)?;
    let mut executable_file = OpenOptions::new()
        .read(true)
        .share_mode(FILE_SHARE_READ)
        .open(&job.spec.executable)
        .map_err(|error| io_context("locking executable for launch", error))?;
    let executable_hash = hash_reader(&mut executable_file)?;
    let (job_object, completion_port) = create_job_object()?;
    let registration = live_containments.register(job.invocation_id, job_object.raw())?;
    progress.cleanup_proven = true;
    let (stdout_read, stdout_write) = create_inherited_pipe()?;
    let (stderr_read, stderr_write) = create_inherited_pipe()?;
    let stdin = open_stdin(job)?;
    let mut attributes = AttributeList::with_job_and_handles(
        job_object.raw(),
        [
            stdin.as_raw_handle() as HANDLE,
            stdout_write.raw(),
            stderr_write.raw(),
        ],
    )
    .map_err(|error| io_context("building born-contained attribute list", error))?;

    let application = wide_null(job.spec.executable.as_os_str());
    let mut command_line = command_line(&job.spec.executable, &job.spec.args);
    let working_directory = wide_null(job.spec.working_directory.as_os_str());
    let mut environment = environment_block(job, endpoint)?;
    let mut startup: STARTUPINFOEXW = unsafe { zeroed() };
    startup.StartupInfo.cb = size_of::<STARTUPINFOEXW>() as u32;
    startup.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
    startup.StartupInfo.hStdInput = stdin.as_raw_handle() as HANDLE;
    startup.StartupInfo.hStdOutput = stdout_write.raw();
    startup.StartupInfo.hStdError = stderr_write.raw();
    startup.lpAttributeList = attributes.raw();
    let mut process: PROCESS_INFORMATION = unsafe { zeroed() };

    // SAFETY: all pointers reference initialized, live buffers; inherited standard handles are
    // valid; PROCESS_INFORMATION is writable; the process starts suspended.
    let created = unsafe {
        CreateProcessW(
            application.as_ptr(),
            command_line.as_mut_ptr(),
            null(),
            null(),
            1,
            CREATE_SUSPENDED
                | CREATE_UNICODE_ENVIRONMENT
                | CREATE_NO_WINDOW
                | EXTENDED_STARTUPINFO_PRESENT,
            environment.as_mut_ptr().cast::<c_void>(),
            working_directory.as_ptr(),
            &startup.StartupInfo,
            &mut process,
        )
    };
    if created == 0 {
        return Err(io_context(
            "creating born-contained suspended process",
            std::io::Error::last_os_error(),
        )
        .into());
    }
    progress.cleanup_proven = false;
    macro_rules! prestart_try {
        ($expression:expr) => {
            match $expression {
                Ok(value) => value,
                Err(error) => {
                    // SAFETY: CreateProcessW succeeded suspended inside this Job Object.
                    unsafe { TerminateJobObject(job_object.raw(), 70) };
                    progress.cleanup_proven =
                        wait_job_empty(job_object.raw(), Duration::from_secs(30)).is_ok();
                    if !progress.cleanup_proven {
                        let mut locked = store
                            .lock()
                            .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?;
                        persist_uncertain_cleanup(&mut locked, job, progress)?;
                        progress.uncertainty_persisted = true;
                        live_containments.transfer_to_reconciler(
                            registration,
                            job.invocation_id,
                            job_object.raw(),
                        )?;
                        let _owned_by_reconciler = job_object.into_raw();
                        spawn_boundary_empty_notification(
                            completion_port,
                            Arc::clone(reconciliation_wake),
                        )?;
                    }
                    return Err(error.into());
                }
            }
        };
    }
    #[cfg(test)]
    if FORCE_PRESTART_FAILURE.replace(false) {
        prestart_try!(Err::<(), _>(std::io::Error::other(
            "forced pre-start failure"
        )));
    }
    let process_handle = prestart_try!(OwnedHandle::new(process.hProcess));
    let thread_handle = prestart_try!(OwnedHandle::new(process.hThread));
    drop(stdout_write);
    drop(stderr_write);
    drop(stdin);

    let image_path = prestart_try!(process_image_path(process_handle.raw()));
    prestart_try!(validate_executable(&image_path));
    let image_matches = prestart_try!(same_windows_path(&image_path, &job.spec.executable));
    #[cfg(test)]
    let image_matches = image_matches && !FORCE_IMAGE_MISMATCH.replace(false);
    if !image_matches {
        let error = std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "created image {} differs from requested executable {}",
                image_path.display(),
                job.spec.executable.display()
            ),
        );
        prestart_try!(Err::<(), _>(error));
    }
    drop(executable_file);
    let root_identity = prestart_try!(match (&job.host_id, &job.boot_id) {
        (Some(host_id), Some(boot_id)) => crate::identity::process_identity_from_handle(
            process_handle.raw(),
            process.dwProcessId,
            host_id,
            boot_id,
        ),
        _ => Err(crate::Error::Unavailable(
            "containment process identity is unavailable".into(),
        )),
    });
    if job.spec.quiet.is_some() || !job.spec.conditions.is_empty() {
        let mut store = prestart_try!(
            store
                .lock()
                .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))
        );
        prestart_try!(store.record_suspended_root(
            job,
            process.dwProcessId,
            &executable_hash,
            &root_identity,
        ));
    } else {
        let mut store = prestart_try!(
            store
                .lock()
                .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))
        );
        prestart_try!(store.mark_started_with_identity(
            job,
            process.dwProcessId,
            &executable_hash,
            Some(&root_identity),
        ));
    }

    let stdout_file = unsafe { File::from_raw_handle(stdout_read.into_raw() as RawHandle) };
    let stderr_file = unsafe { File::from_raw_handle(stderr_read.into_raw() as RawHandle) };
    let stdout = prestart_try!(spawn_drain(
        stdout_file,
        job.stdout_path.clone(),
        job.job_id,
        LogStream::Stdout,
        Arc::clone(store),
        job.role == InvocationRole::Primary,
    ));
    let stderr = prestart_try!(spawn_drain(
        stderr_file,
        job.stderr_path.clone(),
        job.job_id,
        LogStream::Stderr,
        Arc::clone(store),
        job.role == InvocationRole::Primary,
    ));

    let execution = (|| -> RunResult<(u32, bool)> {
        let mut runtime_deadline_unix_millis = job.attempt_deadline_unix_millis;
        let stop_before_resume = pending_stop_verdict(
            job,
            store,
            job.role == InvocationRole::Primary
                && (job.spec.quiet.is_some() || !job.spec.conditions.is_empty()),
        )?;
        let mut timed_out = stop_before_resume
            .as_ref()
            .is_some_and(|stop| stop.verdict == AttemptVerdict::TimedOut);
        if let Some(stop) = stop_before_resume {
            progress.canceled = stop.verdict == AttemptVerdict::Canceled;
            progress.timed_out = timed_out;
            if job.spec.quiet.is_some() || !job.spec.conditions.is_empty() {
                progress.never_run_reason = Some(stop.reason.clone());
            }
            let exit_code = if progress.canceled { 22 } else { 21 };
            // SAFETY: the process is still suspended and born-contained. No user code runs
            // after a cancel/deadline that won between preparation and release.
            unsafe { TerminateJobObject(job_object.raw(), exit_code) };
            // SAFETY: process_handle remains valid.
            let root_wait = unsafe { WaitForSingleObject(process_handle.raw(), 30_000) };
            #[cfg(test)]
            let root_wait = if FORCE_STOPPED_ROOT_WAIT_FAILURE.replace(false) {
                WAIT_TIMEOUT
            } else {
                root_wait
            };
            if root_wait != WAIT_OBJECT_0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "stopped suspended root did not exit within cleanup bound",
                )
                .into());
            }
            if job.spec.quiet.is_some() || !job.spec.conditions.is_empty() {
                wait_job_empty(job_object.raw(), Duration::from_secs(30))?;
                progress.cleanup_proven = true;
                store
                    .lock()
                    .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?
                    .replan_never_run(job, &stop.reason)?;
                live_containments.clear(job.invocation_id);
                progress.pre_release_replanned = true;
                return Ok((0, false));
            }
        } else if job.spec.quiet.is_some() {
            let guarded = match host_observation.with_release_sample(
                process.dwProcessId,
                |sample| -> RunResult<GuardedRelease> {
                    let mut locked = store
                        .lock()
                        .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?;
                    let (authorization_wall, authorization_monotonic) =
                        crate::host_observation::observation_clock()?;
                    let authorization = locked.authorize_release(
                        job,
                        crate::host_observation::ObservationMoment {
                            sample,
                            now_unix_millis: authorization_wall,
                            now_monotonic_millis: authorization_monotonic,
                            live_clock: true,
                        },
                    )?;
                    let crate::store::ReleaseAuthorization::Authorized {
                        runtime_deadline_unix_millis,
                        evidence_expires_monotonic_millis,
                    } = authorization
                    else {
                        let crate::store::ReleaseAuthorization::Deferred { reason } = authorization
                        else {
                            unreachable!()
                        };
                        return Ok(GuardedRelease::Deferred { reason });
                    };
                    progress.durable_release_authorized = true;
                    let (wall_now, monotonic_now) = crate::host_observation::observation_clock()?;
                    let wall_delta = wall_now
                        .checked_sub(sample.captured_unix_millis)
                        .and_then(|delta| u64::try_from(delta).ok())
                        .ok_or_else(|| std::io::Error::other("release wall clock regressed"))?;
                    let monotonic_delta = monotonic_now
                        .checked_sub(sample.captured_monotonic_millis)
                        .ok_or_else(|| {
                            std::io::Error::other("release monotonic clock regressed")
                        })?;
                    if let Some(reason) = locked.pre_resume_defer_reason(job.job_id)? {
                        return Ok(GuardedRelease::Deferred { reason });
                    }
                    if monotonic_now >= evidence_expires_monotonic_millis
                        || wall_delta.abs_diff(monotonic_delta)
                            > host_observation.release_discontinuity_limit_millis()
                    {
                        return Ok(GuardedRelease::Deferred {
                            reason:
                                "authorized release evidence expired or clock discontinuity occurred"
                                    .into(),
                        });
                    }
                    // SAFETY: the primary thread is valid, remains suspended, and the release
                    // barrier prevents provider generation changes through this call.
                    #[cfg(test)]
                    let force_resume_failure = FORCE_RESUME_FAILURE.replace(false);
                    #[cfg(not(test))]
                    let force_resume_failure = false;
                    if force_resume_failure
                        || unsafe { ResumeThread(thread_handle.raw()) } == u32::MAX
                    {
                        return Err(std::io::Error::last_os_error().into());
                    }
                    progress.release_authorized = true;
                    Ok(GuardedRelease::Resumed {
                        runtime_deadline_unix_millis,
                    })
                },
            ) {
                Ok(Ok(guarded)) => guarded,
                Ok(Err(error)) if !progress.durable_release_authorized => GuardedRelease::Deferred {
                    reason: format!("pre-release authorization failed: {error}"),
                },
                Err(error) if !progress.durable_release_authorized => GuardedRelease::Deferred {
                    reason: format!("pre-release observation failed: {error}"),
                },
                Ok(Err(error)) => return Err(error),
                Err(error) => return Err(std::io::Error::other(error).into()),
            };
            match guarded {
                GuardedRelease::Resumed {
                    runtime_deadline_unix_millis: deadline,
                } => {
                    runtime_deadline_unix_millis = deadline;
                    progress.user_code_released = true;
                }
                GuardedRelease::Deferred { reason } => {
                    progress.never_run_reason = Some(reason.clone());
                    // SAFETY: user code remains suspended in this complete Job Object.
                    unsafe { TerminateJobObject(job_object.raw(), 70) };
                    if unsafe { WaitForSingleObject(process_handle.raw(), 30_000) } != WAIT_OBJECT_0
                    {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "deferred suspended root did not exit within cleanup bound",
                        )
                        .into());
                    }
                    wait_job_empty(job_object.raw(), Duration::from_secs(30))?;
                    progress.cleanup_proven = true;
                    store
                        .lock()
                        .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?
                        .replan_never_run(job, &reason)?;
                    live_containments.clear(job.invocation_id);
                    progress.pre_release_replanned = true;
                    return Ok((0, false));
                }
            }
        } else if !job.spec.conditions.is_empty() {
            let guarded = if job.spec.requires_host_observation() {
                match host_observation.with_release_sample(process.dwProcessId, |sample| {
                    let (now_unix_millis, now_monotonic_millis) =
                        crate::host_observation::observation_clock()?;
                    authorize_condition_and_resume(
                        job,
                        store,
                        host_observation,
                        thread_handle.raw(),
                        Some(crate::host_observation::ObservationMoment {
                            sample,
                            now_unix_millis,
                            now_monotonic_millis,
                            live_clock: true,
                        }),
                        progress,
                    )
                }) {
                    Ok(result) => result?,
                    Err(error) => GuardedRelease::Deferred {
                        reason: format!("pre-release observation failed: {error}"),
                    },
                }
            } else {
                authorize_condition_and_resume(
                    job,
                    store,
                    host_observation,
                    thread_handle.raw(),
                    None,
                    progress,
                )?
            };
            match guarded {
                GuardedRelease::Resumed {
                    runtime_deadline_unix_millis: deadline,
                } => {
                    runtime_deadline_unix_millis = deadline;
                    progress.user_code_released = true;
                }
                GuardedRelease::Deferred { reason } => {
                    progress.never_run_reason = Some(reason.clone());
                    // SAFETY: no user code has run and the complete tree remains suspended.
                    unsafe { TerminateJobObject(job_object.raw(), 70) };
                    if unsafe { WaitForSingleObject(process_handle.raw(), 30_000) } != WAIT_OBJECT_0
                    {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "deferred suspended root did not exit within cleanup bound",
                        )
                        .into());
                    }
                    wait_job_empty(job_object.raw(), Duration::from_secs(30))?;
                    progress.cleanup_proven = true;
                    store
                        .lock()
                        .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?
                        .replan_never_run(job, &reason)?;
                    live_containments.clear(job.invocation_id);
                    progress.pre_release_replanned = true;
                    return Ok((0, false));
                }
            }
        } else {
            let stop_at_release = {
                let mut locked = store
                    .lock()
                    .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?;
                let stop = if locked.invocation_stop_requested(job.job_id)? {
                    Some(AttemptVerdict::Canceled)
                } else {
                    let now: i64 = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis()
                        .try_into()
                        .unwrap_or(i64::MAX);
                    job.attempt_deadline_unix_millis
                        .is_some_and(|deadline| deadline <= now)
                        .then_some(AttemptVerdict::TimedOut)
                };
                if stop.is_none() {
                    // SAFETY: the thread remains suspended and the store mutex orders a
                    // concurrent cancellation/deadline commit after this release point.
                    if unsafe { ResumeThread(thread_handle.raw()) } == u32::MAX {
                        return Err(std::io::Error::last_os_error().into());
                    }
                    progress.user_code_released = true;
                }
                stop
            };
            if let Some(stop) = stop_at_release {
                progress.canceled = stop == AttemptVerdict::Canceled;
                timed_out = stop == AttemptVerdict::TimedOut;
                progress.timed_out = timed_out;
                let exit_code = if progress.canceled { 22 } else { 21 };
                // SAFETY: no user code was resumed and the complete tree is contained.
                unsafe { TerminateJobObject(job_object.raw(), exit_code) };
                if unsafe { WaitForSingleObject(process_handle.raw(), 30_000) } != WAIT_OBJECT_0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "stopped suspended root did not exit within cleanup bound",
                    )
                    .into());
                }
            }
        }

        let deadline = runtime_deadline_unix_millis.and_then(|deadline| {
            let now: i64 = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .try_into()
                .unwrap_or(i64::MAX);
            let remaining = u64::try_from(deadline.saturating_sub(now)).unwrap_or(0);
            Instant::now().checked_add(Duration::from_millis(remaining))
        });
        if progress.user_code_released {
            loop {
                // SAFETY: process_handle remains valid throughout the wait.
                let wait = unsafe { WaitForSingleObject(process_handle.raw(), 100) };
                if wait == WAIT_OBJECT_0 {
                    break;
                }
                if wait != WAIT_TIMEOUT {
                    return Err(std::io::Error::last_os_error().into());
                }
                if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                    timed_out = true;
                    progress.timed_out = true;
                    // SAFETY: job is valid and contains the complete tree.
                    unsafe { TerminateJobObject(job_object.raw(), 21) };
                    // SAFETY: process_handle remains valid.
                    if unsafe { WaitForSingleObject(process_handle.raw(), 30_000) } != WAIT_OBJECT_0
                    {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "terminated root did not exit within cleanup bound",
                        )
                        .into());
                    }
                    break;
                }
                if store
                    .lock()
                    .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?
                    .invocation_stop_requested(job.job_id)?
                {
                    progress.canceled = true;
                    // SAFETY: job is valid and contains the complete Invocation tree.
                    unsafe { TerminateJobObject(job_object.raw(), 22) };
                    // SAFETY: process_handle remains valid.
                    if unsafe { WaitForSingleObject(process_handle.raw(), 30_000) } != WAIT_OBJECT_0
                    {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "canceled root did not exit within cleanup bound",
                        )
                        .into());
                    }
                    break;
                }
            }
        }

        let mut exit_code = 0_u32;
        // SAFETY: process handle and output pointer are valid.
        if unsafe { GetExitCodeProcess(process_handle.raw(), &mut exit_code) } == 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        progress.exit_code = Some(exit_code as i32);
        store
            .lock()
            .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?
            .mark_root_exited(job, exit_code as i32)?;
        // Root exit always terminates remaining descendants before cleanup proof.
        // SAFETY: job is valid. It is harmless when already empty.
        unsafe { TerminateJobObject(job_object.raw(), exit_code) };
        wait_job_empty(job_object.raw(), Duration::from_secs(30))?;
        progress.cleanup_proven = true;
        Ok((exit_code, timed_out))
    })();

    if execution.is_err() {
        // SAFETY: the job is live and owns the complete tree.
        unsafe { TerminateJobObject(job_object.raw(), 70) };
        if wait_job_empty(job_object.raw(), Duration::from_secs(30)).is_ok() {
            progress.cleanup_proven = true;
        }
    }
    drop(process_handle);
    drop(thread_handle);

    if !progress.cleanup_proven {
        // The pipes may remain open while an unproven process tree is terminating. Do not
        // block the scheduler indefinitely; the uncertain Containment keeps EOF unclaimed.
        // Persist uncertainty before transferring the still-live Job Object authority. The
        // reconciler keeps kill-on-close coverage instead of turning a timeout into cleanup.
        let mut locked = store
            .lock()
            .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?;
        persist_uncertain_cleanup(&mut locked, job, progress)?;
        drop(locked);
        progress.uncertainty_persisted = true;
        live_containments.transfer_to_reconciler(
            registration,
            job.invocation_id,
            job_object.raw(),
        )?;
        let _owned_by_reconciler = job_object.into_raw();
        spawn_boundary_empty_notification(completion_port, Arc::clone(reconciliation_wake))?;
        return execution;
    }

    let stdout_result = stdout.join().map_err(|_| "stdout drain thread panicked")?;
    let stderr_result = stderr.join().map_err(|_| "stderr drain thread panicked")?;
    stdout_result?;
    stderr_result?;

    let result = execution?;
    // Membership queries hold the same registry mutex used by registration Drop, so no
    // query can observe this raw HANDLE after it is closed or numerically recycled.
    drop(registration);
    drop(job_object);
    Ok(result)
}

fn create_job_object() -> std::io::Result<(OwnedHandle, OwnedHandle)> {
    // SAFETY: null attributes/name request a fresh unnamed Job Object as required by R-RUN-2.
    let handle = unsafe { CreateJobObjectW(null(), null()) };
    let handle = OwnedHandle::new(handle)?;
    let mut information: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { zeroed() };
    information.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
    // SAFETY: information points to the correct structure for the selected class.
    let set = unsafe {
        SetInformationJobObject(
            handle.raw(),
            JobObjectExtendedLimitInformation,
            (&raw const information).cast::<c_void>(),
            size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
        )
    };
    if set == 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: INVALID_HANDLE_VALUE requests a fresh completion port.
    let completion_port = OwnedHandle::new(unsafe {
        CreateIoCompletionPort(INVALID_HANDLE_VALUE, null_mut(), 0, 1)
    })?;
    let association = JOBOBJECT_ASSOCIATE_COMPLETION_PORT {
        CompletionKey: null_mut(),
        CompletionPort: completion_port.raw(),
    };
    // SAFETY: both handles are valid and association has the matching information layout.
    if unsafe {
        SetInformationJobObject(
            handle.raw(),
            JobObjectAssociateCompletionPortInformation,
            (&raw const association).cast::<c_void>(),
            size_of::<JOBOBJECT_ASSOCIATE_COMPLETION_PORT>() as u32,
        )
    } == 0
    {
        return Err(std::io::Error::last_os_error());
    }
    Ok((handle, completion_port))
}

fn spawn_boundary_empty_notification(
    completion_port: OwnedHandle,
    wake: super::ReconciliationWake,
) -> std::io::Result<()> {
    std::thread::Builder::new()
        .name("stillyard-containment-empty".into())
        .spawn(move || {
            const JOB_OBJECT_MSG_ACTIVE_PROCESS_ZERO: u32 = 4;
            loop {
                let mut message = 0_u32;
                let mut completion_key = 0_usize;
                let mut overlapped = null_mut();
                // SAFETY: the completion port remains owned by this thread and all outputs are
                // writable. The wait is event-driven and ends when the boundary becomes empty.
                let received = unsafe {
                    GetQueuedCompletionStatus(
                        completion_port.raw(),
                        &mut message,
                        &mut completion_key,
                        &mut overlapped,
                        u32::MAX,
                    )
                };
                if message == JOB_OBJECT_MSG_ACTIVE_PROCESS_ZERO {
                    wake();
                    break;
                }
                if received == 0 {
                    wake();
                    break;
                }
            }
        })
        .map(|_| ())
}

fn io_context(context: &str, error: std::io::Error) -> std::io::Error {
    std::io::Error::new(error.kind(), format!("{context}: {error}"))
}

fn create_inherited_pipe() -> std::io::Result<(OwnedHandle, OwnedHandle)> {
    let mut read = null_mut();
    let mut write = null_mut();
    let attributes = SECURITY_ATTRIBUTES {
        nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: null_mut(),
        bInheritHandle: 1,
    };
    // SAFETY: output pointers and security attributes are valid.
    if unsafe { CreatePipe(&mut read, &mut write, &attributes, 0) } == 0 {
        return Err(std::io::Error::last_os_error());
    }
    let read = OwnedHandle::new(read)?;
    let write = OwnedHandle::new(write)?;
    // SAFETY: read is valid; clearing inheritance keeps the daemon-side handle private.
    if unsafe { SetHandleInformation(read.raw(), HANDLE_FLAG_INHERIT, 0) } == 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok((read, write))
}

fn open_nul_for_read() -> std::io::Result<OwnedHandle> {
    let nul = wide_null(OsStr::new("NUL"));
    let attributes = SECURITY_ATTRIBUTES {
        nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: null_mut(),
        bInheritHandle: 1,
    };
    // SAFETY: NUL is NUL-terminated and attributes are initialized.
    let handle = unsafe {
        CreateFileW(
            nul.as_ptr(),
            GENERIC_READ,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            &attributes,
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL,
            null_mut(),
        )
    };
    OwnedHandle::new(handle)
}

fn open_stdin(job: &PreparedJob) -> std::io::Result<File> {
    let mut file = match (&job.stdin, &job.stdin_path) {
        (None, None) => {
            let handle = open_nul_for_read()?.into_raw();
            // SAFETY: ownership of the valid NUL handle transfers into File exactly once.
            unsafe { File::from_raw_handle(handle as RawHandle) }
        }
        (Some(expected), Some(path)) => {
            let file = OpenOptions::new()
                .read(true)
                .share_mode(FILE_SHARE_READ)
                .open(path)?;
            if file.metadata()?.len() != expected.length {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "staged stdin length changed before launch",
                ));
            }
            file
        }
        _ => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "partial staged stdin reference",
            ));
        }
    };
    if let Some(expected) = &job.stdin {
        if hash_reader(&mut file)? != expected.sha256 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "staged stdin hash changed before launch",
            ));
        }
        file.seek(SeekFrom::Start(0))?;
    }
    // SAFETY: file owns a valid handle; setting inheritance is limited by the explicit handle
    // list supplied to CreateProcessW.
    if unsafe {
        SetHandleInformation(
            file.as_raw_handle() as HANDLE,
            HANDLE_FLAG_INHERIT,
            HANDLE_FLAG_INHERIT,
        )
    } == 0
    {
        return Err(std::io::Error::last_os_error());
    }
    Ok(file)
}

fn spawn_drain(
    mut input: File,
    path: PathBuf,
    job_id: crate::JobId,
    stream: LogStream,
    store: Arc<Mutex<Store>>,
    publish_job_offset: bool,
) -> std::io::Result<std::thread::JoinHandle<RunResult<()>>> {
    std::thread::Builder::new()
        .name(format!("stillyard-log-{}-{stream:?}", job_id.entity_uuid()))
        .spawn(move || {
            let mut output = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(path)?;
            let mut offset = 0_u64;
            let mut buffer = [0_u8; 64 * 1024];
            loop {
                let read = input.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                output.write_all(&buffer[..read])?;
                output.sync_data()?;
                offset += read as u64;
                if publish_job_offset {
                    store
                        .lock()
                        .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?
                        .commit_log_offset(job_id, stream, offset)?;
                }
            }
            output.sync_all()?;
            Ok(())
        })
}

fn wait_job_empty(handle: HANDLE, timeout: Duration) -> std::io::Result<()> {
    #[cfg(test)]
    if FORCE_WAIT_JOB_EMPTY_FAILURE.replace(false) {
        return Err(std::io::Error::other(
            "forced Job Object empty-proof failure",
        ));
    }
    let deadline = Instant::now() + timeout;
    loop {
        let mut information: JOBOBJECT_BASIC_ACCOUNTING_INFORMATION = unsafe { zeroed() };
        // SAFETY: information matches the requested class and output size.
        let queried = unsafe {
            QueryInformationJobObject(
                handle,
                JobObjectBasicAccountingInformation,
                (&raw mut information).cast::<c_void>(),
                size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>() as u32,
                null_mut(),
            )
        };
        if queried == 0 {
            return Err(std::io::Error::last_os_error());
        }
        if information.ActiveProcesses == 0 {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "job object did not become empty",
            ));
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn process_image_path(handle: HANDLE) -> std::io::Result<PathBuf> {
    let mut buffer = vec![0_u16; 32_768];
    let mut length = buffer.len() as u32;
    // SAFETY: handle is valid and the buffer/length pointers are writable.
    if unsafe { QueryFullProcessImageNameW(handle, 0, buffer.as_mut_ptr(), &mut length) } == 0 {
        return Err(std::io::Error::last_os_error());
    }
    buffer.truncate(length as usize);
    Ok(PathBuf::from(String::from_utf16_lossy(&buffer)))
}

fn validate_paths(executable: &Path, working_directory: &Path) -> std::io::Result<()> {
    validate_executable(executable)?;
    if !working_directory.is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "working directory is missing or not a directory",
        ));
    }
    Ok(())
}

fn validate_executable(path: &Path) -> std::io::Result<()> {
    let metadata = std::fs::symlink_metadata(path)?;
    let wide = wide_null(path.as_os_str());
    // SAFETY: wide is NUL-terminated and remains alive for the call.
    let attributes = unsafe { GetFileAttributesW(wide.as_ptr()) };
    if attributes == INVALID_FILE_ATTRIBUTES {
        return Err(std::io::Error::last_os_error());
    }
    if !metadata.file_type().is_file()
        || metadata.file_type().is_symlink()
        || attributes & FILE_ATTRIBUTE_REPARSE_POINT != 0
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "executable is not an ordinary non-reparse file",
        ));
    }
    Ok(())
}

fn hash_reader(file: &mut File) -> std::io::Result<String> {
    let mut hash = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hash.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hash.finalize()))
}

fn same_windows_path(left: &Path, right: &Path) -> std::io::Result<bool> {
    let left = std::fs::canonicalize(left)?;
    let right = std::fs::canonicalize(right)?;
    Ok(left
        .as_os_str()
        .to_string_lossy()
        .eq_ignore_ascii_case(&right.as_os_str().to_string_lossy()))
}

fn environment_block(job: &PreparedJob, endpoint: &str) -> std::io::Result<Vec<u16>> {
    let mut environment = BTreeMap::<String, String>::new();
    for name in ["SystemRoot", "WINDIR", "TEMP", "TMP"] {
        if let Ok(value) = std::env::var(name) {
            environment.insert(name.to_uppercase(), value);
        }
    }
    for (name, value) in &job.spec.environment.set {
        environment.insert(name.to_uppercase(), value.clone());
    }
    for name in &job.spec.environment.unset {
        environment.remove(&name.to_uppercase());
    }
    environment.insert("STILLYARD_JOB_ID".into(), job.job_id.to_string());
    environment.insert("STILLYARD_ATTEMPT".into(), job.attempt_id.to_string());
    environment.insert(
        "STILLYARD_INVOCATION_ID".into(),
        job.invocation_id.to_string(),
    );
    environment.insert(
        "STILLYARD_ROLE".into(),
        match job.role {
            InvocationRole::Primary => "primary",
            InvocationRole::Probe => "probe",
            InvocationRole::Postcondition => "postcondition",
        }
        .into(),
    );
    environment.insert("STILLYARD_ENDPOINT".into(), endpoint.into());
    environment.insert(
        "STILLYARD_DAEMON_ID".into(),
        job.job_id.store_uuid().to_string(),
    );
    if let Some(primary_result) = &job.primary_result {
        environment.insert(
            "STILLYARD_PRIMARY_RESULT".into(),
            serde_json::to_string(primary_result).map_err(std::io::Error::other)?,
        );
    }
    let mut pairs: Vec<_> = environment.into_iter().collect();
    pairs.sort_by_key(|(name, _)| name.to_uppercase());
    let mut block = Vec::new();
    for (name, value) in pairs {
        block.extend(OsStr::new(&format!("{name}={value}")).encode_wide());
        block.push(0);
    }
    block.push(0);
    Ok(block)
}

fn command_line(executable: &Path, args: &[String]) -> Vec<u16> {
    let mut command = quote_windows_arg(&executable.as_os_str().to_string_lossy());
    for arg in args {
        command.push(' ');
        command.push_str(&quote_windows_arg(arg));
    }
    OsStr::new(&command)
        .encode_wide()
        .chain(std::iter::once(0))
        .collect()
}

fn quote_windows_arg(arg: &str) -> String {
    if !arg.is_empty() && !arg.bytes().any(|byte| matches!(byte, b' ' | b'\t' | b'"')) {
        return arg.to_owned();
    }
    let mut quoted = String::from("\"");
    let mut backslashes = 0;
    for character in arg.chars() {
        if character == '\\' {
            backslashes += 1;
        } else if character == '"' {
            quoted.push_str(&"\\".repeat(backslashes * 2 + 1));
            quoted.push('"');
            backslashes = 0;
        } else {
            quoted.push_str(&"\\".repeat(backslashes));
            backslashes = 0;
            quoted.push(character);
        }
    }
    quoted.push_str(&"\\".repeat(backslashes * 2));
    quoted.push('"');
    quoted
}

fn wide_null(value: &OsStr) -> Vec<u16> {
    value.encode_wide().chain(std::iter::once(0)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::StagedInputRef;
    use crate::runner::LiveContainments;
    use crate::store::{StorePaths, normalized_payload_hash, normalized_payload_hash_with_input};
    use crate::{
        AdmissionDecisionState, AttemptVerdict, ConditionDeadline, ConditionDeadlineOutcome,
        ConditionPredicate, ConditionSpec, EnvironmentSpec, ExitClassification, InvocationId,
        InvocationRole, JobOutcome, JobSpec, JobState, PostconditionSpec, QuietDetector,
        QuietPolicy, ResourceClaims, RetryPolicy, SPEC_VERSION, StdinSpec,
    };
    use uuid::Uuid;

    const TEST_ENDPOINT: &str = r"\\.\pipe\stillyard-runner-test";

    fn job_spec(root: &Path, executable: PathBuf, args: Vec<String>) -> JobSpec {
        JobSpec {
            spec_version: SPEC_VERSION,
            priority: crate::NEUTRAL_JOB_PRIORITY,
            executable,
            args,
            working_directory: root.to_path_buf(),
            stdin: StdinSpec::Eof,
            environment: EnvironmentSpec::default(),
            resources: ResourceClaims::default(),
            observed: None,
            conditions: Vec::new(),
            retry: RetryPolicy::default(),
            postconditions: Vec::new(),
            labels: Vec::new(),
            expected_duration_seconds: Some(1),
            timeout_seconds: Some(10),
            quiet: None,
            artifacts: Vec::new(),
            child_submission_policy: None,
        }
    }

    fn prepared(spec: &JobSpec, root: &Path) -> (PreparedJob, Arc<Mutex<Store>>) {
        let mut store = Store::open(StorePaths::new(root.to_path_buf())).unwrap();
        let hash = normalized_payload_hash(spec).unwrap();
        let submitted = store.submit(Uuid::now_v7(), &hash, spec).unwrap();
        let job = store
            .prepare_job(submitted.receipt.job_id)
            .unwrap()
            .unwrap();
        (job, Arc::new(Mutex::new(store)))
    }

    fn prepared_with_stdin(
        spec: &JobSpec,
        root: &Path,
        bytes: &[u8],
    ) -> (PreparedJob, Arc<Mutex<Store>>) {
        let mut store = Store::open(StorePaths::new(root.to_path_buf())).unwrap();
        let input = StagedInputRef {
            sha256: format!("{:x}", Sha256::digest(bytes)),
            length: bytes.len() as u64,
        };
        let upload_id = Uuid::now_v7();
        store
            .stage_begin(upload_id, &input.sha256, input.length)
            .unwrap();
        store.stage_chunk(upload_id, 0, bytes).unwrap();
        assert_eq!(store.stage_commit(upload_id).unwrap(), input);
        let hash = normalized_payload_hash_with_input(spec, Some(&input)).unwrap();
        let submitted = store
            .submit_with_stdin(Uuid::now_v7(), &hash, spec, Some(&input))
            .unwrap();
        let job = store
            .prepare_job(submitted.receipt.job_id)
            .unwrap()
            .unwrap();
        (job, Arc::new(Mutex::new(store)))
    }

    #[test]
    fn released_user_code_runner_failure_is_interrupted_not_failed() {
        let progress = RunProgress {
            user_code_released: true,
            cleanup_proven: true,
            ..RunProgress::default()
        };
        assert_eq!(
            failed_run_classification(&progress),
            (JobOutcome::Interrupted, "interrupted")
        );
    }

    #[test]
    fn retired_registration_is_negative_until_durable_state_catches_up() {
        let live_containments = LiveContainments::default();
        let invocation_id = InvocationId::new(Uuid::now_v7());
        let registration = live_containments
            .register(invocation_id, INVALID_HANDLE_VALUE)
            .unwrap();
        drop(registration);
        assert_eq!(
            live_containments
                .contains_process(invocation_id, INVALID_HANDLE_VALUE as usize)
                .unwrap(),
            Some(false)
        );
        live_containments.clear(invocation_id);
        assert_eq!(
            live_containments
                .contains_process(invocation_id, INVALID_HANDLE_VALUE as usize)
                .unwrap(),
            None
        );
    }

    #[test]
    fn containment_registry_is_instance_owned_and_transfer_does_not_leak_ownership() {
        let live_containments = LiveContainments::default();
        let other_instance = LiveContainments::default();
        let invocation_id = InvocationId::new(Uuid::now_v7());
        let registration = live_containments
            .register(invocation_id, INVALID_HANDLE_VALUE)
            .unwrap();

        assert_eq!(
            other_instance
                .contains_process(invocation_id, INVALID_HANDLE_VALUE as usize)
                .unwrap(),
            None,
            "another daemon instance must not observe this boundary"
        );
        assert_eq!(Arc::strong_count(&live_containments.inner), 2);
        live_containments
            .transfer_to_reconciler(registration, invocation_id, INVALID_HANDLE_VALUE)
            .unwrap();
        assert_eq!(
            Arc::strong_count(&live_containments.inner),
            1,
            "transfer must not leak the registration's registry owner"
        );
        live_containments.clear(invocation_id);
    }

    #[test]
    fn empty_owned_boundary_is_proven_and_notifies_reconciliation() {
        let live_containments = LiveContainments::default();
        let (job_object, completion_port) = create_job_object().unwrap();
        let invocation_id = InvocationId::new(Uuid::now_v7());
        let registration = live_containments
            .register(invocation_id, job_object.raw())
            .unwrap();
        assert_eq!(
            live_containments.inspect(invocation_id).unwrap(),
            Some(crate::ReconciliationResult::ProvenEmpty)
        );

        // Queue the same completion message Windows emits when the Job's active-process count
        // reaches zero, then prove the event-driven waiter wakes promptly.
        // SAFETY: completion_port is valid and no OVERLAPPED payload is required for Job
        // notification packets.
        assert_ne!(
            unsafe {
                windows_sys::Win32::System::IO::PostQueuedCompletionStatus(
                    completion_port.raw(),
                    4,
                    0,
                    std::ptr::null_mut(),
                )
            },
            0
        );
        let (sender, receiver) = std::sync::mpsc::channel();
        spawn_boundary_empty_notification(
            completion_port,
            Arc::new(move || {
                let _ = sender.send(());
            }),
        )
        .unwrap();
        receiver.recv_timeout(Duration::from_secs(2)).unwrap();

        drop(registration);
        live_containments.clear(invocation_id);
    }

    #[test]
    fn windows_quoting_handles_spaces_quotes_and_trailing_slashes() {
        assert_eq!(quote_windows_arg("plain"), "plain");
        assert_eq!(quote_windows_arg("two words"), "\"two words\"");
        assert_eq!(quote_windows_arg("a\\\"b"), "\"a\\\\\\\"b\"");
        assert_eq!(
            quote_windows_arg("path with space\\"),
            "\"path with space\\\\\""
        );
    }

    #[test]
    fn contained_process_publishes_output_after_success() {
        let temp = tempfile::tempdir().unwrap();
        let command = PathBuf::from(std::env::var_os("SystemRoot").unwrap())
            .join("System32")
            .join("cmd.exe");
        let spec = job_spec(
            temp.path(),
            command,
            vec![
                "/D".into(),
                "/S".into(),
                "/C".into(),
                "echo stillyard-smoke".into(),
            ],
        );
        let (job, store) = prepared(&spec, temp.path());
        run(&job, &store, TEST_ENDPOINT);
        let store = store.lock().unwrap();
        let snapshot = store.status(job.job_id).unwrap();
        assert_eq!(snapshot.state, JobState::Final);
        assert_eq!(
            snapshot.outcome,
            Some(JobOutcome::Succeeded),
            "{snapshot:#?}"
        );
        let logs = store.logs(job.job_id, LogStream::Stdout, 0, 1024).unwrap();
        assert!(String::from_utf8_lossy(&logs.bytes).contains("stillyard-smoke"));
        assert!(logs.eof);
    }

    #[test]
    fn postcondition_retry_runs_a_second_attempt_under_the_same_job() {
        let temp = tempfile::tempdir().unwrap();
        let command = PathBuf::from(std::env::var_os("SystemRoot").unwrap())
            .join("System32")
            .join("cmd.exe");
        let marker = temp.path().join("validator-seen.marker");
        let powershell = PathBuf::from(std::env::var_os("SystemRoot").unwrap())
            .join("System32")
            .join("WindowsPowerShell")
            .join("v1.0")
            .join("powershell.exe");
        let mut spec = job_spec(
            temp.path(),
            command.clone(),
            vec!["/D".into(), "/C".into(), "echo primary".into()],
        );
        spec.retry = RetryPolicy {
            max_attempts: 2,
            backoff_seconds: 0,
            retryable: vec!["postcondition_retryable".into()],
        };
        spec.environment.set.insert(
            "STY_VALIDATOR_MARKER".into(),
            marker.to_string_lossy().into_owned(),
        );
        spec.postconditions.push(PostconditionSpec {
                executable: powershell,
                args: vec![
                    "-NoLogo".into(),
                    "-NoProfile".into(),
                    "-NonInteractive".into(),
                    "-Command".into(),
                    "if (Test-Path -LiteralPath $env:STY_VALIDATOR_MARKER) { exit 0 } else { New-Item -ItemType File -Path $env:STY_VALIDATOR_MARKER | Out-Null; exit 10 }".into(),
                ],
                working_directory: None,
                accepted_exit_codes: vec![0],
                retryable_exit_codes: vec![10],
            });
        let (first, store) = prepared(&spec, temp.path());
        run(&first, &store, TEST_ENDPOINT);
        let second = {
            let mut locked = store.lock().unwrap();
            let snapshot = locked.status(first.job_id).unwrap();
            assert_eq!(snapshot.state, JobState::Pending);
            assert_eq!(
                snapshot.root_exit_code, None,
                "retry must clear stale root exit"
            );
            assert_eq!(
                snapshot.attempts[0].verdict,
                Some(AttemptVerdict::PostconditionRetryable)
            );
            assert_eq!(
                snapshot.attempts[0].invocations[1].role,
                InvocationRole::Postcondition
            );
            assert_eq!(
                snapshot.attempts[0].invocations[1].exit_classification,
                Some(ExitClassification::Retryable)
            );
            locked.prepare_job(first.job_id).unwrap().unwrap()
        };
        run(&second, &store, TEST_ENDPOINT);
        let snapshot = store.lock().unwrap().status(first.job_id).unwrap();
        assert_eq!(
            snapshot.outcome,
            Some(JobOutcome::Succeeded),
            "{snapshot:#?}"
        );
        assert_eq!(snapshot.attempts.len(), 2);
        assert_eq!(
            snapshot.started_unix_millis, snapshot.attempts[0].invocations[0].started_unix_millis,
            "Job start must remain the first primary start across postconditions and retries"
        );
        assert_eq!(
            snapshot.attempts[1].invocations[1].exit_classification,
            Some(ExitClassification::Accepted)
        );
    }

    #[test]
    fn staged_stdin_handle_reaches_the_contained_process() {
        let temp = tempfile::tempdir().unwrap();
        let payload = b"stillyard staged stdin marker\nsecond line\n";
        let mut spec = job_spec(
            temp.path(),
            std::env::current_exe().unwrap(),
            vec![
                "--ignored".into(),
                "--exact".into(),
                "runner::windows::tests::stdin_echo_helper".into(),
                "--nocapture".into(),
            ],
        );
        spec.stdin = StdinSpec::File {
            path: temp.path().join("client-prompt.bin"),
        };
        let (job, store) = prepared_with_stdin(&spec, temp.path(), payload);
        run(&job, &store, TEST_ENDPOINT);
        let store = store.lock().unwrap();
        let snapshot = store.status(job.job_id).unwrap();
        assert_eq!(snapshot.outcome, Some(JobOutcome::Succeeded));
        let logs = store
            .logs(job.job_id, LogStream::Stdout, 0, 64 * 1024)
            .unwrap();
        assert!(
            logs.bytes
                .windows(payload.len())
                .any(|window| window == payload),
            "the managed root must read the immutable staged bytes"
        );
    }

    #[test]
    #[allow(clippy::permissions_set_readonly_false)]
    fn staged_stdin_changed_after_acceptance_fails_before_user_code() {
        let temp = tempfile::tempdir().unwrap();
        let payload = b"trusted stdin";
        let mut spec = job_spec(
            temp.path(),
            std::env::current_exe().unwrap(),
            vec![
                "--ignored".into(),
                "--exact".into(),
                "runner::windows::tests::stdin_echo_helper".into(),
                "--nocapture".into(),
            ],
        );
        spec.stdin = StdinSpec::File {
            path: temp.path().join("client-prompt.bin"),
        };
        let (job, store) = prepared_with_stdin(&spec, temp.path(), payload);
        let blob = job.stdin_path.as_ref().unwrap();
        let mut permissions = std::fs::metadata(blob).unwrap().permissions();
        permissions.set_readonly(false);
        std::fs::set_permissions(blob, permissions).unwrap();
        std::fs::write(blob, b"altered stdin").unwrap();

        run(&job, &store, TEST_ENDPOINT);
        let store = store.lock().unwrap();
        let snapshot = store.status(job.job_id).unwrap();
        assert_eq!(snapshot.outcome, Some(JobOutcome::Failed));
        assert_eq!(snapshot.root_exit_code, None);
        let logs = store.logs(job.job_id, LogStream::Stdout, 0, 1024).unwrap();
        assert!(logs.bytes.is_empty(), "user code must not have run");
    }

    #[test]
    #[ignore = "launched only as a managed staged-stdin probe"]
    fn stdin_echo_helper() {
        std::io::copy(&mut std::io::stdin(), &mut std::io::stdout()).unwrap();
    }

    #[test]
    fn environment_block_has_exact_path_and_no_daemon_ambient_user_environment() {
        let temp = tempfile::tempdir().unwrap();
        let command = PathBuf::from(std::env::var_os("SystemRoot").unwrap())
            .join("System32")
            .join("cmd.exe");
        let mut spec = job_spec(temp.path(), command, vec![]);
        spec.environment
            .set
            .insert("PATH".into(), r"C:\Exact\Tools".into());
        let (job, _store) = prepared(&spec, temp.path());
        let block = environment_block(&job, TEST_ENDPOINT).unwrap();
        let decoded = String::from_utf16(&block[..block.len() - 2]).unwrap();
        let values: BTreeMap<_, _> = decoded
            .split('\0')
            .filter_map(|pair| pair.split_once('='))
            .map(|(name, value)| (name.to_owned(), value.to_owned()))
            .collect();
        assert_eq!(values.get("PATH").unwrap(), r"C:\Exact\Tools");
        assert!(!values.contains_key("USERPROFILE"));
        assert!(!values.contains_key("SSH_AUTH_SOCK"));
        assert!(!values.contains_key("ANTHROPIC_API_KEY"));
        assert_eq!(
            values.get("STILLYARD_ATTEMPT").unwrap(),
            &job.attempt_id.to_string()
        );
        assert_eq!(
            values.get("STILLYARD_DAEMON_ID").unwrap(),
            &job.job_id.store_uuid().to_string()
        );
        assert_eq!(values.get("STILLYARD_ENDPOINT").unwrap(), TEST_ENDPOINT);
        assert!(!values.contains_key("STILLYARD_STORE"));
    }

    #[test]
    fn primary_tree_is_empty_before_postcondition_receives_immutable_result() {
        let temp = tempfile::tempdir().unwrap();
        let powershell = PathBuf::from(std::env::var_os("SystemRoot").unwrap())
            .join("System32")
            .join("WindowsPowerShell")
            .join("v1.0")
            .join("powershell.exe");
        let pid_path = temp.path().join("grandchild.pid");
        let result_path = temp.path().join("primary-result.json");
        let primary_script = format!(
            "$child = Start-Process -FilePath $PSHOME\\powershell.exe -ArgumentList @('-NoLogo','-NoProfile','-NonInteractive','-Command', '$PID | Set-Content -LiteralPath \"{}\"; Start-Sleep -Seconds 30') -PassThru; while (-not (Test-Path -LiteralPath '{}')) {{ Start-Sleep -Milliseconds 10 }}; exit 25",
            pid_path.display(),
            pid_path.display(),
        );
        let postcondition_script = format!(
            "$result = $env:STILLYARD_PRIMARY_RESULT | ConvertFrom-Json; $childPid = [int](Get-Content -LiteralPath '{}'); if (Get-Process -Id $childPid -ErrorAction SilentlyContinue) {{ exit 91 }}; $result | ConvertTo-Json -Compress | Set-Content -LiteralPath '{}'; if ($result.root_exit_code -ne 25 -or $result.verdict -ne 'process_failed' -or $result.containment -ne 'empty') {{ exit 92 }}; exit 0",
            pid_path.display(),
            result_path.display(),
        );
        let mut spec = job_spec(
            temp.path(),
            powershell.clone(),
            vec![
                "-NoLogo".into(),
                "-NoProfile".into(),
                "-NonInteractive".into(),
                "-Command".into(),
                primary_script,
            ],
        );
        spec.postconditions.push(PostconditionSpec {
            executable: powershell,
            args: vec![
                "-NoLogo".into(),
                "-NoProfile".into(),
                "-NonInteractive".into(),
                "-Command".into(),
                postcondition_script,
            ],
            working_directory: None,
            accepted_exit_codes: vec![0],
            retryable_exit_codes: Vec::new(),
        });
        let (job, store) = prepared(&spec, temp.path());
        run(&job, &store, TEST_ENDPOINT);

        let stored_result: crate::PrimaryInvocationResult =
            serde_json::from_reader(std::fs::File::open(&result_path).unwrap()).unwrap();
        assert_eq!(stored_result.job_id, job.job_id);
        assert_eq!(stored_result.attempt_id, job.attempt_id);
        assert_eq!(stored_result.invocation_id, job.invocation_id);
        assert_eq!(stored_result.root_exit_code, Some(25));
        assert_eq!(stored_result.verdict, InvocationVerdict::ProcessFailed);
        assert_eq!(stored_result.containment, crate::ContainmentState::Empty);

        let snapshot = store.lock().unwrap().status(job.job_id).unwrap();
        assert_eq!(snapshot.outcome, Some(JobOutcome::Failed));
        assert_eq!(snapshot.attempts[0].primary_result, Some(stored_result));
        assert_eq!(snapshot.attempts[0].invocations.len(), 2);
        assert_eq!(
            snapshot.attempts[0].invocations[1].exit_classification,
            Some(ExitClassification::Accepted)
        );
    }

    #[test]
    fn timeout_kills_containment_and_releases_lease() {
        let temp = tempfile::tempdir().unwrap();
        let powershell = PathBuf::from(std::env::var_os("SystemRoot").unwrap())
            .join("System32")
            .join("WindowsPowerShell")
            .join("v1.0")
            .join("powershell.exe");
        let mut spec = job_spec(
            temp.path(),
            powershell.clone(),
            vec![
                "-NoLogo".into(),
                "-NoProfile".into(),
                "-NonInteractive".into(),
                "-Command".into(),
                "Start-Sleep -Seconds 30".into(),
            ],
        );
        spec.timeout_seconds = Some(3);
        let marker = temp.path().join("timeout-postcondition-ran.txt");
        spec.postconditions.push(PostconditionSpec {
            executable: powershell,
            args: vec![
                "-NoLogo".into(),
                "-NoProfile".into(),
                "-NonInteractive".into(),
                "-Command".into(),
                format!("Set-Content -LiteralPath '{}' -Value ran", marker.display()),
            ],
            working_directory: None,
            accepted_exit_codes: vec![0],
            retryable_exit_codes: Vec::new(),
        });
        let (job, store) = prepared(&spec, temp.path());
        let started = Instant::now();
        run(&job, &store, TEST_ENDPOINT);
        assert!(started.elapsed() < Duration::from_secs(10));
        let snapshot = store.lock().unwrap().status(job.job_id).unwrap();
        assert_eq!(snapshot.state, JobState::Final);
        assert_eq!(snapshot.outcome, Some(JobOutcome::TimedOut));
        assert_eq!(snapshot.root_exit_code, Some(21));
        assert!(!marker.exists(), "timeout must not launch a postcondition");
        assert_eq!(
            snapshot.attempts[0]
                .primary_result
                .as_ref()
                .map(|result| result.verdict),
            Some(InvocationVerdict::TimedOut)
        );
    }

    #[test]
    fn plain_cancel_terminates_a_running_containment_and_suppresses_retry() {
        let temp = tempfile::tempdir().unwrap();
        let powershell = PathBuf::from(std::env::var_os("SystemRoot").unwrap())
            .join("System32")
            .join("WindowsPowerShell")
            .join("v1.0")
            .join("powershell.exe");
        let mut spec = job_spec(
            temp.path(),
            powershell.clone(),
            vec![
                "-NoLogo".into(),
                "-NoProfile".into(),
                "-NonInteractive".into(),
                "-Command".into(),
                "Start-Sleep -Seconds 30".into(),
            ],
        );
        spec.retry = RetryPolicy {
            max_attempts: 2,
            backoff_seconds: 0,
            retryable: vec!["process_failed".into()],
        };
        let marker = temp.path().join("cancel-postcondition-ran.txt");
        spec.postconditions.push(PostconditionSpec {
            executable: powershell,
            args: vec![
                "-NoLogo".into(),
                "-NoProfile".into(),
                "-NonInteractive".into(),
                "-Command".into(),
                format!("Set-Content -LiteralPath '{}' -Value ran", marker.display()),
            ],
            working_directory: None,
            accepted_exit_codes: vec![0],
            retryable_exit_codes: Vec::new(),
        });
        let (job, store) = prepared(&spec, temp.path());
        let worker_store = Arc::clone(&store);
        let worker_job = job.clone();
        let worker = std::thread::spawn(move || run(&worker_job, &worker_store, TEST_ENDPOINT));
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let started = store
                .lock()
                .unwrap()
                .status(job.job_id)
                .unwrap()
                .attempts
                .first()
                .and_then(|attempt| attempt.invocations.first())
                .is_some_and(|invocation| invocation.state == crate::InvocationState::Started);
            if started {
                break;
            }
            assert!(Instant::now() < deadline, "managed root did not start");
            std::thread::sleep(Duration::from_millis(10));
        }
        store.lock().unwrap().cancel_jobs(&[job.job_id]).unwrap();
        worker.join().unwrap();
        let snapshot = store.lock().unwrap().status(job.job_id).unwrap();
        assert_eq!(snapshot.outcome, Some(JobOutcome::Canceled));
        assert_eq!(snapshot.attempts.len(), 1, "cancel must suppress retry");
        assert_eq!(snapshot.attempts[0].verdict, Some(AttemptVerdict::Canceled));
        assert_eq!(
            snapshot.attempts[0].invocations[0].containment.state,
            crate::ContainmentState::Empty
        );
        assert!(!marker.exists(), "cancel must not launch a postcondition");
        assert_eq!(
            snapshot.attempts[0]
                .primary_result
                .as_ref()
                .map(|result| result.verdict),
            Some(InvocationVerdict::Canceled)
        );
    }

    #[test]
    fn plain_cancel_during_postcondition_remains_canceled() {
        let temp = tempfile::tempdir().unwrap();
        let system32 = PathBuf::from(std::env::var_os("SystemRoot").unwrap()).join("System32");
        let command = system32.join("cmd.exe");
        let powershell = system32
            .join("WindowsPowerShell")
            .join("v1.0")
            .join("powershell.exe");
        let mut spec = job_spec(
            temp.path(),
            command,
            vec!["/D".into(), "/C".into(), "exit 0".into()],
        );
        spec.retry = RetryPolicy {
            max_attempts: 2,
            backoff_seconds: 0,
            retryable: vec!["postcondition_failed".into()],
        };
        spec.postconditions.push(PostconditionSpec {
            executable: powershell,
            args: vec![
                "-NoLogo".into(),
                "-NoProfile".into(),
                "-NonInteractive".into(),
                "-Command".into(),
                "Start-Sleep -Seconds 30".into(),
            ],
            working_directory: None,
            accepted_exit_codes: vec![0],
            retryable_exit_codes: Vec::new(),
        });
        let (job, store) = prepared(&spec, temp.path());
        let worker_store = Arc::clone(&store);
        let worker_job = job.clone();
        let worker = std::thread::spawn(move || run(&worker_job, &worker_store, TEST_ENDPOINT));
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let postcondition_started = store
                .lock()
                .unwrap()
                .status(job.job_id)
                .unwrap()
                .attempts
                .first()
                .and_then(|attempt| attempt.invocations.get(1))
                .is_some_and(|invocation| invocation.state == crate::InvocationState::Started);
            if postcondition_started {
                break;
            }
            assert!(Instant::now() < deadline, "postcondition did not start");
            std::thread::sleep(Duration::from_millis(10));
        }
        store.lock().unwrap().cancel_jobs(&[job.job_id]).unwrap();
        worker.join().unwrap();

        let snapshot = store.lock().unwrap().status(job.job_id).unwrap();
        assert_eq!(snapshot.outcome, Some(JobOutcome::Canceled));
        assert_eq!(snapshot.attempts.len(), 1, "cancel must suppress retry");
        assert_eq!(snapshot.attempts[0].verdict, Some(AttemptVerdict::Canceled));
        assert_eq!(snapshot.attempts[0].invocations.len(), 2);
        assert_eq!(
            snapshot.attempts[0].invocations[1].exit_classification,
            None
        );
        assert_eq!(
            snapshot.attempts[0].invocations[1].containment.state,
            crate::ContainmentState::Empty
        );
    }

    #[test]
    fn cancel_before_postcondition_release_never_runs_validator_code() {
        let temp = tempfile::tempdir().unwrap();
        let marker = temp.path().join("must-not-exist.marker");
        let powershell = PathBuf::from(std::env::var_os("SystemRoot").unwrap())
            .join("System32")
            .join("WindowsPowerShell")
            .join("v1.0")
            .join("powershell.exe");
        let mut spec = job_spec(temp.path(), powershell.clone(), vec!["-NoLogo".into()]);
        spec.environment.set.insert(
            "STY_CANCEL_MARKER".into(),
            marker.to_string_lossy().into_owned(),
        );
        spec.postconditions.push(PostconditionSpec {
                executable: powershell,
                args: vec![
                    "-NoLogo".into(),
                    "-NoProfile".into(),
                    "-NonInteractive".into(),
                    "-Command".into(),
                    "New-Item -ItemType File -Path $env:STY_CANCEL_MARKER | Out-Null; Start-Sleep -Seconds 30".into(),
                ],
                working_directory: None,
                accepted_exit_codes: vec![0],
                retryable_exit_codes: Vec::new(),
            });
        let (primary, store) = prepared(&spec, temp.path());
        {
            let mut locked = store.lock().unwrap();
            locked
                .mark_started(&primary, u32::MAX, "primary-hash")
                .unwrap();
            locked.mark_root_exited(&primary, 0).unwrap();
            locked
                .mark_invocation_resolved(&primary, Some(0), None)
                .unwrap();
            locked
                .record_primary_result(
                    &primary,
                    InvocationVerdict::Succeeded,
                    TerminationReason::Exited,
                )
                .unwrap();
        }
        let postcondition = store
            .lock()
            .unwrap()
            .prepare_postcondition(&primary, 0)
            .unwrap();
        store
            .lock()
            .unwrap()
            .cancel_jobs(&[primary.job_id])
            .unwrap();

        let mut progress = RunProgress {
            cleanup_proven: true,
            ..RunProgress::default()
        };
        let wake: crate::runner::ReconciliationWake = Arc::new(|| {});
        let live_containments = LiveContainments::default();
        let observation = crate::host_observation::HostObservationService::new(Default::default());
        let result = run_inner(
            &postcondition,
            &store,
            TEST_ENDPOINT,
            &live_containments,
            &observation,
            &mut progress,
            &wake,
        )
        .unwrap();
        assert!(progress.canceled);
        assert!(
            !progress.user_code_released,
            "validator root was resumed after durable cancel"
        );
        assert_eq!(result.0, 22);
        assert!(!marker.exists(), "durably canceled validator user code ran");
        let mut locked = store.lock().unwrap();
        locked
            .mark_invocation_resolved(&postcondition, Some(result.0 as i32), None)
            .unwrap();
        live_containments.clear(postcondition.invocation_id);
        locked
            .settle_attempt(&primary, AttemptVerdict::Canceled)
            .unwrap();
    }

    #[test]
    fn attempt_timeout_covers_postconditions() {
        let temp = tempfile::tempdir().unwrap();
        let system32 = PathBuf::from(std::env::var_os("SystemRoot").unwrap()).join("System32");
        let command = system32.join("cmd.exe");
        let powershell = system32
            .join("WindowsPowerShell")
            .join("v1.0")
            .join("powershell.exe");
        let mut spec = job_spec(
            temp.path(),
            command,
            vec!["/D".into(), "/C".into(), "exit 0".into()],
        );
        spec.timeout_seconds = Some(3);
        spec.postconditions.push(PostconditionSpec {
            executable: powershell,
            args: vec![
                "-NoLogo".into(),
                "-NoProfile".into(),
                "-NonInteractive".into(),
                "-Command".into(),
                "Start-Sleep -Seconds 30".into(),
            ],
            working_directory: None,
            accepted_exit_codes: vec![0],
            retryable_exit_codes: Vec::new(),
        });
        let (job, store) = prepared(&spec, temp.path());
        let started = Instant::now();
        run(&job, &store, TEST_ENDPOINT);

        assert!(started.elapsed() < Duration::from_secs(10));
        let snapshot = store.lock().unwrap().status(job.job_id).unwrap();
        assert_eq!(snapshot.outcome, Some(JobOutcome::TimedOut));
        assert_eq!(snapshot.root_exit_code, Some(0));
        assert_eq!(snapshot.attempts[0].verdict, Some(AttemptVerdict::TimedOut));
        assert_eq!(snapshot.attempts[0].invocations.len(), 2);
        assert_eq!(
            snapshot.attempts[0].invocations[1].exit_classification,
            None
        );
        assert_eq!(
            snapshot.attempts[0].invocations[1].containment.state,
            crate::ContainmentState::Empty
        );
    }

    #[test]
    fn timeout_kills_descendant_not_only_root() {
        use windows_sys::Win32::System::Threading::{
            OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
        };

        let temp = tempfile::tempdir().unwrap();
        let pid_file = temp.path().join("descendant.pid");
        let mut spec = job_spec(
            temp.path(),
            std::env::current_exe().unwrap(),
            vec![
                "--ignored".into(),
                "--exact".into(),
                "runner::windows::tests::spawn_descendant_helper".into(),
            ],
        );
        spec.environment.set.insert(
            "STY_TEST_PID_FILE".into(),
            pid_file.to_string_lossy().into_owned(),
        );
        spec.timeout_seconds = Some(5);
        let (job, store) = prepared(&spec, temp.path());
        run(&job, &store, TEST_ENDPOINT);
        let pid: u32 = std::fs::read_to_string(pid_file)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        // SAFETY: the access is read-only and the PID came from the launched descendant.
        let process = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
        if !process.is_null() {
            let mut exit_code = 0_u32;
            // SAFETY: process is a live handle and exit_code is writable.
            assert_ne!(unsafe { GetExitCodeProcess(process, &mut exit_code) }, 0);
            // 259 is STILL_ACTIVE. A root-only termination mutant leaves this descendant live.
            assert_ne!(exit_code, 259);
            // SAFETY: this test owns the process handle.
            unsafe { CloseHandle(process) };
        }
    }

    #[test]
    #[ignore = "launched only as a managed root by timeout_kills_descendant_not_only_root"]
    fn spawn_descendant_helper() {
        let executable = std::env::current_exe().unwrap();
        let mut child = std::process::Command::new(executable)
            .args([
                "--ignored",
                "--exact",
                "runner::windows::tests::descendant_sleeper",
            ])
            .spawn()
            .unwrap();
        std::fs::write(
            std::env::var_os("STY_TEST_PID_FILE").unwrap(),
            child.id().to_string(),
        )
        .unwrap();
        std::thread::sleep(Duration::from_secs(30));
        let _ = child.wait();
    }

    #[test]
    #[ignore = "launched only as a descendant containment probe"]
    fn descendant_sleeper() {
        std::thread::sleep(Duration::from_secs(30));
    }

    #[test]
    fn post_create_pre_resume_failure_proves_empty_and_is_start_failed() {
        let temp = tempfile::tempdir().unwrap();
        let command = PathBuf::from(std::env::var_os("SystemRoot").unwrap())
            .join("System32")
            .join("cmd.exe");
        let marker = temp.path().join("start-failure-postcondition-ran.txt");
        let mut spec = job_spec(
            temp.path(),
            command.clone(),
            vec!["/D".into(), "/C".into(), "echo must-not-run".into()],
        );
        spec.postconditions.push(PostconditionSpec {
            executable: command,
            args: vec![
                "/D".into(),
                "/C".into(),
                format!("echo ran>\"{}\"", marker.display()),
            ],
            working_directory: None,
            accepted_exit_codes: vec![0],
            retryable_exit_codes: Vec::new(),
        });
        let (job, store) = prepared(&spec, temp.path());
        FORCE_PRESTART_FAILURE.set(true);
        run(&job, &store, TEST_ENDPOINT);
        let store = store.lock().unwrap();
        let snapshot = store.status(job.job_id).unwrap();
        assert_eq!(snapshot.state, JobState::Final);
        assert_eq!(snapshot.outcome, Some(JobOutcome::Failed));
        assert_eq!(snapshot.root_exit_code, None);
        let logs = store.logs(job.job_id, LogStream::Stdout, 0, 1024).unwrap();
        assert!(logs.eof);
        assert!(logs.bytes.is_empty());
        assert!(
            !marker.exists(),
            "start failure must not run postconditions"
        );
        assert_eq!(
            snapshot.attempts[0]
                .primary_result
                .as_ref()
                .map(|result| result.verdict),
            Some(InvocationVerdict::StartFailed)
        );
    }

    #[test]
    fn condition_resume_failure_clears_never_started_timestamps() {
        let temp = tempfile::tempdir().unwrap();
        let command = PathBuf::from(std::env::var_os("SystemRoot").unwrap())
            .join("System32")
            .join("cmd.exe");
        let ready = temp.path().join("ready.flag");
        std::fs::write(&ready, b"ready").unwrap();
        let mut spec = job_spec(
            temp.path(),
            command,
            vec!["/D".into(), "/C".into(), "exit 0".into()],
        );
        spec.conditions.push(ConditionSpec {
            predicate: ConditionPredicate::PathExists { path: ready },
            deadline: ConditionDeadline::None,
            on_deadline: ConditionDeadlineOutcome::Failed,
        });
        let mut raw_store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
        let hash = normalized_payload_hash(&spec).unwrap();
        let receipt = raw_store
            .submit(Uuid::now_v7(), &hash, &spec)
            .unwrap()
            .receipt;
        let job = (0..8)
            .find_map(|_| raw_store.prepare_job(receipt.job_id).unwrap())
            .expect("satisfied Condition should prepare a primary");
        let store = Arc::new(Mutex::new(raw_store));
        FORCE_RESUME_FAILURE.set(true);

        run(&job, &store, TEST_ENDPOINT);

        let snapshot = store.lock().unwrap().status(receipt.job_id).unwrap();
        assert_eq!(snapshot.state, JobState::Final);
        assert_eq!(snapshot.outcome, Some(JobOutcome::Failed));
        assert_eq!(
            snapshot.attempts[0].verdict,
            Some(AttemptVerdict::StartFailed)
        );
        assert_eq!(snapshot.started_unix_millis, None);
        assert_eq!(snapshot.attempts[0].started_unix_millis, None);
        assert_eq!(snapshot.attempts[0].deadline_unix_millis, None);
        assert_eq!(
            snapshot.attempts[0].invocations[0].started_unix_millis,
            None
        );
        let primary_result = snapshot.attempts[0]
            .primary_result
            .as_ref()
            .expect("clean resume failure retains its primary result");
        assert_eq!(primary_result.verdict, InvocationVerdict::StartFailed);
        assert_eq!(primary_result.started_unix_millis, None);
        let admission = snapshot.attempts[0]
            .admission
            .as_ref()
            .expect("Condition Attempt has admission history");
        assert_eq!(admission.state, AdmissionDecisionState::Reserved);
        assert!(admission.final_sample);
    }

    #[test]
    fn first_terminal_cleanup_wait_failure_keeps_condition_deadline_outcome() {
        let temp = tempfile::tempdir().unwrap();
        let command = PathBuf::from(std::env::var_os("SystemRoot").unwrap())
            .join("System32")
            .join("cmd.exe");
        let ready = temp.path().join("ready.flag");
        std::fs::write(&ready, b"ready").unwrap();
        let mut spec = job_spec(
            temp.path(),
            command,
            vec!["/D".into(), "/C".into(), "exit 0".into()],
        );
        spec.conditions.push(ConditionSpec {
            predicate: ConditionPredicate::PathExists { path: ready },
            deadline: ConditionDeadline::Relative { seconds: 1 },
            on_deadline: ConditionDeadlineOutcome::Failed,
        });
        let mut raw_store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
        let hash = normalized_payload_hash(&spec).unwrap();
        let receipt = raw_store
            .submit(Uuid::now_v7(), &hash, &spec)
            .unwrap()
            .receipt;
        let job = (0..8)
            .find_map(|_| raw_store.prepare_job(receipt.job_id).unwrap())
            .expect("satisfied Condition should prepare a primary");
        std::thread::sleep(Duration::from_millis(1_050));
        let store = Arc::new(Mutex::new(raw_store));
        FORCE_STOPPED_ROOT_WAIT_FAILURE.set(true);

        run(&job, &store, TEST_ENDPOINT);

        let snapshot = store.lock().unwrap().status(job.job_id).unwrap();
        assert_eq!(snapshot.state, JobState::Final);
        assert_eq!(snapshot.outcome, Some(JobOutcome::Failed));
        assert_eq!(
            snapshot.reason_code.as_deref(),
            Some("condition_deadline_expired")
        );
        assert_eq!(
            snapshot.attempts[0].verdict,
            Some(AttemptVerdict::SafetyFailed)
        );
        assert!(snapshot.attempts[0].primary_result.is_none());
        assert_eq!(snapshot.started_unix_millis, None);
    }

    #[test]
    fn clean_prestart_failure_keeps_condition_deadline_precedence() {
        let temp = tempfile::tempdir().unwrap();
        let command = PathBuf::from(std::env::var_os("SystemRoot").unwrap())
            .join("System32")
            .join("cmd.exe");
        let ready = temp.path().join("ready.flag");
        std::fs::write(&ready, b"ready").unwrap();
        let mut spec = job_spec(
            temp.path(),
            command,
            vec!["/D".into(), "/C".into(), "exit 0".into()],
        );
        spec.retry.max_attempts = 2;
        spec.retry.retryable.push("start_failed".into());
        spec.conditions.push(ConditionSpec {
            predicate: ConditionPredicate::PathExists { path: ready },
            deadline: ConditionDeadline::Relative { seconds: 1 },
            on_deadline: ConditionDeadlineOutcome::Canceled,
        });
        let mut raw_store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
        let hash = normalized_payload_hash(&spec).unwrap();
        let receipt = raw_store
            .submit(Uuid::now_v7(), &hash, &spec)
            .unwrap()
            .receipt;
        let job = (0..8)
            .find_map(|_| raw_store.prepare_job(receipt.job_id).unwrap())
            .expect("satisfied Condition should prepare a primary");
        std::thread::sleep(Duration::from_millis(1_050));
        let store = Arc::new(Mutex::new(raw_store));
        FORCE_PRESTART_FAILURE.set(true);

        run(&job, &store, TEST_ENDPOINT);

        let snapshot = store.lock().unwrap().status(job.job_id).unwrap();
        assert_eq!(snapshot.state, JobState::Final);
        assert_eq!(snapshot.outcome, Some(JobOutcome::Canceled));
        assert_eq!(
            snapshot.reason_code.as_deref(),
            Some("condition_deadline_expired")
        );
        assert_eq!(snapshot.attempts.len(), 1);
        assert_eq!(snapshot.attempts[0].verdict, Some(AttemptVerdict::Canceled));
        assert!(snapshot.attempts[0].primary_result.is_none());
        assert_eq!(snapshot.started_unix_millis, None);
    }

    #[test]
    fn quiet_resume_failure_after_durable_authorization_is_start_failed() {
        let temp = tempfile::tempdir().unwrap();
        let command = PathBuf::from(std::env::var_os("SystemRoot").unwrap())
            .join("System32")
            .join("cmd.exe");
        let mut spec = job_spec(
            temp.path(),
            command,
            vec!["/D".into(), "/C".into(), "exit 0".into()],
        );
        spec.quiet = Some(QuietPolicy {
            stable_seconds: 1,
            max_sample_age_seconds: 3,
            wait_budget_seconds: 5,
            detectors: vec![QuietDetector::CpuUtilization { max_percent: 100 }],
        });
        let observation = crate::host_observation::HostObservationService::new(Default::default());
        let mut raw_store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
        let hash = normalized_payload_hash(&spec).unwrap();
        let receipt = raw_store
            .submit(Uuid::now_v7(), &hash, &spec)
            .unwrap()
            .receipt;
        let _ = observation.sample_now().unwrap();
        std::thread::sleep(Duration::from_millis(100));
        let first = observation.sample_now().unwrap();
        for _ in 0..4 {
            assert!(
                raw_store
                    .prepare_next_job_with_sample(Some(&first))
                    .unwrap()
                    .job
                    .is_none()
            );
        }
        std::thread::sleep(Duration::from_millis(1_050));
        let second = observation.sample_now().unwrap();
        let job = (0..4)
            .find_map(|_| {
                raw_store
                    .prepare_next_job_with_sample(Some(&second))
                    .unwrap()
                    .job
            })
            .expect("stable quiet evidence should prepare a primary");
        let store = Arc::new(Mutex::new(raw_store));
        let live_containments = LiveContainments::default();
        let wake: super::super::ReconciliationWake = Arc::new(|| {});
        FORCE_RESUME_FAILURE.set(true);

        run_with_wake(
            &job,
            &store,
            TEST_ENDPOINT,
            &live_containments,
            &observation,
            &wake,
        );

        let snapshot = store.lock().unwrap().status(receipt.job_id).unwrap();
        assert_eq!(snapshot.state, JobState::Final);
        assert_eq!(snapshot.outcome, Some(JobOutcome::Failed));
        assert_eq!(
            snapshot.attempts[0].verdict,
            Some(AttemptVerdict::StartFailed)
        );
        assert_eq!(
            snapshot.attempts[0]
                .primary_result
                .as_ref()
                .map(|result| result.verdict),
            Some(InvocationVerdict::StartFailed)
        );
        assert_eq!(snapshot.started_unix_millis, None);
        assert_eq!(snapshot.attempts[0].started_unix_millis, None);
        assert_eq!(snapshot.attempts[0].deadline_unix_millis, None);
        assert_eq!(
            snapshot.attempts[0].invocations[0].started_unix_millis,
            None
        );
        assert_eq!(
            snapshot.attempts[0]
                .primary_result
                .as_ref()
                .and_then(|result| result.started_unix_millis),
            None
        );
        let admission = snapshot.attempts[0]
            .admission
            .as_ref()
            .expect("quiet Attempt has admission history");
        assert_eq!(admission.state, AdmissionDecisionState::Reserved);
        assert!(admission.final_sample);
    }

    #[test]
    fn image_mismatch_with_unproven_cleanup_transfers_boundary_to_reconciler() {
        let temp = tempfile::tempdir().unwrap();
        let command = PathBuf::from(std::env::var_os("SystemRoot").unwrap())
            .join("System32")
            .join("cmd.exe");
        let spec = job_spec(
            temp.path(),
            command,
            vec!["/D".into(), "/C".into(), "exit 0".into()],
        );
        let (job, store) = prepared(&spec, temp.path());
        let live_containments = LiveContainments::default();
        let wake: super::super::ReconciliationWake = Arc::new(|| {});
        let observation = crate::host_observation::HostObservationService::new(Default::default());
        FORCE_IMAGE_MISMATCH.set(true);
        FORCE_WAIT_JOB_EMPTY_FAILURE.set(true);

        run_with_wake(
            &job,
            &store,
            TEST_ENDPOINT,
            &live_containments,
            &observation,
            &wake,
        );

        assert!(matches!(
            live_containments
                .inner
                .lock()
                .unwrap()
                .get(&job.invocation_id),
            Some(super::super::RegisteredContainment::Reconciler(_))
        ));
        let snapshot = store.lock().unwrap().status(job.job_id).unwrap();
        assert_eq!(
            snapshot.attempts[0].invocations[0].containment.state,
            crate::ContainmentState::Uncertain
        );
        assert!(snapshot.attempts[0].primary_result.is_none());
        live_containments.clear(job.invocation_id);
    }
}
