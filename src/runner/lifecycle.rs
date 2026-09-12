//! Shared primary/probe/postcondition settlement and retained-Lease lifecycle.
//! Platform executors perform one Invocation and return explicit cleanup progress.
use crate::store::{PreparedJob, Store, StoreError};
use crate::{
    AttemptVerdict, ExitClassification, InvocationRole, InvocationVerdict, TerminationReason,
};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Default)]
pub(super) struct RunProgress {
    pub(super) user_code_released: bool,
    pub(super) durable_release_authorized: bool,
    pub(super) release_authorized: bool,
    pub(super) pre_release_replanned: bool,
    pub(super) cleanup_proven: bool,
    pub(super) uncertainty_persisted: bool,
    pub(super) exit_code: Option<i32>,
    pub(super) timed_out: bool,
    pub(super) canceled: bool,
    pub(super) never_run_reason: Option<String>,
}

pub(super) struct PendingStop {
    pub(super) verdict: AttemptVerdict,
    pub(super) reason: String,
}

pub(super) type RunResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

type Invoke = fn(
    &PreparedJob,
    &Arc<Mutex<Store>>,
    &str,
    &super::LiveContainments,
    &crate::host_observation::HostObservationService,
    &mut RunProgress,
    &super::ReconciliationWake,
) -> RunResult<(u32, bool)>;

pub(super) fn run_with_wake(
    job: &PreparedJob,
    store: &Arc<Mutex<Store>>,
    endpoint: &str,
    live_containments: &super::LiveContainments,
    host_observation: &crate::host_observation::HostObservationService,
    reconciliation_wake: &super::ReconciliationWake,
    execute: Invoke,
) {
    if job.role == InvocationRole::Probe {
        run_probe_with_wake(
            job,
            store,
            endpoint,
            live_containments,
            host_observation,
            reconciliation_wake,
            execute,
        );
        return;
    }
    let mut progress = RunProgress {
        cleanup_proven: true,
        ..RunProgress::default()
    };
    let primary = match execute(
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
            let result = match execute(
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
    execute: Invoke,
) {
    let mut progress = RunProgress {
        cleanup_proven: true,
        ..RunProgress::default()
    };
    match execute(
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

pub(super) fn pending_stop_verdict(
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
pub(super) fn failed_run_classification(
    progress: &RunProgress,
) -> (crate::JobOutcome, &'static str) {
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
            // The platform backend transfers its still-owned boundary to the reconciler before
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

pub(super) fn persist_uncertain_cleanup(
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

fn report_runner_error(job: &PreparedJob, error: &dyn std::error::Error) {
    use std::io::Write as _;
    let _ = writeln!(
        std::io::stderr(),
        "stillyard runner for {} failed: {error}",
        job.job_id
    );
}
