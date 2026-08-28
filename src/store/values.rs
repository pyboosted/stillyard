use super::*;

pub(super) fn parse_job_state(state: &str) -> StoreResult<JobState> {
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

pub(super) fn parse_outcome(outcome: &str) -> StoreResult<JobOutcome> {
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

pub(super) fn parse_attempt_verdict(verdict: &str) -> StoreResult<AttemptVerdict> {
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

pub(super) fn parse_invocation_role(role: &str) -> StoreResult<InvocationRole> {
    match role {
        "primary" => Ok(InvocationRole::Primary),
        "postcondition" => Ok(InvocationRole::Postcondition),
        other => Err(StoreError::InvalidState(format!(
            "unknown Invocation role {other}"
        ))),
    }
}

pub(super) fn parse_invocation_state(state: &str) -> StoreResult<InvocationState> {
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

pub(super) fn parse_containment_state(state: &str) -> StoreResult<ContainmentState> {
    match state {
        "creating" => Ok(ContainmentState::Creating),
        "live" => Ok(ContainmentState::Live),
        "empty" => Ok(ContainmentState::Empty),
        "uncertain" => Ok(ContainmentState::Uncertain),
        "cleared" => Ok(ContainmentState::Cleared),
        other => Err(StoreError::InvalidState(format!(
            "unknown Containment state {other}"
        ))),
    }
}

pub(super) fn parse_reconciliation_result(value: &str) -> StoreResult<ReconciliationResult> {
    match value {
        "still_resolves" => Ok(ReconciliationResult::StillResolves),
        "boundary_not_empty" => Ok(ReconciliationResult::BoundaryNotEmpty),
        "boundary_uninspectable" => Ok(ReconciliationResult::BoundaryUninspectable),
        "identity_unavailable" => Ok(ReconciliationResult::IdentityUnavailable),
        "identity_absent" => Ok(ReconciliationResult::IdentityAbsent),
        "pid_reused" => Ok(ReconciliationResult::PidReused),
        "proven_empty" => Ok(ReconciliationResult::ProvenEmpty),
        "prior_boot" => Ok(ReconciliationResult::PriorBoot),
        other => Err(StoreError::InvalidState(format!(
            "unknown reconciliation result {other}"
        ))),
    }
}

pub(super) fn reconciliation_result_string(value: &ReconciliationResult) -> StoreResult<&str> {
    match value {
        ReconciliationResult::StillResolves => Ok("still_resolves"),
        ReconciliationResult::BoundaryNotEmpty => Ok("boundary_not_empty"),
        ReconciliationResult::BoundaryUninspectable => Ok("boundary_uninspectable"),
        ReconciliationResult::IdentityUnavailable => Ok("identity_unavailable"),
        ReconciliationResult::IdentityAbsent => Ok("identity_absent"),
        ReconciliationResult::PidReused => Ok("pid_reused"),
        ReconciliationResult::ProvenEmpty => Ok("proven_empty"),
        ReconciliationResult::PriorBoot => Ok("prior_boot"),
        ReconciliationResult::Unknown(other) => Err(StoreError::InvalidState(format!(
            "unknown reconciliation result cannot enter durable state: {other}"
        ))),
    }
}

pub(super) fn parse_containment_resolution(value: &str) -> StoreResult<ContainmentResolution> {
    match value {
        "proven_empty" => Ok(ContainmentResolution::ProvenEmpty),
        "reboot" => Ok(ContainmentResolution::Reboot),
        "forced_risk_acceptance" => Ok(ContainmentResolution::ForcedRiskAcceptance),
        other => Err(StoreError::InvalidState(format!(
            "unknown containment resolution {other}"
        ))),
    }
}

pub(super) fn containment_resolution_string(value: &ContainmentResolution) -> StoreResult<&str> {
    match value {
        ContainmentResolution::ProvenEmpty => Ok("proven_empty"),
        ContainmentResolution::Reboot => Ok("reboot"),
        ContainmentResolution::ForcedRiskAcceptance => Ok("forced_risk_acceptance"),
        ContainmentResolution::Unknown(other) => Err(StoreError::InvalidState(format!(
            "unknown containment resolution cannot enter durable state: {other}"
        ))),
    }
}

pub(super) fn process_identity_from_columns(
    pid: Option<u32>,
    host_id: Option<String>,
    boot_id: Option<String>,
    creation_filetime_100ns: Option<i64>,
) -> StoreResult<Option<ProcessIdentity>> {
    match (pid, host_id, boot_id, creation_filetime_100ns) {
        (Some(pid), Some(host_id), Some(boot_id), Some(creation)) => {
            Ok(Some(ProcessIdentity::Windows {
                host_id: HostId(host_id),
                boot_id: BootId(boot_id),
                pid,
                creation_filetime_100ns: u64::try_from(creation).map_err(|_| {
                    StoreError::InvalidState("negative process creation identity".into())
                })?,
            }))
        }
        // Tests and records that never released user code may legitimately have no exact root.
        _ => Ok(None),
    }
}

pub(super) fn release_attempt_lease_if_safe(
    transaction: &Transaction<'_>,
    attempt_id: &str,
) -> StoreResult<bool> {
    let eligible: bool = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM attempts WHERE id = ?1 AND state = 'settled')
            AND NOT EXISTS(
                SELECT 1 FROM containments
                JOIN invocations ON invocations.id = containments.invocation_id
                WHERE invocations.attempt_id = ?1
                  AND containments.state NOT IN ('empty', 'cleared')
            )",
        [attempt_id],
        |row| row.get(0),
    )?;
    if !eligible {
        return Ok(false);
    }
    let released = transaction.execute(
        "UPDATE leases SET state = 'released'
         WHERE attempt_id = ?1 AND state = 'granted'",
        [attempt_id],
    )? > 0;
    Ok(released)
}

pub(super) fn attempt_lease_release_eligible_after_target(
    transaction: &Transaction<'_>,
    attempt_id: &str,
    target_containment_id: &str,
) -> StoreResult<bool> {
    transaction
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM attempts WHERE id = ?1 AND state = 'settled')
                AND EXISTS(SELECT 1 FROM leases
                           WHERE attempt_id = ?1 AND state = 'granted')
                AND NOT EXISTS(
                    SELECT 1 FROM containments
                    JOIN invocations ON invocations.id = containments.invocation_id
                    WHERE invocations.attempt_id = ?1
                      AND containments.id != ?2
                      AND containments.state NOT IN ('empty', 'cleared')
                )",
            params![attempt_id, target_containment_id],
            |row| row.get(0),
        )
        .map_err(Into::into)
}

pub(super) fn parse_exit_classification(value: &str) -> StoreResult<ExitClassification> {
    match value {
        "accepted" => Ok(ExitClassification::Accepted),
        "retryable" => Ok(ExitClassification::Retryable),
        "failed" => Ok(ExitClassification::Failed),
        other => Err(StoreError::InvalidState(format!(
            "unknown exit classification {other}"
        ))),
    }
}

pub(super) fn parse_scheduler_event_kind(value: &str) -> StoreResult<SchedulerEventKind> {
    match value {
        "job_changed" => Ok(SchedulerEventKind::JobChanged),
        "log_committed" => Ok(SchedulerEventKind::LogCommitted),
        "attempt_changed" => Ok(SchedulerEventKind::AttemptChanged),
        "invocation_changed" => Ok(SchedulerEventKind::InvocationChanged),
        "containment_changed" => Ok(SchedulerEventKind::ContainmentChanged),
        "cancellation_requested" => Ok(SchedulerEventKind::CancellationRequested),
        other => Err(StoreError::InvalidState(format!(
            "unknown scheduler event kind {other}"
        ))),
    }
}

pub(super) fn outcome_string(outcome: JobOutcome) -> &'static str {
    match outcome {
        JobOutcome::Succeeded => "succeeded",
        JobOutcome::Failed => "failed",
        JobOutcome::TimedOut => "timed_out",
        JobOutcome::Interrupted => "interrupted",
        JobOutcome::Canceled => "canceled",
        JobOutcome::Skipped => "skipped",
    }
}

pub(super) fn outcome_for_verdict(verdict: AttemptVerdict) -> JobOutcome {
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

pub(super) fn exit_classification_string(classification: ExitClassification) -> &'static str {
    match classification {
        ExitClassification::Accepted => "accepted",
        ExitClassification::Retryable => "retryable",
        ExitClassification::Failed => "failed",
    }
}

pub(super) fn read_diagnostic_tail(path: &Path) -> StoreResult<String> {
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

pub(super) fn bound_snapshot_diagnostics(attempts: &mut [AttemptSnapshot]) {
    let mut remaining = SNAPSHOT_DIAGNOSTIC_BUDGET_BYTES;
    for attempt in attempts.iter_mut().rev() {
        for invocation in attempt.invocations.iter_mut().rev() {
            keep_tail_within_budget(&mut invocation.stderr_tail, &mut remaining);
            keep_tail_within_budget(&mut invocation.stdout_tail, &mut remaining);
        }
    }
}

pub(super) fn keep_tail_within_budget(value: &mut String, remaining: &mut usize) {
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

pub(super) const DOCTOR_CODE_MAX_BYTES: usize = 128;
pub(super) const DOCTOR_SUMMARY_MAX_BYTES: usize = 1_024;
pub(super) const DOCTOR_DETAIL_MAX_BYTES: usize = 2_048;

pub(super) fn bounded_doctor_code(value: String) -> String {
    value
        .chars()
        .map(|character| if character.is_ascii() { character } else { '?' })
        .take(DOCTOR_CODE_MAX_BYTES)
        .collect()
}

pub(super) fn bounded_doctor_text(mut value: String, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value;
    }
    let mut boundary = max_bytes;
    while !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    value.truncate(boundary);
    value
}

pub(super) fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}
