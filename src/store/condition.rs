use super::schedule::condition_blockers_tx;
use super::*;

const PATH_INSPECTION_TIMEOUT: Duration = Duration::from_secs(2);
const PATH_INSPECTION_WORKERS: usize = 8;
const PATH_INSPECTION_QUEUE: usize = 32;
pub(super) const MAX_RETAINED_OBSERVATIONS_PER_CONDITION: usize = 8;
pub(super) const MAX_RETAINED_PROBE_INVOCATIONS_PER_CONDITION: usize = 64;

#[derive(Clone)]
enum PathInspection {
    Present(bool),
    Invalidated(String),
}

pub(super) struct ConditionEvaluations {
    observed_ms: i64,
    observed_monotonic_ms: u64,
    scan_elapsed: Duration,
    paths: std::collections::HashMap<usize, PathInspection>,
}

impl ConditionEvaluations {
    pub(super) fn at_now() -> Self {
        let (observed_ms, observed_monotonic_ms) =
            crate::host_observation::observation_clock().unwrap_or_else(|_| (now_millis(), 0));
        Self {
            observed_ms,
            observed_monotonic_ms,
            scan_elapsed: Duration::ZERO,
            paths: Default::default(),
        }
    }

    pub(super) fn scan_all(conditions: &[crate::ConditionSpec]) -> Self {
        Self::scan_until(conditions, Instant::now() + PATH_INSPECTION_TIMEOUT)
    }

    pub(super) fn scan_many<'a>(
        conditions: impl IntoIterator<Item = &'a [crate::ConditionSpec]>,
    ) -> Vec<Self> {
        let deadline = Instant::now() + PATH_INSPECTION_TIMEOUT;
        conditions
            .into_iter()
            .map(|conditions| Self::scan_until(conditions, deadline))
            .collect()
    }

    fn scan_until(conditions: &[crate::ConditionSpec], deadline: Instant) -> Self {
        let scan_started = Instant::now();
        let mut evaluations = Self::at_now();
        for (index, condition) in conditions.iter().enumerate() {
            if let Some(path) = condition_path(condition) {
                let remaining = deadline.saturating_duration_since(Instant::now());
                let inspection = if remaining.is_zero() {
                    PathInspection::Invalidated(format!(
                        "filesystem rescan batch exceeded the {}ms provider bound",
                        PATH_INSPECTION_TIMEOUT.as_millis()
                    ))
                } else {
                    inspect_path_bounded(path, remaining)
                };
                evaluations.paths.insert(index, inspection);
            }
        }
        evaluations.scan_elapsed = scan_started.elapsed();
        evaluations
    }

    fn path(&self, index: usize) -> StoreResult<&PathInspection> {
        self.paths.get(&index).ok_or_else(|| {
            StoreError::InvalidState(format!(
                "Condition path evaluation {index} was not captured before the write transaction"
            ))
        })
    }

    pub(super) fn expires_monotonic(&self, freshness_millis: u64) -> u64 {
        self.observed_monotonic_ms.saturating_add(freshness_millis)
    }

    pub(super) fn scan_elapsed(&self) -> Duration {
        self.scan_elapsed
    }
}

#[derive(Clone, Copy)]
pub(super) struct ConditionAcceptance<'a> {
    pub(super) store_uuid: Uuid,
    pub(super) daemon_generation: Uuid,
    pub(super) boot_id: Option<&'a BootId>,
    pub(super) accepted_ms: i64,
    pub(super) freshness_millis: u64,
}

pub(super) struct ConditionRefresh {
    pub(super) blockers: Vec<Blocker>,
    pub(super) state_changed: bool,
    pub(super) deadline_expired: bool,
}

#[derive(Clone, Copy)]
pub(super) struct ConditionRefreshContext<'a> {
    pub(super) daemon_generation: Uuid,
    pub(super) boot_id: Option<&'a BootId>,
    pub(super) now: i64,
    pub(super) freshness_millis: u64,
    pub(super) force: bool,
    pub(super) evaluations: &'a ConditionEvaluations,
}

pub(super) fn insert_conditions_tx(
    transaction: &Transaction<'_>,
    job_id: JobId,
    conditions: &[crate::ConditionSpec],
    acceptance: ConditionAcceptance<'_>,
    evaluations: &ConditionEvaluations,
) -> StoreResult<()> {
    for (index, spec) in conditions.iter().enumerate() {
        let condition_id = ConditionId::new(acceptance.store_uuid);
        let deadline = resolve_deadline(spec.deadline, acceptance.accepted_ms);
        let next_probe = matches!(&spec.predicate, ConditionPredicate::Probe { .. })
            .then_some(acceptance.accepted_ms);
        transaction.execute(
            "INSERT INTO conditions(
                id, job_id, condition_index, state, spec_json, deadline_ms,
                deadline_outcome, next_probe_ms
             ) VALUES (?1, ?2, ?3, 'waiting', ?4, ?5, ?6, ?7)",
            params![
                condition_id.entity_uuid().to_string(),
                job_id.entity_uuid().to_string(),
                u32::try_from(index)
                    .map_err(|_| StoreError::InvalidSpec("too many Conditions".into()))?,
                serde_json::to_string(spec)?,
                deadline,
                deadline_outcome_string(spec.on_deadline),
                next_probe,
            ],
        )?;
        if !matches!(&spec.predicate, ConditionPredicate::Probe { .. }) {
            evaluate_one_tx(
                transaction,
                acceptance.daemon_generation,
                acceptance.boot_id,
                &condition_id.entity_uuid().to_string(),
                spec,
                acceptance.accepted_ms,
                evaluations.observed_monotonic_ms,
                acceptance.freshness_millis,
                Some(evaluations),
                index,
            )?;
        }
    }
    Ok(())
}

pub(super) fn refresh_conditions_tx(
    transaction: &Transaction<'_>,
    job_key: &str,
    context: ConditionRefreshContext<'_>,
) -> StoreResult<ConditionRefresh> {
    if let Some(outcome) = expire_condition_deadline_tx(transaction, job_key, context.now)? {
        return Ok(ConditionRefresh {
            blockers: vec![Blocker {
                code: "condition_deadline_expired".into(),
                detail: format!("deadline outcome={outcome}"),
            }],
            state_changed: true,
            deadline_expired: true,
        });
    }

    let mut statement = transaction.prepare(
        "SELECT id, state, spec_json,
                (SELECT fresh_until_ms FROM observations
                 WHERE condition_id = conditions.id ORDER BY rowid DESC LIMIT 1),
                (SELECT daemon_generation FROM observations
                 WHERE condition_id = conditions.id ORDER BY rowid DESC LIMIT 1),
                (SELECT observed_monotonic_ms FROM observations
                 WHERE condition_id = conditions.id ORDER BY rowid DESC LIMIT 1),
                condition_index
         FROM conditions WHERE job_id = ?1 ORDER BY condition_index",
    )?;
    let rows = statement
        .query_map([job_key], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<i64>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<u64>>(5)?,
                row.get::<_, usize>(6)?,
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    drop(statement);
    let mut changed = false;
    for (
        condition_key,
        prior,
        spec_json,
        fresh_until,
        generation,
        observed_monotonic,
        condition_index,
    ) in rows
    {
        let spec: crate::ConditionSpec = serde_json::from_str(&spec_json)?;
        if prior == "failed" || matches!(&spec.predicate, ConditionPredicate::Probe { .. }) {
            continue;
        }
        let stale = observation_is_stale(
            fresh_until,
            generation.as_deref(),
            observed_monotonic,
            context.daemon_generation,
            context.now,
            context.evaluations.observed_monotonic_ms,
            context.freshness_millis,
        );
        if context.force || stale {
            let state = evaluate_one_tx(
                transaction,
                context.daemon_generation,
                context.boot_id,
                &condition_key,
                &spec,
                context.evaluations.observed_ms,
                context.evaluations.observed_monotonic_ms,
                context.freshness_millis,
                Some(context.evaluations),
                condition_index,
            )?;
            let _ = prior;
            let _ = state;
            changed = true;
        }
    }
    let blockers = condition_blockers_tx(transaction, job_key)?;
    Ok(ConditionRefresh {
        blockers,
        state_changed: changed,
        deadline_expired: false,
    })
}

pub(super) fn expire_condition_deadline_tx(
    transaction: &Transaction<'_>,
    job_key: &str,
    now: i64,
) -> StoreResult<Option<String>> {
    let expired: Option<(String, String)> = transaction
        .query_row(
            "SELECT id, deadline_outcome FROM conditions
             WHERE job_id = ?1 AND state != 'failed'
               AND deadline_ms IS NOT NULL AND deadline_ms <= ?2
             ORDER BY deadline_ms, condition_index LIMIT 1",
            params![job_key, now],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if let Some((_condition, outcome)) = expired {
        release_reservation_tx(transaction, job_key)?;
        transaction.execute(
            "UPDATE conditions SET state = 'failed', next_probe_ms = NULL
             WHERE job_id = ?1 AND state != 'failed'",
            [job_key],
        )?;
        finalize_pending_condition_terminal_if_ready_tx(transaction, job_key, now)?;
        return Ok(Some(outcome));
    }
    Ok(None)
}

pub(super) fn job_has_live_probe_tx(
    transaction: &Transaction<'_>,
    job_key: &str,
) -> StoreResult<bool> {
    transaction
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM conditions
                 JOIN invocations ON invocations.condition_id = conditions.id
                 JOIN containments ON containments.invocation_id = invocations.id
                 WHERE conditions.job_id = ?1
                   AND invocations.role = 'probe'
                   AND (invocations.state != 'resolved'
                        OR containments.state IN ('creating', 'live'))
             )",
            [job_key],
            |row| row.get(0),
        )
        .map_err(Into::into)
}

pub(super) fn finalize_pending_condition_terminal_if_ready_tx(
    transaction: &Transaction<'_>,
    job_key: &str,
    now: i64,
) -> StoreResult<bool> {
    let (state, cancel_requested): (String, bool) = transaction.query_row(
        "SELECT state, cancel_requested != 0 FROM jobs WHERE id = ?1",
        [job_key],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    if state != "pending" || job_has_live_probe_tx(transaction, job_key)? {
        return Ok(false);
    }
    let deadline_outcome: Option<String> = transaction
        .query_row(
            "SELECT deadline_outcome FROM conditions
             WHERE job_id = ?1 AND state = 'failed'
               AND deadline_ms IS NOT NULL
             ORDER BY deadline_ms, condition_index LIMIT 1",
            [job_key],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(outcome) = deadline_outcome {
        finalize_pending_condition_terminal_tx(
            transaction,
            job_key,
            &outcome,
            Some("condition_deadline_expired"),
            now,
        )?;
        return Ok(true);
    }
    if cancel_requested {
        finalize_pending_condition_terminal_tx(transaction, job_key, "canceled", None, now)?;
        return Ok(true);
    }
    Ok(false)
}

pub(super) fn finalize_recovered_condition_terminals_tx(
    transaction: &Transaction<'_>,
    now: i64,
) -> StoreResult<bool> {
    let jobs = {
        let mut statement = transaction.prepare(
            "SELECT jobs.id FROM jobs WHERE jobs.state = 'pending'
               AND (jobs.cancel_requested != 0 OR EXISTS(
                   SELECT 1 FROM conditions
                   WHERE conditions.job_id = jobs.id AND conditions.state = 'failed'
                     AND conditions.deadline_ms IS NOT NULL
               ))
             ORDER BY jobs.accepted_ms, jobs.rowid",
        )?;
        statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    let mut changed = false;
    for job_key in jobs {
        changed |= finalize_pending_condition_terminal_if_ready_tx(transaction, &job_key, now)?;
    }
    Ok(changed)
}

pub(super) fn expire_due_condition_deadlines_tx(
    transaction: &Transaction<'_>,
    now: i64,
) -> StoreResult<bool> {
    let due = {
        let mut statement = transaction.prepare(
            "SELECT DISTINCT jobs.id FROM jobs
             JOIN conditions ON conditions.job_id = jobs.id
             WHERE jobs.state = 'pending' AND conditions.state != 'failed'
               AND conditions.deadline_ms IS NOT NULL
               AND conditions.deadline_ms <= ?1
             ORDER BY jobs.accepted_ms, jobs.rowid",
        )?;
        statement
            .query_map([now], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    for job_key in &due {
        expire_condition_deadline_tx(transaction, job_key, now)?;
    }
    Ok(!due.is_empty())
}

pub(super) fn finalize_pending_condition_terminal_tx(
    transaction: &Transaction<'_>,
    job_key: &str,
    outcome: &str,
    reason: Option<&str>,
    now: i64,
) -> StoreResult<()> {
    transaction.execute(
        "UPDATE conditions SET next_probe_ms = NULL WHERE job_id = ?1",
        [job_key],
    )?;
    transaction.execute(
        "UPDATE attempts SET state = 'settled', verdict = ?2,
            safety_reason = COALESCE(?3, safety_reason),
            finished_ms = ?4
         WHERE id = (SELECT attempt_id FROM jobs WHERE id = ?1)
           AND state IN ('planned', 'admitting')",
        params![
            job_key,
            if outcome == "canceled" {
                "canceled"
            } else {
                "safety_failed"
            },
            reason,
            now,
        ],
    )?;
    transaction.execute(
        "UPDATE jobs SET state = 'final', outcome = ?2, reason_code = ?3,
            finished_ms = ?4, retry_not_before_ms = NULL,
            reservation_not_before_ms = NULL
         WHERE id = ?1 AND state = 'pending'",
        params![job_key, outcome, reason, now],
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn repair_resolved_empty_probes_tx(
    transaction: &Transaction<'_>,
    store_uuid: Uuid,
    daemon_generation: Uuid,
    boot_id: Option<&BootId>,
    now: i64,
) -> StoreResult<bool> {
    let rows = {
        let mut statement = transaction.prepare(
            "SELECT conditions.id, conditions.job_id, conditions.spec_json,
                    conditions.state, invocations.id, invocations.root_exit_code,
                    jobs.state, jobs.cancel_requested != 0
             FROM conditions
             JOIN invocations ON invocations.id = conditions.probe_invocation_id
             JOIN containments ON containments.invocation_id = invocations.id
             JOIN jobs ON jobs.id = conditions.job_id
             WHERE invocations.state = 'resolved' AND containments.state = 'empty'
               AND EXISTS(SELECT 1 FROM leases
                          WHERE leases.invocation_id = invocations.id
                            AND leases.state = 'granted')
             ORDER BY conditions.job_id, conditions.condition_index",
        )?;
        statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<i32>>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, bool>(7)?,
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    let changed = !rows.is_empty();
    for (
        condition_key,
        job_key,
        condition_json,
        condition_state,
        invocation_key,
        exit_code,
        job_state,
        cancel_requested,
    ) in rows
    {
        let condition: crate::ConditionSpec = serde_json::from_str(&condition_json)?;
        let ConditionPredicate::Probe { probe } = condition.predicate else {
            return Err(StoreError::InvalidState(
                "resolved empty probe references a non-probe Condition".into(),
            ));
        };
        // The legacy split commit did not persist whether the runner timed out. Reconstructing
        // success from the exit code alone could release primary work after a timed-out probe, so
        // recovery deliberately fails closed and schedules a fresh probe.
        let accepted = false;
        transaction.execute(
            "UPDATE invocations SET exit_classification = COALESCE(exit_classification, ?2)
             WHERE id = ?1",
            params![invocation_key, if accepted { "accepted" } else { "failed" }],
        )?;
        let observation_id = ObservationId::new(store_uuid);
        let monotonic = monotonic_now_for_recovery();
        transaction.execute(
            "INSERT INTO observations(
                id, condition_id, observed_ms, observed_monotonic_ms, boot_id,
                daemon_generation, fresh_until_ms, source, value_json
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?3, 'probe', ?7)",
            params![
                observation_id.entity_uuid().to_string(),
                condition_key,
                now,
                monotonic,
                boot_id.map(|boot| boot.0.as_str()),
                daemon_generation.to_string(),
                serde_json::to_string(&ConditionObservationValue::Probe {
                    exit_code,
                    timed_out: false,
                    accepted,
                })?,
            ],
        )?;
        transaction.execute(
            "DELETE FROM observations WHERE condition_id = ?1 AND id NOT IN (
                 SELECT id FROM observations WHERE condition_id = ?1
                 ORDER BY rowid DESC LIMIT ?2
             )",
            params![condition_key, MAX_RETAINED_OBSERVATIONS_PER_CONDITION],
        )?;
        let next_probe = (!accepted && job_state == "pending" && !cancel_requested).then(|| {
            now.saturating_add(
                i64::try_from(probe.interval_seconds.saturating_mul(1_000)).unwrap_or(i64::MAX),
            )
        });
        transaction.execute(
            "UPDATE conditions SET state = ?2, probe_invocation_id = NULL,
                next_probe_ms = ?3 WHERE id = ?1",
            params![
                condition_key,
                if condition_state == "failed" || job_state != "pending" {
                    condition_state.as_str()
                } else if accepted {
                    "satisfied"
                } else {
                    "waiting"
                },
                next_probe,
            ],
        )?;
        transaction.execute(
            "UPDATE leases SET state = 'released'
             WHERE invocation_id = ?1 AND state = 'granted'",
            [invocation_key],
        )?;
        finalize_pending_condition_terminal_if_ready_tx(transaction, &job_key, now)?;
    }
    Ok(changed)
}

#[derive(Debug)]
struct PrunedProbeLog {
    job_key: String,
    invocation_key: String,
}

fn prune_resolved_probe_history_tx(
    transaction: &Transaction<'_>,
    condition_key: &str,
) -> StoreResult<Vec<PrunedProbeLog>> {
    let stale = {
        let mut statement = transaction.prepare(
            "SELECT jobs.id, invocations.id FROM invocations
             JOIN containments ON containments.invocation_id = invocations.id
             JOIN attempts ON attempts.id = invocations.attempt_id
             JOIN jobs ON jobs.id = attempts.job_id
             WHERE invocations.condition_id = ?1 AND invocations.state = 'resolved'
               AND containments.state IN ('empty', 'cleared')
               AND NOT EXISTS(SELECT 1 FROM leases
                              WHERE leases.invocation_id = invocations.id
                                AND leases.state = 'granted')
               AND NOT EXISTS(SELECT 1 FROM events
                              WHERE events.invocation_id = invocations.id)
             ORDER BY invocations.rowid DESC
             LIMIT -1 OFFSET ?2",
        )?;
        statement
            .query_map(
                params![condition_key, MAX_RETAINED_PROBE_INVOCATIONS_PER_CONDITION],
                |row| {
                    Ok(PrunedProbeLog {
                        job_key: row.get(0)?,
                        invocation_key: row.get(1)?,
                    })
                },
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    for stale_probe in &stale {
        transaction.execute(
            "INSERT OR IGNORE INTO probe_log_gc(invocation_id, job_id) VALUES (?1, ?2)",
            params![stale_probe.invocation_key, stale_probe.job_key],
        )?;
        transaction.execute(
            "DELETE FROM leases WHERE invocation_id = ?1 AND state = 'released'",
            [&stale_probe.invocation_key],
        )?;
        transaction.execute(
            "DELETE FROM containments WHERE invocation_id = ?1
               AND state IN ('empty', 'cleared')",
            [&stale_probe.invocation_key],
        )?;
        transaction.execute(
            "DELETE FROM invocations WHERE id = ?1 AND state = 'resolved'",
            [&stale_probe.invocation_key],
        )?;
    }
    Ok(stale)
}

fn remove_probe_log(path: &Path) -> bool {
    match std::fs::remove_file(path) {
        Ok(()) => true,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
        Err(_) => false,
    }
}

fn remove_pruned_probe_logs(
    paths: &StorePaths,
    store_uuid: Uuid,
    stale: &[PrunedProbeLog],
) -> Vec<String> {
    let mut completed = Vec::new();
    for stale_probe in stale {
        let directory = paths.logs.join(&stale_probe.job_key);
        let durable_invocation = format!("{store_uuid}~{}", stale_probe.invocation_key);
        let stdout_removed =
            remove_probe_log(&directory.join(format!("{durable_invocation}.stdout")));
        let stderr_removed =
            remove_probe_log(&directory.join(format!("{durable_invocation}.stderr")));
        if stdout_removed && stderr_removed {
            completed.push(stale_probe.invocation_key.clone());
        }
    }
    completed
}

fn monotonic_now_for_recovery() -> u64 {
    crate::host_observation::observation_clock()
        .map(|(_, monotonic)| monotonic)
        .unwrap_or(0)
}

pub(super) fn reset_conditions_for_retry_tx(
    transaction: &Transaction<'_>,
    job_key: &str,
    not_before: i64,
) -> StoreResult<()> {
    let mut statement = transaction.prepare(
        "SELECT id, state, spec_json FROM conditions
         WHERE job_id = ?1 ORDER BY condition_index",
    )?;
    let rows = statement
        .query_map([job_key], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    drop(statement);
    for (condition_key, prior_state, spec_json) in rows {
        let spec: crate::ConditionSpec = serde_json::from_str(&spec_json)?;
        let next_probe =
            matches!(&spec.predicate, ConditionPredicate::Probe { .. }).then_some(not_before);
        let next_state = if matches!(&spec.predicate, ConditionPredicate::PathTransition { .. })
            && prior_state == "satisfied"
        {
            "satisfied"
        } else {
            "waiting"
        };
        transaction.execute(
            "UPDATE conditions SET state = ?2, probe_invocation_id = NULL,
                next_probe_ms = ?3 WHERE id = ?1",
            params![condition_key, next_state, next_probe],
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn evaluate_one_tx(
    transaction: &Transaction<'_>,
    daemon_generation: Uuid,
    boot_id: Option<&BootId>,
    condition_key: &str,
    spec: &crate::ConditionSpec,
    observed_ms: i64,
    observed_monotonic_ms: u64,
    freshness_millis: u64,
    evaluations: Option<&ConditionEvaluations>,
    condition_index: usize,
) -> StoreResult<String> {
    let (value, state, transition_armed, source) = match &spec.predicate {
        ConditionPredicate::PathExists { .. } => match evaluations
            .ok_or_else(|| StoreError::InvalidState("missing Condition evaluations".into()))?
            .path(condition_index)?
        {
            PathInspection::Present(exists) => (
                ConditionObservationValue::Path { exists: *exists },
                if *exists { "satisfied" } else { "waiting" },
                None,
                ConditionObservationSource::FilesystemRescan,
            ),
            PathInspection::Invalidated(error) => (
                ConditionObservationValue::Invalidated {
                    reason: error.clone(),
                },
                "waiting",
                None,
                ConditionObservationSource::Invalidation,
            ),
        },
        ConditionPredicate::PathAbsent { .. } => match evaluations
            .ok_or_else(|| StoreError::InvalidState("missing Condition evaluations".into()))?
            .path(condition_index)?
        {
            PathInspection::Present(exists) => (
                ConditionObservationValue::Path { exists: *exists },
                if *exists { "waiting" } else { "satisfied" },
                None,
                ConditionObservationSource::FilesystemRescan,
            ),
            PathInspection::Invalidated(error) => (
                ConditionObservationValue::Invalidated {
                    reason: error.clone(),
                },
                "waiting",
                None,
                ConditionObservationSource::Invalidation,
            ),
        },
        ConditionPredicate::PathTransition { from, to, .. } => match evaluations
            .ok_or_else(|| StoreError::InvalidState("missing Condition evaluations".into()))?
            .path(condition_index)?
        {
            PathInspection::Present(exists) => {
                let current = if *exists {
                    crate::PathConditionState::Present
                } else {
                    crate::PathConditionState::Absent
                };
                let (armed, previously_satisfied): (bool, bool) = transaction.query_row(
                    "SELECT transition_armed != 0, state = 'satisfied'
                     FROM conditions WHERE id = ?1",
                    [condition_key],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
                let armed = armed || current == *from;
                // A transition is an acceptance-anchored event, not a level predicate. Once an
                // authoritative rescan has observed the target after arming, later path changes
                // cannot erase that durable history. Consumers that need the target to remain
                // present/absent add the corresponding level Condition to the AND-set.
                let satisfied = previously_satisfied || (armed && current == *to);
                (
                    ConditionObservationValue::Path { exists: *exists },
                    if satisfied { "satisfied" } else { "waiting" },
                    Some(armed),
                    ConditionObservationSource::FilesystemRescan,
                )
            }
            PathInspection::Invalidated(error) => {
                let previously_satisfied: bool = transaction.query_row(
                    "SELECT state = 'satisfied' FROM conditions WHERE id = ?1",
                    [condition_key],
                    |row| row.get(0),
                )?;
                (
                    ConditionObservationValue::Invalidated {
                        reason: error.clone(),
                    },
                    if previously_satisfied {
                        "satisfied"
                    } else {
                        "waiting"
                    },
                    None,
                    ConditionObservationSource::Invalidation,
                )
            }
        },
        ConditionPredicate::NotBefore { unix_millis } => {
            let reached = observed_ms >= *unix_millis;
            (
                ConditionObservationValue::Time { reached },
                if reached { "satisfied" } else { "waiting" },
                None,
                ConditionObservationSource::Clock,
            )
        }
        ConditionPredicate::Probe { .. } => {
            return Err(StoreError::InvalidState(
                "probe Condition cannot be evaluated as an in-process predicate".into(),
            ));
        }
    };
    let observation = Uuid::now_v7();
    let fresh_until =
        observed_ms.saturating_add(i64::try_from(freshness_millis).unwrap_or(i64::MAX));
    transaction.execute(
        "INSERT INTO observations(
            id, condition_id, observed_ms, observed_monotonic_ms, boot_id,
            daemon_generation, fresh_until_ms, source, value_json
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            observation.to_string(),
            condition_key,
            observed_ms,
            observed_monotonic_ms,
            boot_id.map(|boot| boot.0.as_str()),
            daemon_generation.to_string(),
            fresh_until,
            observation_source_string(source),
            serde_json::to_string(&value)?,
        ],
    )?;
    transaction.execute(
        "DELETE FROM observations WHERE condition_id = ?1 AND id NOT IN (
             SELECT id FROM observations WHERE condition_id = ?1
             ORDER BY rowid DESC LIMIT ?2
         )",
        params![condition_key, MAX_RETAINED_OBSERVATIONS_PER_CONDITION],
    )?;
    transaction.execute(
        "UPDATE conditions SET state = ?2,
            transition_armed = COALESCE(?3, transition_armed) WHERE id = ?1",
        params![condition_key, state, transition_armed],
    )?;
    Ok(state.into())
}

impl Store {
    fn retry_pruned_probe_logs(&mut self) -> StoreResult<()> {
        let stale = {
            let mut statement = self.connection.prepare(
                "SELECT job_id, invocation_id FROM probe_log_gc
                 ORDER BY attempt_count, rowid LIMIT 256",
            )?;
            statement
                .query_map([], |row| {
                    Ok(PrunedProbeLog {
                        job_key: row.get(0)?,
                        invocation_key: row.get(1)?,
                    })
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?
        };
        if stale.is_empty() {
            return Ok(());
        }
        let completed = remove_pruned_probe_logs(&self.paths, self.store_uuid, &stale)
            .into_iter()
            .collect::<std::collections::HashSet<_>>();
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        for stale_probe in stale {
            if completed.contains(&stale_probe.invocation_key) {
                transaction.execute(
                    "DELETE FROM probe_log_gc WHERE invocation_id = ?1",
                    [stale_probe.invocation_key],
                )?;
            } else {
                transaction.execute(
                    "UPDATE probe_log_gc SET attempt_count = CASE
                         WHEN attempt_count = 9223372036854775807 THEN 0
                         ELSE attempt_count + 1 END
                     WHERE invocation_id = ?1",
                    [stale_probe.invocation_key],
                )?;
            }
        }
        transaction.commit()?;
        Ok(())
    }

    pub(super) fn prune_condition_history(&mut self) -> StoreResult<()> {
        let condition_keys = {
            let mut statement = self.connection.prepare(
                "SELECT invocations.condition_id FROM invocations
                 JOIN containments ON containments.invocation_id = invocations.id
                 WHERE invocations.role = 'probe'
                   AND invocations.condition_id IS NOT NULL
                   AND invocations.state = 'resolved'
                   AND containments.state IN ('empty', 'cleared')
                   AND NOT EXISTS(SELECT 1 FROM leases
                                  WHERE leases.invocation_id = invocations.id
                                    AND leases.state = 'granted')
                   AND NOT EXISTS(SELECT 1 FROM events
                                  WHERE events.invocation_id = invocations.id)
                 GROUP BY invocations.condition_id
                 HAVING COUNT(*) > ?1",
            )?;
            statement
                .query_map([MAX_RETAINED_PROBE_INVOCATIONS_PER_CONDITION], |row| {
                    row.get::<_, String>(0)
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?
        };
        if !condition_keys.is_empty() {
            let transaction = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)?;
            for condition_key in condition_keys {
                prune_resolved_probe_history_tx(&transaction, &condition_key)?;
            }
            transaction.commit()?;
        }
        self.retry_pruned_probe_logs()
    }

    pub(super) fn expire_job_condition_deadline(
        &mut self,
        job_id: JobId,
        now: i64,
    ) -> StoreResult<bool> {
        let job_key = self.local_id(job_id)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = expire_condition_deadline_tx(&transaction, &job_key, now)?.is_some();
        if changed {
            transaction.commit()?;
        } else {
            transaction.rollback()?;
        }
        Ok(changed)
    }

    pub(super) fn expire_due_condition_deadlines(&mut self, now: i64) -> StoreResult<bool> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = expire_due_condition_deadlines_tx(&transaction, now)?;
        if changed {
            transaction.commit()?;
        } else {
            transaction.rollback()?;
        }
        Ok(changed)
    }

    pub(super) fn condition_snapshots(&self, job_id: JobId) -> StoreResult<Vec<ConditionSnapshot>> {
        let mut statement = self.connection.prepare(
            "SELECT conditions.id, conditions.condition_index, conditions.state,
                    conditions.spec_json, conditions.deadline_ms,
                    conditions.probe_invocation_id,
                    observations.id, observations.observed_ms,
                    observations.observed_monotonic_ms, observations.boot_id,
                    observations.daemon_generation, observations.fresh_until_ms,
                    observations.source, observations.value_json
             FROM conditions
             LEFT JOIN observations ON observations.id = (
                 SELECT id FROM observations latest
                 WHERE latest.condition_id = conditions.id ORDER BY latest.rowid DESC LIMIT 1
             )
             WHERE conditions.job_id = ?1 ORDER BY conditions.condition_index",
        )?;
        let rows = statement.query_map([self.local_id(job_id)?], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, u32>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<i64>>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, Option<i64>>(7)?,
                row.get::<_, Option<u64>>(8)?,
                row.get::<_, Option<String>>(9)?,
                row.get::<_, Option<String>>(10)?,
                row.get::<_, Option<i64>>(11)?,
                row.get::<_, Option<String>>(12)?,
                row.get::<_, Option<String>>(13)?,
            ))
        })?;
        rows.map(|row| {
            let (
                condition,
                index,
                state,
                spec,
                deadline,
                probe,
                observation,
                observed,
                monotonic,
                boot,
                generation,
                fresh_until,
                source,
                value,
            ) = row?;
            let last_observation = match (
                observation,
                observed,
                monotonic,
                generation,
                fresh_until,
                source,
                value,
            ) {
                (
                    Some(id),
                    Some(observed),
                    Some(monotonic),
                    Some(generation),
                    Some(fresh),
                    Some(source),
                    Some(value),
                ) => Some(ConditionObservationSnapshot {
                    observation_id: ObservationId::from_parts(
                        self.store_uuid,
                        Uuid::parse_str(&id)?,
                    ),
                    value: serde_json::from_str(&value)?,
                    observed_unix_millis: observed,
                    observed_monotonic_millis: monotonic,
                    boot_id: boot.map(BootId),
                    daemon_generation: Uuid::parse_str(&generation)?,
                    fresh_until_unix_millis: fresh,
                    source: parse_observation_source(&source)?,
                }),
                (None, None, None, None, None, None, None) => None,
                _ => {
                    return Err(StoreError::InvalidState(
                        "partial Condition observation row".into(),
                    ));
                }
            };
            Ok(ConditionSnapshot {
                condition_id: ConditionId::from_parts(
                    self.store_uuid,
                    Uuid::parse_str(&condition)?,
                ),
                condition_index: index,
                state: parse_condition_state(&state)?,
                spec: serde_json::from_str(&spec)?,
                deadline_unix_millis: deadline,
                last_observation,
                probe_invocation_id: probe
                    .map(|id| {
                        Uuid::parse_str(&id).map(|id| InvocationId::from_parts(self.store_uuid, id))
                    })
                    .transpose()?,
            })
        })
        .collect()
    }

    pub(super) fn prepare_due_probe(
        &mut self,
        job_id: JobId,
        original_spec: &JobSpec,
        observation: Option<crate::host_observation::ObservationMoment<'_>>,
    ) -> StoreResult<Option<PreparedJob>> {
        let now = now_millis();
        let store_uuid = self.store_uuid;
        let capacities = self.capacities.clone();
        let impact_incompatibilities = self.impact_incompatibilities.clone();
        let startup_identity = self.startup_identity.clone();
        let daemon_generation = self.daemon_generation;
        let paths = self.paths.clone();
        let host_config = self.host_config();
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let job_key = job_id.entity_uuid().to_string();
        let row = transaction
            .query_row(
                "SELECT id, condition_index, spec_json FROM conditions
                 WHERE job_id = ?1 AND state = 'waiting'
                   AND next_probe_ms IS NOT NULL AND next_probe_ms <= ?2
                   AND probe_invocation_id IS NULL
                   AND (deadline_ms IS NULL OR deadline_ms > ?2)
                   AND EXISTS(
                       SELECT 1 FROM jobs
                       WHERE jobs.id = conditions.job_id AND jobs.state = 'pending'
                         AND jobs.cancel_requested = 0
                   )
                 ORDER BY condition_index LIMIT 1",
                params![job_key, now],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, u32>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()?;
        let Some((condition_key, condition_index, condition_json)) = row else {
            transaction.rollback()?;
            return Ok(None);
        };
        let condition: crate::ConditionSpec = serde_json::from_str(&condition_json)?;
        let ConditionPredicate::Probe { probe } = &condition.predicate else {
            return Err(StoreError::InvalidState(
                "non-probe Condition has a probe schedule".into(),
            ));
        };
        let mut probe_spec = original_spec.clone();
        probe_spec.executable = probe.executable.clone();
        probe_spec.args = probe.args.clone();
        probe_spec.working_directory = probe.working_directory.clone();
        probe_spec.stdin = StdinSpec::Eof;
        probe_spec.environment = probe.environment.clone();
        probe_spec.resources = probe.resources.clone();
        probe_spec.observed = None;
        probe_spec.conditions.clear();
        probe_spec.retry = Default::default();
        probe_spec.postconditions.clear();
        probe_spec.expected_duration_seconds = Some(probe.timeout_seconds);
        probe_spec.timeout_seconds = Some(probe.timeout_seconds);
        probe_spec.quiet = None;
        probe_spec.artifacts.clear();
        probe_spec.child_submission_policy = None;
        let claims = ResolvedClaims::resolve(&probe.resources)?;
        let mut debits = active_claims_tx(&transaction)?;
        debits.extend(reservation_claims_tx(&transaction, now, None)?);
        if !claims
            .blockers(&capacities, &debits, &impact_incompatibilities)
            .is_empty()
        {
            transaction.rollback()?;
            return Ok(None);
        }
        if probe_spec.requires_host_observation() {
            let Some(observation) = observation else {
                transaction.rollback()?;
                return Ok(None);
            };
            let context = crate::host_observation::evaluate_admission(
                &probe_spec,
                &host_config,
                observation.sample,
                &debits,
                observation.now_unix_millis,
                observation.now_monotonic_millis,
            );
            if !context.blockers.is_empty() {
                transaction.rollback()?;
                return Ok(None);
            }
        }

        let attempt_key: Option<String> = transaction.query_row(
            "SELECT attempt_id FROM jobs WHERE id = ?1 AND state = 'pending'",
            [&job_key],
            |row| row.get(0),
        )?;
        let attempt_id = match attempt_key {
            Some(key) => {
                let state: String = transaction.query_row(
                    "SELECT state FROM attempts WHERE id = ?1",
                    [&key],
                    |row| row.get(0),
                )?;
                if !matches!(state.as_str(), "planned" | "admitting") {
                    return Err(StoreError::InvalidState(format!(
                        "probe requires a planned/admitting Attempt, found {state}"
                    )));
                }
                AttemptId::from_parts(store_uuid, Uuid::parse_str(&key)?)
            }
            None => {
                let attempt_id = AttemptId::new(store_uuid);
                let attempt_index: u32 = transaction.query_row(
                    "SELECT COALESCE(MAX(attempt_index), 0) + 1 FROM attempts WHERE job_id = ?1",
                    [&job_key],
                    |row| row.get(0),
                )?;
                transaction.execute(
                    "INSERT INTO attempts(id, job_id, state, attempt_index, created_ms)
                     VALUES (?1, ?2, 'planned', ?3, ?4)",
                    params![
                        attempt_id.entity_uuid().to_string(),
                        job_key,
                        attempt_index,
                        now,
                    ],
                )?;
                transaction.execute(
                    "UPDATE jobs SET attempt_id = ?2 WHERE id = ?1 AND state = 'pending'",
                    params![job_key, attempt_id.entity_uuid().to_string()],
                )?;
                attempt_id
            }
        };
        let invocation_id = InvocationId::new(store_uuid);
        let containment_id = ContainmentId::new(store_uuid);
        let role_index: u32 = transaction.query_row(
            "SELECT COALESCE(MAX(role_index), -1) + 1 FROM invocations WHERE attempt_id = ?1",
            [attempt_id.entity_uuid().to_string()],
            |row| row.get(0),
        )?;
        transaction.execute(
            "INSERT INTO invocations(
                id, attempt_id, role, role_index, condition_id, state
             ) VALUES (?1, ?2, 'probe', ?3, ?4, 'prepared')",
            params![
                invocation_id.entity_uuid().to_string(),
                attempt_id.entity_uuid().to_string(),
                role_index,
                condition_key,
            ],
        )?;
        transaction.execute(
            "INSERT INTO containments(
                id, invocation_id, state, host_id, boot_id, daemon_generation, strength, version
             ) VALUES (?1, ?2, 'creating', ?3, ?4, ?5, 'windows_job_object', 1)",
            params![
                containment_id.entity_uuid().to_string(),
                invocation_id.entity_uuid().to_string(),
                startup_identity.host_id.as_ref().map(|value| &value.0),
                startup_identity.boot_id.as_ref().map(|value| &value.0),
                daemon_generation.to_string(),
            ],
        )?;
        transaction.execute(
            "INSERT INTO leases(id, attempt_id, invocation_id, state, claims_json)
             VALUES (?1, ?2, ?3, 'granted', ?4)",
            params![
                Uuid::now_v7().to_string(),
                attempt_id.entity_uuid().to_string(),
                invocation_id.entity_uuid().to_string(),
                serde_json::to_string(&claims)?,
            ],
        )?;
        transaction.execute(
            "UPDATE conditions SET probe_invocation_id = ?2, next_probe_ms = NULL
             WHERE id = ?1 AND probe_invocation_id IS NULL",
            params![condition_key, invocation_id.entity_uuid().to_string()],
        )?;

        let log_directory = paths.logs.join(job_id.entity_uuid().to_string());
        std::fs::create_dir_all(&log_directory)?;
        let prepared = PreparedJob {
            job_id,
            attempt_id,
            invocation_id,
            containment_id,
            spec: probe_spec,
            stdout_path: log_directory.join(format!("{invocation_id}.stdout")),
            stderr_path: log_directory.join(format!("{invocation_id}.stderr")),
            stdin: None,
            stdin_path: None,
            role: InvocationRole::Probe,
            condition_id: Some(ConditionId::from_parts(
                store_uuid,
                Uuid::parse_str(&condition_key)?,
            )),
            attempt_deadline_unix_millis: Some(now.saturating_add(
                i64::try_from(probe.timeout_seconds.saturating_mul(1_000)).unwrap_or(i64::MAX),
            )),
            host_id: startup_identity.host_id.clone(),
            boot_id: startup_identity.boot_id.clone(),
            primary_result: None,
        };
        let _ = condition_index;
        transaction.commit()?;
        Ok(Some(prepared))
    }

    pub(crate) fn settle_probe(
        &mut self,
        probe_job: &PreparedJob,
        exit_code: Option<i32>,
        timed_out: bool,
    ) -> StoreResult<()> {
        let condition_id = probe_job.condition_id.ok_or_else(|| {
            StoreError::InvalidState("probe Invocation has no Condition identity".into())
        })?;
        let now = now_millis();
        // Diagnostic reads are deliberately outside the authoritative write transaction. A tail
        // read failure is represented in-band and cannot split probe cleanup into two commits.
        let stdout_tail = read_diagnostic_tail(&probe_job.stdout_path)
            .unwrap_or_else(|error| format!("[stillyard stdout tail unavailable: {error}]"));
        let stderr_tail = read_diagnostic_tail(&probe_job.stderr_path)
            .unwrap_or_else(|error| format!("[stillyard stderr tail unavailable: {error}]"));
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (condition_json, current_probe, condition_state, job_state, cancel_requested): (
            String,
            Option<String>,
            String,
            String,
            bool,
        ) = transaction.query_row(
            "SELECT conditions.spec_json, conditions.probe_invocation_id,
                        conditions.state, jobs.state, jobs.cancel_requested != 0
                 FROM conditions JOIN jobs ON jobs.id = conditions.job_id
                 WHERE conditions.id = ?1 AND conditions.job_id = ?2",
            params![
                condition_id.entity_uuid().to_string(),
                probe_job.job_id.entity_uuid().to_string(),
            ],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )?;
        if current_probe.as_deref()
            != Some(probe_job.invocation_id.entity_uuid().to_string().as_str())
        {
            return Err(StoreError::InvalidState(
                "resolved probe is not the Condition's unresolved Invocation".into(),
            ));
        }
        let containment_state: String = transaction.query_row(
            "SELECT state FROM containments WHERE id = ?1 AND invocation_id = ?2",
            params![
                probe_job.containment_id.entity_uuid().to_string(),
                probe_job.invocation_id.entity_uuid().to_string(),
            ],
            |row| row.get(0),
        )?;
        if !matches!(containment_state.as_str(), "creating" | "live") {
            return Err(StoreError::InvalidState(format!(
                "probe cannot settle without an owned empty proof from {containment_state}"
            )));
        }
        let condition: crate::ConditionSpec = serde_json::from_str(&condition_json)?;
        let ConditionPredicate::Probe { probe } = condition.predicate else {
            return Err(StoreError::InvalidState(
                "probe Invocation belongs to a non-probe Condition".into(),
            ));
        };
        let accepted =
            !timed_out && exit_code.is_some_and(|code| probe.accepted_exit_codes.contains(&code));
        let value = ConditionObservationValue::Probe {
            exit_code,
            timed_out,
            accepted,
        };
        transaction.execute(
            "UPDATE invocations SET state = 'resolved',
                root_exit_code = COALESCE(?2, root_exit_code),
                exit_classification = ?3, finished_ms = ?4,
                stdout_tail = ?5, stderr_tail = ?6
             WHERE id = ?1 AND state IN ('prepared', 'started', 'exited', 'resolved')",
            params![
                probe_job.invocation_id.entity_uuid().to_string(),
                exit_code,
                if accepted { "accepted" } else { "failed" },
                now,
                stdout_tail,
                stderr_tail,
            ],
        )?;
        transaction.execute(
            "UPDATE containments SET state = 'empty'
             WHERE id = ?1 AND state IN ('creating', 'live')",
            [probe_job.containment_id.entity_uuid().to_string()],
        )?;
        let (_, monotonic) = crate::host_observation::observation_clock().unwrap_or((now, 0));
        let observation_id = ObservationId::new(self.store_uuid);
        transaction.execute(
            "INSERT INTO observations(
                id, condition_id, observed_ms, observed_monotonic_ms, boot_id,
                daemon_generation, fresh_until_ms, source, value_json
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?3, 'probe', ?7)",
            params![
                observation_id.entity_uuid().to_string(),
                condition_id.entity_uuid().to_string(),
                now,
                monotonic,
                self.startup_identity
                    .boot_id
                    .as_ref()
                    .map(|boot| boot.0.as_str()),
                self.daemon_generation.to_string(),
                serde_json::to_string(&value)?,
            ],
        )?;
        transaction.execute(
            "DELETE FROM observations WHERE condition_id = ?1 AND id NOT IN (
                 SELECT id FROM observations WHERE condition_id = ?1
                 ORDER BY rowid DESC LIMIT ?2
             )",
            params![
                condition_id.entity_uuid().to_string(),
                MAX_RETAINED_OBSERVATIONS_PER_CONDITION
            ],
        )?;
        let next_probe = (!accepted
            && condition_state != "failed"
            && job_state == "pending"
            && !cancel_requested)
            .then(|| {
                now.saturating_add(
                    i64::try_from(probe.interval_seconds.saturating_mul(1_000)).unwrap_or(i64::MAX),
                )
            });
        let next_state = if condition_state == "failed" || job_state != "pending" {
            condition_state.as_str()
        } else if accepted {
            "satisfied"
        } else {
            "waiting"
        };
        transaction.execute(
            "UPDATE conditions SET state = ?2, probe_invocation_id = NULL,
                next_probe_ms = ?3 WHERE id = ?1",
            params![
                condition_id.entity_uuid().to_string(),
                next_state,
                next_probe,
            ],
        )?;
        transaction.execute(
            "UPDATE leases SET state = 'released'
             WHERE invocation_id = ?1 AND state = 'granted'",
            [probe_job.invocation_id.entity_uuid().to_string()],
        )?;
        finalize_pending_condition_terminal_if_ready_tx(
            &transaction,
            &probe_job.job_id.entity_uuid().to_string(),
            now,
        )?;
        transaction.commit()?;
        // History GC is ancillary to the already committed authoritative probe settlement. A GC
        // failure must not make the runner retain an empty boundary or retry a non-replayable
        // settlement call.
        let _ = self.prune_condition_history();
        Ok(())
    }

    pub(crate) fn mark_probe_uncertain(
        &mut self,
        probe_job: &PreparedJob,
        exit_code: Option<i32>,
    ) -> StoreResult<()> {
        let condition_id = probe_job.condition_id.ok_or_else(|| {
            StoreError::InvalidState("probe Invocation has no Condition identity".into())
        })?;
        let transaction = self.connection.transaction()?;
        let incident_sequence: u64 = transaction.query_row(
            "SELECT COALESCE(MAX(incident_sequence), 0) + 1 FROM containments",
            [],
            |row| row.get(0),
        )?;
        transaction.execute(
            "UPDATE invocations SET state = 'resolved', root_exit_code = COALESCE(?2, root_exit_code),
                finished_ms = ?3 WHERE id = ?1",
            params![
                probe_job.invocation_id.entity_uuid().to_string(),
                exit_code,
                now_millis(),
            ],
        )?;
        transaction.execute(
            "UPDATE containments SET state = 'uncertain', version = version + 1,
                incident_sequence = COALESCE(incident_sequence, ?2),
                reason_code = 'probe_cleanup_uncertain',
                detail = 'probe cleanup could not be proven within the bounded runner wait',
                opened_ms = COALESCE(opened_ms, ?3), retained_claims_json = ?4
             WHERE id = ?1 AND state IN ('creating', 'live')",
            params![
                probe_job.containment_id.entity_uuid().to_string(),
                incident_sequence,
                now_millis(),
                serde_json::to_string(&probe_job.spec.resources)?,
            ],
        )?;
        transaction.execute(
            "UPDATE conditions SET state = CASE WHEN state = 'failed' THEN state ELSE 'waiting' END
             WHERE id = ?1",
            [condition_id.entity_uuid().to_string()],
        )?;
        let job_key = probe_job.job_id.entity_uuid().to_string();
        finalize_pending_condition_terminal_if_ready_tx(&transaction, &job_key, now_millis())?;
        transaction.commit()?;
        Ok(())
    }
}

fn resolve_deadline(deadline: ConditionDeadline, accepted_ms: i64) -> Option<i64> {
    match deadline {
        ConditionDeadline::None => None,
        ConditionDeadline::Relative { seconds } => Some(
            accepted_ms
                .saturating_add(i64::try_from(seconds.saturating_mul(1_000)).unwrap_or(i64::MAX)),
        ),
        ConditionDeadline::Absolute { unix_millis } => Some(unix_millis),
    }
}

fn deadline_outcome_string(outcome: ConditionDeadlineOutcome) -> &'static str {
    match outcome {
        ConditionDeadlineOutcome::Failed => "failed",
        ConditionDeadlineOutcome::Canceled => "canceled",
    }
}

fn condition_path(condition: &crate::ConditionSpec) -> Option<&Path> {
    match &condition.predicate {
        ConditionPredicate::PathExists { path }
        | ConditionPredicate::PathAbsent { path }
        | ConditionPredicate::PathTransition { path, .. } => Some(path),
        ConditionPredicate::NotBefore { .. } | ConditionPredicate::Probe { .. } => None,
    }
}

fn inspect_path_bounded(path: &Path, timeout: Duration) -> PathInspection {
    #[cfg(test)]
    if path.file_name() == Some(std::ffi::OsStr::new("stillyard-test-slow-condition")) {
        std::thread::sleep(Duration::from_millis(150));
        return PathInspection::Present(false);
    }

    struct Request {
        path: PathBuf,
        response: std::sync::mpsc::SyncSender<std::io::Result<bool>>,
    }
    static INSPECTOR: std::sync::OnceLock<
        std::result::Result<std::sync::mpsc::SyncSender<Request>, String>,
    > = std::sync::OnceLock::new();

    let sender = INSPECTOR.get_or_init(|| {
        let (sender, receiver) = std::sync::mpsc::sync_channel::<Request>(PATH_INSPECTION_QUEUE);
        let receiver = std::sync::Arc::new(std::sync::Mutex::new(receiver));
        for index in 0..PATH_INSPECTION_WORKERS {
            let receiver = std::sync::Arc::clone(&receiver);
            std::thread::Builder::new()
                .name(format!("stillyard-path-inspector-{index}"))
                .spawn(move || {
                    loop {
                        let request = match receiver.lock() {
                            Ok(receiver) => receiver.recv(),
                            Err(_) => return,
                        };
                        let Ok(request) = request else {
                            return;
                        };
                        let result = match std::fs::symlink_metadata(&request.path) {
                            Ok(_) => Ok(true),
                            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
                            Err(error) => Err(error),
                        };
                        let _ = request.response.send(result);
                    }
                })
                .map_err(|error| format!("filesystem inspector unavailable: {error}"))?;
        }
        Ok(sender)
    });
    let sender = match sender {
        Ok(sender) => sender,
        Err(error) => return PathInspection::Invalidated(error.clone()),
    };
    let (response, receiver) = std::sync::mpsc::sync_channel(1);
    if sender
        .try_send(Request {
            path: path.to_path_buf(),
            response,
        })
        .is_err()
    {
        return PathInspection::Invalidated(
            "filesystem rescan deferred because the bounded inspector is busy".into(),
        );
    }
    match receiver.recv_timeout(timeout) {
        Ok(Ok(exists)) => PathInspection::Present(exists),
        Ok(Err(error)) => PathInspection::Invalidated(format!("filesystem rescan failed: {error}")),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => PathInspection::Invalidated(format!(
            "filesystem rescan exceeded the {}ms provider bound",
            PATH_INSPECTION_TIMEOUT.as_millis()
        )),
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            PathInspection::Invalidated("filesystem inspector disconnected".into())
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn observation_is_stale(
    fresh_until: Option<i64>,
    generation: Option<&str>,
    observed_monotonic: Option<u64>,
    daemon_generation: Uuid,
    now: i64,
    now_monotonic: u64,
    freshness_millis: u64,
) -> bool {
    generation != Some(daemon_generation.to_string().as_str())
        || fresh_until.is_none_or(|deadline| deadline <= now)
        || observed_monotonic.is_none_or(|observed| {
            now_monotonic
                .checked_sub(observed)
                .is_none_or(|elapsed| elapsed >= freshness_millis)
        })
}

fn observation_source_string(source: ConditionObservationSource) -> &'static str {
    match source {
        ConditionObservationSource::FilesystemRescan => "filesystem_rescan",
        ConditionObservationSource::Clock => "clock",
        ConditionObservationSource::Probe => "probe",
        ConditionObservationSource::Invalidation => "invalidation",
    }
}

fn parse_observation_source(value: &str) -> StoreResult<ConditionObservationSource> {
    match value {
        "filesystem_rescan" => Ok(ConditionObservationSource::FilesystemRescan),
        "clock" => Ok(ConditionObservationSource::Clock),
        "probe" => Ok(ConditionObservationSource::Probe),
        "invalidation" => Ok(ConditionObservationSource::Invalidation),
        other => Err(StoreError::InvalidState(format!(
            "unknown Condition observation source {other}"
        ))),
    }
}

fn parse_condition_state(value: &str) -> StoreResult<ConditionState> {
    match value {
        "waiting" => Ok(ConditionState::Waiting),
        "satisfied" => Ok(ConditionState::Satisfied),
        "failed" => Ok(ConditionState::Failed),
        other => Err(StoreError::InvalidState(format!(
            "unknown Condition state {other}"
        ))),
    }
}
