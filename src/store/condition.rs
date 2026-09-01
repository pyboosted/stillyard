use super::schedule::condition_blockers_tx;
use super::*;

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

pub(super) fn insert_conditions_tx(
    transaction: &Transaction<'_>,
    job_id: JobId,
    conditions: &[crate::ConditionSpec],
    acceptance: ConditionAcceptance<'_>,
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
                monotonic_now(),
                acceptance.freshness_millis,
                true,
            )?;
        }
    }
    Ok(())
}

pub(super) fn refresh_conditions_tx(
    transaction: &Transaction<'_>,
    daemon_generation: Uuid,
    boot_id: Option<&BootId>,
    job_key: &str,
    now: i64,
    freshness_millis: u64,
    force: bool,
) -> StoreResult<ConditionRefresh> {
    let expired: Option<(String, String)> = transaction
        .query_row(
            "SELECT id, deadline_outcome FROM conditions
             WHERE job_id = ?1 AND deadline_ms IS NOT NULL AND deadline_ms <= ?2
             ORDER BY deadline_ms, condition_index LIMIT 1",
            params![job_key, now],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if let Some((_condition, outcome)) = expired {
        release_reservation_tx(transaction, job_key)?;
        transaction.execute(
            "UPDATE attempts SET state = 'settled', verdict = ?2,
                safety_reason = 'condition_deadline_expired', finished_ms = ?3
             WHERE id = (SELECT attempt_id FROM jobs WHERE id = ?1)
               AND state IN ('planned', 'admitting')",
            params![
                job_key,
                if outcome == "canceled" {
                    "canceled"
                } else {
                    "safety_failed"
                },
                now,
            ],
        )?;
        transaction.execute(
            "UPDATE conditions SET state = 'failed' WHERE job_id = ?1 AND state != 'failed'",
            [job_key],
        )?;
        transaction.execute(
            "UPDATE jobs SET state = 'final', outcome = ?2,
                reason_code = 'condition_deadline_expired', finished_ms = ?3,
                retry_not_before_ms = NULL, reservation_not_before_ms = NULL
             WHERE id = ?1 AND state = 'pending'",
            params![
                job_key,
                if outcome == "canceled" {
                    "canceled"
                } else {
                    "failed"
                },
                now,
            ],
        )?;
        return Ok(ConditionRefresh {
            blockers: vec![Blocker {
                code: "condition_deadline_expired".into(),
                detail: format!("deadline outcome={outcome}"),
            }],
            state_changed: true,
            deadline_expired: true,
        });
    }

    let now_monotonic = monotonic_now();
    let mut statement = transaction.prepare(
        "SELECT id, state, spec_json,
                (SELECT fresh_until_ms FROM observations
                 WHERE condition_id = conditions.id ORDER BY rowid DESC LIMIT 1),
                (SELECT daemon_generation FROM observations
                 WHERE condition_id = conditions.id ORDER BY rowid DESC LIMIT 1)
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
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    drop(statement);
    let mut changed = false;
    for (condition_key, prior, spec_json, fresh_until, generation) in rows {
        let spec: crate::ConditionSpec = serde_json::from_str(&spec_json)?;
        if matches!(&spec.predicate, ConditionPredicate::Probe { .. }) {
            continue;
        }
        let stale = fresh_until.is_none_or(|deadline| deadline <= now)
            || generation.as_deref() != Some(&daemon_generation.to_string());
        if force || stale {
            let state = evaluate_one_tx(
                transaction,
                daemon_generation,
                boot_id,
                &condition_key,
                &spec,
                now,
                now_monotonic,
                freshness_millis,
                force,
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
    _force: bool,
) -> StoreResult<String> {
    let (value, state, transition_armed, source) = match &spec.predicate {
        ConditionPredicate::PathExists { path } => match path_presence(path) {
            Ok(exists) => (
                ConditionObservationValue::Path { exists },
                if exists { "satisfied" } else { "waiting" },
                None,
                ConditionObservationSource::FilesystemRescan,
            ),
            Err(error) => (
                ConditionObservationValue::Invalidated {
                    reason: format!("filesystem rescan failed: {error}"),
                },
                "waiting",
                None,
                ConditionObservationSource::Invalidation,
            ),
        },
        ConditionPredicate::PathAbsent { path } => match path_presence(path) {
            Ok(exists) => (
                ConditionObservationValue::Path { exists },
                if exists { "waiting" } else { "satisfied" },
                None,
                ConditionObservationSource::FilesystemRescan,
            ),
            Err(error) => (
                ConditionObservationValue::Invalidated {
                    reason: format!("filesystem rescan failed: {error}"),
                },
                "waiting",
                None,
                ConditionObservationSource::Invalidation,
            ),
        },
        ConditionPredicate::PathTransition { path, from, to } => match path_presence(path) {
            Ok(exists) => {
                let current = if exists {
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
                    ConditionObservationValue::Path { exists },
                    if satisfied { "satisfied" } else { "waiting" },
                    Some(armed),
                    ConditionObservationSource::FilesystemRescan,
                )
            }
            Err(error) => (
                ConditionObservationValue::Invalidated {
                    reason: format!("filesystem rescan failed: {error}"),
                },
                "waiting",
                None,
                ConditionObservationSource::Invalidation,
            ),
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
        "UPDATE conditions SET state = ?2,
            transition_armed = COALESCE(?3, transition_armed) WHERE id = ?1",
        params![condition_key, state, transition_armed],
    )?;
    Ok(state.into())
}

impl Store {
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
        now: i64,
        observation: Option<crate::host_observation::ObservationMoment<'_>>,
    ) -> StoreResult<Option<PreparedJob>> {
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
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (condition_json, current_probe, condition_state, job_state): (
            String,
            Option<String>,
            String,
            String,
        ) = transaction.query_row(
            "SELECT conditions.spec_json, conditions.probe_invocation_id,
                        conditions.state, jobs.state
                 FROM conditions JOIN jobs ON jobs.id = conditions.job_id
                 WHERE conditions.id = ?1 AND conditions.job_id = ?2",
            params![
                condition_id.entity_uuid().to_string(),
                probe_job.job_id.entity_uuid().to_string(),
            ],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        if current_probe.as_deref()
            != Some(probe_job.invocation_id.entity_uuid().to_string().as_str())
        {
            return Err(StoreError::InvalidState(
                "resolved probe is not the Condition's unresolved Invocation".into(),
            ));
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
            "UPDATE invocations SET exit_classification = ?2 WHERE id = ?1",
            params![
                probe_job.invocation_id.entity_uuid().to_string(),
                if accepted { "accepted" } else { "failed" },
            ],
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
        let next_probe = (!accepted && job_state == "pending").then(|| {
            now.saturating_add(
                i64::try_from(probe.interval_seconds.saturating_mul(1_000)).unwrap_or(i64::MAX),
            )
        });
        let next_state = if job_state != "pending" {
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
        transaction.commit()?;
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

fn path_presence(path: &Path) -> std::io::Result<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

fn monotonic_now() -> u64 {
    crate::host_observation::observation_clock()
        .map(|(_, monotonic)| monotonic)
        .unwrap_or(0)
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
