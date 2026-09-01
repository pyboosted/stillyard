use super::*;

impl Store {
    pub(super) fn advance_observed_admission(
        &mut self,
        job_id: JobId,
        observation: Option<crate::host_observation::ObservationMoment<'_>>,
    ) -> StoreResult<PrepareJob> {
        let job_key = self.local_id(job_id)?;
        let capacities = self.capacities.clone();
        let impact_incompatibilities = self.impact_incompatibilities.clone();
        let store_uuid = self.store_uuid;
        let host_config = self.host_config();
        let condition_spec = self
            .connection
            .query_row(
                "SELECT spec_json FROM jobs WHERE id = ?1 AND state = 'pending'
                   AND COALESCE(retry_not_before_ms, 0) <= ?2",
                params![&job_key, now_millis()],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|json| serde_json::from_str::<JobSpec>(&json))
            .transpose()?;
        let Some(condition_spec) = condition_spec else {
            return Ok(PrepareJob::Blocked);
        };
        let condition_evaluations = ConditionEvaluations::scan_all(&condition_spec.conditions);
        let now = now_millis();
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if expire_due_reservations_tx(&transaction, now)? {
            transaction.commit()?;
            return Ok(PrepareJob::StateChanged);
        }
        let row = transaction
            .query_row(
                "SELECT spec_json, claims_json, stdin_hash, stdin_len, attempt_id
                 FROM jobs WHERE id = ?1 AND state = 'pending'
                   AND COALESCE(retry_not_before_ms, 0) <= ?2",
                params![job_key, now],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<u64>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                    ))
                },
            )
            .optional()?;
        let Some((spec_json, claims_json, stdin_hash, stdin_len, attempt_key)) = row else {
            transaction.rollback()?;
            return Ok(PrepareJob::Blocked);
        };
        let spec: JobSpec = serde_json::from_str(&spec_json)?;
        if expire_condition_deadline_tx(&transaction, &job_key, now)?.is_some() {
            transaction.commit()?;
            return Ok(PrepareJob::StateChanged);
        }
        let quiet = spec.quiet.as_ref();
        let (dependency_blockers, impossible) = dependency_blockers_tx(&transaction, job_id)?;
        if impossible {
            if let Some(attempt_key) = &attempt_key {
                transaction.execute(
                    "UPDATE attempts SET state = 'settled', verdict = 'safety_failed',
                        safety_reason = 'dependency_impossible', finished_ms = ?2
                     WHERE id = ?1 AND state IN ('planned', 'admitting')",
                    params![attempt_key, now],
                )?;
            }
            transaction.execute(
                "UPDATE jobs SET state = 'final', outcome = 'skipped', finished_ms = ?2
                 WHERE id = ?1 AND state = 'pending'",
                params![job_key, now],
            )?;
            transaction.commit()?;
            return Ok(PrepareJob::StateChanged);
        }
        if !dependency_blockers.is_empty() {
            if release_reservation_tx(&transaction, &job_key)? {
                transaction.commit()?;
                return Ok(PrepareJob::StateChanged);
            }
            transaction.rollback()?;
            return Ok(PrepareJob::Blocked);
        }
        let condition_refresh = refresh_conditions_tx(
            &transaction,
            &job_key,
            ConditionRefreshContext {
                daemon_generation: self.daemon_generation,
                boot_id: self.startup_identity.boot_id.as_ref(),
                now,
                freshness_millis: self.observation_config.condition_rescan_interval_millis,
                force: false,
                evaluations: &condition_evaluations,
            },
        )?;
        if condition_refresh.deadline_expired {
            transaction.commit()?;
            return Ok(PrepareJob::StateChanged);
        }
        if !condition_refresh.blockers.is_empty() {
            let reservation_released = release_reservation_tx(&transaction, &job_key)?;
            if reservation_released || condition_refresh.state_changed {
                transaction.commit()?;
            } else {
                transaction.rollback()?;
            }
            if reservation_released {
                return Ok(PrepareJob::StateChanged);
            }
            let observation = observation
                .map(|moment| moment.after_provider_work(condition_evaluations.scan_elapsed()))
                .transpose()?;
            return Ok(match self.prepare_due_probe(job_id, &spec, observation)? {
                Some(probe) => PrepareJob::Ready(Box::new(probe)),
                None => PrepareJob::Blocked,
            });
        }

        let claims: ResolvedClaims = serde_json::from_str(&claims_json)?;
        if attempt_key.is_none() {
            let attempt_id = AttemptId::new(self.store_uuid);
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
            transaction.commit()?;
            return Ok(PrepareJob::StateChanged);
        }
        let attempt_key = attempt_key.expect("checked above");
        let attempt_id = AttemptId::from_parts(self.store_uuid, Uuid::parse_str(&attempt_key)?);
        let active = active_claims_tx(&transaction)?;
        let mut static_blockers = claims.scalar_blockers(&capacities, &[]);
        static_blockers.extend(claims.non_scalar_blockers(&active, &impact_incompatibilities));
        ensure_admitting_row(
            &transaction,
            &attempt_key,
            now,
            host_config.observation.admission_wall_clock_limit_seconds,
        )?;
        if !static_blockers.is_empty() {
            release_reservation_tx(&transaction, &job_key)?;
            if admission_deadline_expired(&transaction, &attempt_key, now)? {
                settle_admission(
                    &transaction,
                    &spec,
                    &job_key,
                    &attempt_key,
                    "admission_starved",
                    now,
                )?;
                transaction.commit()?;
                return Ok(PrepareJob::StateChanged);
            }
            record_admission_blockers(&transaction, &attempt_key, &static_blockers)?;
            pause_quiet_progress(&transaction, &attempt_key)?;
            transaction.commit()?;
            return Ok(PrepareJob::Blocked);
        }

        transaction.execute(
            "UPDATE attempts SET state = 'admitting' WHERE id = ?1 AND state = 'planned'",
            [&attempt_key],
        )?;
        let Some(observation) = observation else {
            release_reservation_tx(&transaction, &job_key)?;
            let blocker = Blocker {
                code: "observation_missing".into(),
                detail: "host observation sample is not available".into(),
            };
            record_admission_blockers(&transaction, &attempt_key, &[blocker])?;
            pause_quiet_progress(&transaction, &attempt_key)?;
            if admission_deadline_expired(&transaction, &attempt_key, now)? {
                settle_admission(
                    &transaction,
                    &spec,
                    &job_key,
                    &attempt_key,
                    "admission_starved",
                    now,
                )?;
                transaction.commit()?;
                return Ok(PrepareJob::StateChanged);
            }
            transaction.commit()?;
            return Ok(PrepareJob::Blocked);
        };
        let observation = observation.after_provider_work(condition_evaluations.scan_elapsed())?;
        let mut context = crate::host_observation::evaluate_admission(
            &spec,
            &host_config,
            observation.sample,
            &active,
            observation.now_unix_millis,
            observation.now_monotonic_millis,
        );
        let admission = admission_state(&transaction, &attempt_key)?;
        if now >= admission.wall_deadline_ms {
            let reason = if quiet.is_some() && context.non_quiet_blockers.is_empty() {
                "quiet_unattainable"
            } else {
                "admission_starved"
            };
            settle_admission(&transaction, &spec, &job_key, &attempt_key, reason, now)?;
            transaction.commit()?;
            return Ok(PrepareJob::StateChanged);
        }
        if !context.non_quiet_blockers.is_empty() {
            release_reservation_tx(&transaction, &job_key)?;
            record_admission_context(&transaction, &attempt_key, &context)?;
            pause_quiet_progress(&transaction, &attempt_key)?;
            transaction.commit()?;
            return Ok(PrepareJob::Blocked);
        }

        if let Some(quiet) = quiet {
            let consumed = accumulated_quiet_budget(&admission, &context);
            let budget_millis = quiet.wait_budget_seconds.saturating_mul(1_000);
            if consumed >= budget_millis {
                settle_admission(
                    &transaction,
                    &spec,
                    &job_key,
                    &attempt_key,
                    "quiet_unattainable",
                    now,
                )?;
                transaction.commit()?;
                return Ok(PrepareJob::StateChanged);
            }
            let stability = next_stability(
                &admission,
                &context,
                host_config.observation.quiet_max_sample_gap_millis,
            );
            let stable_millis = quiet.stable_seconds.saturating_mul(1_000);
            let stable = stability.first_monotonic_ms.is_some_and(|first| {
                context
                    .evaluated_monotonic_millis
                    .checked_sub(first)
                    .is_some_and(|elapsed| elapsed >= stable_millis)
            });
            if !stable && context.quiet_blockers.is_empty() {
                let elapsed = stability
                    .first_monotonic_ms
                    .and_then(|first| context.evaluated_monotonic_millis.checked_sub(first))
                    .unwrap_or(0);
                let blocker = Blocker {
                    code: "quiet_waiting".into(),
                    detail: format!(
                        "quiet sample qualifies for {elapsed}ms of required {stable_millis}ms"
                    ),
                };
                context.quiet_blockers.push(blocker.clone());
                context.blockers.push(blocker);
            }
            update_admission_progress(&transaction, &attempt_key, &context, consumed, stability)?;
            if !stable {
                release_reservation_tx(&transaction, &job_key)?;
                transaction.commit()?;
                return Ok(PrepareJob::Blocked);
            }
        }

        let final_conditions = refresh_conditions_tx(
            &transaction,
            &job_key,
            ConditionRefreshContext {
                daemon_generation: self.daemon_generation,
                boot_id: self.startup_identity.boot_id.as_ref(),
                now: now_millis(),
                freshness_millis: self.observation_config.condition_rescan_interval_millis,
                force: true,
                evaluations: &condition_evaluations,
            },
        )?;
        if final_conditions.deadline_expired || !final_conditions.blockers.is_empty() {
            release_reservation_tx(&transaction, &job_key)?;
            pause_quiet_progress(&transaction, &attempt_key)?;
            transaction.commit()?;
            return Ok(PrepareJob::StateChanged);
        }

        match scalar_disposition_tx(
            &transaction,
            store_uuid,
            &capacities,
            &job_key,
            &claims,
            &active,
            now,
        )? {
            ScalarDisposition::Grant => {}
            ScalarDisposition::Created | ScalarDisposition::StateChanged => {
                transaction.commit()?;
                return Ok(PrepareJob::StateChanged);
            }
            ScalarDisposition::Hold => {
                transaction.commit()?;
                return Ok(PrepareJob::Blocked);
            }
        }

        let stdin = staged_input(stdin_hash, stdin_len)?;
        validate_input_shape(&spec, stdin.as_ref())?;
        let log_directory = self.paths.logs.join(job_id.entity_uuid().to_string());
        std::fs::create_dir_all(&log_directory)?;
        let invocation_id = InvocationId::new(self.store_uuid);
        let containment_id = ContainmentId::new(self.store_uuid);
        let lease_id = Uuid::now_v7();
        let waits_for_release = quiet.is_some() || !spec.conditions.is_empty();
        let attempt_started = (!waits_for_release).then_some(now);
        let attempt_deadline = attempt_started.and_then(|started| {
            spec.timeout_seconds.map(|seconds| {
                started.saturating_add(
                    i64::try_from(seconds.saturating_mul(1_000)).unwrap_or(i64::MAX),
                )
            })
        });
        let role_index: u32 = transaction.query_row(
            "SELECT COALESCE(MAX(role_index), -1) + 1 FROM invocations WHERE attempt_id = ?1",
            [&attempt_key],
            |row| row.get(0),
        )?;
        transaction.execute(
            "UPDATE jobs SET state = 'active', invocation_id = ?2, containment_id = ?3,
                stdout_len = 0, stderr_len = 0, retry_not_before_ms = NULL,
                reservation_not_before_ms = NULL
             WHERE id = ?1 AND state = 'pending' AND attempt_id = ?4",
            params![
                job_key,
                invocation_id.entity_uuid().to_string(),
                containment_id.entity_uuid().to_string(),
                attempt_key,
            ],
        )?;
        transaction.execute(
            "UPDATE attempts SET state = 'starting', started_ms = ?2, deadline_ms = ?3
             WHERE id = ?1 AND state = 'admitting'",
            params![attempt_key, attempt_started, attempt_deadline],
        )?;
        transaction.execute(
            "INSERT INTO invocations(id, attempt_id, role, role_index, state)
             VALUES (?1, ?2, 'primary', ?3, 'prepared')",
            params![
                invocation_id.entity_uuid().to_string(),
                attempt_key,
                role_index,
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
            params![lease_id.to_string(), attempt_key, claims_json],
        )?;
        let evidence = admission_evidence_json(&context)?;
        transaction.execute(
            "UPDATE admissions SET reservation_generation = ?2,
                reservation_evidence_json = ?3, gpu_uuid = ?4, gpu_driver_version = ?5
             WHERE attempt_id = ?1",
            params![
                attempt_key,
                context.observation_generation.to_string(),
                evidence,
                context.gpu_uuid,
                context.gpu_driver_version,
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
            condition_id: None,
            attempt_deadline_unix_millis: attempt_deadline,
            host_id: self.startup_identity.host_id.clone(),
            boot_id: self.startup_identity.boot_id.clone(),
            primary_result: None,
        })))
    }
}

struct AdmissionState {
    wall_deadline_ms: i64,
    quiet_consumed_ms: u64,
    last_eval_monotonic_ms: Option<u64>,
    last_eval_generation: Option<String>,
    quiet_generation: Option<String>,
    quiet_first_monotonic_ms: Option<u64>,
    quiet_last_monotonic_ms: Option<u64>,
}

#[derive(Clone, Copy)]
struct Stability {
    first_monotonic_ms: Option<u64>,
    last_monotonic_ms: Option<u64>,
}

fn admission_state(
    transaction: &Transaction<'_>,
    attempt_key: &str,
) -> StoreResult<AdmissionState> {
    transaction
        .query_row(
            "SELECT wall_deadline_ms, quiet_consumed_ms, last_eval_monotonic_ms,
                    last_eval_generation, quiet_generation, quiet_first_monotonic_ms,
                    quiet_last_monotonic_ms
             FROM admissions WHERE attempt_id = ?1",
            [attempt_key],
            |row| {
                Ok(AdmissionState {
                    wall_deadline_ms: row.get(0)?,
                    quiet_consumed_ms: row.get(1)?,
                    last_eval_monotonic_ms: row.get(2)?,
                    last_eval_generation: row.get(3)?,
                    quiet_generation: row.get(4)?,
                    quiet_first_monotonic_ms: row.get(5)?,
                    quiet_last_monotonic_ms: row.get(6)?,
                })
            },
        )
        .map_err(Into::into)
}

pub(super) fn ensure_admitting_row(
    transaction: &Transaction<'_>,
    attempt_key: &str,
    now: i64,
    wall_limit_seconds: u64,
) -> StoreResult<()> {
    let deadline = now.saturating_add(
        i64::try_from(wall_limit_seconds.saturating_mul(1_000)).unwrap_or(i64::MAX),
    );
    transaction.execute(
        "INSERT OR IGNORE INTO admissions(attempt_id, admitting_started_ms, wall_deadline_ms)
         VALUES (?1, ?2, ?3)",
        params![attempt_key, now, deadline],
    )?;
    Ok(())
}

fn admission_deadline_expired(
    transaction: &Transaction<'_>,
    attempt_key: &str,
    now: i64,
) -> StoreResult<bool> {
    let deadline = transaction
        .query_row(
            "SELECT wall_deadline_ms FROM admissions WHERE attempt_id = ?1",
            [attempt_key],
            |row| row.get::<_, i64>(0),
        )
        .optional()?;
    Ok(deadline.is_some_and(|deadline| now >= deadline))
}

fn accumulated_quiet_budget(
    admission: &AdmissionState,
    context: &crate::host_observation::AdmissionContext,
) -> u64 {
    let generation = context.observation_generation.to_string();
    let additional = match (
        admission.last_eval_generation.as_deref(),
        admission.last_eval_monotonic_ms,
    ) {
        (Some(previous_generation), Some(previous)) if previous_generation == generation => {
            context.evaluated_monotonic_millis.saturating_sub(previous)
        }
        _ => 0,
    };
    admission.quiet_consumed_ms.saturating_add(additional)
}

fn next_stability(
    admission: &AdmissionState,
    context: &crate::host_observation::AdmissionContext,
    maximum_gap_millis: u64,
) -> Stability {
    if !context.quiet_sample_satisfied || !context.quiet_blockers.is_empty() {
        return Stability {
            first_monotonic_ms: None,
            last_monotonic_ms: None,
        };
    }
    let generation = context.observation_generation.to_string();
    let continues = admission.quiet_generation.as_deref() == Some(generation.as_str())
        && admission
            .quiet_last_monotonic_ms
            .and_then(|last| context.evaluated_monotonic_millis.checked_sub(last))
            .is_some_and(|gap| gap <= maximum_gap_millis);
    Stability {
        first_monotonic_ms: if continues {
            admission.quiet_first_monotonic_ms
        } else {
            Some(context.evaluated_monotonic_millis)
        },
        last_monotonic_ms: Some(context.evaluated_monotonic_millis),
    }
}

fn update_admission_progress(
    transaction: &Transaction<'_>,
    attempt_key: &str,
    context: &crate::host_observation::AdmissionContext,
    consumed: u64,
    stability: Stability,
) -> StoreResult<()> {
    let evidence = admission_evidence_json(context)?;
    transaction.execute(
        "UPDATE admissions SET quiet_consumed_ms = ?2, last_eval_monotonic_ms = ?3,
            last_eval_generation = ?4, quiet_generation = ?4,
            quiet_first_monotonic_ms = ?5, quiet_last_monotonic_ms = ?6,
            last_blockers_json = ?7, last_eval_unix_ms = ?8,
            last_evidence_json = ?9 WHERE attempt_id = ?1",
        params![
            attempt_key,
            consumed,
            context.evaluated_monotonic_millis,
            context.observation_generation.to_string(),
            stability.first_monotonic_ms,
            stability.last_monotonic_ms,
            serde_json::to_string(&context.blockers)?,
            context.evaluated_unix_millis,
            evidence,
        ],
    )?;
    Ok(())
}

fn record_admission_context(
    transaction: &Transaction<'_>,
    attempt_key: &str,
    context: &crate::host_observation::AdmissionContext,
) -> StoreResult<()> {
    transaction.execute(
        "UPDATE admissions SET last_blockers_json = ?2, last_eval_unix_ms = ?3,
            last_evidence_json = ?4 WHERE attempt_id = ?1",
        params![
            attempt_key,
            serde_json::to_string(&context.blockers)?,
            context.evaluated_unix_millis,
            admission_evidence_json(context)?,
        ],
    )?;
    Ok(())
}

fn record_admission_blockers(
    transaction: &Transaction<'_>,
    attempt_key: &str,
    blockers: &[Blocker],
) -> StoreResult<()> {
    transaction.execute(
        "UPDATE admissions SET last_blockers_json = ?2 WHERE attempt_id = ?1",
        params![attempt_key, serde_json::to_string(blockers)?],
    )?;
    Ok(())
}

fn pause_quiet_progress(transaction: &Transaction<'_>, attempt_key: &str) -> StoreResult<()> {
    transaction.execute(
        "UPDATE admissions SET last_eval_monotonic_ms = NULL,
            last_eval_generation = NULL, quiet_generation = NULL,
            quiet_first_monotonic_ms = NULL, quiet_last_monotonic_ms = NULL
         WHERE attempt_id = ?1",
        [attempt_key],
    )?;
    Ok(())
}

fn settle_admission(
    transaction: &Transaction<'_>,
    spec: &JobSpec,
    job_key: &str,
    attempt_key: &str,
    reason: &str,
    now: i64,
) -> StoreResult<()> {
    let attempt_index: u32 = transaction.query_row(
        "SELECT attempt_index FROM attempts WHERE id = ?1",
        [attempt_key],
        |row| row.get(0),
    )?;
    transaction.execute(
        "UPDATE attempts SET state = 'settled', verdict = 'safety_failed',
            safety_reason = ?2, finished_ms = ?3 WHERE id = ?1",
        params![attempt_key, reason, now],
    )?;
    let retry = attempt_index < spec.retry.max_attempts
        && spec
            .retry
            .retryable
            .iter()
            .any(|value| value == "safety_failed");
    if retry {
        let not_before = now.saturating_add(
            i64::try_from(spec.retry.backoff_seconds.saturating_mul(1_000)).unwrap_or(i64::MAX),
        );
        transaction.execute(
            "UPDATE jobs SET attempt_id = NULL, invocation_id = NULL, containment_id = NULL,
                retry_not_before_ms = ?2 WHERE id = ?1 AND state = 'pending'",
            params![job_key, not_before],
        )?;
        reset_conditions_for_retry_tx(transaction, job_key, not_before)?;
    } else {
        transaction.execute(
            "UPDATE jobs SET state = 'final', outcome = 'failed', finished_ms = ?2,
                retry_not_before_ms = NULL WHERE id = ?1 AND state = 'pending'",
            params![job_key, now],
        )?;
    }
    Ok(())
}

fn staged_input(
    stdin_hash: Option<String>,
    stdin_len: Option<u64>,
) -> StoreResult<Option<StagedInputRef>> {
    match (stdin_hash, stdin_len) {
        (Some(sha256), Some(length)) => Ok(Some(StagedInputRef { sha256, length })),
        (None, None) => Ok(None),
        _ => Err(StoreError::InvalidState(
            "job has a partial staged stdin reference".into(),
        )),
    }
}

pub(super) fn admission_evidence_json(
    context: &crate::host_observation::AdmissionContext,
) -> StoreResult<String> {
    Ok(serde_json::to_string(&serde_json::json!({
        "evaluated_unix_millis": context.evaluated_unix_millis,
        "evaluated_monotonic_millis": context.evaluated_monotonic_millis,
        "observation_generation": context.observation_generation,
        "blockers": context.blockers,
        "operands": context.operands,
        "detectors": context.detectors,
        "gpu_uuid": context.gpu_uuid,
        "gpu_driver_version": context.gpu_driver_version,
    }))?)
}
