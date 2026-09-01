use super::*;

impl Store {
    pub(crate) fn record_suspended_root(
        &mut self,
        job: &PreparedJob,
        root_pid: u32,
        executable_hash: &str,
        root_identity: &ProcessIdentity,
    ) -> StoreResult<()> {
        let (root_host_id, root_boot_id, root_creation_filetime_100ns) =
            windows_identity_parts(root_identity, root_pid)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let state: (String, String, String) = transaction.query_row(
            "SELECT jobs.state, attempts.state, invocations.state
             FROM jobs JOIN attempts ON attempts.id = jobs.attempt_id
             JOIN invocations ON invocations.id = jobs.invocation_id
             WHERE jobs.id = ?1 AND attempts.id = ?2 AND invocations.id = ?3",
            params![
                job.job_id.entity_uuid().to_string(),
                job.attempt_id.entity_uuid().to_string(),
                job.invocation_id.entity_uuid().to_string(),
            ],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        if state != ("active".into(), "starting".into(), "prepared".into()) {
            return Err(StoreError::InvalidState(format!(
                "suspended root cannot be recorded from {}/{}/{}",
                state.0, state.1, state.2
            )));
        }
        transaction.execute(
            "UPDATE invocations SET root_pid = ?2, executable_hash = ?3,
                daemon_generation = ?4, root_host_id = ?5, root_boot_id = ?6,
                root_creation_filetime_100ns = ?7 WHERE id = ?1 AND state = 'prepared'",
            params![
                job.invocation_id.entity_uuid().to_string(),
                root_pid,
                executable_hash,
                self.daemon_generation.to_string(),
                root_host_id,
                root_boot_id,
                root_creation_filetime_100ns,
            ],
        )?;
        transaction.execute(
            "UPDATE containments SET state = 'live' WHERE id = ?1 AND state = 'creating'",
            [job.containment_id.entity_uuid().to_string()],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn authorize_release(
        &mut self,
        job: &PreparedJob,
        observation: crate::host_observation::ObservationMoment<'_>,
    ) -> StoreResult<ReleaseAuthorization> {
        let capacities = self.capacities.clone();
        let impact_incompatibilities = self.impact_incompatibilities.clone();
        let host_config = self.host_config();
        let condition_evaluations = ConditionEvaluations::scan_all(&job.spec.conditions);
        let evaluation_now = now_millis();
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let row: (String, String, String, bool, String, String) = transaction.query_row(
            "SELECT jobs.state, attempts.state, invocations.state,
                    jobs.cancel_requested != 0, jobs.spec_json, jobs.claims_json
             FROM jobs JOIN attempts ON attempts.id = jobs.attempt_id
             JOIN invocations ON invocations.id = jobs.invocation_id
             WHERE jobs.id = ?1 AND attempts.id = ?2 AND invocations.id = ?3",
            params![
                job.job_id.entity_uuid().to_string(),
                job.attempt_id.entity_uuid().to_string(),
                job.invocation_id.entity_uuid().to_string(),
            ],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )?;
        if row.0 != "active" || row.1 != "starting" || row.2 != "prepared" {
            return Err(StoreError::InvalidState(format!(
                "release cannot be authorized from {}/{}/{}",
                row.0, row.1, row.2
            )));
        }
        if row.3 {
            transaction.rollback()?;
            return Ok(ReleaseAuthorization::Deferred {
                reason: "cancel_requested".into(),
            });
        }
        let spec: JobSpec = serde_json::from_str(&row.4)?;
        let claims: ResolvedClaims = serde_json::from_str(&row.5)?;
        let active = active_claims_excluding_attempt(&transaction, job.attempt_id)?;
        let mut blockers = dependency_blockers_tx(&transaction, job.job_id)?.0;
        let condition_refresh = refresh_conditions_tx(
            &transaction,
            &job.job_id.entity_uuid().to_string(),
            ConditionRefreshContext {
                daemon_generation: self.daemon_generation,
                boot_id: self.startup_identity.boot_id.as_ref(),
                now: evaluation_now,
                freshness_millis: self.observation_config.condition_rescan_interval_millis,
                force: true,
                evaluations: &condition_evaluations,
            },
        )?;
        if condition_refresh.deadline_expired {
            let outcome = condition_refresh
                .blockers
                .first()
                .map(|blocker| blocker.detail.as_str())
                .unwrap_or("deadline outcome=failed");
            transaction.commit()?;
            return Ok(ReleaseAuthorization::Deferred {
                reason: format!("condition_deadline_expired:{outcome}"),
            });
        }
        blockers.extend(condition_refresh.blockers);
        blockers.extend(claims.blockers(&capacities, &active, &impact_incompatibilities));
        let observation = observation.after_provider_work(condition_evaluations.scan_elapsed())?;
        let context = crate::host_observation::evaluate_admission(
            &spec,
            &host_config,
            observation.sample,
            &active,
            observation.now_unix_millis,
            observation.now_monotonic_millis,
        );
        blockers.extend(context.blockers.clone());
        let reservation_generation: Option<String> = transaction.query_row(
            "SELECT reservation_generation FROM admissions WHERE attempt_id = ?1",
            [job.attempt_id.entity_uuid().to_string()],
            |row| row.get(0),
        )?;
        if reservation_generation.as_deref()
            != Some(context.observation_generation.to_string().as_str())
        {
            blockers.push(Blocker {
                code: "observation_generation_changed".into(),
                detail: "observation generation changed after reservation".into(),
            });
        }
        blockers.sort_by(|left, right| {
            left.code
                .cmp(&right.code)
                .then(left.detail.cmp(&right.detail))
        });
        blockers.dedup();
        if !blockers.is_empty() || !context.quiet_sample_satisfied {
            transaction.commit()?;
            return Ok(ReleaseAuthorization::Deferred {
                reason: serde_json::to_string(&blockers)?,
            });
        }
        let max_age = spec
            .minimum_observation_age_millis(&host_config)
            .ok_or_else(|| {
                StoreError::InvalidState("quiet release has no observation age bound".into())
            })?;
        let Some(mut expires) = observation
            .sample
            .captured_monotonic_millis
            .checked_add(max_age)
        else {
            transaction.rollback()?;
            return Ok(ReleaseAuthorization::Deferred {
                reason: "release evidence expiry overflow".into(),
            });
        };
        if !job.spec.conditions.is_empty() {
            expires = expires.min(
                condition_evaluations
                    .expires_monotonic(self.observation_config.condition_rescan_interval_millis),
            );
            let release_monotonic_now = crate::host_observation::observation_clock()
                .map(|(_, monotonic)| monotonic)
                .unwrap_or(u64::MAX);
            if release_monotonic_now >= expires {
                transaction.rollback()?;
                return Ok(ReleaseAuthorization::Deferred {
                    reason: "release evidence expired during the bounded Condition rescan".into(),
                });
            }
        }
        let started_now = now_millis();
        let runtime_deadline = spec.timeout_seconds.map(|seconds| {
            started_now
                .saturating_add(i64::try_from(seconds.saturating_mul(1_000)).unwrap_or(i64::MAX))
        });
        transaction.execute(
            "UPDATE invocations SET state = 'started', started_ms = ?2
             WHERE id = ?1 AND state = 'prepared'",
            params![job.invocation_id.entity_uuid().to_string(), started_now],
        )?;
        transaction.execute(
            "UPDATE attempts SET state = 'running', started_ms = ?2, deadline_ms = ?3
             WHERE id = ?1 AND state = 'starting'",
            params![
                job.attempt_id.entity_uuid().to_string(),
                started_now,
                runtime_deadline,
            ],
        )?;
        transaction.execute(
            "UPDATE jobs SET started_ms = COALESCE(started_ms, ?2)
             WHERE id = ?1 AND state = 'active'",
            params![job.job_id.entity_uuid().to_string(), started_now],
        )?;
        transaction.execute(
            "UPDATE admissions SET release_evidence_json = ?2,
                pre_resume_defer_reason = NULL WHERE attempt_id = ?1",
            params![
                job.attempt_id.entity_uuid().to_string(),
                super::admitting::admission_evidence_json(&context)?,
            ],
        )?;
        transaction.commit()?;
        Ok(ReleaseAuthorization::Authorized {
            runtime_deadline_unix_millis: runtime_deadline,
            evidence_expires_monotonic_millis: expires,
        })
    }

    pub(crate) fn authorize_condition_release(
        &mut self,
        job: &PreparedJob,
        observation: Option<crate::host_observation::ObservationMoment<'_>>,
    ) -> StoreResult<ReleaseAuthorization> {
        let capacities = self.capacities.clone();
        let impact_incompatibilities = self.impact_incompatibilities.clone();
        let host_config = self.host_config();
        let condition_evaluations = ConditionEvaluations::scan_all(&job.spec.conditions);
        let evaluation_now = now_millis();
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let row: (String, String, String, bool, String) = transaction.query_row(
            "SELECT jobs.state, attempts.state, invocations.state,
                    jobs.cancel_requested != 0, jobs.claims_json
             FROM jobs JOIN attempts ON attempts.id = jobs.attempt_id
             JOIN invocations ON invocations.id = jobs.invocation_id
             WHERE jobs.id = ?1 AND attempts.id = ?2 AND invocations.id = ?3",
            params![
                job.job_id.entity_uuid().to_string(),
                job.attempt_id.entity_uuid().to_string(),
                job.invocation_id.entity_uuid().to_string(),
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
        if row.0 != "active" || row.1 != "starting" || row.2 != "prepared" {
            return Err(StoreError::InvalidState(format!(
                "Condition release cannot be authorized from {}/{}/{}",
                row.0, row.1, row.2
            )));
        }
        if row.3 {
            transaction.rollback()?;
            return Ok(ReleaseAuthorization::Deferred {
                reason: "cancel_requested".into(),
            });
        }
        let refresh = refresh_conditions_tx(
            &transaction,
            &job.job_id.entity_uuid().to_string(),
            ConditionRefreshContext {
                daemon_generation: self.daemon_generation,
                boot_id: self.startup_identity.boot_id.as_ref(),
                now: evaluation_now,
                freshness_millis: self.observation_config.condition_rescan_interval_millis,
                force: true,
                evaluations: &condition_evaluations,
            },
        )?;
        if refresh.deadline_expired {
            let outcome = refresh
                .blockers
                .first()
                .map(|blocker| blocker.detail.as_str())
                .unwrap_or("deadline outcome=failed");
            transaction.commit()?;
            return Ok(ReleaseAuthorization::Deferred {
                reason: format!("condition_deadline_expired:{outcome}"),
            });
        }
        let claims: ResolvedClaims = serde_json::from_str(&row.4)?;
        let active = active_claims_excluding_attempt(&transaction, job.attempt_id)?;
        let mut blockers = dependency_blockers_tx(&transaction, job.job_id)?.0;
        blockers.extend(refresh.blockers);
        blockers.extend(claims.blockers(&capacities, &active, &impact_incompatibilities));
        let mut observation_captured_monotonic = None;
        let admission_context = if job.spec.requires_host_observation() {
            match observation {
                Some(observation) => {
                    let observation =
                        observation.after_provider_work(condition_evaluations.scan_elapsed())?;
                    observation_captured_monotonic =
                        Some(observation.sample.captured_monotonic_millis);
                    let context = crate::host_observation::evaluate_admission(
                        &job.spec,
                        &host_config,
                        observation.sample,
                        &active,
                        observation.now_unix_millis,
                        observation.now_monotonic_millis,
                    );
                    blockers.extend(context.blockers.clone());
                    let reservation_generation: Option<String> = transaction.query_row(
                        "SELECT reservation_generation FROM admissions WHERE attempt_id = ?1",
                        [job.attempt_id.entity_uuid().to_string()],
                        |row| row.get(0),
                    )?;
                    if reservation_generation.as_deref()
                        != Some(context.observation_generation.to_string().as_str())
                    {
                        blockers.push(Blocker {
                            code: "observation_generation_changed".into(),
                            detail: "observation generation changed after reservation".into(),
                        });
                    }
                    Some(context)
                }
                None => {
                    blockers.push(Blocker {
                        code: "observation_missing".into(),
                        detail: "release-time host observation sample is not available".into(),
                    });
                    None
                }
            }
        } else {
            None
        };
        blockers.sort_by(|left, right| {
            left.code
                .cmp(&right.code)
                .then(left.detail.cmp(&right.detail))
        });
        blockers.dedup();
        if !blockers.is_empty() {
            transaction.commit()?;
            return Ok(ReleaseAuthorization::Deferred {
                reason: serde_json::to_string(&blockers)?,
            });
        }
        let started_now = now_millis();
        let runtime_deadline = job.spec.timeout_seconds.map(|seconds| {
            started_now
                .saturating_add(i64::try_from(seconds.saturating_mul(1_000)).unwrap_or(i64::MAX))
        });
        transaction.execute(
            "UPDATE invocations SET state = 'started', started_ms = ?2
             WHERE id = ?1 AND state = 'prepared'",
            params![job.invocation_id.entity_uuid().to_string(), started_now],
        )?;
        transaction.execute(
            "UPDATE attempts SET state = 'running', started_ms = ?2, deadline_ms = ?3
             WHERE id = ?1 AND state = 'starting'",
            params![
                job.attempt_id.entity_uuid().to_string(),
                started_now,
                runtime_deadline,
            ],
        )?;
        transaction.execute(
            "UPDATE jobs SET started_ms = COALESCE(started_ms, ?2)
             WHERE id = ?1 AND state = 'active'",
            params![job.job_id.entity_uuid().to_string(), started_now],
        )?;
        let mut evidence_expires = condition_evaluations
            .expires_monotonic(self.observation_config.condition_rescan_interval_millis);
        if let (Some(captured_monotonic), Some(max_age)) = (
            observation_captured_monotonic,
            job.spec.minimum_observation_age_millis(&host_config),
        ) {
            evidence_expires = evidence_expires.min(captured_monotonic.saturating_add(max_age));
        }
        let monotonic_now = crate::host_observation::observation_clock()
            .map(|(_, monotonic)| monotonic)
            .unwrap_or(u64::MAX);
        if monotonic_now >= evidence_expires {
            transaction.rollback()?;
            return Ok(ReleaseAuthorization::Deferred {
                reason: "Condition release evidence expired during the bounded rescan".into(),
            });
        }
        let release_evidence = admission_context
            .as_ref()
            .map(super::admitting::admission_evidence_json)
            .transpose()?
            .unwrap_or_else(|| "{}".into());
        transaction.execute(
            "UPDATE admissions SET pre_resume_defer_reason = NULL,
                release_evidence_json = ?2 WHERE attempt_id = ?1",
            params![job.attempt_id.entity_uuid().to_string(), release_evidence,],
        )?;
        transaction.commit()?;
        Ok(ReleaseAuthorization::Authorized {
            runtime_deadline_unix_millis: runtime_deadline,
            evidence_expires_monotonic_millis: evidence_expires,
        })
    }

    pub(crate) fn pre_resume_defer_reason(&mut self, job_id: JobId) -> StoreResult<Option<String>> {
        let job_key = self.local_id(job_id)?;
        let now = now_millis();
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (state, cancel_requested): (String, bool) = transaction.query_row(
            "SELECT state, cancel_requested != 0 FROM jobs WHERE id = ?1",
            [&job_key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if state != "active" {
            transaction.rollback()?;
            return Ok(Some(format!(
                "job left active state before resume: {state}"
            )));
        }
        let deadline_outcome: Option<String> = transaction
            .query_row(
                "SELECT deadline_outcome FROM conditions
                 WHERE job_id = ?1 AND deadline_ms IS NOT NULL
                   AND (state = 'failed' OR deadline_ms <= ?2)
                 ORDER BY deadline_ms, condition_index LIMIT 1",
                params![job_key, now],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(outcome) = deadline_outcome {
            let reason = format!("condition_deadline_expired:deadline outcome={outcome}");
            transaction.execute(
                "UPDATE conditions SET state = 'failed', next_probe_ms = NULL
                 WHERE job_id = ?1 AND state != 'failed'",
                [&job_key],
            )?;
            transaction.execute(
                "UPDATE admissions SET pre_resume_defer_reason = ?2
                 WHERE attempt_id = (SELECT attempt_id FROM jobs WHERE id = ?1)",
                params![job_key, reason],
            )?;
            transaction.commit()?;
            return Ok(Some(reason));
        }
        if cancel_requested {
            let reason = "cancel_requested".to_owned();
            transaction.execute(
                "UPDATE admissions SET pre_resume_defer_reason = ?2
                 WHERE attempt_id = (SELECT attempt_id FROM jobs WHERE id = ?1)",
                params![job_key, reason],
            )?;
            transaction.commit()?;
            return Ok(Some(reason));
        }
        transaction.rollback()?;
        Ok(None)
    }

    pub(crate) fn replan_never_run(&mut self, job: &PreparedJob, reason: &str) -> StoreResult<()> {
        let now = now_millis();
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let row: (String, String, String, bool, u32, u64, String, u32) = transaction.query_row(
            "SELECT jobs.state, attempts.state, invocations.state,
                    jobs.cancel_requested != 0, admissions.deferral_count,
                    admissions.quiet_consumed_ms, jobs.spec_json, attempts.attempt_index
             FROM jobs JOIN attempts ON attempts.id = jobs.attempt_id
             JOIN invocations ON invocations.id = jobs.invocation_id
             JOIN admissions ON admissions.attempt_id = attempts.id
             WHERE jobs.id = ?1 AND attempts.id = ?2 AND invocations.id = ?3",
            params![
                job.job_id.entity_uuid().to_string(),
                job.attempt_id.entity_uuid().to_string(),
                job.invocation_id.entity_uuid().to_string(),
            ],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                ))
            },
        )?;
        let pre_release_state = matches!(
            (row.1.as_str(), row.2.as_str()),
            ("starting", "prepared") | ("running", "started")
        );
        if row.0 != "active" || !pre_release_state {
            return Err(StoreError::InvalidState(format!(
                "never-run Invocation cannot replan from {}/{}/{}",
                row.0, row.1, row.2
            )));
        }
        transaction.execute(
            "UPDATE invocations SET state = 'resolved', started_ms = NULL, finished_ms = ?2
             WHERE id = ?1 AND state IN ('prepared', 'started')",
            params![job.invocation_id.entity_uuid().to_string(), now],
        )?;
        transaction.execute(
            "UPDATE containments SET state = 'empty' WHERE id = ?1 AND state = 'live'",
            [job.containment_id.entity_uuid().to_string()],
        )?;
        let released = release_never_run_attempt_lease_if_safe(
            &transaction,
            &job.attempt_id.entity_uuid().to_string(),
        )?;
        if !released {
            return Err(StoreError::InvalidState(
                "never-run Attempt Lease was not safe to release".into(),
            ));
        }
        transaction.execute(
            "UPDATE attempts SET started_ms = NULL, deadline_ms = NULL WHERE id = ?1",
            [job.attempt_id.entity_uuid().to_string()],
        )?;
        transaction.execute(
            "UPDATE jobs SET started_ms = (
                 SELECT MIN(started_ms) FROM attempts
                 WHERE attempts.job_id = jobs.id AND started_ms IS NOT NULL
             ) WHERE id = ?1",
            [job.job_id.entity_uuid().to_string()],
        )?;
        let spec: JobSpec = serde_json::from_str(&row.6)?;
        let quiet_budget = spec
            .quiet
            .as_ref()
            .map(|quiet| quiet.wait_budget_seconds.saturating_mul(1_000))
            .unwrap_or(u64::MAX);
        let deferrals = row.4.saturating_add(1);
        let exhausted =
            deferrals >= self.observation_config.pre_release_max_deferrals || row.5 >= quiet_budget;
        let durable_deadline_outcome: Option<String> = transaction
            .query_row(
                "SELECT deadline_outcome FROM conditions
                 WHERE job_id = ?1 AND deadline_ms IS NOT NULL
                   AND (state = 'failed' OR deadline_ms <= ?2)
                 ORDER BY deadline_ms, condition_index LIMIT 1",
                params![job.job_id.entity_uuid().to_string(), now],
                |row| row.get(0),
            )
            .optional()?;
        if durable_deadline_outcome.is_some() {
            transaction.execute(
                "UPDATE conditions SET state = 'failed', next_probe_ms = NULL
                 WHERE job_id = ?1 AND state != 'failed'",
                [job.job_id.entity_uuid().to_string()],
            )?;
        }
        let condition_deadline_outcome = durable_deadline_outcome
            .as_deref()
            .map(|outcome| {
                if outcome == "canceled" {
                    ("canceled", "canceled")
                } else {
                    ("safety_failed", "failed")
                }
            })
            .or_else(|| {
                reason
                    .strip_prefix("condition_deadline_expired:")
                    .map(|detail| {
                        if detail.contains("canceled") {
                            ("canceled", "canceled")
                        } else {
                            ("safety_failed", "failed")
                        }
                    })
            });
        if let Some((verdict, outcome)) = condition_deadline_outcome {
            transaction.execute(
                "UPDATE attempts SET state = 'settled', verdict = ?2,
                    safety_reason = 'condition_deadline_expired', finished_ms = ?3
                 WHERE id = ?1",
                params![job.attempt_id.entity_uuid().to_string(), verdict, now],
            )?;
            transaction.execute(
                "UPDATE jobs SET state = 'final', outcome = ?2,
                    reason_code = 'condition_deadline_expired', finished_ms = ?3,
                    retry_not_before_ms = NULL WHERE id = ?1",
                params![job.job_id.entity_uuid().to_string(), outcome, now],
            )?;
        } else if row.3 {
            transaction.execute(
                "UPDATE attempts SET state = 'settled', verdict = 'canceled',
                    finished_ms = ?2 WHERE id = ?1",
                params![job.attempt_id.entity_uuid().to_string(), now],
            )?;
            transaction.execute(
                "UPDATE jobs SET state = 'final', outcome = 'canceled', finished_ms = ?2
                 WHERE id = ?1",
                params![job.job_id.entity_uuid().to_string(), now],
            )?;
        } else if exhausted {
            transaction.execute(
                "UPDATE admissions SET deferral_count = ?2, last_blockers_json = ?3
                 WHERE attempt_id = ?1",
                params![
                    job.attempt_id.entity_uuid().to_string(),
                    deferrals,
                    serde_json::to_string(&vec![Blocker {
                        code: "pre_release_deferred".into(),
                        detail: reason.to_owned(),
                    }])?,
                ],
            )?;
            let retry = row.7 < spec.retry.max_attempts
                && spec
                    .retry
                    .retryable
                    .iter()
                    .any(|value| value == AttemptVerdict::SafetyFailed.as_str());
            transaction.execute(
                "UPDATE attempts SET state = 'settled', verdict = 'safety_failed',
                    safety_reason = ?2, finished_ms = ?3 WHERE id = ?1",
                params![
                    job.attempt_id.entity_uuid().to_string(),
                    if spec.conditions.is_empty() {
                        "quiet_unattainable"
                    } else {
                        "readiness_unstable"
                    },
                    now,
                ],
            )?;
            if retry {
                let not_before = now.saturating_add(
                    i64::try_from(spec.retry.backoff_seconds.saturating_mul(1_000))
                        .unwrap_or(i64::MAX),
                );
                transaction.execute(
                    "UPDATE jobs SET state = 'pending', attempt_id = NULL,
                        invocation_id = NULL, containment_id = NULL,
                        root_exit_code = NULL, retry_not_before_ms = ?2,
                        cancel_requested = 0 WHERE id = ?1",
                    params![job.job_id.entity_uuid().to_string(), not_before],
                )?;
                reset_conditions_for_retry_tx(
                    &transaction,
                    &job.job_id.entity_uuid().to_string(),
                    not_before,
                )?;
            } else {
                transaction.execute(
                    "UPDATE jobs SET state = 'final', outcome = 'failed', reason_code = ?2,
                        finished_ms = ?3, retry_not_before_ms = NULL WHERE id = ?1",
                    params![
                        job.job_id.entity_uuid().to_string(),
                        if spec.conditions.is_empty() {
                            "quiet_unattainable"
                        } else {
                            "readiness_unstable"
                        },
                        now,
                    ],
                )?;
            }
        } else {
            let not_before = now.saturating_add(
                i64::try_from(self.observation_config.pre_release_backoff_millis)
                    .unwrap_or(i64::MAX),
            );
            transaction.execute(
                "UPDATE admissions SET deferral_count = ?2, retry_not_before_ms = ?3,
                    last_blockers_json = ?4, quiet_generation = NULL,
                    last_eval_monotonic_ms = NULL, last_eval_generation = NULL,
                    quiet_first_monotonic_ms = NULL, quiet_last_monotonic_ms = NULL,
                    reservation_generation = NULL, reservation_evidence_json = NULL,
                    release_evidence_json = NULL WHERE attempt_id = ?1",
                params![
                    job.attempt_id.entity_uuid().to_string(),
                    deferrals,
                    not_before,
                    serde_json::to_string(&vec![Blocker {
                        code: "pre_release_deferred".into(),
                        detail: reason.to_owned(),
                    }])?,
                ],
            )?;
            transaction.execute(
                "UPDATE attempts SET state = 'planned' WHERE id = ?1",
                [job.attempt_id.entity_uuid().to_string()],
            )?;
            transaction.execute(
                "UPDATE jobs SET state = 'pending', invocation_id = NULL,
                    containment_id = NULL, retry_not_before_ms = ?2 WHERE id = ?1",
                params![job.job_id.entity_uuid().to_string(), not_before],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }
}

fn windows_identity_parts(
    root_identity: &ProcessIdentity,
    root_pid: u32,
) -> StoreResult<(&str, &str, i64)> {
    match root_identity {
        ProcessIdentity::Windows {
            host_id,
            boot_id,
            pid,
            creation_filetime_100ns,
        } if *pid == root_pid => Ok((
            host_id.0.as_str(),
            boot_id.0.as_str(),
            i64::try_from(*creation_filetime_100ns).map_err(|_| {
                StoreError::InvalidState("process creation identity exceeds SQLite range".into())
            })?,
        )),
        ProcessIdentity::Windows { pid, .. } => Err(StoreError::InvalidState(format!(
            "process identity PID {pid} does not match created PID {root_pid}"
        ))),
        ProcessIdentity::Unknown { .. } => Err(StoreError::InvalidState(
            "unknown process identity cannot authorize native containment".into(),
        )),
    }
}

fn active_claims_excluding_attempt(
    transaction: &Transaction<'_>,
    excluded: AttemptId,
) -> StoreResult<Vec<ResolvedClaims>> {
    let mut statement = transaction
        .prepare("SELECT claims_json FROM leases WHERE state = 'granted' AND attempt_id != ?1")?;
    let rows = statement.query_map([excluded.entity_uuid().to_string()], |row| {
        row.get::<_, String>(0)
    })?;
    rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
}
