use super::*;

impl Store {
    pub(super) fn recover_interrupted(&mut self) -> StoreResult<()> {
        let interrupted = {
            let mut statement = self.connection.prepare(
                "SELECT containments.id, containments.state, invocations.role,
                        jobs.spec_json, conditions.spec_json
                 FROM containments
                 JOIN invocations ON invocations.id = containments.invocation_id
                 JOIN attempts ON attempts.id = invocations.attempt_id
                 JOIN jobs ON jobs.id = attempts.job_id
                 LEFT JOIN conditions ON conditions.id = invocations.condition_id
                 WHERE containments.state IN ('creating', 'live')
                 ORDER BY containments.rowid",
            )?;
            let rows = statement.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            })?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        let finished = now_millis();
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "UPDATE invocations SET state = 'resolved', finished_ms = ?1
             WHERE state IN ('prepared', 'started', 'exited')",
            [finished],
        )?;
        let mut incident_sequence: u64 = transaction.query_row(
            "SELECT COALESCE(MAX(incident_sequence), 0) FROM containments",
            [],
            |row| row.get(0),
        )?;
        for (containment_id, prior_state, role, spec_json, condition_json) in interrupted {
            incident_sequence = incident_sequence.saturating_add(1);
            let retained_claims = if role == "probe" {
                let condition: crate::ConditionSpec =
                    serde_json::from_str(condition_json.as_deref().ok_or_else(|| {
                        StoreError::InvalidState(
                            "interrupted probe Invocation has no durable Condition".into(),
                        )
                    })?)?;
                let ConditionPredicate::Probe { probe } = condition.predicate else {
                    return Err(StoreError::InvalidState(
                        "interrupted probe Invocation references a non-probe Condition".into(),
                    ));
                };
                serde_json::to_string(&probe.resources)?
            } else {
                serde_json::to_string(&serde_json::from_str::<JobSpec>(&spec_json)?.resources)?
            };
            let (reason, detail) = if prior_state == "creating" {
                (
                    "daemon_restart_before_resume",
                    "daemon restarted before process release; boundary closure awaits proof",
                )
            } else {
                (
                    "daemon_restart_cleanup_unproven",
                    "daemon restarted while containment cleanup was not durably proven",
                )
            };
            transaction.execute(
                "UPDATE containments SET state = 'uncertain', version = version + 1,
                    incident_sequence = ?2, reason_code = ?3, detail = ?4,
                    opened_ms = ?5, retained_claims_json = ?6
                 WHERE id = ?1 AND state IN ('creating', 'live')",
                params![
                    containment_id,
                    incident_sequence,
                    reason,
                    detail,
                    finished,
                    retained_claims,
                ],
            )?;
        }
        let pre_resume_terminals = {
            let mut statement = transaction.prepare(
                "SELECT jobs.id, attempts.id, invocations.id,
                        admissions.pre_resume_defer_reason, attempts.state
                 FROM jobs
                 JOIN attempts ON attempts.id = jobs.attempt_id
                 JOIN invocations ON invocations.id = jobs.invocation_id
                 JOIN admissions ON admissions.attempt_id = attempts.id
                 WHERE jobs.state = 'active'
                   AND attempts.state IN ('starting', 'running')
                   AND invocations.role = 'primary'
                   AND (admissions.pre_resume_defer_reason IS NOT NULL
                        OR EXISTS(
                            SELECT 1 FROM conditions
                            WHERE conditions.job_id = jobs.id AND conditions.state = 'failed'
                              AND conditions.deadline_ms IS NOT NULL
                        )
                        OR (attempts.state = 'starting' AND (
                            jobs.cancel_requested != 0 OR EXISTS(
                                SELECT 1 FROM conditions
                                WHERE conditions.job_id = jobs.id
                                  AND conditions.deadline_ms IS NOT NULL
                                  AND conditions.deadline_ms <= ?1
                            )
                        )))
                 ORDER BY jobs.accepted_ms, jobs.rowid",
            )?;
            statement
                .query_map([finished], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?
        };
        for (job_id, attempt_id, invocation_id, defer_reason, attempt_state) in pre_resume_terminals
        {
            let deadline_outcome: Option<String> = transaction
                .query_row(
                    "SELECT deadline_outcome FROM conditions
                     WHERE job_id = ?1 AND deadline_ms IS NOT NULL
                       AND (state = 'failed'
                            OR ((?3 IS NOT NULL OR ?4 = 'starting') AND deadline_ms <= ?2))
                     ORDER BY deadline_ms, condition_index LIMIT 1",
                    params![job_id, finished, defer_reason, attempt_state],
                    |row| row.get(0),
                )
                .optional()?;
            let (verdict, outcome, reason) = if let Some(deadline_outcome) = deadline_outcome {
                transaction.execute(
                    "UPDATE conditions SET state = 'failed', next_probe_ms = NULL
                     WHERE job_id = ?1 AND state != 'failed'",
                    [&job_id],
                )?;
                if deadline_outcome == "canceled" {
                    ("canceled", "canceled", Some("condition_deadline_expired"))
                } else {
                    (
                        "safety_failed",
                        "failed",
                        Some("condition_deadline_expired"),
                    )
                }
            } else if defer_reason.as_deref() == Some("cancel_requested")
                || attempt_state == "starting"
            {
                ("canceled", "canceled", None)
            } else {
                return Err(StoreError::InvalidState(format!(
                    "active Job {job_id} has an invalid pre-resume terminal latch"
                )));
            };
            transaction.execute(
                "UPDATE invocations SET started_ms = NULL WHERE id = ?1",
                [&invocation_id],
            )?;
            transaction.execute(
                "UPDATE attempts SET state = 'settled', verdict = ?2, safety_reason = ?3,
                    started_ms = NULL, deadline_ms = NULL, finished_ms = ?4
                 WHERE id = ?1 AND state IN ('starting', 'running')",
                params![attempt_id, verdict, reason, finished],
            )?;
            transaction.execute(
                "UPDATE jobs SET state = 'final', outcome = ?2, reason_code = ?3,
                    started_ms = (
                        SELECT MIN(started_ms) FROM attempts
                        WHERE attempts.job_id = jobs.id AND started_ms IS NOT NULL
                    ), finished_ms = ?4
                 WHERE id = ?1 AND state = 'active'",
                params![job_id, outcome, reason, finished],
            )?;
        }
        transaction.execute(
            "UPDATE attempts SET state = 'settled', verdict = 'start_failed', finished_ms = ?1
             WHERE state = 'starting'",
            [finished],
        )?;
        transaction.execute(
            "UPDATE attempts SET state = 'settled', verdict = 'interrupted', finished_ms = ?1
             WHERE state = 'running'",
            [finished],
        )?;
        transaction.execute(
            "UPDATE jobs SET state = 'final',
                outcome = CASE
                    WHEN attempt_id IN (
                        SELECT id FROM attempts
                        WHERE verdict = 'start_failed' AND finished_ms = ?1
                    ) THEN 'failed'
                    ELSE 'interrupted'
                END,
                finished_ms = ?1
             WHERE state = 'active'",
            [finished],
        )?;
        release_all_safe_attempt_leases(&transaction)?;
        repair_resolved_empty_probes_tx(
            &transaction,
            self.store_uuid,
            self.daemon_generation,
            self.startup_identity.boot_id.as_ref(),
            finished,
        )?;
        expire_due_condition_deadlines_tx(&transaction, finished)?;
        finalize_recovered_condition_terminals_tx(&transaction, finished)?;
        transaction.commit()?;
        self.prune_condition_history()?;
        Ok(())
    }

    pub(super) fn resume_received(&mut self) -> StoreResult<()> {
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
                    | StoreError::ManagedWaitRejected { .. }
                    | StoreError::OperationRejected { .. },
                ) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
}
