use super::*;

impl Store {
    pub(super) fn recover_interrupted(&mut self) -> StoreResult<()> {
        let interrupted = {
            let mut statement = self.connection.prepare(
                "SELECT containments.id, containments.state, jobs.spec_json
                 FROM containments
                 JOIN invocations ON invocations.id = containments.invocation_id
                 JOIN attempts ON attempts.id = invocations.attempt_id
                 JOIN jobs ON jobs.id = attempts.job_id
                 WHERE containments.state IN ('creating', 'live')
                 ORDER BY containments.rowid",
            )?;
            let rows = statement.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
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
        for (containment_id, prior_state, spec_json) in interrupted {
            incident_sequence = incident_sequence.saturating_add(1);
            let retained_claims =
                serde_json::to_string(&serde_json::from_str::<JobSpec>(&spec_json)?.resources)?;
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
        transaction.execute(
            "UPDATE attempts SET state = 'settled', verdict = 'start_failed', finished_ms = ?1
             WHERE state = 'starting'",
            [finished],
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
                        SELECT id FROM attempts
                        WHERE verdict = 'start_failed' AND finished_ms = ?1
                    ) THEN 'failed'
                    ELSE 'interrupted'
                END,
                finished_ms = ?1
             WHERE state = 'active'",
            [finished],
        )?;
        transaction.execute(
            "UPDATE leases SET state = 'released'
             WHERE state = 'granted'
               AND EXISTS(SELECT 1 FROM attempts
                          WHERE attempts.id = leases.attempt_id
                            AND attempts.state = 'settled')
               AND NOT EXISTS(
                    SELECT 1 FROM containments
                    JOIN invocations ON invocations.id = containments.invocation_id
                    WHERE invocations.attempt_id = leases.attempt_id
                      AND containments.state NOT IN ('empty', 'cleared')
               )",
            [],
        )?;
        transaction.commit()?;
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
                    | StoreError::ManagedWaitRejected { .. },
                ) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
}
