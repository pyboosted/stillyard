use super::*;

impl Store {
    #[cfg(test)]
    pub(crate) fn mark_started(
        &mut self,
        job: &PreparedJob,
        root_pid: u32,
        executable_hash: &str,
    ) -> StoreResult<()> {
        self.mark_started_with_identity(job, root_pid, executable_hash, None)
    }

    pub(crate) fn mark_started_with_identity(
        &mut self,
        job: &PreparedJob,
        root_pid: u32,
        executable_hash: &str,
        root_identity: Option<&ProcessIdentity>,
    ) -> StoreResult<()> {
        let (root_host_id, root_boot_id, root_creation_filetime_100ns) = match root_identity {
            Some(ProcessIdentity::Windows {
                host_id,
                boot_id,
                pid,
                creation_filetime_100ns,
            }) if *pid == root_pid => (
                Some(host_id.0.as_str()),
                Some(boot_id.0.as_str()),
                Some(i64::try_from(*creation_filetime_100ns).map_err(|_| {
                    StoreError::InvalidState(
                        "process creation identity exceeds SQLite range".into(),
                    )
                })?),
            ),
            Some(ProcessIdentity::Windows { pid, .. }) => {
                return Err(StoreError::InvalidState(format!(
                    "process identity PID {pid} does not match created PID {root_pid}"
                )));
            }
            Some(ProcessIdentity::Unknown { .. }) => {
                return Err(StoreError::InvalidState(
                    "unknown process identity cannot authorize native containment".into(),
                ));
            }
            None => (None, None, None),
        };
        let started = now_millis();
        let transaction = self.connection.transaction()?;
        let state: String = transaction.query_row(
            "SELECT state FROM jobs WHERE id = ?1",
            [job.job_id.entity_uuid().to_string()],
            |row| row.get(0),
        )?;
        let state_allowed = if job.role == InvocationRole::Probe {
            state == "pending"
        } else {
            state == "active"
        };
        if !state_allowed {
            return Err(StoreError::InvalidState(format!(
                "job {} cannot start from {state}",
                job.job_id
            )));
        }
        transaction.execute(
            "UPDATE invocations SET state = 'started', root_pid = ?2,
                executable_hash = ?3, started_ms = ?4, daemon_generation = ?5,
                root_host_id = ?6, root_boot_id = ?7,
                root_creation_filetime_100ns = ?8 WHERE id = ?1",
            params![
                job.invocation_id.entity_uuid().to_string(),
                root_pid,
                executable_hash,
                started,
                self.daemon_generation.to_string(),
                root_host_id,
                root_boot_id,
                root_creation_filetime_100ns,
            ],
        )?;
        transaction.execute(
            "UPDATE containments SET state = 'live' WHERE id = ?1",
            [job.containment_id.entity_uuid().to_string()],
        )?;
        if job.role != InvocationRole::Probe {
            transaction.execute(
                "UPDATE attempts SET state = 'running' WHERE id = ?1",
                [job.attempt_id.entity_uuid().to_string()],
            )?;
        }
        if job.role == InvocationRole::Primary {
            transaction.execute(
                "UPDATE jobs SET started_ms = COALESCE(started_ms, ?2) WHERE id = ?1",
                params![job.job_id.entity_uuid().to_string(), started],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn commit_log_offset(
        &mut self,
        job_id: JobId,
        stream: LogStream,
        offset: u64,
    ) -> StoreResult<()> {
        let column = match stream {
            LogStream::Stdout => "stdout_len",
            LogStream::Stderr => "stderr_len",
        };
        self.connection.execute(
            &format!("UPDATE jobs SET {column} = ?2 WHERE id = ?1"),
            params![self.local_id(job_id)?, offset],
        )?;
        Ok(())
    }

    pub(crate) fn mark_root_exited(
        &mut self,
        job: &PreparedJob,
        exit_code: i32,
    ) -> StoreResult<()> {
        let transaction = self.connection.transaction()?;
        let state: String = transaction.query_row(
            "SELECT state FROM jobs WHERE id = ?1",
            [job.job_id.entity_uuid().to_string()],
            |row| row.get(0),
        )?;
        let state_allowed = if job.role == InvocationRole::Probe {
            matches!(state.as_str(), "pending" | "final")
        } else {
            state == "active"
        };
        if !state_allowed {
            return Err(StoreError::InvalidState(format!(
                "job {} cannot record root exit from {state}",
                job.job_id
            )));
        }
        transaction.execute(
            "UPDATE invocations SET state = 'exited', root_exit_code = ?2, exited_ms = ?3
             WHERE id = ?1 AND state = 'started'",
            params![
                job.invocation_id.entity_uuid().to_string(),
                exit_code,
                now_millis()
            ],
        )?;
        if job.role == InvocationRole::Primary {
            transaction.execute(
                "UPDATE jobs SET root_exit_code = ?2 WHERE id = ?1 AND state = 'active'",
                params![job.job_id.entity_uuid().to_string(), exit_code],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn mark_invocation_resolved(
        &mut self,
        job: &PreparedJob,
        exit_code: Option<i32>,
        classification: Option<ExitClassification>,
    ) -> StoreResult<()> {
        // Diagnostic observations must never prevent the authoritative lifecycle transition.
        // Preserve the read failure in-band while still resolving the Invocation and Lease.
        let stdout_tail = read_diagnostic_tail(&job.stdout_path)
            .unwrap_or_else(|error| format!("[stillyard stdout tail unavailable: {error}]"));
        let stderr_tail = read_diagnostic_tail(&job.stderr_path)
            .unwrap_or_else(|error| format!("[stillyard stderr tail unavailable: {error}]"));
        let transaction = self.connection.transaction()?;
        transaction.execute(
            "UPDATE invocations SET state = 'resolved', root_exit_code = COALESCE(?2, root_exit_code),
                exit_classification = ?3, finished_ms = ?4, stdout_tail = ?5, stderr_tail = ?6
             WHERE id = ?1",
            params![
                job.invocation_id.entity_uuid().to_string(),
                exit_code,
                classification.map(exit_classification_string),
                now_millis(),
                stdout_tail,
                stderr_tail,
            ],
        )?;
        transaction.execute(
            "UPDATE containments SET state = 'empty' WHERE id = ?1",
            [job.containment_id.entity_uuid().to_string()],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn record_primary_result(
        &mut self,
        job: &PreparedJob,
        verdict: InvocationVerdict,
        termination: TerminationReason,
    ) -> StoreResult<PrimaryInvocationResult> {
        if job.role != InvocationRole::Primary {
            return Err(StoreError::InvalidState(
                "only the primary Invocation has a primary result".into(),
            ));
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing: Option<String> = transaction.query_row(
            "SELECT primary_result_json FROM attempts WHERE id = ?1",
            [job.attempt_id.entity_uuid().to_string()],
            |row| row.get(0),
        )?;
        if let Some(existing) = existing {
            let existing: PrimaryInvocationResult = serde_json::from_str(&existing)?;
            validate_primary_result_identity(&existing, job)?;
            validate_primary_result_semantics(
                existing.verdict,
                existing.termination,
                existing.root_exit_code,
            )?;
            if existing.verdict != verdict || existing.termination != termination {
                return Err(StoreError::InvalidState(
                    "durable primary result cannot be replaced".into(),
                ));
            }
            transaction.commit()?;
            return Ok(existing);
        }
        let (state, root_exit_code, started, exited, resolved, containment): (
            String,
            Option<i32>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            String,
        ) = transaction.query_row(
            "SELECT invocations.state, invocations.root_exit_code, invocations.started_ms,
                    invocations.exited_ms, invocations.finished_ms, containments.state
             FROM invocations JOIN containments ON containments.invocation_id = invocations.id
             WHERE invocations.id = ?1 AND invocations.attempt_id = ?2
               AND invocations.role = 'primary'",
            params![
                job.invocation_id.entity_uuid().to_string(),
                job.attempt_id.entity_uuid().to_string(),
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
        if state != "resolved" || containment != "empty" {
            return Err(StoreError::InvalidState(
                "primary result requires a resolved Invocation and empty Containment".into(),
            ));
        }
        validate_primary_result_semantics(verdict, termination, root_exit_code)?;
        let result = PrimaryInvocationResult {
            schema_version: 1,
            job_id: job.job_id,
            attempt_id: job.attempt_id,
            invocation_id: job.invocation_id,
            verdict,
            root_exit_code,
            termination,
            containment: ContainmentState::Empty,
            started_unix_millis: started,
            exited_unix_millis: exited,
            resolved_unix_millis: resolved.ok_or_else(|| {
                StoreError::InvalidState("resolved primary Invocation has no timestamp".into())
            })?,
        };
        transaction.execute(
            "UPDATE attempts SET primary_result_json = ?2 WHERE id = ?1
             AND primary_result_json IS NULL",
            params![
                job.attempt_id.entity_uuid().to_string(),
                serde_json::to_string(&result)?,
            ],
        )?;
        transaction.commit()?;
        Ok(result)
    }

    pub(crate) fn settle_attempt(
        &mut self,
        job: &PreparedJob,
        verdict: AttemptVerdict,
    ) -> StoreResult<bool> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (state, spec_json, attempt_index, cancel_requested): (String, String, u32, bool) =
            transaction.query_row(
                "SELECT jobs.state, jobs.spec_json, attempts.attempt_index,
                        jobs.cancel_requested != 0
                 FROM jobs JOIN attempts ON attempts.id = jobs.attempt_id
                 WHERE jobs.id = ?1 AND attempts.id = ?2",
                params![
                    job.job_id.entity_uuid().to_string(),
                    job.attempt_id.entity_uuid().to_string(),
                ],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )?;
        if state != "active" {
            return Err(StoreError::InvalidState(format!(
                "job {} cannot settle Attempt from {state}",
                job.job_id
            )));
        }
        let spec: JobSpec = serde_json::from_str(&spec_json)?;
        let verdict_text = verdict.as_str();
        let retry = !cancel_requested
            && attempt_index < spec.retry.max_attempts
            && spec
                .retry
                .retryable
                .iter()
                .any(|value| value == verdict_text);
        transaction.execute(
            "UPDATE attempts SET state = 'settled', verdict = ?2, finished_ms = ?3 WHERE id = ?1",
            params![
                job.attempt_id.entity_uuid().to_string(),
                verdict_text,
                now_millis()
            ],
        )?;
        release_attempt_lease_if_safe(&transaction, &job.attempt_id.entity_uuid().to_string())?;
        if retry {
            let not_before = now_millis().saturating_add(
                i64::try_from(spec.retry.backoff_seconds.saturating_mul(1000)).unwrap_or(i64::MAX),
            );
            transaction.execute(
                "UPDATE jobs SET state = 'pending', attempt_id = NULL, invocation_id = NULL,
                    containment_id = NULL, root_exit_code = NULL,
                    retry_not_before_ms = ?2, cancel_requested = 0
                 WHERE id = ?1",
                params![job.job_id.entity_uuid().to_string(), not_before],
            )?;
            reset_conditions_for_retry_tx(
                &transaction,
                &job.job_id.entity_uuid().to_string(),
                not_before,
            )?;
        } else {
            let effective_verdict = if cancel_requested {
                AttemptVerdict::Canceled
            } else {
                verdict
            };
            if effective_verdict != verdict {
                transaction.execute(
                    "UPDATE attempts SET verdict = ?2 WHERE id = ?1",
                    params![
                        job.attempt_id.entity_uuid().to_string(),
                        effective_verdict.as_str()
                    ],
                )?;
            }
            transaction.execute(
                "UPDATE jobs SET state = 'final', outcome = ?2, finished_ms = ?3,
                    retry_not_before_ms = NULL WHERE id = ?1",
                params![
                    job.job_id.entity_uuid().to_string(),
                    outcome_string(outcome_for_verdict(effective_verdict)),
                    now_millis(),
                ],
            )?;
        }
        transaction.commit()?;
        Ok(retry)
    }

    pub(crate) fn invocation_stop_requested(&self, job_id: JobId) -> StoreResult<bool> {
        self.connection
            .query_row(
                "SELECT cancel_requested != 0 OR state = 'final' FROM jobs WHERE id = ?1",
                [self.local_id(job_id)?],
                |row| row.get(0),
            )
            .map_err(Into::into)
    }

    pub(crate) fn cancel_jobs(&mut self, job_ids: &[JobId]) -> StoreResult<Vec<JobSnapshot>> {
        if job_ids.is_empty() {
            return Err(StoreError::InvalidSpec(
                "cancel requires at least one explicit Job ID".into(),
            ));
        }
        if job_ids.len() > MAX_CANCEL_JOBS {
            return Err(StoreError::InvalidSpec(format!(
                "cancel accepts at most {MAX_CANCEL_JOBS} Job IDs per request"
            )));
        }
        let local_ids = job_ids
            .iter()
            .map(|job_id| self.local_id(*job_id))
            .collect::<StoreResult<Vec<_>>>()?;
        let transaction = self.connection.transaction()?;
        for local_id in &local_ids {
            let state = transaction
                .query_row("SELECT state FROM jobs WHERE id = ?1", [local_id], |row| {
                    row.get::<_, String>(0)
                })
                .optional()?
                .ok_or_else(|| StoreError::NotFound(local_id.clone()))?;
            match state.as_str() {
                "pending" => {
                    transaction.execute(
                        "UPDATE attempts SET state = 'settled', verdict = 'canceled',
                            finished_ms = ?2
                         WHERE id = (SELECT attempt_id FROM jobs WHERE id = ?1)
                           AND state IN ('planned', 'admitting')",
                        params![local_id, now_millis()],
                    )?;
                    transaction.execute(
                        "UPDATE jobs SET state = 'final', outcome = 'canceled',
                            cancel_requested = 1, retry_not_before_ms = NULL, finished_ms = ?2
                         WHERE id = ?1",
                        params![local_id, now_millis()],
                    )?;
                }
                "active" => {
                    transaction.execute(
                        "UPDATE jobs SET cancel_requested = 1 WHERE id = ?1",
                        [local_id],
                    )?;
                }
                "final" => {}
                other => {
                    return Err(StoreError::InvalidState(format!(
                        "cannot cancel Job in state {other}"
                    )));
                }
            }
        }
        transaction.commit()?;
        job_ids.iter().map(|job_id| self.status(*job_id)).collect()
    }

    pub(crate) fn mark_finished(
        &mut self,
        job: &PreparedJob,
        exit_code: Option<i32>,
        outcome: JobOutcome,
        verdict: &str,
    ) -> StoreResult<()> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let state: String = transaction.query_row(
            "SELECT state FROM jobs WHERE id = ?1",
            [job.job_id.entity_uuid().to_string()],
            |row| row.get(0),
        )?;
        if state != "active" {
            return Err(StoreError::InvalidState(format!(
                "job {} cannot settle from {state}",
                job.job_id
            )));
        }
        transaction.execute(
            "UPDATE invocations SET state = 'resolved', root_exit_code = ?2,
                finished_ms = ?3 WHERE id = ?1",
            params![
                job.invocation_id.entity_uuid().to_string(),
                exit_code,
                now_millis()
            ],
        )?;
        transaction.execute(
            "UPDATE containments SET state = 'empty' WHERE id = ?1",
            [job.containment_id.entity_uuid().to_string()],
        )?;
        transaction.execute(
            "UPDATE attempts SET state = 'settled', verdict = ?2, finished_ms = ?3 WHERE id = ?1",
            params![
                job.attempt_id.entity_uuid().to_string(),
                verdict,
                now_millis()
            ],
        )?;
        release_attempt_lease_if_safe(&transaction, &job.attempt_id.entity_uuid().to_string())?;
        transaction.execute(
            "UPDATE jobs SET state = 'final', outcome = ?2, root_exit_code = ?3,
                finished_ms = ?4 WHERE id = ?1",
            params![
                job.job_id.entity_uuid().to_string(),
                outcome_string(outcome),
                exit_code,
                now_millis(),
            ],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn mark_uncertain(
        &mut self,
        job: &PreparedJob,
        exit_code: Option<i32>,
        verdict: &str,
    ) -> StoreResult<()> {
        self.mark_uncertain_settlement(job, exit_code, verdict, None, JobOutcome::Interrupted)
    }

    pub(crate) fn mark_pre_release_cleanup_uncertain(
        &mut self,
        job: &PreparedJob,
        exit_code: Option<i32>,
    ) -> StoreResult<()> {
        self.mark_uncertain_settlement(
            job,
            exit_code,
            AttemptVerdict::SafetyFailed.as_str(),
            Some("pre_release_cleanup_uncertain"),
            JobOutcome::Failed,
        )
    }

    fn mark_uncertain_settlement(
        &mut self,
        job: &PreparedJob,
        exit_code: Option<i32>,
        verdict: &str,
        safety_reason: Option<&str>,
        outcome: JobOutcome,
    ) -> StoreResult<()> {
        let transaction = self.connection.transaction()?;
        let state: String = transaction.query_row(
            "SELECT state FROM jobs WHERE id = ?1",
            [job.job_id.entity_uuid().to_string()],
            |row| row.get(0),
        )?;
        if state != "active" {
            return Err(StoreError::InvalidState(format!(
                "job {} cannot become uncertain from {state}",
                job.job_id
            )));
        }
        let incident_sequence: u64 = transaction.query_row(
            "SELECT COALESCE(MAX(incident_sequence), 0) + 1 FROM containments",
            [],
            |row| row.get(0),
        )?;
        let spec_json: String = transaction.query_row(
            "SELECT jobs.spec_json FROM jobs
             JOIN attempts ON attempts.job_id = jobs.id
             JOIN invocations ON invocations.attempt_id = attempts.id
             WHERE invocations.id = ?1",
            [job.invocation_id.entity_uuid().to_string()],
            |row| row.get(0),
        )?;
        let retained_claims_json =
            serde_json::to_string(&serde_json::from_str::<JobSpec>(&spec_json)?.resources)?;
        let opened = now_millis();
        transaction.execute(
            "UPDATE invocations SET state = 'resolved', root_exit_code = COALESCE(?2, root_exit_code),
                finished_ms = ?3 WHERE id = ?1 AND state IN ('prepared', 'started', 'exited')",
            params![
                job.invocation_id.entity_uuid().to_string(),
                exit_code,
                now_millis()
            ],
        )?;
        transaction.execute(
            "UPDATE containments SET state = 'uncertain', version = version + 1,
                incident_sequence = COALESCE(incident_sequence, ?2),
                reason_code = COALESCE(reason_code, ?3),
                detail = COALESCE(detail, ?4), opened_ms = COALESCE(opened_ms, ?5),
                retained_claims_json = COALESCE(retained_claims_json, ?6)
             WHERE id = ?1 AND state IN ('creating', 'live')",
            params![
                job.containment_id.entity_uuid().to_string(),
                incident_sequence,
                safety_reason.unwrap_or(verdict),
                "cleanup could not be proven within the bounded runner wait",
                opened,
                retained_claims_json,
            ],
        )?;
        transaction.execute(
            "UPDATE attempts SET state = 'settled', verdict = ?2, safety_reason = ?3,
                finished_ms = ?4
             WHERE id = ?1 AND state != 'settled'",
            params![
                job.attempt_id.entity_uuid().to_string(),
                verdict,
                safety_reason,
                now_millis()
            ],
        )?;
        // An uncertain Containment deliberately keeps its Lease granted.
        transaction.execute(
            "UPDATE jobs SET state = 'final', outcome = ?2,
                root_exit_code = COALESCE(?3, root_exit_code), finished_ms = ?4
             WHERE id = ?1 AND state = 'active'",
            params![
                job.job_id.entity_uuid().to_string(),
                outcome_string(outcome),
                (job.role == InvocationRole::Primary)
                    .then_some(exit_code)
                    .flatten(),
                now_millis()
            ],
        )?;
        transaction.commit()?;
        Ok(())
    }
}

fn validate_primary_result_identity(
    result: &PrimaryInvocationResult,
    job: &PreparedJob,
) -> StoreResult<()> {
    if result.schema_version != 1
        || result.job_id != job.job_id
        || result.attempt_id != job.attempt_id
        || result.invocation_id != job.invocation_id
        || result.containment != ContainmentState::Empty
    {
        return Err(StoreError::InvalidState(
            "durable primary result identity or schema does not match the active primary Invocation"
                .into(),
        ));
    }
    Ok(())
}

pub(super) fn validate_primary_result_semantics(
    verdict: InvocationVerdict,
    termination: TerminationReason,
    root_exit_code: Option<i32>,
) -> StoreResult<()> {
    let pair_is_valid = matches!(
        (verdict, termination),
        (InvocationVerdict::Succeeded, TerminationReason::Exited)
            | (InvocationVerdict::ProcessFailed, TerminationReason::Exited)
            | (
                InvocationVerdict::StartFailed,
                TerminationReason::StartFailed
            )
            | (InvocationVerdict::TimedOut, TerminationReason::Timeout)
            | (InvocationVerdict::Interrupted, TerminationReason::Interrupt)
            | (
                InvocationVerdict::SafetyFailed,
                TerminationReason::SafetyFailure
            )
            | (InvocationVerdict::Canceled, TerminationReason::Cancel)
    );
    let root_is_valid = match verdict {
        InvocationVerdict::Succeeded => root_exit_code == Some(0),
        InvocationVerdict::ProcessFailed => root_exit_code.is_some_and(|code| code != 0),
        _ => true,
    };
    if !pair_is_valid || !root_is_valid {
        return Err(StoreError::InvalidState(
            "primary Invocation verdict, termination, and root exit are inconsistent".into(),
        ));
    }
    Ok(())
}
