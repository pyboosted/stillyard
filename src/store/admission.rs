use super::*;

impl Store {
    #[cfg(test)]
    pub(crate) fn submit(
        &mut self,
        idempotency_key: Uuid,
        claimed_payload_hash: &str,
        spec: &JobSpec,
    ) -> StoreResult<SubmitResult> {
        self.submit_with_stdin(idempotency_key, claimed_payload_hash, spec, None)
    }

    #[cfg(test)]
    pub(crate) fn submit_with_stdin(
        &mut self,
        idempotency_key: Uuid,
        claimed_payload_hash: &str,
        spec: &JobSpec,
        stdin: Option<&StagedInputRef>,
    ) -> StoreResult<SubmitResult> {
        self.submit_with_stdin_scoped(
            SubmissionScope::Unmanaged,
            idempotency_key,
            claimed_payload_hash,
            spec,
            stdin,
        )
    }

    #[cfg(test)]
    pub(crate) fn submit_with_stdin_scoped(
        &mut self,
        scope: SubmissionScope,
        idempotency_key: Uuid,
        claimed_payload_hash: &str,
        spec: &JobSpec,
        stdin: Option<&StagedInputRef>,
    ) -> StoreResult<SubmitResult> {
        self.submit_with_stdin_scoped_for_wait(
            scope,
            idempotency_key,
            claimed_payload_hash,
            spec,
            stdin,
            false,
        )
    }

    pub(crate) fn submit_with_stdin_scoped_for_wait(
        &mut self,
        scope: SubmissionScope,
        idempotency_key: Uuid,
        claimed_payload_hash: &str,
        spec: &JobSpec,
        stdin: Option<&StagedInputRef>,
        wait_for_completion: bool,
    ) -> StoreResult<SubmitResult> {
        spec.validate()
            .map_err(|error| StoreError::InvalidSpec(error.to_string()))?;
        self.validate_host_job(spec)?;
        validate_input_shape(spec, stdin)?;
        let payload_hash = normalized_payload_hash_with_input(spec, stdin)?;
        if claimed_payload_hash != payload_hash {
            return Err(StoreError::InvalidSpec(
                "payload hash does not match the normalized specification".into(),
            ));
        }
        let key = idempotency_key.to_string();
        let scope_key = scope.key();
        if let Some((
            submission_id,
            stored_hash,
            state,
            job_id,
            spec_json,
            stdin_json,
            kind,
            durable_wait,
            reject_code,
            reject_detail,
        )) = self
            .connection
            .query_row(
                "SELECT id, payload_hash, state, job_id, spec_json, stdin_json, kind, wait_intent,
                        reject_code, reject_detail
                 FROM submissions WHERE scope = ?1 AND idempotency_key = ?2",
                params![scope_key, key],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, bool>(7)?,
                        row.get::<_, Option<String>>(8)?,
                        row.get::<_, Option<String>>(9)?,
                    ))
                },
            )
            .optional()?
        {
            if stored_hash != payload_hash || kind != "job" {
                return Err(StoreError::IdempotencyConflict);
            }
            if state == "accepted" {
                let job_id = job_id.ok_or_else(|| {
                    StoreError::InvalidState("accepted submission has no job".into())
                })?;
                let result = SubmitResult {
                    receipt: self.receipt(
                        SubmissionId::from_parts(self.store_uuid, Uuid::parse_str(&submission_id)?),
                        JobId::from_parts(self.store_uuid, Uuid::parse_str(&job_id)?),
                    )?,
                    should_schedule: false,
                };
                return Ok(result);
            }
            if state == "received" {
                let wait_for_completion = durable_wait || wait_for_completion;
                if wait_for_completion && !durable_wait {
                    self.connection.execute(
                        "UPDATE submissions SET wait_intent = 1 WHERE id = ?1 AND state = 'received'",
                        [&submission_id],
                    )?;
                }
                let durable_spec = serde_json::from_str(&spec_json)?;
                let durable_stdin = stdin_json
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()?;
                return self.accept_received(
                    SubmissionId::from_parts(self.store_uuid, Uuid::parse_str(&submission_id)?),
                    &durable_spec,
                    durable_stdin.as_ref(),
                    scope,
                    wait_for_completion,
                );
            }
            if state == "rejected" {
                return Err(retained_rejection(reject_code, reject_detail));
            }
            return Err(StoreError::InvalidState(format!(
                "terminal submission state {state} cannot be replaced"
            )));
        }

        self.verify_staged_input(spec, stdin)?;
        let submission_id = SubmissionId::new(self.store_uuid);
        let received = self.connection.transaction()?;
        validate_current_parent(&received, self.store_uuid, self.daemon_generation, scope)?;
        let parent = scope.parent();
        received.execute(
            "INSERT INTO submissions(
                id, scope, idempotency_key, payload_hash, state, spec_json, stdin_json, kind,
                parent_job_id, parent_attempt_id, parent_invocation_id, wait_intent, created_ms
             ) VALUES (?1, ?2, ?3, ?4, 'received', ?5, ?6, 'job', ?7, ?8, ?9, ?10, ?11)",
            params![
                submission_id.entity_uuid().to_string(),
                scope_key,
                key,
                payload_hash,
                serde_json::to_string(spec)?,
                stdin.map(serde_json::to_string).transpose()?,
                parent.map(|parent| parent.job_id.entity_uuid().to_string()),
                parent.map(|parent| parent.attempt_id.entity_uuid().to_string()),
                parent.map(|parent| parent.invocation_id.entity_uuid().to_string()),
                wait_for_completion,
                now_millis(),
            ],
        )?;
        received.commit()?;
        self.accept_received(submission_id, spec, stdin, scope, wait_for_completion)
    }

    #[cfg(test)]
    pub(crate) fn submit_batch(
        &mut self,
        idempotency_key: Uuid,
        claimed_payload_hash: &str,
        spec: &BatchSpec,
    ) -> StoreResult<BatchSubmitResult> {
        self.submit_batch_with_stdins(
            idempotency_key,
            claimed_payload_hash,
            spec,
            &Default::default(),
        )
    }

    #[cfg(test)]
    pub(crate) fn submit_batch_with_stdins(
        &mut self,
        idempotency_key: Uuid,
        claimed_payload_hash: &str,
        spec: &BatchSpec,
        stdins: &std::collections::BTreeMap<String, StagedInputRef>,
    ) -> StoreResult<BatchSubmitResult> {
        self.submit_batch_with_stdins_scoped(
            SubmissionScope::Unmanaged,
            idempotency_key,
            claimed_payload_hash,
            spec,
            stdins,
        )
    }

    #[cfg(test)]
    pub(crate) fn submit_batch_with_stdins_scoped(
        &mut self,
        scope: SubmissionScope,
        idempotency_key: Uuid,
        claimed_payload_hash: &str,
        spec: &BatchSpec,
        stdins: &std::collections::BTreeMap<String, StagedInputRef>,
    ) -> StoreResult<BatchSubmitResult> {
        self.submit_batch_with_stdins_scoped_for_wait(
            scope,
            idempotency_key,
            claimed_payload_hash,
            spec,
            stdins,
            false,
        )
    }

    pub(crate) fn submit_batch_with_stdins_scoped_for_wait(
        &mut self,
        scope: SubmissionScope,
        idempotency_key: Uuid,
        claimed_payload_hash: &str,
        spec: &BatchSpec,
        stdins: &std::collections::BTreeMap<String, StagedInputRef>,
        wait_for_completion: bool,
    ) -> StoreResult<BatchSubmitResult> {
        spec.validate()
            .map_err(|error| StoreError::InvalidSpec(error.to_string()))?;
        for member in &spec.jobs {
            self.validate_host_job(&member.spec)?;
        }
        validate_batch_input_shape(spec, stdins)?;
        let payload_hash = normalized_batch_payload_hash_with_inputs(spec, stdins)?;
        if claimed_payload_hash != payload_hash {
            return Err(StoreError::InvalidSpec(
                "payload hash does not match the normalized specification".into(),
            ));
        }
        let key = idempotency_key.to_string();
        let scope_key = scope.key();
        if let Some((
            submission,
            stored_hash,
            state,
            batch,
            spec_json,
            stdin_json,
            kind,
            durable_wait,
            reject_code,
            reject_detail,
        )) = self
            .connection
            .query_row(
                "SELECT id, payload_hash, state, batch_id, spec_json, stdin_json, kind, wait_intent,
                        reject_code, reject_detail
                 FROM submissions WHERE scope = ?1 AND idempotency_key = ?2",
                params![scope_key, key],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, bool>(7)?,
                        row.get::<_, Option<String>>(8)?,
                        row.get::<_, Option<String>>(9)?,
                    ))
                },
            )
            .optional()?
        {
            if stored_hash != payload_hash || kind != "batch" {
                return Err(StoreError::IdempotencyConflict);
            }
            let submission_id =
                SubmissionId::from_parts(self.store_uuid, Uuid::parse_str(&submission)?);
            if state == "accepted" {
                let batch_id = batch.ok_or_else(|| {
                    StoreError::InvalidState("accepted batch submission has no batch".into())
                })?;
                let result = BatchSubmitResult {
                    receipt: self.batch_receipt(
                        submission_id,
                        BatchId::from_parts(self.store_uuid, Uuid::parse_str(&batch_id)?),
                    )?,
                    should_schedule: false,
                };
                return Ok(result);
            }
            if state == "received" {
                let wait_for_completion = durable_wait || wait_for_completion;
                if wait_for_completion && !durable_wait {
                    self.connection.execute(
                        "UPDATE submissions SET wait_intent = 1 WHERE id = ?1 AND state = 'received'",
                        [&submission],
                    )?;
                }
                let durable: BatchSpec = serde_json::from_str(&spec_json)?;
                let durable_stdins = stdin_json
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()?
                    .unwrap_or_default();
                return self.accept_received_batch(
                    submission_id,
                    &durable,
                    &durable_stdins,
                    scope,
                    wait_for_completion,
                );
            }
            if state == "rejected" {
                return Err(retained_rejection(reject_code, reject_detail));
            }
            return Err(StoreError::InvalidState(format!(
                "terminal submission state {state} cannot be replaced"
            )));
        }

        self.verify_staged_batch_inputs(spec, stdins)?;
        let submission_id = SubmissionId::new(self.store_uuid);
        let received = self.connection.transaction()?;
        validate_current_parent(&received, self.store_uuid, self.daemon_generation, scope)?;
        let parent = scope.parent();
        received.execute(
            "INSERT INTO submissions(
                id, scope, idempotency_key, payload_hash, state, spec_json, stdin_json, kind,
                parent_job_id, parent_attempt_id, parent_invocation_id, wait_intent, created_ms
             ) VALUES (?1, ?2, ?3, ?4, 'received', ?5, ?6, 'batch', ?7, ?8, ?9, ?10, ?11)",
            params![
                submission_id.entity_uuid().to_string(),
                scope_key,
                key,
                payload_hash,
                serde_json::to_string(spec)?,
                serde_json::to_string(stdins)?,
                parent.map(|parent| parent.job_id.entity_uuid().to_string()),
                parent.map(|parent| parent.attempt_id.entity_uuid().to_string()),
                parent.map(|parent| parent.invocation_id.entity_uuid().to_string()),
                wait_for_completion,
                now_millis(),
            ],
        )?;
        received.commit()?;
        self.accept_received_batch(submission_id, spec, stdins, scope, wait_for_completion)
    }

    pub(super) fn accept_received_batch(
        &mut self,
        submission_id: SubmissionId,
        spec: &BatchSpec,
        stdins: &std::collections::BTreeMap<String, StagedInputRef>,
        scope: SubmissionScope,
        wait_for_completion: bool,
    ) -> StoreResult<BatchSubmitResult> {
        if let Err(error) = self.verify_staged_batch_inputs(spec, stdins) {
            self.reject_received_with(submission_id, error_code::REJECTED, &error.to_string())?;
            return Err(StoreError::Rejected(error.to_string()));
        }
        let batch_id = BatchId::new(self.store_uuid);
        let accepted_ms = now_millis();
        let jobs: StoreResult<Vec<_>> = spec
            .jobs
            .iter()
            .map(|member| {
                Ok((
                    JobId::new(self.store_uuid),
                    ResolvedClaims::resolve(&member.spec.resources)
                        .map_err(|error| StoreError::InvalidSpec(error.to_string()))?,
                    member.spec.clone(),
                    stdins.get(&member.name).cloned(),
                ))
            })
            .collect();
        let jobs = match jobs {
            Ok(jobs) => jobs,
            Err(error) => {
                self.reject_received_for_error(submission_id, &error)?;
                return Err(StoreError::Rejected(error.to_string()));
            }
        };
        let names: std::collections::HashMap<_, _> = spec
            .jobs
            .iter()
            .zip(&jobs)
            .map(|(member, (job_id, _, _, _))| (member.name.as_str(), *job_id))
            .collect();
        let store_uuid = self.store_uuid;
        let daemon_generation = self.daemon_generation;
        let capacities = self.capacities.clone();
        let impact_incompatibilities = self.impact_incompatibilities.clone();
        let transaction = self.connection.transaction()?;
        let state: String = transaction.query_row(
            "SELECT state FROM submissions WHERE id = ?1",
            [submission_id.entity_uuid().to_string()],
            |row| row.get(0),
        )?;
        if state == "accepted" {
            let existing: String = transaction.query_row(
                "SELECT batch_id FROM submissions WHERE id = ?1",
                [submission_id.entity_uuid().to_string()],
                |row| row.get(0),
            )?;
            transaction.commit()?;
            return Ok(BatchSubmitResult {
                receipt: self.batch_receipt(
                    submission_id,
                    BatchId::from_parts(self.store_uuid, Uuid::parse_str(&existing)?),
                )?,
                should_schedule: false,
            });
        }
        if state != "received" {
            return Err(StoreError::InvalidState(format!(
                "submission {submission_id} is terminal in state {state}"
            )));
        }
        if let Err(error) =
            validate_current_parent(&transaction, self.store_uuid, self.daemon_generation, scope)
        {
            drop(transaction);
            self.reject_received_for_error(submission_id, &error)?;
            return Err(StoreError::Rejected(error.to_string()));
        }
        let parent = scope.parent();
        transaction.execute(
            "INSERT INTO batches(id, state, submission_id, accepted_ms)
             VALUES (?1, 'retained', ?2, ?3)",
            params![
                batch_id.entity_uuid().to_string(),
                submission_id.entity_uuid().to_string(),
                accepted_ms,
            ],
        )?;
        for (index, (member, (job_id, claims, accepted_spec, stdin))) in
            spec.jobs.iter().zip(&jobs).enumerate()
        {
            transaction.execute(
                "INSERT INTO jobs(
                    id, submission_id, batch_id, batch_member, batch_index, state,
                    spec_json, claims_json, stdin_hash, stdin_len,
                    parent_job_id, parent_attempt_id, parent_invocation_id, accepted_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5, 'pending', ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                params![
                    job_id.entity_uuid().to_string(),
                    submission_id.entity_uuid().to_string(),
                    batch_id.entity_uuid().to_string(),
                    member.name,
                    index as u64,
                    serde_json::to_string(accepted_spec)?,
                    serde_json::to_string(claims)?,
                    stdin.as_ref().map(|stdin| stdin.sha256.as_str()),
                    stdin.as_ref().map(|stdin| stdin.length),
                    parent.map(|parent| parent.job_id.entity_uuid().to_string()),
                    parent.map(|parent| parent.attempt_id.entity_uuid().to_string()),
                    parent.map(|parent| parent.invocation_id.entity_uuid().to_string()),
                    accepted_ms,
                ],
            )?;
        }
        for (member, (successor, _, _, _)) in spec.jobs.iter().zip(&jobs) {
            for dependency in &member.dependencies {
                let predecessor = names.get(dependency.job.as_str()).copied().ok_or_else(|| {
                    StoreError::InvalidState(format!(
                        "retained batch member {} has unknown predecessor {}",
                        member.name, dependency.job
                    ))
                })?;
                transaction.execute(
                    "INSERT INTO dependencies(predecessor_id, successor_id, kind)
                     VALUES (?1, ?2, ?3)",
                    params![
                        predecessor.entity_uuid().to_string(),
                        successor.entity_uuid().to_string(),
                        dependency_kind(dependency.on),
                    ],
                )?;
            }
        }
        if wait_for_completion {
            let targets = jobs
                .iter()
                .map(|(job_id, _, _, _)| *job_id)
                .collect::<Vec<_>>();
            if let Err(error) = validate_managed_wait_targets(
                &transaction,
                store_uuid,
                daemon_generation,
                &capacities,
                &impact_incompatibilities,
                scope,
                &targets,
            ) {
                drop(transaction);
                self.reject_received_for_error(submission_id, &error)?;
                return Err(error);
            }
        }
        transaction.execute(
            "UPDATE submissions SET state = 'accepted', batch_id = ?2,
                daemon_generation = ?3 WHERE id = ?1",
            params![
                submission_id.entity_uuid().to_string(),
                batch_id.entity_uuid().to_string(),
                daemon_generation.to_string()
            ],
        )?;
        transaction.commit()?;
        Ok(BatchSubmitResult {
            receipt: self.batch_receipt(submission_id, batch_id)?,
            should_schedule: true,
        })
    }

    pub(super) fn batch_receipt(
        &self,
        submission_id: SubmissionId,
        batch_id: BatchId,
    ) -> StoreResult<BatchReceipt> {
        if batch_id.store_uuid() != self.store_uuid {
            return Err(StoreError::NotFound(batch_id.to_string()));
        }
        let mut statement = self.connection.prepare(
            "SELECT id, batch_member FROM jobs WHERE batch_id = ?1 ORDER BY batch_index",
        )?;
        let rows = statement.query_map([batch_id.entity_uuid().to_string()], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut jobs = Vec::new();
        for row in rows {
            let (job, name) = row?;
            let job_id = JobId::from_parts(self.store_uuid, Uuid::parse_str(&job)?);
            jobs.push(BatchJobReceipt {
                name,
                receipt: self.receipt(submission_id, job_id)?,
            });
        }
        if jobs.is_empty() {
            return Err(StoreError::InvalidState(format!(
                "retained batch {batch_id} has no members"
            )));
        }
        Ok(BatchReceipt {
            submission_id,
            batch_id,
            submission_state: SubmissionState::Accepted,
            jobs,
            daemon_generation: self.accepting_daemon_generation(submission_id)?,
        })
    }

    pub(super) fn reject_received(&mut self, submission_id: SubmissionId) -> StoreResult<()> {
        self.reject_received_with(
            submission_id,
            error_code::REJECTED,
            "the retained submission decision is rejected",
        )
    }

    pub(super) fn reject_received_with(
        &mut self,
        submission_id: SubmissionId,
        code: &str,
        detail: &str,
    ) -> StoreResult<()> {
        self.connection.execute(
            "UPDATE submissions
             SET state = 'rejected', reject_code = ?2, reject_detail = ?3
             WHERE id = ?1 AND state = 'received'",
            params![submission_id.entity_uuid().to_string(), code, detail],
        )?;
        Ok(())
    }

    pub(super) fn reject_received_for_error(
        &mut self,
        submission_id: SubmissionId,
        error: &StoreError,
    ) -> StoreResult<()> {
        let (code, detail) = rejection_decision(error);
        self.reject_received_with(submission_id, &code, &detail)
    }

    #[cfg(test)]
    pub(crate) fn recover_submission(
        &self,
        idempotency_key: Uuid,
        payload_hash: &str,
    ) -> StoreResult<RecoveryResult> {
        self.recover_submission_scoped(SubmissionScope::Unmanaged, idempotency_key, payload_hash)
    }

    pub(crate) fn recover_submission_scoped(
        &self,
        scope: SubmissionScope,
        idempotency_key: Uuid,
        payload_hash: &str,
    ) -> StoreResult<RecoveryResult> {
        let scope_key = scope.key();
        let row = self
            .connection
            .query_row(
                "SELECT id, payload_hash, state, job_id, batch_id, kind,
                        reject_code, reject_detail
                 FROM submissions WHERE scope = ?1 AND idempotency_key = ?2",
                params![scope_key, idempotency_key.to_string()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, Option<String>>(7)?,
                    ))
                },
            )
            .optional()?;
        let Some((
            submission_id,
            stored_hash,
            state,
            job_id,
            batch_id,
            kind,
            reject_code,
            reject_detail,
        )) = row
        else {
            return match scope {
                SubmissionScope::Unmanaged => Ok(RecoveryResult::Unknown),
                SubmissionScope::Managed(_) => {
                    match validate_current_parent(
                        &self.connection,
                        self.store_uuid,
                        self.daemon_generation,
                        scope,
                    ) {
                        Ok(()) => Ok(RecoveryResult::NotReceived),
                        Err(StoreError::Rejected(_)) => Ok(RecoveryResult::Unknown),
                        Err(error) => Err(error),
                    }
                }
            };
        };
        if stored_hash != payload_hash {
            return Ok(RecoveryResult::Conflict);
        }
        let submission_id =
            SubmissionId::from_parts(self.store_uuid, Uuid::parse_str(&submission_id)?);
        match state.as_str() {
            "received" => Ok(RecoveryResult::Received { submission_id }),
            "accepted" => {
                if kind == "batch" {
                    let batch_id = batch_id.ok_or_else(|| {
                        StoreError::InvalidState("accepted batch submission has no batch".into())
                    })?;
                    Ok(RecoveryResult::AcceptedBatch(self.batch_receipt(
                        submission_id,
                        BatchId::from_parts(self.store_uuid, Uuid::parse_str(&batch_id)?),
                    )?))
                } else {
                    let job_id = job_id.ok_or_else(|| {
                        StoreError::InvalidState("accepted submission has no job".into())
                    })?;
                    Ok(RecoveryResult::Accepted(self.receipt(
                        submission_id,
                        JobId::from_parts(self.store_uuid, Uuid::parse_str(&job_id)?),
                    )?))
                }
            }
            "rejected" => Ok(RecoveryResult::Rejected {
                code: reject_code.unwrap_or_else(|| error_code::REJECTED.into()),
                detail: reject_detail.unwrap_or_else(|| "submission was rejected".into()),
            }),
            other => Err(StoreError::InvalidState(format!(
                "unknown submission state {other}"
            ))),
        }
    }

    pub(super) fn accept_received(
        &mut self,
        submission_id: SubmissionId,
        spec: &JobSpec,
        stdin: Option<&StagedInputRef>,
        scope: SubmissionScope,
        wait_for_completion: bool,
    ) -> StoreResult<SubmitResult> {
        if let Err(error) = self.verify_staged_input(spec, stdin) {
            self.reject_received_with(submission_id, error_code::REJECTED, &error.to_string())?;
            return Err(StoreError::Rejected(error.to_string()));
        }
        let accepted_spec = spec.clone();
        let job_id = JobId::new(self.store_uuid);
        let claims = match ResolvedClaims::resolve(&spec.resources) {
            Ok(claims) => claims,
            Err(error) => {
                self.reject_received_with(submission_id, error_code::REJECTED, &error.to_string())?;
                return Err(StoreError::Rejected(error.to_string()));
            }
        };
        let accepted_ms = now_millis();
        let store_uuid = self.store_uuid;
        let daemon_generation = self.daemon_generation;
        let capacities = self.capacities.clone();
        let impact_incompatibilities = self.impact_incompatibilities.clone();
        let transaction = self.connection.transaction()?;
        let state: String = transaction.query_row(
            "SELECT state FROM submissions WHERE id = ?1",
            [submission_id.entity_uuid().to_string()],
            |row| row.get(0),
        )?;
        if state == "accepted" {
            let existing: String = transaction.query_row(
                "SELECT job_id FROM submissions WHERE id = ?1",
                [submission_id.entity_uuid().to_string()],
                |row| row.get(0),
            )?;
            transaction.commit()?;
            return Ok(SubmitResult {
                receipt: self.receipt(
                    submission_id,
                    JobId::from_parts(self.store_uuid, Uuid::parse_str(&existing)?),
                )?,
                should_schedule: false,
            });
        }
        if state != "received" {
            return Err(StoreError::InvalidState(format!(
                "submission {submission_id} is terminal in state {state}"
            )));
        }
        if let Err(error) =
            validate_current_parent(&transaction, self.store_uuid, self.daemon_generation, scope)
        {
            drop(transaction);
            self.reject_received_for_error(submission_id, &error)?;
            return Err(StoreError::Rejected(error.to_string()));
        }
        let parent = scope.parent();
        transaction.execute(
            "INSERT INTO jobs(
                id, submission_id, state, spec_json, claims_json, stdin_hash, stdin_len,
                parent_job_id, parent_attempt_id, parent_invocation_id, accepted_ms
             ) VALUES (?1, ?2, 'pending', ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                job_id.entity_uuid().to_string(),
                submission_id.entity_uuid().to_string(),
                serde_json::to_string(&accepted_spec)?,
                serde_json::to_string(&claims)?,
                stdin.map(|stdin| stdin.sha256.as_str()),
                stdin.map(|stdin| stdin.length),
                parent.map(|parent| parent.job_id.entity_uuid().to_string()),
                parent.map(|parent| parent.attempt_id.entity_uuid().to_string()),
                parent.map(|parent| parent.invocation_id.entity_uuid().to_string()),
                accepted_ms,
            ],
        )?;
        if wait_for_completion {
            if let Err(error) = validate_managed_wait_targets(
                &transaction,
                store_uuid,
                daemon_generation,
                &capacities,
                &impact_incompatibilities,
                scope,
                &[job_id],
            ) {
                drop(transaction);
                self.reject_received_for_error(submission_id, &error)?;
                return Err(error);
            }
        }
        transaction.execute(
            "UPDATE submissions SET state = 'accepted', job_id = ?2,
                daemon_generation = ?3 WHERE id = ?1",
            params![
                submission_id.entity_uuid().to_string(),
                job_id.entity_uuid().to_string(),
                daemon_generation.to_string()
            ],
        )?;
        transaction.commit()?;
        Ok(SubmitResult {
            receipt: self.receipt(submission_id, job_id)?,
            should_schedule: true,
        })
    }

    pub(crate) fn receipt(
        &self,
        submission_id: SubmissionId,
        job_id: JobId,
    ) -> StoreResult<JobReceipt> {
        let state: String = self.connection.query_row(
            "SELECT state FROM jobs WHERE id = ?1",
            [self.local_id(job_id)?],
            |row| row.get(0),
        )?;
        let queue_rank = if state == "pending" {
            Some(self.connection.query_row(
                "SELECT COUNT(*) FROM jobs
                 WHERE state = 'pending' AND rowid <= (
                     SELECT rowid FROM jobs WHERE id = ?1
                 )",
                [job_id.entity_uuid().to_string()],
                |row| row.get::<_, u64>(0),
            )?)
        } else {
            None
        };
        let blockers = if state == "pending" {
            self.blockers_for_job(job_id)?
        } else {
            Vec::new()
        };
        let estimate = self.estimate_for_job(job_id, &blockers)?;
        let parent = self.parent_for_job(job_id)?;
        Ok(JobReceipt {
            submission_id,
            job_id,
            submission_state: SubmissionState::Accepted,
            job_state: parse_job_state(&state)?,
            blockers,
            queue_rank,
            estimate,
            parent,
            gpu_provenance: self.gpu_provenance_for_job(job_id)?,
            admission: self.admission_for_job(job_id)?,
            daemon_generation: self.accepting_daemon_generation(submission_id)?,
        })
    }

    pub(crate) fn managed_containment_candidates(&self) -> StoreResult<Vec<ManagedCandidate>> {
        let current_generation = self.daemon_generation.to_string();
        let mut statement = self.connection.prepare(
            "SELECT jobs.id, attempts.id, invocations.id, jobs.spec_json, jobs.parent_job_id,
                    jobs.state, jobs.attempt_id, jobs.invocation_id, attempts.state,
                    invocations.state, invocations.root_pid, invocations.root_exit_code,
                    invocations.daemon_generation, containments.state
             FROM invocations
             JOIN attempts ON attempts.id = invocations.attempt_id
             JOIN jobs ON jobs.id = attempts.job_id
             JOIN containments ON containments.invocation_id = invocations.id
             WHERE invocations.role = 'primary'
               AND invocations.daemon_generation = ?1
               AND containments.state = 'live'",
        )?;
        let rows = statement.query_map([&current_generation], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, Option<String>>(7)?,
                row.get::<_, String>(8)?,
                row.get::<_, String>(9)?,
                row.get::<_, Option<u32>>(10)?,
                row.get::<_, Option<i32>>(11)?,
                row.get::<_, Option<String>>(12)?,
                row.get::<_, String>(13)?,
            ))
        })?;
        let mut candidates = Vec::new();
        for row in rows {
            let (
                job,
                attempt,
                invocation,
                spec_json,
                parent_job,
                job_state,
                job_attempt,
                job_invocation,
                attempt_state,
                invocation_state,
                root_pid,
                root_exit_code,
                daemon_generation,
                containment_state,
            ) = row?;
            let spec: JobSpec = serde_json::from_str(&spec_json)?;
            let current = job_state == "active"
                && job_attempt.as_deref() == Some(attempt.as_str())
                && job_invocation.as_deref() == Some(invocation.as_str())
                && attempt_state == "running"
                && invocation_state == "started"
                && root_pid.is_some()
                && root_exit_code.is_none()
                && daemon_generation.as_deref() == Some(current_generation.as_str())
                && containment_state == "live";
            candidates.push(ManagedCandidate {
                parent: ManagedParent {
                    job_id: JobId::from_parts(self.store_uuid, Uuid::parse_str(&job)?),
                    attempt_id: AttemptId::from_parts(self.store_uuid, Uuid::parse_str(&attempt)?),
                    invocation_id: InvocationId::from_parts(
                        self.store_uuid,
                        Uuid::parse_str(&invocation)?,
                    ),
                },
                parent_job_id: parent_job
                    .map(|job| Uuid::parse_str(&job))
                    .transpose()?
                    .map(|job| JobId::from_parts(self.store_uuid, job)),
                submissions_enabled: spec.allow_child_submissions,
                current,
            });
        }
        Ok(candidates)
    }
}

pub(super) fn rejection_decision(error: &StoreError) -> (String, String) {
    match error {
        StoreError::BlockedByAncestor(detail) => {
            (error_code::BLOCKED_BY_ANCESTOR.into(), detail.clone())
        }
        StoreError::ManagedWaitRejected { code, detail } => (code.clone(), detail.clone()),
        _ => (error_code::REJECTED.into(), error.to_string()),
    }
}

pub(super) fn retained_rejection(code: Option<String>, detail: Option<String>) -> StoreError {
    let code = code.unwrap_or_else(|| error_code::REJECTED.into());
    let detail = detail.unwrap_or_else(|| "the retained submission decision is rejected".into());
    match code.as_str() {
        error_code::BLOCKED_BY_ANCESTOR => StoreError::BlockedByAncestor(detail),
        error_code::RESOURCE_CAPACITY => StoreError::ManagedWaitRejected { code, detail },
        _ => StoreError::Rejected(detail),
    }
}

pub(super) fn validate_current_parent(
    connection: &Connection,
    store_uuid: Uuid,
    daemon_generation: Uuid,
    scope: SubmissionScope,
) -> StoreResult<()> {
    let SubmissionScope::Managed(parent) = scope else {
        return Ok(());
    };
    if parent.job_id.store_uuid() != store_uuid
        || parent.attempt_id.store_uuid() != store_uuid
        || parent.invocation_id.store_uuid() != store_uuid
    {
        return Err(StoreError::Rejected(
            "managed parent belongs to a foreign store".into(),
        ));
    }
    let spec_json = connection
        .query_row(
            "SELECT jobs.spec_json
             FROM jobs
             JOIN attempts ON attempts.id = jobs.attempt_id
             JOIN invocations ON invocations.id = jobs.invocation_id
             JOIN containments ON containments.invocation_id = invocations.id
             WHERE jobs.id = ?1
               AND attempts.id = ?2
               AND invocations.id = ?3
               AND jobs.state = 'active'
               AND attempts.state = 'running'
               AND invocations.state = 'started'
               AND invocations.role = 'primary'
               AND invocations.root_pid IS NOT NULL
               AND invocations.root_exit_code IS NULL
               AND invocations.daemon_generation = ?4
               AND containments.state = 'live'",
            params![
                parent.job_id.entity_uuid().to_string(),
                parent.attempt_id.entity_uuid().to_string(),
                parent.invocation_id.entity_uuid().to_string(),
                daemon_generation.to_string(),
            ],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    let Some(spec_json) = spec_json else {
        return Err(StoreError::Rejected(
            "managed parent is no longer the live current primary Invocation".into(),
        ));
    };
    let spec: JobSpec = serde_json::from_str(&spec_json)?;
    if !spec.allow_child_submissions {
        return Err(StoreError::Rejected(
            "managed parent does not allow child submissions".into(),
        ));
    }
    Ok(())
}

pub(super) fn validate_managed_wait_targets(
    connection: &Connection,
    store_uuid: Uuid,
    daemon_generation: Uuid,
    capacities: &ResourceCapacities,
    impact_incompatibilities: &std::collections::BTreeMap<String, Vec<String>>,
    scope: SubmissionScope,
    targets: &[JobId],
) -> StoreResult<()> {
    let SubmissionScope::Managed(parent) = scope else {
        return Ok(());
    };
    if targets.is_empty() {
        return Err(StoreError::Rejected(
            "managed wait requires at least one target".into(),
        ));
    }
    validate_current_parent(connection, store_uuid, daemon_generation, scope)?;
    let ancestor_claims = managed_ancestor_claims(connection, store_uuid, parent)?;
    let mut pending = std::collections::VecDeque::from_iter(targets.iter().copied());
    let mut visited = std::collections::HashSet::new();
    let mut waited_claims = Vec::new();
    while let Some(job_id) = pending.pop_front() {
        if job_id.store_uuid() != store_uuid {
            return Err(StoreError::Rejected(format!(
                "managed wait target {job_id} belongs to a foreign store"
            )));
        }
        let job_key = job_id.entity_uuid().to_string();
        if !visited.insert(job_key.clone()) {
            continue;
        }
        if job_id == parent.job_id {
            return Err(StoreError::BlockedByAncestor(
                "the dependency closure reaches the waiting Job itself".into(),
            ));
        }
        if !job_descends_from(connection, store_uuid, job_id, parent.job_id)? {
            return Err(StoreError::Rejected(format!(
                "managed wait target {job_id} is not an authenticated descendant of {}",
                parent.job_id
            )));
        }
        let (state, claims_json, display_name) = connection
            .query_row(
                "SELECT state, claims_json, COALESCE(batch_member, id) FROM jobs WHERE id = ?1",
                [&job_key],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(job_id.to_string()))?;
        if state == "final" {
            continue;
        }
        let claims: ResolvedClaims = serde_json::from_str(&claims_json)?;
        waited_claims.push((job_id, display_name, claims));
        let mut statement = connection.prepare(
            "SELECT dependencies.predecessor_id
             FROM dependencies
             JOIN jobs ON jobs.id = dependencies.predecessor_id
             WHERE dependencies.successor_id = ?1 AND jobs.state != 'final'
             ORDER BY jobs.accepted_ms, jobs.rowid",
        )?;
        let predecessors = statement
            .query_map([&job_key], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        for predecessor in predecessors {
            pending.push_back(JobId::from_parts(
                store_uuid,
                Uuid::parse_str(&predecessor)?,
            ));
        }
    }
    for (job_id, display_name, claims) in waited_claims {
        let blockers =
            claims.ancestor_blockers(capacities, &ancestor_claims, impact_incompatibilities);
        if !blockers.is_empty() {
            let detail = format!(
                "target {display_name} ({job_id}): {}",
                blockers
                    .iter()
                    .map(|blocker| blocker.detail.as_str())
                    .collect::<Vec<_>>()
                    .join("; ")
            );
            if blockers
                .iter()
                .any(|blocker| blocker.code == error_code::RESOURCE_CAPACITY)
            {
                return Err(StoreError::ManagedWaitRejected {
                    code: error_code::RESOURCE_CAPACITY.into(),
                    detail,
                });
            }
            return Err(StoreError::BlockedByAncestor(detail));
        }
    }
    Ok(())
}

pub(super) fn job_descends_from(
    connection: &Connection,
    store_uuid: Uuid,
    job_id: JobId,
    ancestor_id: JobId,
) -> StoreResult<bool> {
    let mut current = job_id;
    let mut visited = std::collections::HashSet::new();
    loop {
        if !visited.insert(current.entity_uuid()) {
            return Err(StoreError::InvalidState(
                "managed parent graph contains a cycle".into(),
            ));
        }
        let columns = connection
            .query_row(
                "SELECT parent_job_id, parent_attempt_id, parent_invocation_id
                 FROM jobs WHERE id = ?1",
                [current.entity_uuid().to_string()],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(current.to_string()))?;
        let Some(parent) = managed_parent_from_columns(store_uuid, columns)? else {
            return Ok(false);
        };
        if parent.job_id == ancestor_id {
            return Ok(true);
        }
        current = parent.job_id;
    }
}

pub(super) fn managed_ancestor_claims(
    connection: &Connection,
    store_uuid: Uuid,
    parent: ManagedParent,
) -> StoreResult<Vec<ResolvedClaims>> {
    let mut current = Some(parent);
    let mut visited = std::collections::HashSet::new();
    let mut claims = Vec::new();
    while let Some(ancestor) = current {
        if !visited.insert((
            ancestor.job_id.entity_uuid(),
            ancestor.attempt_id.entity_uuid(),
        )) {
            return Err(StoreError::InvalidState(
                "managed ancestor graph contains a cycle".into(),
            ));
        }
        let lease = connection
            .query_row(
                "SELECT leases.state, leases.claims_json
                 FROM leases
                 JOIN attempts ON attempts.id = leases.attempt_id
                 WHERE attempts.id = ?1 AND attempts.job_id = ?2",
                params![
                    ancestor.attempt_id.entity_uuid().to_string(),
                    ancestor.job_id.entity_uuid().to_string(),
                ],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?
            .ok_or_else(|| {
                StoreError::InvalidState(format!(
                    "managed ancestor {} has no Lease for Attempt {}",
                    ancestor.job_id, ancestor.attempt_id
                ))
            })?;
        match lease.0.as_str() {
            "granted" => claims.push(serde_json::from_str(&lease.1)?),
            "released" => {}
            other => {
                return Err(StoreError::InvalidState(format!(
                    "managed ancestor Lease has unknown state {other}"
                )));
            }
        }
        let columns = connection.query_row(
            "SELECT parent_job_id, parent_attempt_id, parent_invocation_id
             FROM jobs WHERE id = ?1",
            [ancestor.job_id.entity_uuid().to_string()],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            },
        )?;
        current = managed_parent_from_columns(store_uuid, columns)?;
    }
    Ok(claims)
}

pub(super) fn managed_parent_from_columns(
    store_uuid: Uuid,
    columns: (Option<String>, Option<String>, Option<String>),
) -> StoreResult<Option<ManagedParent>> {
    match columns {
        (None, None, None) => Ok(None),
        (Some(job), Some(attempt), Some(invocation)) => Ok(Some(ManagedParent {
            job_id: JobId::from_parts(store_uuid, Uuid::parse_str(&job)?),
            attempt_id: AttemptId::from_parts(store_uuid, Uuid::parse_str(&attempt)?),
            invocation_id: InvocationId::from_parts(store_uuid, Uuid::parse_str(&invocation)?),
        })),
        _ => Err(StoreError::InvalidState(
            "managed parent columns are only partially populated".into(),
        )),
    }
}

pub(super) fn dependency_blockers_tx(
    transaction: &rusqlite::Transaction<'_>,
    job_id: JobId,
) -> StoreResult<(Vec<Blocker>, bool)> {
    let mut statement = transaction.prepare(
        "SELECT dependencies.kind, jobs.state, jobs.outcome, jobs.batch_member
         FROM dependencies JOIN jobs ON jobs.id = dependencies.predecessor_id
         WHERE dependencies.successor_id = ?1 ORDER BY jobs.batch_index",
    )?;
    let rows = statement.query_map([job_id.entity_uuid().to_string()], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<String>>(3)?,
        ))
    })?;
    let mut blockers = Vec::new();
    let mut impossible = false;
    for row in rows {
        let (kind, state, outcome, name) = row?;
        if state != "final" {
            blockers.push(Blocker {
                code: "dependency_pending".into(),
                detail: name.unwrap_or_else(|| "predecessor".into()),
            });
            continue;
        }
        let satisfied = match kind.as_str() {
            "success" => outcome.as_deref() == Some("succeeded"),
            "failure" => outcome.as_deref() == Some("failed"),
            "terminal" => true,
            other => {
                return Err(StoreError::InvalidState(format!(
                    "unknown dependency kind {other}"
                )));
            }
        };
        impossible |= !satisfied;
    }
    Ok((blockers, impossible))
}

pub(super) fn active_claims_tx(
    transaction: &rusqlite::Transaction<'_>,
) -> StoreResult<Vec<ResolvedClaims>> {
    let mut statement =
        transaction.prepare("SELECT claims_json FROM leases WHERE state = 'granted'")?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
}

pub(super) fn dependency_kind(kind: crate::DependencyKind) -> &'static str {
    match kind {
        crate::DependencyKind::Success => "success",
        crate::DependencyKind::Failure => "failure",
        crate::DependencyKind::Terminal => "terminal",
    }
}
