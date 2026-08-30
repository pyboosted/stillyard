use super::*;

impl Store {
    pub(super) fn accepting_daemon_generation(
        &self,
        submission_id: SubmissionId,
    ) -> StoreResult<Uuid> {
        let value: String = self.connection.query_row(
            "SELECT daemon_generation FROM submissions WHERE id = ?1 AND state = 'accepted'",
            [submission_id.entity_uuid().to_string()],
            |row| row.get(0),
        )?;
        Ok(Uuid::parse_str(&value)?)
    }

    pub(super) fn parent_for_job(&self, job_id: JobId) -> StoreResult<Option<ManagedParent>> {
        let row = self.connection.query_row(
            "SELECT parent_job_id, parent_attempt_id, parent_invocation_id
             FROM jobs WHERE id = ?1",
            [self.local_id(job_id)?],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            },
        )?;
        managed_parent_from_columns(self.store_uuid, row)
    }

    pub(super) fn gpu_provenance_for_job(
        &self,
        job_id: JobId,
    ) -> StoreResult<Option<crate::GpuProvenance>> {
        self.connection
            .query_row(
                "SELECT admissions.gpu_uuid, admissions.gpu_driver_version
                 FROM admissions JOIN attempts ON attempts.id = admissions.attempt_id
                 WHERE attempts.job_id = ?1 AND admissions.gpu_uuid IS NOT NULL
                   AND admissions.gpu_driver_version IS NOT NULL
                 ORDER BY attempts.attempt_index DESC LIMIT 1",
                [self.local_id(job_id)?],
                |row| {
                    Ok(crate::GpuProvenance {
                        uuid: row.get(0)?,
                        driver_version: row.get(1)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub(super) fn admission_for_job(
        &self,
        job_id: JobId,
    ) -> StoreResult<Option<AdmissionDecisionSnapshot>> {
        let attempt_id = self
            .connection
            .query_row(
                "SELECT admissions.attempt_id FROM admissions
                 JOIN attempts ON attempts.id = admissions.attempt_id
                 WHERE attempts.job_id = ?1
                 ORDER BY attempts.attempt_index DESC LIMIT 1",
                [self.local_id(job_id)?],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        attempt_id
            .as_deref()
            .map(|attempt_id| self.admission_for_attempt_key(attempt_id))
            .transpose()
    }

    pub(super) fn admission_for_attempt(
        &self,
        attempt_id: AttemptId,
    ) -> StoreResult<Option<AdmissionDecisionSnapshot>> {
        if attempt_id.store_uuid() != self.store_uuid {
            return Err(StoreError::NotFound(attempt_id.to_string()));
        }
        self.admission_for_attempt_key(&attempt_id.entity_uuid().to_string())
            .map(Some)
            .or_else(|error| match error {
                StoreError::Sqlite(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                other => Err(other),
            })
    }

    fn admission_for_attempt_key(
        &self,
        attempt_id: &str,
    ) -> StoreResult<AdmissionDecisionSnapshot> {
        let (
            attempt_state,
            verdict,
            deferral_count,
            last_eval_unix_millis,
            last_blockers_json,
            last_evidence_json,
            reservation_evidence_json,
            release_evidence_json,
            gpu_uuid,
            gpu_driver_version,
        ) = self.connection.query_row(
            "SELECT attempts.state, attempts.verdict, admissions.deferral_count,
                    admissions.last_eval_unix_ms, admissions.last_blockers_json,
                    admissions.last_evidence_json, admissions.reservation_evidence_json,
                    admissions.release_evidence_json, admissions.gpu_uuid,
                    admissions.gpu_driver_version
             FROM admissions JOIN attempts ON attempts.id = admissions.attempt_id
             WHERE admissions.attempt_id = ?1",
            [attempt_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, u32>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, Option<String>>(8)?,
                    row.get::<_, Option<String>>(9)?,
                ))
            },
        )?;
        let final_sample = release_evidence_json.is_some();
        let evidence_json = release_evidence_json
            .as_deref()
            .or(reservation_evidence_json.as_deref())
            .or(last_evidence_json.as_deref());
        let evidence = evidence_json
            .map(serde_json::from_str::<AdmissionEvidenceRecord>)
            .transpose()?
            .unwrap_or_default();
        let fallback_blockers = serde_json::from_str::<Vec<Blocker>>(&last_blockers_json)?;
        let state = if verdict.as_deref() == Some("safety_failed") {
            AdmissionDecisionState::Failed
        } else if attempt_state == "planned" && deferral_count > 0 {
            AdmissionDecisionState::Replanned
        } else if matches!(attempt_state.as_str(), "planned" | "admitting") {
            AdmissionDecisionState::Waiting
        } else if attempt_state == "starting" {
            AdmissionDecisionState::Reserved
        } else if matches!(attempt_state.as_str(), "running" | "settled")
            && reservation_evidence_json.is_some()
        {
            AdmissionDecisionState::Released
        } else {
            AdmissionDecisionState::Failed
        };
        let gpu_provenance = gpu_uuid
            .zip(gpu_driver_version)
            .map(|(uuid, driver_version)| GpuProvenance {
                uuid,
                driver_version,
            });
        Ok(AdmissionDecisionSnapshot {
            state,
            evaluated_unix_millis: evidence.evaluated_unix_millis.or(last_eval_unix_millis),
            observation_generation: evidence.observation_generation,
            blockers: if evidence.blockers.is_empty()
                && matches!(
                    state,
                    AdmissionDecisionState::Waiting
                        | AdmissionDecisionState::Replanned
                        | AdmissionDecisionState::Failed
                ) {
                fallback_blockers
            } else {
                evidence.blockers
            },
            operands: evidence.operands,
            detectors: evidence.detectors,
            gpu_provenance,
            final_sample,
            deferral_count,
        })
    }

    pub(super) fn blockers_for_job(&self, job_id: JobId) -> StoreResult<Vec<Blocker>> {
        let job_key = self.local_id(job_id)?;
        let mut blockers = self.dependency_blockers(&job_key)?.0;
        if !self.startup_identity.capable() {
            blockers.push(Blocker {
                code: "host_capability_unavailable".into(),
                detail: self.startup_identity.failures.join("; "),
            });
        }
        let retry_not_before: Option<i64> = self.connection.query_row(
            "SELECT retry_not_before_ms FROM jobs WHERE id = ?1",
            [&job_key],
            |row| row.get(0),
        )?;
        if retry_not_before.is_some_and(|instant| instant > now_millis()) {
            blockers.push(Blocker {
                code: "retry_backoff".into(),
                detail: format!("retry_not_before_unix_millis={}", retry_not_before.unwrap()),
            });
        }
        let claims: String = self.connection.query_row(
            "SELECT claims_json FROM jobs WHERE id = ?1",
            [&job_key],
            |row| row.get(0),
        )?;
        let claims: ResolvedClaims = serde_json::from_str(&claims)?;
        blockers.extend(claims.blockers(
            &self.capacities,
            &self.active_and_reserved_claims_before(&job_key)?,
            &self.impact_incompatibilities,
        ));
        let admission_blockers: Option<String> = self
            .connection
            .query_row(
                "SELECT admissions.last_blockers_json FROM admissions
                 JOIN jobs ON jobs.attempt_id = admissions.attempt_id
                 WHERE jobs.id = ?1",
                [&job_key],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(json) = admission_blockers {
            blockers.extend(serde_json::from_str::<Vec<Blocker>>(&json)?);
        }
        blockers.sort_by(|left, right| {
            left.code
                .cmp(&right.code)
                .then(left.detail.cmp(&right.detail))
        });
        blockers.dedup();
        Ok(blockers)
    }

    pub(super) fn active_and_reserved_claims_before(
        &self,
        job_key: &str,
    ) -> StoreResult<Vec<ResolvedClaims>> {
        let mut granted = self.active_claims()?;
        let mut statement = self.connection.prepare(
            "SELECT id, claims_json, retry_not_before_ms FROM jobs
             WHERE state = 'pending' AND rowid < (SELECT rowid FROM jobs WHERE id = ?1)
             ORDER BY accepted_ms, rowid",
        )?;
        let rows = statement.query_map([job_key], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<i64>>(2)?,
            ))
        })?;
        for row in rows {
            let (candidate, claims, retry_not_before) = row?;
            if retry_not_before.is_some_and(|instant| instant > now_millis()) {
                continue;
            }
            let (dependencies, impossible) = self.dependency_blockers(&candidate)?;
            if impossible || !dependencies.is_empty() {
                continue;
            }
            let claims: ResolvedClaims = serde_json::from_str(&claims)?;
            if claims
                .blockers(&self.capacities, &granted, &self.impact_incompatibilities)
                .is_empty()
            {
                granted.push(claims);
            }
        }
        Ok(granted)
    }

    pub(super) fn dependency_blockers(&self, job_key: &str) -> StoreResult<(Vec<Blocker>, bool)> {
        let mut statement = self.connection.prepare(
            "SELECT dependencies.kind, jobs.state, jobs.outcome, jobs.batch_member
             FROM dependencies JOIN jobs ON jobs.id = dependencies.predecessor_id
             WHERE dependencies.successor_id = ?1 ORDER BY jobs.batch_index",
        )?;
        let rows = statement.query_map([job_key], |row| {
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
            let label = name.unwrap_or_else(|| "predecessor".into());
            if state != "final" {
                blockers.push(Blocker {
                    code: "dependency_pending".into(),
                    detail: label,
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
            if !satisfied {
                impossible = true;
                blockers.push(Blocker {
                    code: "dependency_impossible".into(),
                    detail: format!("{label} finalized as {}", outcome.unwrap_or_default()),
                });
            }
        }
        Ok((blockers, impossible))
    }

    pub(super) fn active_claims(&self) -> StoreResult<Vec<ResolvedClaims>> {
        let mut statement = self
            .connection
            .prepare("SELECT claims_json FROM leases WHERE state = 'granted'")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
    }

    pub(super) fn estimate_for_job(
        &self,
        job_id: JobId,
        blockers: &[Blocker],
    ) -> StoreResult<Estimate> {
        if blockers.is_empty() {
            return Ok(Estimate {
                confidence: crate::EstimateConfidence::Estimated,
                start_in_millis: Some(0),
                assumptions: vec!["currently admissible".into()],
            });
        }
        if blockers.iter().any(|blocker| {
            blocker.code == "resource_capacity" || blocker.code == "dependency_impossible"
        }) {
            return Ok(Estimate::unknown(
                "a configured-capacity or impossible-dependency blocker has no time estimate",
            ));
        }
        if blockers
            .iter()
            .any(|blocker| blocker.code == "dependency_pending")
        {
            return Ok(Estimate::unknown(
                "dependency completion is not estimated without walking its full predecessor closure",
            ));
        }
        let claims_json: String = self.connection.query_row(
            "SELECT claims_json FROM jobs WHERE id = ?1",
            [self.local_id(job_id)?],
            |row| row.get(0),
        )?;
        let claims: ResolvedClaims = serde_json::from_str(&claims_json)?;
        let retained = self.settled_granted_claims()?;
        if !claims
            .blockers(&self.capacities, &retained, &self.impact_incompatibilities)
            .is_empty()
        {
            return Ok(Estimate::unknown(
                "a retained Lease from an uncertain Containment has no automatic release estimate",
            ));
        }
        let mut statement = self.connection.prepare(
            "SELECT accepted_ms, started_ms, spec_json, state FROM jobs
             WHERE id != ?1 AND (
                 state = 'active' OR (
                     state = 'pending' AND rowid < (SELECT rowid FROM jobs WHERE id = ?1)
                 )
             ) ORDER BY accepted_ms, rowid",
        )?;
        let rows = statement.query_map([self.local_id(job_id)?], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<i64>>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;
        let now = now_millis();
        let mut estimate = 0_u64;
        let mut saw_job = false;
        for row in rows {
            let (accepted, started, json, state) = row?;
            saw_job = true;
            let spec: JobSpec = serde_json::from_str(&json)?;
            let Some(seconds) = spec.expected_duration_seconds else {
                return Ok(Estimate::unknown(
                    "at least one running or earlier queued job has no declared duration",
                ));
            };
            let elapsed = started
                .map(|began| now.saturating_sub(began) as u64)
                .unwrap_or(0);
            if state == "active" && elapsed >= seconds.saturating_mul(1000) {
                return Ok(Estimate::unknown(
                    "a running job has exceeded its declared duration",
                ));
            }
            let _ = accepted;
            estimate =
                estimate.saturating_add(seconds.saturating_mul(1000).saturating_sub(elapsed));
        }
        if saw_job {
            Ok(Estimate {
                confidence: crate::EstimateConfidence::Estimated,
                start_in_millis: Some(estimate),
                assumptions: vec![
                    "conservative FIFO estimate from declared durations of running and earlier queued jobs; orthogonal work may start sooner".into(),
                ],
            })
        } else {
            Ok(Estimate::unknown(
                "blocked work has no sufficient declared running duration",
            ))
        }
    }

    pub(super) fn settled_granted_claims(&self) -> StoreResult<Vec<ResolvedClaims>> {
        let mut statement = self.connection.prepare(
            "SELECT leases.claims_json FROM leases
             JOIN attempts ON attempts.id = leases.attempt_id
             WHERE leases.state = 'granted' AND attempts.state = 'settled'",
        )?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
    }

    #[cfg(test)]
    pub(crate) fn prepare_next_job(&mut self) -> StoreResult<Option<PreparedJob>> {
        Ok(self.prepare_next_job_with_progress()?.job)
    }

    #[cfg(test)]
    pub(crate) fn prepare_next_job_with_progress(&mut self) -> StoreResult<PrepareNext> {
        self.prepare_next_job_with_observation(None)
    }

    #[cfg(test)]
    pub(crate) fn prepare_next_job_with_observation(
        &mut self,
        observation: Option<crate::host_observation::ObservationMoment<'_>>,
    ) -> StoreResult<PrepareNext> {
        self.prepare_next_job_with_observation_source(|| Ok(observation))
    }

    pub(crate) fn prepare_next_job_with_sample(
        &mut self,
        sample: Option<&crate::host_observation::HostSample>,
    ) -> StoreResult<PrepareNext> {
        self.prepare_next_job_with_observation_source(|| {
            let Some(sample) = sample else {
                return Ok(None);
            };
            let (now_unix_millis, now_monotonic_millis) =
                crate::host_observation::observation_clock()?;
            Ok(Some(crate::host_observation::ObservationMoment {
                sample,
                now_unix_millis,
                now_monotonic_millis,
            }))
        })
    }

    fn prepare_next_job_with_observation_source<'a>(
        &mut self,
        mut observation: impl FnMut() -> StoreResult<
            Option<crate::host_observation::ObservationMoment<'a>>,
        >,
    ) -> StoreResult<PrepareNext> {
        let mut state_changed = false;
        loop {
            let mut skipped_in_pass = false;
            for job_id in self.pending_jobs()? {
                match self.prepare_job_inner(job_id, observation()?)? {
                    PrepareJob::Ready(job) => {
                        return Ok(PrepareNext {
                            job: Some(*job),
                            state_changed,
                        });
                    }
                    PrepareJob::Blocked => {}
                    PrepareJob::StateChanged => {
                        skipped_in_pass = true;
                        state_changed = true;
                    }
                }
            }
            if !skipped_in_pass {
                return Ok(PrepareNext {
                    job: None,
                    state_changed,
                });
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn prepare_job(&mut self, job_id: JobId) -> StoreResult<Option<PreparedJob>> {
        Ok(match self.prepare_job_inner(job_id, None)? {
            PrepareJob::Ready(job) => Some(*job),
            PrepareJob::Blocked | PrepareJob::StateChanged => None,
        })
    }

    pub(super) fn prepare_job_inner(
        &mut self,
        job_id: JobId,
        observation: Option<crate::host_observation::ObservationMoment<'_>>,
    ) -> StoreResult<PrepareJob> {
        let job_key = self.local_id(job_id)?;
        if !self.startup_identity.capable() {
            return Ok(PrepareJob::Blocked);
        }
        let observed_job = self
            .connection
            .query_row(
                "SELECT spec_json FROM jobs WHERE id = ?1 AND state = 'pending'",
                [&job_key],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|json| serde_json::from_str::<JobSpec>(&json))
            .transpose()?
            .is_some_and(|spec| spec.requires_host_observation());
        if observed_job {
            return self.advance_observed_admission(job_id, observation);
        }
        let capacities = self.capacities.clone();
        let impact_incompatibilities = self.impact_incompatibilities.clone();
        let host_config = self.host_config();
        let attempt_id = AttemptId::new(self.store_uuid);
        let invocation_id = InvocationId::new(self.store_uuid);
        let containment_id = ContainmentId::new(self.store_uuid);
        let lease_id = Uuid::now_v7();
        let transaction = self.connection.transaction()?;
        let row = transaction
            .query_row(
                "SELECT spec_json, claims_json, stdin_hash, stdin_len
                 FROM jobs WHERE id = ?1 AND state = 'pending'
                   AND COALESCE(retry_not_before_ms, 0) <= ?2",
                params![job_key, now_millis()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<u64>>(3)?,
                    ))
                },
            )
            .optional()?;
        let Some((spec_json, claims_json, stdin_hash, stdin_len)) = row else {
            transaction.rollback()?;
            return Ok(PrepareJob::Blocked);
        };
        let (dependency_blockers, impossible) = dependency_blockers_tx(&transaction, job_id)?;
        if impossible {
            transaction.execute(
                "UPDATE jobs SET state = 'final', outcome = 'skipped', finished_ms = ?2
                 WHERE id = ?1 AND state = 'pending'",
                params![job_id.entity_uuid().to_string(), now_millis()],
            )?;
            transaction.commit()?;
            return Ok(PrepareJob::StateChanged);
        }
        if !dependency_blockers.is_empty() {
            transaction.rollback()?;
            return Ok(PrepareJob::Blocked);
        }
        let claims: ResolvedClaims = serde_json::from_str(&claims_json)?;
        let spec: JobSpec = serde_json::from_str(&spec_json)?;
        let active = active_claims_tx(&transaction)?;
        if !claims
            .blockers(&capacities, &active, &impact_incompatibilities)
            .is_empty()
        {
            transaction.rollback()?;
            return Ok(PrepareJob::Blocked);
        }
        if spec.requires_host_observation() {
            let Some(observation) = observation else {
                transaction.rollback()?;
                return Ok(PrepareJob::Blocked);
            };
            let context = crate::host_observation::evaluate_admission(
                &spec,
                &host_config,
                observation.sample,
                &active,
                observation.now_unix_millis,
                observation.now_monotonic_millis,
            );
            if !context.blockers.is_empty() || spec.quiet.is_some() {
                transaction.rollback()?;
                return Ok(PrepareJob::Blocked);
            }
        }
        let stdin = match (stdin_hash, stdin_len) {
            (Some(sha256), Some(length)) => Some(StagedInputRef { sha256, length }),
            (None, None) => None,
            _ => {
                return Err(StoreError::InvalidState(
                    "job has a partial staged stdin reference".into(),
                ));
            }
        };
        validate_input_shape(&spec, stdin.as_ref())?;
        let log_directory = self.paths.logs.join(job_id.entity_uuid().to_string());
        std::fs::create_dir_all(&log_directory)?;
        let attempt_index: u32 = transaction.query_row(
            "SELECT COALESCE(MAX(attempt_index), 0) + 1 FROM attempts WHERE job_id = ?1",
            [job_id.entity_uuid().to_string()],
            |row| row.get(0),
        )?;
        transaction.execute(
            "UPDATE jobs SET state = 'active', attempt_id = ?2, invocation_id = ?3,
                containment_id = ?4, stdout_len = 0, stderr_len = 0,
                retry_not_before_ms = NULL WHERE id = ?1 AND state = 'pending'",
            params![
                job_id.entity_uuid().to_string(),
                attempt_id.entity_uuid().to_string(),
                invocation_id.entity_uuid().to_string(),
                containment_id.entity_uuid().to_string(),
            ],
        )?;
        let attempt_started = now_millis();
        let attempt_deadline = spec.timeout_seconds.map(|seconds| {
            attempt_started
                .saturating_add(i64::try_from(seconds.saturating_mul(1000)).unwrap_or(i64::MAX))
        });
        transaction.execute(
            "INSERT INTO attempts(
                id, job_id, state, attempt_index, created_ms, started_ms, deadline_ms
             ) VALUES (?1, ?2, 'starting', ?3, ?4, ?4, ?5)",
            params![
                attempt_id.entity_uuid().to_string(),
                job_id.entity_uuid().to_string(),
                attempt_index,
                attempt_started,
                attempt_deadline,
            ],
        )?;
        transaction.execute(
            "INSERT INTO invocations(id, attempt_id, role, state)
             VALUES (?1, ?2, 'primary', 'prepared')",
            params![
                invocation_id.entity_uuid().to_string(),
                attempt_id.entity_uuid().to_string()
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
            params![
                lease_id.to_string(),
                attempt_id.entity_uuid().to_string(),
                claims_json,
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
            attempt_deadline_unix_millis: attempt_deadline,
            host_id: self.startup_identity.host_id.clone(),
            boot_id: self.startup_identity.boot_id.clone(),
            primary_result: None,
        })))
    }

    pub(crate) fn prepare_postcondition(
        &mut self,
        primary: &PreparedJob,
        index: usize,
    ) -> StoreResult<PreparedJob> {
        let postcondition =
            primary.spec.postconditions.get(index).ok_or_else(|| {
                StoreError::InvalidState("postcondition index out of range".into())
            })?;
        let invocation_id = InvocationId::new(self.store_uuid);
        let containment_id = ContainmentId::new(self.store_uuid);
        let transaction = self.connection.transaction()?;
        let current: (String, String, Option<i64>, Option<String>) = transaction.query_row(
            "SELECT jobs.state, jobs.attempt_id, attempts.deadline_ms,
                    attempts.primary_result_json
             FROM jobs JOIN attempts ON attempts.id = jobs.attempt_id WHERE jobs.id = ?1",
            [primary.job_id.entity_uuid().to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        if current.0 != "active" || current.1 != primary.attempt_id.entity_uuid().to_string() {
            return Err(StoreError::InvalidState(
                "postcondition requires the current active Attempt".into(),
            ));
        }
        let primary_result: PrimaryInvocationResult = current
            .3
            .as_deref()
            .ok_or_else(|| {
                StoreError::InvalidState(
                    "postcondition requires a durable primary Invocation result".into(),
                )
            })
            .and_then(|value| serde_json::from_str(value).map_err(Into::into))?;
        if primary_result.schema_version != 1
            || primary_result.job_id != primary.job_id
            || primary_result.attempt_id != primary.attempt_id
            || primary_result.invocation_id != primary.invocation_id
            || primary_result.containment != ContainmentState::Empty
        {
            return Err(StoreError::InvalidState(
                "postcondition requires a matching versioned primary result with empty Containment proof"
                .into(),
            ));
        }
        super::lifecycle::validate_primary_result_semantics(
            primary_result.verdict,
            primary_result.termination,
            primary_result.root_exit_code,
        )?;
        let primary_containment: String = transaction.query_row(
            "SELECT containments.state FROM containments
             JOIN invocations ON invocations.id = containments.invocation_id
             WHERE invocations.id = ?1 AND invocations.attempt_id = ?2
               AND invocations.role = 'primary'",
            params![
                primary.invocation_id.entity_uuid().to_string(),
                primary.attempt_id.entity_uuid().to_string(),
            ],
            |row| row.get(0),
        )?;
        let lease_granted: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM leases WHERE attempt_id = ?1 AND state = 'granted')",
            [primary.attempt_id.entity_uuid().to_string()],
            |row| row.get(0),
        )?;
        if primary_containment != "empty" || !lease_granted {
            return Err(StoreError::InvalidState(
                "postcondition release requires empty primary Containment and the same granted work Lease".into(),
            ));
        }
        let role_index: u32 = transaction.query_row(
            "SELECT COALESCE(MAX(role_index), -1) + 1
             FROM invocations WHERE attempt_id = ?1",
            [primary.attempt_id.entity_uuid().to_string()],
            |row| row.get(0),
        )?;
        let postcondition_index = u32::try_from(index)
            .map_err(|_| StoreError::InvalidState("too many postconditions".into()))?;
        transaction.execute(
            "INSERT INTO invocations(
                id, attempt_id, role, role_index, postcondition_index, state
             ) VALUES (?1, ?2, 'postcondition', ?3, ?4, 'prepared')",
            params![
                invocation_id.entity_uuid().to_string(),
                primary.attempt_id.entity_uuid().to_string(),
                role_index,
                postcondition_index,
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
            "UPDATE jobs SET invocation_id = ?2, containment_id = ?3 WHERE id = ?1",
            params![
                primary.job_id.entity_uuid().to_string(),
                invocation_id.entity_uuid().to_string(),
                containment_id.entity_uuid().to_string(),
            ],
        )?;
        transaction.commit()?;
        let mut spec = primary.spec.clone();
        spec.executable = postcondition.executable.clone();
        spec.args = postcondition.args.clone();
        if let Some(working_directory) = &postcondition.working_directory {
            spec.working_directory = working_directory.clone();
        }
        spec.stdin = StdinSpec::Eof;
        spec.observed = None;
        spec.quiet = None;
        spec.postconditions.clear();
        spec.child_submission_policy = None;
        let log_directory = self
            .paths
            .logs
            .join(primary.job_id.entity_uuid().to_string());
        Ok(PreparedJob {
            job_id: primary.job_id,
            attempt_id: primary.attempt_id,
            invocation_id,
            containment_id,
            spec,
            stdout_path: log_directory.join(format!("{invocation_id}.stdout")),
            stderr_path: log_directory.join(format!("{invocation_id}.stderr")),
            stdin: None,
            stdin_path: None,
            role: InvocationRole::Postcondition,
            attempt_deadline_unix_millis: current.2,
            host_id: self.startup_identity.host_id.clone(),
            boot_id: self.startup_identity.boot_id.clone(),
            primary_result: Some(primary_result),
        })
    }

    pub(crate) fn pending_jobs(&self) -> StoreResult<Vec<JobId>> {
        let mut statement = self
            .connection
            .prepare("SELECT id FROM jobs WHERE state = 'pending' ORDER BY accepted_ms, rowid")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        let mut jobs = Vec::new();
        for row in rows {
            jobs.push(JobId::from_parts(self.store_uuid, Uuid::parse_str(&row?)?));
        }
        Ok(jobs)
    }

    pub(crate) fn host_observation_demand(&self) -> StoreResult<bool> {
        let mut statement = self.connection.prepare(
            "SELECT jobs.state, jobs.spec_json FROM jobs
             LEFT JOIN attempts ON attempts.id = jobs.attempt_id
             WHERE (jobs.state = 'pending'
                    AND COALESCE(jobs.retry_not_before_ms, 0) <= ?1)
                OR (jobs.state = 'active' AND attempts.state = 'starting')",
        )?;
        let rows = statement.query_map([now_millis()], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        for row in rows {
            let (state, spec_json) = row?;
            let spec: JobSpec = serde_json::from_str(&spec_json)?;
            if (state == "pending" && spec.requires_host_observation())
                || (state == "active" && spec.quiet.is_some())
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(crate) fn host_observation_requirements(
        &self,
    ) -> StoreResult<crate::host_observation::HostObservationRequirements> {
        let mut required = crate::host_observation::HostObservationRequirements {
            memory: self.capacities.ram_mb > 0,
            gpu: self.capacities.gpu_slots > 0
                || self
                    .capacities
                    .custom
                    .iter()
                    .any(|(name, capacity)| name.starts_with("vram_mb:") && *capacity > 0),
            gpu_uuid: self
                .observation_config
                .gpu_slot_uuid
                .as_deref()
                .and_then(|uuid| crate::spec::canonical_gpu_uuid(uuid).ok()),
            ..Default::default()
        };
        let mut statement = self
            .connection
            .prepare("SELECT spec_json FROM jobs WHERE state IN ('pending', 'active')")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        for row in rows {
            let spec: JobSpec = serde_json::from_str(&row?)?;
            required.memory |= spec.resources.ram_mb.unwrap_or(0) > 0;
            required.gpu |= spec.resources.gpu_slots.unwrap_or(0) > 0
                || spec
                    .resources
                    .custom
                    .keys()
                    .any(|name| name.starts_with("vram_mb:"));
            if let Some(observed) = &spec.observed {
                required.cpu |= observed.cpu_utilization_percent_at_most.is_some();
                required.gpu |= !observed.gpu_utilization_percent_at_most.is_empty();
            }
            if let Some(quiet) = &spec.quiet {
                for detector in &quiet.detectors {
                    match detector {
                        crate::QuietDetector::CpuUtilization { .. } => required.cpu = true,
                        crate::QuietDetector::DiskUtilization { .. } => required.disk = true,
                        crate::QuietDetector::BlockedProcesses => required.processes = true,
                        crate::QuietDetector::GpuUtilization { .. } => required.gpu = true,
                        crate::QuietDetector::ForeignGpuCompute { .. } => {
                            required.gpu = true;
                            required.processes = true;
                        }
                    }
                }
            }
        }
        Ok(required)
    }

    pub(crate) fn next_retry_delay(
        &self,
        scheduling_pass_started: i64,
    ) -> StoreResult<Option<std::time::Duration>> {
        let now = now_millis();
        let next: Option<i64> = self.connection.query_row(
            "SELECT MIN(retry_not_before_ms) FROM jobs
             WHERE state = 'pending' AND retry_not_before_ms > ?1",
            [scheduling_pass_started],
            |row| row.get(0),
        )?;
        Ok(next.map(|instant| {
            std::time::Duration::from_millis(
                u64::try_from(instant.saturating_sub(now)).unwrap_or(0),
            )
        }))
    }
}

#[derive(Default, serde::Deserialize)]
struct AdmissionEvidenceRecord {
    #[serde(default)]
    evaluated_unix_millis: Option<i64>,
    #[serde(default)]
    observation_generation: Option<Uuid>,
    #[serde(default)]
    blockers: Vec<Blocker>,
    #[serde(default)]
    operands: Vec<crate::ObservedOperandSnapshot>,
    #[serde(default)]
    detectors: Vec<crate::DetectorEvidenceSnapshot>,
}
