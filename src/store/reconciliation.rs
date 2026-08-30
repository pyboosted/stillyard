use super::*;

#[derive(Clone, Default)]
pub(crate) struct ReconciliationObservations {
    entries: std::collections::BTreeMap<ContainmentId, (i64, ReconciliationResult)>,
}

impl ReconciliationObservations {
    pub(crate) fn record(&mut self, containment_id: ContainmentId, result: ReconciliationResult) {
        const MAX_OBSERVATIONS: usize = 256;
        if !self.entries.contains_key(&containment_id) && self.entries.len() >= MAX_OBSERVATIONS {
            if let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, (observed, _))| *observed)
                .map(|(containment_id, _)| *containment_id)
            {
                self.entries.remove(&oldest);
            }
        }
        self.entries.insert(containment_id, (now_millis(), result));
    }

    pub(super) fn get(
        &self,
        containment_id: &ContainmentId,
    ) -> Option<&(i64, ReconciliationResult)> {
        self.entries.get(containment_id)
    }
}

impl Store {
    pub(crate) fn reconciliation_context(&self) -> Option<(HostId, BootId, Uuid)> {
        Some((
            self.startup_identity.host_id.clone()?,
            self.startup_identity.boot_id.clone()?,
            self.daemon_generation,
        ))
    }

    pub(crate) fn reconciliation_candidates(
        &self,
        after_sequence: u64,
        limit: u32,
    ) -> StoreResult<Vec<ReconciliationCandidate>> {
        let mut statement = self.connection.prepare(
            "SELECT containments.id, containments.invocation_id, invocations.attempt_id,
                    containments.version, containments.host_id, containments.boot_id,
                    containments.daemon_generation, invocations.root_pid,
                    invocations.root_host_id, invocations.root_boot_id,
                    invocations.root_creation_filetime_100ns,
                    daemon_generations.process_identity_json, containments.incident_sequence
             FROM containments
             JOIN invocations ON invocations.id = containments.invocation_id
             LEFT JOIN daemon_generations
               ON daemon_generations.generation = containments.daemon_generation
             WHERE containments.state = 'uncertain'
             ORDER BY (containments.incident_sequence > ?1) DESC,
                      containments.incident_sequence, containments.id
             LIMIT ?2",
        )?;
        let rows = statement.query_map(params![after_sequence, limit.min(32)], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, u64>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, Option<u32>>(7)?,
                row.get::<_, Option<String>>(8)?,
                row.get::<_, Option<String>>(9)?,
                row.get::<_, Option<i64>>(10)?,
                row.get::<_, Option<String>>(11)?,
                row.get::<_, u64>(12)?,
            ))
        })?;
        let mut candidates = Vec::new();
        for row in rows {
            let (
                containment,
                invocation,
                attempt,
                version,
                host,
                boot,
                generation,
                root_pid,
                root_host,
                root_boot,
                root_creation,
                prior_daemon,
                incident_sequence,
            ) = row?;
            candidates.push(ReconciliationCandidate {
                containment_id: ContainmentId::from_parts(
                    self.store_uuid,
                    Uuid::parse_str(&containment)?,
                ),
                invocation_id: InvocationId::from_parts(
                    self.store_uuid,
                    Uuid::parse_str(&invocation)?,
                ),
                attempt_id: AttemptId::from_parts(self.store_uuid, Uuid::parse_str(&attempt)?),
                version,
                host_id: host.map(HostId),
                boot_id: boot.map(BootId),
                daemon_generation: generation
                    .map(|value| Uuid::parse_str(&value))
                    .transpose()?,
                root_pid_recorded: root_pid.is_some(),
                root_identity: process_identity_from_columns(
                    root_pid,
                    root_host,
                    root_boot,
                    root_creation,
                )?,
                prior_daemon_identity: prior_daemon
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()?,
                incident_sequence,
            });
        }
        Ok(candidates)
    }

    pub(crate) fn reconciliation_candidate(
        &self,
        containment_id: ContainmentId,
    ) -> StoreResult<ReconciliationCandidate> {
        let local_id = self.local_containment_id(containment_id)?;
        let row = self
            .connection
            .query_row(
                "SELECT containments.invocation_id, invocations.attempt_id,
                        containments.version, containments.host_id, containments.boot_id,
                        containments.daemon_generation, invocations.root_pid,
                        invocations.root_host_id, invocations.root_boot_id,
                        invocations.root_creation_filetime_100ns,
                        daemon_generations.process_identity_json,
                        containments.incident_sequence, containments.state
                 FROM containments
                 JOIN invocations ON invocations.id = containments.invocation_id
                 LEFT JOIN daemon_generations
                   ON daemon_generations.generation = containments.daemon_generation
                 WHERE containments.id = ?1",
                [&local_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, u64>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, Option<u32>>(6)?,
                        row.get::<_, Option<String>>(7)?,
                        row.get::<_, Option<String>>(8)?,
                        row.get::<_, Option<i64>>(9)?,
                        row.get::<_, Option<String>>(10)?,
                        row.get::<_, Option<u64>>(11)?,
                        row.get::<_, String>(12)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| StoreError::OperationRejected {
                code: "containment_not_uncertain".into(),
                detail: "containment does not exist in the current store".into(),
            })?;
        let (
            invocation,
            attempt,
            version,
            host,
            boot,
            generation,
            root_pid,
            root_host,
            root_boot,
            root_creation,
            prior_daemon,
            incident_sequence,
            state,
        ) = row;
        if state == "cleared" {
            return Err(StoreError::OperationRejected {
                code: "containment_already_cleared".into(),
                detail: "containment is already cleared".into(),
            });
        }
        if state != "uncertain" {
            return Err(StoreError::OperationRejected {
                code: "containment_not_uncertain".into(),
                detail: format!("containment state is {state}, not uncertain"),
            });
        }
        Ok(ReconciliationCandidate {
            containment_id,
            invocation_id: InvocationId::from_parts(self.store_uuid, Uuid::parse_str(&invocation)?),
            attempt_id: AttemptId::from_parts(self.store_uuid, Uuid::parse_str(&attempt)?),
            version,
            host_id: host.map(HostId),
            boot_id: boot.map(BootId),
            daemon_generation: generation
                .map(|value| Uuid::parse_str(&value))
                .transpose()?,
            root_pid_recorded: root_pid.is_some(),
            root_identity: process_identity_from_columns(
                root_pid,
                root_host,
                root_boot,
                root_creation,
            )?,
            prior_daemon_identity: prior_daemon
                .as_deref()
                .map(serde_json::from_str)
                .transpose()?,
            incident_sequence: incident_sequence.unwrap_or(0),
        })
    }

    pub(crate) fn clearance_authorization_evidence(
        &self,
    ) -> StoreResult<(Vec<InvocationId>, Vec<ProcessIdentity>)> {
        let generation = self.daemon_generation.to_string();
        let mut handles = self.connection.prepare(
            "SELECT invocations.id FROM containments
             JOIN invocations ON invocations.id = containments.invocation_id
             WHERE containments.daemon_generation = ?1
               AND containments.state IN ('creating', 'live', 'uncertain')
             ORDER BY invocations.id",
        )?;
        let invocation_rows = handles.query_map([generation], |row| row.get::<_, String>(0))?;
        let invocations = invocation_rows
            .map(|row| {
                Ok(InvocationId::from_parts(
                    self.store_uuid,
                    Uuid::parse_str(&row?)?,
                ))
            })
            .collect::<StoreResult<Vec<_>>>()?;
        let mut roots = self.connection.prepare(
            "SELECT invocations.root_pid, invocations.root_host_id,
                    invocations.root_boot_id, invocations.root_creation_filetime_100ns
             FROM containments
             JOIN invocations ON invocations.id = containments.invocation_id
             WHERE containments.state = 'uncertain'",
        )?;
        let root_rows = roots.query_map([], |row| {
            Ok((
                row.get::<_, Option<u32>>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<i64>>(3)?,
            ))
        })?;
        let mut identities = Vec::new();
        for row in root_rows {
            let (pid, host, boot, creation) = row?;
            if let Some(identity) = process_identity_from_columns(pid, host, boot, creation)? {
                identities.push(identity);
            }
        }
        Ok((invocations, identities))
    }

    pub(crate) fn latest_unresolved_incident_sequence(&self) -> StoreResult<Option<u64>> {
        self.connection
            .query_row(
                "SELECT MAX(incident_sequence) FROM containments WHERE state = 'uncertain'",
                [],
                |row| row.get(0),
            )
            .map_err(Into::into)
    }

    pub(crate) fn commit_containment_resolution(
        &mut self,
        candidate: &ReconciliationCandidate,
        resolution: ContainmentResolution,
        last_reconciliation: ReconciliationResult,
        origin: ClearanceOrigin,
        forced: Option<ForcedClearanceAudit>,
        expected_authorization_invocations: Option<&[InvocationId]>,
    ) -> StoreResult<Option<ClearContainmentResult>> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        type ContainmentVersionRow = (String, u64, Option<String>, Option<String>, Option<String>);
        let current: Option<ContainmentVersionRow> = transaction
            .query_row(
                "SELECT state, version, host_id, boot_id, daemon_generation
                     FROM containments WHERE id = ?1",
                [candidate.containment_id.entity_uuid().to_string()],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()?;
        let Some((state, version, host, boot, generation)) = current else {
            return Ok(None);
        };
        if state != "uncertain"
            || version != candidate.version
            || host.as_deref() != candidate.host_id.as_ref().map(|value| value.0.as_str())
            || boot.as_deref() != candidate.boot_id.as_ref().map(|value| value.0.as_str())
            || generation.as_deref()
                != candidate
                    .daemon_generation
                    .as_ref()
                    .map(|value| value.to_string())
                    .as_deref()
        {
            return Ok(None);
        }
        if let Some(expected) = expected_authorization_invocations {
            let mut statement = transaction.prepare(
                "SELECT invocations.id FROM containments
                 JOIN invocations ON invocations.id = containments.invocation_id
                 WHERE containments.daemon_generation = ?1
                   AND containments.state IN ('creating', 'live', 'uncertain')
                 ORDER BY invocations.id",
            )?;
            let observed = statement
                .query_map([self.daemon_generation.to_string()], |row| {
                    row.get::<_, String>(0)
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            drop(statement);
            let expected = expected
                .iter()
                .map(|invocation| invocation.entity_uuid().to_string())
                .collect::<Vec<_>>();
            if observed != expected {
                return Ok(None);
            }
        }
        let attempt_id = candidate.attempt_id.entity_uuid().to_string();
        let target_id = candidate.containment_id.entity_uuid().to_string();
        let lease_released =
            attempt_lease_release_eligible_after_target(&transaction, &attempt_id, &target_id)?;
        let audit = ContainmentResolutionAudit {
            resolved_unix_millis: now_millis(),
            daemon_generation: self.daemon_generation,
            resolution: resolution.clone(),
            last_reconciliation: last_reconciliation.clone(),
            origin,
            forced,
            lease_released,
        };
        transaction.execute(
            "UPDATE containments SET state = 'cleared', version = version + 1,
                resolution = ?2, resolved_ms = ?3, last_reconciliation = ?4,
                resolution_audit_json = ?5
             WHERE id = ?1 AND state = 'uncertain' AND version = ?6",
            params![
                target_id,
                containment_resolution_string(&resolution)?,
                audit.resolved_unix_millis,
                reconciliation_result_string(&last_reconciliation)?,
                serde_json::to_string(&audit)?,
                candidate.version,
            ],
        )?;
        if lease_released {
            transaction.execute(
                "UPDATE leases SET state = 'released'
                 WHERE attempt_id = ?1 AND state = 'granted'",
                [&attempt_id],
            )?;
        }
        transaction.commit()?;
        Ok(Some(ClearContainmentResult {
            schema_version: 1,
            containment_id: candidate.containment_id,
            prior_state: ContainmentState::Uncertain,
            state: ContainmentState::Cleared,
            audit,
        }))
    }

    pub(crate) fn persisted_clearance(
        &self,
        containment_id: ContainmentId,
    ) -> StoreResult<Option<ClearContainmentResult>> {
        let local_id = self.local_containment_id(containment_id)?;
        self.connection
            .query_row(
                "SELECT state, resolution_audit_json FROM containments WHERE id = ?1",
                [local_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
            )
            .optional()?
            .map(|(state, audit)| {
                if state != "cleared" {
                    return Ok(None);
                }
                let audit = audit.ok_or_else(|| {
                    StoreError::InvalidState("cleared containment has no resolution audit".into())
                })?;
                Ok(Some(ClearContainmentResult {
                    schema_version: 1,
                    containment_id,
                    prior_state: ContainmentState::Uncertain,
                    state: ContainmentState::Cleared,
                    audit: serde_json::from_str(&audit)?,
                }))
            })
            .transpose()
            .map(Option::flatten)
    }
}
