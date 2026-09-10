//! Reconstruct coordinator obligations from the reset-independent journal.
//! This never reconstructs lost native Jobs or guesses that a manager is empty.
use super::*;

impl Store {
    /// Existing receipt replay remains available, but new submissions cannot
    /// keep replenishing the native queue while same-store repair drains it.
    pub(super) fn check_new_submission_during_repair(&self) -> StoreResult<()> {
        if self.authority.is_some()
            && self
                .authority_snapshot()?
                .coordinator
                .is_some_and(|history| {
                    history.store_uuid == self.store_uuid && history.pending_reset.is_some()
                })
        {
            return Err(StoreError::OperationRejected {
                code: "authority_repair_pending".into(),
                detail: "same-store history repair temporarily refuses new submissions; existing receipt recovery and cancellation remain available".into(),
            });
        }
        Ok(())
    }

    // A same-UUID rollback can resurrect previously completed native Jobs as
    // pending. Never reopen admission until the owner has canceled every
    // retained unfinished Job and all native SQL boundaries have resolved.
    fn rollback_native_history_quiescent(&self) -> StoreResult<bool> {
        Ok(self.connection.query_row("SELECT NOT EXISTS(SELECT 1 FROM jobs WHERE state!='final') AND NOT EXISTS(SELECT 1 FROM leases WHERE state='granted') AND NOT EXISTS(SELECT 1 FROM containments WHERE state NOT IN ('empty','cleared'))",[],|r|r.get(0))?)
    }
    pub(crate) fn prepare_machine_reset_recovery(
        &mut self,
    ) -> StoreResult<crate::AuthoritySnapshot> {
        self.reconcile_pending_domain_retirement()?;
        let snapshot = self.authority_snapshot()?;
        let Some(history) = &snapshot.coordinator else {
            return Ok(snapshot);
        };
        let Some(gate) = &history.pending_reset else {
            return Ok(snapshot);
        };
        // Only covered native history can be repaired. For same-UUID rollback,
        // preserve every native row and require explicit normal cancellation of
        // unfinished Jobs before rebuilding the machine extension.
        if snapshot.native_coverage_store != Some(history.store_uuid)
            || (history.store_uuid == self.store_uuid
                && !self.rollback_native_history_quiescent()?)
        {
            return Ok(snapshot);
        }
        let marker = gate.reset_id.to_string();
        let installed: Option<String> = self
            .connection
            .query_row(
                "SELECT value FROM machine_meta WHERE key='reset_inventory'",
                [],
                |r| r.get(0),
            )
            .optional()?;
        let journal = self
            .authority
            .clone()
            .ok_or_else(|| StoreError::InvalidState("authority missing".into()))?;
        let (anchors, pending) = {
            let a = journal
                .lock()
                .map_err(|_| StoreError::InvalidState("authority mutex poisoned".into()))?;
            (a.participants()?, a.pending_machine_commit()?)
        };
        if installed.as_deref() == Some(&marker)
            && pending.is_none()
            && snapshot.pending_machine_operation.is_none()
        {
            return Ok(snapshot);
        }
        if anchors.iter().any(|a| !a.committed) {
            return Ok(snapshot);
        }
        // No starts can pass the durable global gate. Rebuild only the machine
        // extension in this replacement store, preserving all native Job rows.
        let topology = self.machine_topology()?;
        let retirement = self.authority_lock()?.pending_domain_retirement()?;
        let mut grants = snapshot
            .machine_obligations
            .iter()
            .cloned()
            .map(|g| (g.grant_id, g))
            .collect::<std::collections::BTreeMap<_, _>>();
        if let Some(intent) = &pending {
            for grant in &intent.grants {
                grants.insert(grant.grant_id, grant.clone());
            }
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute_batch("DELETE FROM machine_ticket_identities; DELETE FROM machine_readiness;
            DELETE FROM machine_reservations; DELETE FROM machine_grants;
            DELETE FROM machine_candidates; DELETE FROM machine_reconciles;
            DELETE FROM machine_operations; DELETE FROM machine_challenges;
            DELETE FROM machine_domains; DELETE FROM machine_queue WHERE owner NOT LIKE 'native:%';")?;
        if let Some(audit) = &retirement {
            // This projection was found divergent. The full immutable external
            // audit remains authoritative; preserve every unrelated SQL receipt.
            tx.execute(
                "DELETE FROM machine_domain_retirements WHERE domain_id=?1",
                [audit.request.domain_id.0.to_string()],
            )?;
        }
        for anchor in &anchors {
            let r = &anchor.registration;
            let accepted = anchor.accepted_sequence.max(
                pending
                    .as_ref()
                    .filter(|p| p.request.session.domain_id == r.installation.domain_id)
                    .map_or(0, |p| p.request.request_sequence),
            );
            // Missing response history is retired explicitly, never replayed as
            // new work. The one pending externally journaled response is retained.
            let session = anchor.session.as_ref();
            tx.execute("INSERT INTO machine_domains(domain_id,registration_sha256,installation_json,manager_store_uuid,connection_epoch,executor_incarnation,reconciliation_required,retired_sequence_floor,accepted_sequence) VALUES (?1,?2,?3,?4,?5,?6,1,?7,?7)",params![r.installation.domain_id.0.to_string(),crate::machine::payload_hash(r)?,serde_json::to_string(&r.installation)?,r.manager_store_uuid.to_string(),session.map_or(0,|s|s.connection_epoch),session.map(|s|s.executor_incarnation.to_string()),accepted])?;
            let mut a = journal
                .lock()
                .map_err(|_| StoreError::InvalidState("authority mutex poisoned".into()))?;
            a.checkpoint_machine_sequence(r.installation.domain_id, accepted)?;
            a.retire_machine_sequences(r.installation.domain_id, accepted)?;
        }
        let mut ordered = grants.values().collect::<Vec<_>>();
        ordered.sort_by_key(|g| (g.queue_sequence, g.grant_id));
        for grant in ordered {
            let c = &grant.candidate;
            let key = super::machine_allocation::key_string(&c.key)?;
            let job = match c.owner {
                crate::machine::AllocationOwner::Work { job_id, .. }
                | crate::machine::AllocationOwner::Probe { job_id, .. } => job_id,
            };
            let owner = format!("remote:{job}");
            tx.execute(
                "INSERT OR IGNORE INTO machine_queue(owner,accepted_ms,priority) VALUES (?1,?2,?3)",
                params![owner, grant.queue_accepted_unix_millis, c.priority],
            )?;
            let (accepted, priority): (i64, i8) = tx.query_row(
                "SELECT accepted_ms,priority FROM machine_queue WHERE owner=?1",
                [&owner],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            if accepted != grant.queue_accepted_unix_millis || priority != c.priority {
                return Err(StoreError::InvalidState(
                    "recovered same-Job queue anchors conflict".into(),
                ));
            }
            tx.execute("UPDATE machine_meta SET value=MAX(CAST(value AS INTEGER),?1) WHERE key='schedule_ms'",[grant.offered_unix_millis.max(grant.queue_accepted_unix_millis)])?;
            tx.execute("INSERT INTO machine_candidates(allocation_key,domain_id,candidate_json,queue_owner,state,expires_ms,revision) VALUES (?1,?2,?3,?4,?5,?6,?7)",params![key,c.key.domain_id.0.to_string(),serde_json::to_string(c)?,owner,if grant.state == crate::machine::GrantState::Released {"released"} else {"withdrawn"},i64::MAX,c.revision])?;
            let (domains, topology) = topology.as_ref().ok_or_else(|| {
                StoreError::InvalidState("recovered Grant has no registered topology".into())
            })?;
            let expanded = topology
                .expand(c.key.domain_id, &super::machine_queue::claims(&c.claims))
                .map_err(StoreError::InvalidState)?;
            let physical = super::machine_queue::physical_claims(&expanded, domains.machine_scope);
            super::machine_queue::save_grant(&tx, grant, Some(&physical))?;
        }
        if let Some(intent) = &pending {
            let r = &intent.request;
            tx.execute(
                "INSERT INTO machine_operations VALUES (?1,?2,?3,?4,?5)",
                params![
                    r.session.domain_id.0.to_string(),
                    r.request_sequence,
                    r.operation_id.to_string(),
                    r.payload_sha256,
                    serde_json::to_string(&intent.outcome)?
                ],
            )?;
        }
        tx.execute(
            "INSERT OR REPLACE INTO machine_meta VALUES ('reset_inventory',?1)",
            [marker],
        )?;
        tx.commit()?;
        if let Some(intent) = pending {
            journal
                .lock()
                .map_err(|_| StoreError::InvalidState("authority mutex poisoned".into()))?
                .finish_machine_commit(intent.request.operation_id)?;
        }
        self.reconcile_pending_domain_retirement()?;
        self.authority_snapshot()
    }

    /// OS proof was collected outside the Store mutex. Recheck the complete
    /// immutable snapshot and SQLite reconciliation state before opening the gate.
    pub(crate) fn finish_machine_reset_recovery(
        &self,
        expected: &crate::AuthoritySnapshot,
        native_empty: bool,
    ) -> StoreResult<crate::AuthoritySnapshot> {
        if !native_empty
            || expected
                .coordinator
                .as_ref()
                .is_none_or(|h| h.pending_reset.is_none())
        {
            return self.authority_snapshot();
        }
        let covered: bool = self.connection.query_row("SELECT NOT EXISTS(SELECT 1 FROM machine_domains WHERE reconciliation_required!=0) AND NOT EXISTS(SELECT 1 FROM machine_grants WHERE state IN ('offered','armed','uncertain'))",[],|r|r.get(0))?;
        let marker: Option<String> = self
            .connection
            .query_row(
                "SELECT value FROM machine_meta WHERE key='reset_inventory'",
                [],
                |r| r.get(0),
            )
            .optional()?;
        if !covered
            || (expected
                .coordinator
                .as_ref()
                .is_some_and(|h| h.store_uuid == self.store_uuid)
                && !self.rollback_native_history_quiescent()?)
            || marker
                != expected
                    .coordinator
                    .as_ref()
                    .and_then(|h| h.pending_reset.as_ref())
                    .map(|g| g.reset_id.to_string())
            || !expected.machine_obligations.is_empty()
            || expected.pending_machine_operation.is_some()
            || expected.holds.iter().any(|h| !h.released)
        {
            return self.authority_snapshot();
        }
        let process = self
            .startup_identity
            .daemon_process
            .clone()
            .ok_or_else(|| {
                StoreError::InvalidState("native creator identity unavailable".into())
            })?;
        self.authority_lock()?.complete_machine_reset(
            expected,
            self.store_uuid,
            self.daemon_generation,
            process,
        )?;
        self.authority_snapshot()
    }
}
