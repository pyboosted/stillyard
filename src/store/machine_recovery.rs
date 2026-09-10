use super::*;

impl Store {
    /// Finish the already journaled transition before serving another mutation.
    /// An acknowledged permission can never disappear merely with the SQLite DB.
    pub(super) fn reconcile_pending_machine_commit(&mut self) -> StoreResult<()> {
        let Some(journal) = self.authority.clone() else {
            return Ok(());
        };
        let (pending, snapshot) = {
            let authority = journal
                .lock()
                .map_err(|_| StoreError::InvalidState("authority mutex poisoned".into()))?;
            (authority.pending_machine_commit(), authority.snapshot())
        };
        let Ok(Some(intent)) = pending else {
            return Ok(());
        };
        if snapshot.coordinator.as_ref().is_none_or(|history| {
            history.store_uuid != self.store_uuid || history.pending_reset.is_some()
        }) {
            return Ok(());
        }
        let request = &intent.request;
        let domain = request.session.domain_id.0.to_string();
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let row:Option<(String,String,String)>=tx.query_row("SELECT operation_id,payload_sha256,response_json FROM machine_operations WHERE domain_id=?1 AND sequence=?2",params![domain,request.request_sequence],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
        let accepted: Option<u64> = tx
            .query_row(
                "SELECT accepted_sequence FROM machine_domains WHERE domain_id=?1",
                [&domain],
                |r| r.get(0),
            )
            .optional()?;
        let inconsistent = match &row {
            Some((id, hash, outcome)) => {
                id != &request.operation_id.to_string()
                    || hash != &request.payload_sha256
                    || serde_json::from_str::<crate::machine::Outcome>(outcome)? != intent.outcome
                    || accepted != Some(request.request_sequence)
            }
            None => accepted.and_then(|seq| seq.checked_add(1)) != Some(request.request_sequence),
        };
        if inconsistent {
            tx.rollback()?;
            journal
                .lock()
                .map_err(|_| StoreError::InvalidState("authority mutex poisoned".into()))?
                .record_reset("machine commit journal and SQLite operation history disagree")?;
            return Ok(());
        }
        for grant in &intent.grants {
            let key = super::machine_allocation::key_string(&grant.candidate.key)?;
            let exists: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM machine_grants WHERE allocation_key=?1)",
                [&key],
                |r| r.get(0),
            )?;
            if !exists {
                tx.rollback()?;
                journal
                    .lock()
                    .map_err(|_| StoreError::InvalidState("authority mutex poisoned".into()))?
                    .record_reset("machine commit lost its allocation row")?;
                return Ok(());
            }
            if row.is_none() {
                // The permission was already durably prepared while its Offer
                // was valid; admission stayed closed throughout this commit gap.
                super::machine_queue::save_grant(&tx, grant, None)?;
                if grant.state == crate::machine::GrantState::Released {
                    tx.execute("UPDATE machine_candidates SET state='released',reservation_deadline_ms=NULL WHERE allocation_key=?1",[key])?;
                }
            } else {
                let persisted = super::machine_allocation::load_grant(&tx, &grant.candidate.key)?;
                if persisted != *grant {
                    tx.rollback()?;
                    journal
                        .lock()
                        .map_err(|_| StoreError::InvalidState("authority mutex poisoned".into()))?
                        .record_reset("machine commit Grant differs from its durable permission")?;
                    return Ok(());
                }
            }
        }
        if row.is_none() {
            if let crate::machine::Outcome::Acknowledged { through_sequence } = intent.outcome {
                super::machine_allocation::compact_operations(
                    &tx,
                    request.session.domain_id,
                    through_sequence,
                )?;
            }
            if matches!(intent.outcome, crate::machine::Outcome::Reconciled { .. }) {
                tx.execute(
                    "UPDATE machine_domains SET reconciliation_required=0 WHERE domain_id=?1",
                    [&domain],
                )?;
                tx.execute(
                    "DELETE FROM machine_reconciles WHERE domain_id=?1",
                    [&domain],
                )?;
            }
            tx.execute(
                "INSERT INTO machine_operations VALUES (?1,?2,?3,?4,?5)",
                params![
                    domain,
                    request.request_sequence,
                    request.operation_id.to_string(),
                    request.payload_sha256,
                    serde_json::to_string(&intent.outcome)?
                ],
            )?;
            tx.execute(
                "UPDATE machine_domains SET accepted_sequence=?2 WHERE domain_id=?1",
                params![domain, request.request_sequence],
            )?;
            tx.execute(
                "UPDATE machine_meta SET value=CAST(value AS INTEGER)+1 WHERE key='revision'",
                [],
            )?;
        }
        tx.commit()?;
        journal
            .lock()
            .map_err(|_| StoreError::InvalidState("authority mutex poisoned".into()))?
            .finish_machine_commit(request.operation_id)?;
        Ok(())
    }
}
