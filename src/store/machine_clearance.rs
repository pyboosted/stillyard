//! Owner-audited domain retirement, recoverable before native admission starts.
use super::*;
use crate::machine::{
    DomainRetirementAudit, DomainRetirementReceipt, DomainRetirementRequest, GrantState,
};

impl Store {
    pub(crate) fn retire_machine_domain(
        &mut self,
        request: DomainRetirementRequest,
        requester: ProcessIdentity,
        principal: String,
    ) -> StoreResult<DomainRetirementReceipt> {
        self.reconcile_pending_domain_retirement()?;
        self.reconcile_pending_machine_commit()?;
        let operation = request.operation_id;
        super::machine_allocation::crash_boundary(
            &self.paths.root,
            operation,
            "before_retirement_journal",
        );
        let audit = self
            .authority_lock()?
            .prepare_domain_retirement(request, requester, principal, now_millis())
            .map_err(|error| {
                if error.kind() == std::io::ErrorKind::InvalidInput {
                    super::machine::rejected("retirement_conflict", &error.to_string())
                } else {
                    StoreError::Io(error)
                }
            })?;
        // An identical completed replay returns the immutable original receipt.
        if let Some(receipt) = self
            .authority_lock()?
            .retired_domain_receipt(audit.request.domain_id)
        {
            return Ok(receipt);
        }
        super::machine_allocation::crash_boundary(
            &self.paths.root,
            operation,
            "after_retirement_journal",
        );
        if let Err(error) = self.apply_domain_retirement(&audit) {
            if matches!(
                error,
                StoreError::InvalidState(_) | StoreError::Json(_) | StoreError::Id(_)
            ) {
                self.authority_lock()?.record_reset(
                    "SQL retirement inventory diverges from its accepted external audit",
                )?;
            }
            return Err(super::machine::rejected(
                "retirement_stalled",
                &format!(
                    "operation {operation} remains fenced: {error}; inspect authority status and repair coordinator history"
                ),
            ));
        }
        super::machine_allocation::crash_boundary(
            &self.paths.root,
            operation,
            "after_retirement_sql",
        );
        self.authority_lock()?.finish_domain_retirement(operation)?;
        super::machine_allocation::crash_boundary(
            &self.paths.root,
            operation,
            "after_retirement_ack",
        );
        Ok(audit.receipt()?)
    }

    pub(super) fn reconcile_pending_domain_retirement(&mut self) -> StoreResult<()> {
        let audit = self.authority_lock()?.pending_domain_retirement()?;
        if let Some(audit) = audit {
            if let Err(error) = self.apply_domain_retirement(&audit) {
                if matches!(
                    error,
                    StoreError::InvalidState(_) | StoreError::Json(_) | StoreError::Id(_)
                ) {
                    self.authority_lock()?.record_reset(&format!(
                        "SQL retirement operation {} needs inventory repair: {error}",
                        audit.request.operation_id
                    ))?;
                    // Keep the fence and debit, but remain inspectable. Explicit
                    // machine recovery can rebuild this divergent SQL projection
                    // from continuous external authority before finishing it.
                    return Ok(());
                }
                return Err(error);
            }
            self.authority_lock()?
                .finish_domain_retirement(audit.request.operation_id)?;
        }
        Ok(())
    }

    fn apply_domain_retirement(&mut self, audit: &DomainRetirementAudit) -> StoreResult<()> {
        let domain = audit.request.domain_id.0.to_string();
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let receipt = serde_json::to_string(&audit.receipt()?)?;
        let prior:Option<(String,String)>=tx.query_row("SELECT operation_id,receipt_json FROM machine_domain_retirements WHERE domain_id=?1",[&domain],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
        if let Some((operation, old)) = prior {
            if operation != audit.request.operation_id.to_string() || old != receipt {
                return Err(StoreError::InvalidState(
                    "domain retirement SQL audit differs".into(),
                ));
            }
            tx.commit()?;
            return Ok(());
        }
        let grants = {
            let mut q=tx.prepare("SELECT snapshot_json FROM machine_grants JOIN machine_candidates USING(allocation_key) WHERE domain_id=?1 AND machine_grants.state IN ('offered','armed','uncertain')")?;
            q.query_map([&domain], |r| r.get::<_, String>(0))?
                .map(|r| Ok(serde_json::from_str::<crate::machine::GrantSnapshot>(&r?)?))
                .collect::<StoreResult<Vec<_>>>()?
        };
        for mut grant in grants {
            let recorded = audit.preview.inventory.grants.iter().find(|g| {
                g.grant_id == grant.grant_id
                    && g.candidate == grant.candidate
                    && g.offer_nonce == grant.offer_nonce
                    && grant.tickets.iter().all(|t| g.tickets.contains(t))
            });
            if let Some(recorded) = recorded {
                grant = recorded.clone();
            } else if grant.state != GrantState::Offered {
                return Err(StoreError::InvalidState(
                    "SQL contains start rights outside the accepted retirement inventory".into(),
                ));
            }
            grant.state = if grant.state == GrantState::Offered {
                GrantState::Expired
            } else {
                GrantState::Released
            };
            grant.risk_clearance =
                (grant.state == GrantState::Released).then_some(audit.request.operation_id);
            grant.sealed_release = None;
            grant.released_unix_millis = Some(audit.retired_unix_millis);
            super::machine_queue::save_grant(&tx, &grant, None)?;
        }
        tx.execute("UPDATE machine_candidates SET state='canceled',reservation_deadline_ms=NULL WHERE domain_id=?1",[&domain])?;
        tx.execute("DELETE FROM machine_reservations WHERE allocation_key IN (SELECT allocation_key FROM machine_candidates WHERE domain_id=?1)",[&domain])?;
        tx.execute(
            "DELETE FROM machine_challenges WHERE domain_id=?1",
            [&domain],
        )?;
        tx.execute(
            "DELETE FROM machine_reconciles WHERE domain_id=?1",
            [&domain],
        )?;
        tx.execute("UPDATE machine_domains SET reconciliation_required=0,executor_incarnation=NULL WHERE domain_id=?1",[&domain])?;
        tx.execute(
            "INSERT INTO machine_domain_retirements VALUES (?1,?2,?3)",
            params![audit.request.operation_id.to_string(), domain, receipt],
        )?;
        tx.commit()?;
        Ok(())
    }
}
