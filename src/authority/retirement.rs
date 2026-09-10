//! Explicit owner risk clearance. A retired writer identity never revives.
use super::*;
use crate::machine::{DomainRetirementAudit, DomainRetirementReceipt, DomainRetirementRequest};

const MAX_RETIRED_DOMAINS: usize = 4096;
// Reserve the worst bounded receipt (including JSON escaping) for each live
// participant before it can gain rights. Retirement spends its own reservation.
pub(super) fn reserved_retirement_bytes(registry: &Registry) -> u64 {
    registry
        .participants
        .keys()
        .filter(|id| !registry.retired_domains.contains_key(id))
        .count() as u64
        * 16
        * 1024
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RetiredDomain {
    pub(super) installation_nonce: Uuid,
    pub(super) manager_store_uuid: Uuid,
    pub(super) receipt: DomainRetirementReceipt,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PendingRetirement {
    pub(super) operation_id: Uuid,
    pub(super) domain_id: crate::ExecutionDomainId,
    pub(super) audit_sha256: String,
}
fn audit_path(directory: &Path, operation: Uuid) -> PathBuf {
    directory.join(format!("domain-retirement-{operation}.json"))
}

impl Authority {
    pub(crate) fn is_retired_domain(&self, domain: crate::ExecutionDomainId) -> bool {
        matches!(&self.state,State::Ready(r) if r.retired_domains.contains_key(&domain))
    }
    pub(crate) fn retired_domain_receipt(
        &self,
        domain: crate::ExecutionDomainId,
    ) -> Option<DomainRetirementReceipt> {
        match &self.state {
            State::Ready(r)
                if r.pending_domain_retirement
                    .as_ref()
                    .is_none_or(|p| p.domain_id != domain) =>
            {
                r.retired_domains.get(&domain).map(|r| r.receipt.clone())
            }
            _ => None,
        }
    }
    pub(crate) fn retired_installation_receipt(
        &self,
        nonce: Uuid,
        store: Uuid,
    ) -> Option<DomainRetirementReceipt> {
        let State::Ready(registry) = &self.state else {
            return None;
        };
        registry
            .retired_domains
            .iter()
            .filter(|(_, r)| r.installation_nonce == nonce || r.manager_store_uuid == store)
            .find_map(|(domain, _)| self.retired_domain_receipt(*domain))
    }
    pub(crate) fn pending_installation_retirement(&self, nonce: Uuid, store: Uuid) -> Option<Uuid> {
        let State::Ready(registry) = &self.state else {
            return None;
        };
        let pending = registry.pending_domain_retirement.as_ref()?;
        let retired = registry.retired_domains.get(&pending.domain_id)?;
        (retired.installation_nonce == nonce || retired.manager_store_uuid == store)
            .then_some(pending.operation_id)
    }
    pub(super) fn check_retirement_pairing(
        &self,
        r: &crate::machine::PairingRegistration,
    ) -> std::io::Result<()> {
        let State::Ready(registry) = &self.state else {
            return Err(std::io::Error::other("authority history unavailable"));
        };
        if registry.pending_domain_retirement.is_some() {
            return Err(std::io::Error::other(
                "pending domain retirement must finish before pairing",
            ));
        }
        if registry
            .retired_domains
            .contains_key(&r.installation.domain_id)
            || registry.retired_domains.values().any(|old| {
                old.installation_nonce == r.installation.installation_nonce
                    || old.manager_store_uuid == r.manager_store_uuid
            })
            || registry.participants.values().any(|old| {
                old.registration.installation.domain_id != r.installation.domain_id
                    && old.registration.manager_store_uuid == r.manager_store_uuid
            })
        {
            return Err(std::io::Error::other(
                "domain, installation or manager store identity is already used or retired",
            ));
        }
        if !registry
            .participants
            .contains_key(&r.installation.domain_id)
            && registry.retired_domains.len() + registry.participants.len() >= MAX_RETIRED_DOMAINS
        {
            return Err(std::io::Error::other(
                "retired identity budget is exhausted; preserve the authority and its audits",
            ));
        }
        Ok(())
    }
    pub(crate) fn pending_domain_retirement(
        &self,
    ) -> std::io::Result<Option<DomainRetirementAudit>> {
        let State::Ready(registry) = &self.state else {
            return Ok(None);
        };
        let Some(pending) = &registry.pending_domain_retirement else {
            return Ok(None);
        };
        let audit: DomainRetirementAudit = read_with_limit(
            &audit_path(&self.directory, pending.operation_id),
            MAX_MACHINE_JOURNAL_BYTES,
        )?;
        if crate::machine::payload_hash(&audit)? != pending.audit_sha256
            || audit.request.operation_id != pending.operation_id
            || audit.request.domain_id != pending.domain_id
        {
            return Err(std::io::Error::other(
                "pending retirement audit identity/checksum differs",
            ));
        }
        Ok(Some(audit))
    }
    pub(crate) fn prepare_domain_retirement(
        &mut self,
        request: DomainRetirementRequest,
        requester: ProcessIdentity,
        principal: String,
        now: i64,
    ) -> std::io::Result<DomainRetirementAudit> {
        if request.operation_id.is_nil()
            || !valid_reason(&request.reason)
            || principal.is_empty()
            || principal.len() > 256
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "retirement needs an operation, reason and authenticated owner principal",
            ));
        }
        let State::Ready(registry) = &self.state else {
            return Err(std::io::Error::other("authority history unavailable"));
        };
        if registry.pending_machine_commit.is_some() {
            return Err(std::io::Error::other(
                "reconcile the pending machine commit before domain retirement",
            ));
        }
        if registry
            .pending_domain_retirement
            .as_ref()
            .is_some_and(|p| p.operation_id != request.operation_id)
        {
            return Err(std::io::Error::other(
                "another domain retirement requires recovery",
            ));
        }
        if registry
            .participants
            .values()
            .any(|p| p.registration.parent_domain == request.domain_id)
        {
            return Err(std::io::Error::other(
                "retire registered descendant domains before retiring their parent",
            ));
        }
        let path = audit_path(&self.directory, request.operation_id);
        if let Some(retired) = registry.retired_domains.get(&request.domain_id) {
            if retired.receipt.operation_id != request.operation_id {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "domain was already retired by operation {}",
                        retired.receipt.operation_id
                    ),
                ));
            }
        }
        let audit = if path.exists() {
            let old: DomainRetirementAudit = read_with_limit(&path, MAX_MACHINE_JOURNAL_BYTES)?;
            if old.request != request {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "retirement operation payload conflict",
                ));
            }
            old
        } else {
            let preview = self.domain_clearance_preview(request.domain_id)?;
            DomainRetirementAudit {
                preview,
                request: request.clone(),
                requester,
                requester_principal: principal,
                retired_unix_millis: now,
            }
        };
        if let Some(retired) = registry.retired_domains.get(&request.domain_id) {
            if retired.receipt.operation_id != request.operation_id
                || retired.receipt != audit.receipt()?
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "domain was already retired by another operation",
                ));
            }
            return Ok(audit);
        }
        let preview = self.domain_clearance_preview(request.domain_id)?;
        if preview != audit.preview || preview.sha256 != request.expected_inventory_sha256 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "retirement inventory changed; inspect the complete current preview",
            ));
        }
        if !preview.inventory.grants.is_empty() && !request.accept_risk {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "outstanding Grants require explicit acceptance of unproven process cleanup",
            ));
        }
        if !preview.inventory.committed
            && (preview.inventory.session.is_some() || !preview.inventory.grants.is_empty())
        {
            return Err(std::io::Error::other(
                "uncommitted registration unexpectedly owns launch authority",
            ));
        }
        let receipt = audit.receipt()?;
        if serde_json::to_vec(&receipt)?.len() > 12 * 1024 {
            return Err(std::io::Error::other(
                "authenticated retirement receipt exceeds reserved metadata budget",
            ));
        }
        let bytes = serde_json::to_vec(&audit)?;
        if bytes.len() as u64 > MAX_MACHINE_JOURNAL_BYTES {
            return Err(std::io::Error::other(
                "domain retirement audit exceeds durable byte limit",
            ));
        }
        let mut next = registry.as_ref().clone();
        next.retired_domains.insert(
            request.domain_id,
            RetiredDomain {
                installation_nonce: preview.inventory.installation.installation_nonce,
                manager_store_uuid: preview.inventory.manager_store_uuid,
                receipt: receipt.clone(),
            },
        );
        next.pending_domain_retirement = Some(PendingRetirement {
            operation_id: request.operation_id,
            domain_id: request.domain_id,
            audit_sha256: receipt.audit_sha256,
        });
        self.check_publication(&next)?;
        if !path.exists() {
            atomic_write(&path, &bytes)?;
        }
        // The tombstone is the fence. Existing permissions remain charged until
        // the SQL retirement commit has completed and this pending slot finishes.
        self.publish(next)?;
        Ok(audit)
    }
    pub(crate) fn finish_domain_retirement(&mut self, operation: Uuid) -> std::io::Result<()> {
        let State::Ready(registry) = &self.state else {
            return Err(std::io::Error::other("authority history unavailable"));
        };
        let Some(pending) = &registry.pending_domain_retirement else {
            return Ok(());
        };
        if pending.operation_id != operation {
            return Err(std::io::Error::other(
                "pending domain retirement operation changed",
            ));
        }
        let mut next = registry.as_ref().clone();
        next.machine_permissions
            .retain(|_, g| g.candidate.key.domain_id != pending.domain_id);
        next.participants.remove(&pending.domain_id);
        next.pending_domain_retirement = None;
        self.publish(next)
    }

    pub(super) fn validate_retirements(
        directory: &Path,
        registry: &Registry,
    ) -> std::io::Result<()> {
        if registry.retired_domains.len() > MAX_RETIRED_DOMAINS {
            return Err(std::io::Error::other(
                "retired domain inventory exceeds bounds",
            ));
        }
        for (domain, retired) in &registry.retired_domains {
            if domain != &retired.receipt.domain_id
                || retired.installation_nonce.is_nil()
                || retired.manager_store_uuid.is_nil()
                || retired.manager_store_uuid != retired.receipt.manager_store_uuid
                || retired.receipt.operation_id.is_nil()
            {
                return Err(std::io::Error::other("invalid retired domain identity"));
            }
            let pending = registry
                .pending_domain_retirement
                .as_ref()
                .is_some_and(|p| p.domain_id == *domain);
            if !pending
                && (registry.participants.contains_key(domain)
                    || registry
                        .participants
                        .values()
                        .any(|p| p.registration.parent_domain == *domain)
                    || registry
                        .machine_permissions
                        .values()
                        .any(|g| g.candidate.key.domain_id == *domain))
            {
                return Err(std::io::Error::other(
                    "completed retired identity still owns launch authority",
                ));
            }
            if registry
                .pending_machine_commit
                .as_ref()
                .is_some_and(|p| p.request.session.domain_id == *domain)
            {
                return Err(std::io::Error::other(
                    "machine commit attempts to revive a retired identity",
                ));
            }
        }
        if let Some(p) = &registry.pending_domain_retirement {
            let audit: DomainRetirementAudit = read_with_limit(
                &audit_path(directory, p.operation_id),
                MAX_MACHINE_JOURNAL_BYTES,
            )?;
            let receipt = audit.receipt()?;
            if registry.pending_machine_commit.is_some()
                || audit.request.operation_id != p.operation_id
                || audit.request.domain_id != p.domain_id
                || crate::machine::payload_hash(&audit)? != p.audit_sha256
                || registry.retired_domains.get(&p.domain_id).is_none_or(|r| {
                    r.receipt != receipt || r.receipt.audit_sha256 != p.audit_sha256
                })
            {
                return Err(std::io::Error::other(
                    "pending retirement audit/fence is inconsistent",
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::machine::*;

    #[test]
    fn pairing_budget_reserves_every_admitted_retirement_after_hold_and_session_growth() {
        let directory = crate::test_support::durable_tempdir().unwrap();
        let host = HostId("metadata-budget-host".into());
        let mut a = Authority::open(directory.path().into(), host.clone());
        a.initialize().unwrap();
        let actor = ProcessIdentity::Windows {
            host_id: host,
            boot_id: crate::BootId("test-boot".into()),
            pid: 42,
            creation_filetime_100ns: 123,
        };
        let mut holds = Vec::new();
        for _ in 0..40 {
            let id = Uuid::now_v7();
            a.hold(id, "reserved cleanup metadata".into(), actor.clone())
                .unwrap();
            holds.push(id);
        }
        let domains = a.snapshot().domains.unwrap();
        let mut registrations = Vec::new();
        loop {
            let r = PairingRegistration {
                installation: InstallationIdentity {
                    installation_nonce: Uuid::now_v7(),
                    domain_id: crate::ExecutionDomainId(Uuid::now_v7()),
                    owner_uid: 1000,
                    runtime_registration: "retirement-budget".into(),
                    role: ParticipantRole::Executor,
                },
                manager_store_uuid: Uuid::now_v7(),
                parent_domain: domains.machine_scope,
                budgets: Default::default(),
                aliases: Default::default(),
                secret: [7; 32],
            };
            match a.prepare_pairing(r.clone()) {
                Ok(()) => {
                    a.commit_pairing(r.installation.domain_id).unwrap();
                    registrations.push(r);
                }
                Err(e) => {
                    assert!(e.to_string().contains("budget"));
                    break;
                }
            }
        }
        assert!(registrations.len() > 20);
        // Session checkpoints and late maximum escaped release reasons must
        // not spend the cleanup reservation belonging to these participants.
        for r in &registrations {
            let epoch = a
                .reserve_machine_connection(r.installation.domain_id, 0)
                .unwrap();
            a.accept_machine_session(SessionIdentity {
                machine_id: domains.machine_id,
                authority_epoch: a.snapshot().epoch.unwrap(),
                domain_id: r.installation.domain_id,
                manager_store_uuid: r.manager_store_uuid,
                executor_incarnation: Uuid::now_v7(),
                connection_epoch: epoch,
            })
            .unwrap();
        }
        for id in holds {
            a.force_release(id, "\u{1}".repeat(1024), actor.clone())
                .unwrap();
        }
        for r in &registrations {
            let p = a
                .domain_clearance_preview(r.installation.domain_id)
                .unwrap();
            let audit = a
                .prepare_domain_retirement(
                    DomainRetirementRequest {
                        operation_id: Uuid::now_v7(),
                        domain_id: r.installation.domain_id,
                        expected_inventory_sha256: p.sha256,
                        reason: "\u{1}".repeat(1024),
                        accept_risk: false,
                    },
                    actor.clone(),
                    "S-1-owner".into(),
                    1,
                )
                .unwrap();
            a.finish_domain_retirement(audit.request.operation_id)
                .unwrap();
        }
        assert_eq!(a.snapshot().retired_domains.len(), registrations.len());
        let budget = a.snapshot().storage_budget.unwrap();
        assert_eq!(budget.reserved_recovery_bytes, 0);
        assert!(budget.registry_bytes < budget.registry_limit_bytes);
    }

    #[test]
    fn missing_or_changed_pending_retirement_audit_keeps_authority_closed() {
        let directory = crate::test_support::durable_tempdir().unwrap();
        let host = HostId("retirement-audit-host".into());
        let mut a = Authority::open(directory.path().into(), host.clone());
        a.initialize().unwrap();
        let registration = PairingRegistration {
            installation: InstallationIdentity {
                installation_nonce: Uuid::now_v7(),
                domain_id: crate::ExecutionDomainId(Uuid::now_v7()),
                owner_uid: 1000,
                runtime_registration: "pending-retirement".into(),
                role: ParticipantRole::Executor,
            },
            manager_store_uuid: Uuid::now_v7(),
            parent_domain: a.snapshot().domains.unwrap().machine_scope,
            budgets: Default::default(),
            aliases: Default::default(),
            secret: [7; 32],
        };
        a.prepare_pairing(registration.clone()).unwrap();
        let preview = a
            .domain_clearance_preview(registration.installation.domain_id)
            .unwrap();
        let request = DomainRetirementRequest {
            operation_id: Uuid::now_v7(),
            domain_id: registration.installation.domain_id,
            expected_inventory_sha256: preview.sha256,
            reason: "test interrupted registration retirement".into(),
            accept_risk: false,
        };
        let actor = ProcessIdentity::Windows {
            host_id: host.clone(),
            boot_id: crate::BootId("test-boot".into()),
            pid: 42,
            creation_filetime_100ns: 123,
        };
        let audit = a
            .prepare_domain_retirement(request.clone(), actor, "S-1-test-owner".into(), 1)
            .unwrap();
        assert!(a.is_retired_domain(request.domain_id));
        assert!(a.snapshot().blocker.is_some());
        assert!(a.reserve_machine_connection(request.domain_id, 0).is_err());
        assert!(a.prepare_pairing(registration.clone()).is_err());
        let path = audit_path(directory.path(), request.operation_id);
        let bytes = std::fs::read(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        let absent = Authority::open(directory.path().into(), host.clone());
        assert!(absent.snapshot().blocker.is_some());
        assert!(absent.snapshot().epoch.is_none());
        assert!(
            absent
                .snapshot()
                .detail
                .unwrap()
                .contains("domain-retirement-")
        );
        let mut changed = audit.clone();
        changed.request.reason.push('!');
        atomic_write(&path, &serde_json::to_vec(&changed).unwrap()).unwrap();
        let corrupt = Authority::open(directory.path().into(), host.clone());
        assert!(corrupt.snapshot().blocker.is_some());
        assert!(corrupt.snapshot().epoch.is_none());
        atomic_write(&path, &bytes).unwrap();
        let mut restored = Authority::open(directory.path().into(), host.clone());
        assert_eq!(
            restored.pending_domain_retirement().unwrap(),
            Some(audit.clone())
        );
        restored
            .finish_domain_retirement(request.operation_id)
            .unwrap();
        let mut reopened = Authority::open(directory.path().into(), host);
        assert!(reopened.snapshot().blocker.is_none());
        assert!(reopened.participants().unwrap().is_empty());
        assert_eq!(
            reopened.retired_domain_receipt(request.domain_id),
            Some(audit.receipt().unwrap())
        );
        assert!(reopened.prepare_pairing(registration).is_err());
        assert!(
            path.exists(),
            "commit blob GC must preserve retirement audits"
        );
    }
}
