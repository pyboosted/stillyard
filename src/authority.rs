//! Durable admission interlock. This is the bootstrap/maintenance foundation, not
//! the attached-domain allocator. SQLite reset must never reset these obligations.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{AuthorityHold, AuthoritySnapshot, HostId, ProcessIdentity};

mod retirement;

const MAX_REGISTRY_BYTES: u64 = 4 * 1024 * 1024;
const MAX_HOLDS: usize = 1024;
// A commit may repeat the complete <=16 MiB reconciliation inventory plus
// request/outcome frames. Its separate immutable blob cannot consume the space
// needed by the small authority pointer or by a cleanup acknowledgement.
const MAX_MACHINE_JOURNAL_BYTES: u64 = 32 * 1024 * 1024;
const REGISTRY_COMMIT_HEADROOM: u64 = 1024 * 1024;

fn reserved_recovery_bytes(registry: &Registry) -> u64 {
    retirement::reserved_retirement_bytes(registry)
        + registry.holds.values().filter(|h| !h.released).count() as u64 * 64 * 1024
}

fn storage_budget(registry: &Registry) -> std::io::Result<crate::AuthorityStorageBudget> {
    // Published registries already carry the checksum-addressed blob pointer.
    // Do not reserialize the up-to-32-MiB pending body merely for observation.
    let mut payload = registry.clone();
    payload.pending_machine_commit = None;
    let bytes = serde_json::to_vec(&Envelope {
        payload,
        sha256: "0".repeat(64),
    })?
    .len() as u64;
    let reserved = reserved_recovery_bytes(registry);
    Ok(crate::AuthorityStorageBudget {
        registry_bytes: bytes,
        registry_limit_bytes: MAX_REGISTRY_BYTES,
        reserved_recovery_bytes: reserved,
        new_admission_headroom_bytes: (MAX_REGISTRY_BYTES - REGISTRY_COMMIT_HEADROOM)
            .saturating_sub(bytes)
            .min((MAX_REGISTRY_BYTES - 256 * 1024).saturating_sub(bytes.saturating_add(reserved))),
    })
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Registry {
    version: u32,
    epoch: Uuid,
    /// The installation anchor stays immutable across fenced authority epochs.
    /// Old registries use their initial epoch as this stable identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    anchor_epoch: Option<Uuid>,
    host: HostId,
    holds: BTreeMap<Uuid, AuthorityHold>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    domains: Option<crate::AuthorityDomains>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    coordinator: Option<crate::machine::CoordinatorHistory>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    participants: BTreeMap<crate::ExecutionDomainId, ParticipantAnchor>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    machine_permissions: BTreeMap<crate::GrantId, crate::machine::GrantSnapshot>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    native_permissions: BTreeMap<crate::InvocationId, crate::machine::NativeStartPermission>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    native_coverage_store: Option<Uuid>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    retired_allocations: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pending_machine_commit: Option<MachineCommitIntent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pending_machine_blob: Option<MachineBlob>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    retired_domains: BTreeMap<crate::ExecutionDomainId, retirement::RetiredDomain>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pending_domain_retirement: Option<retirement::PendingRetirement>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MachineBlob {
    operation_id: Uuid,
    sha256: String,
}
impl MachineBlob {
    fn path(&self, directory: &Path) -> PathBuf {
        directory.join(format!(
            "machine-commit-{}-{}.json",
            self.operation_id, self.sha256
        ))
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MachineCommitIntent {
    pub(crate) request: crate::machine::Request,
    pub(crate) outcome: crate::machine::Outcome,
    pub(crate) grants: Vec<crate::machine::GrantSnapshot>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ParticipantAnchor {
    pub(crate) registration: crate::machine::PairingRegistration,
    pub(crate) committed: bool,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub(crate) next_connection_epoch: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) session: Option<crate::machine::SessionIdentity>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub(crate) accepted_sequence: u64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub(crate) retired_sequence_floor: u64,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    payload: Registry,
    sha256: String,
}

enum State {
    Uninitialized,
    Ready(Box<Registry>),
    Unknown(String),
}

pub(crate) struct Authority {
    directory: PathBuf,
    host: HostId,
    state: State,
}

fn new_domains() -> crate::AuthorityDomains {
    crate::AuthorityDomains {
        machine_id: Uuid::now_v7(),
        machine_scope: crate::ExecutionDomainId(Uuid::now_v7()),
        native_domain: crate::ExecutionDomainId(Uuid::now_v7()),
    }
}

#[cfg(test)]
mod topology_tests {
    use super::*;

    #[test]
    fn reset_gate_survives_both_crash_sides_and_preserves_predecessor_identity() {
        let temp = crate::test_support::durable_tempdir().unwrap();
        let host = HostId("fixture-host".into());
        let directory = temp.path().join("authority");
        let mut authority = Authority::open(directory.clone(), host.clone());
        authority.initialize().unwrap();
        let old_store = Uuid::now_v7();
        let old_generation = Uuid::now_v7();
        let old_identity = ProcessIdentity::Windows {
            host_id: host.clone(),
            boot_id: crate::BootId("fixture-boot".into()),
            pid: 42,
            creation_filetime_100ns: 17,
        };
        authority
            .bind_coordinator(old_store, old_generation, old_identity.clone())
            .unwrap();

        // A crash after the gate but before file deletion leaves the old UUID.
        Authority::before_store_reset(directory.clone(), Some(&host)).unwrap();
        let mut reopened = Authority::open(directory.clone(), host.clone());
        let gated = reopened.snapshot();
        assert_eq!(
            gated.blocker.as_deref(),
            Some("authority_reconciliation_required")
        );
        reopened
            .bind_coordinator(old_store, Uuid::now_v7(), old_identity.clone())
            .unwrap();
        assert_eq!(reopened.snapshot(), gated);

        // A new SQLite UUID and a new daemon cannot consume or replace the gate.
        reopened
            .bind_coordinator(Uuid::now_v7(), Uuid::now_v7(), old_identity.clone())
            .unwrap();
        assert_eq!(reopened.snapshot(), gated);
        let history = gated.coordinator.unwrap();
        assert_eq!(history.store_uuid, old_store);
        assert_eq!(history.daemon_generation, old_generation);
        assert_eq!(history.process_identity, old_identity);
        assert_eq!(
            Authority::open(directory, host).snapshot().coordinator,
            Some(history)
        );
    }

    #[test]
    fn missing_database_uuid_cannot_rebind_continuous_authority_as_empty() {
        let temp = crate::test_support::durable_tempdir().unwrap();
        let host = HostId("fixture-host".into());
        let directory = temp.path().join("authority");
        let mut authority = Authority::open(directory.clone(), host.clone());
        authority.initialize().unwrap();
        let old_store = Uuid::now_v7();
        let identity = ProcessIdentity::Windows {
            host_id: host.clone(),
            boot_id: crate::BootId("fixture-boot".into()),
            pid: 42,
            creation_filetime_100ns: 17,
        };
        authority
            .bind_coordinator(old_store, Uuid::now_v7(), identity.clone())
            .unwrap();
        let mut reopened = Authority::open(directory, host);
        reopened
            .bind_coordinator(Uuid::now_v7(), Uuid::now_v7(), identity)
            .unwrap();
        assert_eq!(
            reopened.snapshot().blocker.as_deref(),
            Some("authority_reconciliation_required")
        );
        assert_eq!(
            reopened.snapshot().coordinator.unwrap().store_uuid,
            old_store
        );
        assert_eq!(
            reopened.initialize().unwrap().blocker.as_deref(),
            Some("authority_reconciliation_required")
        );
    }

    #[test]
    fn continuous_legacy_registry_gets_stable_domains_without_releasing_obligations() {
        let temp = crate::test_support::durable_tempdir().unwrap();
        let host = HostId("fixture-host".into());
        let directory = temp.path().join("authority");
        let mut authority = Authority::open(directory.clone(), host.clone());
        authority.initialize().unwrap();
        let operation = Uuid::now_v7();
        authority
            .hold(
                operation,
                "fixture retained work".into(),
                ProcessIdentity::Windows {
                    host_id: host.clone(),
                    boot_id: crate::BootId("fixture-boot".into()),
                    pid: 1,
                    creation_filetime_100ns: 1,
                },
            )
            .unwrap();
        let mut legacy = match &authority.state {
            State::Ready(registry) => registry.as_ref().clone(),
            _ => panic!("initialized fixture"),
        };
        let epoch = legacy.epoch;
        legacy.domains = None;
        authority.publish(legacy).unwrap();
        let upgraded = Authority::open(directory.clone(), host.clone()).snapshot();
        assert_eq!(upgraded.epoch, Some(epoch));
        assert_eq!(upgraded.blocker.as_deref(), Some("authority_held"));
        assert_eq!(upgraded.holds[0].id, operation);
        assert!(!upgraded.holds[0].released);
        assert!(upgraded.domains.is_some());
        assert_eq!(
            Authority::open(directory, host).snapshot().domains,
            upgraded.domains
        );
    }
}

impl Authority {
    pub(crate) fn pending_machine_commit(&self) -> std::io::Result<Option<MachineCommitIntent>> {
        match &self.state {
            State::Ready(registry) => Ok(registry.pending_machine_commit.clone()),
            _ => Err(std::io::Error::other("authority history unavailable")),
        }
    }

    pub(crate) fn record_native_start(
        &mut self,
        permission: crate::machine::NativeStartPermission,
    ) -> std::io::Result<()> {
        let State::Ready(registry) = &self.state else {
            return Err(std::io::Error::other(
                "native start authority history unavailable",
            ));
        };
        if let Some(old) = registry.native_permissions.get(&permission.invocation_id) {
            return if old == &permission {
                Ok(())
            } else {
                Err(std::io::Error::other(
                    "native Invocation already names another root or allocation",
                ))
            };
        }
        let key = permission.allocation.key.as_ref().ok_or_else(|| {
            std::io::Error::other("native allocation authority identity unavailable")
        })?;
        if registry.epoch != key.authority_epoch
            || registry.native_coverage_store != Some(key.manager_store_uuid)
            || registry
                .domains
                .as_ref()
                .is_none_or(|d| d.machine_id != key.machine_id || d.native_domain != key.domain_id)
            || registry
                .coordinator
                .as_ref()
                .is_none_or(|c| c.store_uuid != key.manager_store_uuid || c.pending_reset.is_some())
            || permission.invocation_id.store_uuid() != key.manager_store_uuid
            || permission.containment_id.store_uuid() != key.manager_store_uuid
            || permission.allocation.grant_id.entity_uuid() != key.lease_id
        {
            return Err(std::io::Error::other(
                "native start does not belong to this continuous authority",
            ));
        }
        if permission.allocation.grant_id.store_uuid() != key.manager_store_uuid
            || permission.allocation.lease_id != key.lease_id
            || permission.allocation.state != crate::machine::GrantState::Armed
        {
            return Err(std::io::Error::other(
                "native permission does not name its granted Lease",
            ));
        }
        if registry.native_permissions.len() >= 65_536 {
            return Err(std::io::Error::other(
                "native start registry exhausted; cleanup must reconcile",
            ));
        }
        let mut next = registry.as_ref().clone();
        next.native_permissions
            .insert(permission.invocation_id, permission);
        self.publish(next)
    }

    pub(crate) fn establish_native_coverage(
        &mut self,
        store: Uuid,
        empty: bool,
    ) -> std::io::Result<()> {
        let State::Ready(registry) = &self.state else {
            return Ok(());
        };
        if registry.native_coverage_store.is_some()
            || registry
                .coordinator
                .as_ref()
                .is_none_or(|c| c.store_uuid != store || c.pending_reset.is_some())
        {
            return Ok(());
        }
        if !empty {
            return self
                .record_reset("native start history predates external coverage and is not empty");
        }
        let mut next = registry.as_ref().clone();
        next.native_coverage_store = Some(store);
        self.publish(next)
    }

    /// The caller has checked durable empty/cleared containment rows. Missing
    /// SQL rows never call this method and therefore cannot erase a start right.
    pub(crate) fn retire_native_starts(
        &mut self,
        proven: &[crate::InvocationId],
    ) -> std::io::Result<()> {
        let State::Ready(registry) = &self.state else {
            return Err(std::io::Error::other(
                "native cleanup authority history unavailable",
            ));
        };
        if !proven
            .iter()
            .any(|id| registry.native_permissions.contains_key(id))
        {
            return Ok(());
        }
        let mut next = registry.as_ref().clone();
        for id in proven {
            next.native_permissions.remove(id);
        }
        self.publish(next)
    }

    pub(crate) fn prepare_machine_commit(
        &mut self,
        intent: MachineCommitIntent,
    ) -> std::io::Result<()> {
        let next = self.proposed_machine_commit(intent)?;
        self.publish(next)
    }

    pub(crate) fn check_machine_commit(&self, intent: MachineCommitIntent) -> std::io::Result<()> {
        let next = self.proposed_machine_commit(intent)?;
        self.check_publication(&next)?;
        Ok(())
    }

    fn proposed_machine_commit(&self, intent: MachineCommitIntent) -> std::io::Result<Registry> {
        if self.is_retired_domain(intent.request.session.domain_id) {
            return Err(std::io::Error::other(
                "retired domain cannot commit machine operations",
            ));
        }
        let State::Ready(registry) = &self.state else {
            return Err(std::io::Error::other("authority history unavailable"));
        };
        if registry.pending_domain_retirement.is_some() {
            return Err(std::io::Error::other(
                "pending domain retirement must finish before a machine commit",
            ));
        }
        let anchor = registry
            .participants
            .get(&intent.request.session.domain_id)
            .ok_or_else(|| std::io::Error::other("machine commit participant anchor is absent"))?;
        if !anchor.committed || anchor.session.as_ref() != Some(&intent.request.session) {
            return Err(std::io::Error::other("machine commit session was fenced"));
        }
        if let Some(pending) = &registry.pending_machine_commit {
            if serde_json::to_vec(pending)? == serde_json::to_vec(&intent)? {
                return Ok(registry.as_ref().clone());
            }
            return Err(std::io::Error::other(
                "another machine commit requires reconciliation",
            ));
        }
        let mut next = registry.as_ref().clone();
        for grant in &intent.grants {
            if grant.state == crate::machine::GrantState::Released {
                continue;
            }
            if next
                .retired_allocations
                .contains(&crate::machine::payload_hash(&grant.candidate.key)?)
            {
                return Err(std::io::Error::other(
                    "retired allocation cannot acquire new start rights",
                ));
            }
            if next.machine_permissions.values().any(|old| {
                old.candidate.key == grant.candidate.key
                    && (old.grant_id != grant.grant_id || old.offer_nonce != grant.offer_nonce)
            }) {
                return Err(std::io::Error::other(
                    "allocation already owns another potentially used Grant",
                ));
            }
            next.machine_permissions
                .insert(grant.grant_id, grant.clone());
        }
        next.pending_machine_commit = Some(intent);
        Ok(next)
    }

    pub(crate) fn finish_machine_commit(&mut self, operation_id: Uuid) -> std::io::Result<()> {
        let State::Ready(registry) = &self.state else {
            return Err(std::io::Error::other("authority history unavailable"));
        };
        let Some(intent) = &registry.pending_machine_commit else {
            return Ok(());
        };
        if intent.request.operation_id != operation_id {
            return Err(std::io::Error::other("machine commit identity changed"));
        }
        let mut next = registry.as_ref().clone();
        for grant in &intent.grants {
            if grant.state == crate::machine::GrantState::Released {
                next.retired_allocations
                    .insert(crate::machine::payload_hash(&grant.candidate.key)?);
                next.machine_permissions.remove(&grant.grant_id);
            }
        }
        next.pending_machine_commit = None;
        let anchor = next
            .participants
            .get_mut(&intent.request.session.domain_id)
            .ok_or_else(|| std::io::Error::other("machine commit participant anchor missing"))?;
        anchor.accepted_sequence = anchor
            .accepted_sequence
            .max(intent.request.request_sequence);
        if let crate::machine::Outcome::Acknowledged { through_sequence } = intent.outcome {
            if through_sequence >= intent.request.request_sequence {
                return Err(std::io::Error::other("invalid journaled acknowledgement"));
            }
            anchor.retired_sequence_floor = anchor.retired_sequence_floor.max(through_sequence);
        }
        self.publish(next)
    }

    /// Compact external duplicates only with matching Released tombstones in
    /// continuous coordinator SQL. Losing SQL gates admission and rotates epoch.
    pub(crate) fn retirement_candidates(&self, store: Uuid) -> Vec<String> {
        match &self.state {
            State::Ready(r)
                if r.pending_machine_commit.is_none()
                    && r.coordinator
                        .as_ref()
                        .is_some_and(|h| h.store_uuid == store && h.pending_reset.is_none()) =>
            {
                r.retired_allocations.iter().cloned().collect()
            }
            _ => vec![],
        }
    }
    pub(crate) fn compact_machine_retirements(
        &mut self,
        store: Uuid,
        proven: &BTreeSet<String>,
    ) -> std::io::Result<()> {
        let candidates = self.retirement_candidates(store);
        if !candidates.iter().any(|key| proven.contains(key)) {
            return Ok(());
        }
        let State::Ready(registry) = &self.state else {
            return Err(std::io::Error::other("retirement history unavailable"));
        };
        let mut next = registry.as_ref().clone();
        next.retired_allocations
            .retain(|hash| !proven.contains(hash));
        self.publish(next)
    }

    pub(crate) fn domain_clearance_preview(
        &self,
        domain: crate::ExecutionDomainId,
    ) -> std::io::Result<crate::machine::DomainClearancePreview> {
        let State::Ready(registry) = &self.state else {
            return Err(std::io::Error::other("authority history is unavailable"));
        };
        if registry.pending_machine_commit.is_some() {
            return Err(std::io::Error::other(
                "reconcile the pending machine commit before taking a clearance preview",
            ));
        }
        let participant = registry
            .participants
            .get(&domain)
            .ok_or_else(|| std::io::Error::other("paired domain is absent"))?;
        let domains = registry
            .domains
            .as_ref()
            .ok_or_else(|| std::io::Error::other("machine topology is unavailable"))?;
        crate::machine::DomainClearancePreview::new(crate::machine::DomainClearanceInventory {
            machine_id: domains.machine_id,
            authority_epoch: registry.epoch,
            installation: participant.registration.installation.clone(),
            manager_store_uuid: participant.registration.manager_store_uuid,
            session: participant.session.clone(),
            committed: participant.committed,
            grants: registry
                .machine_permissions
                .values()
                .filter(|g| g.candidate.key.domain_id == domain)
                .cloned()
                .collect(),
        })
    }

    pub(crate) fn participants(&self) -> std::io::Result<Vec<ParticipantAnchor>> {
        match &self.state {
            State::Ready(registry) => Ok(registry.participants.values().cloned().collect()),
            _ => Err(std::io::Error::other(
                "machine authority history is unavailable",
            )),
        }
    }

    pub(crate) fn reserve_machine_connection(
        &mut self,
        domain: crate::ExecutionDomainId,
        sql_epoch: u64,
    ) -> std::io::Result<u64> {
        if self.is_retired_domain(domain) {
            return Err(std::io::Error::other("retired domain cannot reconnect"));
        }
        let State::Ready(registry) = &self.state else {
            return Err(std::io::Error::other("pairing history unavailable"));
        };
        let mut next = registry.as_ref().clone();
        let anchor = next
            .participants
            .get_mut(&domain)
            .filter(|a| a.committed)
            .ok_or_else(|| std::io::Error::other("participant anchor missing"))?;
        let epoch = anchor
            .next_connection_epoch
            .max(sql_epoch)
            .checked_add(1)
            .filter(|e| *e <= i64::MAX as u64)
            .ok_or_else(|| std::io::Error::other("connection epoch exhausted"))?;
        anchor.next_connection_epoch = epoch;
        self.publish(next)?;
        Ok(epoch)
    }

    pub(crate) fn accept_machine_session(
        &mut self,
        session: crate::machine::SessionIdentity,
    ) -> std::io::Result<()> {
        if self.is_retired_domain(session.domain_id) {
            return Err(std::io::Error::other("retired domain cannot reconnect"));
        }
        let State::Ready(registry) = &self.state else {
            return Err(std::io::Error::other("pairing history unavailable"));
        };
        let mut next = registry.as_ref().clone();
        let anchor = next
            .participants
            .get_mut(&session.domain_id)
            .filter(|a| a.committed)
            .ok_or_else(|| std::io::Error::other("participant anchor missing"))?;
        if anchor.session.as_ref() == Some(&session) {
            return Ok(());
        }
        if anchor.next_connection_epoch != session.connection_epoch
            || anchor.registration.manager_store_uuid != session.manager_store_uuid
            || registry.epoch != session.authority_epoch
            || registry
                .domains
                .as_ref()
                .is_none_or(|d| d.machine_id != session.machine_id)
            || anchor
                .session
                .as_ref()
                .is_some_and(|old| old.connection_epoch >= session.connection_epoch)
        {
            return Err(std::io::Error::other(
                "session identity or durable connection epoch changed",
            ));
        }
        anchor.session = Some(session);
        self.publish(next)
    }

    pub(crate) fn checkpoint_machine_sequence(
        &mut self,
        domain: crate::ExecutionDomainId,
        sequence: u64,
    ) -> std::io::Result<()> {
        let State::Ready(registry) = &self.state else {
            return Err(std::io::Error::other(
                "operation checkpoint history unavailable",
            ));
        };
        let anchor = registry
            .participants
            .get(&domain)
            .filter(|a| a.committed)
            .ok_or_else(|| std::io::Error::other("participant anchor missing"))?;
        if sequence <= anchor.accepted_sequence {
            return Ok(());
        }
        if sequence > i64::MAX as u64 {
            return Err(std::io::Error::other("operation sequence exhausted"));
        }
        let mut next = registry.as_ref().clone();
        next.participants
            .get_mut(&domain)
            .unwrap()
            .accepted_sequence = sequence;
        self.publish(next)
    }

    pub(crate) fn retire_machine_sequences(
        &mut self,
        domain: crate::ExecutionDomainId,
        floor: u64,
    ) -> std::io::Result<()> {
        let State::Ready(registry) = &self.state else {
            return Err(std::io::Error::other(
                "sequence retirement history unavailable",
            ));
        };
        let anchor = registry
            .participants
            .get(&domain)
            .ok_or_else(|| std::io::Error::other("participant anchor missing"))?;
        if floor > anchor.accepted_sequence {
            return Err(std::io::Error::other(
                "cannot retire an unaccepted operation",
            ));
        }
        if floor <= anchor.retired_sequence_floor {
            return Ok(());
        }
        let mut next = registry.as_ref().clone();
        next.participants
            .get_mut(&domain)
            .unwrap()
            .retired_sequence_floor = floor;
        self.publish(next)
    }

    /// Called only after platform proof covers the predecessor and EVERY native
    /// permission, and SQLite confirms every paired manager has reconciled empty.
    /// Publishing the new epoch is the final step; a crash before it stays gated.
    pub(crate) fn complete_machine_reset(
        &mut self,
        expected: &crate::AuthoritySnapshot,
        store: Uuid,
        generation: Uuid,
        process: ProcessIdentity,
    ) -> std::io::Result<()> {
        if &self.snapshot() != expected {
            return Err(std::io::Error::other(
                "recovery inventory changed; retry its OS proof",
            ));
        }
        let State::Ready(registry) = &self.state else {
            return Err(std::io::Error::other("authority history unavailable"));
        };
        let history = registry
            .coordinator
            .as_ref()
            .filter(|h| h.pending_reset.is_some())
            .ok_or_else(|| std::io::Error::other("no pending reset"))?;
        if registry.native_coverage_store != Some(history.store_uuid)
            || !registry.machine_permissions.is_empty()
            || registry.pending_machine_commit.is_some()
            || registry.pending_domain_retirement.is_some()
            || registry.holds.values().any(|h| !h.released)
            || registry.participants.values().any(|a| !a.committed)
        {
            return Err(std::io::Error::other(
                "reset still has uncovered obligations",
            ));
        }
        let mut next = registry.as_ref().clone();
        // Retain the exact completed inventory and platform identities in a
        // durable audit file before retiring any native permissions.
        let audit = self.directory.join(format!(
            "reset-{}-{}.json",
            history.pending_reset.as_ref().unwrap().reset_id,
            crate::machine::payload_hash(expected)?
        ));
        let bytes = serde_json::to_vec_pretty(expected)?;
        if audit.exists() {
            if std::fs::read(&audit)? != bytes {
                return Err(std::io::Error::other("reset audit inventory differs"));
            }
        } else {
            atomic_write(&audit, &bytes)?;
        }
        next.anchor_epoch = Some(registry.anchor_epoch.unwrap_or(registry.epoch));
        next.epoch = Uuid::now_v7();
        // Every old allocation key is now fenced by the rotated epoch.
        next.retired_allocations.clear();
        next.native_permissions.clear();
        next.native_coverage_store = Some(store);
        next.coordinator = Some(crate::machine::CoordinatorHistory {
            store_uuid: store,
            daemon_generation: generation,
            process_identity: process,
            pending_reset: None,
        });
        for anchor in next.participants.values_mut() {
            anchor.session = None;
        }
        self.publish(next)
    }

    pub(crate) fn prepare_pairing(
        &mut self,
        registration: crate::machine::PairingRegistration,
    ) -> std::io::Result<()> {
        self.check_retirement_pairing(&registration)?;
        let State::Ready(registry) = &self.state else {
            return Err(std::io::Error::other(
                "pairing requires continuous authority history",
            ));
        };
        let id = registration.installation.domain_id;
        if let Some(old) = registry.participants.get(&id) {
            if old.registration == registration {
                return Ok(());
            }
            return Err(std::io::Error::other(
                "domain already belongs to another pairing payload",
            ));
        }
        if registry.participants.len() >= crate::machine::MAX_DOMAINS - 2 {
            return Err(std::io::Error::other("machine domain limit reached"));
        }
        if registry.participants.values().any(|p| {
            p.registration.installation.installation_nonce
                == registration.installation.installation_nonce
        }) {
            return Err(std::io::Error::other(
                "installation nonce already owns a domain",
            ));
        }
        let mut next = registry.as_ref().clone();
        next.participants.insert(
            id,
            ParticipantAnchor {
                registration,
                committed: false,
                next_connection_epoch: 0,
                session: None,
                accepted_sequence: 0,
                retired_sequence_floor: 0,
            },
        );
        self.publish(next)
    }

    pub(crate) fn commit_pairing(&mut self, id: crate::ExecutionDomainId) -> std::io::Result<()> {
        let State::Ready(registry) = &self.state else {
            return Err(std::io::Error::other("pairing authority unavailable"));
        };
        let mut next = registry.as_ref().clone();
        let anchor = next
            .participants
            .get_mut(&id)
            .ok_or_else(|| std::io::Error::other("pairing anchor missing"))?;
        if anchor.committed {
            return Ok(());
        }
        anchor.committed = true;
        self.publish(next)
    }

    /// Called under the store singleton lock, before deleting any SQLite file.
    /// Existing unknown history is already closed; never synthesize continuity.
    pub(crate) fn before_store_reset(
        directory: PathBuf,
        host: Option<&HostId>,
    ) -> std::io::Result<()> {
        let Some(host) = host else {
            return Ok(());
        };
        let mut authority = Self::open(directory, host.clone());
        authority.record_reset("coordinator SQLite history is being reset")
    }

    pub(crate) fn record_reset(&mut self, reason: &str) -> std::io::Result<()> {
        let State::Ready(registry) = &self.state else {
            return Ok(());
        };
        let Some(history) = &registry.coordinator else {
            return Ok(());
        };
        if history.pending_reset.is_some() {
            return Ok(());
        }
        let mut next = registry.as_ref().clone();
        next.coordinator.as_mut().unwrap().pending_reset = Some(crate::machine::ResetGate {
            reset_id: Uuid::now_v7(),
            displaced_store_uuid: history.store_uuid,
            reason: reason.into(),
        });
        self.publish(next)
    }

    /// Retain predecessor identity while a reset is unresolved. A new daemon or
    /// store never replaces the identity needed to inspect the old obligations.
    pub(crate) fn bind_coordinator(
        &mut self,
        store_uuid: Uuid,
        daemon_generation: Uuid,
        process_identity: ProcessIdentity,
    ) -> std::io::Result<()> {
        let State::Ready(registry) = &self.state else {
            return Ok(());
        };
        if let Some(history) = &registry.coordinator {
            if history.pending_reset.is_some() {
                return Ok(());
            }
            if history.store_uuid != store_uuid {
                return self.record_reset(
                    "coordinator store UUID differs from its durable authority anchor",
                );
            }
        }
        let mut next = registry.as_ref().clone();
        next.coordinator = Some(crate::machine::CoordinatorHistory {
            store_uuid,
            daemon_generation,
            process_identity,
            pending_reset: None,
        });
        self.publish(next)
    }

    /// Missing history is closed, including when both the anchor and registry are
    /// gone. Only an explicit owner initialization can declare a new empty domain.
    pub(crate) fn open(directory: PathBuf, host: HostId) -> Self {
        let state = match Self::load(&directory, &host) {
            Ok(Some(registry)) => State::Ready(Box::new(registry)),
            Ok(None) => State::Uninitialized,
            Err(error) => State::Unknown(error.to_string()),
        };
        let mut authority = Self {
            directory,
            host,
            state,
        };
        // A continuous alpha.15 registry may acquire stable topology identities.
        // No obligation is cleared. Failed publication makes the authority Unknown.
        if let State::Ready(registry) = &authority.state {
            if registry.domains.is_none() {
                let mut upgraded = registry.as_ref().clone();
                upgraded.domains = Some(new_domains());
                if let Err(error) = authority.publish(upgraded) {
                    authority.state =
                        State::Unknown(format!("publishing native domain identities: {error}"));
                }
            }
        }
        authority.prune_machine_blobs();
        authority
    }

    fn load(directory: &Path, host: &HostId) -> std::io::Result<Option<Registry>> {
        let anchor = directory.join("anchor.json");
        let journal = directory.join("registry.json");
        if !anchor.try_exists()? && !journal.try_exists()? {
            return Ok(None);
        }
        let anchor: (u32, Uuid, HostId) = read_bounded(&anchor)?;
        let envelope: Envelope = read_bounded(&journal)?;
        let expected = format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&envelope.payload)?)
        );
        if expected != envelope.sha256 {
            return Err(std::io::Error::other("authority history checksum mismatch"));
        }
        let mut registry = envelope.payload;
        if let Some(blob) = &registry.pending_machine_blob {
            if blob.operation_id.is_nil()
                || blob.sha256.len() != 64
                || !blob.sha256.bytes().all(|b| b.is_ascii_hexdigit())
                || registry.pending_machine_commit.is_some()
            {
                return Err(std::io::Error::other(
                    "invalid external machine commit reference",
                ));
            }
            let intent: MachineCommitIntent =
                read_with_limit(&blob.path(directory), MAX_MACHINE_JOURNAL_BYTES)?;
            if intent.request.operation_id != blob.operation_id
                || crate::machine::payload_hash(&intent)? != blob.sha256
            {
                return Err(std::io::Error::other(
                    "external machine commit checksum/identity mismatch",
                ));
            }
            registry.pending_machine_commit = Some(intent);
        }
        if anchor
            != (
                1,
                registry.anchor_epoch.unwrap_or(registry.epoch),
                host.clone(),
            )
            || registry.version != 1
            || &registry.host != host
            || registry.domains.as_ref().is_some_and(|domains| {
                domains.machine_id.is_nil()
                    || domains.machine_scope.0.is_nil()
                    || domains.native_domain.0.is_nil()
                    || domains.machine_scope == domains.native_domain
                    || domains.machine_id == domains.machine_scope.0
                    || domains.machine_id == domains.native_domain.0
            })
            || registry.holds.len() > MAX_HOLDS
            || registry.participants.len() > crate::machine::MAX_DOMAINS - 2
            || registry.participants.iter().any(|(id, p)| {
                id != &p.registration.installation.domain_id
                    || id.0.is_nil()
                    || p.registration.installation.installation_nonce.is_nil()
                    || p.registration.manager_store_uuid.is_nil()
            })
            || registry.coordinator.as_ref().is_some_and(|history| {
                history.store_uuid.is_nil()
                    || history.daemon_generation.is_nil()
                    || history.pending_reset.as_ref().is_some_and(|gate| {
                        gate.reset_id.is_nil()
                            || gate.displaced_store_uuid != history.store_uuid
                            || !valid_reason(&gate.reason)
                    })
            })
            || registry.holds.iter().any(|(id, hold)| {
                id != &hold.id
                    || !valid_reason(&hold.reason)
                    || hold.released != hold.release_reason.is_some()
                    || hold.released != hold.released_by.is_some()
                    || hold
                        .release_reason
                        .as_ref()
                        .is_some_and(|reason| !valid_reason(reason))
                    || hold.bootstrap.as_ref().is_some_and(|binding| {
                        binding.work.operation_id != *id
                            || crate::bootstrap::validate_binding(binding).is_err()
                    })
                    || hold.cleanup_proof.as_ref().is_some_and(|proof| {
                        !hold.released
                            || hold.bootstrap.as_ref().is_none_or(|binding| {
                                crate::bootstrap::validate_proof(binding, proof).is_err()
                            })
                    })
            })
        {
            return Err(std::io::Error::other(
                "authority identity/schema/history mismatch",
            ));
        }
        Self::validate_retirements(directory, &registry)?;
        Ok(Some(registry))
    }

    pub(crate) fn snapshot(&self) -> AuthoritySnapshot {
        match &self.state {
            State::Uninitialized => AuthoritySnapshot {
                epoch: None,
                domains: None,
                coordinator: None,
                machine_obligations: vec![],
                native_obligations: vec![],
                native_coverage_store: None,
                pending_machine_operation: None,
                retired_domains: vec![],
                storage_budget: None,
                blocker: Some("authority_uninitialized".into()),
                detail: Some(
                    "Explicit owner initialization with no outstanding work is required".into(),
                ),
                holds: vec![],
            },
            State::Unknown(detail) => AuthoritySnapshot {
                epoch: None,
                domains: None,
                coordinator: None,
                machine_obligations: vec![],
                native_obligations: vec![],
                native_coverage_store: None,
                pending_machine_operation: None,
                retired_domains: vec![],
                storage_budget: None,
                blocker: Some("authority_history_unknown".into()),
                detail: Some(detail.clone()),
                holds: vec![],
            },
            State::Ready(registry) => {
                let blocker = self.admission_blocker();
                let budget = storage_budget(registry).ok();
                let mut detail = blocker.as_ref().map(|(_, detail)| detail.clone());
                if budget.as_ref().is_some_and(|b| {
                    b.registry_bytes.saturating_add(b.reserved_recovery_bytes)
                        > b.registry_limit_bytes
                }) {
                    let warning = "legacy authority exceeds reserved cleanup byte budget; preserve its files and inspect storage_budget before attempting releases";
                    detail = Some(match detail {
                        Some(detail) => format!("{detail}; {warning}"),
                        None => warning.into(),
                    });
                }
                AuthoritySnapshot {
                    storage_budget: budget,
                    epoch: Some(registry.epoch),
                    domains: registry.domains.clone(),
                    coordinator: registry.coordinator.clone(),
                    machine_obligations: registry.machine_permissions.values().cloned().collect(),
                    native_obligations: registry.native_permissions.values().cloned().collect(),
                    native_coverage_store: registry.native_coverage_store,
                    pending_machine_operation: registry
                        .pending_machine_commit
                        .as_ref()
                        .map(|intent| intent.request.operation_id)
                        .or_else(|| {
                            registry
                                .pending_domain_retirement
                                .as_ref()
                                .map(|p| p.operation_id)
                        }),
                    retired_domains: registry
                        .retired_domains
                        .iter()
                        .filter(|(domain, _)| {
                            registry
                                .pending_domain_retirement
                                .as_ref()
                                .is_none_or(|p| p.domain_id != **domain)
                        })
                        .map(|(_, r)| r.receipt.clone())
                        .collect(),
                    blocker: blocker.as_ref().map(|(code, _)| code.clone()),
                    detail,
                    holds: registry.holds.values().cloned().collect(),
                }
            }
        }
    }

    pub(crate) fn admission_blocker(&self) -> Option<(String, String)> {
        match &self.state {
            State::Uninitialized => Some((
                "authority_uninitialized".into(),
                "Explicit owner initialization with no outstanding work is required".into(),
            )),
            State::Unknown(detail) => Some(("authority_history_unknown".into(), detail.clone())),
            State::Ready(registry) if registry.pending_domain_retirement.is_some()=>Some(("authority_retirement_pending".into(),"An owner-audited domain retirement must finish its durable SQL commit".into())),
            State::Ready(registry) if registry.pending_machine_commit.is_some() => Some((
                "authority_commit_pending".into(),
                "A potentially used machine permission requires its durable SQLite commit to be reconciled".into(),
            )),
            State::Ready(registry) if registry.participants.values().any(|p| !p.committed) => Some((
                "authority_pairing_incomplete".into(),
                "Pairing intent is durable; replay the same owner registration to reconcile its SQLite commit".into(),
            )),
            State::Ready(registry) if registry.coordinator.as_ref().is_some_and(|history| history.pending_reset.is_some()) => Some((
                "authority_reconciliation_required".into(),
                "Coordinator history was reset; every potentially used allocation requires reconciliation".into(),
            )),
            State::Ready(registry) if registry.holds.values().any(|hold| !hold.released) => Some((
                "authority_held".into(),
                "Durable admission hold; process exit and store reset do not release it".into(),
            )),
            State::Ready(_) => None,
        }
    }

    pub(crate) fn initialize(&mut self) -> std::io::Result<AuthoritySnapshot> {
        match self.state {
            State::Ready(_) => return Ok(self.snapshot()),
            State::Unknown(_) => {
                return Err(std::io::Error::other(
                    "Cannot initialize over uncertain authority history",
                ));
            }
            State::Uninitialized => {}
        }
        // Mark uncertain before any file operation: an error can occur after a
        // durable write. A later request must not reinterpret an error as absence.
        self.state = State::Unknown("authority initialization incomplete".into());
        std::fs::create_dir_all(&self.directory)?;
        crate::filesystem::require_durable_local_filesystem(&self.directory)?;
        let registry = Registry {
            version: 1,
            anchor_epoch: None,
            epoch: Uuid::now_v7(),
            host: self.host.clone(),
            holds: BTreeMap::new(),
            domains: Some(new_domains()),
            coordinator: None,
            participants: BTreeMap::new(),
            machine_permissions: BTreeMap::new(),
            native_permissions: BTreeMap::new(),
            native_coverage_store: None,
            retired_allocations: BTreeSet::new(),
            pending_machine_commit: None,
            pending_machine_blob: None,
            retired_domains: BTreeMap::new(),
            pending_domain_retirement: None,
        };
        let anchor = self.directory.join("anchor.json");
        // An existing anchor cannot be silently replaced, even after a partial init.
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&anchor)?;
        serde_json::to_writer(&mut file, &(1, registry.epoch, self.host.clone()))?;
        file.sync_all()?;
        self.publish(registry)?;
        Ok(self.snapshot())
    }

    pub(crate) fn hold(
        &mut self,
        id: Uuid,
        reason: String,
        requester: ProcessIdentity,
    ) -> std::io::Result<AuthoritySnapshot> {
        if !valid_reason(&reason) {
            return Err(std::io::Error::other(
                "Hold reason must contain 1..1024 bytes without NUL",
            ));
        }
        let State::Ready(registry) = &self.state else {
            return Err(std::io::Error::other(
                "authority history is not initialized and continuous",
            ));
        };
        if let Some(previous) = registry.holds.get(&id) {
            if previous.reason != reason {
                return Err(std::io::Error::other(
                    "authority operation ID payload conflict",
                ));
            }
            // A retired operation is a tombstone, never permission for another run.
            return Ok(self.snapshot());
        }
        if registry.holds.len() >= MAX_HOLDS {
            return Err(std::io::Error::other(
                "authority history limit reached; no obligation was evicted",
            ));
        }
        let mut next = registry.as_ref().clone();
        next.holds.insert(
            id,
            AuthorityHold {
                id,
                reason,
                requester,
                released: false,
                release_reason: None,
                released_by: None,
                bootstrap: None,
                cleanup_proof: None,
            },
        );
        self.publish(next)?;
        Ok(self.snapshot())
    }

    /// Explicit audited operator release. It is deliberately not a runtime empty
    /// proof and must not be used by an automatic WSL wrapper.
    pub(crate) fn force_release(
        &mut self,
        id: Uuid,
        reason: String,
        requester: ProcessIdentity,
    ) -> std::io::Result<AuthoritySnapshot> {
        if !valid_reason(&reason) {
            return Err(std::io::Error::other(
                "Risk acceptance reason must contain 1..1024 bytes without NUL",
            ));
        }
        let State::Ready(registry) = &self.state else {
            return Err(std::io::Error::other("authority history is unknown"));
        };
        let mut next = registry.as_ref().clone();
        let hold = next
            .holds
            .get_mut(&id)
            .ok_or_else(|| std::io::Error::other("unknown authority hold"))?;
        if hold.released {
            if hold.release_reason.as_ref() != Some(&reason) {
                return Err(std::io::Error::other("authority release payload conflict"));
            }
            return Ok(self.snapshot());
        }
        hold.released = true;
        hold.release_reason = Some(reason);
        hold.released_by = Some(requester);
        self.publish(next)?;
        Ok(self.snapshot())
    }

    pub(crate) fn arm_bootstrap(
        &mut self,
        binding: crate::BootstrapBinding,
        requester: ProcessIdentity,
    ) -> std::io::Result<AuthoritySnapshot> {
        let State::Ready(registry) = &self.state else {
            return Err(std::io::Error::other("authority history is not continuous"));
        };
        let id = binding.work.operation_id;
        if let Some(previous) = registry.holds.get(&id) {
            if previous.bootstrap.as_ref() != Some(&binding) || previous.requester != requester {
                return Err(std::io::Error::other(
                    "bootstrap operation payload/owner conflict",
                ));
            }
            return Ok(self.snapshot());
        }
        if registry.holds.values().any(|hold| !hold.released) || registry.holds.len() >= MAX_HOLDS {
            return Err(std::io::Error::other(
                "another obligation or history limit blocks bootstrap",
            ));
        }
        let mut next = registry.as_ref().clone();
        next.holds.insert(
            id,
            AuthorityHold {
                id,
                reason: "WSL bootstrap obligation".into(),
                requester,
                released: false,
                release_reason: None,
                released_by: None,
                bootstrap: Some(binding),
                cleanup_proof: None,
            },
        );
        self.publish(next)?;
        Ok(self.snapshot())
    }

    pub(crate) fn seal_bootstrap(
        &mut self,
        proof: crate::BootstrapProof,
        requester: ProcessIdentity,
    ) -> std::io::Result<AuthoritySnapshot> {
        let State::Ready(registry) = &self.state else {
            return Err(std::io::Error::other("authority history is not continuous"));
        };
        let mut next = registry.as_ref().clone();
        let hold = next
            .holds
            .get_mut(&proof.operation_id)
            .ok_or_else(|| std::io::Error::other("bootstrap obligation not found"))?;
        let binding = hold
            .bootstrap
            .as_ref()
            .ok_or_else(|| std::io::Error::other("hold is not a bootstrap obligation"))?;
        crate::bootstrap::validate_proof(binding, &proof)?;
        if hold.released {
            if hold.cleanup_proof.as_ref() != Some(&proof) {
                return Err(std::io::Error::other(
                    "bootstrap release replay conflicts with tombstone",
                ));
            }
            return Ok(self.snapshot());
        }
        hold.released = true;
        hold.release_reason = Some("trusted Linux supervisor sealed its empty cgroup".into());
        hold.released_by = Some(requester);
        hold.cleanup_proof = Some(proof);
        self.publish(next)?;
        Ok(self.snapshot())
    }

    fn prune_machine_blobs(&self) {
        let State::Ready(registry) = &self.state else {
            return;
        };
        let keep = registry
            .pending_machine_blob
            .as_ref()
            .map(|b| b.path(&self.directory));
        let Ok(entries) = std::fs::read_dir(&self.directory) else {
            return;
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name
                .to_str()
                .and_then(|n| n.strip_prefix("machine-commit-"))
                .and_then(|n| n.strip_suffix(".json"))
            else {
                continue;
            };
            let valid = name.len() == 101
                && name.get(..36).is_some_and(|s| Uuid::parse_str(s).is_ok())
                && name.get(36..37) == Some("-")
                && name
                    .get(37..)
                    .is_some_and(|s| s.bytes().all(|b| b.is_ascii_hexdigit()));
            if valid
                && keep.as_ref() != Some(&entry.path())
                && entry.file_type().is_ok_and(|t| t.is_file())
            {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }

    fn check_publication(&self, registry: &Registry) -> std::io::Result<EncodedRegistry> {
        let encoded = encode_registry(registry)?;
        if let State::Ready(old) = &self.state {
            let future = encoded.0.len() as u64 + reserved_recovery_bytes(registry);
            let prior_future = encode_registry(old)?.0.len() as u64 + reserved_recovery_bytes(old);
            if future > MAX_REGISTRY_BYTES && future > prior_future {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "authority metadata growth would consume reserved cleanup space",
                ));
            }
            if admission_bytes(registry)? > admission_bytes(old)?
                && (encoded.0.len() as u64 > MAX_REGISTRY_BYTES - REGISTRY_COMMIT_HEADROOM
                    || future > MAX_REGISTRY_BYTES - 256 * 1024)
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "authority admission budget reached; reserved space is for cleanup and session recovery",
                ));
            }
        }
        Ok(encoded)
    }

    fn publish(&mut self, mut registry: Registry) -> std::io::Result<()> {
        let (bytes, blob) = self.check_publication(&registry)?;
        let previous = match &self.state {
            State::Ready(old) => old.pending_machine_blob.clone(),
            _ => None,
        };
        registry.pending_machine_blob = blob.as_ref().map(|(reference, _)| reference.clone());
        if let Some((reference, contents)) = &blob {
            let path = reference.path(&self.directory);
            if path.exists() {
                if std::fs::read(&path)? != *contents {
                    return Err(std::io::Error::other(
                        "immutable machine commit blob changed",
                    ));
                }
            } else {
                atomic_write(&path, contents)?;
            }
        }
        self.state = State::Unknown("authority publication outcome is uncertain".into());
        atomic_write(&self.directory.join("registry.json"), &bytes)?;
        self.state = State::Ready(Box::new(registry));
        // The new pointer is durable before retiring an old blob. An orphan
        // after a crash is harmless; it is never treated as an active commit.
        if let Some(old) = previous {
            if blob
                .as_ref()
                .is_none_or(|(new, _)| old.sha256 != new.sha256)
            {
                let _ = std::fs::remove_file(old.path(&self.directory));
            }
        }
        self.prune_machine_blobs();
        Ok(())
    }
}

fn admission_bytes(registry: &Registry) -> std::io::Result<usize> {
    Ok(serde_json::to_vec(&(
        registry
            .holds
            .values()
            .filter(|h| !h.released)
            .collect::<Vec<_>>(),
        registry
            .participants
            .values()
            .map(|p| &p.registration)
            .collect::<Vec<_>>(),
        &registry.machine_permissions,
        &registry.native_permissions,
    ))?
    .len())
}

type EncodedRegistry = (Vec<u8>, Option<(MachineBlob, Vec<u8>)>);
fn encode_registry(registry: &Registry) -> std::io::Result<EncodedRegistry> {
    let mut persisted = registry.clone();
    let blob = if let Some(intent) = persisted.pending_machine_commit.take() {
        let bytes = serde_json::to_vec(&intent)?;
        if bytes.len() as u64 > MAX_MACHINE_JOURNAL_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "machine commit exceeds durable journal byte budget",
            ));
        }
        let reference = MachineBlob {
            operation_id: intent.request.operation_id,
            sha256: format!("{:x}", Sha256::digest(&bytes)),
        };
        persisted.pending_machine_blob = Some(reference.clone());
        Some((reference, bytes))
    } else {
        persisted.pending_machine_blob = None;
        None
    };
    let bytes = serde_json::to_vec(&Envelope {
        sha256: crate::machine::payload_hash(&persisted)?,
        payload: persisted,
    })?;
    let limit = MAX_REGISTRY_BYTES;
    if bytes.len() as u64 > limit {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "authority registry exceeds durable byte budget; retire existing obligations",
        ));
    }
    Ok((bytes, blob))
}

fn valid_reason(reason: &str) -> bool {
    !reason.is_empty() && reason.len() <= 1024 && !reason.contains('\0')
}

fn read_bounded<T: serde::de::DeserializeOwned>(path: &Path) -> std::io::Result<T> {
    read_with_limit(path, MAX_REGISTRY_BYTES)
}
fn read_with_limit<T: serde::de::DeserializeOwned>(path: &Path, limit: u64) -> std::io::Result<T> {
    let mut bytes = Vec::new();
    File::open(path)
        .and_then(|file| file.take(limit + 1).read_to_end(&mut bytes))
        .map_err(|e| std::io::Error::new(e.kind(), format!("{}: {e}", path.display())))?;
    if bytes.len() as u64 > limit {
        return Err(std::io::Error::other(format!(
            "{}: authority record exceeds byte limit",
            path.display()
        )));
    }
    serde_json::from_slice(&bytes)
        .map_err(|e| std::io::Error::other(format!("{}: {e}", path.display())))
}

fn atomic_write(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let temporary = path.with_extension(format!("{}.pending", Uuid::now_v7()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        replace(&temporary, path)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

#[cfg(windows)]
fn replace(source: &Path, destination: &Path) -> std::io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
    };
    let source: Vec<u16> = source.as_os_str().encode_wide().chain(Some(0)).collect();
    let destination: Vec<u16> = destination
        .as_os_str()
        .encode_wide()
        .chain(Some(0))
        .collect();
    // SAFETY: both paths are owned NUL-terminated UTF-16 buffers for this call.
    if unsafe {
        MoveFileExW(
            source.as_ptr(),
            destination.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    } == 0
    {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(not(windows))]
fn replace(source: &Path, destination: &Path) -> std::io::Result<()> {
    std::fs::rename(source, destination)?;
    File::open(
        destination
            .parent()
            .ok_or_else(|| std::io::Error::other("no authority parent"))?,
    )?
    .sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn host() -> HostId {
        HostId("authority-test-host".into())
    }
    fn requester() -> ProcessIdentity {
        ProcessIdentity::Windows {
            host_id: host(),
            boot_id: crate::BootId("test-boot".into()),
            pid: 42,
            creation_filetime_100ns: 123,
        }
    }

    #[test]
    fn saturated_registry_still_journals_large_release_and_detects_missing_blob() {
        use crate::machine::*;
        let root = crate::test_support::durable_tempdir().unwrap();
        let mut authority = Authority::open(root.path().to_owned(), host());
        authority.initialize().unwrap();
        let store = Uuid::now_v7();
        let session = SessionIdentity {
            machine_id: authority.snapshot().domains.unwrap().machine_id,
            authority_epoch: authority.snapshot().epoch.unwrap(),
            domain_id: crate::ExecutionDomainId(Uuid::now_v7()),
            manager_store_uuid: store,
            executor_incarnation: Uuid::now_v7(),
            connection_epoch: 1,
        };
        let registration = PairingRegistration {
            installation: InstallationIdentity {
                installation_nonce: Uuid::now_v7(),
                domain_id: session.domain_id,
                owner_uid: 1000,
                runtime_registration: "journal-budget-fixture".into(),
                role: ParticipantRole::Executor,
            },
            manager_store_uuid: store,
            parent_domain: authority.snapshot().domains.unwrap().machine_scope,
            budgets: BTreeMap::new(),
            aliases: BTreeMap::new(),
            secret: [7; 32],
        };
        let State::Ready(base) = &authority.state else {
            panic!("initialized authority");
        };
        let mut registry = base.as_ref().clone();
        registry.participants.insert(
            session.domain_id,
            ParticipantAnchor {
                registration,
                committed: true,
                next_connection_epoch: 1,
                session: Some(session.clone()),
                accepted_sequence: 0,
                retired_sequence_floor: 0,
            },
        );
        let mut grants = Vec::new();
        loop {
            let key = AllocationKey {
                machine_id: session.machine_id,
                authority_epoch: session.authority_epoch,
                domain_id: session.domain_id,
                manager_store_uuid: store,
                lease_id: Uuid::now_v7(),
            };
            let tickets = (0..10)
                .map(|i| InvocationIntent {
                    invocation_id: format!("{store}~{}", Uuid::now_v7()).parse().unwrap(),
                    containment_id: format!("{store}~{}", Uuid::now_v7()).parse().unwrap(),
                    role: crate::InvocationRole::Postcondition,
                    role_index: i,
                    release_sequence: u64::from(i) + 1,
                    executable_sha256: "a".repeat(64),
                    boundary_sha256: "b".repeat(64),
                    readiness_challenge: Uuid::now_v7(),
                    previous_cleanup: None,
                })
                .collect();
            let grant = GrantSnapshot {
                risk_clearance: None,
                uncertainty_reason: None,
                queue_accepted_unix_millis: 1,
                queue_sequence: 1,
                grant_id: format!("{store}~{}", Uuid::now_v7()).parse().unwrap(),
                candidate: Candidate {
                    key: key.clone(),
                    owner: AllocationOwner::Work {
                        job_id: format!("{store}~{}", Uuid::now_v7()).parse().unwrap(),
                        attempt_id: format!("{store}~{}", Uuid::now_v7()).parse().unwrap(),
                    },
                    revision: 1,
                    priority: 0,
                    claims: Claims {
                        scalars: [("я".repeat(1000), 1)].into(),
                        ..Claims::default()
                    },
                    configuration_sha256: "a".repeat(64),
                    observed: None,
                    quiet: None,
                },
                offer_nonce: Uuid::now_v7(),
                state: GrantState::Armed,
                offered_unix_millis: 1,
                offer_deadline_unix_millis: 2,
                armed_unix_millis: Some(1),
                released_unix_millis: None,
                tickets,
                sealed_release: None,
            };
            registry
                .machine_permissions
                .insert(grant.grant_id, grant.clone());
            if encode_registry(&registry).unwrap().0.len() as u64
                > MAX_REGISTRY_BYTES - REGISTRY_COMMIT_HEADROOM - 1024
            {
                registry.machine_permissions.remove(&grant.grant_id);
                break;
            }
            grants.push(grant);
        }
        // The same saturated inventory must also admit owner risk retirement,
        // without consuming the normal release test's separate authority.
        let retirement_root = crate::test_support::durable_tempdir().unwrap();
        let mut retirement_authority = Authority::open(retirement_root.path().into(), host());
        retirement_authority.initialize().unwrap();
        retirement_authority.publish(registry.clone()).unwrap();
        let preview = retirement_authority
            .domain_clearance_preview(session.domain_id)
            .unwrap();
        let retirement = retirement_authority
            .prepare_domain_retirement(
                DomainRetirementRequest {
                    operation_id: Uuid::now_v7(),
                    domain_id: session.domain_id,
                    expected_inventory_sha256: preview.sha256,
                    reason: "\u{1}".repeat(1024),
                    accept_risk: true,
                },
                requester(),
                "S-1-fixture-owner".into(),
                3,
            )
            .unwrap();
        assert_eq!(
            retirement_authority.snapshot().machine_obligations.len(),
            grants.len()
        );
        retirement_authority
            .finish_domain_retirement(retirement.request.operation_id)
            .unwrap();
        assert!(
            retirement_authority
                .snapshot()
                .machine_obligations
                .is_empty()
        );
        assert_eq!(retirement_authority.snapshot().retired_domains.len(), 1);
        authority.publish(registry).unwrap();
        let mut extra = grants[0].clone();
        extra.grant_id = format!("{store}~{}", Uuid::now_v7()).parse().unwrap();
        extra.candidate.key.lease_id = Uuid::now_v7();
        extra
            .candidate
            .claims
            .scalars
            .insert("x".repeat(100_000), 1);
        let request = Request::new(
            session.clone(),
            1,
            Uuid::now_v7(),
            Command::Arm {
                key: extra.candidate.key.clone(),
                offer_nonce: extra.offer_nonce,
            },
        )
        .unwrap();
        assert_eq!(
            authority
                .check_machine_commit(MachineCommitIntent {
                    request,
                    outcome: Outcome::Grant {
                        grant: Box::new(extra.clone())
                    },
                    grants: vec![extra]
                })
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::InvalidInput
        );
        for grant in &mut grants {
            grant.state = GrantState::Released;
            grant.sealed_release = Some(SealedRelease {
                key: grant.candidate.key.clone(),
                offer_nonce: grant.offer_nonce,
                sealed_sequence: 1,
                tickets: grant
                    .tickets
                    .iter()
                    .map(|t| TicketCleanup {
                        invocation_id: t.invocation_id,
                        release_sequence: t.release_sequence,
                        boundary_sha256: t.boundary_sha256.clone(),
                        proof_sha256: "c".repeat(64),
                        user_code_released: true,
                    })
                    .collect(),
            });
        }
        let request = Request::new(
            session,
            1,
            Uuid::now_v7(),
            Command::ReconcileCommit {
                snapshot_id: Uuid::now_v7(),
            },
        )
        .unwrap();
        let operation = request.operation_id;
        let intent = MachineCommitIntent {
            request,
            outcome: Outcome::Reconciled {
                end_sequence: 0,
                released: grants.iter().map(|g| g.candidate.key.clone()).collect(),
            },
            grants,
        };
        assert!(
            serde_json::to_vec(&intent).unwrap().len() as u64 > MAX_REGISTRY_BYTES,
            "control must exceed the old shared file limit"
        );
        authority.check_machine_commit(intent.clone()).unwrap();
        authority.prepare_machine_commit(intent).unwrap();
        let State::Ready(registry) = &authority.state else {
            panic!("prepared authority");
        };
        let blob = registry
            .pending_machine_blob
            .as_ref()
            .unwrap()
            .path(root.path());
        assert!(std::fs::metadata(&blob).unwrap().len() > MAX_REGISTRY_BYTES);
        assert!(
            std::fs::metadata(root.path().join("registry.json"))
                .unwrap()
                .len()
                < MAX_REGISTRY_BYTES
        );
        let bytes = std::fs::read(&blob).unwrap();
        std::fs::remove_file(&blob).unwrap();
        assert_eq!(
            Authority::open(root.path().to_owned(), host())
                .snapshot()
                .blocker
                .as_deref(),
            Some("authority_history_unknown")
        );
        atomic_write(&blob, &bytes).unwrap();
        let mut recovered = Authority::open(root.path().to_owned(), host());
        assert!(recovered.pending_machine_commit().unwrap().is_some());
        recovered.finish_machine_commit(operation).unwrap();
        assert!(recovered.snapshot().machine_obligations.is_empty());
        assert!(!blob.exists());
        assert!(
            Authority::open(root.path().to_owned(), host())
                .pending_machine_commit()
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn legacy_hold_inventory_reports_missing_cleanup_reservation_before_release() {
        let root = crate::test_support::durable_tempdir().unwrap();
        let mut authority = Authority::open(root.path().to_owned(), host());
        authority.initialize().unwrap();
        authority
            .hold(Uuid::now_v7(), "legacy work".into(), requester())
            .unwrap();
        let State::Ready(registry) = &mut authority.state else {
            panic!()
        };
        let sample = registry.holds.values().next().unwrap().clone();
        // Simulate a checksummed registry from before reservations were enforced.
        for _ in 0..70 {
            let mut hold = sample.clone();
            hold.id = Uuid::now_v7();
            registry.holds.insert(hold.id, hold);
        }
        let bytes = encode_registry(registry).unwrap().0;
        std::fs::write(root.path().join("registry.json"), bytes).unwrap();
        let reopened = Authority::open(root.path().to_owned(), host());
        let snapshot = reopened.snapshot();
        assert_eq!(snapshot.holds.len(), 71);
        assert!(
            snapshot
                .detail
                .unwrap()
                .contains("legacy authority exceeds reserved cleanup")
        );
        let budget = snapshot.storage_budget.unwrap();
        assert!(
            budget.registry_bytes + budget.reserved_recovery_bytes > budget.registry_limit_bytes
        );
    }

    #[test]
    fn missing_and_lost_history_never_implies_free_capacity() {
        let root = crate::test_support::durable_tempdir().unwrap();
        let mut authority = Authority::open(root.path().to_owned(), host());
        assert!(authority.snapshot().blocker.is_some());
        authority.initialize().unwrap();
        assert!(authority.snapshot().blocker.is_none());
        authority
            .hold(Uuid::now_v7(), "external work".into(), requester())
            .unwrap();
        drop(authority);
        let reopened = Authority::open(root.path().to_owned(), host());
        assert_eq!(
            reopened.snapshot().blocker.as_deref(),
            Some("authority_held")
        );
        std::fs::remove_file(root.path().join("registry.json")).unwrap();
        let mut lost = Authority::open(root.path().to_owned(), host());
        assert_eq!(
            lost.snapshot().blocker.as_deref(),
            Some("authority_history_unknown")
        );
        assert!(lost.initialize().is_err());
        std::fs::remove_file(root.path().join("anchor.json")).unwrap();
        assert!(
            Authority::open(root.path().to_owned(), host())
                .snapshot()
                .blocker
                .is_some()
        );
    }

    #[test]
    fn exact_replay_never_rearms_a_retired_hold_and_conflict_rejects() {
        let root = crate::test_support::durable_tempdir().unwrap();
        let mut authority = Authority::open(root.path().to_owned(), host());
        authority.initialize().unwrap();
        let id = Uuid::now_v7();
        authority.hold(id, "work".into(), requester()).unwrap();
        assert!(
            authority
                .hold(id, "different work".into(), requester())
                .is_err()
        );
        authority
            .force_release(id, "test-only risk clearance".into(), requester())
            .unwrap();
        drop(authority);
        let mut reopened = Authority::open(root.path().to_owned(), host());
        let replay = reopened.hold(id, "work".into(), requester()).unwrap();
        assert!(replay.holds[0].released);
        assert!(replay.blocker.is_none());
        let wrong_host = Authority::open(root.path().to_owned(), HostId("different-host".into()));
        assert_eq!(
            wrong_host.snapshot().blocker.as_deref(),
            Some("authority_history_unknown")
        );
    }

    #[test]
    fn syntactically_valid_corruption_and_failed_publication_close_admission() {
        let root = crate::test_support::durable_tempdir().unwrap();
        let mut authority = Authority::open(root.path().to_owned(), host());
        authority.initialize().unwrap();
        let id = Uuid::now_v7();
        authority.hold(id, "work".into(), requester()).unwrap();
        let journal = root.path().join("registry.json");
        let mut record: serde_json::Value = read_bounded(&journal).unwrap();
        record["payload"]["holds"] = serde_json::json!({});
        std::fs::write(&journal, serde_json::to_vec(&record).unwrap()).unwrap();
        let corrupted = Authority::open(root.path().to_owned(), host());
        assert_eq!(
            corrupted.snapshot().blocker.as_deref(),
            Some("authority_history_unknown")
        );

        // Force publication failure after the in-memory state becomes uncertain.
        std::fs::remove_file(&journal).unwrap();
        std::fs::create_dir(&journal).unwrap();
        assert!(
            authority
                .force_release(id, "test release".into(), requester())
                .is_err()
        );
        assert_eq!(
            authority.snapshot().blocker.as_deref(),
            Some("authority_history_unknown")
        );
        assert!(authority.initialize().is_err());
    }
}
