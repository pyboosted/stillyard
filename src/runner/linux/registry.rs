//! Live peer membership and recovery share the same pinned executor history.
use super::cgroup::Boundary;
use super::journal::Journal;
use crate::{InvocationId, ReconciliationResult};
use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

#[derive(Clone, Default)]
pub(crate) struct Registry {
    signer: Arc<OnceLock<crate::identity::attestation::Signer>>,
    journal: Arc<Mutex<Option<Journal>>>,
    active: Arc<Mutex<HashMap<InvocationId, Entry>>>,
}

#[cfg(test)]
thread_local! {
    static AFTER_CLEANUP_SEAL: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

enum Entry {
    Live(Arc<Boundary>),
    Sealed,
    Uninspectable,
}
fn poisoned() -> io::Error {
    io::Error::other("Linux containment registry mutex poisoned")
}

impl Registry {
    pub(crate) fn durable_seals(&self) -> io::Result<Vec<(InvocationId, String, String)>> {
        self.with_journal(|journal| {
            journal
                .records()?
                .values()
                .filter_map(|record| record.seal.as_ref())
                .map(|seal| {
                    Ok((
                        seal.invocation,
                        seal.boundary_sha256.clone(),
                        seal.sha256()?,
                    ))
                })
                .collect()
        })
    }
    pub(crate) fn persist_cleanup(
        &self,
        store: &mut crate::store::Store,
        invocation: InvocationId,
    ) -> crate::store::StoreResult<()> {
        let (boundary, proof) = self.with_journal(|journal| {
            let seal = journal
                .records()?
                .get(&invocation)
                .and_then(|record| record.seal.as_ref())
                .ok_or_else(|| {
                    io::Error::other("reconciled Invocation has no durable executor seal")
                })?;
            Ok((seal.boundary_sha256.clone(), seal.sha256()?))
        })?;
        store.record_attached_cleanup(invocation, &boundary, &proof, None)
    }
    pub(super) fn install(
        &self,
        journal: Journal,
        signer: crate::identity::attestation::Signer,
    ) -> io::Result<()> {
        let mut current = self.journal.lock().map_err(|_| poisoned())?;
        if current.is_some() {
            return Err(io::Error::other(
                "Linux executor history is already installed",
            ));
        }
        let mut entries = self.active.lock().map_err(|_| poisoned())?;
        for (id, record) in journal.records()? {
            entries.insert(
                *id,
                if record.seal.is_some() {
                    Entry::Sealed
                } else {
                    record
                        .boundary
                        .as_ref()
                        .and_then(|identity| Boundary::reopen(identity).ok())
                        .map_or(Entry::Uninspectable, |boundary| {
                            Entry::Live(Arc::new(boundary))
                        })
                },
            );
        }
        self.signer
            .set(signer)
            .map_err(|_| io::Error::other("Linux daemon signer is already installed"))?;
        *current = Some(journal);
        Ok(())
    }

    pub(crate) fn server_context(
        &self,
        parent: crate::ManagedParent,
    ) -> io::Result<crate::identity::attestation::ManagedServer> {
        self.signer
            .get()
            .ok_or_else(|| io::Error::other("Linux daemon signer is unavailable"))?
            .context(parent)
    }
    pub(crate) fn sign_response(
        &self,
        parent: crate::ManagedParent,
        nonce: [u8; 32],
        request_sha256: String,
        response: crate::protocol::Response,
    ) -> io::Result<crate::identity::attestation::Proof> {
        self.signer
            .get()
            .ok_or_else(|| io::Error::other("Linux daemon signer is unavailable"))?
            .sign(parent, nonce, request_sha256, response)
    }

    pub(super) fn with_journal<T>(
        &self,
        action: impl FnOnce(&mut Journal) -> io::Result<T>,
    ) -> io::Result<T> {
        let mut guard = self.journal.lock().map_err(|_| poisoned())?;
        action(
            guard
                .as_mut()
                .ok_or_else(|| io::Error::other("Linux executor history is unavailable"))?,
        )
    }

    pub(super) fn register(&self, id: InvocationId, boundary: Arc<Boundary>) -> io::Result<()> {
        self.with_journal(|journal| {
            let records = journal.records()?;
            let record = records
                .get(&id)
                .ok_or_else(|| io::Error::other("uncommitted containment registration"))?;
            if record.seal.is_some() || record.boundary.as_ref() != Some(&boundary.identity) {
                return Err(io::Error::other(
                    "containment registration disagrees with history",
                ));
            }
            let mut active = self.active.lock().map_err(|_| poisoned())?;
            if active.contains_key(&id) {
                return Err(io::Error::other("duplicate Linux containment registration"));
            }
            active.insert(id, Entry::Live(boundary));
            Ok(())
        })
    }

    pub(crate) fn contains(
        &self,
        id: InvocationId,
        process_handle: usize,
    ) -> io::Result<Option<bool>> {
        // Cleanup removes the kernel boundary before publishing its sealed entry.
        // Serialize membership with that transition; an unrelated child finishing
        // must not make an authenticated parent's next RPC temporarily unverifiable.
        // Authentication can wait up to the cleanup deadline (currently 30 s);
        // increasing that deadline also increases this RPC latency bound.
        let journal = self.journal.lock().map_err(|_| poisoned())?;
        let active = self.active.lock().map_err(|_| poisoned())?;
        match active.get(&id) {
            Some(Entry::Live(boundary)) => boundary.contains_process(process_handle).map(Some),
            Some(Entry::Sealed) => Ok(Some(false)),
            Some(Entry::Uninspectable) => Ok(None),
            None => {
                // A caller may hold a Store candidate snapshot from before clear().
                // Only the retained exact Invocation seal proves negative membership.
                let sealed = match journal.as_ref() {
                    Some(journal) => journal
                        .records()?
                        .get(&id)
                        .is_some_and(|record| record.seal.is_some()),
                    None => false,
                };
                Ok(sealed.then_some(false))
            }
        }
    }
    pub(crate) fn inspect(&self, id: InvocationId) -> io::Result<Option<ReconciliationResult>> {
        // Empty-but-unsealed never releases a Lease. Keep the membership handle
        // and negative tombstone until the durable Store transition completes.
        let active = self.active.lock().map_err(|_| poisoned())?;
        Ok(match active.get(&id) {
            Some(Entry::Sealed) => Some(ReconciliationResult::ProvenEmpty),
            Some(Entry::Live(boundary)) => Some(match boundary.populated() {
                Ok(true) => ReconciliationResult::BoundaryNotEmpty,
                _ => ReconciliationResult::BoundaryUninspectable,
            }),
            Some(Entry::Uninspectable) => Some(ReconciliationResult::BoundaryUninspectable),
            None => None,
        })
    }
    pub(crate) fn clear(&self, id: InvocationId) {
        if let Ok(mut entries) = self.active.lock() {
            entries.remove(&id);
        }
    }
    pub(super) fn cleanup(
        &self,
        id: InvocationId,
        deadline: Instant,
    ) -> io::Result<super::journal::Seal> {
        self.with_journal(|journal| {
            let seal = journal.cleanup(id, deadline)?;
            #[cfg(test)]
            AFTER_CLEANUP_SEAL.with(|hook| {
                if let Some(hook) = hook.borrow_mut().take() {
                    hook();
                }
            });
            self.active
                .lock()
                .map_err(|_| poisoned())?
                .insert(id, Entry::Sealed);
            Ok(seal)
        })
    }
    pub(crate) fn reconcile(
        &self,
        candidate: &crate::store::ReconciliationCandidate,
        deadline: Instant,
    ) -> io::Result<ReconciliationResult> {
        self.with_journal(|journal| {
            let record = journal
                .records()?
                .get(&candidate.invocation_id)
                .ok_or_else(|| io::Error::other("unknown Linux reconciliation obligation"))?;
            if record.containment != candidate.containment_id
                || Some(record.daemon_generation) != candidate.daemon_generation
                || candidate
                    .root_identity
                    .as_ref()
                    .is_some_and(|root| record.root.as_ref() != Some(root))
            {
                return Err(io::Error::other(
                    "Linux reconciliation identity disagrees with executor history",
                ));
            }
            journal.cleanup(candidate.invocation_id, deadline)?;
            self.active
                .lock()
                .map_err(|_| poisoned())?
                .insert(candidate.invocation_id, Entry::Sealed);
            Ok(ReconciliationResult::ProvenEmpty)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use uuid::Uuid;

    #[test]
    #[ignore = "requires delegated cgroup under a protected system bootstrap Job"]
    fn linux_registry_requires_durable_seal_and_matches_reconciliation_identity() {
        let temp = tempfile::tempdir_in(env!("CARGO_MANIFEST_DIR")).unwrap();
        std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let path = temp.path().join("executor");
        let anchor = super::super::journal::Anchor {
            journal: Uuid::now_v7(),
            store: Uuid::now_v7(),
            domain: crate::ExecutionDomainId(Uuid::now_v7()),
        };
        let journal = Journal::initialize(&path, anchor.clone()).unwrap();
        let generation = Uuid::now_v7();
        let registry = Registry::default();
        registry
            .install(
                journal,
                crate::identity::attestation::Signer::new(
                    anchor.store,
                    generation,
                    "/tmp/registry-fixture".into(),
                )
                .unwrap(),
            )
            .unwrap();
        let invocation = InvocationId::from_parts(anchor.store, Uuid::now_v7());
        let containment = crate::ContainmentId::from_parts(anchor.store, Uuid::now_v7());
        // SAFETY: geteuid has no preconditions.
        let process =
            crate::identity::linux::Process::open(std::process::id(), unsafe { libc::geteuid() })
                .unwrap();
        let parent = std::env::var_os("STILLYARD_TEST_CGROUP_ROOT").unwrap();
        let boundary = registry
            .with_journal(|j| {
                j.create(
                    std::path::Path::new(&parent),
                    invocation,
                    containment,
                    Uuid::now_v7(),
                    generation,
                    process.identity.clone(),
                )
            })
            .unwrap();
        registry.register(invocation, Arc::new(boundary)).unwrap();
        assert_eq!(
            registry
                .contains(invocation, process.proc_handle())
                .unwrap(),
            Some(false)
        );
        assert_eq!(
            registry.inspect(invocation).unwrap(),
            Some(ReconciliationResult::BoundaryUninspectable),
            "empty live cgroup bypassed durable sealing"
        );
        let mut candidate = crate::store::ReconciliationCandidate {
            containment_id: crate::ContainmentId::from_parts(anchor.store, Uuid::now_v7()),
            invocation_id: invocation,
            attempt_id: crate::AttemptId::from_parts(anchor.store, Uuid::now_v7()),
            version: 1,
            host_id: None,
            boot_id: None,
            daemon_generation: Some(generation),
            root_pid_recorded: false,
            root_identity: None,
            prior_daemon_identity: None,
            incident_sequence: 1,
        };
        assert!(
            registry
                .reconcile(
                    &candidate,
                    Instant::now() + std::time::Duration::from_secs(5)
                )
                .is_err()
        );
        candidate.containment_id = containment;
        assert_eq!(
            registry
                .reconcile(
                    &candidate,
                    Instant::now() + std::time::Duration::from_secs(5)
                )
                .unwrap(),
            ReconciliationResult::ProvenEmpty
        );
        assert_eq!(
            registry.inspect(invocation).unwrap(),
            Some(ReconciliationResult::ProvenEmpty)
        );
        assert_eq!(
            registry
                .contains(invocation, process.proc_handle())
                .unwrap(),
            Some(false)
        );
        registry.clear(invocation);
        assert_eq!(
            registry
                .contains(invocation, process.proc_handle())
                .unwrap(),
            Some(false),
            "a stale candidate snapshot still has durable negative membership"
        );
        drop(registry);
        let registry = Registry::default();
        registry
            .install(
                Journal::open(&path, &anchor).unwrap(),
                crate::identity::attestation::Signer::new(
                    anchor.store,
                    Uuid::now_v7(),
                    "/tmp/registry-fixture".into(),
                )
                .unwrap(),
            )
            .unwrap();
        assert_eq!(
            registry.inspect(invocation).unwrap(),
            Some(ReconciliationResult::ProvenEmpty)
        );
    }
    #[test]
    #[ignore = "requires delegated cgroup under a protected system bootstrap Job"]
    fn linux_registry_membership_waits_for_cleanup_and_retains_negative_proof() {
        let temp = tempfile::tempdir_in(env!("CARGO_MANIFEST_DIR")).unwrap();
        std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let path = temp.path().join("executor");
        let anchor = super::super::journal::Anchor {
            journal: Uuid::now_v7(),
            store: Uuid::now_v7(),
            domain: crate::ExecutionDomainId(Uuid::now_v7()),
        };
        let journal = Journal::initialize(&path, anchor.clone()).unwrap();
        let generation = Uuid::now_v7();
        let registry = Registry::default();
        registry
            .install(
                journal,
                crate::identity::attestation::Signer::new(
                    anchor.store,
                    generation,
                    "/tmp/registry-fixture".into(),
                )
                .unwrap(),
            )
            .unwrap();
        let invocation = InvocationId::from_parts(anchor.store, Uuid::now_v7());
        let containment = crate::ContainmentId::from_parts(anchor.store, Uuid::now_v7());
        // SAFETY: geteuid has no preconditions.
        let process =
            crate::identity::linux::Process::open(std::process::id(), unsafe { libc::geteuid() })
                .unwrap();
        let parent = std::env::var_os("STILLYARD_TEST_CGROUP_ROOT").unwrap();
        let boundary = registry
            .with_journal(|j| {
                j.create(
                    std::path::Path::new(&parent),
                    invocation,
                    containment,
                    Uuid::now_v7(),
                    generation,
                    process.identity.clone(),
                )
            })
            .unwrap();
        assert_eq!(Registry::default().contains(invocation, 0).unwrap(), None);
        assert_eq!(
            registry.contains(invocation, 0).unwrap(),
            None,
            "an existing unsealed journal record must not prove nonmembership"
        );
        registry.register(invocation, Arc::new(boundary)).unwrap();

        let (removed_tx, removed_rx) = std::sync::mpsc::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        let cleanup_registry = registry.clone();
        let cleaner = std::thread::spawn(move || {
            AFTER_CLEANUP_SEAL.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    removed_tx.send(()).unwrap();
                    resume_rx
                        .recv_timeout(std::time::Duration::from_secs(5))
                        .unwrap();
                }));
            });
            cleanup_registry.cleanup(
                invocation,
                Instant::now() + std::time::Duration::from_secs(5),
            )
        });
        removed_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let membership_registry = registry.clone();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let member = std::thread::spawn(move || {
            entered_tx.send(()).unwrap();
            result_tx
                .send(membership_registry.contains(invocation, process.proc_handle()))
                .unwrap();
        });
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let premature = result_rx.recv_timeout(std::time::Duration::from_millis(100));
        // Always release the cleaner, including on the old implementation's failure.
        resume_tx.send(()).unwrap();
        cleaner.join().unwrap().unwrap();
        member.join().unwrap();
        assert!(
            matches!(premature, Err(std::sync::mpsc::RecvTimeoutError::Timeout)),
            "membership observed the removed boundary before sealed publication: {premature:?}"
        );
        assert_eq!(result_rx.recv().unwrap().unwrap(), Some(false));
        registry.clear(invocation);
        assert_eq!(registry.contains(invocation, 0).unwrap(), Some(false));
        let unknown = InvocationId::from_parts(anchor.store, Uuid::now_v7());
        assert_eq!(
            registry.contains(unknown, 0).unwrap(),
            None,
            "an absent registry entry alone must never prove nonmembership"
        );
    }
}
