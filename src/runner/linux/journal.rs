//! Reset-independent executor obligations. A missing cgroup is never a seal.
use super::cgroup::{Boundary, Identity};
use crate::{ContainmentId, InvocationId, ProcessIdentity};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::time::Instant;
use uuid::Uuid;

const MAX_BYTES: usize = 16 * 1024 * 1024;
const MAX_RECORDS: usize = 16_384;
const MAX_ACTIVE: usize = 1024;

#[cfg(test)]
thread_local! {
    static FAIL_BEFORE_SEAL: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Anchor {
    pub(super) journal: Uuid,
    pub(super) store: Uuid,
    pub(super) domain: crate::ExecutionDomainId,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    version: u32,
    anchor: Anchor,
    records: BTreeMap<InvocationId, Record>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Record {
    pub(super) containment: ContainmentId,
    pub(super) lease: Uuid,
    pub(super) daemon_generation: Uuid,
    pub(super) creator: ProcessIdentity,
    pub(super) boundary: Option<Identity>,
    pub(super) root: Option<ProcessIdentity>,
    pub(super) executable_sha256: Option<String>,
    pub(super) release_intent: Option<crate::machine::InvocationTicket>,
    pub(super) seal: Option<Seal>,
}

impl Record {
    pub(super) fn boundary_sha256(&self) -> io::Result<String> {
        let boundary = self
            .boundary
            .as_ref()
            .ok_or_else(|| invalid("missing executor boundary"))?;
        digest(&(
            self.containment,
            self.lease,
            self.daemon_generation,
            &self.creator,
            boundary,
            &self.root,
            &self.executable_sha256,
        ))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Seal {
    pub(super) invocation: InvocationId,
    pub(super) boundary_sha256: String,
    pub(super) seal_id: Uuid,
    pub(super) possibly_released: bool,
}
impl Seal {
    pub(super) fn sha256(&self) -> io::Result<String> {
        digest(self)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    state: State,
    sha256: String,
}

pub(super) struct Journal {
    directory: PathBuf,
    directory_guard: File,
    _lock: File,
    state: State,
    poisoned: bool,
}

fn invalid(message: &str) -> io::Error {
    io::Error::other(message)
}
pub(super) fn digest(value: &impl Serialize) -> io::Result<String> {
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(value)?)))
}
fn owned(path: &Path, directory: bool) -> io::Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(
            libc::O_NOFOLLOW | libc::O_CLOEXEC | if directory { libc::O_DIRECTORY } else { 0 },
        )
        .open(path)?;
    let meta = file.metadata()?;
    // SAFETY: geteuid has no preconditions.
    if meta.uid() != unsafe { libc::geteuid() }
        || meta.mode() & 0o077 != 0
        || (directory && !meta.is_dir())
        || (!directory && !meta.is_file())
    {
        return Err(invalid(
            "executor history must be an owner-only regular file/directory",
        ));
    }
    Ok(file)
}
fn read(path: &Path) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    owned(path, false)?
        .take(MAX_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_BYTES {
        return Err(invalid("executor history exceeds size bound"));
    }
    Ok(bytes)
}
fn publish(directory: &Path, name: &str, bytes: &[u8], replace: bool) -> io::Result<()> {
    if bytes.len() > MAX_BYTES {
        return Err(invalid("executor history exceeds size bound"));
    }
    let temporary = directory.join(format!(".publish-{}", Uuid::now_v7()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    let target = directory.join(name);
    if replace {
        std::fs::rename(&temporary, &target)?;
    } else {
        std::fs::hard_link(&temporary, &target)?;
        std::fs::remove_file(&temporary)?;
    }
    File::open(directory)?.sync_all()
}

impl Journal {
    /// Called only by explicit installation after an empty-work inventory. An
    /// existing, missing or damaged installation is never initialized on open.
    pub(super) fn initialize(directory: &Path, anchor: Anchor) -> io::Result<Self> {
        if anchor.journal.is_nil() || anchor.store.is_nil() || anchor.domain.0.is_nil() {
            return Err(invalid("nil executor installation identity"));
        }
        let parent = directory
            .parent()
            .ok_or_else(|| invalid("executor journal has no parent"))?;
        owned(parent, true)?;
        crate::filesystem::require_durable_local_filesystem(parent)?;
        std::fs::DirBuilder::new().mode(0o700).create(directory)?;
        File::open(parent)?.sync_all()?;
        let state = State {
            version: 1,
            anchor: anchor.clone(),
            records: BTreeMap::new(),
        };
        publish(
            directory,
            "anchor.json",
            &serde_json::to_vec(&anchor)?,
            false,
        )?;
        let envelope = Envelope {
            sha256: digest(&state)?,
            state,
        };
        publish(
            directory,
            "state.json",
            &serde_json::to_vec(&envelope)?,
            false,
        )?;
        Self::open(directory, &anchor)
    }

    pub(super) fn open(directory: &Path, expected: &Anchor) -> io::Result<Self> {
        let directory_guard = owned(directory, true)?;
        crate::filesystem::require_durable_local_filesystem(directory)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(directory.join("executor.lock"))?;
        let guard = owned(&directory.join("executor.lock"), false)?;
        if (guard.metadata()?.dev(), guard.metadata()?.ino())
            != (lock.metadata()?.dev(), lock.metadata()?.ino())
        {
            return Err(invalid("executor lock was replaced"));
        }
        fs2::FileExt::try_lock_exclusive(&lock)?;
        let anchor: Anchor = serde_json::from_slice(&read(&directory.join("anchor.json"))?)?;
        let envelope: Envelope = serde_json::from_slice(&read(&directory.join("state.json"))?)?;
        if &anchor != expected
            || envelope.state.anchor != anchor
            || envelope.state.version != 1
            || digest(&envelope.state)? != envelope.sha256
            || envelope.state.records.len() > MAX_RECORDS
        {
            return Err(invalid("executor history/anchor is unknown or mismatched"));
        }
        for (invocation, record) in &envelope.state.records {
            if invocation.store_uuid() != anchor.store
                || record.containment.store_uuid() != anchor.store
                || record.lease.is_nil()
                || record.daemon_generation.is_nil()
            {
                return Err(invalid("executor history contains a foreign obligation"));
            }
            if let Some(seal) = &record.seal {
                if seal.invocation != *invocation
                    || seal.seal_id.is_nil()
                    || seal.boundary_sha256 != record.boundary_sha256()?
                    || seal.possibly_released != record.release_intent.is_some()
                {
                    return Err(invalid("invalid executor seal"));
                }
            }
        }
        Ok(Self {
            directory: directory.to_owned(),
            directory_guard,
            _lock: lock,
            state: envelope.state,
            poisoned: false,
        })
    }

    fn check(&self) -> io::Result<()> {
        if self.poisoned {
            return Err(invalid(
                "executor publication failed; history requires recovery",
            ));
        }
        let meta = owned(&self.directory, true)?.metadata()?;
        let pin = self.directory_guard.metadata()?;
        if (meta.dev(), meta.ino()) != (pin.dev(), pin.ino()) || pin.nlink() == 0 {
            return Err(invalid("executor journal directory was replaced"));
        }
        let anchor: Anchor = serde_json::from_slice(&read(&self.directory.join("anchor.json"))?)?;
        let disk: Envelope = serde_json::from_slice(&read(&self.directory.join("state.json"))?)?;
        if anchor != self.state.anchor
            || disk.sha256 != digest(&disk.state)?
            || digest(&self.state)? != disk.sha256
        {
            return Err(invalid("executor history changed outside its writer"));
        }
        Ok(())
    }
    fn commit(&mut self, state: State) -> io::Result<()> {
        self.check()?;
        let envelope = Envelope {
            sha256: digest(&state)?,
            state,
        };
        // Once publication is attempted, even an fsync error is an uncertain
        // commit. This process must not continue from its old in-memory state.
        self.poisoned = true;
        publish(
            &self.directory,
            "state.json",
            &serde_json::to_vec(&envelope)?,
            true,
        )?;
        self.state = envelope.state;
        self.poisoned = false;
        Ok(())
    }
    pub(super) fn records(&self) -> io::Result<&BTreeMap<InvocationId, Record>> {
        self.check()?;
        Ok(&self.state.records)
    }

    pub(super) fn create(
        &mut self,
        parent: &Path,
        invocation: InvocationId,
        containment: ContainmentId,
        lease: Uuid,
        generation: Uuid,
        creator: ProcessIdentity,
    ) -> io::Result<Boundary> {
        self.check()?;
        let active = self
            .state
            .records
            .values()
            .filter(|r| r.seal.is_none())
            .count();
        // Reserve serialized space for every pending ticket and seal as well
        // as disk space. Exhaustion rejects new obligations, never cleanup.
        if serde_json::to_vec(&self.state)?
            .len()
            .saturating_add((active + 1) * 16_384)
            > MAX_BYTES - 1024 * 1024
        {
            return Err(invalid("executor history cleanup headroom unavailable"));
        }
        if invocation.store_uuid() != self.state.anchor.store
            || containment.store_uuid() != self.state.anchor.store
            || lease.is_nil()
            || generation.is_nil()
            || self.state.records.contains_key(&invocation)
            || self.state.records.len() >= MAX_RECORDS
            || active >= MAX_ACTIVE
            || !matches!(creator, ProcessIdentity::Linux { .. })
        {
            return Err(invalid(
                "duplicate, foreign or over-budget executor creation intent",
            ));
        }
        let mut space = std::mem::MaybeUninit::<libc::statvfs>::uninit();
        // SAFETY: pinned journal directory and writable statvfs storage.
        if unsafe { libc::fstatvfs(self.directory_guard.as_raw_fd(), space.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: successful fstatvfs initialized the structure.
        let space = unsafe { space.assume_init() };
        if u128::from(space.f_bavail) * u128::from(space.f_frsize)
            < (MAX_BYTES as u128 * 3 + (active as u128 + 1) * 65536)
        {
            return Err(invalid("executor cleanup publication headroom unavailable"));
        }
        let mut next = self.state.clone();
        next.records.insert(
            invocation,
            Record {
                containment,
                lease,
                daemon_generation: generation,
                creator,
                boundary: None,
                root: None,
                executable_sha256: None,
                release_intent: None,
                seal: None,
            },
        );
        self.commit(next)?;
        let boundary = Boundary::create(parent, invocation.entity_uuid())?;
        let mut next = self.state.clone();
        next.records.get_mut(&invocation).unwrap().boundary = Some(boundary.identity.clone());
        self.commit(next)?;
        Ok(boundary)
    }

    pub(super) fn ready(
        &mut self,
        invocation: InvocationId,
        launch: &super::launch::PreparedLaunch,
    ) -> io::Result<()> {
        let mut next = self.state.clone();
        let record = next
            .records
            .get_mut(&invocation)
            .ok_or_else(|| invalid("unknown executor intent"))?;
        if record.seal.is_some() || record.root.is_some() || record.release_intent.is_some() {
            return Err(invalid("executor root was already prepared or sealed"));
        }
        let boundary = Boundary::reopen(
            record
                .boundary
                .as_ref()
                .ok_or_else(|| invalid("executor boundary not committed"))?,
        )?;
        if !boundary.contains_process(launch.root.proc_handle())? {
            return Err(invalid("prepared root left the recorded boundary"));
        }
        record.root = Some(launch.root.identity.clone());
        record.executable_sha256 = Some(launch.requested_sha256.clone());
        self.commit(next)
    }

    /// Call before SQLite ticket consumption. A crash after this point is
    /// possibly-released even if the kernel barrier was never actually resumed.
    pub(super) fn release_intent(
        &mut self,
        ticket: &crate::machine::InvocationTicket,
    ) -> io::Result<()> {
        if serde_json::to_vec(ticket)?.len() > 8192 {
            return Err(invalid("executor ticket exceeds reserved history bound"));
        }
        let mut next = self.state.clone();
        let record = next
            .records
            .get_mut(&ticket.intent.invocation_id)
            .ok_or_else(|| invalid("unknown executor ticket"))?;
        if record.seal.is_some()
            || record.release_intent.is_some()
            || record.root.is_none()
            || ticket.key.manager_store_uuid != next.anchor.store
            || ticket.key.domain_id != next.anchor.domain
            || ticket.key.lease_id != record.lease
            || ticket.intent.containment_id != record.containment
            || record.executable_sha256.as_deref() != Some(ticket.intent.executable_sha256.as_str())
            || record.boundary_sha256()? != ticket.intent.boundary_sha256
        {
            return Err(invalid(
                "ticket does not match an unreleased prepared executor intent",
            ));
        }
        record.release_intent = Some(ticket.clone());
        self.commit(next)
    }

    pub(super) fn cleanup(
        &mut self,
        invocation: InvocationId,
        deadline: Instant,
    ) -> io::Result<Seal> {
        self.check()?;
        let record = self
            .state
            .records
            .get(&invocation)
            .ok_or_else(|| invalid("unknown executor cleanup"))?;
        if let Some(seal) = &record.seal {
            return Ok(seal.clone());
        }
        let identity = record.boundary.as_ref().ok_or_else(|| {
            invalid("creation interrupted before boundary identity; cleanup unknown")
        })?;
        Boundary::reopen(identity)?.kill_and_seal(deadline)?;
        #[cfg(test)]
        if FAIL_BEFORE_SEAL.replace(false) {
            return Err(invalid("injected crash after removal before durable seal"));
        }
        let seal = Seal {
            invocation,
            boundary_sha256: record.boundary_sha256()?,
            seal_id: Uuid::now_v7(),
            possibly_released: record.release_intent.is_some(),
        };
        let mut next = self.state.clone();
        next.records.get_mut(&invocation).unwrap().seal = Some(seal.clone());
        self.commit(next)?;
        Ok(seal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::time::Duration;

    fn anchor() -> Anchor {
        Anchor {
            journal: Uuid::now_v7(),
            store: Uuid::now_v7(),
            domain: crate::ExecutionDomainId(Uuid::now_v7()),
        }
    }
    fn current() -> ProcessIdentity {
        // SAFETY: geteuid has no preconditions.
        crate::identity::linux::Process::open(std::process::id(), unsafe { libc::geteuid() })
            .unwrap()
            .identity
    }

    #[test]
    fn linux_executor_journal_never_reinitializes_missing_or_corrupt_history() {
        let temp = tempfile::tempdir_in(env!("CARGO_MANIFEST_DIR")).unwrap();
        std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let path = temp.path().join("executor");
        let anchor = anchor();
        assert!(Journal::open(&path, &anchor).is_err());
        let journal = Journal::initialize(&path, anchor.clone()).unwrap();
        assert!(
            Journal::open(&path, &anchor).is_err(),
            "duplicate writer acquired executor history"
        );
        drop(journal);
        assert!(Journal::initialize(&path, anchor.clone()).is_err());
        let mut wrong = anchor.clone();
        wrong.store = Uuid::now_v7();
        assert!(Journal::open(&path, &wrong).is_err());
        let journal = Journal::open(&path, &anchor).unwrap();
        let original = std::fs::read(path.join("state.json")).unwrap();
        std::fs::write(path.join("state.json"), b"{}").unwrap();
        assert!(
            journal.records().is_err(),
            "live writer accepted external history loss"
        );
        drop(journal);
        assert!(Journal::open(&path, &anchor).is_err());
        std::fs::write(path.join("state.json"), original).unwrap();
        std::fs::remove_file(path.join("anchor.json")).unwrap();
        assert!(Journal::open(&path, &anchor).is_err());
    }

    #[test]
    #[ignore = "requires delegated cgroup under a protected system bootstrap Job"]
    fn linux_executor_journal_recovers_live_boundary_and_seals_once() {
        let root = std::env::var_os("STILLYARD_TEST_CGROUP_ROOT").unwrap();
        let temp = tempfile::tempdir_in(env!("CARGO_MANIFEST_DIR")).unwrap();
        std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let path = temp.path().join("executor");
        let helper = temp.path().join("stillyard");
        std::fs::copy(
            std::env::var_os("STILLYARD_TEST_EXECUTABLE").unwrap(),
            &helper,
        )
        .unwrap();
        let anchor = anchor();
        let invocation = InvocationId::from_parts(anchor.store, Uuid::now_v7());
        let containment = ContainmentId::from_parts(anchor.store, Uuid::now_v7());
        let lease = Uuid::now_v7();
        let generation = Uuid::now_v7();
        let mut journal = Journal::initialize(&path, anchor.clone()).unwrap();
        let boundary = journal
            .create(
                Path::new(&root),
                invocation,
                containment,
                lease,
                generation,
                current(),
            )
            .unwrap();
        let spec=super::super::launch::LaunchSpec {executable:"/usr/bin/python3".into(),args:vec!["-c".into(),"import subprocess,time; subprocess.Popen(['/usr/bin/python3','-c','import time;time.sleep(60)']); time.sleep(60)".into()],working_directory:temp.path().into(),environment:BTreeMap::new()};
        let mut launch = super::super::launch::PreparedLaunch::prepare(
            &boundary,
            &helper,
            &spec,
            File::open("/dev/null").unwrap(),
            Instant::now() + Duration::from_secs(15),
        )
        .unwrap();
        journal.ready(invocation, &launch).unwrap();
        let session = crate::machine::SessionIdentity {
            machine_id: Uuid::now_v7(),
            authority_epoch: Uuid::now_v7(),
            domain_id: anchor.domain,
            manager_store_uuid: anchor.store,
            executor_incarnation: Uuid::now_v7(),
            connection_epoch: 1,
        };
        let ticket = crate::machine::InvocationTicket {
            grant_id: crate::GrantId::from_parts(Uuid::now_v7(), Uuid::now_v7()),
            key: crate::machine::AllocationKey {
                machine_id: session.machine_id,
                authority_epoch: session.authority_epoch,
                domain_id: anchor.domain,
                manager_store_uuid: anchor.store,
                lease_id: lease,
            },
            offer_nonce: Uuid::now_v7(),
            intent: crate::machine::InvocationIntent {
                invocation_id: invocation,
                containment_id: containment,
                role: crate::InvocationRole::Primary,
                role_index: 0,
                release_sequence: 1,
                executable_sha256: launch.requested_sha256.clone(),
                boundary_sha256: journal.records().unwrap()[&invocation]
                    .boundary_sha256()
                    .unwrap(),
                readiness_challenge: Uuid::now_v7(),
                previous_cleanup: None,
            },
            configuration_sha256: "a".repeat(64),
            issued_unix_millis: 1,
            host_observation_generation: Uuid::now_v7(),
            host_sample_unix_millis: 1,
            session,
        };
        journal.release_intent(&ticket).unwrap();
        assert!(
            journal.release_intent(&ticket).is_err(),
            "a release intent was reused"
        );
        launch.release().unwrap();
        assert!(boundary.populated().unwrap(), "live boundary disappeared");
        drop(journal);
        let mut journal = Journal::open(&path, &anchor).unwrap();
        assert!(journal.records().unwrap()[&invocation].seal.is_none());
        let seal = journal
            .cleanup(invocation, Instant::now() + Duration::from_secs(5))
            .unwrap();
        assert!(seal.possibly_released);
        drop(journal);
        let mut journal = Journal::open(&path, &anchor).unwrap();
        assert_eq!(journal.cleanup(invocation, Instant::now()).unwrap(), seal);
        assert_eq!(seal.sha256().unwrap().len(), 64);
        assert!(
            journal
                .create(
                    Path::new(&root),
                    invocation,
                    containment,
                    lease,
                    generation,
                    current()
                )
                .is_err()
        );
        assert!(Boundary::reopen(&boundary.identity).is_err());
    }

    #[test]
    #[ignore = "requires delegated cgroup under a protected system bootstrap Job"]
    fn linux_executor_journal_absence_after_seal_gap_retains_uncertainty() {
        let root = std::env::var_os("STILLYARD_TEST_CGROUP_ROOT").unwrap();
        let temp = tempfile::tempdir_in(env!("CARGO_MANIFEST_DIR")).unwrap();
        std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let path = temp.path().join("executor");
        let anchor = anchor();
        let invocation = InvocationId::from_parts(anchor.store, Uuid::now_v7());
        let mut journal = Journal::initialize(&path, anchor.clone()).unwrap();
        journal
            .create(
                Path::new(&root),
                invocation,
                ContainmentId::from_parts(anchor.store, Uuid::now_v7()),
                Uuid::now_v7(),
                Uuid::now_v7(),
                current(),
            )
            .unwrap();
        FAIL_BEFORE_SEAL.set(true);
        assert!(
            journal
                .cleanup(invocation, Instant::now() + Duration::from_secs(5))
                .is_err()
        );
        drop(journal);
        let mut recovered = Journal::open(&path, &anchor).unwrap();
        assert!(recovered.records().unwrap()[&invocation].seal.is_none());
        assert!(
            recovered
                .cleanup(invocation, Instant::now() + Duration::from_secs(5))
                .is_err(),
            "missing cgroup was accepted as a seal"
        );
    }
}

#[cfg(test)]
mod cross_os;
