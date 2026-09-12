//! The installed pairing survives SQLite replacement. Opening a daemon never
//! creates or repairs this anchor, nor treats a missing anchor as standalone.
use super::*;
use std::io;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};

const MAX_ANCHOR_BYTES: u64 = 65_536;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Configuration {
    pub version: u32,
    pub pairing: crate::machine::PairingRegistration,
    pub coordinator_installation: Uuid,
    pub machine_id: Uuid,
    pub bridge_executable: PathBuf,
    pub bridge_sha256: String,
    pub coordinator_endpoint: String,
    pub interop_socket: PathBuf,
    pub executor_cgroup: PathBuf,
    pub journal: Uuid,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    configuration: Configuration,
    sha256: String,
}

fn invalid(message: &str) -> io::Error {
    io::Error::other(message)
}
pub(super) fn fingerprint(c: &Configuration) -> io::Result<String> {
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(c)?)))
}
fn owned(path: &Path, directory: bool) -> io::Result<File> {
    let f = OpenOptions::new()
        .read(true)
        .custom_flags(
            libc::O_NOFOLLOW | libc::O_CLOEXEC | if directory { libc::O_DIRECTORY } else { 0 },
        )
        .open(path)?;
    let m = f.metadata()?;
    // SAFETY: geteuid has no preconditions.
    if m.uid() != unsafe { libc::geteuid() }
        || m.mode() & 0o077 != 0
        || (directory && !m.is_dir())
        || (!directory && !m.is_file())
    {
        return Err(invalid("attached installation must be owner-only"));
    }
    Ok(f)
}
impl Configuration {
    fn validate(&self) -> io::Result<()> {
        // This initial runtime profile explicitly binds WSL interop and cgroup
        // delegation. It is not a generic container/standalone Linux profile.
        if self.version != 1
            || self.pairing.manager_store_uuid.is_nil()
            || self.pairing.installation.installation_nonce.is_nil()
            || self.pairing.installation.domain_id.0.is_nil()
            || self.coordinator_installation.is_nil()
            || self.machine_id.is_nil()
            || self.journal.is_nil()
            || self.pairing.installation.role != crate::machine::ParticipantRole::Executor
            || self.pairing.secret == [0; 32]
            || self.pairing.installation.runtime_registration.is_empty()
            || !self.bridge_executable.is_absolute()
            || !self.interop_socket.is_absolute()
            || !self.executor_cgroup.is_absolute()
            || [
                &self.bridge_executable,
                &self.interop_socket,
                &self.executor_cgroup,
            ]
            .into_iter()
            .any(|path| {
                path.components().any(|part| {
                    matches!(
                        part,
                        std::path::Component::ParentDir | std::path::Component::CurDir
                    )
                })
            })
            || !self.executor_cgroup.starts_with("/sys/fs/cgroup")
            || self.executor_cgroup == Path::new("/sys/fs/cgroup")
            || self.coordinator_endpoint.is_empty()
            || self.coordinator_endpoint.contains('\0')
            || self.bridge_sha256.len() != 64
            || !self.bridge_sha256.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(invalid("invalid attached WSL installation configuration"));
        }
        // SAFETY: geteuid has no preconditions.
        if self.pairing.installation.owner_uid != unsafe { libc::geteuid() } {
            return Err(invalid("attached installation belongs to another owner"));
        }
        Ok(())
    }
}

/// The open directory is pinned and the file is read through that descriptor,
/// so replacing an intermediate path cannot swap a partially inspected anchor.
pub(in crate::store) fn load(root: &Path) -> io::Result<Option<Configuration>> {
    let path = root.join("attachment");
    match std::fs::symlink_metadata(&path) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
        Ok(_) => {}
    }
    let directory = owned(&path, true)?;
    use std::os::fd::AsRawFd;
    let selected = PathBuf::from(format!(
        "/proc/self/fd/{}/anchor.json",
        directory.as_raw_fd()
    ));
    let mut bytes = Vec::new();
    owned(&selected, false)?
        .take(MAX_ANCHOR_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_ANCHOR_BYTES {
        return Err(invalid("attached anchor exceeds byte bound"));
    }
    let envelope: Envelope = serde_json::from_slice(&bytes)?;
    if fingerprint(&envelope.configuration)? != envelope.sha256 {
        return Err(invalid("attached anchor checksum changed"));
    }
    envelope.configuration.validate()?;
    Ok(Some(envelope.configuration))
}

/// Explicit installation only, after verifying no local obligations. A crash
/// leaves an incomplete installation closed for inspection, never auto-repaired.
pub(super) fn create(root: &Path, configuration: &Configuration) -> io::Result<()> {
    configuration.validate()?;
    owned(root, true)?;
    crate::filesystem::require_durable_local_filesystem(root)?;
    let directory = root.join("attachment");
    std::fs::DirBuilder::new().mode(0o700).create(&directory)?;
    File::open(root)?.sync_all()?;
    let bytes = serde_json::to_vec(&Envelope {
        configuration: configuration.clone(),
        sha256: fingerprint(configuration)?,
    })?;
    if bytes.len() as u64 > MAX_ANCHOR_BYTES {
        return Err(invalid("attached anchor exceeds byte bound"));
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(directory.join("anchor.json"))?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    File::open(directory)?.sync_all()
}

pub(crate) fn validate_store(root: &Path, c: &Connection, store: Uuid) -> StoreResult<()> {
    let mode: Option<String> = c
        .query_row(
            "SELECT store_uuid FROM attached_local_mode WHERE singleton=1",
            [],
            |r| r.get(0),
        )
        .optional()?;
    match (load(root)?,mode) {
        (None,None) => Ok(()),
        (Some(anchor),Some(mode)) if anchor.pairing.manager_store_uuid == store && mode == store.to_string() => {
            let continuous: String = c.query_row("SELECT store_uuid FROM attached_peer WHERE singleton=1",[],|r|r.get(0))?;
            if continuous != mode { return Err(StoreError::InvalidState("paired protocol history differs from installed store".into())); }
            let pinned: String = c.query_row("SELECT value FROM attached_meta WHERE key='installation_sha256'",[],|r|r.get(0))?;
            if pinned != fingerprint(&anchor)? { return Err(StoreError::InvalidState("installed pairing configuration changed".into())); }
            Ok(())
        }
        _ => Err(StoreError::InvalidState("attached installation or paired store history is missing or replaced; admission remains fenced".into())),
    }
}

impl Store {
    /// The command takes the same exclusive Store lock as the daemon before
    /// calling this. Never accept a pairing secret from argv or print it.
    pub(crate) fn install_attached_from_file(&mut self, path: &Path) -> StoreResult<()> {
        let mut bytes = Vec::new();
        owned(path, false)?
            .take(MAX_ANCHOR_BYTES + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_ANCHOR_BYTES {
            return Err(StoreError::Io(invalid(
                "attachment configuration exceeds byte bound",
            )));
        }
        let configuration: Configuration = serde_json::from_slice(&bytes)?;
        self.install_attached_configuration(&configuration)
    }

    /// Caller owns the stopped daemon's exclusive Store lease. This is an
    /// explicit installation transaction; recovery never invokes it.
    pub(crate) fn install_attached_configuration(
        &mut self,
        configuration: &Configuration,
    ) -> StoreResult<()> {
        configuration.validate()?;
        if configuration.pairing.manager_store_uuid != self.store_uuid {
            return Err(StoreError::InvalidState(
                "installation names another manager store".into(),
            ));
        }
        let occupied: bool = self.connection.query_row(
            "SELECT
            EXISTS(SELECT 1 FROM attached_local_mode) OR
            EXISTS(SELECT 1 FROM leases WHERE state='granted') OR
            EXISTS(SELECT 1 FROM containments WHERE state NOT IN ('empty','cleared'))",
            [],
            |r| r.get(0),
        )?;
        if occupied || self.authority.is_some() {
            return Err(StoreError::InvalidState(
                "installation requires a stopped unpaired manager without outstanding obligations"
                    .into(),
            ));
        }
        create(&self.paths.root, configuration)?;
        crate::runner::linux::initialize_history(
            &self.paths.root.join("attachment/executor"),
            configuration.journal,
            self.store_uuid,
            configuration.pairing.installation.domain_id,
        )?;
        let tx = self.connection.transaction()?;
        crate::machine::manager::initialize(&tx, self.store_uuid).map_err(protocol_error)?;
        tx.execute(
            "INSERT INTO attached_meta VALUES ('installation_sha256',?1)",
            [fingerprint(configuration)?],
        )?;
        tx.execute(
            "INSERT INTO attached_local_mode(singleton,store_uuid) VALUES (1,?1)",
            [self.store_uuid.to_string()],
        )?;
        tx.commit()?;
        validate_store(&self.paths.root, &self.connection, self.store_uuid)
    }
}
