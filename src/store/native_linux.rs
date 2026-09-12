//! Explicit native installation, outside the resettable SQLite history.
use super::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};

const DIRECTORY: &str = "native-linux";
const MAX_BYTES: u64 = 65_536;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Configuration {
    pub(crate) version: u32,
    pub(crate) installation: Uuid,
    pub(crate) owner_uid: u32,
    pub(crate) host_id: HostId,
    pub(crate) store_uuid: Uuid,
    pub(crate) domain: crate::ExecutionDomainId,
    pub(crate) journal: Uuid,
    pub(crate) executor_cgroup: PathBuf,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    configuration: Configuration,
    sha256: String,
}

fn invalid(message: &str) -> io::Error {
    io::Error::other(message)
}

fn owner() -> u32 {
    // SAFETY: geteuid has no preconditions.
    unsafe { libc::geteuid() }
}

fn owned(path: &Path, directory: bool) -> io::Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(
            libc::O_NOFOLLOW | libc::O_CLOEXEC | if directory { libc::O_DIRECTORY } else { 0 },
        )
        .open(path)?;
    let metadata = file.metadata()?;
    if metadata.uid() != owner()
        || metadata.mode() & 0o077 != 0
        || (directory && !metadata.is_dir())
        || (!directory && !metadata.is_file())
    {
        return Err(invalid(
            "native installation must be an owner-only file or directory",
        ));
    }
    Ok(file)
}

fn fingerprint(configuration: &Configuration) -> io::Result<String> {
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(configuration)?)
    ))
}

fn publish(directory: &Path, name: &str, value: &impl Serialize) -> io::Result<()> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() as u64 > MAX_BYTES {
        return Err(invalid("native installation exceeds byte bound"));
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(directory.join(name))?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    File::open(directory)?.sync_all()
}

impl Configuration {
    fn validate(&self) -> io::Result<()> {
        if self.version != 1
            || self.installation.is_nil()
            || self.store_uuid.is_nil()
            || self.domain.0.is_nil()
            || self.journal.is_nil()
            || self.owner_uid != owner()
            || self.host_id.0.is_empty()
            || !self.executor_cgroup.is_absolute()
            || !self.executor_cgroup.starts_with("/sys/fs/cgroup")
            || self.executor_cgroup == Path::new("/sys/fs/cgroup")
            || self.executor_cgroup.components().any(|c| {
                matches!(
                    c,
                    std::path::Component::ParentDir | std::path::Component::CurDir
                )
            })
        {
            return Err(invalid(
                "invalid native Linux installation identity or executor path",
            ));
        }
        Ok(())
    }
}

pub(crate) fn load(root: &Path) -> io::Result<Option<Configuration>> {
    let path = root.join(DIRECTORY);
    match std::fs::symlink_metadata(&path) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
        Ok(_) => {}
    }
    if std::fs::symlink_metadata(root.join("attachment")).is_ok() {
        return Err(invalid("native and attached installation markers conflict"));
    }
    let directory = owned(&path, true)?;
    let anchor = PathBuf::from(format!(
        "/proc/self/fd/{}/anchor.json",
        directory.as_raw_fd()
    ));
    let mut bytes = Vec::new();
    owned(&anchor, false)?
        .take(MAX_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_BYTES {
        return Err(invalid("native installation anchor exceeds byte bound"));
    }
    let envelope: Envelope = serde_json::from_slice(&bytes)?;
    envelope.configuration.validate()?;
    if fingerprint(&envelope.configuration)? != envelope.sha256 {
        return Err(invalid("native installation anchor checksum differs"));
    }
    Ok(Some(envelope.configuration))
}

pub(super) fn initialize_schema(connection: &Connection) -> StoreResult<()> {
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS native_linux_installation(
        singleton INTEGER PRIMARY KEY CHECK(singleton=1), store_uuid TEXT NOT NULL,
        installation_sha256 TEXT NOT NULL);",
    )?;
    Ok(())
}

pub(crate) fn validate_store(root: &Path, connection: &Connection, store: Uuid) -> StoreResult<()> {
    let marker: Option<(String, String)> = connection.query_row(
        "SELECT store_uuid,installation_sha256 FROM native_linux_installation WHERE singleton=1",
        [], |r| Ok((r.get(0)?, r.get(1)?))).optional()?;
    match (load(root)?, marker) {
        (None, None) => Ok(()),
        (Some(configuration), Some((stored, hash)))
            if configuration.store_uuid == store
                && stored == store.to_string()
                && fingerprint(&configuration)? == hash =>
        {
            Ok(())
        }
        _ => Err(StoreError::InvalidState(
            "native installation or Store history is missing or replaced; admission remains fenced"
                .into(),
        )),
    }
}

pub(crate) fn require_native_host() -> io::Result<()> {
    let kernel = std::fs::read_to_string("/proc/sys/kernel/osrelease")?.to_ascii_lowercase();
    if kernel.contains("microsoft") || kernel.contains("wsl") {
        return Err(invalid(
            "standalone Linux installation requires a native host; WSL must retain its Windows authority",
        ));
    }
    Ok(())
}

impl Store {
    pub(super) fn validate_native_executor_inventory(&self) -> StoreResult<()> {
        if load(&self.paths.root)?.is_none() {
            return Ok(());
        }
        let registry = self.native_executor.as_ref().ok_or_else(|| {
            StoreError::InvalidState("native executor journal must be open before admission".into())
        })?;
        let inventory = registry.native_inventory()?;
        validate_inventory(
            &self.connection,
            &inventory,
            &self.authority_snapshot()?.native_obligations,
        )?;
        for permission in inventory
            .iter()
            .filter(|record| !record.sealed)
            .filter_map(|record| record.permission.as_ref())
        {
            let job = match permission.allocation.owner {
                crate::machine::AllocationOwner::Work { job_id, .. }
                | crate::machine::AllocationOwner::Probe { job_id, .. } => job_id,
            };
            if !self
                .native_allocations(job)?
                .contains(&permission.allocation)
            {
                return Err(StoreError::InvalidState(
                    "native executor permission and SQL resource debit differ; admission is fenced"
                        .into(),
                ));
            }
        }
        Ok(())
    }
    pub(crate) fn native_linux_runtime(
        &mut self,
        endpoint: &str,
    ) -> StoreResult<Option<crate::runner::LiveContainments>> {
        validate_store(&self.paths.root, &self.connection, self.store_uuid)?;
        let Some(configuration) = load(&self.paths.root)? else {
            return Ok(None);
        };
        require_native_host()?;
        let registry = crate::runner::linux::installed_registry(
            &self.paths.root.join(DIRECTORY).join("executor"),
            configuration.journal,
            self.store_uuid,
            configuration.domain,
            self.daemon_generation,
            endpoint.into(),
        )?;
        // Authority reconciliation may retire native permissions only after this
        // exact executor history has been validated and its seals can be queried.
        self.native_executor = Some(registry.clone());
        self.attach_authority()?;
        Ok(Some(crate::runner::LiveContainments::native_linux(
            registry,
        )))
    }

    pub(crate) fn linux_launch_identity(
        &self,
        job: &PreparedJob,
    ) -> StoreResult<(Uuid, PathBuf, Uuid, bool)> {
        self.check_authority_release()?;
        if let Some(configuration) = load(&self.paths.root)? {
            if self.native_executor.is_none() {
                return Err(StoreError::InvalidState(
                    "native executor history is not installed in this daemon".into(),
                ));
            }
            let lease: String = self.connection.query_row("SELECT id FROM leases
                WHERE state='granted' AND attempt_id=?1
                  AND ((?2='probe' AND invocation_id=?3) OR (?2!='probe' AND invocation_id IS NULL))",
                params![job.attempt_id.entity_uuid().to_string(),
                    if job.role == InvocationRole::Probe { "probe" } else { "work" },
                    job.invocation_id.entity_uuid().to_string()], |r| r.get(0))?;
            Ok((
                Uuid::parse_str(&lease)?,
                configuration.executor_cgroup,
                self.daemon_generation,
                false,
            ))
        } else {
            let (lease, parent, generation) = self.attached_launch_identity(job)?;
            Ok((lease, parent, generation, true))
        }
    }

    pub(crate) fn record_linux_cleanup(
        &mut self,
        invocation: InvocationId,
        boundary: &str,
        proof: &str,
        never: Option<&crate::machine::manager::release::NeverReleased>,
    ) -> StoreResult<()> {
        if load(&self.paths.root)?.is_some() {
            validate_store(&self.paths.root, &self.connection, self.store_uuid)?;
            if never.is_some() {
                return Err(StoreError::InvalidState(
                    "native cleanup cannot consume attached Ticket proof".into(),
                ));
            }
            self.native_executor
                .as_ref()
                .ok_or_else(|| {
                    StoreError::InvalidState(
                        "native cleanup requires its open executor journal".into(),
                    )
                })?
                .verify_durable_seal(invocation, boundary, proof)?;
            Ok(())
        } else {
            self.record_attached_cleanup(invocation, boundary, proof, never)
        }
    }

    /// Caller owns the stopped Store and endpoint leases. A partial installation
    /// remains visibly incomplete; ordinary startup never finishes it implicitly.
    pub(crate) fn install_native_linux(&mut self, executors: &Path) -> StoreResult<Configuration> {
        require_native_host()?;
        if self.authority.is_some()
            || self.paths.root.join("authority").exists()
            || self.paths.root.join(DIRECTORY).exists()
            || self.paths.root.join("attachment").exists()
        {
            return Err(StoreError::InvalidState("native installation requires a fresh stopped Store without prior authority or runtime installation".into()));
        }
        let occupied: bool = self.connection.query_row("SELECT
            EXISTS(SELECT 1 FROM leases WHERE state='granted') OR
            EXISTS(SELECT 1 FROM containments WHERE state NOT IN ('empty','cleared')) OR
            EXISTS(SELECT 1 FROM attached_local_mode) OR EXISTS(SELECT 1 FROM native_linux_installation)",
            [], |r| r.get(0))?;
        if occupied {
            return Err(StoreError::InvalidState(
                "native installation has outstanding work or a prior runtime binding".into(),
            ));
        }
        let executor_cgroup = std::fs::canonicalize(executors)?;
        if !executor_cgroup.starts_with("/sys/fs/cgroup")
            || executor_cgroup == Path::new("/sys/fs/cgroup")
        {
            return Err(StoreError::InvalidState(
                "native executors require a dedicated delegated cgroup".into(),
            ));
        }
        for control in ["cgroup.kill", "cgroup.procs"] {
            OpenOptions::new()
                .write(true)
                .open(executor_cgroup.join(control))?;
        }
        if !std::fs::read_to_string(executor_cgroup.join("cgroup.events"))?
            .lines()
            .any(|line| line == "populated 0")
        {
            return Err(StoreError::InvalidState(
                "initial executor cgroup is not empty".into(),
            ));
        }
        owned(&self.paths.root, true)?;
        crate::filesystem::require_durable_local_filesystem(&self.paths.root)?;
        self.startup_identity = crate::identity::probe_attached_linux_identity();
        let host_id = self.startup_identity.host_id.clone().ok_or_else(|| {
            StoreError::InvalidState("native process identity is unavailable".into())
        })?;
        let directory = self.paths.root.join(DIRECTORY);
        std::fs::DirBuilder::new().mode(0o700).create(&directory)?;
        File::open(&self.paths.root)?.sync_all()?;
        let installation = Uuid::now_v7();
        publish(
            &directory,
            "intent.json",
            &serde_json::json!({
                "installation": installation, "store_uuid": self.store_uuid,
                "owner_uid": owner(), "executor_cgroup": executor_cgroup,
            }),
        )?;
        bind_unbound_store(&self.connection, Some(&host_id))?;
        self.bound_host_id = Some(host_id.clone());
        self.attach_authority_inner(None)?;
        let authority = self.initialize_authority()?;
        let configuration = Configuration {
            version: 1,
            installation,
            owner_uid: owner(),
            host_id,
            store_uuid: self.store_uuid,
            domain: authority
                .domains
                .ok_or_else(|| {
                    StoreError::InvalidState("native authority domain is unavailable".into())
                })?
                .native_domain,
            journal: Uuid::now_v7(),
            executor_cgroup,
        };
        configuration.validate()?;
        crate::runner::linux::initialize_history(
            &directory.join("executor"),
            configuration.journal,
            self.store_uuid,
            configuration.domain,
        )?;
        let sha256 = fingerprint(&configuration)?;
        let tx = self.connection.transaction()?;
        tx.execute(
            "INSERT INTO native_linux_installation VALUES (1,?1,?2)",
            params![self.store_uuid.to_string(), sha256],
        )?;
        tx.commit()?;
        publish(
            &directory,
            "anchor.json",
            &Envelope {
                configuration: configuration.clone(),
                sha256,
            },
        )?;
        validate_store(&self.paths.root, &self.connection, self.store_uuid)?;
        Ok(configuration)
    }
}

fn validate_inventory(
    connection: &Connection,
    inventory: &[crate::runner::linux::registry::NativeObligation],
    permissions: &[crate::machine::NativeStartPermission],
) -> StoreResult<()> {
    for record in inventory.iter().filter(|record| !record.sealed) {
        let retained: bool = connection.query_row(
            "SELECT EXISTS(
            SELECT 1 FROM containments c JOIN invocations i ON c.invocation_id=i.id
            JOIN leases l ON l.attempt_id=i.attempt_id
            WHERE c.id=?1 AND i.id=?2 AND l.id=?3 AND l.state='granted'
              AND c.state NOT IN ('empty','cleared')
              AND ((i.role='probe' AND l.invocation_id=i.id)
                OR (i.role!='probe' AND l.invocation_id IS NULL)))",
            params![
                record.containment.entity_uuid().to_string(),
                record.invocation.entity_uuid().to_string(),
                record.lease.to_string()
            ],
            |r| r.get(0),
        )?;
        if !retained {
            return Err(StoreError::InvalidState(
                "unsealed native executor obligation has no matching retained SQL Lease; admission is fenced".into()));
        }
    }
    for permission in permissions {
        if !inventory.iter().any(|record| {
            record.invocation == permission.invocation_id
                && record.permission.as_ref() == Some(permission)
        }) {
            return Err(StoreError::InvalidState(
                "native authority permission has no matching executor history; admission is fenced"
                    .into(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn fixture(root: &Path) -> Configuration {
        let configuration = Configuration {
            version: 1,
            installation: Uuid::now_v7(),
            owner_uid: owner(),
            host_id: HostId("test-native-host".into()),
            store_uuid: Uuid::now_v7(),
            domain: crate::ExecutionDomainId(Uuid::now_v7()),
            journal: Uuid::now_v7(),
            executor_cgroup: "/sys/fs/cgroup/test-only/executors".into(),
        };
        let directory = root.join(DIRECTORY);
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&directory)
            .unwrap();
        publish(
            &directory,
            "anchor.json",
            &Envelope {
                sha256: fingerprint(&configuration).unwrap(),
                configuration: configuration.clone(),
            },
        )
        .unwrap();
        configuration
    }

    #[test]
    fn sql_rollback_or_released_debit_cannot_hide_unsealed_native_obligation() {
        use crate::runner::linux::registry::NativeObligation;
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE containments(id TEXT,invocation_id TEXT,state TEXT);
            CREATE TABLE invocations(id TEXT,attempt_id TEXT,role TEXT);
            CREATE TABLE leases(id TEXT,attempt_id TEXT,invocation_id TEXT,state TEXT);",
            )
            .unwrap();
        let store = Uuid::now_v7();
        let obligation = NativeObligation {
            invocation: InvocationId::from_parts(store, Uuid::now_v7()),
            containment: ContainmentId::from_parts(store, Uuid::now_v7()),
            lease: Uuid::now_v7(),
            sealed: false,
            permission: None,
        };
        let mut inventory = vec![obligation];
        // The installed marker and Store UUID could both survive an SQL backup.
        assert!(validate_inventory(&connection, &inventory, &[]).is_err());
        let record = &inventory[0];
        connection
            .execute(
                "INSERT INTO containments VALUES (?1,?2,'uncertain')",
                params![
                    record.containment.entity_uuid().to_string(),
                    record.invocation.entity_uuid().to_string()
                ],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO invocations VALUES (?1,'attempt','primary')",
                [record.invocation.entity_uuid().to_string()],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO leases VALUES (?1,'attempt',NULL,'granted')",
                [record.lease.to_string()],
            )
            .unwrap();
        validate_inventory(&connection, &inventory, &[]).unwrap();
        connection
            .execute("UPDATE leases SET state='released'", [])
            .unwrap();
        assert!(validate_inventory(&connection, &inventory, &[]).is_err());
        inventory[0].sealed = true;
        validate_inventory(&connection, &inventory, &[]).unwrap();
        inventory[0].sealed = false;
        connection
            .execute("UPDATE leases SET state='granted'", [])
            .unwrap();
        connection
            .execute("UPDATE containments SET state='empty'", [])
            .unwrap();
        assert!(validate_inventory(&connection, &inventory, &[]).is_err());
    }

    #[test]
    fn native_installation_requires_both_anchor_and_exact_sql_binding() {
        let temp = tempfile::tempdir().unwrap();
        let connection = Connection::open_in_memory().unwrap();
        initialize_schema(&connection).unwrap();
        let configuration = fixture(temp.path());
        let store = configuration.store_uuid;
        assert!(validate_store(temp.path(), &connection, store).is_err());
        connection
            .execute(
                "INSERT INTO native_linux_installation VALUES(1,?1,?2)",
                params![store.to_string(), fingerprint(&configuration).unwrap()],
            )
            .unwrap();
        validate_store(temp.path(), &connection, store).unwrap();
        assert!(validate_store(temp.path(), &connection, Uuid::now_v7()).is_err());
        connection
            .execute(
                "UPDATE native_linux_installation SET installation_sha256='wrong'",
                [],
            )
            .unwrap();
        assert!(validate_store(temp.path(), &connection, store).is_err());
        std::fs::remove_dir_all(temp.path().join(DIRECTORY)).unwrap();
        assert!(validate_store(temp.path(), &connection, store).is_err());
    }

    #[test]
    fn native_installation_partial_conflicting_or_unsafe_history_is_not_absence() {
        let temp = tempfile::tempdir().unwrap();
        assert!(load(temp.path()).unwrap().is_none());
        let configuration = fixture(temp.path());
        assert_eq!(
            load(temp.path()).unwrap().unwrap().store_uuid,
            configuration.store_uuid
        );
        std::fs::create_dir(temp.path().join("attachment")).unwrap();
        assert!(load(temp.path()).is_err());
        std::fs::remove_dir(temp.path().join("attachment")).unwrap();
        let path = temp.path().join(DIRECTORY).join("anchor.json");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(load(temp.path()).is_err());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let mut wrong = configuration.clone();
        wrong.owner_uid = wrong.owner_uid.wrapping_add(1);
        std::fs::write(
            &path,
            serde_json::to_vec(&Envelope {
                sha256: fingerprint(&wrong).unwrap(),
                configuration: wrong,
            })
            .unwrap(),
        )
        .unwrap();
        assert!(load(temp.path()).is_err());
        std::fs::write(&path, b"{}").unwrap();
        assert!(load(temp.path()).is_err());
        std::fs::remove_file(&path).unwrap();
        assert!(load(temp.path()).is_err());
        std::os::unix::fs::symlink(temp.path().join("nonexistent"), &path).unwrap();
        assert!(load(temp.path()).is_err());
    }
}
