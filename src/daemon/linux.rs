//! Owner-only Unix RPC. Socket ownership and store ownership are separate leases.
use super::*;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::net::UnixListener;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(super) struct PeerProcess {
    pub(super) handle: usize,
    pub(super) pid: u32,
    pub(super) identity: Option<crate::ProcessIdentity>,
    process: crate::identity::linux::Process,
}

pub(super) fn peer_principal(peer: &PeerProcess) -> Result<String> {
    peer.process.principal().map_err(unavailable)
}

pub(super) fn peer_image_path(peer: &PeerProcess) -> io::Result<PathBuf> {
    peer.process.principal()?;
    std::fs::read_link(format!("/proc/self/fd/{}/exe", peer.handle))
}

fn unavailable(error: io::Error) -> Error {
    Error::Unavailable(error.to_string())
}

fn owner() -> u32 {
    // SAFETY: geteuid has no preconditions.
    unsafe { libc::geteuid() }
}

fn private_parent(endpoint: &Path) -> io::Result<()> {
    let parent = endpoint
        .parent()
        .ok_or_else(|| io::Error::other("socket has no parent"))?;
    match std::fs::symlink_metadata(parent) {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            std::fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(parent)?;
        }
        Err(error) => return Err(error),
    }
    let metadata = std::fs::symlink_metadata(parent)?;
    if !metadata.is_dir() || metadata.uid() != owner() || metadata.mode() & 0o077 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "Unix endpoint parent must be an owner-only directory",
        ));
    }
    Ok(())
}

// Never unlink this file: replacing its inode would split singleton ownership.
pub(super) struct EndpointLease {
    _lock: File,
}

pub(super) fn acquire_endpoint_lease(endpoint: &str) -> Result<EndpointLease> {
    private_parent(Path::new(endpoint)).map_err(unavailable)?;
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(format!("{endpoint}.lock"))
        .map_err(unavailable)?;
    let metadata = lock.metadata().map_err(unavailable)?;
    if !metadata.is_file() || metadata.uid() != owner() || metadata.mode() & 0o077 != 0 {
        return Err(Error::Unavailable("unsafe Unix endpoint lease file".into()));
    }
    lock.try_lock_exclusive().map_err(|error| {
        Error::Unavailable(format!("daemon endpoint is already owned: {error}"))
    })?;
    Ok(EndpointLease { _lock: lock })
}

pub(super) struct ServerEndpoint {
    listener: UnixListener,
    path: PathBuf,
    identity: (u64, u64),
}

impl Drop for ServerEndpoint {
    fn drop(&mut self) {
        if std::fs::symlink_metadata(&self.path)
            .is_ok_and(|m| m.file_type().is_socket() && (m.dev(), m.ino()) == self.identity)
        {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// Caller retains the endpoint lease throughout bind and serving. Only a refused
/// connection to the same owned socket permits stale-path reclamation.
pub(super) fn bind_endpoint(endpoint: &str) -> Result<ServerEndpoint> {
    private_parent(Path::new(endpoint)).map_err(unavailable)?;
    match std::fs::symlink_metadata(endpoint) {
        Ok(old) => {
            if !old.file_type().is_socket() || old.uid() != owner() || old.mode() & 0o077 != 0 {
                return Err(Error::Unavailable(
                    "refusing to replace an unsafe Unix endpoint".into(),
                ));
            }
            match crate::client::linux::connect(
                endpoint,
                Instant::now() + Duration::from_millis(100),
            ) {
                Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => {}
                _ => {
                    return Err(Error::Unavailable(
                        "Unix endpoint still has a listener or cannot be inspected".into(),
                    ));
                }
            }
            let current = std::fs::symlink_metadata(endpoint).map_err(unavailable)?;
            if (current.dev(), current.ino()) != (old.dev(), old.ino()) {
                return Err(Error::Unavailable(
                    "Unix endpoint changed during stale-path recovery".into(),
                ));
            }
            std::fs::remove_file(endpoint).map_err(unavailable)?;
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(unavailable(error)),
    }
    let listener = UnixListener::bind(endpoint).map_err(unavailable)?;
    let metadata = std::fs::symlink_metadata(endpoint).map_err(unavailable)?;
    let server = ServerEndpoint {
        listener,
        path: endpoint.into(),
        identity: (metadata.dev(), metadata.ino()),
    };
    std::fs::set_permissions(endpoint, std::fs::Permissions::from_mode(0o600))
        .map_err(unavailable)?;
    Ok(server)
}

struct WorkerSlot(Arc<AtomicUsize>);
impl Drop for WorkerSlot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

pub(super) fn serve(
    store: SharedStore,
    scheduler: Arc<DaemonReactor>,
    server: ServerEndpoint,
) -> Result<()> {
    let workers = Arc::new(AtomicUsize::new(0));
    loop {
        let (socket, _) = match server.listener.accept() {
            Ok(connection) => connection,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(unavailable(error)),
        };
        if workers
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                (count < 64).then_some(count + 1)
            })
            .is_err()
        {
            continue;
        }
        let slot = WorkerSlot(Arc::clone(&workers));
        // Pin the kernel's connection peer before reading attacker-controlled bytes.
        let process = match crate::identity::linux::Process::from_socket(&socket) {
            Ok(process) => process,
            Err(_) => continue,
        };
        let crate::ProcessIdentity::Linux { pid, .. } = process.identity else {
            continue;
        };
        let peer = PeerProcess {
            handle: process.proc_handle(),
            pid,
            identity: Some(process.identity.clone()),
            process,
        };
        let store = Arc::clone(&store);
        let scheduler = Arc::clone(&scheduler);
        let deadline = Instant::now() + Duration::from_secs(30);
        let _ = std::thread::Builder::new()
            .name("stillyard-client".into())
            .spawn(move || {
                let _slot = slot;
                let mut stream = crate::client::linux::DeadlineStream {
                    stream: socket,
                    deadline,
                };
                let response = match read_frame::<Request>(&mut stream) {
                    Ok(request) => handle_request(&store, &scheduler, Some(&peer), request),
                    Err(error) => Response::Error {
                        code: "invalid_request".into(),
                        message: error.to_string(),
                    },
                };
                let _ = write_frame(&mut stream, &response);
            });
    }
}
