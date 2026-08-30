use std::path::PathBuf;
#[cfg(windows)]
use std::sync::{Arc, Condvar, Mutex};
#[cfg(windows)]
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[cfg(windows)]
use fs2::FileExt;

#[cfg(windows)]
use crate::instance::{
    current_user_sid_string, default_instance, resolve_endpoint, resolve_store_root,
};
#[cfg(windows)]
use crate::protocol::{PROTOCOL_VERSION, Request, Response, read_frame, write_frame};
#[cfg(windows)]
use crate::store::{
    DoctorSnapshotCache, ManagedCandidate, Store, StoreError, StorePaths, SubmissionScope,
    open_lock,
};
use crate::{Error, Result};

#[cfg(windows)]
type SharedStore = Arc<Mutex<Store>>;

#[cfg(windows)]
struct PeerProcess {
    handle: usize,
    pid: u32,
    identity: Option<crate::ProcessIdentity>,
}

#[cfg(windows)]
impl Drop for PeerProcess {
    fn drop(&mut self) {
        // SAFETY: the accept loop transfers one owned process handle into this guard.
        unsafe {
            windows_sys::Win32::Foundation::CloseHandle(
                self.handle as windows_sys::Win32::Foundation::HANDLE,
            )
        };
    }
}

#[cfg(windows)]
pub(crate) fn run(store_root: Option<PathBuf>, endpoint: Option<String>) -> Result<()> {
    let store_root = store_root.or_else(|| std::env::var_os("STILLYARD_STORE").map(PathBuf::from));
    let endpoint = endpoint.or_else(|| std::env::var("STILLYARD_ENDPOINT").ok());
    validate_instance_tuple(store_root.is_some(), endpoint.is_some())?;
    let (store_root, endpoint) = match (store_root, endpoint) {
        (None, None) => {
            let selected = default_instance()?;
            (Some(selected.store_path), Some(selected.endpoint))
        }
        selected => selected,
    };
    let store_root = resolve_store_root(store_root)?;
    let endpoint = resolve_endpoint(endpoint)?;
    let _endpoint_lease = acquire_endpoint_lease(&endpoint)?;
    let first_pipe = create_pipe_instance(&endpoint, true)?;
    let (_lock, store) = open_store_under_lock(StorePaths::new(store_root))?;
    let store = Arc::new(Mutex::new(store));
    let observation_config = store
        .lock()
        .map_err(|_| Error::Unavailable("store mutex poisoned".into()))?
        .host_config()
        .observation;
    let doctor_snapshots = {
        let store = store
            .lock()
            .map_err(|_| Error::Unavailable("store mutex poisoned".into()))?;
        DoctorSnapshotCache::new(store.store_uuid(), store.daemon_generation())
    };
    let scheduler = DaemonReactor::start(
        Arc::clone(&store),
        endpoint,
        observation_config,
        doctor_snapshots,
    );
    let notifier = Arc::downgrade(&scheduler);
    store
        .lock()
        .map_err(|_| Error::Unavailable("store mutex poisoned".into()))?
        .set_change_notifier(Arc::new(move || {
            if let Some(notifier) = notifier.upgrade() {
                notifier.notify_change();
            }
        }));
    scheduler.wake();
    serve(store, scheduler, first_pipe)
}

#[cfg(windows)]
fn validate_instance_tuple(store_selected: bool, endpoint_selected: bool) -> Result<()> {
    if store_selected != endpoint_selected {
        return Err(Error::InvalidSpec(
            "an explicit daemon instance requires both store and endpoint coordinates".into(),
        ));
    }
    Ok(())
}

#[cfg(windows)]
fn open_store_under_lock(paths: StorePaths) -> Result<(std::fs::File, Store)> {
    paths
        .ensure()
        .map_err(|error| Error::Unavailable(error.to_string()))?;
    let lock = open_lock(&paths.lock).map_err(|error| Error::Unavailable(error.to_string()))?;
    lock.try_lock_exclusive()
        .map_err(|error| Error::Unavailable(format!("daemon already running: {error}")))?;
    let store = Store::open(paths).map_err(|error| Error::Unavailable(error.to_string()))?;
    Ok((lock, store))
}

#[cfg(not(windows))]
pub(crate) fn run(_store_root: Option<PathBuf>, _endpoint: Option<String>) -> Result<()> {
    Err(Error::UnsupportedPlatform(std::env::consts::OS))
}

#[cfg(windows)]
mod reactor;
#[cfg(windows)]
mod reconciliation;
#[cfg(windows)]
mod rpc;
#[cfg(windows)]
mod transport;

#[cfg(windows)]
use reactor::*;
#[cfg(windows)]
use reconciliation::*;
#[cfg(windows)]
use rpc::handle_request;
#[cfg(windows)]
use transport::{acquire_endpoint_lease, create_pipe_instance, serve};

#[cfg(all(test, windows))]
mod tests;
