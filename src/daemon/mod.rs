use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[cfg(windows)]
use fs2::FileExt;

#[cfg(windows)]
use crate::instance::{current_user_sid_string, resolve_endpoint, resolve_store_root};
use crate::protocol::{PROTOCOL_VERSION, Request, Response};
#[cfg(windows)]
use crate::protocol::{read_frame, write_frame};
use crate::store::{ManagedCandidate, Store, StoreError, SubmissionScope};
#[cfg(windows)]
use crate::store::{StorePaths, open_lock};
use crate::{Error, Result};

type SharedStore = Arc<Mutex<Store>>;

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
    let store_root = resolve_store_root(store_root)?;
    let endpoint = resolve_endpoint(endpoint)?;
    let _endpoint_lease = acquire_endpoint_lease(&endpoint)?;
    let first_pipe = create_pipe_instance(&endpoint, true)?;
    let (_lock, store) = open_store_under_lock(StorePaths::new(store_root))?;
    let store = Arc::new(Mutex::new(store));
    let scheduler = DaemonReactor::start(Arc::clone(&store), endpoint);
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

mod reactor;
#[cfg(windows)]
mod reconciliation;
mod rpc;
mod transport;

use reactor::*;
use reconciliation::*;
use rpc::handle_request;
use transport::{acquire_endpoint_lease, create_pipe_instance, serve};

#[cfg(all(test, windows))]
mod tests;
