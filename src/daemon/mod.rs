use std::path::PathBuf;
#[cfg(any(windows, target_os = "linux"))]
use std::sync::{Arc, Condvar, Mutex};
#[cfg(any(windows, target_os = "linux"))]
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[cfg(any(windows, target_os = "linux"))]
use fs2::FileExt;

#[cfg(any(windows, target_os = "linux"))]
use crate::instance::{default_instance, resolve_endpoint, resolve_store_root};
#[cfg(any(windows, target_os = "linux"))]
use crate::protocol::{PROTOCOL_VERSION, Request, Response, read_frame, write_frame};
#[cfg(any(windows, target_os = "linux"))]
use crate::store::{
    DoctorSnapshotCache, ManagedCandidate, Store, StoreError, StorePaths, SubmissionScope,
    open_lock,
};
use crate::{Error, Result};

#[cfg(any(windows, target_os = "linux"))]
type SharedStore = Arc<Mutex<Store>>;

#[cfg(any(windows, target_os = "linux"))]
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
    #[cfg(windows)]
    let first_endpoint = transport::create_pipe_instance(&endpoint, true)?;
    #[cfg(target_os = "linux")]
    let first_endpoint = transport::bind_endpoint(&endpoint)?;
    let (_lock, mut store) = open_store_under_lock(StorePaths::new(store_root))?;
    #[cfg(target_os = "linux")]
    let attached = store
        .attached_runtime(&endpoint)
        .map_err(|e| Error::Unavailable(e.to_string()))?;
    #[cfg(target_os = "linux")]
    let (live_containments, releases) = match attached {
        Some((live, releases)) => (live, Some(releases)),
        None => {
            match store
                .native_linux_runtime(&endpoint)
                .map_err(|e| Error::Unavailable(e.to_string()))?
            {
                Some(live) => (live, None),
                None => {
                    store
                        .attach_authority()
                        .map_err(|e| Error::Unavailable(e.to_string()))?;
                    (crate::runner::LiveContainments::default(), None)
                }
            }
        }
    };
    #[cfg(windows)]
    store
        .attach_authority()
        .map_err(|error| Error::Unavailable(error.to_string()))?;
    #[cfg(windows)]
    let live_containments = crate::runner::LiveContainments::default();
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
        live_containments,
    );
    let notifier = Arc::downgrade(&scheduler);
    store
        .lock()
        .map_err(|_| Error::Unavailable("store mutex poisoned".into()))?
        .set_change_notifier(Arc::new(move || {
            if let Some(notifier) = notifier.upgrade() {
                notifier.notify_change();
                #[cfg(target_os = "linux")]
                notifier.live_containments.wake_attached();
            }
        }));
    #[cfg(target_os = "linux")]
    if let Some(releases) = releases {
        attached::start(Arc::clone(&store), Arc::downgrade(&scheduler), releases)?;
    }
    scheduler.wake();
    serve(store, scheduler, first_endpoint)
}

#[cfg(any(windows, target_os = "linux"))]
fn validate_instance_tuple(store_selected: bool, endpoint_selected: bool) -> Result<()> {
    if store_selected != endpoint_selected {
        return Err(Error::InvalidSpec(
            "an explicit daemon instance requires both store and endpoint coordinates".into(),
        ));
    }
    Ok(())
}

#[cfg(any(windows, target_os = "linux"))]
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

/// Explicit stopped-manager setup. Resolving coordinates neither starts the
/// daemon nor creates machine authority or outstanding scheduling rights.
#[cfg(target_os = "linux")]
pub(crate) fn configure_native_linux(
    store_root: Option<PathBuf>,
    endpoint: Option<String>,
    executor_cgroup: PathBuf,
) -> Result<serde_json::Value> {
    crate::store::native_linux::require_native_host()
        .map_err(|e| Error::Unavailable(e.to_string()))?;
    validate_instance_tuple(store_root.is_some(), endpoint.is_some())?;
    let store_root = resolve_store_root(store_root)?;
    let endpoint = resolve_endpoint(endpoint)?;
    let _endpoint_lease = acquire_endpoint_lease(&endpoint)?;
    let (_lock, mut store) = open_store_under_lock(StorePaths::new(store_root.clone()))?;
    let configuration = store
        .install_native_linux(&executor_cgroup)
        .map_err(|e| Error::Unavailable(e.to_string()))?;
    Ok(serde_json::json!({
        "store_uuid": store.store_uuid(), "store_path": store_root,
        "endpoint": endpoint, "configuration": configuration,
    }))
}

/// Explicit stopped-manager setup for a Windows-coordinated WSL installation.
#[cfg(target_os = "linux")]
pub(crate) fn configure_wsl_attachment(
    store_root: Option<PathBuf>,
    endpoint: Option<String>,
    configuration: Option<PathBuf>,
) -> Result<serde_json::Value> {
    validate_instance_tuple(store_root.is_some(), endpoint.is_some())?;
    let store_root = resolve_store_root(store_root)?;
    let endpoint = resolve_endpoint(endpoint)?;
    let _endpoint_lease = acquire_endpoint_lease(&endpoint)?;
    let (_lock, mut store) = open_store_under_lock(StorePaths::new(store_root.clone()))?;
    if let Some(configuration) = configuration.as_ref() {
        store
            .install_attached_from_file(configuration)
            .map_err(|e| Error::Unavailable(e.to_string()))?;
    }
    Ok(serde_json::json!({
        "store_uuid": store.store_uuid(), "store_path": store_root,
        "endpoint": endpoint, "owner": crate::instance::current_owner_principal()?,
        "configuration_applied": configuration.is_some()
    }))
}

#[cfg(all(test, target_os = "linux"))]
mod installation_tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn explicit_wsl_setup_preserves_store_and_obeys_daemon_lock() {
        let temp = crate::test_support::durable_tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(socket_dir.path(), std::fs::Permissions::from_mode(0o700))
            .unwrap();
        let root = temp.path().join("store");
        let endpoint = socket_dir
            .path()
            .join("rpc.sock")
            .to_str()
            .unwrap()
            .to_owned();
        let first =
            configure_wsl_attachment(Some(root.clone()), Some(endpoint.clone()), None).unwrap();
        let second =
            configure_wsl_attachment(Some(root.clone()), Some(endpoint.clone()), None).unwrap();
        assert_eq!(first["store_uuid"], second["store_uuid"]);
        let (_lock, _store) = open_store_under_lock(StorePaths::new(root.clone())).unwrap();
        assert!(
            configure_wsl_attachment(Some(root.clone()), Some(endpoint.clone()), None).is_err()
        );
        drop(_store);
        drop(_lock);
        let input = temp.path().join("pairing.json");
        std::fs::write(&input, b"{}").unwrap();
        std::fs::set_permissions(&input, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(configure_wsl_attachment(Some(root.clone()), Some(endpoint), Some(input)).is_err());
        assert!(!root.join("attachment").exists());
    }
}

#[cfg(not(any(windows, target_os = "linux")))]
pub(crate) fn run(_store_root: Option<PathBuf>, _endpoint: Option<String>) -> Result<()> {
    Err(Error::UnsupportedPlatform(std::env::consts::OS))
}

#[cfg(target_os = "linux")]
mod attached;
#[cfg(any(windows, target_os = "linux"))]
mod reactor;
#[cfg(any(windows, target_os = "linux"))]
mod reconciliation;
#[cfg(any(windows, target_os = "linux"))]
mod rpc;
#[cfg(windows)]
mod transport;
#[cfg(target_os = "linux")]
#[path = "linux.rs"]
mod transport;
#[cfg(windows)]
use crate::instance::current_user_sid_string;
#[cfg(all(test, windows))]
use transport::create_pipe_instance;

#[cfg(any(windows, target_os = "linux"))]
use reactor::*;
#[cfg(any(windows, target_os = "linux"))]
use reconciliation::*;
#[cfg(any(windows, target_os = "linux"))]
use rpc::handle_request;
#[cfg(any(windows, target_os = "linux"))]
use transport::{PeerProcess, acquire_endpoint_lease, serve};

#[cfg(all(test, windows))]
mod tests;
