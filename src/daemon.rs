use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[cfg(windows)]
use fs2::FileExt;

#[cfg(windows)]
use crate::client::{current_user_sid_string, resolve_endpoint, resolve_store_root};
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
    let scheduler = Scheduler::start(Arc::clone(&store), endpoint);
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
struct EndpointLease(windows_sys::Win32::Foundation::HANDLE);

#[cfg(windows)]
impl Drop for EndpointLease {
    fn drop(&mut self) {
        // SAFETY: the lease owns the mutex handle returned by CreateMutexW.
        unsafe { windows_sys::Win32::Foundation::CloseHandle(self.0) };
    }
}

#[cfg(windows)]
struct OwnedPipe(windows_sys::Win32::Foundation::HANDLE);

#[cfg(windows)]
impl OwnedPipe {
    fn as_raw(&self) -> windows_sys::Win32::Foundation::HANDLE {
        self.0
    }

    fn into_raw(self) -> windows_sys::Win32::Foundation::HANDLE {
        let handle = self.0;
        std::mem::forget(self);
        handle
    }
}

#[cfg(windows)]
impl Drop for OwnedPipe {
    fn drop(&mut self) {
        // SAFETY: this guard exclusively owns a valid named-pipe handle.
        unsafe { windows_sys::Win32::Foundation::CloseHandle(self.0) };
    }
}

#[cfg(windows)]
fn create_pipe_instance(endpoint: &str, first: bool) -> Result<OwnedPipe> {
    use std::ffi::c_void;
    use std::mem::size_of;
    use std::os::windows::ffi::OsStrExt;

    use windows_sys::Win32::Foundation::{INVALID_HANDLE_VALUE, LocalFree};
    use windows_sys::Win32::Security::Authorization::{
        ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1,
    };
    use windows_sys::Win32::Security::{PSECURITY_DESCRIPTOR, SECURITY_ATTRIBUTES};
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_FLAG_FIRST_PIPE_INSTANCE, PIPE_ACCESS_DUPLEX,
    };
    use windows_sys::Win32::System::Pipes::{
        CreateNamedPipeW, PIPE_REJECT_REMOTE_CLIENTS, PIPE_TYPE_BYTE, PIPE_WAIT,
    };

    struct LocalAllocation(*mut c_void);

    impl Drop for LocalAllocation {
        fn drop(&mut self) {
            if !self.0.is_null() {
                // SAFETY: the pointer came from a Windows API documented to use LocalAlloc.
                unsafe { LocalFree(self.0) };
            }
        }
    }

    let sid = current_user_sid_string()?;
    let sddl: Vec<u16> = std::ffi::OsStr::new(&format!("D:P(A;;GA;;;{sid})"))
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    let mut descriptor: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
    // SAFETY: SDDL is NUL-terminated and descriptor is writable.
    if unsafe {
        ConvertStringSecurityDescriptorToSecurityDescriptorW(
            sddl.as_ptr(),
            SDDL_REVISION_1,
            &mut descriptor,
            std::ptr::null_mut(),
        )
    } == 0
    {
        return Err(Error::Unavailable(format!(
            "cannot secure daemon endpoint {endpoint:?}: {}",
            std::io::Error::last_os_error()
        )));
    }
    let descriptor = LocalAllocation(descriptor);
    let attributes = SECURITY_ATTRIBUTES {
        nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: descriptor.0,
        bInheritHandle: 0,
    };
    let pipe_name: Vec<u16> = std::ffi::OsStr::new(endpoint)
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    // SAFETY: pipe_name is NUL-terminated and attributes points to a live self-relative
    // descriptor whose DACL grants access only to the daemon owner's SID.
    let handle = unsafe {
        CreateNamedPipeW(
            pipe_name.as_ptr(),
            PIPE_ACCESS_DUPLEX
                | if first {
                    FILE_FLAG_FIRST_PIPE_INSTANCE
                } else {
                    0
                },
            PIPE_TYPE_BYTE | PIPE_WAIT | PIPE_REJECT_REMOTE_CLIENTS,
            255,
            64 * 1024,
            64 * 1024,
            5_000,
            &attributes,
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(Error::Unavailable(format!(
            "cannot create {}daemon endpoint {endpoint:?}: {}",
            if first { "exclusive " } else { "" },
            std::io::Error::last_os_error()
        )));
    }
    Ok(OwnedPipe(handle))
}

#[cfg(windows)]
fn acquire_endpoint_lease(endpoint: &str) -> Result<EndpointLease> {
    use std::ffi::c_void;
    use std::mem::size_of;
    use std::os::windows::ffi::OsStrExt;

    use sha2::{Digest, Sha256};
    use windows_sys::Win32::Foundation::{
        CloseHandle, ERROR_ALREADY_EXISTS, GetLastError, LocalFree,
    };
    use windows_sys::Win32::Security::Authorization::{
        ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1,
    };
    use windows_sys::Win32::Security::{PSECURITY_DESCRIPTOR, SECURITY_ATTRIBUTES};
    use windows_sys::Win32::System::Threading::CreateMutexW;

    let sid = current_user_sid_string()?;
    let identity = format!("{sid}\0{}", endpoint.to_ascii_lowercase());
    let digest = format!("{:x}", Sha256::digest(identity.as_bytes()));
    let name: Vec<u16> =
        std::ffi::OsStr::new(&format!("Global\\StillyardEndpoint-{}", &digest[..32]))
            .encode_wide()
            .chain(std::iter::once(0))
            .collect();
    let sddl: Vec<u16> = std::ffi::OsStr::new(&format!("D:P(A;;GA;;;{sid})"))
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    let mut descriptor: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
    // SAFETY: SDDL is NUL-terminated and descriptor is writable.
    if unsafe {
        ConvertStringSecurityDescriptorToSecurityDescriptorW(
            sddl.as_ptr(),
            SDDL_REVISION_1,
            &mut descriptor,
            std::ptr::null_mut(),
        )
    } == 0
    {
        return Err(Error::Unavailable(format!(
            "cannot secure daemon endpoint lease: {}",
            std::io::Error::last_os_error()
        )));
    }
    struct Descriptor(*mut c_void);
    impl Drop for Descriptor {
        fn drop(&mut self) {
            if !self.0.is_null() {
                // SAFETY: the descriptor was allocated by the SDDL conversion API.
                unsafe { LocalFree(self.0) };
            }
        }
    }
    let descriptor = Descriptor(descriptor);
    let attributes = SECURITY_ATTRIBUTES {
        nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: descriptor.0,
        bInheritHandle: 0,
    };
    // SAFETY: name is NUL-terminated; attributes points to a live owner-only descriptor and the
    // returned non-inheritable handle is retained for the complete daemon lifetime.
    let handle = unsafe { CreateMutexW(&attributes, 0, name.as_ptr()) };
    if handle.is_null() {
        return Err(Error::Unavailable(format!(
            "cannot claim daemon endpoint {endpoint:?}: {}",
            std::io::Error::last_os_error()
        )));
    }
    // SAFETY: GetLastError must be read immediately after CreateMutexW.
    if unsafe { GetLastError() } == ERROR_ALREADY_EXISTS {
        // SAFETY: this branch still owns the returned reference to the existing mutex.
        unsafe { CloseHandle(handle) };
        return Err(Error::Unavailable(format!(
            "daemon endpoint is already owned: {endpoint}"
        )));
    }
    Ok(EndpointLease(handle))
}

struct Scheduler {
    signal: Arc<(Mutex<bool>, Condvar)>,
    events: Arc<(Mutex<u64>, Condvar)>,
    endpoint: Arc<str>,
}

fn submission_context(
    store: &SharedStore,
    peer: Option<&PeerProcess>,
) -> std::result::Result<crate::SubmissionContext, StoreError> {
    let (store_uuid, candidates) = {
        let store = store
            .lock()
            .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?;
        (store.store_uuid(), store.managed_containment_candidates()?)
    };
    let parent = match peer {
        Some(peer) => authenticate_managed_peer(peer, &candidates)?,
        None => None,
    };
    Ok(crate::SubmissionContext { store_uuid, parent })
}

fn resolve_managed_membership(
    candidates: &[ManagedCandidate],
    mut is_member: impl FnMut(crate::InvocationId) -> std::io::Result<Option<bool>>,
) -> std::result::Result<Option<crate::ManagedParent>, StoreError> {
    let mut matched = Vec::new();
    for candidate in candidates {
        match is_member(candidate.parent.invocation_id).map_err(StoreError::Io)? {
            Some(true) => {}
            Some(false) => continue,
            None => {
                return Err(StoreError::InvalidState(
                    "a possibly live managed Containment has no daemon-held handle".into(),
                ));
            }
        }
        matched.push(candidate);
    }
    if matched.is_empty() {
        return Ok(None);
    }

    // A process in a nested Windows Job hierarchy is a member of the immediate Job and every
    // ancestor Job. Select the unique leaf containment, then prove that every other match is on
    // its direct durable parent chain. Multiple leaves are an ambiguous authority match.
    let leaves = matched
        .iter()
        .copied()
        .filter(|candidate| {
            !matched
                .iter()
                .any(|other| other.parent_job_id == Some(candidate.parent.job_id))
        })
        .collect::<Vec<_>>();
    let [immediate] = leaves.as_slice() else {
        return Err(StoreError::Rejected(
            "named-pipe peer belongs to ambiguous Stillyard containments".into(),
        ));
    };
    let mut lineage = std::collections::HashSet::new();
    let mut current = Some(*immediate);
    while let Some(candidate) = current {
        if !lineage.insert(candidate.parent.job_id) {
            return Err(StoreError::InvalidState(
                "managed containment parent graph contains a cycle".into(),
            ));
        }
        current = candidate.parent_job_id.and_then(|parent_job_id| {
            matched
                .iter()
                .copied()
                .find(|parent| parent.parent.job_id == parent_job_id)
        });
    }
    if lineage.len() != matched.len() {
        return Err(StoreError::Rejected(
            "named-pipe peer belongs to unrelated Stillyard containments".into(),
        ));
    }
    if !immediate.current {
        return Err(StoreError::Rejected(
            "the containing primary is no longer current and live".into(),
        ));
    }
    if !immediate.submissions_enabled {
        return Err(StoreError::Rejected(
            "the containing primary does not allow child submissions".into(),
        ));
    }
    Ok(Some(immediate.parent))
}

#[cfg(windows)]
fn authenticate_managed_peer(
    peer: &PeerProcess,
    candidates: &[ManagedCandidate],
) -> std::result::Result<Option<crate::ManagedParent>, StoreError> {
    resolve_managed_membership(candidates, |invocation_id| {
        crate::runner::process_in_containment(invocation_id, peer.handle)
    })
    .map_err(|error| match error {
        StoreError::Io(source) => StoreError::InvalidState(format!(
            "cannot inspect named-pipe peer {}: {source}",
            peer.pid
        )),
        other => other,
    })
}

#[cfg(not(windows))]
fn authenticate_managed_peer(
    _peer: &PeerProcess,
    _candidates: &[ManagedCandidate],
) -> std::result::Result<Option<crate::ManagedParent>, StoreError> {
    Ok(None)
}

impl Scheduler {
    fn start(store: SharedStore, endpoint: String) -> Arc<Self> {
        let scheduler = Arc::new(Self {
            signal: Arc::new((Mutex::new(false), Condvar::new())),
            events: Arc::new((Mutex::new(0), Condvar::new())),
            endpoint: Arc::from(endpoint),
        });
        let worker = Arc::clone(&scheduler);
        std::thread::Builder::new()
            .name("stillyard-scheduler".into())
            .spawn(move || worker.run(store))
            .expect("scheduler thread must start");
        scheduler
    }

    fn wake(&self) {
        let (lock, condition) = &*self.signal;
        if let Ok(mut pending) = lock.lock() {
            *pending = true;
            condition.notify_one();
        }
        self.notify_change();
    }

    fn notify_change(&self) {
        let (lock, condition) = &*self.events;
        if let Ok(mut generation) = lock.lock() {
            *generation = generation.wrapping_add(1);
            condition.notify_all();
        }
    }

    fn wait_snapshot(
        &self,
        store: &SharedStore,
        job_id: crate::JobId,
        max_wait: Duration,
    ) -> std::result::Result<crate::JobSnapshot, StoreError> {
        let deadline = Instant::now() + max_wait;
        loop {
            let observed = self
                .events
                .0
                .lock()
                .map_err(|_| StoreError::InvalidState("event mutex poisoned".into()))
                .map(|generation| *generation)?;
            let snapshot = store
                .lock()
                .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?
                .status(job_id)?;
            if snapshot.is_final() || Instant::now() >= deadline {
                return Ok(snapshot);
            }
            let (lock, condition) = &*self.events;
            let mut generation = lock
                .lock()
                .map_err(|_| StoreError::InvalidState("event mutex poisoned".into()))?;
            while *generation == observed {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                let waited = condition
                    .wait_timeout(generation, remaining)
                    .map_err(|_| StoreError::InvalidState("event mutex poisoned".into()))?;
                generation = waited.0;
                if waited.1.timed_out() {
                    break;
                }
            }
        }
    }

    fn wait_observation(
        &self,
        store: &SharedStore,
        selector: &crate::JobSelector,
        cursor: Option<crate::EventCursor>,
        limit: u32,
        max_wait: Duration,
    ) -> std::result::Result<crate::ObservationFrame, StoreError> {
        let deadline = Instant::now() + max_wait;
        let mut cursor = cursor;
        loop {
            let observed = self
                .events
                .0
                .lock()
                .map_err(|_| StoreError::InvalidState("event mutex poisoned".into()))
                .map(|generation| *generation)?;
            let requested = cursor;
            let frame = store
                .lock()
                .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?
                .observe(selector, cursor, limit)?;
            let ready = match &frame {
                crate::ObservationFrame::Events { events, .. } => !events.is_empty(),
                crate::ObservationFrame::Gap { .. } => true,
            } || requested != Some(frame.cursor());
            if ready || Instant::now() >= deadline {
                return Ok(frame);
            }
            cursor = Some(frame.cursor());
            let (lock, condition) = &*self.events;
            let mut generation = lock
                .lock()
                .map_err(|_| StoreError::InvalidState("event mutex poisoned".into()))?;
            while *generation == observed {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                let waited = condition
                    .wait_timeout(generation, remaining)
                    .map_err(|_| StoreError::InvalidState("event mutex poisoned".into()))?;
                generation = waited.0;
                if waited.1.timed_out() {
                    break;
                }
            }
        }
    }

    fn run(self: Arc<Self>, store: SharedStore) {
        const RECONCILIATION_BACKOFF_SECONDS: [u64; 9] = [1, 2, 4, 8, 16, 30, 60, 120, 300];
        let mut reconciliation_cursor = 0_u64;
        let mut reconciliation_known_latest = 0_u64;
        let mut reconciliation_backoff = 0_usize;
        let mut reconciliation_deadline: Option<Instant> = None;
        loop {
            let newest_incident = store
                .lock()
                .ok()
                .and_then(|guard| guard.latest_unresolved_incident_sequence().ok())
                .flatten();
            let new_incident =
                newest_incident.is_some_and(|sequence| sequence > reconciliation_known_latest);
            if new_incident {
                reconciliation_known_latest =
                    newest_incident.unwrap_or(reconciliation_known_latest);
                reconciliation_backoff = 0;
            }
            let reconciliation_due = new_incident
                || reconciliation_deadline.is_none()
                || reconciliation_deadline.is_some_and(|deadline| Instant::now() >= deadline);
            if reconciliation_due {
                let snapshot = store.lock().ok().and_then(|guard| {
                    let context = guard.reconciliation_context()?;
                    let candidates = guard
                        .reconciliation_candidates(reconciliation_cursor, 32)
                        .ok()?;
                    Some((context, candidates))
                });
                if let Some((context, candidates)) = snapshot {
                    if candidates.is_empty() {
                        reconciliation_deadline = None;
                        reconciliation_backoff = 0;
                    } else {
                        for candidate in &candidates {
                            let (resolution, evidence) =
                                probe_reconciliation_candidate(candidate, &context);
                            if let Ok(mut guard) = store.lock() {
                                guard.record_reconciliation_observation(
                                    candidate.containment_id,
                                    evidence.clone(),
                                );
                            }
                            if let Some(resolution) = resolution {
                                let committed = store.lock().ok().and_then(|mut guard| {
                                    guard
                                        .commit_containment_resolution(
                                            candidate,
                                            resolution,
                                            evidence,
                                            crate::ClearanceOrigin::Automatic,
                                            None,
                                            None,
                                        )
                                        .ok()
                                        .flatten()
                                });
                                if let Some(committed) = committed {
                                    crate::runner::clear_containment_registration(
                                        candidate.invocation_id,
                                    );
                                    if committed.audit.lease_released {
                                        if let Ok(mut pending) = self.signal.0.lock() {
                                            *pending = true;
                                        }
                                    }
                                }
                            }
                        }
                        reconciliation_cursor = candidates
                            .last()
                            .map_or(reconciliation_cursor, |candidate| {
                                candidate.incident_sequence
                            });
                        let unresolved = store
                            .lock()
                            .ok()
                            .and_then(|guard| guard.latest_unresolved_incident_sequence().ok())
                            .flatten()
                            .is_some();
                        if unresolved {
                            let delay = RECONCILIATION_BACKOFF_SECONDS[reconciliation_backoff
                                .min(RECONCILIATION_BACKOFF_SECONDS.len() - 1)];
                            reconciliation_backoff = reconciliation_backoff.saturating_add(1);
                            reconciliation_deadline =
                                Instant::now().checked_add(Duration::from_secs(delay));
                        } else {
                            reconciliation_deadline = None;
                            reconciliation_backoff = 0;
                        }
                    }
                }
            }
            let retry_scan_started = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .try_into()
                .unwrap_or(i64::MAX);
            let mut retry = false;
            let next = {
                let mut guard = match store.lock() {
                    Ok(guard) => guard,
                    Err(_) => return,
                };
                match guard.prepare_next_job_with_progress() {
                    Ok(job) => job,
                    Err(_) => {
                        retry = true;
                        crate::store::PrepareNext {
                            job: None,
                            // prepare_next_job may have committed skip closure before a later
                            // SQLite error. A spurious notification is safer than hiding it.
                            state_changed: true,
                        }
                    }
                }
            };
            if next.state_changed {
                self.notify_change();
            }
            if let Some(job) = next.job {
                self.notify_change();
                let worker_store = Arc::clone(&store);
                let worker_scheduler = Arc::clone(&self);
                let worker_endpoint = Arc::clone(&self.endpoint);
                let thread_job = job.clone();
                let spawned = std::thread::Builder::new()
                    .name(format!("stillyard-job-{}", job.job_id.entity_uuid()))
                    .spawn(move || {
                        let wake_scheduler = Arc::downgrade(&worker_scheduler);
                        crate::runner::run(
                            thread_job,
                            worker_store,
                            worker_endpoint,
                            Arc::new(move || {
                                if let Some(scheduler) = wake_scheduler.upgrade() {
                                    scheduler.wake();
                                }
                            }),
                        );
                        worker_scheduler.wake();
                    });
                if let Err(error) = spawned {
                    if let Ok(mut guard) = store.lock() {
                        let _ = guard.mark_finished(
                            &job,
                            None,
                            crate::JobOutcome::Failed,
                            "start_failed",
                        );
                    }
                    eprintln!(
                        "stillyard could not start worker thread for {}: {error}",
                        job.job_id
                    );
                    self.wake();
                    std::thread::sleep(Duration::from_millis(100));
                }
                continue;
            }
            if retry {
                std::thread::sleep(Duration::from_millis(100));
                continue;
            }
            let retry_delay = store
                .lock()
                .ok()
                .and_then(|guard| guard.next_retry_delay(retry_scan_started).ok())
                .flatten();
            let retry_deadline = retry_delay.and_then(|delay| Instant::now().checked_add(delay));
            let wake_deadline = match (retry_deadline, reconciliation_deadline) {
                (Some(left), Some(right)) => Some(left.min(right)),
                (Some(deadline), None) | (None, Some(deadline)) => Some(deadline),
                (None, None) => None,
            };
            let (lock, condition) = &*self.signal;
            let mut pending = match lock.lock() {
                Ok(pending) => pending,
                Err(_) => return,
            };
            while !*pending {
                if let Some(deadline) = wake_deadline {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        break;
                    }
                    let waited = match condition.wait_timeout(pending, remaining) {
                        Ok(waited) => waited,
                        Err(_) => return,
                    };
                    pending = waited.0;
                    if waited.1.timed_out() {
                        break;
                    }
                } else {
                    pending = match condition.wait(pending) {
                        Ok(pending) => pending,
                        Err(_) => return,
                    };
                }
            }
            *pending = false;
        }
    }
}

fn probe_reconciliation_candidate(
    candidate: &crate::store::ReconciliationCandidate,
    context: &(crate::HostId, crate::BootId, uuid::Uuid),
) -> (
    Option<crate::ContainmentResolution>,
    crate::ReconciliationResult,
) {
    let (current_host, current_boot, current_generation) = context;
    if candidate.host_id.as_ref() != Some(current_host) {
        return (None, crate::ReconciliationResult::IdentityUnavailable);
    }
    let Some(recorded_boot) = candidate.boot_id.as_ref() else {
        return (None, crate::ReconciliationResult::IdentityUnavailable);
    };
    if recorded_boot != current_boot {
        return if candidate.daemon_generation != Some(*current_generation) {
            (
                Some(crate::ContainmentResolution::Reboot),
                crate::ReconciliationResult::PriorBoot,
            )
        } else {
            (None, crate::ReconciliationResult::IdentityUnavailable)
        };
    }
    if candidate.daemon_generation == Some(*current_generation) {
        return match crate::runner::inspect_owned_containment(candidate.invocation_id) {
            Ok(Some(crate::ReconciliationResult::ProvenEmpty)) => (
                Some(crate::ContainmentResolution::ProvenEmpty),
                crate::ReconciliationResult::ProvenEmpty,
            ),
            Ok(Some(evidence)) => (None, evidence),
            Ok(None) => (None, crate::ReconciliationResult::BoundaryUninspectable),
            Err(_) => (None, crate::ReconciliationResult::BoundaryUninspectable),
        };
    }
    let Some(prior_daemon) = candidate.prior_daemon_identity.as_ref() else {
        return (None, crate::ReconciliationResult::IdentityUnavailable);
    };
    let prior_daemon =
        crate::identity::probe_recorded_process(prior_daemon, current_host, current_boot);
    if !matches!(
        prior_daemon,
        crate::ReconciliationResult::IdentityAbsent | crate::ReconciliationResult::PidReused
    ) {
        return (None, prior_daemon);
    }
    let Some(root_identity) = candidate.root_identity.as_ref() else {
        if candidate.root_pid_recorded {
            return (None, crate::ReconciliationResult::IdentityUnavailable);
        }
        return (
            Some(crate::ContainmentResolution::ProvenEmpty),
            crate::ReconciliationResult::IdentityAbsent,
        );
    };
    let root = crate::identity::probe_recorded_process(root_identity, current_host, current_boot);
    if matches!(
        root,
        crate::ReconciliationResult::IdentityAbsent | crate::ReconciliationResult::PidReused
    ) {
        (Some(crate::ContainmentResolution::ProvenEmpty), root)
    } else {
        (None, root)
    }
}

fn authorize_force_peer(
    peer: &PeerProcess,
    requester: &crate::ProcessIdentity,
    authorization_invocations: &[crate::InvocationId],
    unresolved_roots: &[crate::ProcessIdentity],
) -> std::result::Result<(), StoreError> {
    for &invocation_id in authorization_invocations {
        match crate::runner::process_in_containment(invocation_id, peer.handle) {
            Ok(Some(true)) => {
                return Err(StoreError::OperationRejected {
                    code: "containment_caller_managed".into(),
                    detail: "a managed process cannot accept containment risk".into(),
                });
            }
            Ok(Some(false)) => {}
            Ok(None) | Err(_) => {
                return Err(StoreError::OperationRejected {
                    code: "containment_authorization_unavailable".into(),
                    detail: "a current-generation containment boundary cannot be inspected".into(),
                });
            }
        }
    }
    if unresolved_roots
        .iter()
        .any(|identity| identity == requester)
    {
        return Err(StoreError::OperationRejected {
            code: "containment_caller_managed".into(),
            detail: "the requester is an unresolved recorded containment root".into(),
        });
    }
    Ok(())
}

fn force_clear_containment(
    store: &SharedStore,
    scheduler: &Scheduler,
    peer: Option<&PeerProcess>,
    containment_id: crate::ContainmentId,
) -> std::result::Result<crate::ClearContainmentResult, StoreError> {
    let peer = peer.ok_or_else(|| StoreError::OperationRejected {
        code: "containment_requester_unidentifiable".into(),
        detail: "force-clear requires a connected peer process".into(),
    })?;
    let requester = peer
        .identity
        .clone()
        .ok_or_else(|| StoreError::OperationRejected {
            code: "containment_requester_unidentifiable".into(),
            detail: "the connection-time requester identity is unavailable".into(),
        })?;
    let (context, mut authorization_invocations, mut unresolved_roots) = {
        let guard = store
            .lock()
            .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?;
        let context =
            guard
                .reconciliation_context()
                .ok_or_else(|| StoreError::OperationRejected {
                    code: "containment_requester_unidentifiable".into(),
                    detail: "the daemon host/boot identity is unavailable".into(),
                })?;
        let (authorization_invocations, unresolved_roots) =
            guard.clearance_authorization_evidence()?;
        (context, authorization_invocations, unresolved_roots)
    };

    authorize_force_peer(
        peer,
        &requester,
        &authorization_invocations,
        &unresolved_roots,
    )?;
    let mut candidate = {
        let guard = store
            .lock()
            .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?;
        if let Some(result) = guard.persisted_clearance(containment_id)? {
            return Ok(result);
        }
        guard.reconciliation_candidate(containment_id)?
    };

    let (automatic_resolution, automatic_evidence) =
        probe_reconciliation_candidate(&candidate, &context);
    if let Ok(mut guard) = store.lock() {
        guard.record_reconciliation_observation(
            candidate.containment_id,
            automatic_evidence.clone(),
        );
    }
    if let Some(resolution) = automatic_resolution {
        if let Some(result) = store
            .lock()
            .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?
            .commit_containment_resolution(
                &candidate,
                resolution,
                automatic_evidence.clone(),
                crate::ClearanceOrigin::Automatic,
                None,
                None,
            )?
        {
            crate::runner::clear_containment_registration(candidate.invocation_id);
            if result.audit.lease_released {
                scheduler.wake();
            }
            return Ok(result);
        }
    }
    match automatic_evidence {
        crate::ReconciliationResult::BoundaryNotEmpty => {
            return Err(StoreError::OperationRejected {
                code: "containment_boundary_not_empty".into(),
                detail: "the daemon-owned containment boundary is known nonempty".into(),
            });
        }
        crate::ReconciliationResult::BoundaryUninspectable
            if candidate.daemon_generation == Some(context.2) =>
        {
            return Err(StoreError::OperationRejected {
                code: "containment_owned_boundary_uninspectable".into(),
                detail: "restart the daemon to close the owned boundary before risk acceptance"
                    .into(),
            });
        }
        _ => {}
    }
    if candidate.host_id.as_ref() != Some(&context.0) {
        return Err(StoreError::OperationRejected {
            code: "containment_host_mismatch".into(),
            detail: "containment host identity does not match the daemon".into(),
        });
    }
    let mut target_evidence = match candidate.root_identity.as_ref() {
        Some(identity) => crate::identity::probe_recorded_process(identity, &context.0, &context.1),
        None if !candidate.root_pid_recorded => crate::ReconciliationResult::IdentityAbsent,
        None => crate::ReconciliationResult::IdentityUnavailable,
    };
    match target_evidence {
        crate::ReconciliationResult::StillResolves => {
            return Err(StoreError::OperationRejected {
                code: "containment_identity_still_resolves".into(),
                detail: "the exact recorded root process is still running".into(),
            });
        }
        crate::ReconciliationResult::IdentityUnavailable => {
            return Err(StoreError::OperationRejected {
                code: "containment_identity_unavailable".into(),
                detail: "the exact recorded root identity cannot be inspected".into(),
            });
        }
        crate::ReconciliationResult::IdentityAbsent | crate::ReconciliationResult::PidReused => {}
        _ => {
            return Err(StoreError::OperationRejected {
                code: "containment_identity_unavailable".into(),
                detail: "the target identity has no affirmative absence evidence".into(),
            });
        }
    }
    let requested_unix_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX);
    let forced = crate::ForcedClearanceAudit {
        requested_unix_millis,
        requester,
    };
    for attempt in 0..2 {
        if let Some(result) = store
            .lock()
            .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?
            .commit_containment_resolution(
                &candidate,
                crate::ContainmentResolution::ForcedRiskAcceptance,
                target_evidence.clone(),
                crate::ClearanceOrigin::Forced,
                Some(forced.clone()),
                Some(&authorization_invocations),
            )?
        {
            if result.audit.lease_released {
                scheduler.wake();
            }
            return Ok(result);
        }
        if let Some(result) = store
            .lock()
            .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?
            .persisted_clearance(containment_id)?
        {
            return Ok(result);
        }
        if attempt == 0 {
            let guard = store
                .lock()
                .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?;
            if let Some(result) = guard.persisted_clearance(containment_id)? {
                return Ok(result);
            }
            candidate = guard.reconciliation_candidate(containment_id)?;
            drop(guard);
            if candidate.host_id.as_ref() != Some(&context.0) {
                return Err(StoreError::OperationRejected {
                    code: "containment_host_mismatch".into(),
                    detail: "containment host identity changed during force-clear".into(),
                });
            }
            target_evidence = match candidate.root_identity.as_ref() {
                Some(identity) => {
                    crate::identity::probe_recorded_process(identity, &context.0, &context.1)
                }
                None if !candidate.root_pid_recorded => crate::ReconciliationResult::IdentityAbsent,
                None => crate::ReconciliationResult::IdentityUnavailable,
            };
            if !matches!(
                target_evidence,
                crate::ReconciliationResult::IdentityAbsent
                    | crate::ReconciliationResult::PidReused
            ) {
                return Err(StoreError::OperationRejected {
                    code: "containment_identity_unavailable".into(),
                    detail: "containment evidence changed during force-clear".into(),
                });
            }
            (authorization_invocations, unresolved_roots) = store
                .lock()
                .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?
                .clearance_authorization_evidence()?;
            authorize_force_peer(
                peer,
                &forced.requester,
                &authorization_invocations,
                &unresolved_roots,
            )?;
            continue;
        }
    }
    Err(StoreError::OperationRejected {
        code: "containment_authorization_unavailable".into(),
        detail: "containment evidence changed during force-clear".into(),
    })
}

fn handle_request(
    store: &SharedStore,
    scheduler: &Scheduler,
    peer: Option<&PeerProcess>,
    request: Request,
) -> Response {
    let result = match request {
        Request::Ping {} => {
            return Response::Pong {
                protocol_version: PROTOCOL_VERSION,
            };
        }
        Request::StageBegin {
            upload_id,
            expected_sha256,
            expected_length,
        } => store
            .lock()
            .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))
            .and_then(|store| store.stage_begin(upload_id, &expected_sha256, expected_length))
            .map(|next_offset| Response::StageReady { next_offset }),
        Request::StageChunk {
            upload_id,
            offset,
            bytes,
        } => store
            .lock()
            .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))
            .and_then(|store| store.stage_chunk(upload_id, offset, &bytes))
            .map(|next_offset| Response::StageReady { next_offset }),
        Request::StageCommit { upload_id } => store
            .lock()
            .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))
            .and_then(|store| store.stage_commit(upload_id))
            .map(|input| Response::StageCommitted { input }),
        Request::SubmissionContext { claimed_parent } => {
            submission_context(store, peer).and_then(|context| {
                if claimed_parent.is_some() && claimed_parent != context.parent {
                    return Err(StoreError::Rejected(
                        "claimed managed parent does not match daemon-held OS containment".into(),
                    ));
                }
                Ok(Response::SubmissionContext(context))
            })
        }
        Request::Submit {
            idempotency_key,
            payload_hash,
            spec,
            stdin,
            expected_store_uuid,
            expected_parent,
            wait_for_completion,
        } => submission_context(store, peer)
            .and_then(|context| {
                if context.parent != expected_parent {
                    return Err(StoreError::Rejected(
                        "submission parent changed after client preflight".into(),
                    ));
                }
                store
                    .lock()
                    .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))
                    .and_then(|mut store| {
                        if expected_store_uuid
                            .is_some_and(|expected| expected != store.store_uuid())
                        {
                            return Err(StoreError::InvalidState(
                                "store identity changed during submission".into(),
                            ));
                        }
                        store.submit_with_stdin_scoped_for_wait(
                            context
                                .parent
                                .map_or(SubmissionScope::Unmanaged, SubmissionScope::Managed),
                            idempotency_key,
                            &payload_hash,
                            &spec,
                            stdin.as_ref(),
                            wait_for_completion,
                        )
                    })
            })
            .map(|submitted| {
                if submitted.should_schedule {
                    scheduler.wake();
                }
                Response::Submitted(submitted.receipt)
            }),
        Request::SubmitBatch {
            idempotency_key,
            payload_hash,
            spec,
            stdins,
            expected_store_uuid,
            expected_parent,
            wait_for_completion,
        } => submission_context(store, peer)
            .and_then(|context| {
                if context.parent != expected_parent {
                    return Err(StoreError::Rejected(
                        "submission parent changed after client preflight".into(),
                    ));
                }
                store
                    .lock()
                    .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))
                    .and_then(|mut store| {
                        if expected_store_uuid
                            .is_some_and(|expected| expected != store.store_uuid())
                        {
                            return Err(StoreError::InvalidState(
                                "store identity changed during submission".into(),
                            ));
                        }
                        store.submit_batch_with_stdins_scoped_for_wait(
                            context
                                .parent
                                .map_or(SubmissionScope::Unmanaged, SubmissionScope::Managed),
                            idempotency_key,
                            &payload_hash,
                            &spec,
                            &stdins,
                            wait_for_completion,
                        )
                    })
            })
            .map(|submitted| {
                if submitted.should_schedule {
                    scheduler.wake();
                }
                Response::BatchSubmitted(submitted.receipt)
            }),
        Request::Recover {
            idempotency_key,
            payload_hash,
            expected_parent,
        } => submission_context(store, peer).and_then(|context| {
            if context.parent != expected_parent {
                return Err(StoreError::Rejected(
                    "recovery parent changed after client preflight".into(),
                ));
            }
            store
                .lock()
                .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))
                .and_then(|store| {
                    store
                        .recover_submission_scoped(
                            context
                                .parent
                                .map_or(SubmissionScope::Unmanaged, SubmissionScope::Managed),
                            idempotency_key,
                            &payload_hash,
                        )
                        .map(|recovery| Response::Recovered {
                            store_uuid: store.store_uuid(),
                            recovery,
                        })
                })
        }),
        Request::Status { job_id } => store
            .lock()
            .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))
            .and_then(|store| store.status(job_id))
            .map(|snapshot| Response::Snapshot(Box::new(snapshot))),
        Request::List {
            selector,
            cursor,
            limit,
        } => store
            .lock()
            .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))
            .and_then(|store| store.list_jobs(&selector, cursor, limit))
            .map(Response::Listed),
        Request::Observe {
            selector,
            cursor,
            limit,
            max_wait_millis,
            managed_wait,
        } => (|| {
            if managed_wait {
                let context = submission_context(store, peer)?;
                let crate::JobSelector::Jobs { job_ids } = &selector else {
                    return Err(StoreError::InvalidSpec(
                        "managed wait observation requires explicit Job IDs".into(),
                    ));
                };
                store
                    .lock()
                    .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?
                    .validate_managed_wait(
                        context
                            .parent
                            .map_or(SubmissionScope::Unmanaged, SubmissionScope::Managed),
                        job_ids,
                    )?;
            }
            scheduler
                .wait_observation(
                    store,
                    &selector,
                    cursor,
                    limit,
                    Duration::from_millis(u64::from(max_wait_millis.min(60_000))),
                )
                .map(Response::Observed)
        })(),
        Request::Cancel { job_ids } => store
            .lock()
            .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))
            .and_then(|mut store| store.cancel_jobs(&job_ids))
            .map(|snapshots| {
                scheduler.wake();
                Response::Canceled { snapshots }
            }),
        Request::Wait {
            job_id,
            max_wait_millis,
            claimed_parent,
        } => submission_context(store, peer).and_then(|context| {
            if claimed_parent.is_some() && claimed_parent != context.parent {
                return Err(StoreError::Rejected(
                    "claimed managed parent does not match daemon-held OS containment".into(),
                ));
            }
            store
                .lock()
                .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?
                .validate_managed_wait(
                    context
                        .parent
                        .map_or(SubmissionScope::Unmanaged, SubmissionScope::Managed),
                    &[job_id],
                )?;
            scheduler
                .wait_snapshot(
                    store,
                    job_id,
                    Duration::from_millis(u64::from(max_wait_millis.min(1_000))),
                )
                .map(|snapshot| Response::Snapshot(Box::new(snapshot)))
        }),
        Request::Logs {
            job_id,
            stream,
            offset,
            limit,
        } => store
            .lock()
            .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))
            .and_then(|store| store.logs(job_id, stream, offset, limit))
            .map(Response::Logs),
        Request::DaemonStatus {} => store
            .lock()
            .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))
            .and_then(|store| store.daemon_status(&scheduler.endpoint))
            .map(Response::DaemonStatus),
        Request::Doctor { cursor, limit } => store
            .lock()
            .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))
            .and_then(|store| store.doctor(&scheduler.endpoint, cursor, limit))
            .map(|snapshot| Response::Doctor(Box::new(snapshot))),
        Request::ForceClearContainment { containment_id } => {
            force_clear_containment(store, scheduler, peer, containment_id)
                .map(Response::ContainmentCleared)
        }
    };
    result.unwrap_or_else(|error| match error {
        StoreError::NotFound(_) => Response::Error {
            code: "not_found".into(),
            message: error.to_string(),
        },
        StoreError::IdempotencyConflict => Response::Error {
            code: "idempotency_conflict".into(),
            message: error.to_string(),
        },
        StoreError::Rejected(_) => Response::Error {
            code: "rejected".into(),
            message: error.to_string(),
        },
        StoreError::OperationRejected { code, detail } => Response::Error {
            code,
            message: detail,
        },
        StoreError::BlockedByAncestor(detail) => Response::Error {
            code: "blocked_by_ancestor".into(),
            message: detail,
        },
        StoreError::ManagedWaitRejected { code, detail } => Response::Error {
            code,
            message: detail,
        },
        StoreError::InvalidSpec(_) => Response::Error {
            code: "invalid_spec".into(),
            message: error.to_string(),
        },
        _ => Response::Error {
            code: "store_error".into(),
            message: error.to_string(),
        },
    })
}

#[cfg(windows)]
fn serve(store: SharedStore, scheduler: Arc<Scheduler>, first_pipe: OwnedPipe) -> Result<()> {
    use std::fs::File;
    use std::os::windows::io::FromRawHandle;

    use windows_sys::Win32::Foundation::{
        CloseHandle, ERROR_PIPE_CONNECTED, FILETIME, GetLastError,
    };
    use windows_sys::Win32::System::Pipes::{ConnectNamedPipe, GetNamedPipeClientProcessId};
    use windows_sys::Win32::System::SystemInformation::GetSystemTimeAsFileTime;
    use windows_sys::Win32::System::Threading::{
        GetProcessTimes, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
    };

    fn filetime_value(value: FILETIME) -> u64 {
        u64::from(value.dwLowDateTime) | (u64::from(value.dwHighDateTime) << 32)
    }

    let peer_identity_context = store.lock().ok().and_then(|guard| {
        guard
            .reconciliation_context()
            .map(|(host_id, boot_id, _)| (host_id, boot_id))
    });
    let mut pending_pipe = Some(first_pipe);
    loop {
        let pipe = match pending_pipe.take() {
            Some(pipe) => pipe,
            None => match create_pipe_instance(scheduler.endpoint.as_ref(), false) {
                Ok(pipe) => pipe,
                Err(_) => {
                    std::thread::sleep(Duration::from_millis(25));
                    continue;
                }
            },
        };
        let handle = pipe.as_raw();
        // SAFETY: handle is a fresh named-pipe server handle.
        let connected = unsafe { ConnectNamedPipe(handle, std::ptr::null_mut()) };
        if connected == 0 {
            // SAFETY: GetLastError has no preconditions.
            let error = unsafe { GetLastError() };
            if error != ERROR_PIPE_CONNECTED {
                continue;
            }
        }
        let mut connection_observed: FILETIME = unsafe { std::mem::zeroed() };
        // SAFETY: connection_observed is writable. Capturing this before resolving/opening the
        // PID lets us reject a process created only after the connected peer exited.
        unsafe { GetSystemTimeAsFileTime(&mut connection_observed) };
        let mut peer_pid = 0_u32;
        // SAFETY: handle is a connected server pipe and peer_pid is writable.
        if unsafe { GetNamedPipeClientProcessId(handle, &mut peer_pid) } == 0 {
            continue;
        }
        // Open the peer before reading its frame. Keeping this kernel handle closes the large PID
        // reuse window between pipe identification and managed-containment authentication.
        // SAFETY: peer_pid came from the connected pipe; requested access is read-only.
        let peer_process = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, peer_pid) };
        if peer_process.is_null() {
            continue;
        }
        let mut creation: FILETIME = unsafe { std::mem::zeroed() };
        let mut exit: FILETIME = unsafe { std::mem::zeroed() };
        let mut kernel: FILETIME = unsafe { std::mem::zeroed() };
        let mut user: FILETIME = unsafe { std::mem::zeroed() };
        // A recycled PID necessarily belongs to a process created after the connection was
        // observed. Reject it before handing either handle to a worker.
        if unsafe {
            GetProcessTimes(
                peer_process,
                &mut creation,
                &mut exit,
                &mut kernel,
                &mut user,
            )
        } == 0
            || filetime_value(creation) > filetime_value(connection_observed)
        {
            // SAFETY: the process handle has not been transferred; the pipe guard closes itself.
            unsafe { CloseHandle(peer_process) };
            continue;
        }
        let store = Arc::clone(&store);
        let scheduler = Arc::clone(&scheduler);
        let peer_identity = peer_identity_context
            .as_ref()
            .and_then(|(host_id, boot_id)| {
                crate::identity::process_identity_from_handle(
                    peer_process,
                    peer_pid,
                    host_id,
                    boot_id,
                )
                .ok()
            });
        // Raw Windows handles are pointer-typed and therefore not `Send`; their integer value is
        // safe to transfer because this thread owns the handle after a successful connection.
        let handle_value = pipe.into_raw() as usize;
        let peer_process_value = peer_process as usize;
        let spawned = std::thread::Builder::new()
            .name("stillyard-client".into())
            .spawn(move || {
                let handle = handle_value as windows_sys::Win32::Foundation::HANDLE;
                // SAFETY: ownership of the valid pipe handle is transferred to File exactly once.
                let mut pipe = unsafe { File::from_raw_handle(handle as _) };
                let peer = PeerProcess {
                    handle: peer_process_value,
                    pid: peer_pid,
                    identity: peer_identity,
                };
                let response = match read_frame::<Request>(&mut pipe) {
                    Ok(request) => handle_request(&store, &scheduler, Some(&peer), request),
                    Err(error) => Response::Error {
                        code: "invalid_request".into(),
                        message: error.to_string(),
                    },
                };
                let _ = write_frame(&mut pipe, &response);
            });
        if spawned.is_err() {
            // SAFETY: ownership was not transferred because the thread was not created.
            unsafe {
                CloseHandle(handle);
                CloseHandle(peer_process);
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    }
}

#[cfg(not(windows))]
fn serve(_store: SharedStore, _scheduler: Arc<Scheduler>) -> Result<()> {
    Err(Error::UnsupportedPlatform(std::env::consts::OS))
}

#[cfg(all(test, windows))]
mod tests {
    use super::*;

    fn observation_scheduler() -> Arc<Scheduler> {
        Arc::new(Scheduler {
            signal: Arc::new((Mutex::new(false), Condvar::new())),
            events: Arc::new((Mutex::new(0), Condvar::new())),
            endpoint: Arc::from(r"\\.\pipe\stillyard-daemon-test"),
        })
    }

    #[test]
    fn endpoint_lease_is_exclusive_and_released_with_its_handle() {
        let endpoint = format!(r"\\.\pipe\stillyard-lease-test-{}", uuid::Uuid::now_v7());
        let first = acquire_endpoint_lease(&endpoint).unwrap();
        assert!(matches!(
            acquire_endpoint_lease(&endpoint),
            Err(Error::Unavailable(_))
        ));
        drop(first);
        acquire_endpoint_lease(&endpoint).unwrap();
    }

    #[test]
    fn explicit_instance_tuple_accepts_only_both_coordinates_or_neither() {
        for store_cli in [false, true] {
            for endpoint_cli in [false, true] {
                for store_env in [false, true] {
                    for endpoint_env in [false, true] {
                        let store_selected = store_cli || store_env;
                        let endpoint_selected = endpoint_cli || endpoint_env;
                        let result = validate_instance_tuple(store_selected, endpoint_selected);
                        assert_eq!(
                            result.is_ok(),
                            store_selected == endpoint_selected,
                            "store_cli={store_cli}, endpoint_cli={endpoint_cli}, store_env={store_env}, endpoint_env={endpoint_env}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn first_pipe_instance_is_exclusive_and_released_with_its_handle() {
        let endpoint = format!(r"\\.\pipe\stillyard-pipe-test-{}", uuid::Uuid::now_v7());
        let first = create_pipe_instance(&endpoint, true).unwrap();
        assert!(matches!(
            create_pipe_instance(&endpoint, true),
            Err(Error::Unavailable(_))
        ));
        drop(first);
        create_pipe_instance(&endpoint, true).unwrap();
    }

    fn candidate(store: uuid::Uuid, enabled: bool) -> ManagedCandidate {
        ManagedCandidate {
            parent: crate::ManagedParent {
                job_id: crate::JobId::from_parts(store, uuid::Uuid::now_v7()),
                attempt_id: crate::AttemptId::from_parts(store, uuid::Uuid::now_v7()),
                invocation_id: crate::InvocationId::from_parts(store, uuid::Uuid::now_v7()),
            },
            parent_job_id: None,
            submissions_enabled: enabled,
            current: true,
        }
    }

    #[test]
    fn peer_membership_derives_one_enabled_parent_and_rejects_ambiguity() {
        let store = uuid::Uuid::now_v7();
        let first = candidate(store, true);
        let second = candidate(store, true);
        assert_eq!(
            resolve_managed_membership(&[first], |id| {
                Ok(Some(id == first.parent.invocation_id))
            })
            .unwrap(),
            Some(first.parent)
        );
        assert!(matches!(
            resolve_managed_membership(&[first, second], |_| Ok(Some(true))),
            Err(StoreError::Rejected(_))
        ));
    }

    #[test]
    fn nested_membership_selects_the_unique_immediate_containment() {
        let store = uuid::Uuid::now_v7();
        let outer = candidate(store, true);
        let mut inner = candidate(store, true);
        inner.parent_job_id = Some(outer.parent.job_id);
        assert_eq!(
            resolve_managed_membership(&[outer, inner], |_| Ok(Some(true))).unwrap(),
            Some(inner.parent)
        );

        inner.submissions_enabled = false;
        assert!(matches!(
            resolve_managed_membership(&[outer, inner], |_| Ok(Some(true))),
            Err(StoreError::Rejected(_))
        ));
    }

    #[test]
    fn peer_inside_disabled_primary_is_rejected_not_downgraded_to_unmanaged() {
        let candidate = candidate(uuid::Uuid::now_v7(), false);
        assert!(matches!(
            resolve_managed_membership(&[candidate], |_| Ok(Some(true))),
            Err(StoreError::Rejected(_))
        ));
    }

    #[test]
    fn peer_inside_root_exited_or_uncertain_containment_is_rejected() {
        let mut candidate = candidate(uuid::Uuid::now_v7(), true);
        candidate.current = false;
        assert!(matches!(
            resolve_managed_membership(&[candidate], |_| Ok(Some(true))),
            Err(StoreError::Rejected(_))
        ));
    }

    #[test]
    fn missing_handle_never_downgrades_a_possible_managed_peer_to_unmanaged() {
        let mut candidate = candidate(uuid::Uuid::now_v7(), true);
        candidate.current = false;
        assert!(matches!(
            resolve_managed_membership(&[candidate], |_| Ok(None)),
            Err(StoreError::InvalidState(_))
        ));
    }

    #[test]
    fn singleton_lock_is_acquired_before_destructive_store_open() {
        let temp = tempfile::tempdir().unwrap();
        let paths = StorePaths::new(temp.path().to_path_buf());
        paths.ensure().unwrap();
        let connection = rusqlite::Connection::open(&paths.database).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
                 INSERT INTO meta(key, value) VALUES ('schema_epoch', 'obsolete-schema');",
            )
            .unwrap();
        drop(connection);

        let held_lock = open_lock(&paths.lock).unwrap();
        held_lock.try_lock_exclusive().unwrap();
        assert!(matches!(
            open_store_under_lock(StorePaths::new(temp.path().to_path_buf())),
            Err(Error::Unavailable(message)) if message.contains("daemon already running")
        ));

        let connection = rusqlite::Connection::open(&paths.database).unwrap();
        let epoch: String = connection
            .query_row(
                "SELECT value FROM meta WHERE key = 'schema_epoch'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(epoch, "obsolete-schema");
    }

    #[test]
    fn commit_at_wait_boundary_wakes_from_durable_event() {
        let temp = tempfile::tempdir().unwrap();
        let paths = StorePaths::new(temp.path().to_path_buf());
        let mut store = Store::open_with_capacities(
            paths,
            crate::ResourceCapacities {
                cpu_units: 1,
                ..Default::default()
            },
        )
        .unwrap();
        let spec = crate::JobSpec {
            spec_version: crate::SPEC_VERSION,
            executable: temp.path().join("tool.exe"),
            args: Vec::new(),
            working_directory: temp.path().to_path_buf(),
            stdin: crate::StdinSpec::Eof,
            environment: Default::default(),
            resources: Default::default(),
            conditions: Vec::new(),
            retry: Default::default(),
            postconditions: Vec::new(),
            labels: Vec::new(),
            expected_duration_seconds: None,
            timeout_seconds: None,
            quiet: None,
            artifacts: Vec::new(),
            allow_child_submissions: false,
        };
        let hash = crate::store::normalized_payload_hash(&spec).unwrap();
        let receipt = store
            .submit(uuid::Uuid::now_v7(), &hash, &spec)
            .unwrap()
            .receipt;
        let cursor = store
            .list_jobs(&crate::JobSelector::All, None, 1)
            .unwrap()
            .event_cursor;
        let scheduler = observation_scheduler();
        let notifier = Arc::downgrade(&scheduler);
        store.set_change_notifier(Arc::new(move || {
            if let Some(notifier) = notifier.upgrade() {
                notifier.notify_change();
            }
        }));
        let store = Arc::new(Mutex::new(store));
        let waiting_store = Arc::clone(&store);
        let waiting_scheduler = Arc::clone(&scheduler);
        let waiter = std::thread::spawn(move || {
            waiting_scheduler.wait_observation(
                &waiting_store,
                &crate::JobSelector::All,
                Some(cursor),
                16,
                Duration::from_secs(2),
            )
        });
        std::thread::sleep(Duration::from_millis(25));
        let committed_at = Instant::now();
        store
            .lock()
            .unwrap()
            .commit_log_offset(receipt.job_id, crate::LogStream::Stdout, 7)
            .unwrap();
        let frame = waiter.join().unwrap().unwrap();
        assert!(
            committed_at.elapsed() < Duration::from_millis(500),
            "waiter slept until its timeout instead of consuming the notification"
        );
        assert!(matches!(
            &frame,
            crate::ObservationFrame::Events { events, .. }
                if events.iter().any(|event| event.kind == crate::SchedulerEventKind::LogCommitted)
        ));

        let before_second = frame.cursor();
        store
            .lock()
            .unwrap()
            .commit_log_offset(receipt.job_id, crate::LogStream::Stdout, 8)
            .unwrap();
        let started = Instant::now();
        let already_committed = scheduler
            .wait_observation(
                &store,
                &crate::JobSelector::All,
                Some(before_second),
                16,
                Duration::from_secs(2),
            )
            .unwrap();
        assert!(started.elapsed() < Duration::from_millis(100));
        assert!(matches!(
            already_committed,
            crate::ObservationFrame::Events { ref events, .. }
                if events.iter().any(|event| event.kind == crate::SchedulerEventKind::LogCommitted)
        ));

        let invalidation_cursor = store
            .lock()
            .unwrap()
            .list_jobs(&crate::JobSelector::All, None, 1)
            .unwrap()
            .event_cursor;
        let other = crate::JobSpec {
            labels: vec![crate::Label {
                key: "other".into(),
                value: "job".into(),
            }],
            ..spec
        };
        let other_hash = crate::store::normalized_payload_hash(&other).unwrap();
        store
            .lock()
            .unwrap()
            .submit(uuid::Uuid::now_v7(), &other_hash, &other)
            .unwrap();
        let started = Instant::now();
        let invalidation = scheduler
            .wait_observation(
                &store,
                &crate::JobSelector::Jobs {
                    job_ids: vec![receipt.job_id],
                },
                Some(invalidation_cursor),
                16,
                Duration::from_secs(2),
            )
            .unwrap();
        assert!(started.elapsed() < Duration::from_millis(100));
        assert!(matches!(
            invalidation,
            crate::ObservationFrame::Events { ref events, cursor }
                if events.is_empty() && cursor.sequence > invalidation_cursor.sequence
        ));
    }

    #[test]
    fn boot_change_is_proof_only_for_a_prior_generation() {
        let store_uuid = uuid::Uuid::now_v7();
        let generation = uuid::Uuid::now_v7();
        let host = crate::HostId("host".into());
        let candidate = crate::store::ReconciliationCandidate {
            containment_id: crate::ContainmentId::new(store_uuid),
            invocation_id: crate::InvocationId::new(store_uuid),
            attempt_id: crate::AttemptId::new(store_uuid),
            version: 1,
            host_id: Some(host.clone()),
            boot_id: Some(crate::BootId("prior-boot".into())),
            daemon_generation: Some(generation),
            root_pid_recorded: false,
            root_identity: None,
            prior_daemon_identity: None,
            incident_sequence: 1,
        };
        let current = (host, crate::BootId("current-boot".into()), generation);
        assert_eq!(
            probe_reconciliation_candidate(&candidate, &current),
            (None, crate::ReconciliationResult::IdentityUnavailable)
        );
        let mut prior = candidate;
        prior.daemon_generation = Some(uuid::Uuid::now_v7());
        assert_eq!(
            probe_reconciliation_candidate(&prior, &current),
            (
                Some(crate::ContainmentResolution::Reboot),
                crate::ReconciliationResult::PriorBoot
            )
        );
    }

    #[test]
    fn prior_generation_requires_absent_daemon_and_exact_root_evidence() {
        let startup = crate::identity::probe_startup_identity();
        let host = startup.host_id.unwrap();
        let boot = startup.boot_id.unwrap();
        let current_process = startup.daemon_process.unwrap();
        let absent_daemon = match current_process.clone() {
            crate::ProcessIdentity::Windows {
                host_id,
                boot_id,
                pid,
                creation_filetime_100ns,
            } => crate::ProcessIdentity::Windows {
                host_id,
                boot_id,
                pid,
                creation_filetime_100ns: creation_filetime_100ns.saturating_add(1),
            },
            _ => unreachable!("Windows test requires Windows process identity"),
        };
        let store_uuid = uuid::Uuid::now_v7();
        let current_generation = uuid::Uuid::now_v7();
        let current = (host.clone(), boot.clone(), current_generation);
        let candidate = crate::store::ReconciliationCandidate {
            containment_id: crate::ContainmentId::new(store_uuid),
            invocation_id: crate::InvocationId::new(store_uuid),
            attempt_id: crate::AttemptId::new(store_uuid),
            version: 1,
            host_id: Some(host.clone()),
            boot_id: Some(boot),
            daemon_generation: Some(uuid::Uuid::now_v7()),
            root_pid_recorded: false,
            root_identity: None,
            prior_daemon_identity: Some(absent_daemon),
            incident_sequence: 1,
        };
        assert_eq!(
            probe_reconciliation_candidate(&candidate, &current),
            (
                Some(crate::ContainmentResolution::ProvenEmpty),
                crate::ReconciliationResult::IdentityAbsent
            )
        );

        let mut live_root = candidate.clone();
        live_root.root_pid_recorded = true;
        live_root.root_identity = Some(current_process);
        assert_eq!(
            probe_reconciliation_candidate(&live_root, &current),
            (None, crate::ReconciliationResult::StillResolves)
        );

        let mut pid_only = candidate.clone();
        pid_only.root_pid_recorded = true;
        assert_eq!(
            probe_reconciliation_candidate(&pid_only, &current),
            (None, crate::ReconciliationResult::IdentityUnavailable)
        );

        let mut foreign_host = candidate;
        foreign_host.host_id = Some(crate::HostId("foreign".into()));
        assert_eq!(
            probe_reconciliation_candidate(&foreign_host, &current),
            (None, crate::ReconciliationResult::IdentityUnavailable)
        );
    }

    #[test]
    fn force_authorization_fails_closed_for_roots_and_missing_handles() {
        let startup = crate::identity::probe_startup_identity();
        let requester = startup.daemon_process.unwrap();
        let peer = PeerProcess {
            handle: 0,
            pid: std::process::id(),
            identity: Some(requester.clone()),
        };
        assert!(matches!(
            authorize_force_peer(&peer, &requester, &[], std::slice::from_ref(&requester)),
            Err(StoreError::OperationRejected { code, .. })
                if code == "containment_caller_managed"
        ));
        assert!(matches!(
            authorize_force_peer(
                &peer,
                &requester,
                &[crate::InvocationId::new(uuid::Uuid::now_v7())],
                &[],
            ),
            Err(StoreError::OperationRejected { code, .. })
                if code == "containment_authorization_unavailable"
        ));
    }
}
