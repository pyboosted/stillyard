use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

#[cfg(windows)]
use fs2::FileExt;

#[cfg(windows)]
use crate::client::default_store_root;
#[cfg(windows)]
use crate::client::{current_user_sid_string, default_endpoint};
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
pub(crate) fn run() -> Result<()> {
    let (_lock, store) = open_store_under_lock(StorePaths::new(default_store_root()?))?;
    let store = Arc::new(Mutex::new(store));
    let scheduler = Scheduler::start(Arc::clone(&store));
    scheduler.wake();
    serve(store, scheduler)
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
pub(crate) fn run() -> Result<()> {
    Err(Error::UnsupportedPlatform(std::env::consts::OS))
}

struct Scheduler {
    signal: Arc<(Mutex<bool>, Condvar)>,
    events: Arc<(Mutex<u64>, Condvar)>,
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
    let mut matched = None;
    for candidate in candidates {
        match is_member(candidate.parent.invocation_id).map_err(StoreError::Io)? {
            Some(true) => {}
            Some(false) => continue,
            None if candidate.current => {
                return Err(StoreError::InvalidState(
                    "current managed Containment has no live daemon handle".into(),
                ));
            }
            None => continue,
        }
        if matched.is_some() {
            return Err(StoreError::Rejected(
                "named-pipe peer belongs to multiple Stillyard containments".into(),
            ));
        }
        if !candidate.current {
            return Err(StoreError::Rejected(
                "the containing primary is no longer current and live".into(),
            ));
        }
        if !candidate.submissions_enabled {
            return Err(StoreError::Rejected(
                "the containing primary does not allow child submissions".into(),
            ));
        }
        matched = Some(candidate.parent);
    }
    Ok(matched)
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
    fn start(store: SharedStore) -> Arc<Self> {
        let scheduler = Arc::new(Self {
            signal: Arc::new((Mutex::new(false), Condvar::new())),
            events: Arc::new((Mutex::new(0), Condvar::new())),
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

    fn run(self: Arc<Self>, store: SharedStore) {
        loop {
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
                let thread_job = job.clone();
                let spawned = std::thread::Builder::new()
                    .name(format!("stillyard-job-{}", job.job_id.entity_uuid()))
                    .spawn(move || {
                        crate::runner::run(thread_job, worker_store);
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
            let (lock, condition) = &*self.signal;
            let mut pending = match lock.lock() {
                Ok(pending) => pending,
                Err(_) => return,
            };
            while !*pending {
                pending = match condition.wait(pending) {
                    Ok(pending) => pending,
                    Err(_) => return,
                };
            }
            *pending = false;
        }
    }
}

fn handle_request(
    store: &SharedStore,
    scheduler: &Scheduler,
    peer: Option<&PeerProcess>,
    request: Request,
) -> Response {
    let result = match request {
        Request::Ping => {
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
                        store.submit_with_stdin_scoped(
                            context
                                .parent
                                .map_or(SubmissionScope::Unmanaged, SubmissionScope::Managed),
                            idempotency_key,
                            &payload_hash,
                            &spec,
                            stdin.as_ref(),
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
                        store.submit_batch_with_stdins_scoped(
                            context
                                .parent
                                .map_or(SubmissionScope::Unmanaged, SubmissionScope::Managed),
                            idempotency_key,
                            &payload_hash,
                            &spec,
                            &stdins,
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
        Request::Wait {
            job_id,
            max_wait_millis,
        } => scheduler
            .wait_snapshot(
                store,
                job_id,
                Duration::from_millis(u64::from(max_wait_millis.min(1_000))),
            )
            .map(|snapshot| Response::Snapshot(Box::new(snapshot))),
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
        Request::DaemonStatus => store
            .lock()
            .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))
            .and_then(|store| store.daemon_status())
            .map(Response::DaemonStatus),
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
fn serve(store: SharedStore, scheduler: Arc<Scheduler>) -> Result<()> {
    use std::ffi::c_void;
    use std::fs::File;
    use std::mem::size_of;
    use std::os::windows::ffi::OsStrExt;
    use std::os::windows::io::FromRawHandle;

    use windows_sys::Win32::Foundation::{
        CloseHandle, ERROR_PIPE_CONNECTED, FILETIME, GetLastError, INVALID_HANDLE_VALUE, LocalFree,
    };
    use windows_sys::Win32::Security::Authorization::{
        ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1,
    };
    use windows_sys::Win32::Security::{PSECURITY_DESCRIPTOR, SECURITY_ATTRIBUTES};
    use windows_sys::Win32::Storage::FileSystem::PIPE_ACCESS_DUPLEX;
    use windows_sys::Win32::System::Pipes::{
        ConnectNamedPipe, CreateNamedPipeW, GetNamedPipeClientProcessId,
        PIPE_REJECT_REMOTE_CLIENTS, PIPE_TYPE_BYTE, PIPE_WAIT,
    };
    use windows_sys::Win32::System::SystemInformation::GetSystemTimeAsFileTime;
    use windows_sys::Win32::System::Threading::{
        GetProcessTimes, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
    };

    fn filetime_value(value: FILETIME) -> u64 {
        u64::from(value.dwLowDateTime) | (u64::from(value.dwHighDateTime) << 32)
    }

    struct LocalAllocation(*mut c_void);

    impl Drop for LocalAllocation {
        fn drop(&mut self) {
            if !self.0.is_null() {
                // SAFETY: the pointer came from a Windows API documented to use LocalAlloc.
                unsafe { LocalFree(self.0) };
            }
        }
    }

    struct PipeSecurity {
        descriptor: LocalAllocation,
    }

    impl PipeSecurity {
        fn owner_only() -> std::io::Result<Self> {
            let sid = current_user_sid_string().map_err(std::io::Error::other)?;
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
                return Err(std::io::Error::last_os_error());
            }
            Ok(Self {
                descriptor: LocalAllocation(descriptor),
            })
        }

        fn attributes(&mut self) -> SECURITY_ATTRIBUTES {
            SECURITY_ATTRIBUTES {
                nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
                lpSecurityDescriptor: self.descriptor.0,
                bInheritHandle: 0,
            }
        }
    }

    let endpoint = default_endpoint()?;
    let pipe_name: Vec<u16> = std::ffi::OsStr::new(&endpoint)
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    loop {
        let mut security = PipeSecurity::owner_only()?;
        let attributes = security.attributes();
        // SAFETY: pipe_name is NUL-terminated and attributes points to a live self-relative
        // descriptor whose DACL grants access only to the daemon owner's SID.
        let handle = unsafe {
            CreateNamedPipeW(
                pipe_name.as_ptr(),
                PIPE_ACCESS_DUPLEX,
                PIPE_TYPE_BYTE | PIPE_WAIT | PIPE_REJECT_REMOTE_CLIENTS,
                255,
                64 * 1024,
                64 * 1024,
                5_000,
                &attributes,
            )
        };
        if handle == INVALID_HANDLE_VALUE {
            std::thread::sleep(Duration::from_millis(25));
            continue;
        }
        // SAFETY: handle is a fresh named-pipe server handle.
        let connected = unsafe { ConnectNamedPipe(handle, std::ptr::null_mut()) };
        if connected == 0 {
            // SAFETY: GetLastError has no preconditions.
            let error = unsafe { GetLastError() };
            if error != ERROR_PIPE_CONNECTED {
                // SAFETY: handle is owned by this function and not converted to File.
                unsafe { CloseHandle(handle) };
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
            // SAFETY: ownership has not yet been transferred to File.
            unsafe { CloseHandle(handle) };
            continue;
        }
        // Open the peer before reading its frame. Keeping this kernel handle closes the large PID
        // reuse window between pipe identification and managed-containment authentication.
        // SAFETY: peer_pid came from the connected pipe; requested access is read-only.
        let peer_process = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, peer_pid) };
        if peer_process.is_null() {
            // SAFETY: ownership has not yet been transferred to File.
            unsafe { CloseHandle(handle) };
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
            // SAFETY: neither handle has been transferred.
            unsafe {
                CloseHandle(handle);
                CloseHandle(peer_process);
            }
            continue;
        }
        let store = Arc::clone(&store);
        let scheduler = Arc::clone(&scheduler);
        // Raw Windows handles are pointer-typed and therefore not `Send`; their integer value is
        // safe to transfer because this thread owns the handle after a successful connection.
        let handle_value = handle as usize;
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

    fn candidate(store: uuid::Uuid, enabled: bool) -> ManagedCandidate {
        ManagedCandidate {
            parent: crate::ManagedParent {
                job_id: crate::JobId::from_parts(store, uuid::Uuid::now_v7()),
                attempt_id: crate::AttemptId::from_parts(store, uuid::Uuid::now_v7()),
                invocation_id: crate::InvocationId::from_parts(store, uuid::Uuid::now_v7()),
            },
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
    fn singleton_lock_is_acquired_before_destructive_store_open() {
        let temp = tempfile::tempdir().unwrap();
        let paths = StorePaths::new(temp.path().to_path_buf());
        paths.ensure().unwrap();
        let connection = rusqlite::Connection::open(&paths.database).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
                 INSERT INTO meta(key, value) VALUES ('schema_epoch', 'obsolete-alpha-schema');",
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
        assert_eq!(epoch, "obsolete-alpha-schema");
    }
}
