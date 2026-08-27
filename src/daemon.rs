use std::sync::{Arc, Condvar, Mutex};

use fs2::FileExt;

use crate::client::{DEFAULT_PIPE_NAME, default_store_root};
use crate::protocol::{PROTOCOL_VERSION, Request, Response, read_frame, write_frame};
use crate::store::{Store, StoreError, StorePaths, open_lock};
use crate::{Error, Result};

type SharedStore = Arc<Mutex<Store>>;

pub(crate) fn run() -> Result<()> {
    let paths = StorePaths::new(default_store_root()?);
    paths
        .ensure()
        .map_err(|error| Error::Unavailable(error.to_string()))?;
    let lock = open_lock(&paths.lock).map_err(|error| Error::Unavailable(error.to_string()))?;
    lock.try_lock_exclusive()
        .map_err(|error| Error::Unavailable(format!("daemon already running: {error}")))?;

    let store = Arc::new(Mutex::new(
        Store::open(paths).map_err(|error| Error::Unavailable(error.to_string()))?,
    ));
    let scheduler = Scheduler::start(Arc::clone(&store));
    scheduler.wake();
    serve(store, scheduler)
}

struct Scheduler {
    signal: Arc<(Mutex<bool>, Condvar)>,
    events: Arc<(Mutex<u64>, Condvar)>,
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

    fn wait_final(
        &self,
        store: &SharedStore,
        job_id: crate::JobId,
    ) -> std::result::Result<crate::JobSnapshot, StoreError> {
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
            if snapshot.is_final() {
                return Ok(snapshot);
            }
            let (lock, condition) = &*self.events;
            let mut generation = lock
                .lock()
                .map_err(|_| StoreError::InvalidState("event mutex poisoned".into()))?;
            while *generation == observed {
                generation = condition
                    .wait(generation)
                    .map_err(|_| StoreError::InvalidState("event mutex poisoned".into()))?;
            }
        }
    }

    fn run(&self, store: SharedStore) {
        loop {
            let next = {
                let mut guard = match store.lock() {
                    Ok(guard) => guard,
                    Err(_) => return,
                };
                match guard.pending_jobs() {
                    Ok(jobs) => jobs
                        .into_iter()
                        .next()
                        .and_then(|job_id| guard.prepare_job(job_id).ok().flatten()),
                    Err(_) => None,
                }
            };
            if let Some(job) = next {
                self.notify_change();
                crate::runner::run(job, Arc::clone(&store));
                self.notify_change();
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

fn handle_request(store: &SharedStore, scheduler: &Scheduler, request: Request) -> Response {
    let result = match request {
        Request::Ping => {
            return Response::Pong {
                protocol_version: PROTOCOL_VERSION,
            };
        }
        Request::Submit {
            idempotency_key,
            payload_hash,
            spec,
        } => store
            .lock()
            .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))
            .and_then(|mut store| store.submit(idempotency_key, &payload_hash, &spec))
            .map(|submitted| {
                if submitted.should_schedule {
                    scheduler.wake();
                }
                Response::Submitted(submitted.receipt)
            }),
        Request::Recover {
            idempotency_key,
            payload_hash,
        } => store
            .lock()
            .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))
            .and_then(|store| store.recover_submission(idempotency_key, &payload_hash))
            .map(Response::Recovered),
        Request::Status { job_id } => store
            .lock()
            .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))
            .and_then(|store| store.status(job_id))
            .map(|snapshot| Response::Snapshot(Box::new(snapshot))),
        Request::Wait { job_id } => scheduler
            .wait_final(store, job_id)
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
        CloseHandle, ERROR_PIPE_CONNECTED, GetLastError, INVALID_HANDLE_VALUE, LocalFree,
    };
    use windows_sys::Win32::Security::Authorization::{
        ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW,
        SDDL_REVISION_1,
    };
    use windows_sys::Win32::Security::{
        GetTokenInformation, PSECURITY_DESCRIPTOR, SECURITY_ATTRIBUTES, TOKEN_QUERY, TOKEN_USER,
        TokenUser,
    };
    use windows_sys::Win32::Storage::FileSystem::PIPE_ACCESS_DUPLEX;
    use windows_sys::Win32::System::Pipes::{
        ConnectNamedPipe, CreateNamedPipeW, PIPE_REJECT_REMOTE_CLIENTS, PIPE_TYPE_BYTE, PIPE_WAIT,
    };
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

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
            let mut token = std::ptr::null_mut();
            // SAFETY: the pseudo process handle is valid and token is writable.
            if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
                return Err(std::io::Error::last_os_error());
            }
            struct TokenGuard(windows_sys::Win32::Foundation::HANDLE);
            impl Drop for TokenGuard {
                fn drop(&mut self) {
                    // SAFETY: this guard owns a valid token handle.
                    unsafe { CloseHandle(self.0) };
                }
            }
            let token = TokenGuard(token);
            let mut required = 0_u32;
            // SAFETY: this is the documented sizing call.
            unsafe {
                GetTokenInformation(token.0, TokenUser, std::ptr::null_mut(), 0, &mut required)
            };
            if required < size_of::<TOKEN_USER>() as u32 {
                return Err(std::io::Error::last_os_error());
            }
            let mut buffer = vec![0_u8; required as usize];
            // SAFETY: buffer has the size requested by the API and remains alive while SID is used.
            if unsafe {
                GetTokenInformation(
                    token.0,
                    TokenUser,
                    buffer.as_mut_ptr().cast(),
                    required,
                    &mut required,
                )
            } == 0
            {
                return Err(std::io::Error::last_os_error());
            }
            // SAFETY: a successful TokenUser query returns a TOKEN_USER at the buffer start.
            let user = unsafe { &*buffer.as_ptr().cast::<TOKEN_USER>() };
            let mut sid_text = std::ptr::null_mut();
            // SAFETY: the SID belongs to the live token buffer and output is writable.
            if unsafe { ConvertSidToStringSidW(user.User.Sid, &mut sid_text) } == 0 {
                return Err(std::io::Error::last_os_error());
            }
            let sid_allocation = LocalAllocation(sid_text.cast());
            let mut length = 0_usize;
            // SAFETY: ConvertSidToStringSidW returns a NUL-terminated string.
            while unsafe { *sid_text.add(length) } != 0 {
                length += 1;
            }
            // SAFETY: the measured range is valid UTF-16 from the Windows API.
            let sid =
                String::from_utf16_lossy(unsafe { std::slice::from_raw_parts(sid_text, length) });
            drop(sid_allocation);
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

    let pipe_name: Vec<u16> = std::ffi::OsStr::new(DEFAULT_PIPE_NAME)
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
            return Err(std::io::Error::last_os_error().into());
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
        let store = Arc::clone(&store);
        let scheduler = Arc::clone(&scheduler);
        // Raw Windows handles are pointer-typed and therefore not `Send`; their integer value is
        // safe to transfer because this thread owns the handle after a successful connection.
        let handle_value = handle as usize;
        std::thread::Builder::new()
            .name("stillyard-client".into())
            .spawn(move || {
                let handle = handle_value as windows_sys::Win32::Foundation::HANDLE;
                // SAFETY: ownership of the valid pipe handle is transferred to File exactly once.
                let mut pipe = unsafe { File::from_raw_handle(handle as _) };
                let response = match read_frame::<Request>(&mut pipe) {
                    Ok(request) => handle_request(&store, &scheduler, request),
                    Err(error) => Response::Error {
                        code: "invalid_request".into(),
                        message: error.to_string(),
                    },
                };
                let _ = write_frame(&mut pipe, &response);
            })
            .map_err(Error::Io)?;
    }
}

#[cfg(not(windows))]
fn serve(_store: SharedStore, _scheduler: Arc<Scheduler>) -> Result<()> {
    Err(Error::UnsupportedPlatform(std::env::consts::OS))
}
