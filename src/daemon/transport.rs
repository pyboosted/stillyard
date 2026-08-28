use super::*;

#[cfg(windows)]
pub(super) struct EndpointLease(windows_sys::Win32::Foundation::HANDLE);

#[cfg(windows)]
impl Drop for EndpointLease {
    fn drop(&mut self) {
        // SAFETY: the lease owns the mutex handle returned by CreateMutexW.
        unsafe { windows_sys::Win32::Foundation::CloseHandle(self.0) };
    }
}

#[cfg(windows)]
pub(super) struct OwnedPipe(windows_sys::Win32::Foundation::HANDLE);

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
pub(super) fn create_pipe_instance(endpoint: &str, first: bool) -> Result<OwnedPipe> {
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
pub(super) fn acquire_endpoint_lease(endpoint: &str) -> Result<EndpointLease> {
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

#[cfg(windows)]
pub(super) fn serve(
    store: SharedStore,
    scheduler: Arc<DaemonReactor>,
    first_pipe: OwnedPipe,
) -> Result<()> {
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
pub(super) fn serve(_store: SharedStore, _scheduler: Arc<DaemonReactor>) -> Result<()> {
    Err(Error::UnsupportedPlatform(std::env::consts::OS))
}
