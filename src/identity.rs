#[cfg(windows)]
use crate::ReconciliationResult;
use crate::{BootId, Error, HostId, ProcessIdentity, Result};

#[derive(Clone, Debug)]
pub(crate) struct StartupIdentity {
    pub(crate) host_id: Option<HostId>,
    pub(crate) boot_id: Option<BootId>,
    pub(crate) daemon_process: Option<ProcessIdentity>,
    pub(crate) failures: Vec<String>,
}

impl StartupIdentity {
    pub(crate) fn capable(&self) -> bool {
        self.host_id.is_some() && self.boot_id.is_some() && self.daemon_process.is_some()
    }
}

#[cfg(windows)]
pub(crate) fn probe_startup_identity() -> StartupIdentity {
    let mut failures = Vec::new();
    let host_id = probe_host_id()
        .map_err(|error| failures.push(error.to_string()))
        .ok();
    let boot_id = probe_boot_id()
        .map_err(|error| failures.push(error.to_string()))
        .ok();
    let daemon_process = match (&host_id, &boot_id) {
        (Some(host_id), Some(boot_id)) => current_process_identity(host_id, boot_id)
            .map_err(|error| failures.push(error.to_string()))
            .ok(),
        _ => None,
    };
    StartupIdentity {
        host_id,
        boot_id,
        daemon_process,
        failures,
    }
}

#[cfg(not(windows))]
pub(crate) fn probe_startup_identity() -> StartupIdentity {
    StartupIdentity {
        host_id: None,
        boot_id: None,
        daemon_process: None,
        failures: vec!["native containment identity is unavailable on this platform".into()],
    }
}

#[cfg(windows)]
fn probe_host_id() -> Result<HostId> {
    use std::os::windows::ffi::OsStrExt;

    use sha2::{Digest, Sha256};
    use windows_sys::Win32::System::Registry::{
        HKEY_LOCAL_MACHINE, RRF_RT_REG_SZ, RRF_SUBKEY_WOW6464KEY, RegGetValueW,
    };

    let subkey: Vec<u16> = std::ffi::OsStr::new(r"SOFTWARE\Microsoft\Cryptography")
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    let value: Vec<u16> = std::ffi::OsStr::new("MachineGuid")
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    let mut bytes = 0_u32;
    // SAFETY: registry paths are NUL-terminated and the first call requests the required size.
    let status = unsafe {
        RegGetValueW(
            HKEY_LOCAL_MACHINE,
            subkey.as_ptr(),
            value.as_ptr(),
            RRF_RT_REG_SZ | RRF_SUBKEY_WOW6464KEY,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            &mut bytes,
        )
    };
    if status != 0 {
        return Err(Error::Unavailable(format!(
            "cannot read Windows machine identity: OS error {status}"
        )));
    }
    let mut buffer = vec![0_u16; (bytes as usize).div_ceil(2)];
    // SAFETY: buffer has the byte size reported by the preceding query.
    let status = unsafe {
        RegGetValueW(
            HKEY_LOCAL_MACHINE,
            subkey.as_ptr(),
            value.as_ptr(),
            RRF_RT_REG_SZ | RRF_SUBKEY_WOW6464KEY,
            std::ptr::null_mut(),
            buffer.as_mut_ptr().cast(),
            &mut bytes,
        )
    };
    if status != 0 {
        return Err(Error::Unavailable(format!(
            "cannot read Windows machine identity: OS error {status}"
        )));
    }
    let length = buffer
        .iter()
        .position(|unit| *unit == 0)
        .unwrap_or(buffer.len());
    let machine_guid = String::from_utf16(&buffer[..length])
        .map_err(|_| Error::Unavailable("Windows machine identity is not valid UTF-16".into()))?;
    let mut digest = Sha256::new();
    digest.update(b"stillyard-host-id-v1\0");
    digest.update(machine_guid.trim().to_ascii_lowercase().as_bytes());
    Ok(HostId(format!("sha256:{:x}", digest.finalize())))
}

#[cfg(windows)]
fn probe_boot_id() -> Result<BootId> {
    use std::ffi::c_void;
    use std::os::windows::ffi::OsStrExt;

    use windows_sys::Win32::System::LibraryLoader::{GetModuleHandleW, GetProcAddress};
    use windows_sys::core::GUID;

    #[repr(C)]
    struct BootEnvironmentInformation {
        boot_identifier: GUID,
        firmware_type: u32,
        boot_flags: u64,
    }

    type NtQuerySystemInformation =
        unsafe extern "system" fn(u32, *mut c_void, u32, *mut u32) -> i32;

    let module_name: Vec<u16> = std::ffi::OsStr::new("ntdll.dll")
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    // SAFETY: ntdll is loaded in every Windows process and the name is NUL-terminated.
    let module = unsafe { GetModuleHandleW(module_name.as_ptr()) };
    if module.is_null() {
        return Err(Error::Unavailable(format!(
            "cannot resolve ntdll for boot identity: OS error {}",
            std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
        )));
    }
    // SAFETY: the export name is static and NUL-terminated.
    let procedure = unsafe { GetProcAddress(module, c"NtQuerySystemInformation".as_ptr().cast()) }
        .ok_or_else(|| Error::Unavailable("NtQuerySystemInformation is unavailable".into()))?;
    // SAFETY: the named ntdll export has this stable NT ABI signature.
    let query: NtQuerySystemInformation = unsafe { std::mem::transmute(procedure) };
    let mut information: BootEnvironmentInformation = unsafe { std::mem::zeroed() };
    let mut returned = 0_u32;
    // SystemBootEnvironmentInformation is SYSTEM_INFORMATION_CLASS 90.
    // SAFETY: information is writable and its size matches the selected information class.
    let status = unsafe {
        query(
            90,
            (&raw mut information).cast(),
            std::mem::size_of::<BootEnvironmentInformation>() as u32,
            &mut returned,
        )
    };
    if status < 0 {
        return Err(Error::Unavailable(format!(
            "cannot query Windows boot identity: NTSTATUS 0x{:08x}",
            status as u32
        )));
    }
    let guid = information.boot_identifier;
    let bytes = guid.data4;
    Ok(BootId(format!(
        "{:08x}-{:04x}-{:04x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        guid.data1,
        guid.data2,
        guid.data3,
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7]
    )))
}

#[cfg(windows)]
fn current_process_identity(host_id: &HostId, boot_id: &BootId) -> Result<ProcessIdentity> {
    use windows_sys::Win32::System::Threading::GetCurrentProcess;

    // SAFETY: GetCurrentProcess returns a process-local pseudohandle that must not be closed.
    process_identity_from_handle(
        unsafe { GetCurrentProcess() },
        std::process::id(),
        host_id,
        boot_id,
    )
}

#[cfg(windows)]
pub(crate) fn process_identity_from_handle(
    handle: windows_sys::Win32::Foundation::HANDLE,
    pid: u32,
    host_id: &HostId,
    boot_id: &BootId,
) -> Result<ProcessIdentity> {
    use windows_sys::Win32::Foundation::FILETIME;
    use windows_sys::Win32::System::Threading::GetProcessTimes;

    let mut creation: FILETIME = unsafe { std::mem::zeroed() };
    let mut exit: FILETIME = unsafe { std::mem::zeroed() };
    let mut kernel: FILETIME = unsafe { std::mem::zeroed() };
    let mut user: FILETIME = unsafe { std::mem::zeroed() };
    // SAFETY: handle identifies a process and all FILETIME outputs are writable.
    if unsafe { GetProcessTimes(handle, &mut creation, &mut exit, &mut kernel, &mut user) } == 0 {
        return Err(Error::Unavailable(format!(
            "cannot read process creation identity for PID {pid}: OS error {}",
            std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
        )));
    }
    Ok(ProcessIdentity::Windows {
        host_id: host_id.clone(),
        boot_id: boot_id.clone(),
        pid,
        creation_filetime_100ns: u64::from(creation.dwLowDateTime)
            | (u64::from(creation.dwHighDateTime) << 32),
    })
}

#[cfg(windows)]
pub(crate) fn probe_recorded_process(
    recorded: &ProcessIdentity,
    current_host_id: &HostId,
    current_boot_id: &BootId,
) -> ReconciliationResult {
    use windows_sys::Win32::Foundation::{CloseHandle, ERROR_INVALID_PARAMETER, WAIT_OBJECT_0};
    use windows_sys::Win32::System::Threading::{
        OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, WaitForSingleObject,
    };

    let ProcessIdentity::Windows {
        host_id,
        boot_id,
        pid,
        creation_filetime_100ns,
    } = recorded
    else {
        return ReconciliationResult::IdentityUnavailable;
    };
    if host_id != current_host_id {
        return ReconciliationResult::IdentityUnavailable;
    }
    if boot_id != current_boot_id {
        return ReconciliationResult::PriorBoot;
    }
    // SAFETY: access is query/synchronize only and never grants termination rights.
    const SYNCHRONIZE_ACCESS: u32 = 0x0010_0000;
    let handle = unsafe {
        OpenProcess(
            PROCESS_QUERY_LIMITED_INFORMATION | SYNCHRONIZE_ACCESS,
            0,
            *pid,
        )
    };
    if handle.is_null() {
        return if std::io::Error::last_os_error().raw_os_error()
            == Some(ERROR_INVALID_PARAMETER as i32)
        {
            ReconciliationResult::IdentityAbsent
        } else {
            ReconciliationResult::IdentityUnavailable
        };
    }
    let observed = process_identity_from_handle(handle, *pid, current_host_id, current_boot_id);
    // A signaled process object is terminated even if another observer retains the object.
    let terminated = unsafe { WaitForSingleObject(handle, 0) == WAIT_OBJECT_0 };
    // SAFETY: OpenProcess returned this owned handle.
    unsafe { CloseHandle(handle) };
    match observed {
        Ok(ProcessIdentity::Windows {
            creation_filetime_100ns: observed_creation,
            ..
        }) if observed_creation != *creation_filetime_100ns => ReconciliationResult::PidReused,
        Ok(_) if terminated => ReconciliationResult::IdentityAbsent,
        Ok(_) => ReconciliationResult::StillResolves,
        Err(_) => ReconciliationResult::IdentityUnavailable,
    }
}

#[cfg(not(windows))]
#[allow(dead_code)]
pub(crate) fn process_identity_from_handle(
    _handle: usize,
    _pid: u32,
    _host_id: &HostId,
    _boot_id: &BootId,
) -> Result<ProcessIdentity> {
    Err(Error::UnsupportedPlatform(std::env::consts::OS))
}

#[cfg(all(test, windows))]
mod tests {
    use std::os::windows::io::AsRawHandle;

    use super::*;

    #[test]
    fn exact_process_probe_distinguishes_match_reuse_and_exit() {
        let startup = probe_startup_identity();
        let host = startup.host_id.unwrap();
        let boot = startup.boot_id.unwrap();
        let current = startup.daemon_process.unwrap();
        assert_eq!(
            probe_recorded_process(&current, &host, &boot),
            ReconciliationResult::StillResolves
        );
        let reused = match current {
            ProcessIdentity::Windows {
                host_id,
                boot_id,
                pid,
                creation_filetime_100ns,
            } => ProcessIdentity::Windows {
                host_id,
                boot_id,
                pid,
                creation_filetime_100ns: creation_filetime_100ns.saturating_add(1),
            },
            ProcessIdentity::Unknown { .. } => panic!("Windows probe returned unknown identity"),
        };
        assert_eq!(
            probe_recorded_process(&reused, &host, &boot),
            ReconciliationResult::PidReused
        );

        let mut child = std::process::Command::new("cmd.exe")
            .args(["/d", "/c", "exit", "0"])
            .spawn()
            .unwrap();
        let child_identity = process_identity_from_handle(
            child.as_raw_handle() as windows_sys::Win32::Foundation::HANDLE,
            child.id(),
            &host,
            &boot,
        )
        .unwrap();
        child.wait().unwrap();
        assert_eq!(
            probe_recorded_process(&child_identity, &host, &boot),
            ReconciliationResult::IdentityAbsent
        );
    }
}
