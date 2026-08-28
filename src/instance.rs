use std::path::PathBuf;

use directories::ProjectDirs;
#[cfg(windows)]
use sha2::{Digest, Sha256};

use crate::{Error, Result};

pub(crate) fn default_store_root() -> Result<PathBuf> {
    let project = ProjectDirs::from("org", "stillyard", "Stillyard")
        .ok_or_else(|| Error::Unavailable("cannot resolve per-user data directory".into()))?;
    Ok(project.data_local_dir().to_path_buf())
}

pub(crate) fn default_endpoint() -> Result<String> {
    #[cfg(windows)]
    let identity = current_user_sid_string()?;
    #[cfg(windows)]
    let digest = format!("{:x}", Sha256::digest(identity.as_bytes()));
    #[cfg(windows)]
    return Ok(format!(r"\\.\pipe\stillyard-v6-{}", &digest[..16]));
    #[cfg(not(windows))]
    return Ok(default_store_root()?
        .join("stillyard-v6.sock")
        .to_string_lossy()
        .into_owned());
}

#[cfg_attr(not(windows), allow(dead_code))]
pub(crate) fn resolve_store_root(selected: Option<PathBuf>) -> Result<PathBuf> {
    let selected = selected.map(Ok).unwrap_or_else(default_store_root)?;
    if !selected.is_absolute() {
        return Err(Error::InvalidSpec(format!(
            "daemon store path must be absolute: {}",
            selected.display()
        )));
    }
    let mut existing = selected.as_path();
    while !existing.try_exists()? {
        existing = existing.parent().ok_or_else(|| {
            Error::InvalidSpec(format!(
                "daemon store path has no existing ancestor: {}",
                selected.display()
            ))
        })?;
    }
    crate::filesystem::require_fixed_local_ntfs(existing)?;
    std::fs::create_dir_all(&selected)?;
    crate::filesystem::require_fixed_local_ntfs(&selected)?;
    Ok(std::fs::canonicalize(selected)?)
}

#[cfg_attr(not(windows), allow(dead_code))]
pub(crate) fn resolve_endpoint(selected: Option<String>) -> Result<String> {
    let endpoint = selected.map(Ok).unwrap_or_else(default_endpoint)?;
    validate_endpoint(&endpoint)?;
    Ok(endpoint)
}

pub(crate) fn validate_endpoint(endpoint: &str) -> Result<()> {
    if endpoint.is_empty() || endpoint.contains('\0') {
        return Err(Error::InvalidSpec(
            "daemon endpoint is empty or contains NUL".into(),
        ));
    }
    #[cfg(windows)]
    {
        if endpoint.encode_utf16().count() > 256 {
            return Err(Error::InvalidSpec(
                "Windows named-pipe endpoint exceeds 256 UTF-16 code units".into(),
            ));
        }
        let Some(name) = endpoint.get(9..) else {
            return Err(Error::InvalidSpec(format!(
                "Windows endpoint must be a local \\\\.\\pipe\\NAME path: {endpoint:?}"
            )));
        };
        if !endpoint[..9].eq_ignore_ascii_case(r"\\.\pipe\")
            || name.is_empty()
            || name.contains('\\')
            || name.contains('/')
            || !name.is_ascii()
        {
            return Err(Error::InvalidSpec(format!(
                "Windows endpoint must be an ASCII local \\\\.\\pipe\\NAME path: {endpoint:?}"
            )));
        }
    }
    Ok(())
}

pub(crate) fn endpoints_equal(left: &str, right: &str) -> bool {
    #[cfg(windows)]
    return left.eq_ignore_ascii_case(right);
    #[cfg(not(windows))]
    return left == right;
}

#[cfg(windows)]
pub(crate) fn current_user_sid_string() -> Result<String> {
    use std::ffi::c_void;
    use std::mem::size_of;
    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, LocalFree};
    use windows_sys::Win32::Security::Authorization::ConvertSidToStringSidW;
    use windows_sys::Win32::Security::{GetTokenInformation, TOKEN_QUERY, TOKEN_USER, TokenUser};
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

    struct HandleGuard(HANDLE);
    impl Drop for HandleGuard {
        fn drop(&mut self) {
            // SAFETY: this guard owns the token handle.
            unsafe { CloseHandle(self.0) };
        }
    }
    struct LocalGuard(*mut c_void);
    impl Drop for LocalGuard {
        fn drop(&mut self) {
            if !self.0.is_null() {
                // SAFETY: this pointer came from ConvertSidToStringSidW.
                unsafe { LocalFree(self.0) };
            }
        }
    }

    let mut token = std::ptr::null_mut();
    // SAFETY: the current-process pseudo handle is valid and token is writable.
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let token = HandleGuard(token);
    let mut required = 0_u32;
    // SAFETY: this is the documented sizing call.
    unsafe { GetTokenInformation(token.0, TokenUser, std::ptr::null_mut(), 0, &mut required) };
    if required < size_of::<TOKEN_USER>() as u32 {
        return Err(std::io::Error::last_os_error().into());
    }
    let mut buffer = vec![0_u8; required as usize];
    // SAFETY: the buffer has the exact requested size and all outputs are writable.
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
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: a successful TokenUser query places TOKEN_USER at the buffer start.
    let user = unsafe { &*buffer.as_ptr().cast::<TOKEN_USER>() };
    let mut sid_text = std::ptr::null_mut();
    // SAFETY: the SID belongs to the live token buffer and output is writable.
    if unsafe { ConvertSidToStringSidW(user.User.Sid, &mut sid_text) } == 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let sid = LocalGuard(sid_text.cast());
    let mut length = 0_usize;
    // SAFETY: ConvertSidToStringSidW returns a NUL-terminated UTF-16 string.
    while unsafe { *sid_text.add(length) } != 0 {
        length += 1;
    }
    // SAFETY: the measured range belongs to the live LocalGuard allocation.
    let text = String::from_utf16_lossy(unsafe { std::slice::from_raw_parts(sid_text, length) });
    drop(sid);
    Ok(text)
}
