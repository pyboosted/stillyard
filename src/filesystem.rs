use std::io;
use std::path::Path;

/// Enforces the v0.1 durability boundary before durable files are opened.
#[cfg(windows)]
pub(crate) fn require_fixed_local_ntfs(path: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_ATTRIBUTE_REPARSE_POINT, GetDriveTypeW, GetFileAttributesW, GetVolumeInformationW,
        GetVolumePathNameW, INVALID_FILE_ATTRIBUTES,
    };
    use windows_sys::Win32::System::WindowsProgramming::DRIVE_FIXED;

    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut existing = Some(absolute.as_path());
    while let Some(candidate) = existing {
        let candidate_wide: Vec<u16> = candidate
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect();
        // SAFETY: candidate_wide is NUL-terminated and remains alive for the call.
        let attributes = unsafe { GetFileAttributesW(candidate_wide.as_ptr()) };
        if attributes != INVALID_FILE_ATTRIBUTES && attributes & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "v0.1 durable paths reject redirected/reparse component {}",
                    candidate.display()
                ),
            ));
        }
        existing = candidate.parent();
    }
    let wide: Vec<u16> = absolute
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    let mut volume_root = vec![0_u16; 32_768];
    // SAFETY: both buffers remain alive, are NUL-terminated/zeroed, and lengths are exact.
    if unsafe {
        GetVolumePathNameW(
            wide.as_ptr(),
            volume_root.as_mut_ptr(),
            volume_root.len() as u32,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: GetVolumePathNameW emitted a NUL-terminated root in the owned buffer.
    let drive_type = unsafe { GetDriveTypeW(volume_root.as_ptr()) };
    let mut filesystem = [0_u16; 32];
    // SAFETY: all optional output pointers may be null and the filesystem buffer is writable.
    if unsafe {
        GetVolumeInformationW(
            volume_root.as_ptr(),
            std::ptr::null_mut(),
            0,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            filesystem.as_mut_ptr(),
            filesystem.len() as u32,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    let length = filesystem
        .iter()
        .position(|unit| *unit == 0)
        .unwrap_or(filesystem.len());
    let filesystem = String::from_utf16_lossy(&filesystem[..length]);
    validate_volume(drive_type == DRIVE_FIXED, &filesystem)
}

#[cfg(not(windows))]
pub(crate) fn require_fixed_local_ntfs(_path: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg_attr(not(windows), allow(dead_code))]
fn validate_volume(fixed: bool, filesystem: &str) -> io::Result<()> {
    if !fixed || !filesystem.eq_ignore_ascii_case("NTFS") {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!(
                "v0.1 durable paths require local fixed NTFS (fixed={fixed}, filesystem={filesystem:?})"
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_fixed_ntfs_is_accepted() {
        assert!(validate_volume(true, "NTFS").is_ok());
        assert!(validate_volume(false, "NTFS").is_err());
        assert!(validate_volume(true, "ReFS").is_err());
        assert!(validate_volume(true, "").is_err());
    }
}
