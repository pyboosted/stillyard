//! A mutable staged file is never handed to the final executable directly.
use crate::store::PreparedJob;
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::fd::{AsRawFd, FromRawFd};

pub(super) fn open(job: &PreparedJob) -> io::Result<File> {
    let (expected, path) = match (&job.stdin, &job.stdin_path) {
        (None, None) => return File::open("/dev/null"),
        (Some(expected), Some(path)) => (expected, path),
        _ => return Err(io::Error::other("partial staged input reference")),
    };
    let mut input = File::open(path)?;
    if input.metadata()?.len() != expected.length {
        return Err(io::Error::other("staged input length changed"));
    }
    // SAFETY: static NUL-terminated name; successful descriptor is newly owned.
    let fd = unsafe {
        libc::memfd_create(
            c"stillyard-stdin".as_ptr(),
            libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: memfd_create transferred exclusive descriptor ownership.
    let mut snapshot = unsafe { File::from_raw_fd(fd) };
    let mut hash = Sha256::new();
    let mut length = 0_u64;
    let mut buffer = [0_u8; 65536];
    loop {
        let count = input.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        length = length.saturating_add(count as u64);
        if length > expected.length {
            return Err(io::Error::other("staged input grew"));
        }
        hash.update(&buffer[..count]);
        snapshot.write_all(&buffer[..count])?;
    }
    if length != expected.length || format!("{:x}", hash.finalize()) != expected.sha256 {
        return Err(io::Error::other("staged input contents changed"));
    }
    // SAFETY: owned sealable memfd with no writable mappings.
    if unsafe {
        libc::fcntl(
            snapshot.as_raw_fd(),
            libc::F_ADD_SEALS,
            libc::F_SEAL_WRITE | libc::F_SEAL_GROW | libc::F_SEAL_SHRINK | libc::F_SEAL_SEAL,
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    snapshot.seek(SeekFrom::Start(0))?;
    Ok(snapshot)
}
