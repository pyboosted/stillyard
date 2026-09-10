//! Kernel-pinned local process identity. Process death is never a cgroup-empty proof.
use std::fs::File;
use std::io::{self, Read};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::fs::MetadataExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

use crate::{BootId, HostId, ProcessIdentity};
use sha2::{Digest, Sha256};

pub(crate) struct Process {
    pidfd: OwnedFd,
    proc_directory: File,
    pub(crate) identity: ProcessIdentity,
}

fn invalid(message: &str) -> io::Error {
    io::Error::other(message)
}

fn bounded_text(path: &Path) -> io::Result<String> {
    let mut text = String::new();
    File::open(path)?.take(65_537).read_to_string(&mut text)?;
    if text.len() > 65_536 {
        return Err(invalid("process evidence exceeds byte bound"));
    }
    Ok(text)
}

fn start_ticks(text: &str, expected_pid: u32) -> io::Result<u64> {
    let (pid, _) = text
        .split_once(" (")
        .ok_or_else(|| invalid("invalid proc stat PID"))?;
    if pid.parse::<u32>().map_err(io::Error::other)? != expected_pid {
        return Err(invalid("proc stat PID changed"));
    }
    // comm may contain spaces, newlines and closing parentheses. Only the last
    // delimiter precedes the fixed kernel fields, starting at field 3 (state).
    let (_, tail) = text
        .rsplit_once(") ")
        .ok_or_else(|| invalid("invalid proc stat comm"))?;
    let ticks = tail
        .split_whitespace()
        .nth(19)
        .ok_or_else(|| invalid("missing process start ticks"))?
        .parse::<u64>()
        .map_err(io::Error::other)?;
    if ticks == 0 {
        return Err(invalid("zero process start ticks"));
    }
    Ok(ticks)
}

fn host_and_boot() -> io::Result<(HostId, BootId)> {
    let machine = bounded_text(Path::new("/etc/machine-id"))?;
    let machine = machine.trim();
    if machine.len() != 32 || !machine.bytes().all(|c| c.is_ascii_hexdigit()) {
        return Err(invalid("Linux installation machine-id is unavailable"));
    }
    let mut digest = Sha256::new();
    digest.update(b"stillyard-linux-host-id-v1\0");
    digest.update(machine.to_ascii_lowercase().as_bytes());
    let boot = bounded_text(Path::new("/proc/sys/kernel/random/boot_id"))?;
    let boot = uuid::Uuid::parse_str(boot.trim()).map_err(io::Error::other)?;
    if boot.is_nil() {
        return Err(invalid("nil Linux boot identity"));
    }
    Ok((
        HostId(format!("sha256:{:x}", digest.finalize())),
        BootId(boot.to_string()),
    ))
}

fn alive(pidfd: &OwnedFd) -> io::Result<bool> {
    let mut pfd = libc::pollfd {
        fd: pidfd.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: pfd owns a live pidfd number for the entire zero-time poll.
    let result = unsafe { libc::poll(&mut pfd, 1, 0) };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    if pfd.revents & libc::POLLNVAL != 0 {
        return Err(invalid("invalid process pidfd"));
    }
    Ok(result == 0)
}

pub(crate) struct SocketOwner {
    pid: u32,
    uid: u32,
    pidfd: OwnedFd,
}
impl SocketOwner {
    pub(crate) fn ensure_alive(&self) -> io::Result<()> {
        if alive(&self.pidfd)? {
            Ok(())
        } else {
            Err(invalid("connection peer exited"))
        }
    }
}
/// Pins an owner peer even if an ancestor PID is invisible in this namespace.
/// This is NOT server-image authentication; opaque peers require an independent
/// per-Invocation challenge proof before their responses can be trusted.
pub(crate) fn pin_socket_owner(socket: &UnixStream) -> io::Result<SocketOwner> {
    let mut credentials = std::mem::MaybeUninit::<libc::ucred>::uninit();
    let mut length = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    // SAFETY: getsockopt writes at most the supplied output size.
    if unsafe {
        libc::getsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            credentials.as_mut_ptr().cast(),
            &mut length,
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    if length as usize != std::mem::size_of::<libc::ucred>() {
        return Err(invalid("short peer credentials"));
    }
    // SAFETY: successful SO_PEERCRED returned the exact initialized length.
    let credentials = unsafe { credentials.assume_init() };
    // SAFETY: geteuid has no preconditions.
    if credentials.pid < 0 || credentials.uid != unsafe { libc::geteuid() } {
        return Err(invalid(
            "Unix peer belongs to another owner or PID namespace",
        ));
    }
    let mut descriptor = -1_i32;
    let mut length = std::mem::size_of::<i32>() as libc::socklen_t;
    // SO_PEERPIDFD pins the actual connection peer, closing the PID-reuse
    // race of SO_PEERCRED followed by pidfd_open. No PID-only fallback.
    // SAFETY: descriptor and length are writable, and the socket is live.
    if unsafe {
        libc::getsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERPIDFD,
            (&raw mut descriptor).cast(),
            &mut length,
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    if descriptor < 0 {
        return Err(invalid("peer pidfd unavailable"));
    }
    // SAFETY: successful SO_PEERPIDFD transferred ownership of this fd.
    let pidfd = unsafe { OwnedFd::from_raw_fd(descriptor) };
    if length as usize != std::mem::size_of::<i32>() {
        return Err(invalid("short peer pidfd"));
    }
    if !alive(&pidfd)? {
        return Err(invalid("connection peer exited"));
    }
    Ok(SocketOwner {
        pid: credentials.pid as u32,
        uid: credentials.uid,
        pidfd,
    })
}

impl Process {
    pub(crate) fn proc_handle(&self) -> usize {
        self.proc_directory.as_raw_fd() as usize
    }
    pub(crate) fn principal(&self) -> io::Result<String> {
        let ProcessIdentity::Linux { uid, .. } = self.identity else {
            return Err(invalid("non-Linux peer"));
        };
        if self.proc_directory.metadata()?.uid() != uid || !alive(&self.pidfd)? {
            return Err(invalid("peer identity no longer resolves"));
        }
        Ok(format!("uid:{uid}"))
    }
    pub(crate) fn open(pid: u32, uid: u32) -> io::Result<Self> {
        // SAFETY: pidfd_open returns a new owned fd or an error; flags are zero.
        let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: a successful pidfd_open transferred ownership of this fd.
        Self::from_pinned(pid, uid, unsafe { OwnedFd::from_raw_fd(fd as i32) })
    }
    pub(crate) fn from_socket(socket: &UnixStream) -> io::Result<Self> {
        let peer = pin_socket_owner(socket)?;
        if peer.pid == 0 {
            return Err(invalid("connection peer is outside this PID namespace"));
        }
        Self::from_pinned(peer.pid, peer.uid, peer.pidfd)
    }

    fn from_pinned(pid: u32, uid: u32, pidfd: OwnedFd) -> io::Result<Self> {
        if !alive(&pidfd)? {
            return Err(invalid("peer exited before authentication"));
        }
        let proc_directory = File::open(format!("/proc/{pid}"))?;
        let base = PathBuf::from(format!("/proc/self/fd/{}", proc_directory.as_raw_fd()));
        let first = start_ticks(&bounded_text(&base.join("stat"))?, pid)?;
        if proc_directory.metadata()?.uid() != uid {
            return Err(invalid("peer owner changed"));
        }
        let namespace = std::fs::metadata(base.join("ns/pid"))?.ino();
        let (host_id, boot_id) = host_and_boot()?;
        if namespace == 0
            || first != start_ticks(&bounded_text(&base.join("stat"))?, pid)?
            || !alive(&pidfd)?
        {
            return Err(invalid("peer identity changed during authentication"));
        }
        Ok(Self {
            pidfd,
            proc_directory,
            identity: ProcessIdentity::Linux {
                host_id,
                boot_id,
                pid,
                start_ticks: first,
                pid_namespace_inode: namespace,
                uid,
            },
        })
    }

    pub(crate) fn verify_executable(&self, expected: &Path) -> io::Result<()> {
        let actual = File::open(format!(
            "/proc/self/fd/{}/exe",
            self.proc_directory.as_raw_fd()
        ))?;
        let expected = File::open(expected)?;
        let actual = actual.metadata()?;
        let expected = expected.metadata()?;
        let ProcessIdentity::Linux { uid, .. } = self.identity else {
            return Err(invalid("non-Linux peer"));
        };
        if !actual.is_file()
            || (actual.dev(), actual.ino()) != (expected.dev(), expected.ino())
            || self.proc_directory.metadata()?.uid() != uid
            || !alive(&self.pidfd)?
        {
            return Err(invalid("Unix peer executable or process identity differs"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn comm_delimiters_do_not_change_the_kernel_start_field() {
        let mut fields = vec!["S".to_owned()];
        fields.extend((4..=22).map(|n| if n == 22 { "92345".into() } else { "0".into() }));
        let stat = format!("17 (odd ) name\n) {}", fields.join(" "));
        assert_eq!(start_ticks(&stat, 17).unwrap(), 92345);
        assert!(start_ticks(&stat, 18).is_err());
        assert!(start_ticks("17 (x) S", 17).is_err());
    }

    #[test]
    fn socket_peer_is_kernel_pinned_and_image_checked() {
        let (left, _right) = UnixStream::pair().unwrap();
        let peer = Process::from_socket(&left).unwrap();
        assert!(
            matches!(peer.identity, ProcessIdentity::Linux { pid, .. } if pid == std::process::id())
        );
        peer.verify_executable(&std::env::current_exe().unwrap())
            .unwrap();
        assert!(peer.verify_executable(Path::new("/usr/bin/true")).is_err());
    }
}
