//! Pinned cgroup v2 boundaries. Missing paths and dead roots are never empty proof.
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Identity {
    pub(crate) boot_id: String,
    pub(crate) path: PathBuf,
    pub(crate) device: u64,
    pub(crate) inode: u64,
}

pub(crate) struct Boundary {
    directory: File,
    pub(crate) identity: Identity,
}

fn boot() -> io::Result<String> {
    let text = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")?;
    let id = uuid::Uuid::parse_str(text.trim()).map_err(io::Error::other)?;
    if id.is_nil() {
        return Err(io::Error::other("nil kernel boot identity"));
    }
    Ok(id.to_string())
}

fn open_directory(path: &Path) -> io::Result<File> {
    let directory = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?;
    let mut fs = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: live owned fd and writable statfs storage.
    if unsafe { libc::fstatfs(directory.as_raw_fd(), fs.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful fstatfs initialized the structure.
    if unsafe { fs.assume_init() }.f_type != libc::CGROUP2_SUPER_MAGIC {
        return Err(io::Error::other("execution boundary is not cgroup v2"));
    }
    Ok(directory)
}

impl Boundary {
    pub(crate) fn create(parent: &Path, invocation: uuid::Uuid) -> io::Result<Self> {
        if invocation.is_nil() {
            return Err(io::Error::other("nil boundary identity"));
        }
        let parent = std::fs::canonicalize(parent)?;
        let parent_guard = open_directory(&parent)?;
        let parent_fd = PathBuf::from(format!("/proc/self/fd/{}", parent_guard.as_raw_fd()));
        let name = format!("invocation-{invocation}");
        // Exclusive creation prevents a second attempt from reusing a boundary.
        std::fs::create_dir(parent_fd.join(&name))?;
        let directory = open_directory(&parent_fd.join(&name))?;
        let metadata = directory.metadata()?;
        let identity = Identity {
            boot_id: boot()?,
            path: parent.join(name),
            device: metadata.dev(),
            inode: metadata.ino(),
        };
        let boundary = Self {
            directory,
            identity,
        };
        // Opening the control file confirms actual delegated write capability.
        OpenOptions::new()
            .write(true)
            .open(boundary.file("cgroup.kill"))?;
        if boundary.populated()? {
            return Err(io::Error::other("new boundary is not empty"));
        }
        Ok(boundary)
    }

    pub(crate) fn reopen(identity: &Identity) -> io::Result<Self> {
        if boot()? != identity.boot_id {
            return Err(io::Error::other("cgroup belongs to another boot"));
        }
        let boundary = Self {
            directory: open_directory(&identity.path)?,
            identity: identity.clone(),
        };
        boundary.check_identity()?;
        Ok(boundary)
    }

    fn file(&self, name: &str) -> PathBuf {
        PathBuf::from(format!(
            "/proc/self/fd/{}/{name}",
            self.directory.as_raw_fd()
        ))
    }

    pub(super) fn attachment_file(&self) -> io::Result<File> {
        self.check_identity()?;
        OpenOptions::new()
            .write(true)
            .open(self.file("cgroup.procs"))
    }

    fn check_identity(&self) -> io::Result<()> {
        let pinned = self.directory.metadata()?;
        let current = std::fs::symlink_metadata(&self.identity.path)?;
        if !current.is_dir()
            || pinned.nlink() == 0
            || (pinned.dev(), pinned.ino()) != (self.identity.device, self.identity.inode)
            || (current.dev(), current.ino()) != (self.identity.device, self.identity.inode)
        {
            return Err(io::Error::other("recorded cgroup was replaced or removed"));
        }
        Ok(())
    }

    pub(crate) fn populated(&self) -> io::Result<bool> {
        self.check_identity()?;
        let mut text = String::new();
        File::open(self.file("cgroup.events"))?
            .take(4097)
            .read_to_string(&mut text)?;
        if text.len() > 4096 {
            return Err(io::Error::other("oversized cgroup event evidence"));
        }
        let values = text
            .lines()
            .filter_map(|line| line.strip_prefix("populated "))
            .collect::<Vec<_>>();
        match values.as_slice() {
            ["0"] => Ok(false),
            ["1"] => Ok(true),
            _ => Err(io::Error::other("invalid cgroup populated evidence")),
        }
    }

    /// The caller owns a live process guard and its pre-user-code release barrier.
    pub(crate) fn attach_stopped(&self, pid: u32) -> io::Result<()> {
        self.check_identity()?;
        OpenOptions::new()
            .write(true)
            .open(self.file("cgroup.procs"))?
            .write_all(pid.to_string().as_bytes())?;
        let text = std::fs::read_to_string(self.file("cgroup.procs"))?;
        if !text
            .lines()
            .any(|line| line.parse::<u32>().ok() == Some(pid))
        {
            return Err(io::Error::other(
                "stopped root did not enter recorded cgroup",
            ));
        }
        Ok(())
    }

    /// proc_handle is borrowed from the RPC's kernel-pinned peer guard, not a
    /// PID supplied by the caller. Descendants inherit this same cgroup.
    pub(crate) fn contains_process(&self, proc_handle: usize) -> io::Result<bool> {
        self.check_identity()?;
        let stat = std::fs::read_to_string(format!("/proc/self/fd/{proc_handle}/stat"))?;
        let pid = stat
            .split_once(" (")
            .ok_or_else(|| io::Error::other("invalid pinned process stat"))?
            .0
            .parse::<u32>()
            .map_err(io::Error::other)?;
        let mut text = String::new();
        File::open(self.file("cgroup.procs"))?
            .take(1_048_577)
            .read_to_string(&mut text)?;
        if text.len() > 1_048_576 {
            return Err(io::Error::other(
                "containment membership exceeds evidence bound",
            ));
        }
        Ok(text
            .lines()
            .any(|line| line.parse::<u32>().ok() == Some(pid)))
    }

    /// Successful return is a seal observation, which must be persisted before
    /// any Lease/Grant release. A crash before persistence remains uncertain.
    pub(crate) fn kill_and_seal(&self, deadline: Instant) -> io::Result<()> {
        self.check_identity()?;
        OpenOptions::new()
            .write(true)
            .open(self.file("cgroup.kill"))?
            .write_all(b"1")?;
        while self.populated()? {
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "cgroup remains populated; allocation retained",
                ));
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        self.check_identity()?;
        // Invocation user code sees read-only cgroups, so it cannot add child
        // boundaries. Unexpected descendants make rmdir fail closed.
        std::fs::remove_dir(&self.identity.path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn linux_cgroup_rejects_an_ordinary_directory_as_boundary_evidence() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("cgroup.events"), b"populated 0\n").unwrap();
        std::fs::write(directory.path().join("cgroup.kill"), b"").unwrap();
        assert!(Boundary::create(directory.path(), uuid::Uuid::now_v7()).is_err());
    }

    #[test]
    #[ignore = "requires nested delegation under the protected system bootstrap Job"]
    fn linux_cgroup_real_identity_empty_seal_and_missing_path_negative() {
        let root = std::env::var_os("STILLYARD_TEST_CGROUP_ROOT")
            .expect("explicit protected test delegation");
        let boundary = Boundary::create(Path::new(&root), uuid::Uuid::now_v7()).unwrap();
        assert!(!boundary.populated().unwrap());
        let mut wrong = boundary.identity.clone();
        wrong.inode += 1;
        assert!(Boundary::reopen(&wrong).is_err());
        let reopened = Boundary::reopen(&boundary.identity).unwrap();
        assert!(!reopened.populated().unwrap());
        boundary
            .kill_and_seal(Instant::now() + Duration::from_secs(3))
            .unwrap();
        assert!(
            Boundary::reopen(&boundary.identity).is_err(),
            "absence must not become an empty proof"
        );
        assert!(reopened.populated().is_err());
    }

    #[test]
    #[ignore = "requires nested delegation under the protected system bootstrap Job"]
    fn linux_cgroup_root_exit_keeps_descendant_debit_until_recursive_kill() {
        use std::process::{Command, Stdio};
        let root = std::env::var_os("STILLYARD_TEST_CGROUP_ROOT")
            .expect("explicit protected test delegation");
        let boundary = Boundary::create(Path::new(&root), uuid::Uuid::now_v7()).unwrap();
        // This trusted fixture waits before forking its live descendant. It is
        // a cleanup control, not evidence of the executor's user-code barrier.
        let mut child = Command::new("/usr/bin/python3").args(["-c", "import subprocess,sys; assert sys.stdin.read(1)=='R'; subprocess.Popen(['/usr/bin/python3','-c','import time; time.sleep(60)']); print('forked',flush=True)"])
            .stdin(Stdio::piped()).stdout(Stdio::piped()).spawn().unwrap();
        let process =
            crate::identity::linux::Process::open(child.id(), unsafe { libc::geteuid() }).unwrap();
        boundary.attach_stopped(child.id()).unwrap();
        assert!(boundary.contains_process(process.proc_handle()).unwrap());
        child.stdin.take().unwrap().write_all(b"R").unwrap();
        let until = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(exit) = child.try_wait().unwrap() {
                assert!(exit.success());
                break;
            }
            assert!(Instant::now() < until, "trusted fixture root did not exit");
            std::thread::sleep(Duration::from_millis(10));
        }
        // A root-only implementation would return false here and release early.
        assert!(boundary.populated().unwrap());
        let reopened = Boundary::reopen(&boundary.identity).unwrap();
        assert!(reopened.populated().unwrap());
        reopened
            .kill_and_seal(Instant::now() + Duration::from_secs(5))
            .unwrap();
        assert!(Boundary::reopen(&boundary.identity).is_err());
    }
}
