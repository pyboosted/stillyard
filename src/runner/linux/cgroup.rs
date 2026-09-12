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

/// Called under stopped Store/history locks after full durable quiescence.
/// This creates no Invocation boundary and publishes no cleanup proof.
pub(crate) fn restore_executor_root(path: &Path, ram_mb: u64) -> io::Result<Identity> {
    // SAFETY: geteuid has no preconditions.
    let uid = unsafe { libc::geteuid() };
    let expected = PathBuf::from(format!(
        "/sys/fs/cgroup/user.slice/user-{uid}.slice/user@{uid}.service/app.slice/stillyard-delegation.service/executors"
    ));
    if path != expected || !(512..=1_048_576).contains(&ram_mb) {
        return Err(io::Error::other(
            "native restore requires its supported delegated unit and saved RAM budget",
        ));
    }
    let parent = path.parent().unwrap();
    if std::fs::canonicalize(parent)? != parent {
        return Err(io::Error::other(
            "native delegation parent is not canonical",
        ));
    }
    let parent_guard = open_directory(parent)?;
    if parent_guard.metadata()?.uid() != uid {
        return Err(io::Error::other(
            "native delegation parent is not owned by this user",
        ));
    }
    let pinned = PathBuf::from(format!("/proc/self/fd/{}", parent_guard.as_raw_fd()));
    let controllers = std::fs::read_to_string(pinned.join("cgroup.subtree_control"))?;
    if !["cpu", "memory", "pids"]
        .iter()
        .all(|name| controllers.split_whitespace().any(|value| value == *name))
    {
        return Err(io::Error::other(
            "native executor controllers are not delegated",
        ));
    }
    let root = pinned.join("executors");
    let created = match std::fs::symlink_metadata(&root) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            std::fs::create_dir(&root)?;
            true
        }
        Err(e) => return Err(e),
        Ok(_) => false,
    };
    let directory = open_directory(&root)?;
    let metadata = directory.metadata()?;
    if metadata.uid() != uid {
        return Err(io::Error::other("native executor owner changed"));
    }
    let identity = Identity {
        boot_id: boot()?,
        path: path.to_path_buf(),
        device: metadata.dev(),
        inode: metadata.ino(),
    };
    let boundary = Boundary {
        directory,
        identity,
    };
    if boundary.populated()?
        || std::fs::read_dir(&root)?.any(|entry| {
            entry.map_or(true, |e| {
                e.file_type().map_or(true, |t| t.is_dir() || t.is_symlink())
            })
        })
    {
        return Err(io::Error::other(
            "native executor root is not empty or contains unexpected children",
        ));
    }
    let memory = (ram_mb * 1024 * 1024).to_string();
    if created {
        std::fs::write(
            boundary.file("cgroup.subtree_control"),
            "+cpu +memory +pids",
        )?;
        std::fs::write(boundary.file("memory.max"), &memory)?;
        std::fs::write(boundary.file("pids.max"), "4096")?;
    }
    let controllers = std::fs::read_to_string(boundary.file("cgroup.subtree_control"))?;
    if std::fs::read_to_string(boundary.file("memory.max"))?.trim() != memory
        || std::fs::read_to_string(boundary.file("pids.max"))?.trim() != "4096"
        || !["cpu", "memory", "pids"]
            .iter()
            .all(|name| controllers.split_whitespace().any(|v| v == *name))
    {
        return Err(io::Error::other(
            "native executor configuration is partial or changed; refusing repair",
        ));
    }
    OpenOptions::new()
        .write(true)
        .open(boundary.file("cgroup.kill"))?;
    OpenOptions::new()
        .write(true)
        .open(boundary.file("cgroup.procs"))?;
    boundary.check_identity()?;
    let parent_now = std::fs::symlink_metadata(parent)?;
    if (parent_now.dev(), parent_now.ino())
        != (
            parent_guard.metadata()?.dev(),
            parent_guard.metadata()?.ino(),
        )
    {
        return Err(io::Error::other(
            "native delegation parent changed during restoration",
        ));
    }
    Ok(boundary.identity)
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
