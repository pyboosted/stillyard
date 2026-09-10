//! Trusted stub and kernel exec-stop barrier. No user entry point runs before
//! the caller commits its start right and invokes `release` on the tracer thread.
use super::cgroup::Boundary;
use crate::identity::linux::Process;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::ffi::CString;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::net::UnixListener;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

#[cfg(test)]
thread_local! {
    static FORCE_IMAGE_MISMATCH: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LaunchSpec {
    pub(super) executable: PathBuf,
    pub(super) args: Vec<String>,
    pub(super) working_directory: PathBuf,
    pub(super) environment: BTreeMap<String, String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireLaunch {
    spec: LaunchSpec,
    control_key: [u8; 32],
    script_sha256: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecPermission {
    mac: [u8; 32],
}

fn permission_mac(key: &[u8; 32], ready: &Ready) -> io::Result<hmac::Hmac<Sha256>> {
    use hmac::Mac;
    let mut mac = hmac::Hmac::<Sha256>::new_from_slice(key).map_err(io::Error::other)?;
    mac.update(b"stillyard-linux-kernel-exec-permission-v1\0");
    mac.update(&serde_json::to_vec(ready)?);
    Ok(mac)
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Ready {
    challenge: uuid::Uuid,
    requested_sha256: String,
    image_device: u64,
    image_inode: u64,
    image_sha256: String,
    cwd_device: u64,
    cwd_inode: u64,
}

struct ControlDirectory(PathBuf);
impl ControlDirectory {
    fn new() -> io::Result<Self> {
        let path = std::env::temp_dir().join(format!("sy-{}", uuid::Uuid::now_v7().simple()));
        std::fs::DirBuilder::new().mode(0o700).create(&path)?;
        Ok(Self(path))
    }
}
impl Drop for ControlDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(self.0.join("control"));
        let _ = std::fs::remove_file(self.0.join("spec.json"));
        let _ = std::fs::remove_dir(&self.0);
    }
}

fn invalid(message: &str) -> io::Error {
    io::Error::other(message)
}
fn hash(file: &mut File) -> io::Result<String> {
    file.seek(SeekFrom::Start(0))?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 65536];
    let mut total = 0_u64;
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        total += n as u64;
        if total > 1024 * 1024 * 1024 {
            return Err(invalid("executable exceeds 1 GiB inspection bound"));
        }
        digest.update(&buffer[..n]);
    }
    file.seek(SeekFrom::Start(0))?;
    Ok(format!("{:x}", digest.finalize()))
}

fn snapshot_script(path: &Path) -> io::Result<Option<(File, String)>> {
    let mut source = File::open(path)?;
    let mut magic = [0; 2];
    if source.read(&mut magic)? != 2 || magic != *b"#!" {
        return Ok(None);
    }
    // Mount the private snapshot at the requested script name, preserving
    // interpreter argv, __file__, sibling imports and script-relative resources.
    if std::fs::canonicalize(path)? != path {
        return Err(invalid(
            "script execution requires its canonical path; resolve symlinks before submission",
        ));
    }
    if source.metadata()?.mode() & 0o111 == 0 {
        return Err(invalid("script is not executable"));
    }
    source.seek(SeekFrom::Start(0))?;
    let name = CString::new("stillyard-script").unwrap();
    // SAFETY: valid NUL-terminated name and flags; successful fd is newly owned.
    let fd =
        unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_ALLOW_SEALING | libc::MFD_CLOEXEC) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut snapshot = unsafe { File::from_raw_fd(fd) };
    if std::io::copy(&mut source.take(64 * 1024 * 1024 + 1), &mut snapshot)? > 64 * 1024 * 1024 {
        return Err(invalid("script exceeds 64 MiB snapshot bound"));
    }
    let digest = hash(&mut snapshot)?;
    // SAFETY: owned memfd with sealing enabled and no writable mappings.
    if unsafe {
        libc::fcntl(
            fd,
            libc::F_ADD_SEALS,
            libc::F_SEAL_WRITE | libc::F_SEAL_GROW | libc::F_SEAL_SHRINK | libc::F_SEAL_SEAL,
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(Some((snapshot, digest)))
}

fn ptrace(request: libc::c_uint, pid: u32, data: usize) -> io::Result<()> {
    // SAFETY: request accepts a PID, null address and integer/pointer-sized data;
    // no borrowed memory is passed to the kernel.
    if unsafe {
        libc::ptrace(
            request,
            pid as libc::pid_t,
            std::ptr::null_mut::<libc::c_void>(),
            data as *mut libc::c_void,
        )
    } == -1
    {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
fn wait_status(pid: u32) -> io::Result<Option<i32>> {
    let mut status = 0;
    // SAFETY: one exact traced PID and writable status; never reap another worker.
    let result = unsafe { libc::waitpid(pid as i32, &mut status, libc::__WALL | libc::WNOHANG) };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok((result > 0).then_some(status))
}

pub(super) struct PreparedLaunch {
    pub(super) wrapper: Child,
    pub(super) root: Process,
    pub(super) requested_sha256: String,
    tracer: libc::pid_t,
    released: bool,
    finished: bool,
}

impl PreparedLaunch {
    pub(super) fn prepare(
        boundary: &Boundary,
        helper: &Path,
        spec: &LaunchSpec,
        stdin: File,
        deadline: Instant,
    ) -> io::Result<Self> {
        let control = ControlDirectory::new()?;
        let spec_path = control.0.join("spec.json");
        let mut control_key = [0; 32];
        getrandom::fill(&mut control_key).map_err(io::Error::other)?;
        let script = snapshot_script(&spec.executable)?;
        let script_sha256 = script.as_ref().map(|(_, digest)| digest.clone());
        let bytes = serde_json::to_vec(
            &serde_json::json!({"spec": spec, "control_key": control_key, "script_sha256": script_sha256}),
        )?;
        if bytes.len() > crate::machine::MAX_FRAME_BYTES {
            return Err(invalid("launch spec exceeds frame bound"));
        }
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&spec_path)?;
        file.write_all(&bytes)?;
        drop(file);
        let socket_path = control.0.join("control");
        let listener = UnixListener::bind(&socket_path)?;
        std::fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(0o600))?;
        listener.set_nonblocking(true)?;
        let attachment = boundary.attachment_file()?;
        let mut command = Command::new("/usr/bin/bwrap");
        command.args([
            "--bind",
            "/",
            "/",
            "--dev",
            "/dev",
            "--proc",
            "/proc",
            "--unshare-user",
            "--unshare-pid",
            "--unshare-uts",
            "--unshare-cgroup",
            "--new-session",
            "--die-with-parent",
            "--ro-bind",
            "/dev/null",
            "/init",
            "--tmpfs",
            "/run",
            "--ro-bind",
            "/sys/fs/cgroup",
            "/sys/fs/cgroup",
        ]);
        // Tests receive a writable alias inside the outer bootstrap cgroup.
        // The actual Invocation must not inherit that alternate writable mount.
        if let Some(alias) = std::env::var_os("STILLYARD_TEST_CGROUP_ROOT") {
            command.arg("--ro-bind").arg(&alias).arg(&alias);
        }
        if let Some((snapshot, _)) = &script {
            command
                .args(["--perms", "0500", "--ro-bind-data"])
                .arg(snapshot.as_raw_fd().to_string())
                .arg(&spec.executable);
        }
        command.arg("--").arg(helper);
        command
            .arg("linux-executor-stub")
            .arg("--spec")
            .arg(&spec_path)
            .arg("--control")
            .arg(&socket_path);
        // Apply caller environment only in final exec, never to trusted code.
        command.env_clear();
        command
            .stdin(stdin)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        // SAFETY: the fork child performs only one async-signal-safe write before
        // exec; captured File keeps this exact cgroup fd alive in the parent.
        unsafe {
            command.pre_exec(move || {
                if let Some((snapshot, _)) = &script {
                    if libc::fcntl(snapshot.as_raw_fd(), libc::F_SETFD, 0) != 0 {
                        return Err(io::Error::last_os_error());
                    }
                }
                if libc::write(attachment.as_raw_fd(), b"0".as_ptr().cast(), 1) != 1 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut wrapper = command.spawn()?;
        let result = (|| {
            let socket = loop {
                match listener.accept() {
                    Ok((socket, _)) => break socket,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        if wrapper.try_wait()?.is_some() {
                            return Err(invalid(
                                "trusted namespace wrapper exited before readiness",
                            ));
                        }
                        if Instant::now() >= deadline {
                            return Err(io::Error::new(
                                io::ErrorKind::TimedOut,
                                "trusted stub readiness timed out",
                            ));
                        }
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => return Err(error),
                }
            };
            let root = Process::from_socket(&socket)?;
            root.verify_executable(helper)?;
            if !boundary.contains_process(root.proc_handle())? {
                return Err(invalid("trusted stub is outside recorded cgroup"));
            }
            let crate::ProcessIdentity::Linux { pid, .. } = root.identity else {
                return Err(invalid("non-Linux stub identity"));
            };
            let mut stream = crate::client::linux::DeadlineStream {
                stream: socket,
                deadline,
            };
            let ready: Ready = crate::machine::read_frame(&mut stream)?;
            if script_sha256
                .as_ref()
                .is_some_and(|expected| *expected != ready.requested_sha256)
            {
                return Err(invalid("mounted script differs from parent snapshot"));
            }
            ptrace(
                libc::PTRACE_SEIZE,
                pid,
                (libc::PTRACE_O_TRACEEXEC | libc::PTRACE_O_EXITKILL) as usize,
            )?;
            use hmac::Mac;
            let permission = ExecPermission {
                mac: permission_mac(&control_key, &ready)?
                    .finalize()
                    .into_bytes()
                    .into(),
            };
            // Authorizes only kernel exec, never the user entry point. The
            // namespace-hidden parent proves knowledge of this private key.
            crate::machine::write_frame(&mut stream, &permission)?;
            loop {
                if let Some(status) = wait_status(pid)? {
                    if !libc::WIFSTOPPED(status) || status >> 16 != libc::PTRACE_EVENT_EXEC {
                        return Err(invalid("unexpected status before kernel exec barrier"));
                    }
                    break;
                }
                if Instant::now() >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "kernel exec barrier timed out",
                    ));
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            let base = PathBuf::from(format!("/proc/self/fd/{}", root.proc_handle()));
            let mut image = File::open(base.join("exe"))?;
            let image_meta = image.metadata()?;
            let cwd_meta = std::fs::metadata(base.join("cwd"))?;
            #[cfg(test)]
            let ready = {
                let mut ready = ready;
                if FORCE_IMAGE_MISMATCH.replace(false) {
                    ready.image_inode ^= 1;
                }
                ready
            };
            if (image_meta.dev(), image_meta.ino()) != (ready.image_device, ready.image_inode)
                || hash(&mut image)? != ready.image_sha256
                || (cwd_meta.dev(), cwd_meta.ino()) != (ready.cwd_device, ready.cwd_inode)
                || !boundary.contains_process(root.proc_handle())?
            {
                return Err(invalid(
                    "loaded executable, interpreter, working directory or cgroup differs at exec stop",
                ));
            }
            Ok((root, ready.requested_sha256))
        })();
        match result {
            Ok((root, requested_sha256)) => Ok(Self {
                wrapper,
                root,
                requested_sha256,
                tracer: unsafe { libc::gettid() },
                released: false,
                finished: false,
            }),
            Err(error) => {
                let _ = wrapper.kill();
                Err(error)
            }
        }
    }

    fn pid(&self) -> u32 {
        match self.root.identity {
            crate::ProcessIdentity::Linux { pid, .. } => pid,
            _ => unreachable!(),
        }
    }
    fn check_thread(&self) -> io::Result<()> {
        // SAFETY: gettid has no preconditions.
        if unsafe { libc::gettid() } != self.tracer {
            return Err(invalid("ptrace barrier moved to another thread"));
        }
        Ok(())
    }
    pub(super) fn release(&mut self) -> io::Result<()> {
        self.check_thread()?;
        if self.released || self.finished {
            return Err(invalid("Invocation release was already attempted"));
        }
        self.released = true; // Any OS error after this point is possibly released.
        ptrace(libc::PTRACE_CONT, self.pid(), 0)
    }
    pub(super) fn poll_root(&mut self) -> io::Result<Option<i32>> {
        self.check_thread()?;
        if !self.released || self.finished {
            return Err(invalid("root is not a running Invocation"));
        }
        let Some(status) = wait_status(self.pid())? else {
            return Ok(None);
        };
        if libc::WIFEXITED(status) || libc::WIFSIGNALED(status) {
            self.finished = true;
            return Ok(Some(if libc::WIFEXITED(status) {
                libc::WEXITSTATUS(status)
            } else {
                128 + libc::WTERMSIG(status)
            }));
        }
        if libc::WIFSTOPPED(status) {
            let event = status >> 16;
            if event == libc::PTRACE_EVENT_STOP {
                ptrace(libc::PTRACE_LISTEN, self.pid(), 0)?;
            } else {
                let signal = if event == libc::PTRACE_EVENT_EXEC {
                    0
                } else {
                    libc::WSTOPSIG(status)
                };
                ptrace(libc::PTRACE_CONT, self.pid(), signal as usize)?;
            }
        }
        Ok(None)
    }

    /// Resource release requires the separate durable cgroup seal. This only
    /// reaps our traced child and wrapper after that cleanup has completed.
    pub(super) fn reap_after_cleanup(&mut self, deadline: Instant) -> io::Result<Option<i32>> {
        self.check_thread()?;
        let mut exit = None;
        while !self.finished {
            if let Some(status) = wait_status(self.pid())? {
                if libc::WIFEXITED(status) || libc::WIFSIGNALED(status) {
                    self.finished = true;
                    exit = Some(if libc::WIFEXITED(status) {
                        libc::WEXITSTATUS(status)
                    } else {
                        128 + libc::WTERMSIG(status)
                    });
                } else if libc::WIFSTOPPED(status) {
                    ptrace(libc::PTRACE_CONT, self.pid(), libc::SIGKILL as usize)?;
                }
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "cleaned tracer root could not be reaped",
                ));
            }
            if !self.finished {
                std::thread::sleep(Duration::from_millis(5));
            }
        }
        while self.wrapper.try_wait()?.is_none() {
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "cleaned wrapper could not be reaped",
                ));
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        Ok(exit)
    }
}
impl Drop for PreparedLaunch {
    fn drop(&mut self) {
        // Kill-on-parent-death is only a kill request. Caller still needs the
        // recursive cgroup seal; neither wrapper exit nor this Drop releases it.
        let _ = self.wrapper.kill();
        let _ = self.wrapper.try_wait();
    }
}

pub(crate) fn run_stub(spec_path: &Path, control: &Path) -> io::Result<()> {
    // SO_PEERCRED identifies the thread group. The actual exec must occur on
    // its sole leader; exec from an untraced libtest worker can kill the leader.
    // SAFETY: getpid/gettid have no preconditions.
    if unsafe { libc::getpid() != libc::gettid() }
        || std::fs::read_dir("/proc/self/task")?.count() != 1
    {
        return Err(invalid(
            "trusted exec helper must have one thread on its process leader",
        ));
    }
    let mut bytes = Vec::new();
    File::open(spec_path)?
        .take(crate::machine::MAX_FRAME_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > crate::machine::MAX_FRAME_BYTES {
        return Err(invalid("launch spec exceeds frame bound"));
    }
    let wire: WireLaunch = serde_json::from_slice(&bytes)?;
    let spec = wire.spec;
    if !spec.executable.is_absolute() || !spec.working_directory.is_absolute() {
        return Err(invalid("launch paths must be absolute"));
    }
    let mut executable = File::open(&spec.executable)?;
    if !executable.metadata()?.is_file() || executable.metadata()?.mode() & 0o111 == 0 {
        return Err(invalid("requested image is not executable"));
    }
    let requested_sha256 = hash(&mut executable)?;
    let mut header = [0_u8; 256];
    let count = executable.read(&mut header)?;
    executable.seek(SeekFrom::Start(0))?;
    let mut image = executable.try_clone()?;
    if header[..count].starts_with(b"#!") {
        let line = header[2..count]
            .split(|b| *b == b'\n')
            .next()
            .ok_or_else(|| invalid("invalid shebang"))?;
        let interpreter = std::str::from_utf8(line)
            .map_err(io::Error::other)?
            .trim_start()
            .split([' ', '\t'])
            .next()
            .ok_or_else(|| invalid("missing shebang interpreter"))?;
        if !Path::new(interpreter).is_absolute() {
            return Err(invalid("shebang interpreter must be absolute"));
        }
        image = File::open(interpreter)?;
        let mut magic = [0; 4];
        image.read_exact(&mut magic)?;
        if magic != *b"\x7fELF" {
            return Err(invalid("nested shebang interpreters are unsupported"));
        }
        if wire.script_sha256.as_deref() != Some(requested_sha256.as_str()) {
            return Err(invalid("script has no matching private mount snapshot"));
        }
    } else if !header[..count].starts_with(b"\x7fELF") {
        return Err(invalid(
            "requested image is neither ELF nor a supported shebang script",
        ));
    }
    if wire.script_sha256.is_some() && !header[..count].starts_with(b"#!") {
        return Err(invalid(
            "private script snapshot no longer contains a shebang",
        ));
    }
    let image_sha256 = hash(&mut image)?;
    let image_meta = image.metadata()?;
    let cwd = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY)
        .open(&spec.working_directory)?;
    let cwd_meta = cwd.metadata()?;
    let deadline = Instant::now() + Duration::from_secs(30);
    let socket = crate::client::linux::connect(
        control
            .to_str()
            .ok_or_else(|| invalid("non-UTF8 control path"))?,
        deadline,
    )?;
    let parent = crate::identity::linux::pin_socket_owner(&socket)?;
    let mut stream = crate::client::linux::DeadlineStream {
        stream: socket,
        deadline,
    };
    let ready = Ready {
        challenge: uuid::Uuid::now_v7(),
        requested_sha256,
        image_device: image_meta.dev(),
        image_inode: image_meta.ino(),
        image_sha256,
        cwd_device: cwd_meta.dev(),
        cwd_inode: cwd_meta.ino(),
    };
    crate::machine::write_frame(&mut stream, &ready)?;
    let permission: ExecPermission = crate::machine::read_frame(&mut stream)?;
    use hmac::Mac;
    permission_mac(&wire.control_key, &ready)?
        .verify_slice(&permission.mac)
        .map_err(|_| invalid("kernel-exec permission authentication failed"))?;
    parent.ensure_alive()?;
    let args = std::iter::once(spec.executable.as_os_str().as_bytes())
        .chain(spec.args.iter().map(|s| s.as_bytes()))
        .map(CString::new)
        .collect::<Result<Vec<_>, _>>()
        .map_err(io::Error::other)?;
    let environment = spec
        .environment
        .iter()
        .map(|(key, value)| {
            if key.is_empty() || key.contains('=') {
                return Err(invalid("invalid environment key"));
            }
            CString::new(format!("{key}={value}")).map_err(io::Error::other)
        })
        .collect::<io::Result<Vec<_>>>()?;
    let argv = args
        .iter()
        .map(|s| s.as_ptr())
        .chain(std::iter::once(std::ptr::null()))
        .collect::<Vec<_>>();
    let envp = environment
        .iter()
        .map(|s| s.as_ptr())
        .chain(std::iter::once(std::ptr::null()))
        .collect::<Vec<_>>();
    let script_path = wire
        .script_sha256
        .as_ref()
        .map(|_| CString::new(spec.executable.as_os_str().as_bytes()).map_err(io::Error::other))
        .transpose()?;
    // SAFETY: cwd/executable fds and all NUL-terminated argument buffers remain
    // live; successful fexecve never returns and the tracer stops before entry.
    unsafe {
        if libc::fchdir(cwd.as_raw_fd()) != 0 {
            return Err(io::Error::last_os_error());
        }
        if let Some(path) = &script_path {
            libc::execve(path.as_ptr(), argv.as_ptr(), envp.as_ptr());
        } else {
            libc::fexecve(executable.as_raw_fd(), argv.as_ptr(), envp.as_ptr());
        }
    }
    Err(io::Error::last_os_error())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "invoked only as the user program in the nested namespace control"]
    fn linux_attestation_client_fixture() {
        let expected: crate::ManagedParent =
            serde_json::from_str(&std::env::var("STILLYARD_TEST_PARENT").unwrap()).unwrap();
        let client = crate::Client::builder()
            .endpoint(std::env::var("STILLYARD_ENDPOINT").unwrap())
            .daemon_executable(std::env::var_os("STILLYARD_TEST_EXPECTED_SERVER").unwrap())
            .auto_start(false)
            .connect(Instant::now() + Duration::from_secs(5), None)
            .unwrap();
        let context = client
            .submission_context(Instant::now() + Duration::from_secs(5), None)
            .unwrap();
        assert_eq!(context.parent, Some(expected));
        assert_eq!(context.store_uuid, expected.invocation_id.store_uuid());
    }

    #[test]
    #[ignore = "requires delegated cgroup under a protected system bootstrap Job"]
    fn linux_namespace_attestation_uses_real_peer_and_rejects_outside_proxy() {
        use crate::identity::attestation::{ManagedServer, Signer, Trusted};
        use crate::protocol::{Request, Response};
        use std::sync::Arc;
        let root = std::env::var_os("STILLYARD_TEST_CGROUP_ROOT").unwrap();
        let temporary = tempfile::tempdir().unwrap();
        let endpoint = temporary.path().join("server.sock");
        let listener = UnixListener::bind(&endpoint).unwrap();
        std::fs::set_permissions(&endpoint, std::fs::Permissions::from_mode(0o600)).unwrap();
        listener.set_nonblocking(true).unwrap();
        let store = uuid::Uuid::now_v7();
        let generation = uuid::Uuid::now_v7();
        let parent = crate::ManagedParent {
            job_id: crate::JobId::from_parts(store, uuid::Uuid::now_v7()),
            attempt_id: crate::AttemptId::from_parts(store, uuid::Uuid::now_v7()),
            invocation_id: crate::InvocationId::from_parts(store, uuid::Uuid::now_v7()),
        };
        let signer = Signer::new(store, generation, endpoint.to_str().unwrap().into()).unwrap();
        let context: ManagedServer = signer.context(parent).unwrap();
        let expected_server = std::env::current_exe().unwrap();
        let helper = temporary.path().join("stillyard");
        std::fs::copy(
            std::env::var_os("STILLYARD_TEST_EXECUTABLE").unwrap(),
            &helper,
        )
        .unwrap();
        let program = temporary.path().join("client-fixture");
        std::fs::copy(&expected_server, &program).unwrap();
        let boundary = Arc::new(
            Boundary::create(Path::new(&root), parent.invocation_id.entity_uuid()).unwrap(),
        );
        let peer_boundary = Arc::clone(&boundary);
        let server = std::thread::spawn(move || {
            for index in 0..3 {
                let deadline = Instant::now() + Duration::from_secs(20);
                let socket = loop {
                    match listener.accept() {
                        Ok((socket, _)) => break socket,
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                            assert!(Instant::now() < deadline, "attested client never connected");
                            std::thread::sleep(Duration::from_millis(5));
                        }
                        Err(e) => panic!("{e}"),
                    }
                };
                let peer = Process::from_socket(&socket).unwrap();
                let mut socket = crate::client::linux::DeadlineStream {
                    stream: socket,
                    deadline,
                };
                let request: Request = crate::protocol::read_frame(&mut socket).unwrap();
                let Request::Attested {
                    daemon_generation,
                    parent: claimed,
                    nonce,
                    request,
                } = request
                else {
                    panic!("namespace client did not request an attestation");
                };
                assert_eq!(daemon_generation, generation);
                assert_eq!(claimed, parent);
                let member = peer_boundary.contains_process(peer.proc_handle()).unwrap();
                assert_eq!(
                    member,
                    index < 2,
                    "proxy did not have the expected kernel membership"
                );
                let response = if member {
                    let hash = crate::identity::attestation::request_hash(&request).unwrap();
                    let response = match *request {
                        Request::Ping {} => Response::Pong {
                            protocol_version: crate::protocol::PROTOCOL_VERSION,
                        },
                        Request::SubmissionContext { claimed_parent }
                            if claimed_parent == Some(parent) =>
                        {
                            Response::SubmissionContext(crate::SubmissionContext {
                                store_uuid: store,
                                parent: Some(parent),
                            })
                        }
                        _ => panic!("unexpected namespace request"),
                    };
                    Response::Attested(Box::new(
                        signer.sign(parent, nonce, hash, response).unwrap(),
                    ))
                } else {
                    Response::Error {
                        code: "unmanaged_proxy".into(),
                        message: "actual peer is outside the Invocation".into(),
                    }
                };
                crate::protocol::write_frame(&mut socket, &response).unwrap();
            }
        });
        let environment = [
            ("STILLYARD_ENDPOINT".into(), endpoint.display().to_string()),
            ("STILLYARD_JOB_ID".into(), parent.job_id.to_string()),
            ("STILLYARD_ATTEMPT".into(), parent.attempt_id.to_string()),
            (
                "STILLYARD_INVOCATION_ID".into(),
                parent.invocation_id.to_string(),
            ),
            ("STILLYARD_DAEMON_ID".into(), generation.to_string()),
            ("STILLYARD_ROLE".into(), "primary".into()),
            (
                "STILLYARD_SERVER_ATTESTATION".into(),
                serde_json::to_string(&context).unwrap(),
            ),
            (
                "STILLYARD_TEST_EXPECTED_SERVER".into(),
                expected_server.display().to_string(),
            ),
            (
                "STILLYARD_TEST_PARENT".into(),
                serde_json::to_string(&parent).unwrap(),
            ),
        ]
        .into();
        let spec = LaunchSpec {
            executable: program,
            args: vec![
                "--exact".into(),
                "runner::linux::launch::tests::linux_attestation_client_fixture".into(),
                "--ignored".into(),
                "--nocapture".into(),
            ],
            working_directory: temporary.path().into(),
            environment,
        };
        let mut launch = PreparedLaunch::prepare(
            &boundary,
            &helper,
            &spec,
            File::open("/dev/null").unwrap(),
            Instant::now() + Duration::from_secs(15),
        )
        .unwrap();
        launch.release().unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        let exit = loop {
            if let Some(code) = launch.poll_root().unwrap() {
                break code;
            }
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(5));
        };
        let trusted = Trusted::new(
            context,
            endpoint.to_str().unwrap(),
            &expected_server,
            parent,
        )
        .unwrap();
        assert!(
            crate::client::linux::attested_request(
                endpoint.to_str().unwrap(),
                &trusted,
                &Request::Ping {},
                Instant::now() + Duration::from_secs(5)
            )
            .is_err(),
            "outside proxy received an authenticated parent response"
        );
        server.join().unwrap();
        boundary
            .kill_and_seal(Instant::now() + Duration::from_secs(5))
            .unwrap();
        let mut stderr = String::new();
        launch
            .wrapper
            .stderr
            .take()
            .unwrap()
            .read_to_string(&mut stderr)
            .unwrap();
        assert_eq!(exit, 0, "nested client failed: {stderr}");
    }

    #[test]
    fn linux_exec_permission_binds_private_key_and_fresh_ready_challenge() {
        use hmac::Mac;
        let mut ready = Ready {
            challenge: uuid::Uuid::now_v7(),
            requested_sha256: "a".repeat(64),
            image_device: 1,
            image_inode: 2,
            image_sha256: "b".repeat(64),
            cwd_device: 1,
            cwd_inode: 3,
        };
        let signature = permission_mac(&[7; 32], &ready)
            .unwrap()
            .finalize()
            .into_bytes();
        assert!(
            permission_mac(&[7; 32], &ready)
                .unwrap()
                .verify_slice(&signature)
                .is_ok()
        );
        assert!(
            permission_mac(&[8; 32], &ready)
                .unwrap()
                .verify_slice(&signature)
                .is_err()
        );
        ready.challenge = uuid::Uuid::now_v7();
        assert!(
            permission_mac(&[7; 32], &ready)
                .unwrap()
                .verify_slice(&signature)
                .is_err()
        );
    }

    #[test]
    #[ignore = "requires delegated cgroup under a protected system bootstrap Job"]
    fn linux_exec_barrier_prevents_user_code_and_pins_script_bytes() {
        let root = std::env::var_os("STILLYARD_TEST_CGROUP_ROOT").expect("protected delegation");
        let temporary = tempfile::tempdir().unwrap();
        let helper = temporary.path().join("stillyard");
        let built =
            std::env::var_os("STILLYARD_TEST_EXECUTABLE").expect("explicit compiled CLI fixture");
        std::fs::copy(built, &helper).unwrap();
        let preload_source = temporary.path().join("preload.c");
        let preload = temporary.path().join("preload.so");
        std::fs::write(
            &preload_source,
            r#"
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
__attribute__((constructor)) static void marker(void) {
    const char *path = getenv("STILLYARD_LOADER_MARKER");
    if (!path) return;
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (fd >= 0) { (void)write(fd, "loader", 6); close(fd); }
}
"#,
        )
        .unwrap();
        let compiled = Command::new("/usr/bin/cc")
            .args(["-shared", "-fPIC", "-o"])
            .arg(&preload)
            .arg(&preload_source)
            .output()
            .unwrap();
        assert!(
            compiled.status.success(),
            "{}",
            String::from_utf8_lossy(&compiled.stderr)
        );
        for script in [false, true] {
            let boundary = Boundary::create(Path::new(&root), uuid::Uuid::now_v7()).unwrap();
            let marker = temporary.path().join(if script {
                "script-marker"
            } else {
                "elf-marker"
            });
            let escaped = temporary.path().join("changed-script-marker");
            let loader_marker = temporary.path().join(if script {
                "script-loader"
            } else {
                "elf-loader"
            });
            let code = format!(
                "import errno,pathlib,subprocess; pathlib.Path({:?}).write_text('original');\ntry:\n subprocess.run(['/mnt/c/Windows/System32/cmd.exe','/d','/c','exit 0'],check=False)\nexcept OSError as e:\n assert e.errno == errno.EACCES, e\nelse:\n raise AssertionError('Windows interop escaped')\nprint('execution-and-interop-control-passed',flush=True)\n",
                marker.to_str().unwrap()
            );
            let (executable, args) = if script {
                let path = temporary.path().join("program.py");
                std::fs::write(
                    &path,
                    format!(
                        "#!/usr/bin/python3\nassert __file__ == {:?}, __file__\n{code}",
                        path.to_str().unwrap()
                    ),
                )
                .unwrap();
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
                (path, vec![])
            } else {
                (PathBuf::from("/usr/bin/python3"), vec!["-c".into(), code])
            };
            let spec = LaunchSpec {
                executable: executable.clone(),
                args,
                working_directory: temporary.path().to_owned(),
                environment: [
                    ("PATH".into(), "/usr/bin:/bin".into()),
                    ("LD_PRELOAD".into(), preload.display().to_string()),
                    (
                        "STILLYARD_LOADER_MARKER".into(),
                        loader_marker.display().to_string(),
                    ),
                ]
                .into(),
            };
            let expected = hash(&mut File::open(&executable).unwrap()).unwrap();
            let mut launch = PreparedLaunch::prepare(
                &boundary,
                &helper,
                &spec,
                File::open("/dev/null").unwrap(),
                Instant::now() + Duration::from_secs(15),
            )
            .unwrap();
            assert_eq!(launch.requested_sha256, expected);
            assert!(boundary.populated().unwrap());
            std::thread::sleep(Duration::from_millis(50));
            assert!(!marker.exists(), "user code ran before committed release");
            assert!(
                !loader_marker.exists(),
                "caller LD_PRELOAD ran in the trusted helper or before exec release"
            );
            if script {
                std::fs::write(&executable, format!("#!/usr/bin/python3\nimport pathlib; pathlib.Path({:?}).write_text('changed')\n", escaped.to_str().unwrap())).unwrap();
            }
            launch.release().unwrap();
            assert!(launch.release().is_err(), "second release was accepted");
            let until = Instant::now() + Duration::from_secs(10);
            let exit = loop {
                if let Some(code) = launch.poll_root().unwrap() {
                    break code;
                }
                assert!(Instant::now() < until, "released root did not exit");
                std::thread::sleep(Duration::from_millis(5));
            };
            boundary
                .kill_and_seal(Instant::now() + Duration::from_secs(5))
                .unwrap();
            let mut stderr = String::new();
            launch
                .wrapper
                .stderr
                .take()
                .unwrap()
                .read_to_string(&mut stderr)
                .unwrap();
            assert_eq!(exit, 0, "{stderr}");
            assert_eq!(std::fs::read_to_string(&marker).unwrap(), "original");
            assert_eq!(std::fs::read_to_string(&loader_marker).unwrap(), "loader");
            assert!(
                !escaped.exists(),
                "interpreter read replaced source instead of sealed script"
            );
        }
    }

    #[test]
    #[ignore = "requires delegated cgroup under a protected system bootstrap Job"]
    fn linux_exec_barrier_bad_image_and_abandoned_release_never_run_user_code() {
        let root = std::env::var_os("STILLYARD_TEST_CGROUP_ROOT").expect("protected delegation");
        let temporary = tempfile::tempdir().unwrap();
        let helper = temporary.path().join("stillyard");
        std::fs::copy(
            std::env::var_os("STILLYARD_TEST_EXECUTABLE").unwrap(),
            &helper,
        )
        .unwrap();
        let marker = temporary.path().join("must-not-run");
        let spec = LaunchSpec {
            executable: "/usr/bin/python3".into(),
            args: vec![
                "-c".into(),
                format!(
                    "from pathlib import Path; Path({:?}).touch()",
                    marker.to_str().unwrap()
                ),
            ],
            working_directory: temporary.path().to_owned(),
            environment: BTreeMap::new(),
        };
        for mismatch in [true, false] {
            let boundary = Boundary::create(Path::new(&root), uuid::Uuid::now_v7()).unwrap();
            FORCE_IMAGE_MISMATCH.set(mismatch);
            let result = PreparedLaunch::prepare(
                &boundary,
                &helper,
                &spec,
                File::open("/dev/null").unwrap(),
                Instant::now() + Duration::from_secs(15),
            );
            if mismatch {
                assert!(
                    matches!(&result, Err(error) if error.to_string().contains("loaded executable")),
                    "loaded image mismatch was not rejected at the kernel barrier"
                );
            } else {
                assert!(result.is_ok());
            }
            // An abandoned prepared launch cannot acquire permission by EOF.
            drop(result);
            boundary
                .kill_and_seal(Instant::now() + Duration::from_secs(5))
                .unwrap();
            assert!(!marker.exists(), "failed/abandoned launch ran user code");
        }
    }
}
