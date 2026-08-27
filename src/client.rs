#[cfg(windows)]
use std::ffi::OsStr;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use directories::ProjectDirs;
use sha2::{Digest, Sha256};

use crate::protocol::{PROTOCOL_VERSION, Request, Response};
#[cfg(windows)]
use crate::protocol::{read_frame, write_frame};
use crate::{
    BatchReceipt, BatchSpec, CancellationToken, DaemonSnapshot, Error, JobId, JobReceipt,
    JobSnapshot, JobSpec, LogChunk, LogStream, RecoveryResult, Result, SubmitOptions,
};

#[derive(Clone, Debug)]
pub struct ClientBuilder {
    endpoint: Option<String>,
    auto_start: bool,
    daemon_executable: Option<PathBuf>,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self {
            endpoint: None,
            auto_start: true,
            daemon_executable: None,
        }
    }
}

impl ClientBuilder {
    #[must_use]
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    #[must_use]
    pub fn auto_start(mut self, auto_start: bool) -> Self {
        self.auto_start = auto_start;
        self
    }

    /// Selects the daemon binary used for auto-start and server-image authentication.
    #[must_use]
    pub fn daemon_executable(mut self, executable: impl Into<PathBuf>) -> Self {
        self.daemon_executable = Some(executable.into());
        self
    }

    pub fn connect(
        self,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<Client> {
        let explicit_endpoint = self.endpoint.is_some();
        let endpoint = self.endpoint.unwrap_or(default_endpoint()?);
        let daemon_executable = self
            .daemon_executable
            .map(Ok)
            .unwrap_or_else(default_daemon_executable)?;
        let client = Client {
            endpoint,
            daemon_executable,
        };
        match client.ping(deadline, cancellation) {
            Ok(()) => Ok(client),
            Err(Error::Unavailable(_)) if self.auto_start => {
                if std::env::var_os("STILLYARD_JOB_ID").is_some()
                    || std::env::var_os("STILLYARD_ROLE").is_some()
                {
                    return Err(Error::Unavailable(
                        "a managed child may not auto-start the daemon".into(),
                    ));
                }
                if explicit_endpoint {
                    return Err(Error::Unavailable(
                        "auto-start is unavailable for an explicit endpoint".into(),
                    ));
                }
                let mut daemon = start_daemon(&client.daemon_executable)?;
                let startup_deadline = deadline.min(Instant::now() + Duration::from_secs(10));
                let mut child_exit = None;
                loop {
                    if let Err(error) = check_wait(startup_deadline, cancellation) {
                        return Err(match error {
                            Error::DeadlineElapsed => Error::Unavailable(match child_exit {
                                Some(status) => format!(
                                    "daemon did not become ready within 10 seconds; spawned candidate exited with {status}"
                                ),
                                None => "daemon did not become ready within 10 seconds".into(),
                            }),
                            other => other,
                        });
                    }
                    if child_exit.is_none() {
                        child_exit = daemon.try_wait()?;
                    }
                    match client.ping(startup_deadline, cancellation) {
                        Ok(()) => return Ok(client),
                        Err(Error::Unavailable(_)) => std::thread::sleep(Duration::from_millis(25)),
                        Err(error) => return Err(error),
                    }
                }
            }
            Err(error) => Err(error),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Client {
    endpoint: String,
    daemon_executable: PathBuf,
}

impl Client {
    #[must_use]
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    pub fn connect(deadline: Instant, cancellation: Option<&CancellationToken>) -> Result<Self> {
        Self::builder().connect(deadline, cancellation)
    }

    pub fn submit(
        &self,
        spec: JobSpec,
        options: &SubmitOptions,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<JobReceipt> {
        spec.validate()?;
        let normalized = serde_json::to_vec(&spec)?;
        let payload_hash = format!("{:x}", Sha256::digest(&normalized));
        if let Some(path) = &options.result_file {
            write_initial_result_file(path, options, &payload_hash)?;
        }
        let response = self.request(
            Request::Submit {
                idempotency_key: options.idempotency_key,
                payload_hash: payload_hash.clone(),
                spec: Box::new(spec),
            },
            deadline,
            cancellation,
        )?;
        match response {
            Response::Submitted(receipt) => {
                if let Some(path) = &options.result_file {
                    write_result_file(
                        path,
                        options.idempotency_key,
                        &payload_hash,
                        Some(serde_json::to_value(&receipt)?),
                    )?;
                }
                Ok(receipt)
            }
            response => response_error(response),
        }
    }

    pub fn submit_batch(
        &self,
        spec: BatchSpec,
        options: &SubmitOptions,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<BatchReceipt> {
        spec.validate()?;
        let normalized = serde_json::to_vec(&spec)?;
        let payload_hash = format!("{:x}", Sha256::digest(&normalized));
        if let Some(path) = &options.result_file {
            write_initial_result_file(path, options, &payload_hash)?;
        }
        let response = self.request(
            Request::SubmitBatch {
                idempotency_key: options.idempotency_key,
                payload_hash: payload_hash.clone(),
                spec: Box::new(spec),
            },
            deadline,
            cancellation,
        )?;
        match response {
            Response::BatchSubmitted(receipt) => {
                if let Some(path) = &options.result_file {
                    write_result_file(
                        path,
                        options.idempotency_key,
                        &payload_hash,
                        Some(serde_json::to_value(&receipt)?),
                    )?;
                }
                Ok(receipt)
            }
            response => response_error(response),
        }
    }

    pub fn recover_submission(
        &self,
        idempotency_key: uuid::Uuid,
        payload_hash: impl Into<String>,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<RecoveryResult> {
        match self.request(
            Request::Recover {
                idempotency_key,
                payload_hash: payload_hash.into(),
            },
            deadline,
            cancellation,
        )? {
            Response::Recovered(recovery) => Ok(recovery),
            response => response_error(response),
        }
    }

    pub fn recover_result_file(
        &self,
        path: &Path,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<RecoveryResult> {
        let record: ResultFileRecord = serde_json::from_reader(std::fs::File::open(path)?)?;
        if record.version != 1 {
            return Err(Error::Protocol(format!(
                "unsupported result-file version {}",
                record.version
            )));
        }
        self.recover_submission(
            record.idempotency_key,
            record.payload_hash,
            deadline,
            cancellation,
        )
    }

    pub fn status(
        &self,
        job_id: JobId,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<JobSnapshot> {
        match self.request(Request::Status { job_id }, deadline, cancellation)? {
            Response::Snapshot(snapshot) => Ok(*snapshot),
            response => response_error(response),
        }
    }

    pub fn wait(
        &self,
        job_id: JobId,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<JobSnapshot> {
        loop {
            let response = self.request(
                Request::Wait {
                    job_id,
                    max_wait_millis: 1_000,
                },
                deadline,
                cancellation,
            )?;
            match response {
                Response::Snapshot(snapshot) if snapshot.is_final() => return Ok(*snapshot),
                Response::Snapshot(_) => continue,
                response => return response_error(response),
            }
        }
    }

    pub fn submit_and_wait(
        &self,
        spec: JobSpec,
        options: &SubmitOptions,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<JobSnapshot> {
        let receipt = self.submit(spec, options, deadline, cancellation)?;
        self.wait(receipt.job_id, deadline, cancellation)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn logs(
        &self,
        job_id: JobId,
        stream: LogStream,
        offset: u64,
        limit: u32,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<LogChunk> {
        match self.request(
            Request::Logs {
                job_id,
                stream,
                offset,
                limit,
            },
            deadline,
            cancellation,
        )? {
            Response::Logs(chunk) => Ok(chunk),
            response => response_error(response),
        }
    }

    pub fn daemon_status(
        &self,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<DaemonSnapshot> {
        match self.request(Request::DaemonStatus, deadline, cancellation)? {
            Response::DaemonStatus(status) => Ok(status),
            response => response_error(response),
        }
    }

    fn ping(&self, deadline: Instant, cancellation: Option<&CancellationToken>) -> Result<()> {
        match self.request(Request::Ping, deadline, cancellation)? {
            Response::Pong { protocol_version } if protocol_version == PROTOCOL_VERSION => Ok(()),
            Response::Pong { protocol_version } => Err(Error::Protocol(format!(
                "daemon protocol {protocol_version}, client protocol {PROTOCOL_VERSION}"
            ))),
            response => response_error(response),
        }
    }

    fn request(
        &self,
        request: Request,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<Response> {
        check_wait(deadline, cancellation)?;
        let endpoint = self.endpoint.clone();
        let daemon_executable = self.daemon_executable.clone();
        let (sender, receiver) = mpsc::sync_channel(1);
        std::thread::spawn(move || {
            let result = transport_request(&endpoint, &daemon_executable, &request, deadline);
            let _ = sender.send(result);
        });
        loop {
            check_wait(deadline, cancellation)?;
            let remaining = deadline.saturating_duration_since(Instant::now());
            match receiver.recv_timeout(remaining.min(Duration::from_millis(25))) {
                Ok(result) => return result,
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(Error::Unavailable("transport worker stopped".into()));
                }
            }
        }
    }
}

fn response_error<T>(response: Response) -> Result<T> {
    match response {
        Response::Error { code, message } if code == "invalid_spec" => {
            Err(Error::InvalidSpec(message))
        }
        Response::Error { code, message } => Err(Error::Protocol(format!("{code}: {message}"))),
        _ => Err(Error::Protocol("unexpected response variant".into())),
    }
}

fn check_wait(deadline: Instant, cancellation: Option<&CancellationToken>) -> Result<()> {
    if cancellation.is_some_and(CancellationToken::is_canceled) {
        return Err(Error::Canceled);
    }
    if Instant::now() >= deadline {
        return Err(Error::DeadlineElapsed);
    }
    Ok(())
}

#[cfg(windows)]
fn transport_request(
    endpoint: &str,
    daemon_executable: &Path,
    request: &Request,
    deadline: Instant,
) -> Result<Response> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Foundation::ERROR_PIPE_BUSY;
    use windows_sys::Win32::System::Pipes::WaitNamedPipeW;

    let mut pipe = loop {
        match OpenOptions::new().read(true).write(true).open(endpoint) {
            Ok(pipe) => break pipe,
            Err(error) if error.raw_os_error() == Some(ERROR_PIPE_BUSY as i32) => {
                if Instant::now() >= deadline {
                    return Err(Error::DeadlineElapsed);
                }
                let remaining = deadline.saturating_duration_since(Instant::now());
                let timeout = remaining.min(Duration::from_secs(1)).as_millis().max(1) as u32;
                let endpoint: Vec<u16> = OsStr::new(endpoint)
                    .encode_wide()
                    .chain(std::iter::once(0))
                    .collect();
                // SAFETY: endpoint is NUL-terminated and remains alive for the call.
                unsafe { WaitNamedPipeW(endpoint.as_ptr(), timeout) };
            }
            Err(error) => return Err(Error::Unavailable(error.to_string())),
        }
    };
    verify_pipe_server(&pipe, daemon_executable)?;
    write_frame(&mut pipe, request)?;
    read_frame(&mut pipe).map_err(Error::from)
}

#[cfg(windows)]
fn verify_pipe_server(pipe: &std::fs::File, daemon_executable: &Path) -> Result<()> {
    use std::os::windows::io::AsRawHandle;
    use std::path::PathBuf;
    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
    use windows_sys::Win32::System::Pipes::GetNamedPipeServerProcessId;
    use windows_sys::Win32::System::Threading::{
        OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, QueryFullProcessImageNameW,
    };

    let mut pid = 0_u32;
    // SAFETY: pipe owns a live named-pipe handle and pid is writable.
    if unsafe { GetNamedPipeServerProcessId(pipe.as_raw_handle() as HANDLE, &mut pid) } == 0 {
        return Err(Error::Unavailable(format!(
            "cannot identify named-pipe server: {}",
            std::io::Error::last_os_error()
        )));
    }
    // SAFETY: the requested access is read-only and pid came from the kernel for this pipe.
    let process = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
    if process.is_null() {
        return Err(Error::Unavailable(format!(
            "cannot inspect named-pipe server process {pid}: {}",
            std::io::Error::last_os_error()
        )));
    }
    struct ProcessHandle(HANDLE);
    impl Drop for ProcessHandle {
        fn drop(&mut self) {
            // SAFETY: this guard owns a valid process handle.
            unsafe { CloseHandle(self.0) };
        }
    }
    let process = ProcessHandle(process);
    let mut image = vec![0_u16; 32_768];
    let mut length = image.len() as u32;
    // SAFETY: process is live and the output buffer/length are writable.
    if unsafe { QueryFullProcessImageNameW(process.0, 0, image.as_mut_ptr(), &mut length) } == 0 {
        return Err(Error::Unavailable(format!(
            "cannot inspect named-pipe server image: {}",
            std::io::Error::last_os_error()
        )));
    }
    image.truncate(length as usize);
    let server = std::fs::canonicalize(PathBuf::from(String::from_utf16_lossy(&image)))?;
    let expected = std::fs::canonicalize(daemon_executable)?;
    if server != expected {
        return Err(Error::Unavailable(format!(
            "named-pipe server image mismatch: expected {}, found {}",
            expected.display(),
            server.display()
        )));
    }
    Ok(())
}

#[cfg(not(windows))]
fn transport_request(
    _endpoint: &str,
    _daemon_executable: &Path,
    _request: &Request,
    _deadline: Instant,
) -> Result<Response> {
    Err(Error::UnsupportedPlatform(std::env::consts::OS))
}

#[cfg(windows)]
fn start_daemon(executable: &Path) -> Result<std::process::Child> {
    use std::os::windows::process::CommandExt;
    use std::process::{Command, Stdio};

    const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
    const DETACHED_PROCESS: u32 = 0x0000_0008;
    const CREATE_NO_WINDOW: u32 = 0x0800_0000;
    const CREATE_BREAKAWAY_FROM_JOB: u32 = 0x0100_0000;

    let store_root = default_store_root()?;
    std::fs::create_dir_all(&store_root)?;
    Command::new(executable)
        .args(["daemon", "--background-child"])
        .current_dir(store_root)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .creation_flags(
            CREATE_NEW_PROCESS_GROUP
                | DETACHED_PROCESS
                | CREATE_NO_WINDOW
                | CREATE_BREAKAWAY_FROM_JOB,
        )
        .spawn()
        .map_err(|error| Error::Unavailable(format!("cannot start daemon: {error}")))
}

#[cfg(not(windows))]
fn start_daemon(_executable: &Path) -> Result<std::process::Child> {
    Err(Error::UnsupportedPlatform(std::env::consts::OS))
}

fn default_daemon_executable() -> Result<PathBuf> {
    let current = std::env::current_exe()?;
    let is_stillyard = current
        .file_stem()
        .is_some_and(|name| name.eq_ignore_ascii_case("stillyard"));
    if is_stillyard {
        return Ok(current);
    }
    let filename = if cfg!(windows) {
        "stillyard.exe"
    } else {
        "stillyard"
    };
    current
        .parent()
        .map(|parent| parent.join(filename))
        .ok_or_else(|| Error::Unavailable("cannot resolve sibling stillyard daemon".into()))
}

fn write_initial_result_file(
    path: &Path,
    options: &SubmitOptions,
    payload_hash: &str,
) -> Result<()> {
    if path.exists() {
        return Err(Error::InvalidSpec(format!(
            "result file already exists: {}",
            path.display()
        )));
    }
    write_result_file(path, options.idempotency_key, payload_hash, None)
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ResultFileRecord {
    version: u32,
    idempotency_key: uuid::Uuid,
    payload_hash: String,
    receipt: Option<serde_json::Value>,
}

fn write_result_file(
    path: &Path,
    idempotency_key: uuid::Uuid,
    payload_hash: &str,
    receipt: Option<serde_json::Value>,
) -> Result<()> {
    write_json_atomically(
        path,
        &ResultFileRecord {
            version: 1,
            idempotency_key,
            payload_hash: payload_hash.to_owned(),
            receipt,
        },
    )
}

fn write_json_atomically(path: &Path, value: &impl serde::Serialize) -> Result<()> {
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => std::env::current_dir()?,
    };
    std::fs::create_dir_all(&parent)?;
    crate::filesystem::require_fixed_local_ntfs(&parent)?;
    let temp = parent.join(format!(".stillyard-result-{}.tmp", uuid::Uuid::now_v7()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    drop(file);
    replace_file_atomically(&temp, path)?;
    Ok(())
}

#[cfg(windows)]
fn replace_file_atomically(source: &Path, destination: &Path) -> Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
    };

    let source: Vec<u16> = source
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    let destination: Vec<u16> = destination
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    // SAFETY: both strings are NUL-terminated and remain alive for the call.
    let moved = unsafe {
        MoveFileExW(
            source.as_ptr(),
            destination.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if moved == 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

#[cfg(not(windows))]
fn replace_file_atomically(source: &Path, destination: &Path) -> Result<()> {
    std::fs::rename(source, destination)?;
    Ok(())
}

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
    return Ok(format!(r"\\.\pipe\stillyard-v3-{}", &digest[..16]));
    #[cfg(not(windows))]
    return Ok(default_store_root()?
        .join("stillyard-v3.sock")
        .to_string_lossy()
        .into_owned());
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
