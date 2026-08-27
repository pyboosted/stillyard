use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use directories::ProjectDirs;
use sha2::{Digest, Sha256};

use crate::protocol::{PROTOCOL_VERSION, Request, Response, read_frame, write_frame};
use crate::{
    CancellationToken, DaemonSnapshot, Error, JobId, JobReceipt, JobSnapshot, JobSpec, LogChunk,
    LogStream, RecoveryResult, Result, SubmitOptions,
};

pub(crate) const DEFAULT_PIPE_NAME: &str = r"\\.\pipe\stillyard-v1";

#[derive(Clone, Debug)]
pub struct ClientBuilder {
    endpoint: String,
    auto_start: bool,
    daemon_executable: Option<PathBuf>,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self {
            endpoint: DEFAULT_PIPE_NAME.to_owned(),
            auto_start: true,
            daemon_executable: None,
        }
    }
}

impl ClientBuilder {
    #[must_use]
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = endpoint.into();
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
        let daemon_executable = self
            .daemon_executable
            .map(Ok)
            .unwrap_or_else(default_daemon_executable)?;
        let client = Client {
            endpoint: self.endpoint,
            daemon_executable,
        };
        match client.ping(deadline, cancellation) {
            Ok(()) => Ok(client),
            Err(Error::Unavailable(_)) if self.auto_start => {
                start_daemon(&client.daemon_executable)?;
                loop {
                    check_wait(deadline, cancellation)?;
                    match client.ping(deadline, cancellation) {
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
                        Some(&receipt),
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
        match self.request(Request::Wait { job_id }, deadline, cancellation)? {
            Response::Snapshot(snapshot) => Ok(*snapshot),
            response => response_error(response),
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
            let result = transport_request(&endpoint, &daemon_executable, &request);
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
) -> Result<Response> {
    let mut pipe = OpenOptions::new()
        .read(true)
        .write(true)
        .open(endpoint)
        .map_err(|error| Error::Unavailable(error.to_string()))?;
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
) -> Result<Response> {
    Err(Error::UnsupportedPlatform(std::env::consts::OS))
}

#[cfg(windows)]
fn start_daemon(executable: &Path) -> Result<()> {
    use std::os::windows::process::CommandExt;
    use std::process::Command;

    const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
    const DETACHED_PROCESS: u32 = 0x0000_0008;
    const CREATE_NO_WINDOW: u32 = 0x0800_0000;

    Command::new(executable)
        .args(["daemon", "--background-child"])
        .creation_flags(CREATE_NEW_PROCESS_GROUP | DETACHED_PROCESS | CREATE_NO_WINDOW)
        .spawn()
        .map_err(|error| Error::Unavailable(format!("cannot start daemon: {error}")))?;
    Ok(())
}

#[cfg(not(windows))]
fn start_daemon(_executable: &Path) -> Result<()> {
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
    receipt: Option<JobReceipt>,
}

fn write_result_file(
    path: &Path,
    idempotency_key: uuid::Uuid,
    payload_hash: &str,
    receipt: Option<&JobReceipt>,
) -> Result<()> {
    write_json_atomically(
        path,
        &ResultFileRecord {
            version: 1,
            idempotency_key,
            payload_hash: payload_hash.to_owned(),
            receipt: receipt.cloned(),
        },
    )
}

fn write_json_atomically(path: &Path, value: &impl serde::Serialize) -> Result<()> {
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => std::env::current_dir()?,
    };
    std::fs::create_dir_all(&parent)?;
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
