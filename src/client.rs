use std::collections::BTreeMap;
#[cfg(windows)]
use std::ffi::OsStr;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use directories::ProjectDirs;
use fs2::FileExt;
use sha2::{Digest, Sha256};

use crate::protocol::{PROTOCOL_VERSION, Request, Response, StagedInputRef};
#[cfg(windows)]
use crate::protocol::{read_frame, write_frame};
use crate::{
    BatchReceipt, BatchSpec, CancellationToken, DaemonSnapshot, Error, JobId, JobReceipt,
    JobSnapshot, JobSpec, LogChunk, LogStream, ManagedParent, RecoveryResult, Result,
    SubmissionContext, SubmitOptions,
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
        let managed_endpoint = std::env::var("STILLYARD_ENDPOINT").ok();
        let endpoint = self
            .endpoint
            .or_else(|| managed_endpoint.clone())
            .unwrap_or(default_endpoint()?);
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
            Err(Error::Unavailable(_)) if self.auto_start && managed_endpoint.is_none() => {
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

#[derive(Clone, Copy, Debug)]
struct StreamProgress {
    eof: bool,
    caught_up: bool,
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
        let stdin = inspect_stdin(&spec.stdin)?;
        let normalized = serde_json::to_vec(&(&spec, stdin.as_ref().map(|(input, _)| input)))?;
        let payload_hash = format!("{:x}", Sha256::digest(&normalized));
        let context = self.submission_context(deadline, cancellation)?;
        if let Some(path) = &options.result_file {
            prepare_result_file(path, options, &payload_hash, &self.endpoint, context)?;
        }
        let stdin = match stdin {
            Some((input, path)) => {
                Some(self.upload_stdin(&path, &input, deadline, cancellation)?)
            }
            None => None,
        };
        let response = self.request(
            Request::Submit {
                idempotency_key: options.idempotency_key,
                payload_hash: payload_hash.clone(),
                spec: Box::new(spec),
                stdin,
                expected_store_uuid: Some(context.store_uuid),
                expected_parent: context.parent,
            },
            deadline,
            cancellation,
        )?;
        match response {
            Response::Submitted(receipt) => {
                if receipt.parent != context.parent {
                    return Err(Error::Protocol(
                        "daemon returned a receipt for a different managed parent".into(),
                    ));
                }
                if let Some(path) = &options.result_file {
                    persist_result_receipt(
                        path,
                        &ResultFileRecord {
                            version: 3,
                            idempotency_key: options.idempotency_key,
                            payload_hash: payload_hash.clone(),
                            endpoint: self.endpoint.clone(),
                            store_uuid: context.store_uuid,
                            parent: context.parent,
                            receipt: None,
                        },
                        RecoveryResult::Accepted(receipt.clone()),
                    )?;
                }
                Ok(receipt)
            }
            response => {
                if let Some(path) = &options.result_file {
                    persist_submit_decision(
                        path,
                        options.idempotency_key,
                        &payload_hash,
                        &self.endpoint,
                        context,
                        &response,
                    )?;
                }
                response_error(response)
            }
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
        let mut inspected = Vec::new();
        let mut input_refs = BTreeMap::new();
        for member in &spec.jobs {
            if let Some((input, path)) = inspect_stdin(&member.spec.stdin)? {
                input_refs.insert(member.name.clone(), input.clone());
                inspected.push((member.name.clone(), input, path));
            }
        }
        let normalized = serde_json::to_vec(&(&spec, &input_refs))?;
        let payload_hash = format!("{:x}", Sha256::digest(&normalized));
        let context = self.submission_context(deadline, cancellation)?;
        if let Some(path) = &options.result_file {
            prepare_result_file(path, options, &payload_hash, &self.endpoint, context)?;
        }
        let mut stdins = BTreeMap::new();
        for (name, input, path) in inspected {
            stdins.insert(
                name,
                self.upload_stdin(&path, &input, deadline, cancellation)?,
            );
        }
        let response = self.request(
            Request::SubmitBatch {
                idempotency_key: options.idempotency_key,
                payload_hash: payload_hash.clone(),
                spec: Box::new(spec),
                stdins,
                expected_store_uuid: Some(context.store_uuid),
                expected_parent: context.parent,
            },
            deadline,
            cancellation,
        )?;
        match response {
            Response::BatchSubmitted(receipt) => {
                if receipt
                    .jobs
                    .iter()
                    .any(|member| member.receipt.parent != context.parent)
                {
                    return Err(Error::Protocol(
                        "daemon returned a Batch receipt for a different managed parent".into(),
                    ));
                }
                if let Some(path) = &options.result_file {
                    persist_result_receipt(
                        path,
                        &ResultFileRecord {
                            version: 3,
                            idempotency_key: options.idempotency_key,
                            payload_hash: payload_hash.clone(),
                            endpoint: self.endpoint.clone(),
                            store_uuid: context.store_uuid,
                            parent: context.parent,
                            receipt: None,
                        },
                        RecoveryResult::AcceptedBatch(receipt.clone()),
                    )?;
                }
                Ok(receipt)
            }
            response => {
                if let Some(path) = &options.result_file {
                    persist_submit_decision(
                        path,
                        options.idempotency_key,
                        &payload_hash,
                        &self.endpoint,
                        context,
                        &response,
                    )?;
                }
                response_error(response)
            }
        }
    }

    fn upload_stdin(
        &self,
        path: &Path,
        expected: &StagedInputRef,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<StagedInputRef> {
        const CHUNK_BYTES: usize = 256 * 1024;

        let upload_id = uuid::Uuid::now_v7();
        let mut offset = match self.request(
            Request::StageBegin {
                upload_id,
                expected_sha256: expected.sha256.clone(),
                expected_length: expected.length,
            },
            deadline,
            cancellation,
        )? {
            Response::StageReady { next_offset } => next_offset,
            response => return response_error(response),
        };
        let mut input = std::fs::File::open(path)?;
        input.seek(SeekFrom::Start(offset))?;
        let mut buffer = vec![0_u8; CHUNK_BYTES];
        while offset < expected.length {
            check_wait(deadline, cancellation)?;
            let remaining = usize::try_from(expected.length - offset)
                .unwrap_or(usize::MAX)
                .min(buffer.len());
            let read = input.read(&mut buffer[..remaining])?;
            if read == 0 {
                return Err(Error::InvalidSpec(format!(
                    "stdin file changed or was truncated during upload: {}",
                    path.display()
                )));
            }
            offset = match self.request(
                Request::StageChunk {
                    upload_id,
                    offset,
                    bytes: buffer[..read].to_vec(),
                },
                deadline,
                cancellation,
            )? {
                Response::StageReady { next_offset } => next_offset,
                response => return response_error(response),
            };
        }
        if input.read(&mut [0_u8; 1])? != 0 {
            return Err(Error::InvalidSpec(format!(
                "stdin file grew during upload: {}",
                path.display()
            )));
        }
        match self.request(Request::StageCommit { upload_id }, deadline, cancellation)? {
            Response::StageCommitted { input } if input == *expected => Ok(input),
            Response::StageCommitted { .. } => Err(Error::Protocol(
                "daemon committed different stdin metadata".into(),
            )),
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
        let context = self.submission_context(deadline, cancellation)?;
        self.recover_submission_with_store(
            idempotency_key,
            payload_hash.into(),
            context.parent,
            deadline,
            cancellation,
        )
        .map(|(_, recovery)| recovery)
    }

    fn recover_submission_with_store(
        &self,
        idempotency_key: uuid::Uuid,
        payload_hash: String,
        expected_parent: Option<ManagedParent>,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<(uuid::Uuid, RecoveryResult)> {
        match self.request(
            Request::Recover {
                idempotency_key,
                payload_hash,
                expected_parent,
            },
            deadline,
            cancellation,
        )? {
            Response::Recovered {
                store_uuid,
                recovery,
            } => Ok((store_uuid, recovery)),
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
        if record.version != 3 {
            return Err(Error::Protocol(format!(
                "unsupported result-file version {}",
                record.version
            )));
        }
        if record.endpoint != self.endpoint {
            return Err(Error::Protocol(format!(
                "result file belongs to endpoint {:?}, connected to {:?}",
                record.endpoint, self.endpoint
            )));
        }
        let context = self.submission_context(deadline, cancellation)?;
        if context.store_uuid != record.store_uuid {
            return Err(Error::Protocol(format!(
                "result file belongs to store {}, connected to {}",
                record.store_uuid, context.store_uuid
            )));
        }
        if context.parent != record.parent {
            return Err(Error::Protocol(
                "result file managed parent does not match the current authenticated caller".into(),
            ));
        }
        let (store_uuid, recovery) = self.recover_submission_with_store(
            record.idempotency_key,
            record.payload_hash.clone(),
            record.parent,
            deadline,
            cancellation,
        )?;
        if store_uuid != record.store_uuid {
            return Err(Error::Protocol(format!(
                "result file belongs to store {}, connected to {}",
                record.store_uuid, store_uuid
            )));
        }
        persist_recovery(path, &record, &recovery)?;
        Ok(recovery)
    }

    /// Returns the store and server-authenticated managed parent for this client process.
    pub fn submission_context(
        &self,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<SubmissionContext> {
        let claimed_parent = claimed_managed_parent()?;
        match self.request(
            Request::SubmissionContext { claimed_parent },
            deadline,
            cancellation,
        )? {
            Response::SubmissionContext(context) => Ok(context),
            response => response_error(response),
        }
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

    #[allow(clippy::too_many_arguments)]
    pub fn wait_with_passthrough(
        &self,
        job_id: JobId,
        stdout_offset: &mut u64,
        stderr_offset: &mut u64,
        stdout: &mut impl Write,
        stderr: &mut impl Write,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<JobSnapshot> {
        loop {
            let snapshot = match self.request(
                Request::Wait {
                    job_id,
                    max_wait_millis: 250,
                },
                deadline,
                cancellation,
            )? {
                Response::Snapshot(snapshot) => *snapshot,
                response => return response_error(response),
            };
            let stdout_progress = self.passthrough_stream(
                job_id,
                LogStream::Stdout,
                stdout_offset,
                stdout,
                deadline,
                cancellation,
            )?;
            let stderr_progress = self.passthrough_stream(
                job_id,
                LogStream::Stderr,
                stderr_offset,
                stderr,
                deadline,
                cancellation,
            )?;
            if passthrough_is_complete(&snapshot, stdout_progress, stderr_progress) {
                stdout.flush()?;
                stderr.flush()?;
                return Ok(snapshot);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn passthrough_stream(
        &self,
        job_id: JobId,
        stream: LogStream,
        offset: &mut u64,
        output: &mut impl Write,
        deadline: Instant,
        cancellation: Option<&CancellationToken>,
    ) -> Result<StreamProgress> {
        let chunk = self.logs(job_id, stream, *offset, 1024 * 1024, deadline, cancellation)?;
        if let Some(gap) = chunk.gap {
            return Err(Error::Protocol(format!(
                "canonical {stream:?} log gap at offset {}: {gap}",
                *offset
            )));
        }
        let caught_up = chunk.bytes.is_empty() || chunk.eof;
        if !chunk.bytes.is_empty() {
            output.write_all(&chunk.bytes)?;
            output.flush()?;
            *offset = chunk.next_offset;
        }
        Ok(StreamProgress {
            eof: chunk.eof,
            caught_up,
        })
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

fn passthrough_is_complete(
    snapshot: &JobSnapshot,
    stdout: StreamProgress,
    stderr: StreamProgress,
) -> bool {
    passthrough_state_is_complete(snapshot.is_final(), snapshot.outcome, stdout, stderr)
}

fn passthrough_state_is_complete(
    is_final: bool,
    outcome: Option<crate::JobOutcome>,
    stdout: StreamProgress,
    stderr: StreamProgress,
) -> bool {
    if !is_final {
        return false;
    }
    if stdout.eof && stderr.eof {
        return true;
    }
    outcome == Some(crate::JobOutcome::Interrupted) && stdout.caught_up && stderr.caught_up
}

fn inspect_stdin(stdin: &crate::StdinSpec) -> Result<Option<(StagedInputRef, PathBuf)>> {
    const MAX_STDIN_BYTES: u64 = 64 * 1024 * 1024;

    let crate::StdinSpec::File { path } = stdin else {
        return Ok(None);
    };
    let mut file = std::fs::File::open(path)?;
    let length = file.metadata()?.len();
    if length > MAX_STDIN_BYTES {
        return Err(Error::InvalidSpec(format!(
            "stdin file exceeds the {MAX_STDIN_BYTES}-byte alpha limit"
        )));
    }
    let mut hash = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hash.update(&buffer[..read]);
    }
    Ok(Some((
        StagedInputRef {
            sha256: format!("{:x}", hash.finalize()),
            length,
        },
        path.clone(),
    )))
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

fn claimed_managed_parent() -> Result<Option<ManagedParent>> {
    let job = std::env::var("STILLYARD_JOB_ID").ok();
    let attempt = std::env::var("STILLYARD_ATTEMPT").ok();
    let invocation = std::env::var("STILLYARD_INVOCATION_ID").ok();
    if job.is_none() && attempt.is_none() && invocation.is_none() {
        return Ok(None);
    }
    let (Some(job), Some(attempt), Some(invocation)) = (job, attempt, invocation) else {
        return Err(Error::Protocol(
            "managed environment has incomplete Job/Attempt/Invocation coordinates".into(),
        ));
    };
    let parent = ManagedParent {
        job_id: job
            .parse()
            .map_err(|_| Error::Protocol("invalid STILLYARD_JOB_ID".into()))?,
        attempt_id: attempt
            .parse()
            .map_err(|_| Error::Protocol("invalid STILLYARD_ATTEMPT".into()))?,
        invocation_id: invocation
            .parse()
            .map_err(|_| Error::Protocol("invalid STILLYARD_INVOCATION_ID".into()))?,
    };
    if parent.job_id.store_uuid() != parent.attempt_id.store_uuid()
        || parent.job_id.store_uuid() != parent.invocation_id.store_uuid()
    {
        return Err(Error::Protocol(
            "managed environment coordinates belong to different stores".into(),
        ));
    }
    Ok(Some(parent))
}

fn prepare_result_file(
    path: &Path,
    options: &SubmitOptions,
    payload_hash: &str,
    endpoint: &str,
    context: SubmissionContext,
) -> Result<()> {
    let record = ResultFileRecord {
        version: 3,
        idempotency_key: options.idempotency_key,
        payload_hash: payload_hash.to_owned(),
        endpoint: endpoint.to_owned(),
        store_uuid: context.store_uuid,
        parent: context.parent,
        receipt: None,
    };
    with_result_file_lock(path, || match write_json_new_atomically(path, &record) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let existing: ResultFileRecord = serde_json::from_reader(std::fs::File::open(path)?)?;
            validate_managed_resubmit(&existing, &record).map_err(|detail| {
                Error::InvalidSpec(format!(
                    "result file {} cannot authorize managed resubmission: {detail}",
                    path.display()
                ))
            })
        }
        Err(error) => Err(Error::Io(error)),
    })
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ResultFileRecord {
    version: u32,
    idempotency_key: uuid::Uuid,
    payload_hash: String,
    endpoint: String,
    store_uuid: uuid::Uuid,
    parent: Option<ManagedParent>,
    receipt: Option<RecoveryResult>,
}

fn validate_managed_resubmit(
    existing: &ResultFileRecord,
    proposed: &ResultFileRecord,
) -> std::result::Result<(), &'static str> {
    if existing.version != 3 {
        return Err("unsupported result-file version");
    }
    if existing.idempotency_key != proposed.idempotency_key
        || existing.payload_hash != proposed.payload_hash
        || existing.endpoint != proposed.endpoint
        || existing.store_uuid != proposed.store_uuid
        || existing.parent != proposed.parent
    {
        return Err("identity, key, or normalized payload differs");
    }
    if proposed.parent.is_none() {
        return Err("an unmanaged submission can never reuse a result file");
    }
    if existing.receipt != Some(RecoveryResult::NotReceived) {
        return Err("the latest durable recovery result is not not_received");
    }
    Ok(())
}

fn persist_recovery(
    path: &Path,
    record: &ResultFileRecord,
    recovery: &RecoveryResult,
) -> Result<()> {
    if matches!(recovery, RecoveryResult::Unknown) {
        return Ok(());
    }
    persist_result_receipt(path, record, recovery.clone())
}

fn persist_submit_decision(
    path: &Path,
    idempotency_key: uuid::Uuid,
    payload_hash: &str,
    endpoint: &str,
    context: SubmissionContext,
    response: &Response,
) -> Result<()> {
    let decision = match response {
        Response::Error { code, .. } if code == "idempotency_conflict" => {
            Some(RecoveryResult::Conflict)
        }
        _ => None,
    };
    if let Some(decision) = decision {
        persist_result_receipt(
            path,
            &ResultFileRecord {
                version: 3,
                idempotency_key,
                payload_hash: payload_hash.to_owned(),
                endpoint: endpoint.to_owned(),
                store_uuid: context.store_uuid,
                parent: context.parent,
                receipt: None,
            },
            decision,
        )?;
    }
    Ok(())
}

fn persist_result_receipt(
    path: &Path,
    expected: &ResultFileRecord,
    receipt: RecoveryResult,
) -> Result<()> {
    with_result_file_lock(path, || {
        let mut current: ResultFileRecord = serde_json::from_reader(std::fs::File::open(path)?)?;
        validate_result_file_identity(&current, expected).map_err(|detail| {
            Error::Protocol(format!(
                "result file {} changed identity while updating it: {detail}",
                path.display()
            ))
        })?;
        if current
            .receipt
            .as_ref()
            .is_some_and(|existing| same_terminal_decision(existing, &receipt))
        {
            // Accepted receipts contain live queue/state estimates. The durable decision is the
            // Submission/Job identity, so a refresh of those volatile fields is a no-op rather
            // than a competing terminal decision or a reason to churn the receipt file.
            return Ok(());
        }
        let may_advance = match current.receipt.as_ref() {
            None => true,
            Some(RecoveryResult::NotReceived) => !matches!(receipt, RecoveryResult::Unknown),
            Some(RecoveryResult::Received { .. }) => matches!(
                receipt,
                RecoveryResult::Received { .. }
                    | RecoveryResult::Accepted(_)
                    | RecoveryResult::AcceptedBatch(_)
                    | RecoveryResult::Rejected { .. }
                    | RecoveryResult::Conflict
            ),
            Some(existing) => existing == &receipt,
        };
        if !may_advance {
            return Err(Error::Protocol(
                "result-file update would regress or replace its durable decision".into(),
            ));
        }
        if current.receipt.as_ref() != Some(&receipt) {
            current.receipt = Some(receipt);
            write_json_atomically(path, &current)?;
        }
        Ok(())
    })
}

fn same_terminal_decision(existing: &RecoveryResult, proposed: &RecoveryResult) -> bool {
    match (existing, proposed) {
        (RecoveryResult::Accepted(existing), RecoveryResult::Accepted(proposed)) => {
            existing.submission_id == proposed.submission_id
                && existing.job_id == proposed.job_id
                && existing.parent == proposed.parent
        }
        (RecoveryResult::AcceptedBatch(existing), RecoveryResult::AcceptedBatch(proposed)) => {
            existing.submission_id == proposed.submission_id
                && existing.batch_id == proposed.batch_id
                && existing.jobs.len() == proposed.jobs.len()
                && existing
                    .jobs
                    .iter()
                    .zip(&proposed.jobs)
                    .all(|(existing, proposed)| {
                        existing.name == proposed.name
                            && existing.receipt.submission_id == proposed.receipt.submission_id
                            && existing.receipt.job_id == proposed.receipt.job_id
                            && existing.receipt.parent == proposed.receipt.parent
                    })
        }
        (RecoveryResult::Rejected { .. }, RecoveryResult::Rejected { .. })
        | (RecoveryResult::Conflict, RecoveryResult::Conflict) => existing == proposed,
        _ => false,
    }
}

fn validate_result_file_identity(
    current: &ResultFileRecord,
    expected: &ResultFileRecord,
) -> std::result::Result<(), &'static str> {
    if current.version != 3 || expected.version != 3 {
        return Err("unsupported result-file version");
    }
    if current.idempotency_key != expected.idempotency_key
        || current.payload_hash != expected.payload_hash
        || current.endpoint != expected.endpoint
        || current.store_uuid != expected.store_uuid
        || current.parent != expected.parent
    {
        return Err("identity, key, or normalized payload differs");
    }
    Ok(())
}

fn with_result_file_lock<T>(path: &Path, action: impl FnOnce() -> Result<T>) -> Result<T> {
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => std::env::current_dir()?,
    };
    std::fs::create_dir_all(&parent)?;
    crate::filesystem::require_fixed_local_ntfs(&parent)?;
    let mut lock_name = path.as_os_str().to_os_string();
    lock_name.push(".lock");
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(PathBuf::from(lock_name))?;
    lock.lock_exclusive()?;
    let result = action();
    let unlock = FileExt::unlock(&lock).map_err(Error::Io);
    match (result, unlock) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Ok(value), Ok(())) => Ok(value),
    }
}

#[cfg(test)]
fn write_result_file(
    path: &Path,
    idempotency_key: uuid::Uuid,
    payload_hash: &str,
    endpoint: &str,
    context: SubmissionContext,
    receipt: Option<RecoveryResult>,
) -> Result<()> {
    write_json_atomically(
        path,
        &ResultFileRecord {
            version: 3,
            idempotency_key,
            payload_hash: payload_hash.to_owned(),
            endpoint: endpoint.to_owned(),
            store_uuid: context.store_uuid,
            parent: context.parent,
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
    let result = replace_file_atomically(&temp, path);
    if result.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    result
}

fn write_json_new_atomically(path: &Path, value: &impl serde::Serialize) -> std::io::Result<()> {
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => std::env::current_dir()?,
    };
    std::fs::create_dir_all(&parent)?;
    crate::filesystem::require_fixed_local_ntfs(&parent).map_err(std::io::Error::other)?;
    let temp = parent.join(format!(".stillyard-result-{}.tmp", uuid::Uuid::now_v7()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        serde_json::to_writer_pretty(&mut file, value).map_err(std::io::Error::other)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        drop(file);
        // Creating a hard link is an atomic create-if-absent operation on the required local
        // NTFS volume. Removing the temporary name cannot invalidate the published receipt.
        std::fs::hard_link(&temp, path)?;
        let _ = std::fs::remove_file(&temp);
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    result
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
    return Ok(format!(r"\\.\pipe\stillyard-v5-{}", &digest[..16]));
    #[cfg(not(windows))]
    return Ok(default_store_root()?
        .join("stillyard-v5.sock")
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

#[cfg(test)]
mod tests {
    use super::*;

    fn managed_parent(store: uuid::Uuid) -> ManagedParent {
        ManagedParent {
            job_id: crate::JobId::from_parts(store, uuid::Uuid::now_v7()),
            attempt_id: crate::AttemptId::from_parts(store, uuid::Uuid::now_v7()),
            invocation_id: crate::InvocationId::from_parts(store, uuid::Uuid::now_v7()),
        }
    }

    #[test]
    fn result_file_fresh_create_is_atomic_and_never_overwrites() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("operation.result.json");
        let first = SubmitOptions::new(uuid::Uuid::now_v7());
        let store_uuid = uuid::Uuid::now_v7();
        let context = SubmissionContext {
            store_uuid,
            parent: None,
        };
        prepare_result_file(&path, &first, "first-hash", "pipe-a", context).unwrap();
        let before = std::fs::read(&path).unwrap();

        let second = SubmitOptions::new(uuid::Uuid::now_v7());
        assert!(matches!(
            prepare_result_file(
                &path,
                &second,
                "second-hash",
                "pipe-b",
                SubmissionContext {
                    store_uuid: uuid::Uuid::now_v7(),
                    parent: None,
                }
            ),
            Err(Error::InvalidSpec(_))
        ));
        assert_eq!(std::fs::read(&path).unwrap(), before);

        write_result_file(
            &path,
            first.idempotency_key,
            "first-hash",
            "pipe-a",
            context,
            Some(RecoveryResult::NotReceived),
        )
        .unwrap();
        let record: ResultFileRecord =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(record.idempotency_key, first.idempotency_key);
        assert_eq!(record.payload_hash, "first-hash");
        assert_eq!(record.endpoint, "pipe-a");
        assert_eq!(record.store_uuid, store_uuid);
        assert_eq!(record.receipt, Some(RecoveryResult::NotReceived));
    }

    #[test]
    fn unknown_recovery_preserves_the_last_durable_receipt() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("operation.result.json");
        let record = ResultFileRecord {
            version: 3,
            idempotency_key: uuid::Uuid::now_v7(),
            payload_hash: "payload".into(),
            endpoint: "pipe-a".into(),
            store_uuid: uuid::Uuid::now_v7(),
            parent: None,
            receipt: Some(RecoveryResult::Conflict),
        };
        write_json_atomically(&path, &record).unwrap();
        let before = std::fs::read(&path).unwrap();
        persist_recovery(&path, &record, &RecoveryResult::Unknown).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), before);
        assert!(matches!(
            persist_recovery(&path, &record, &RecoveryResult::NotReceived),
            Err(Error::Protocol(_))
        ));
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }

    #[test]
    fn stale_recovery_cannot_overwrite_a_concurrent_accepted_receipt() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("operation.result.json");
        let store_uuid = uuid::Uuid::now_v7();
        let submission_id = crate::SubmissionId::from_parts(store_uuid, uuid::Uuid::now_v7());
        let stale = ResultFileRecord {
            version: 3,
            idempotency_key: uuid::Uuid::now_v7(),
            payload_hash: "payload".into(),
            endpoint: "pipe-a".into(),
            store_uuid,
            parent: None,
            receipt: Some(RecoveryResult::NotReceived),
        };
        write_json_atomically(&path, &stale).unwrap();
        let accepted = RecoveryResult::Accepted(JobReceipt {
            submission_id,
            job_id: crate::JobId::from_parts(store_uuid, uuid::Uuid::now_v7()),
            submission_state: crate::SubmissionState::Accepted,
            job_state: crate::JobState::Pending,
            blockers: Vec::new(),
            queue_rank: Some(1),
            estimate: crate::Estimate::unknown("test"),
            parent: None,
        });
        persist_result_receipt(&path, &stale, accepted.clone()).unwrap();

        assert!(matches!(
            persist_recovery(&path, &stale, &RecoveryResult::Received { submission_id }),
            Err(Error::Protocol(_))
        ));
        let durable: ResultFileRecord =
            serde_json::from_reader(std::fs::File::open(&path).unwrap()).unwrap();
        assert_eq!(durable.receipt, Some(accepted));
    }

    #[test]
    fn accepted_refresh_uses_stable_identity_and_keeps_the_durable_receipt() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("operation.result.json");
        let store_uuid = uuid::Uuid::now_v7();
        let submission_id = crate::SubmissionId::from_parts(store_uuid, uuid::Uuid::now_v7());
        let job_id = crate::JobId::from_parts(store_uuid, uuid::Uuid::now_v7());
        let record = ResultFileRecord {
            version: 3,
            idempotency_key: uuid::Uuid::now_v7(),
            payload_hash: "payload".into(),
            endpoint: "pipe-a".into(),
            store_uuid,
            parent: None,
            receipt: Some(RecoveryResult::Accepted(JobReceipt {
                submission_id,
                job_id,
                submission_state: crate::SubmissionState::Accepted,
                job_state: crate::JobState::Pending,
                blockers: Vec::new(),
                queue_rank: Some(1),
                estimate: crate::Estimate::unknown("pending"),
                parent: None,
            })),
        };
        write_json_atomically(&path, &record).unwrap();
        let before = std::fs::read(&path).unwrap();
        let refreshed = RecoveryResult::Accepted(JobReceipt {
            submission_id,
            job_id,
            submission_state: crate::SubmissionState::Accepted,
            job_state: crate::JobState::Final,
            blockers: Vec::new(),
            queue_rank: None,
            estimate: crate::Estimate::unknown("final"),
            parent: None,
        });
        persist_result_receipt(&path, &record, refreshed).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), before);

        let foreign = RecoveryResult::Accepted(JobReceipt {
            submission_id,
            job_id: crate::JobId::from_parts(store_uuid, uuid::Uuid::now_v7()),
            submission_state: crate::SubmissionState::Accepted,
            job_state: crate::JobState::Final,
            blockers: Vec::new(),
            queue_rank: None,
            estimate: crate::Estimate::unknown("foreign"),
            parent: None,
        });
        assert!(matches!(
            persist_result_receipt(&path, &record, foreign),
            Err(Error::Protocol(_))
        ));
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }

    #[test]
    fn accepted_batch_refresh_pins_the_complete_member_identity() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("batch.result.json");
        let store_uuid = uuid::Uuid::now_v7();
        let submission_id = crate::SubmissionId::from_parts(store_uuid, uuid::Uuid::now_v7());
        let batch_id = crate::BatchId::from_parts(store_uuid, uuid::Uuid::now_v7());
        let receipt = |job_id, rank| JobReceipt {
            submission_id,
            job_id,
            submission_state: crate::SubmissionState::Accepted,
            job_state: crate::JobState::Pending,
            blockers: Vec::new(),
            queue_rank: Some(rank),
            estimate: crate::Estimate::unknown("pending"),
            parent: None,
        };
        let durable_batch = BatchReceipt {
            submission_id,
            batch_id,
            submission_state: crate::SubmissionState::Accepted,
            jobs: vec![
                crate::BatchJobReceipt {
                    name: "first".into(),
                    receipt: receipt(
                        crate::JobId::from_parts(store_uuid, uuid::Uuid::now_v7()),
                        1,
                    ),
                },
                crate::BatchJobReceipt {
                    name: "second".into(),
                    receipt: receipt(
                        crate::JobId::from_parts(store_uuid, uuid::Uuid::now_v7()),
                        2,
                    ),
                },
            ],
        };
        let record = ResultFileRecord {
            version: 3,
            idempotency_key: uuid::Uuid::now_v7(),
            payload_hash: "batch-payload".into(),
            endpoint: "pipe-a".into(),
            store_uuid,
            parent: None,
            receipt: Some(RecoveryResult::AcceptedBatch(durable_batch.clone())),
        };
        write_json_atomically(&path, &record).unwrap();
        let before = std::fs::read(&path).unwrap();

        let mut refreshed = durable_batch.clone();
        for member in &mut refreshed.jobs {
            member.receipt.job_state = crate::JobState::Final;
            member.receipt.queue_rank = None;
            member.receipt.estimate = crate::Estimate::unknown("final");
        }
        persist_result_receipt(&path, &record, RecoveryResult::AcceptedBatch(refreshed)).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), before);

        let mut foreign_batch = durable_batch.clone();
        foreign_batch.batch_id = crate::BatchId::from_parts(store_uuid, uuid::Uuid::now_v7());
        let mut truncated = durable_batch.clone();
        truncated.jobs.pop();
        let mut foreign_member = durable_batch;
        foreign_member.jobs[1].receipt.job_id =
            crate::JobId::from_parts(store_uuid, uuid::Uuid::now_v7());
        for mutant in [foreign_batch, truncated, foreign_member] {
            assert!(matches!(
                persist_result_receipt(&path, &record, RecoveryResult::AcceptedBatch(mutant)),
                Err(Error::Protocol(_))
            ));
            assert_eq!(std::fs::read(&path).unwrap(), before);
        }
    }

    #[test]
    fn only_exact_managed_not_received_receipt_authorizes_result_file_reuse() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("managed.result.json");
        let store_uuid = uuid::Uuid::now_v7();
        let parent = managed_parent(store_uuid);
        let options = SubmitOptions::new(uuid::Uuid::now_v7());
        let record = ResultFileRecord {
            version: 3,
            idempotency_key: options.idempotency_key,
            payload_hash: "payload".into(),
            endpoint: "pipe-a".into(),
            store_uuid,
            parent: Some(parent),
            receipt: Some(RecoveryResult::NotReceived),
        };
        write_json_atomically(&path, &record).unwrap();
        let before = std::fs::read(&path).unwrap();
        prepare_result_file(
            &path,
            &options,
            "payload",
            "pipe-a",
            SubmissionContext {
                store_uuid,
                parent: Some(parent),
            },
        )
        .unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), before);

        let mut unknown = record;
        unknown.receipt = Some(RecoveryResult::Unknown);
        write_json_atomically(&path, &unknown).unwrap();
        assert!(matches!(
            prepare_result_file(
                &path,
                &options,
                "payload",
                "pipe-a",
                SubmissionContext {
                    store_uuid,
                    parent: Some(parent),
                },
            ),
            Err(Error::InvalidSpec(_))
        ));

        unknown.receipt = Some(RecoveryResult::NotReceived);
        write_json_atomically(&path, &unknown).unwrap();
        assert!(matches!(
            prepare_result_file(
                &path,
                &options,
                "payload",
                "pipe-a",
                SubmissionContext {
                    store_uuid,
                    parent: Some(managed_parent(store_uuid)),
                },
            ),
            Err(Error::InvalidSpec(_))
        ));

        persist_submit_decision(
            &path,
            options.idempotency_key,
            "payload",
            "pipe-a",
            SubmissionContext {
                store_uuid,
                parent: Some(parent),
            },
            &Response::Error {
                code: "idempotency_conflict".into(),
                message: "conflict".into(),
            },
        )
        .unwrap();
        let conflicted: ResultFileRecord =
            serde_json::from_reader(std::fs::File::open(&path).unwrap()).unwrap();
        assert_eq!(conflicted.receipt, Some(RecoveryResult::Conflict));
    }

    #[test]
    fn every_result_file_identity_discriminant_blocks_replay() {
        let store_uuid = uuid::Uuid::now_v7();
        let parent = managed_parent(store_uuid);
        let baseline = ResultFileRecord {
            version: 3,
            idempotency_key: uuid::Uuid::now_v7(),
            payload_hash: "payload".into(),
            endpoint: "pipe-a".into(),
            store_uuid,
            parent: Some(parent),
            receipt: Some(RecoveryResult::NotReceived),
        };
        for changed in [
            ResultFileRecord {
                idempotency_key: uuid::Uuid::now_v7(),
                ..baseline.clone()
            },
            ResultFileRecord {
                payload_hash: "changed".into(),
                ..baseline.clone()
            },
            ResultFileRecord {
                endpoint: "pipe-b".into(),
                ..baseline.clone()
            },
            ResultFileRecord {
                store_uuid: uuid::Uuid::now_v7(),
                ..baseline.clone()
            },
        ] {
            assert!(validate_managed_resubmit(&baseline, &changed).is_err());
        }
    }

    #[test]
    fn recovery_rejects_a_foreign_endpoint_without_touching_the_receipt() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("operation.result.json");
        let record = ResultFileRecord {
            version: 3,
            idempotency_key: uuid::Uuid::now_v7(),
            payload_hash: "payload".into(),
            endpoint: "pipe-a".into(),
            store_uuid: uuid::Uuid::now_v7(),
            parent: None,
            receipt: Some(RecoveryResult::Conflict),
        };
        write_json_atomically(&path, &record).unwrap();
        let before = std::fs::read(&path).unwrap();
        let client = Client {
            endpoint: "pipe-b".into(),
            daemon_executable: temp.path().join("unused.exe"),
        };
        assert!(matches!(
            client.recover_result_file(&path, Instant::now() + Duration::from_secs(1), None,),
            Err(Error::Protocol(_))
        ));
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }

    #[test]
    fn recover_missing_result_file_does_not_create_it() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("missing.result.json");
        let client = Client {
            endpoint: "unused".into(),
            daemon_executable: temp.path().join("unused.exe"),
        };
        assert!(
            client
                .recover_result_file(&path, Instant::now() + Duration::from_secs(1), None)
                .is_err()
        );
        assert!(!path.exists());
    }

    #[test]
    fn interrupted_passthrough_stops_at_a_quiescent_unclaimed_prefix() {
        let caught_up = StreamProgress {
            eof: false,
            caught_up: true,
        };
        assert!(passthrough_state_is_complete(
            true,
            Some(crate::JobOutcome::Interrupted),
            caught_up,
            caught_up,
        ));
        assert!(!passthrough_state_is_complete(
            true,
            Some(crate::JobOutcome::Failed),
            caught_up,
            caught_up,
        ));
        assert!(!passthrough_state_is_complete(
            false,
            Some(crate::JobOutcome::Interrupted),
            caught_up,
            caught_up,
        ));
    }
}
