use std::sync::{Arc, Mutex};

use crate::store::{PreparedJob, Store};

#[cfg(windows)]
static LIVE_CONTAINMENTS: std::sync::OnceLock<
    Mutex<std::collections::HashMap<crate::InvocationId, usize>>,
> = std::sync::OnceLock::new();

#[cfg(windows)]
struct ContainmentRegistration(crate::InvocationId);

#[cfg(windows)]
impl Drop for ContainmentRegistration {
    fn drop(&mut self) {
        if let Ok(mut registry) = LIVE_CONTAINMENTS.get_or_init(Default::default).lock() {
            registry.remove(&self.0);
        }
    }
}

#[cfg(windows)]
fn register_containment(
    invocation_id: crate::InvocationId,
    handle: windows_sys::Win32::Foundation::HANDLE,
) -> std::io::Result<ContainmentRegistration> {
    let mut registry = LIVE_CONTAINMENTS
        .get_or_init(Default::default)
        .lock()
        .map_err(|_| std::io::Error::other("containment registry mutex poisoned"))?;
    if registry.contains_key(&invocation_id) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "Invocation containment is already registered",
        ));
    }
    registry.insert(invocation_id, handle as usize);
    Ok(ContainmentRegistration(invocation_id))
}

#[cfg(windows)]
pub(crate) fn process_in_containment(
    invocation_id: crate::InvocationId,
    process_handle: usize,
) -> std::io::Result<Option<bool>> {
    use windows_sys::Win32::System::JobObjects::IsProcessInJob;

    let registry = LIVE_CONTAINMENTS
        .get_or_init(Default::default)
        .lock()
        .map_err(|_| std::io::Error::other("containment registry mutex poisoned"))?;
    let Some(job_handle) = registry.get(&invocation_id).copied() else {
        return Ok(None);
    };
    let mut member = 0;
    // SAFETY: the registry lock prevents the runner from unregistering and closing the Job
    // Object while both handles are inspected; process_handle is owned by the pipe worker.
    if unsafe {
        IsProcessInJob(
            process_handle as windows_sys::Win32::Foundation::HANDLE,
            job_handle as windows_sys::Win32::Foundation::HANDLE,
            &mut member,
        )
    } == 0
    {
        return Err(std::io::Error::last_os_error());
    }
    Ok(Some(member != 0))
}

pub(crate) fn run(job: PreparedJob, store: Arc<Mutex<Store>>) {
    #[cfg(windows)]
    windows::run(&job, &store);

    #[cfg(not(windows))]
    let _ = (job, store);
}

#[cfg(windows)]
mod windows {
    use std::collections::BTreeMap;
    use std::ffi::{OsStr, c_void};
    use std::fs::{File, OpenOptions};
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::mem::{size_of, zeroed};
    use std::os::windows::ffi::OsStrExt;
    use std::os::windows::fs::OpenOptionsExt;
    use std::os::windows::io::{AsRawHandle, FromRawHandle, RawHandle};
    use std::path::{Path, PathBuf};
    use std::ptr::{null, null_mut};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use sha2::{Digest, Sha256};
    use windows_sys::Win32::Foundation::{
        CloseHandle, GENERIC_READ, HANDLE, HANDLE_FLAG_INHERIT, INVALID_HANDLE_VALUE,
        SetHandleInformation, WAIT_OBJECT_0, WAIT_TIMEOUT,
    };
    use windows_sys::Win32::Security::SECURITY_ATTRIBUTES;
    use windows_sys::Win32::Storage::FileSystem::{
        CreateFileW, FILE_ATTRIBUTE_NORMAL, FILE_ATTRIBUTE_REPARSE_POINT, FILE_SHARE_READ,
        FILE_SHARE_WRITE, GetFileAttributesW, INVALID_FILE_ATTRIBUTES, OPEN_EXISTING,
    };
    use windows_sys::Win32::System::JobObjects::{
        CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
        JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
        JobObjectBasicAccountingInformation, JobObjectExtendedLimitInformation,
        QueryInformationJobObject, SetInformationJobObject, TerminateJobObject,
    };
    use windows_sys::Win32::System::Pipes::CreatePipe;
    use windows_sys::Win32::System::Threading::{
        CREATE_NO_WINDOW, CREATE_SUSPENDED, CREATE_UNICODE_ENVIRONMENT, CreateProcessW,
        DeleteProcThreadAttributeList, EXTENDED_STARTUPINFO_PRESENT, GetExitCodeProcess,
        InitializeProcThreadAttributeList, PROC_THREAD_ATTRIBUTE_HANDLE_LIST,
        PROC_THREAD_ATTRIBUTE_JOB_LIST, PROCESS_INFORMATION, QueryFullProcessImageNameW,
        ResumeThread, STARTF_USESTDHANDLES, STARTUPINFOEXW, UpdateProcThreadAttribute,
        WaitForSingleObject,
    };

    use crate::store::{PreparedJob, Store, StoreError};
    use crate::{JobOutcome, LogStream};

    #[cfg(test)]
    thread_local! {
        static FORCE_PRESTART_FAILURE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }

    struct OwnedHandle(HANDLE);

    impl OwnedHandle {
        fn new(handle: HANDLE) -> std::io::Result<Self> {
            if handle.is_null() || handle == INVALID_HANDLE_VALUE {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(Self(handle))
            }
        }

        fn raw(&self) -> HANDLE {
            self.0
        }

        fn into_raw(mut self) -> HANDLE {
            let handle = self.0;
            self.0 = null_mut();
            handle
        }
    }

    impl Drop for OwnedHandle {
        fn drop(&mut self) {
            if !self.0.is_null() && self.0 != INVALID_HANDLE_VALUE {
                // SAFETY: the handle is owned and closed exactly once.
                unsafe { CloseHandle(self.0) };
            }
        }
    }

    struct AttributeList {
        storage: Vec<u8>,
        initialized: bool,
        job: Box<HANDLE>,
        inherited_handles: Box<[HANDLE; 3]>,
    }

    impl AttributeList {
        fn with_job_and_handles(
            job: HANDLE,
            inherited_handles: [HANDLE; 3],
        ) -> std::io::Result<Self> {
            let mut bytes = 0_usize;
            // SAFETY: the documented sizing call accepts a null list and writes the required size.
            unsafe { InitializeProcThreadAttributeList(null_mut(), 2, 0, &mut bytes) };
            if bytes == 0 {
                return Err(std::io::Error::last_os_error());
            }
            let mut list = Self {
                storage: vec![0_u8; bytes],
                initialized: false,
                job: Box::new(job),
                inherited_handles: Box::new(inherited_handles),
            };
            // SAFETY: storage is writable and has the exact size returned by the sizing call.
            if unsafe { InitializeProcThreadAttributeList(list.raw(), 2, 0, &mut bytes) } == 0 {
                return Err(std::io::Error::last_os_error());
            }
            list.initialized = true;
            // SAFETY: the list is initialized and `job` remains alive through CreateProcessW.
            if unsafe {
                UpdateProcThreadAttribute(
                    list.raw(),
                    0,
                    PROC_THREAD_ATTRIBUTE_JOB_LIST as usize,
                    (list.job.as_ref() as *const HANDLE).cast::<c_void>(),
                    size_of::<HANDLE>(),
                    null_mut(),
                    null(),
                )
            } == 0
            {
                return Err(std::io::Error::last_os_error());
            }
            // SAFETY: the handles are inheritable and the boxed array remains alive through
            // CreateProcessW. The explicit list prevents unrelated daemon handles from leaking.
            if unsafe {
                UpdateProcThreadAttribute(
                    list.raw(),
                    0,
                    PROC_THREAD_ATTRIBUTE_HANDLE_LIST as usize,
                    list.inherited_handles.as_ptr().cast::<c_void>(),
                    size_of::<[HANDLE; 3]>(),
                    null_mut(),
                    null(),
                )
            } == 0
            {
                return Err(std::io::Error::last_os_error());
            }
            Ok(list)
        }

        fn raw(&mut self) -> *mut c_void {
            self.storage.as_mut_ptr().cast()
        }
    }

    impl Drop for AttributeList {
        fn drop(&mut self) {
            if self.initialized {
                // SAFETY: a nonempty list was initialized before construction completed.
                unsafe { DeleteProcThreadAttributeList(self.raw()) };
            }
        }
    }

    #[derive(Default)]
    struct RunProgress {
        user_code_released: bool,
        cleanup_proven: bool,
        exit_code: Option<i32>,
        timed_out: bool,
    }

    type RunResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

    pub(super) fn run(job: &PreparedJob, store: &Arc<Mutex<Store>>) {
        let mut progress = RunProgress {
            cleanup_proven: true,
            ..RunProgress::default()
        };
        if let Err(error) = run_inner(job, store, &mut progress) {
            if let Ok(mut store) = store.lock() {
                if progress.cleanup_proven {
                    let (outcome, verdict) = failed_run_classification(&progress);
                    let _ = store.mark_finished(job, progress.exit_code, outcome, verdict);
                } else {
                    let _ = store.mark_uncertain(job, progress.exit_code, "interrupted");
                }
            }
            use std::io::Write as _;
            let _ = writeln!(
                std::io::stderr(),
                "stillyard runner for {} failed: {error}",
                job.job_id
            );
        }
    }

    fn failed_run_classification(progress: &RunProgress) -> (JobOutcome, &'static str) {
        if progress.timed_out {
            (JobOutcome::TimedOut, "timed_out")
        } else if progress.user_code_released {
            (JobOutcome::Interrupted, "interrupted")
        } else {
            (JobOutcome::Failed, "start_failed")
        }
    }

    fn run_inner(
        job: &PreparedJob,
        store: &Arc<Mutex<Store>>,
        progress: &mut RunProgress,
    ) -> RunResult<()> {
        validate_paths(&job.spec.executable, &job.spec.working_directory)?;
        let mut executable_file = OpenOptions::new()
            .read(true)
            .share_mode(FILE_SHARE_READ)
            .open(&job.spec.executable)
            .map_err(|error| io_context("locking executable for launch", error))?;
        let executable_hash = hash_reader(&mut executable_file)?;
        let job_object = create_job_object()?;
        let registration = super::register_containment(job.invocation_id, job_object.raw())?;
        progress.cleanup_proven = true;
        let (stdout_read, stdout_write) = create_inherited_pipe()?;
        let (stderr_read, stderr_write) = create_inherited_pipe()?;
        let stdin = open_stdin(job)?;
        let mut attributes = AttributeList::with_job_and_handles(
            job_object.raw(),
            [
                stdin.as_raw_handle() as HANDLE,
                stdout_write.raw(),
                stderr_write.raw(),
            ],
        )
        .map_err(|error| io_context("building born-contained attribute list", error))?;

        let application = wide_null(job.spec.executable.as_os_str());
        let mut command_line = command_line(&job.spec.executable, &job.spec.args);
        let working_directory = wide_null(job.spec.working_directory.as_os_str());
        let mut environment = environment_block(job)?;
        let mut startup: STARTUPINFOEXW = unsafe { zeroed() };
        startup.StartupInfo.cb = size_of::<STARTUPINFOEXW>() as u32;
        startup.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
        startup.StartupInfo.hStdInput = stdin.as_raw_handle() as HANDLE;
        startup.StartupInfo.hStdOutput = stdout_write.raw();
        startup.StartupInfo.hStdError = stderr_write.raw();
        startup.lpAttributeList = attributes.raw();
        let mut process: PROCESS_INFORMATION = unsafe { zeroed() };

        // SAFETY: all pointers reference initialized, live buffers; inherited standard handles are
        // valid; PROCESS_INFORMATION is writable; the process starts suspended.
        let created = unsafe {
            CreateProcessW(
                application.as_ptr(),
                command_line.as_mut_ptr(),
                null(),
                null(),
                1,
                CREATE_SUSPENDED
                    | CREATE_UNICODE_ENVIRONMENT
                    | CREATE_NO_WINDOW
                    | EXTENDED_STARTUPINFO_PRESENT,
                environment.as_mut_ptr().cast::<c_void>(),
                working_directory.as_ptr(),
                &startup.StartupInfo,
                &mut process,
            )
        };
        if created == 0 {
            return Err(io_context(
                "creating born-contained suspended process",
                std::io::Error::last_os_error(),
            )
            .into());
        }
        progress.cleanup_proven = false;
        macro_rules! prestart_try {
            ($expression:expr) => {
                match $expression {
                    Ok(value) => value,
                    Err(error) => {
                        // SAFETY: CreateProcessW succeeded suspended inside this Job Object.
                        unsafe { TerminateJobObject(job_object.raw(), 70) };
                        progress.cleanup_proven =
                            wait_job_empty(job_object.raw(), Duration::from_secs(30)).is_ok();
                        return Err(error.into());
                    }
                }
            };
        }
        #[cfg(test)]
        if FORCE_PRESTART_FAILURE.replace(false) {
            prestart_try!(Err::<(), _>(std::io::Error::other(
                "forced pre-start failure"
            )));
        }
        let process_handle = prestart_try!(OwnedHandle::new(process.hProcess));
        let thread_handle = prestart_try!(OwnedHandle::new(process.hThread));
        drop(stdout_write);
        drop(stderr_write);
        drop(stdin);

        let image_path = prestart_try!(process_image_path(process_handle.raw()));
        prestart_try!(validate_executable(&image_path));
        if !prestart_try!(same_windows_path(&image_path, &job.spec.executable)) {
            let error = std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "created image {} differs from requested executable {}",
                    image_path.display(),
                    job.spec.executable.display()
                ),
            );
            // SAFETY: the process is suspended inside this job and no user code can have run.
            unsafe { TerminateJobObject(job_object.raw(), 70) };
            progress.cleanup_proven =
                wait_job_empty(job_object.raw(), Duration::from_secs(30)).is_ok();
            return Err(error.into());
        }
        drop(executable_file);
        {
            let mut store = prestart_try!(
                store
                    .lock()
                    .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))
            );
            prestart_try!(store.mark_started(job, process.dwProcessId, &executable_hash));
        }

        let stdout_file = unsafe { File::from_raw_handle(stdout_read.into_raw() as RawHandle) };
        let stderr_file = unsafe { File::from_raw_handle(stderr_read.into_raw() as RawHandle) };
        let stdout = prestart_try!(spawn_drain(
            stdout_file,
            job.stdout_path.clone(),
            job.job_id,
            LogStream::Stdout,
            Arc::clone(store),
        ));
        let stderr = prestart_try!(spawn_drain(
            stderr_file,
            job.stderr_path.clone(),
            job.job_id,
            LogStream::Stderr,
            Arc::clone(store),
        ));

        let execution = (|| -> RunResult<(u32, bool)> {
            // SAFETY: the primary thread handle is valid and has not been resumed before.
            if unsafe { ResumeThread(thread_handle.raw()) } == u32::MAX {
                return Err(std::io::Error::last_os_error().into());
            }
            progress.user_code_released = true;

            let deadline = job
                .spec
                .timeout_seconds
                .and_then(|seconds| Instant::now().checked_add(Duration::from_secs(seconds)));
            let mut timed_out = false;
            loop {
                // SAFETY: process_handle remains valid throughout the wait.
                let wait = unsafe { WaitForSingleObject(process_handle.raw(), 100) };
                if wait == WAIT_OBJECT_0 {
                    break;
                }
                if wait != WAIT_TIMEOUT {
                    return Err(std::io::Error::last_os_error().into());
                }
                if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                    timed_out = true;
                    progress.timed_out = true;
                    // SAFETY: job is valid and contains the complete tree.
                    unsafe { TerminateJobObject(job_object.raw(), 21) };
                    // SAFETY: process_handle remains valid.
                    if unsafe { WaitForSingleObject(process_handle.raw(), 30_000) } != WAIT_OBJECT_0
                    {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "terminated root did not exit within cleanup bound",
                        )
                        .into());
                    }
                    break;
                }
            }

            let mut exit_code = 0_u32;
            // SAFETY: process handle and output pointer are valid.
            if unsafe { GetExitCodeProcess(process_handle.raw(), &mut exit_code) } == 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            progress.exit_code = Some(exit_code as i32);
            store
                .lock()
                .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?
                .mark_root_exited(job, exit_code as i32)?;
            // Root exit always terminates remaining descendants before cleanup proof.
            // SAFETY: job is valid. It is harmless when already empty.
            unsafe { TerminateJobObject(job_object.raw(), exit_code) };
            wait_job_empty(job_object.raw(), Duration::from_secs(30))?;
            progress.cleanup_proven = true;
            Ok((exit_code, timed_out))
        })();

        if execution.is_err() {
            // SAFETY: the job is live and owns the complete tree.
            unsafe { TerminateJobObject(job_object.raw(), 70) };
            if wait_job_empty(job_object.raw(), Duration::from_secs(30)).is_ok() {
                progress.cleanup_proven = true;
            }
        }
        drop(process_handle);
        drop(thread_handle);

        if !progress.cleanup_proven {
            // The pipes may remain open while an unproven process tree is terminating. Do not
            // block the scheduler indefinitely; the uncertain Containment keeps EOF unclaimed.
            // Leave the live authority set before unpublishing its handle. Closing the Job Object
            // afterwards applies KILL_ON_JOB_CLOSE to any process that escaped the bounded wait.
            store
                .lock()
                .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?
                .mark_uncertain(job, progress.exit_code, "interrupted")?;
            drop(registration);
            drop(job_object);
            return execution.map(|_| ());
        }

        let stdout_result = stdout.join().map_err(|_| "stdout drain thread panicked")?;
        let stderr_result = stderr.join().map_err(|_| "stderr drain thread panicked")?;
        stdout_result?;
        stderr_result?;

        let (exit_code, timed_out) = execution?;

        let (outcome, verdict) = if timed_out {
            (JobOutcome::TimedOut, "timed_out")
        } else if exit_code == 0 {
            (JobOutcome::Succeeded, "succeeded")
        } else {
            (JobOutcome::Failed, "process_failed")
        };
        store
            .lock()
            .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?
            .mark_finished(job, Some(exit_code as i32), outcome, verdict)?;
        // Membership queries hold the same registry mutex used by registration Drop, so no
        // query can observe this raw HANDLE after it is closed or numerically recycled.
        drop(registration);
        drop(job_object);
        Ok(())
    }

    fn create_job_object() -> std::io::Result<OwnedHandle> {
        // SAFETY: null attributes/name request a fresh unnamed Job Object as required by R-RUN-2.
        let handle = unsafe { CreateJobObjectW(null(), null()) };
        let handle = OwnedHandle::new(handle)?;
        let mut information: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { zeroed() };
        information.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        // SAFETY: information points to the correct structure for the selected class.
        let set = unsafe {
            SetInformationJobObject(
                handle.raw(),
                JobObjectExtendedLimitInformation,
                (&raw const information).cast::<c_void>(),
                size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            )
        };
        if set == 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(handle)
    }

    fn io_context(context: &str, error: std::io::Error) -> std::io::Error {
        std::io::Error::new(error.kind(), format!("{context}: {error}"))
    }

    fn create_inherited_pipe() -> std::io::Result<(OwnedHandle, OwnedHandle)> {
        let mut read = null_mut();
        let mut write = null_mut();
        let attributes = SECURITY_ATTRIBUTES {
            nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: null_mut(),
            bInheritHandle: 1,
        };
        // SAFETY: output pointers and security attributes are valid.
        if unsafe { CreatePipe(&mut read, &mut write, &attributes, 0) } == 0 {
            return Err(std::io::Error::last_os_error());
        }
        let read = OwnedHandle::new(read)?;
        let write = OwnedHandle::new(write)?;
        // SAFETY: read is valid; clearing inheritance keeps the daemon-side handle private.
        if unsafe { SetHandleInformation(read.raw(), HANDLE_FLAG_INHERIT, 0) } == 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok((read, write))
    }

    fn open_nul_for_read() -> std::io::Result<OwnedHandle> {
        let nul = wide_null(OsStr::new("NUL"));
        let attributes = SECURITY_ATTRIBUTES {
            nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: null_mut(),
            bInheritHandle: 1,
        };
        // SAFETY: NUL is NUL-terminated and attributes are initialized.
        let handle = unsafe {
            CreateFileW(
                nul.as_ptr(),
                GENERIC_READ,
                FILE_SHARE_READ | FILE_SHARE_WRITE,
                &attributes,
                OPEN_EXISTING,
                FILE_ATTRIBUTE_NORMAL,
                null_mut(),
            )
        };
        OwnedHandle::new(handle)
    }

    fn open_stdin(job: &PreparedJob) -> std::io::Result<File> {
        let mut file = match (&job.stdin, &job.stdin_path) {
            (None, None) => {
                let handle = open_nul_for_read()?.into_raw();
                // SAFETY: ownership of the valid NUL handle transfers into File exactly once.
                unsafe { File::from_raw_handle(handle as RawHandle) }
            }
            (Some(expected), Some(path)) => {
                let file = OpenOptions::new()
                    .read(true)
                    .share_mode(FILE_SHARE_READ)
                    .open(path)?;
                if file.metadata()?.len() != expected.length {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "staged stdin length changed before launch",
                    ));
                }
                file
            }
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "partial staged stdin reference",
                ));
            }
        };
        if let Some(expected) = &job.stdin {
            if hash_reader(&mut file)? != expected.sha256 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "staged stdin hash changed before launch",
                ));
            }
            file.seek(SeekFrom::Start(0))?;
        }
        // SAFETY: file owns a valid handle; setting inheritance is limited by the explicit handle
        // list supplied to CreateProcessW.
        if unsafe {
            SetHandleInformation(
                file.as_raw_handle() as HANDLE,
                HANDLE_FLAG_INHERIT,
                HANDLE_FLAG_INHERIT,
            )
        } == 0
        {
            return Err(std::io::Error::last_os_error());
        }
        Ok(file)
    }

    fn spawn_drain(
        mut input: File,
        path: PathBuf,
        job_id: crate::JobId,
        stream: LogStream,
        store: Arc<Mutex<Store>>,
    ) -> std::io::Result<std::thread::JoinHandle<RunResult<()>>> {
        std::thread::Builder::new()
            .name(format!("stillyard-log-{}-{stream:?}", job_id.entity_uuid()))
            .spawn(move || {
                let mut output = OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .open(path)?;
                let mut offset = 0_u64;
                let mut buffer = [0_u8; 64 * 1024];
                loop {
                    let read = input.read(&mut buffer)?;
                    if read == 0 {
                        break;
                    }
                    output.write_all(&buffer[..read])?;
                    output.sync_data()?;
                    offset += read as u64;
                    store
                        .lock()
                        .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?
                        .commit_log_offset(job_id, stream, offset)?;
                }
                output.sync_all()?;
                Ok(())
            })
    }

    fn wait_job_empty(handle: HANDLE, timeout: Duration) -> std::io::Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            let mut information: JOBOBJECT_BASIC_ACCOUNTING_INFORMATION = unsafe { zeroed() };
            // SAFETY: information matches the requested class and output size.
            let queried = unsafe {
                QueryInformationJobObject(
                    handle,
                    JobObjectBasicAccountingInformation,
                    (&raw mut information).cast::<c_void>(),
                    size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>() as u32,
                    null_mut(),
                )
            };
            if queried == 0 {
                return Err(std::io::Error::last_os_error());
            }
            if information.ActiveProcesses == 0 {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "job object did not become empty",
                ));
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    }

    fn process_image_path(handle: HANDLE) -> std::io::Result<PathBuf> {
        let mut buffer = vec![0_u16; 32_768];
        let mut length = buffer.len() as u32;
        // SAFETY: handle is valid and the buffer/length pointers are writable.
        if unsafe { QueryFullProcessImageNameW(handle, 0, buffer.as_mut_ptr(), &mut length) } == 0 {
            return Err(std::io::Error::last_os_error());
        }
        buffer.truncate(length as usize);
        Ok(PathBuf::from(String::from_utf16_lossy(&buffer)))
    }

    fn validate_paths(executable: &Path, working_directory: &Path) -> std::io::Result<()> {
        validate_executable(executable)?;
        if !working_directory.is_dir() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "working directory is missing or not a directory",
            ));
        }
        Ok(())
    }

    fn validate_executable(path: &Path) -> std::io::Result<()> {
        let metadata = std::fs::symlink_metadata(path)?;
        let wide = wide_null(path.as_os_str());
        // SAFETY: wide is NUL-terminated and remains alive for the call.
        let attributes = unsafe { GetFileAttributesW(wide.as_ptr()) };
        if attributes == INVALID_FILE_ATTRIBUTES {
            return Err(std::io::Error::last_os_error());
        }
        if !metadata.file_type().is_file()
            || metadata.file_type().is_symlink()
            || attributes & FILE_ATTRIBUTE_REPARSE_POINT != 0
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "executable is not an ordinary non-reparse file",
            ));
        }
        Ok(())
    }

    fn hash_reader(file: &mut File) -> std::io::Result<String> {
        let mut hash = Sha256::new();
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hash.update(&buffer[..read]);
        }
        Ok(format!("{:x}", hash.finalize()))
    }

    fn same_windows_path(left: &Path, right: &Path) -> std::io::Result<bool> {
        let left = std::fs::canonicalize(left)?;
        let right = std::fs::canonicalize(right)?;
        Ok(left
            .as_os_str()
            .to_string_lossy()
            .eq_ignore_ascii_case(&right.as_os_str().to_string_lossy()))
    }

    fn environment_block(job: &PreparedJob) -> std::io::Result<Vec<u16>> {
        let mut environment = BTreeMap::<String, String>::new();
        for name in ["SystemRoot", "WINDIR", "TEMP", "TMP"] {
            if let Ok(value) = std::env::var(name) {
                environment.insert(name.to_uppercase(), value);
            }
        }
        for (name, value) in &job.spec.environment.set {
            environment.insert(name.to_uppercase(), value.clone());
        }
        for name in &job.spec.environment.unset {
            environment.remove(&name.to_uppercase());
        }
        environment.insert("STILLYARD_JOB_ID".into(), job.job_id.to_string());
        environment.insert("STILLYARD_ATTEMPT".into(), job.attempt_id.to_string());
        environment.insert(
            "STILLYARD_INVOCATION_ID".into(),
            job.invocation_id.to_string(),
        );
        environment.insert("STILLYARD_ROLE".into(), "primary".into());
        environment.insert(
            "STILLYARD_ENDPOINT".into(),
            crate::client::default_endpoint().map_err(std::io::Error::other)?,
        );
        environment.insert(
            "STILLYARD_DAEMON_ID".into(),
            job.job_id.store_uuid().to_string(),
        );
        let mut pairs: Vec<_> = environment.into_iter().collect();
        pairs.sort_by_key(|(name, _)| name.to_uppercase());
        let mut block = Vec::new();
        for (name, value) in pairs {
            block.extend(OsStr::new(&format!("{name}={value}")).encode_wide());
            block.push(0);
        }
        block.push(0);
        Ok(block)
    }

    fn command_line(executable: &Path, args: &[String]) -> Vec<u16> {
        let mut command = quote_windows_arg(&executable.as_os_str().to_string_lossy());
        for arg in args {
            command.push(' ');
            command.push_str(&quote_windows_arg(arg));
        }
        OsStr::new(&command)
            .encode_wide()
            .chain(std::iter::once(0))
            .collect()
    }

    fn quote_windows_arg(arg: &str) -> String {
        if !arg.is_empty() && !arg.bytes().any(|byte| matches!(byte, b' ' | b'\t' | b'"')) {
            return arg.to_owned();
        }
        let mut quoted = String::from("\"");
        let mut backslashes = 0;
        for character in arg.chars() {
            if character == '\\' {
                backslashes += 1;
            } else if character == '"' {
                quoted.push_str(&"\\".repeat(backslashes * 2 + 1));
                quoted.push('"');
                backslashes = 0;
            } else {
                quoted.push_str(&"\\".repeat(backslashes));
                backslashes = 0;
                quoted.push(character);
            }
        }
        quoted.push_str(&"\\".repeat(backslashes * 2));
        quoted.push('"');
        quoted
    }

    fn wide_null(value: &OsStr) -> Vec<u16> {
        value.encode_wide().chain(std::iter::once(0)).collect()
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::protocol::StagedInputRef;
        use crate::store::{
            StorePaths, normalized_payload_hash, normalized_payload_hash_with_input,
        };
        use crate::{
            EnvironmentSpec, JobSpec, JobState, ResourceClaims, RetryPolicy, SPEC_VERSION,
            StdinSpec,
        };
        use uuid::Uuid;

        fn job_spec(root: &Path, executable: PathBuf, args: Vec<String>) -> JobSpec {
            JobSpec {
                spec_version: SPEC_VERSION,
                executable,
                args,
                working_directory: root.to_path_buf(),
                stdin: StdinSpec::Eof,
                environment: EnvironmentSpec::default(),
                resources: ResourceClaims::default(),
                conditions: Vec::new(),
                retry: RetryPolicy::default(),
                labels: Vec::new(),
                expected_duration_seconds: Some(1),
                timeout_seconds: Some(10),
                quiet: None,
                artifacts: Vec::new(),
                allow_child_submissions: false,
            }
        }

        fn prepared(spec: &JobSpec, root: &Path) -> (PreparedJob, Arc<Mutex<Store>>) {
            let mut store = Store::open(StorePaths::new(root.to_path_buf())).unwrap();
            let hash = normalized_payload_hash(spec).unwrap();
            let submitted = store.submit(Uuid::now_v7(), &hash, spec).unwrap();
            let job = store
                .prepare_job(submitted.receipt.job_id)
                .unwrap()
                .unwrap();
            (job, Arc::new(Mutex::new(store)))
        }

        fn prepared_with_stdin(
            spec: &JobSpec,
            root: &Path,
            bytes: &[u8],
        ) -> (PreparedJob, Arc<Mutex<Store>>) {
            let mut store = Store::open(StorePaths::new(root.to_path_buf())).unwrap();
            let input = StagedInputRef {
                sha256: format!("{:x}", Sha256::digest(bytes)),
                length: bytes.len() as u64,
            };
            let upload_id = Uuid::now_v7();
            store
                .stage_begin(upload_id, &input.sha256, input.length)
                .unwrap();
            store.stage_chunk(upload_id, 0, bytes).unwrap();
            assert_eq!(store.stage_commit(upload_id).unwrap(), input);
            let hash = normalized_payload_hash_with_input(spec, Some(&input)).unwrap();
            let submitted = store
                .submit_with_stdin(Uuid::now_v7(), &hash, spec, Some(&input))
                .unwrap();
            let job = store
                .prepare_job(submitted.receipt.job_id)
                .unwrap()
                .unwrap();
            (job, Arc::new(Mutex::new(store)))
        }

        #[test]
        fn released_user_code_runner_failure_is_interrupted_not_failed() {
            let progress = RunProgress {
                user_code_released: true,
                cleanup_proven: true,
                ..RunProgress::default()
            };
            assert_eq!(
                failed_run_classification(&progress),
                (JobOutcome::Interrupted, "interrupted")
            );
        }

        #[test]
        fn windows_quoting_handles_spaces_quotes_and_trailing_slashes() {
            assert_eq!(quote_windows_arg("plain"), "plain");
            assert_eq!(quote_windows_arg("two words"), "\"two words\"");
            assert_eq!(quote_windows_arg("a\\\"b"), "\"a\\\\\\\"b\"");
            assert_eq!(
                quote_windows_arg("path with space\\"),
                "\"path with space\\\\\""
            );
        }

        #[test]
        fn contained_process_publishes_output_after_success() {
            let temp = tempfile::tempdir().unwrap();
            let command = PathBuf::from(std::env::var_os("SystemRoot").unwrap())
                .join("System32")
                .join("cmd.exe");
            let spec = job_spec(
                temp.path(),
                command,
                vec![
                    "/D".into(),
                    "/S".into(),
                    "/C".into(),
                    "echo stillyard-smoke".into(),
                ],
            );
            let (job, store) = prepared(&spec, temp.path());
            run(&job, &store);
            let store = store.lock().unwrap();
            let snapshot = store.status(job.job_id).unwrap();
            assert_eq!(snapshot.state, JobState::Final);
            assert_eq!(snapshot.outcome, Some(JobOutcome::Succeeded));
            let logs = store.logs(job.job_id, LogStream::Stdout, 0, 1024).unwrap();
            assert!(String::from_utf8_lossy(&logs.bytes).contains("stillyard-smoke"));
            assert!(logs.eof);
        }

        #[test]
        fn staged_stdin_handle_reaches_the_contained_process() {
            let temp = tempfile::tempdir().unwrap();
            let payload = b"stillyard staged stdin marker\nsecond line\n";
            let mut spec = job_spec(
                temp.path(),
                std::env::current_exe().unwrap(),
                vec![
                    "--ignored".into(),
                    "--exact".into(),
                    "runner::windows::tests::stdin_echo_helper".into(),
                    "--nocapture".into(),
                ],
            );
            spec.stdin = StdinSpec::File {
                path: temp.path().join("client-prompt.bin"),
            };
            let (job, store) = prepared_with_stdin(&spec, temp.path(), payload);
            run(&job, &store);
            let store = store.lock().unwrap();
            let snapshot = store.status(job.job_id).unwrap();
            assert_eq!(snapshot.outcome, Some(JobOutcome::Succeeded));
            let logs = store
                .logs(job.job_id, LogStream::Stdout, 0, 64 * 1024)
                .unwrap();
            assert!(
                logs.bytes
                    .windows(payload.len())
                    .any(|window| window == payload),
                "the managed root must read the immutable staged bytes"
            );
        }

        #[test]
        #[allow(clippy::permissions_set_readonly_false)]
        fn staged_stdin_changed_after_acceptance_fails_before_user_code() {
            let temp = tempfile::tempdir().unwrap();
            let payload = b"trusted stdin";
            let mut spec = job_spec(
                temp.path(),
                std::env::current_exe().unwrap(),
                vec![
                    "--ignored".into(),
                    "--exact".into(),
                    "runner::windows::tests::stdin_echo_helper".into(),
                    "--nocapture".into(),
                ],
            );
            spec.stdin = StdinSpec::File {
                path: temp.path().join("client-prompt.bin"),
            };
            let (job, store) = prepared_with_stdin(&spec, temp.path(), payload);
            let blob = job.stdin_path.as_ref().unwrap();
            let mut permissions = std::fs::metadata(blob).unwrap().permissions();
            permissions.set_readonly(false);
            std::fs::set_permissions(blob, permissions).unwrap();
            std::fs::write(blob, b"altered stdin").unwrap();

            run(&job, &store);
            let store = store.lock().unwrap();
            let snapshot = store.status(job.job_id).unwrap();
            assert_eq!(snapshot.outcome, Some(JobOutcome::Failed));
            assert_eq!(snapshot.root_exit_code, None);
            let logs = store.logs(job.job_id, LogStream::Stdout, 0, 1024).unwrap();
            assert!(logs.bytes.is_empty(), "user code must not have run");
        }

        #[test]
        #[ignore = "launched only as a managed staged-stdin probe"]
        fn stdin_echo_helper() {
            std::io::copy(&mut std::io::stdin(), &mut std::io::stdout()).unwrap();
        }

        #[test]
        fn environment_block_has_exact_path_and_no_daemon_ambient_user_profile() {
            let temp = tempfile::tempdir().unwrap();
            let command = PathBuf::from(std::env::var_os("SystemRoot").unwrap())
                .join("System32")
                .join("cmd.exe");
            let mut spec = job_spec(temp.path(), command, vec![]);
            spec.environment
                .set
                .insert("PATH".into(), r"C:\Exact\Tools".into());
            let (job, _store) = prepared(&spec, temp.path());
            let block = environment_block(&job).unwrap();
            let decoded = String::from_utf16(&block[..block.len() - 2]).unwrap();
            let values: BTreeMap<_, _> = decoded
                .split('\0')
                .filter_map(|pair| pair.split_once('='))
                .map(|(name, value)| (name.to_owned(), value.to_owned()))
                .collect();
            assert_eq!(values.get("PATH").unwrap(), r"C:\Exact\Tools");
            assert!(!values.contains_key("USERPROFILE"));
            assert!(!values.contains_key("SSH_AUTH_SOCK"));
            assert!(!values.contains_key("ANTHROPIC_API_KEY"));
            assert_eq!(
                values.get("STILLYARD_ATTEMPT").unwrap(),
                &job.attempt_id.to_string()
            );
            assert_eq!(
                values.get("STILLYARD_DAEMON_ID").unwrap(),
                &job.job_id.store_uuid().to_string()
            );
        }

        #[test]
        fn timeout_kills_containment_and_releases_lease() {
            let temp = tempfile::tempdir().unwrap();
            let powershell = PathBuf::from(std::env::var_os("SystemRoot").unwrap())
                .join("System32")
                .join("WindowsPowerShell")
                .join("v1.0")
                .join("powershell.exe");
            let mut spec = job_spec(
                temp.path(),
                powershell,
                vec![
                    "-NoLogo".into(),
                    "-NoProfile".into(),
                    "-NonInteractive".into(),
                    "-Command".into(),
                    "Start-Sleep -Seconds 30".into(),
                ],
            );
            spec.timeout_seconds = Some(1);
            let (job, store) = prepared(&spec, temp.path());
            let started = Instant::now();
            run(&job, &store);
            assert!(started.elapsed() < Duration::from_secs(10));
            let snapshot = store.lock().unwrap().status(job.job_id).unwrap();
            assert_eq!(snapshot.state, JobState::Final);
            assert_eq!(snapshot.outcome, Some(JobOutcome::TimedOut));
            assert_eq!(snapshot.root_exit_code, Some(21));
        }

        #[test]
        fn timeout_kills_descendant_not_only_root() {
            use windows_sys::Win32::System::Threading::{
                OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
            };

            let temp = tempfile::tempdir().unwrap();
            let pid_file = temp.path().join("descendant.pid");
            let mut spec = job_spec(
                temp.path(),
                std::env::current_exe().unwrap(),
                vec![
                    "--ignored".into(),
                    "--exact".into(),
                    "runner::windows::tests::spawn_descendant_helper".into(),
                ],
            );
            spec.environment.set.insert(
                "STY_TEST_PID_FILE".into(),
                pid_file.to_string_lossy().into_owned(),
            );
            spec.timeout_seconds = Some(5);
            let (job, store) = prepared(&spec, temp.path());
            run(&job, &store);
            let pid: u32 = std::fs::read_to_string(pid_file)
                .unwrap()
                .trim()
                .parse()
                .unwrap();
            // SAFETY: the access is read-only and the PID came from the launched descendant.
            let process = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
            if !process.is_null() {
                let mut exit_code = 0_u32;
                // SAFETY: process is a live handle and exit_code is writable.
                assert_ne!(unsafe { GetExitCodeProcess(process, &mut exit_code) }, 0);
                // 259 is STILL_ACTIVE. A root-only termination mutant leaves this descendant live.
                assert_ne!(exit_code, 259);
                // SAFETY: this test owns the process handle.
                unsafe { CloseHandle(process) };
            }
        }

        #[test]
        #[ignore = "launched only as a managed root by timeout_kills_descendant_not_only_root"]
        fn spawn_descendant_helper() {
            let executable = std::env::current_exe().unwrap();
            let mut child = std::process::Command::new(executable)
                .args([
                    "--ignored",
                    "--exact",
                    "runner::windows::tests::descendant_sleeper",
                ])
                .spawn()
                .unwrap();
            std::fs::write(
                std::env::var_os("STY_TEST_PID_FILE").unwrap(),
                child.id().to_string(),
            )
            .unwrap();
            std::thread::sleep(Duration::from_secs(30));
            let _ = child.wait();
        }

        #[test]
        #[ignore = "launched only as a descendant containment probe"]
        fn descendant_sleeper() {
            std::thread::sleep(Duration::from_secs(30));
        }

        #[test]
        fn post_create_pre_resume_failure_proves_empty_and_is_start_failed() {
            let temp = tempfile::tempdir().unwrap();
            let command = PathBuf::from(std::env::var_os("SystemRoot").unwrap())
                .join("System32")
                .join("cmd.exe");
            let spec = job_spec(
                temp.path(),
                command,
                vec!["/D".into(), "/C".into(), "echo must-not-run".into()],
            );
            let (job, store) = prepared(&spec, temp.path());
            FORCE_PRESTART_FAILURE.set(true);
            run(&job, &store);
            let store = store.lock().unwrap();
            let snapshot = store.status(job.job_id).unwrap();
            assert_eq!(snapshot.state, JobState::Final);
            assert_eq!(snapshot.outcome, Some(JobOutcome::Failed));
            assert_eq!(snapshot.root_exit_code, None);
            let logs = store.logs(job.job_id, LogStream::Stdout, 0, 1024).unwrap();
            assert!(logs.eof);
            assert!(logs.bytes.is_empty());
        }
    }
}
