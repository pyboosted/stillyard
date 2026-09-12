//! Optional native companion for protected cross-OS bootstrap acceptance.
//! It inherits the primary's Windows Job Object, streams, environment and Lease.
use std::ffi::OsString;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use stillyard::{Client, Error, ProcessIdentity, Result};

pub struct Controller {
    child: Option<Child>,
    deadline: Instant,
}
impl Controller {
    pub fn start(
        client: &Client,
        executable: &Path,
        args: &[OsString],
        timeout: Duration,
    ) -> Result<Self> {
        if !executable.is_absolute() || !executable.is_file() {
            return Err(Error::InvalidSpec(
                "native controller requires an existing absolute executable".into(),
            ));
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        let parent = client
            .submission_context(deadline, None)?
            .parent
            .ok_or_else(|| {
                Error::InvalidSpec(
                    "native controller requires a bounded native Stillyard Job".into(),
                )
            })?;
        let job = client.status(parent.job_id, deadline, None)?;
        let primary = job
            .attempts
            .iter()
            .flat_map(|attempt| &attempt.invocations)
            .find(|invocation| invocation.invocation_id == parent.invocation_id);
        if job.invocation_id != Some(parent.invocation_id)
            || job.attempt_id != Some(parent.attempt_id)
            || job.cancel_requested
            || job.spec.timeout_seconds.is_none_or(|seconds| seconds == 0)
            || !primary.is_some_and(|invocation| {
                invocation.role == stillyard::InvocationRole::Primary
                    && matches!(
                        invocation.root_identity,
                        Some(ProcessIdentity::Windows { pid, .. }) if pid == std::process::id()
                    )
            })
        {
            return Err(Error::InvalidSpec(
                "native controller requires the current primary root with a finite Job timeout"
                    .into(),
            ));
        }
        // No CREATE_BREAKAWAY_FROM_JOB or detached launch. The ordinary Job
        // accounts for this native work even if the later BootstrapArm rejects.
        let controller_deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
            Error::InvalidSpec("native controller timeout exceeds the monotonic clock range".into())
        })?;
        let child = Command::new(executable)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()?;
        Ok(Self {
            child: Some(child),
            deadline: controller_deadline,
        })
    }
    pub fn finish(mut self) -> Result<i32> {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Foundation::{WAIT_FAILED, WAIT_OBJECT_0, WAIT_TIMEOUT};
        use windows_sys::Win32::System::Threading::WaitForSingleObject;
        let child = self.child.as_mut().expect("controller is present");
        let millis = self
            .deadline
            .saturating_duration_since(Instant::now())
            .as_millis()
            .min(u128::from(u32::MAX - 1)) as u32;
        // SAFETY: Child owns this live process handle throughout the bounded wait.
        let result = unsafe { WaitForSingleObject(child.as_raw_handle(), millis) };
        match result {
            WAIT_OBJECT_0 => {}
            WAIT_TIMEOUT => {
                return Err(Error::Unavailable(
                    "native controller exceeded its bootstrap companion deadline".into(),
                ));
            }
            WAIT_FAILED => return Err(std::io::Error::last_os_error().into()),
            _ => {
                return Err(Error::Unavailable(
                    "unexpected native controller wait result".into(),
                ));
            }
        }
        let status = child.wait()?;
        self.child.take();
        Ok(if status.success() {
            0
        } else {
            status.code().unwrap_or(1).clamp(1, 255)
        })
    }
}
impl Drop for Controller {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.kill();
            // This is only prompt root cancellation. The daemon recursively
            // cleans the enclosing Windows Job before releasing its Lease;
            // a Linux bootstrap obligation still requires its separate seal.
        }
    }
}
