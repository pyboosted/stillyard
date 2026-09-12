//! Transitional bootstrap path. The final attached-domain protocol replaces this
//! whole-authority interlock with scoped Grants; neither path frees on bridge EOF.

use crate::{BootstrapBinding, BootstrapProof, BootstrapWork, Client, Error, Result};
use sha2::{Digest, Sha256};

pub(crate) fn validate_binding(binding: &BootstrapBinding) -> std::io::Result<()> {
    let work = &binding.work;
    let serialized = serde_json::to_vec(work)?;
    if serialized.len() > 8192
        || !(1..=86400).contains(&work.timeout_seconds)
        || work.operation_id != binding.parent.invocation_id.entity_uuid()
        || work.distribution.is_empty()
        || work.user.is_empty()
        || !work.executable.starts_with('/')
        || !work.working_directory.starts_with('/')
        || [
            &work.distribution,
            &work.user,
            &work.executable,
            &work.working_directory,
        ]
        .into_iter()
        .chain(work.args.iter())
        .chain(work.environment.values())
        .any(|value| value.contains('\0'))
        || work
            .environment
            .keys()
            .any(|key| key.is_empty() || key.contains(['=', '\0']))
        || format!("{:x}", Sha256::digest(&serialized)) != binding.request_sha256
    {
        return Err(std::io::Error::other(
            "invalid bootstrap descriptor, identity, or payload hash",
        ));
    }
    Ok(())
}

pub(crate) fn validate_proof(
    binding: &BootstrapBinding,
    proof: &BootstrapProof,
) -> std::io::Result<()> {
    if proof.operation_id != binding.work.operation_id
        || proof.request_sha256 != binding.request_sha256
        || proof.phase != "sealed_empty"
        || uuid::Uuid::parse_str(&proof.boot_id).is_err()
        || !proof.cgroup_path.starts_with("/sys/fs/cgroup/")
        || !proof.cgroup_path.ends_with("/work")
        || proof.cgroup_path.len() > 4096
        || proof.cgroup_path.contains('\0')
        || proof.cgroup_inode == 0
        || !["exited", "canceled", "timed_out", "intermediary_lost"]
            .contains(&proof.termination.as_str())
    {
        return Err(std::io::Error::other(
            "bootstrap proof identity or seal is invalid",
        ));
    }
    Ok(())
}

impl Client {
    /// Execute transitional WSL bootstrap work from this daemon's authenticated
    /// native primary. A durable obligation is acknowledged before Linux is called.
    #[cfg(windows)]
    pub fn run_wsl_bootstrap(&self, mut work: BootstrapWork) -> Result<BootstrapProof> {
        use std::time::{Duration, Instant};
        let parent = self
            .submission_context(Instant::now() + Duration::from_secs(10), None)?
            .parent
            .ok_or_else(|| {
                Error::InvalidSpec("bootstrap run requires a native Stillyard Job".into())
            })?;
        if !work.operation_id.is_nil() && work.operation_id != parent.invocation_id.entity_uuid() {
            return Err(Error::InvalidSpec(
                "bootstrap operation identity is daemon-derived".into(),
            ));
        }
        work.operation_id = parent.invocation_id.entity_uuid();
        let request = serde_json::to_string(&work)?;
        let binding = BootstrapBinding {
            parent,
            work,
            request_sha256: format!("{:x}", Sha256::digest(request.as_bytes())),
        };
        validate_binding(&binding)?;
        let state =
            self.arm_bootstrap(binding.clone(), Instant::now() + Duration::from_secs(10))?;
        if !state.holds.iter().any(|hold| {
            hold.id == binding.work.operation_id
                && !hold.released
                && hold.bootstrap.as_ref() == Some(&binding)
        }) {
            return Err(Error::Unavailable(
                "bootstrap was retired or not durably armed; no Linux launch".into(),
            ));
        }
        let proof = invoke_supervisor(&binding, "run", &request, true)?;
        self.seal_bootstrap(proof.clone(), Instant::now() + Duration::from_secs(10))?;
        Ok(proof)
    }

    /// Reconcile a retained obligation using the same embedded trusted inspector.
    /// A missing local record remains uncertain; this never launches user work.
    #[cfg(windows)]
    pub fn reconcile_wsl_bootstrap(&self, operation_id: uuid::Uuid) -> Result<BootstrapProof> {
        use std::time::{Duration, Instant};
        let state = self.authority_status(Instant::now() + Duration::from_secs(10), None)?;
        let hold = state
            .holds
            .iter()
            .find(|hold| hold.id == operation_id)
            .ok_or_else(|| Error::InvalidSpec("unknown bootstrap obligation".into()))?;
        let binding = hold
            .bootstrap
            .as_ref()
            .ok_or_else(|| Error::InvalidSpec("hold is not bootstrap work".into()))?;
        let proof = invoke_supervisor(binding, "inspect", &binding.request_sha256, false)?;
        self.seal_bootstrap(proof.clone(), Instant::now() + Duration::from_secs(10))?;
        Ok(proof)
    }

    #[cfg(not(windows))]
    pub fn run_wsl_bootstrap(&self, _work: BootstrapWork) -> Result<BootstrapProof> {
        Err(Error::UnsupportedPlatform(std::env::consts::OS))
    }

    #[cfg(not(windows))]
    pub fn reconcile_wsl_bootstrap(&self, _operation_id: uuid::Uuid) -> Result<BootstrapProof> {
        Err(Error::UnsupportedPlatform(std::env::consts::OS))
    }
}

#[cfg(windows)]
fn invoke_supervisor(
    binding: &BootstrapBinding,
    mode: &str,
    argument: &str,
    forward_output: bool,
) -> Result<BootstrapProof> {
    use std::io::{BufRead, BufReader, Write};
    use std::process::{Command, Stdio};
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Frame {
        stream: Option<String>,
        bytes: Option<Vec<u8>>,
        proof: Option<BootstrapProof>,
        heartbeat: Option<bool>,
    }
    const SOURCE: &str = include_str!("../scripts/wsl-bootstrap-supervisor.py");
    let code = format!(
        "s={};n={{'__name__':'stillyard_embedded'}};exec(compile(s,'stillyard-bootstrap','exec'),n);n['main'](s)",
        serde_json::to_string(SOURCE)?
    );
    let executable = std::path::PathBuf::from(
        std::env::var_os("SystemRoot").unwrap_or_else(|| r"C:\Windows".into()),
    )
    .join("System32/wsl.exe");
    let mut child = Command::new(executable)
        .args([
            "--distribution",
            &binding.work.distribution,
            "--user",
            &binding.work.user,
            "--cd",
            "/tmp",
            "--exec",
            "/usr/bin/python3",
            "-c",
            &code,
            mode,
            &binding.work.operation_id.to_string(),
            argument,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()?;
    let mut stream = BufReader::new(
        child
            .stdout
            .take()
            .ok_or_else(|| Error::Unavailable("bootstrap stdout missing".into()))?,
    );
    let mut proof = None;
    loop {
        let mut line = Vec::new();
        // Each embedded-helper frame has at most 4096 bytes of output. Bound the
        // receive buffer even when a broken or replaced helper violates framing.
        let mut limited = std::io::Read::take(&mut stream, 32769);
        let count = limited.read_until(b'\n', &mut line)?;
        if count == 0 {
            break;
        }
        if count > 32768 || line.last() != Some(&b'\n') {
            return Err(Error::Protocol(
                "bootstrap frame exceeds limit or is truncated; authority retained".into(),
            ));
        }
        let frame: Frame = serde_json::from_slice(&line)?;
        if let Some(value) = frame.proof {
            validate_proof(binding, &value)?;
            if proof.replace(value).is_some() {
                return Err(Error::Protocol("duplicate bootstrap proof".into()));
            }
        } else if let (Some(name), Some(bytes)) = (frame.stream, frame.bytes) {
            if forward_output {
                match name.as_str() {
                    "stdout" => {
                        std::io::stdout().write_all(&bytes)?;
                        std::io::stdout().flush()?;
                    }
                    "stderr" => {
                        std::io::stderr().write_all(&bytes)?;
                        std::io::stderr().flush()?;
                    }
                    _ => return Err(Error::Protocol("unknown bootstrap log stream".into())),
                }
            }
        } else if frame.heartbeat != Some(true) {
            return Err(Error::Protocol("invalid bootstrap frame".into()));
        }
    }
    if !child.wait()?.success() {
        return Err(Error::Unavailable(
            "Linux bootstrap outcome uncertain; authority retained for reconciliation".into(),
        ));
    }
    proof.ok_or_else(|| {
        Error::Unavailable("Linux bootstrap returned no sealed proof; authority retained".into())
    })
}
