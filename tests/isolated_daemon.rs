#![cfg(windows)]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use stillyard::{
    Client, DaemonSnapshot, DoctorCheckStatus, DoctorSnapshot, EnvironmentSpec, Error,
    GpuProviderConfig, HostConfig, HostObservationConfig, JobId, JobSpec, LogStream, ProcessRules,
    QuietDetector, QuietPolicy, ResourceCapacities, ResourceClaims, RetryPolicy, SPEC_VERSION,
    StdinSpec, SubmitOptions,
};
use uuid::Uuid;

struct ChildGuard(Option<Child>);

impl ChildGuard {
    fn new(child: Child) -> Self {
        Self(Some(child))
    }

    fn child_mut(&mut self) -> &mut Child {
        self.0.as_mut().expect("child guard is populated")
    }

    fn kill_and_wait(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        self.kill_and_wait();
    }
}

fn spawn_daemon(executable: &Path, store: &Path, endpoint: &str) -> ChildGuard {
    ChildGuard::new(
        Command::new(executable)
            .args(["--endpoint", endpoint, "daemon", "--store"])
            .arg(store)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    )
}

fn connect(executable: &Path, endpoint: &str) -> Client {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match Client::builder()
            .endpoint(endpoint)
            .daemon_executable(executable)
            .auto_start(false)
            .connect(Instant::now() + Duration::from_millis(250), None)
        {
            Ok(client) => return client,
            Err(Error::Unavailable(_) | Error::DeadlineElapsed) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(25));
            }
            Err(error) => panic!("isolated daemon did not become ready: {error}"),
        }
    }
}

fn wait_for_exit(child: &mut Child, timeout: Duration) -> ExitStatus {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        assert!(Instant::now() < deadline, "contending daemon did not exit");
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn copied_daemon(root: &Path) -> PathBuf {
    let source = PathBuf::from(env!("CARGO_BIN_EXE_stillyard"));
    let pinned_dir = root.join("pinned-revision");
    std::fs::create_dir_all(&pinned_dir).unwrap();
    let pinned = pinned_dir.join("stillyard.exe");
    std::fs::copy(source, &pinned).unwrap();
    pinned
}

fn build_nvml_generation_fixture(runtime: &Path) {
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("nvml_generation_guard.c");
    let output = runtime.join("nvml.dll");
    let compile = Command::new("cl.exe")
        .current_dir(runtime)
        .args(["/nologo", "/LD", "/O2"])
        .arg(source)
        .arg("/link")
        .arg(format!("/OUT:{}", output.display()))
        .output()
        .unwrap();
    assert!(
        compile.status.success(),
        "NVML fixture build failed:\n{}\n{}",
        String::from_utf8_lossy(&compile.stdout),
        String::from_utf8_lossy(&compile.stderr)
    );
    assert!(output.is_file());
}

fn canary_daemon(root: &Path) -> (PathBuf, PathBuf) {
    let executable = root.join("daemon-canary.cmd");
    let marker = root.join("daemon-canary-invoked.txt");
    std::fs::write(
        &executable,
        format!("@echo invoked>\"{}\"\r\n@exit /b 91\r\n", marker.display()),
    )
    .unwrap();
    (executable, marker)
}

fn durable_id<T: std::str::FromStr>(store: Uuid) -> T
where
    T::Err: std::fmt::Debug,
{
    format!("{store}~{}", Uuid::now_v7()).parse().unwrap()
}

#[test]
fn daemon_instance_tuple_accepts_mixed_sources_and_rejects_singletons() {
    let temp = tempfile::tempdir().unwrap();
    let pinned = copied_daemon(temp.path());

    for (name, args, store_env, endpoint_env) in [
        (
            "cli-store-only",
            vec![
                "daemon".to_owned(),
                "--store".to_owned(),
                temp.path().join("rejected-cli-store").display().to_string(),
            ],
            None,
            None,
        ),
        (
            "cli-endpoint-only",
            vec![
                "--endpoint".to_owned(),
                format!(r"\\.\pipe\stillyard-rejected-{}", Uuid::now_v7()),
                "daemon".to_owned(),
            ],
            None,
            None,
        ),
        (
            "env-store-only",
            vec!["daemon".to_owned()],
            Some(temp.path().join("rejected-env-store")),
            None,
        ),
        (
            "env-endpoint-only",
            vec!["daemon".to_owned()],
            None,
            Some(format!(r"\\.\pipe\stillyard-rejected-{}", Uuid::now_v7())),
        ),
    ] {
        let mut command = Command::new(&pinned);
        command
            .args(args)
            .env_remove("STILLYARD_STORE")
            .env_remove("STILLYARD_ENDPOINT")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        if let Some(store) = store_env {
            command.env("STILLYARD_STORE", store);
        }
        if let Some(endpoint) = endpoint_env {
            command.env("STILLYARD_ENDPOINT", endpoint);
        }
        assert!(!command.status().unwrap().success(), "{name}");
    }
    assert!(!temp.path().join("rejected-cli-store").exists());
    assert!(!temp.path().join("rejected-env-store").exists());

    let store_from_cli = temp.path().join("store-from-cli");
    let endpoint_from_env = format!(r"\\.\pipe\stillyard-mixed-{}", Uuid::now_v7());
    let mut cli_store = ChildGuard::new(
        Command::new(&pinned)
            .args(["daemon", "--store"])
            .arg(&store_from_cli)
            .env("STILLYARD_ENDPOINT", &endpoint_from_env)
            .env_remove("STILLYARD_STORE")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    connect(&pinned, &endpoint_from_env);
    cli_store.kill_and_wait();

    let store_from_env = temp.path().join("store-from-env");
    let endpoint_from_cli = format!(r"\\.\pipe\stillyard-mixed-{}", Uuid::now_v7());
    let mut cli_endpoint = ChildGuard::new(
        Command::new(&pinned)
            .args(["--endpoint", &endpoint_from_cli, "daemon"])
            .env("STILLYARD_STORE", &store_from_env)
            .env_remove("STILLYARD_ENDPOINT")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    connect(&pinned, &endpoint_from_cli);
    cli_endpoint.kill_and_wait();
}

#[test]
fn pinned_isolated_daemons_coexist_and_own_both_coordinates() {
    let temp = tempfile::tempdir().unwrap();
    let pinned = copied_daemon(temp.path());
    let store_a = temp.path().join("store-a");
    let store_b = temp.path().join("store-b");
    let store_c = temp.path().join("store-c");
    let endpoint_a = format!(r"\\.\pipe\stillyard-isolated-a-{}", Uuid::now_v7());
    let endpoint_b = format!(r"\\.\pipe\stillyard-isolated-b-{}", Uuid::now_v7());
    let endpoint_c = format!(r"\\.\pipe\stillyard-isolated-c-{}", Uuid::now_v7());
    let (canary, canary_marker) = canary_daemon(temp.path());

    let mut daemon_a = spawn_daemon(&pinned, &store_a, &endpoint_a);
    let _daemon_b = spawn_daemon(&pinned, &store_b, &endpoint_b);
    let client_a = connect(&pinned, &endpoint_a);
    let client_b = connect(&pinned, &endpoint_b);
    let status_a = client_a
        .daemon_status(Instant::now() + Duration::from_secs(2), None)
        .unwrap();
    let status_b = client_b
        .daemon_status(Instant::now() + Duration::from_secs(2), None)
        .unwrap();
    assert_eq!(status_a.endpoint, endpoint_a);
    assert_eq!(status_b.endpoint, endpoint_b);
    assert_eq!(
        status_a.store_path,
        std::fs::canonicalize(&store_a).unwrap()
    );
    assert_eq!(
        status_b.store_path,
        std::fs::canonicalize(&store_b).unwrap()
    );
    assert_ne!(status_a.store_uuid, status_b.store_uuid);
    let doctor = client_b
        .doctor(None, None, Instant::now() + Duration::from_secs(2), None)
        .unwrap();
    assert_eq!(doctor.daemon, status_b);
    assert!(doctor.daemon.process_identity.is_some());
    assert!(doctor.host.host_id.is_some());
    assert!(doctor.host.boot_id.is_some());
    let cli_doctor = Command::new(&pinned)
        .args(["--endpoint", &endpoint_b, "doctor", "--json"])
        .output()
        .unwrap();
    assert!(
        cli_doctor.status.success(),
        "doctor CLI failed: {}",
        String::from_utf8_lossy(&cli_doctor.stderr)
    );
    let cli_doctor: DoctorSnapshot = serde_json::from_slice(&cli_doctor.stdout).unwrap();
    assert_eq!(cli_doctor.daemon, doctor.daemon);
    assert_eq!(cli_doctor.store, doctor.store);

    let nested = JobSpec {
        spec_version: SPEC_VERSION,
        executable: pinned.clone(),
        args: vec!["daemon-status".into()],
        working_directory: temp.path().to_path_buf(),
        stdin: StdinSpec::Eof,
        environment: EnvironmentSpec::default(),
        resources: ResourceClaims::default(),
        observed: None,
        conditions: Vec::new(),
        retry: RetryPolicy::default(),
        postconditions: Vec::new(),
        labels: Vec::new(),
        expected_duration_seconds: Some(1),
        timeout_seconds: Some(10),
        quiet: None,
        artifacts: Vec::new(),
        child_submission_policy: None,
    };
    let receipt = client_b
        .submit(
            nested,
            &SubmitOptions::new(Uuid::now_v7()),
            Instant::now() + Duration::from_secs(5),
            None,
        )
        .unwrap();
    let nested_snapshot = client_b
        .wait(
            receipt.job_id,
            Instant::now() + Duration::from_secs(10),
            None,
        )
        .unwrap();
    let output = client_b
        .logs(
            receipt.job_id,
            LogStream::Stdout,
            0,
            64 * 1024,
            Instant::now() + Duration::from_secs(2),
            None,
        )
        .unwrap();
    let nested_stderr = client_b
        .logs(
            receipt.job_id,
            LogStream::Stderr,
            0,
            64 * 1024,
            Instant::now() + Duration::from_secs(2),
            None,
        )
        .unwrap();
    assert_eq!(
        nested_snapshot.outcome,
        Some(stillyard::JobOutcome::Succeeded),
        "nested CLI failed: {}",
        String::from_utf8_lossy(&nested_stderr.bytes)
    );
    let nested_status: DaemonSnapshot = serde_json::from_slice(&output.bytes).unwrap();
    assert_eq!(nested_status.endpoint, endpoint_b);
    assert_eq!(nested_status.store_uuid, status_b.store_uuid);

    let foreign: JobId = durable_id(status_a.store_uuid);
    assert!(matches!(
        client_b.status(foreign, Instant::now() + Duration::from_secs(2), None),
        Err(Error::NotFound { detail })
            if detail == format!("not found: foreign durable ID from store {}", status_a.store_uuid)
    ));
    let wrong_image = Client::builder()
        .endpoint(&endpoint_a)
        .daemon_executable(&canary)
        .connect(Instant::now() + Duration::from_secs(2), None)
        .unwrap_err();
    let expected_image = std::fs::canonicalize(&canary).unwrap();
    let actual_image = std::fs::canonicalize(&pinned).unwrap();
    assert!(matches!(
        wrong_image,
        Error::Protocol(detail)
            if detail == format!(
                "named-pipe server image mismatch: expected {}, found {}",
                expected_image.display(),
                actual_image.display()
            )
    ));
    assert!(
        !canary_marker.exists(),
        "wrong-image rejection attempted auto-start"
    );

    let mut same_endpoint = spawn_daemon(&pinned, &store_c, &endpoint_a);
    assert!(!wait_for_exit(same_endpoint.child_mut(), Duration::from_secs(3)).success());
    let mut same_store = spawn_daemon(&pinned, &store_a, &endpoint_c);
    assert!(!wait_for_exit(same_store.child_mut(), Duration::from_secs(3)).success());

    let helper = std::env::current_exe().unwrap();
    let outer_store = Uuid::now_v7();
    let parent_job: JobId = durable_id(outer_store);
    let parent_attempt: stillyard::AttemptId = durable_id(outer_store);
    let parent_invocation: stillyard::InvocationId = durable_id(outer_store);
    let helper_status = Command::new(&helper)
        .args(["--ignored", "--exact", "isolated_client_helper"])
        .env("ISOLATED_TARGET_ENDPOINT", &endpoint_b)
        .env("ISOLATED_DAEMON_EXECUTABLE", &pinned)
        .env("STILLYARD_ENDPOINT", &endpoint_a)
        .env("STILLYARD_JOB_ID", parent_job.to_string())
        .env("STILLYARD_ATTEMPT", parent_attempt.to_string())
        .env("STILLYARD_INVOCATION_ID", parent_invocation.to_string())
        .status()
        .unwrap();
    assert!(helper_status.success());

    daemon_a.kill_and_wait();
    let _replacement = spawn_daemon(&pinned, &store_a, &endpoint_a);
    let reopened = connect(&pinned, &endpoint_a)
        .daemon_status(Instant::now() + Duration::from_secs(2), None)
        .unwrap();
    assert_eq!(reopened.store_uuid, status_a.store_uuid);
}

#[test]
#[ignore = "launched as a scoped managed-environment client helper"]
fn isolated_client_helper() {
    let endpoint = std::env::var("ISOLATED_TARGET_ENDPOINT").unwrap();
    let daemon = PathBuf::from(std::env::var_os("ISOLATED_DAEMON_EXECUTABLE").unwrap());
    let client = Client::builder()
        .endpoint(&endpoint)
        .daemon_executable(daemon)
        .auto_start(false)
        .connect(Instant::now() + Duration::from_secs(2), None)
        .unwrap();
    let context = client
        .submission_context(Instant::now() + Duration::from_secs(2), None)
        .unwrap();
    assert_eq!(context.parent, None);
    assert_eq!(
        client
            .daemon_status(Instant::now() + Duration::from_secs(2), None)
            .unwrap()
            .endpoint,
        endpoint
    );

    let inherited_endpoint = std::env::var("STILLYARD_ENDPOINT").unwrap();
    let inherited = Client::builder()
        .endpoint(inherited_endpoint)
        .daemon_executable(std::env::var_os("ISOLATED_DAEMON_EXECUTABLE").unwrap())
        .auto_start(false)
        .connect(Instant::now() + Duration::from_secs(2), None)
        .unwrap();
    assert!(matches!(
        inherited.submission_context(Instant::now() + Duration::from_secs(2), None),
        Err(Error::Rejected { code, detail })
            if code == "rejected"
                && detail == "submission rejected: claimed managed parent does not match daemon-held OS containment"
    ));
}

#[test]
fn absent_custom_endpoints_never_auto_start() {
    let temp = tempfile::tempdir().unwrap();
    let (canary, marker) = canary_daemon(temp.path());
    let helper = std::env::current_exe().unwrap();

    for mode in ["builder", "environment"] {
        let endpoint = format!(r"\\.\pipe\stillyard-absent-{}", Uuid::now_v7());
        let mut command = Command::new(&helper);
        command
            .args(["--ignored", "--exact", "no_autostart_helper"])
            .env("ISOLATED_ENDPOINT_MODE", mode)
            .env("ISOLATED_TARGET_ENDPOINT", &endpoint)
            .env("ISOLATED_DAEMON_EXECUTABLE", &canary)
            .env("ISOLATED_CANARY_MARKER", &marker)
            .env_remove("STILLYARD_STORE")
            .env_remove("STILLYARD_ENDPOINT")
            .env_remove("STILLYARD_JOB_ID")
            .env_remove("STILLYARD_ATTEMPT")
            .env_remove("STILLYARD_INVOCATION_ID")
            .env_remove("STILLYARD_ROLE");
        if mode == "environment" {
            command.env("STILLYARD_ENDPOINT", &endpoint);
        }
        assert!(command.status().unwrap().success(), "mode={mode}");
        assert!(!marker.exists(), "mode={mode} attempted daemon auto-start");
    }
}

#[test]
fn external_nvml_generation_change_never_releases_the_suspended_child() {
    let temp = tempfile::tempdir().unwrap();
    let runtime = temp.path().join("runtime");
    let store = temp.path().join("store");
    std::fs::create_dir_all(&runtime).unwrap();
    std::fs::create_dir_all(&store).unwrap();
    let source_executable = PathBuf::from(env!("CARGO_BIN_EXE_stillyard"));
    let executable = runtime.join("stillyard.exe");
    std::fs::copy(source_executable, &executable).unwrap();
    build_nvml_generation_fixture(&runtime);

    let gpu_uuid = "GPU-a1144c26-a15c-cba1-3b7a-870c755ef08a";
    let config = HostConfig {
        resources: ResourceCapacities {
            gpu_slots: 1,
            ..Default::default()
        },
        impact_incompatibilities: Default::default(),
        observation: HostObservationConfig {
            sample_interval_millis: 100,
            quiet_max_sample_gap_millis: 200,
            generation_max_cadence_gap_millis: 500,
            memory_max_sample_age_millis: 500,
            gpu_slot_uuid: Some(gpu_uuid.into()),
            process_rules: ProcessRules::default(),
            pre_release_max_deferrals: 1,
            pre_release_backoff_millis: 100,
            admission_wall_clock_limit_seconds: 10,
            gpu_provider: GpuProviderConfig::Nvml,
            ..Default::default()
        },
    };
    std::fs::write(
        store.join("config.json"),
        serde_json::to_vec_pretty(&config).unwrap(),
    )
    .unwrap();
    let endpoint = format!(r"\\.\pipe\stillyard-a05-{}", Uuid::now_v7());
    let _daemon = spawn_daemon(&executable, &store, &endpoint);
    let client = connect(&executable, &endpoint);
    let doctor_output = Command::new(&executable)
        .args(["--endpoint", &endpoint, "doctor", "--json"])
        .output()
        .unwrap();
    assert!(
        doctor_output.status.success(),
        "fixture doctor failed: {}",
        String::from_utf8_lossy(&doctor_output.stderr)
    );
    let doctor: DoctorSnapshot = serde_json::from_slice(&doctor_output.stdout).unwrap();
    assert!(doctor.coverage.iter().any(|coverage| {
        coverage.detector == "gpu_placement" && coverage.status == DoctorCheckStatus::Pass
    }));

    let system_root = PathBuf::from(std::env::var_os("SystemRoot").unwrap());
    let child = runtime.join("stillyard-a05-child.exe");
    std::fs::copy(system_root.join("System32").join("cmd.exe"), &child).unwrap();
    let marker = temp.path().join("forbidden-release.txt");
    let job = JobSpec {
        spec_version: SPEC_VERSION,
        executable: child,
        args: vec![
            "/d".into(),
            "/c".into(),
            format!("echo released>\"{}\"", marker.display()),
        ],
        working_directory: temp.path().to_path_buf(),
        stdin: StdinSpec::Eof,
        environment: EnvironmentSpec::default(),
        resources: ResourceClaims {
            gpu_slots: Some(1),
            ..Default::default()
        },
        observed: None,
        conditions: Vec::new(),
        retry: RetryPolicy::default(),
        postconditions: Vec::new(),
        labels: Vec::new(),
        expected_duration_seconds: Some(1),
        timeout_seconds: Some(10),
        quiet: Some(QuietPolicy {
            stable_seconds: 1,
            max_sample_age_seconds: 1,
            wait_budget_seconds: 5,
            detectors: vec![QuietDetector::GpuUtilization {
                gpu_uuid: gpu_uuid.into(),
                max_percent: 0,
            }],
        }),
        artifacts: Vec::new(),
        child_submission_policy: None,
    };
    let spec_path = temp.path().join("strict-job.json");
    std::fs::write(&spec_path, serde_json::to_vec_pretty(&job).unwrap()).unwrap();
    let submit = Command::new(&executable)
        .args(["--endpoint", &endpoint, "submit", "--spec"])
        .arg(&spec_path)
        .args(["--wait", "--deadline-seconds", "20"])
        .output()
        .unwrap();
    assert!(
        !submit.status.success(),
        "generation-contaminated strict Job unexpectedly succeeded"
    );
    assert!(
        !marker.exists(),
        "A-05: user code was released from stale reservation evidence"
    );
    let jobs = client
        .list(
            stillyard::JobSelector::default(),
            None,
            10,
            Instant::now() + Duration::from_secs(2),
            None,
        )
        .unwrap();
    let summary = jobs.jobs.last().expect("strict fixture Job is retained");
    let snapshot = client
        .status(
            summary.job_id,
            Instant::now() + Duration::from_secs(2),
            None,
        )
        .unwrap();
    assert_eq!(snapshot.outcome, Some(stillyard::JobOutcome::Failed));
    assert_eq!(
        snapshot.attempts[0].reason_code.as_deref(),
        Some("quiet_unattainable")
    );
    assert!(
        snapshot
            .admission
            .as_ref()
            .is_some_and(|admission| admission.deferral_count >= 1),
        "A-05 must prove a reserved suspended child reached final-sample deferral"
    );
}

#[test]
#[ignore = "launched as an absent-custom-endpoint client helper"]
fn no_autostart_helper() {
    let endpoint = std::env::var("ISOLATED_TARGET_ENDPOINT").unwrap();
    let executable = PathBuf::from(std::env::var_os("ISOLATED_DAEMON_EXECUTABLE").unwrap());
    let marker = PathBuf::from(std::env::var_os("ISOLATED_CANARY_MARKER").unwrap());
    let mut builder = Client::builder().daemon_executable(executable);
    if std::env::var("ISOLATED_ENDPOINT_MODE").unwrap() == "builder" {
        builder = builder.endpoint(&endpoint);
    }
    assert!(matches!(
        builder.connect(Instant::now() + Duration::from_millis(500), None),
        Err(Error::Unavailable(detail))
            if detail.starts_with("auto-start is unavailable for an explicit endpoint; connection failed:")
    ));
    assert!(!marker.exists());
}
