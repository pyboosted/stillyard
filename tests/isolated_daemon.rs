#![cfg(windows)]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use stillyard::{
    BatchMember, BatchSpec, Client, DaemonSnapshot, DoctorCheckStatus, DoctorSnapshot,
    EnsureOptions, EnsureOutcome, EnsureReport, EnsuredJob, EnvironmentSpec, Error, ExitSource,
    GpuProviderConfig, HostConfig, HostObservationConfig, JobId, JobSpec, LogStream, ProcessRules,
    QuietDetector, QuietPolicy, ResourceCapacities, ResourceClaims, RetryPolicy, SPEC_VERSION,
    StdinSpec, SubmitOptions, WaitOutcome, WaitReport,
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

fn command_spec(root: &Path, command: &str) -> JobSpec {
    JobSpec {
        spec_version: SPEC_VERSION,
        executable: PathBuf::from(r"C:\Windows\System32\cmd.exe"),
        args: vec!["/d".into(), "/c".into(), command.into()],
        working_directory: root.to_path_buf(),
        stdin: StdinSpec::Eof,
        environment: EnvironmentSpec::default(),
        resources: ResourceClaims::default(),
        observed: None,
        conditions: Vec::new(),
        retry: RetryPolicy::default(),
        postconditions: Vec::new(),
        labels: Vec::new(),
        expected_duration_seconds: Some(1),
        timeout_seconds: Some(30),
        quiet: None,
        artifacts: Vec::new(),
        child_submission_policy: None,
    }
}

fn seed_unresolved_incidents(store: &Path, count: u64) {
    let connection = rusqlite::Connection::open(store.join("stillyard.sqlite3")).unwrap();
    connection
        .pragma_update(None, "foreign_keys", true)
        .unwrap();
    let spec = JobSpec {
        spec_version: SPEC_VERSION,
        executable: PathBuf::from(r"C:\Windows\System32\cmd.exe"),
        args: vec!["/d".into(), "/c".into(), "exit 0".into()],
        working_directory: store.to_path_buf(),
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
    let spec_json = serde_json::to_string(&spec).unwrap();
    let transaction = connection.unchecked_transaction().unwrap();
    for sequence in 1..=count {
        let submission = Uuid::now_v7().to_string();
        let job = Uuid::now_v7().to_string();
        let attempt = Uuid::now_v7().to_string();
        let invocation = Uuid::now_v7().to_string();
        let containment = Uuid::now_v7().to_string();
        transaction
            .execute(
                "INSERT INTO submissions(
                    id, scope, idempotency_key, payload_hash, state, spec_json, kind, created_ms
                 ) VALUES (?1, 'unmanaged', ?2, 'fixture', 'accepted', ?3, 'job', ?4)",
                rusqlite::params![submission, Uuid::now_v7().to_string(), spec_json, sequence],
            )
            .unwrap();
        transaction
            .execute(
                "INSERT INTO jobs(
                    id, submission_id, state, outcome, spec_json, claims_json,
                    attempt_id, invocation_id, containment_id, accepted_ms, finished_ms
                 ) VALUES (?1, ?2, 'final', 'interrupted', ?3, '{}', ?4, ?5, ?6, ?7, ?7)",
                rusqlite::params![
                    job,
                    submission,
                    spec_json,
                    attempt,
                    invocation,
                    containment,
                    sequence
                ],
            )
            .unwrap();
        transaction
            .execute(
                "INSERT INTO attempts(
                    id, job_id, state, attempt_index, verdict, created_ms, finished_ms
                 ) VALUES (?1, ?2, 'settled', 1, 'interrupted', ?3, ?3)",
                rusqlite::params![attempt, job, sequence],
            )
            .unwrap();
        transaction
            .execute(
                "INSERT INTO invocations(
                    id, attempt_id, role, role_index, state, finished_ms
                 ) VALUES (?1, ?2, 'primary', 0, 'resolved', ?3)",
                rusqlite::params![invocation, attempt, sequence],
            )
            .unwrap();
        transaction
            .execute(
                "INSERT INTO containments(
                    id, invocation_id, state, strength, incident_sequence, reason_code,
                    detail, opened_ms, retained_claims_json
                 ) VALUES (?1, ?2, 'uncertain', 'windows_job_object', ?3,
                           'rpc_fixture', 'snapshot pagination fixture', ?3, '{}')",
                rusqlite::params![containment, invocation, sequence],
            )
            .unwrap();
    }
    transaction.commit().unwrap();
}

#[test]
fn doctor_complete_crosses_transport_pages_and_restart_rejects_old_cursor() {
    let temp = tempfile::tempdir().unwrap();
    let pinned = copied_daemon(temp.path());
    let store = temp.path().join("store");
    let endpoint = format!(r"\\.\pipe\stillyard-doctor-pages-{}", Uuid::now_v7());

    let mut daemon = spawn_daemon(&pinned, &store, &endpoint);
    let _ = connect(&pinned, &endpoint);
    daemon.kill_and_wait();
    seed_unresolved_incidents(&store, 257);

    daemon = spawn_daemon(&pinned, &store, &endpoint);
    let client = connect(&pinned, &endpoint);
    let first = client
        .doctor(
            None,
            Some(113),
            Instant::now() + Duration::from_secs(10),
            None,
        )
        .unwrap();
    assert_eq!(first.incidents.total_unresolved, 257);
    assert_eq!(first.incidents.incidents.len(), 113);
    let old_cursor = first.incidents.next_cursor.unwrap();

    let complete = client
        .doctor_complete(Instant::now() + Duration::from_secs(10), None)
        .unwrap();
    assert_eq!(complete.total_unresolved, 257);
    assert_eq!(complete.incidents.len(), 257);
    assert_eq!(
        complete
            .incidents
            .iter()
            .map(|incident| incident.incident_id)
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        257
    );

    daemon.kill_and_wait();
    let _restarted_daemon = spawn_daemon(&pinned, &store, &endpoint);
    let restarted = connect(&pinned, &endpoint);
    assert!(matches!(
        restarted.doctor(
            Some(old_cursor),
            Some(113),
            Instant::now() + Duration::from_secs(10),
            None,
        ),
        Err(Error::ViewStale { detail }) if detail.contains("generation")
    ));
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
fn ensure_concurrent_callers_converge_and_conflict_is_typed() {
    let temp = tempfile::tempdir().unwrap();
    let pinned = copied_daemon(temp.path());
    let store = temp.path().join("ensure-store");
    let endpoint = format!(r"\\.\pipe\stillyard-ensure-{}", Uuid::now_v7());
    let _daemon = spawn_daemon(&pinned, &store, &endpoint);
    let client = connect(&pinned, &endpoint);
    let key = Uuid::now_v7();
    let spec = command_spec(temp.path(), "ping -n 2 127.0.0.1 >nul");
    let result_file = temp.path().join("concurrent-ensure.result.json");
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
    let callers = (0..2)
        .map(|_| {
            let client = client.clone();
            let spec = spec.clone();
            let barrier = std::sync::Arc::clone(&barrier);
            let result_file = result_file.clone();
            std::thread::spawn(move || {
                barrier.wait();
                client
                    .ensure_job(
                        spec,
                        &EnsureOptions::new(key).with_result_file(result_file),
                        Instant::now() + Duration::from_secs(10),
                        None,
                    )
                    .unwrap()
            })
        })
        .collect::<Vec<_>>();
    barrier.wait();
    let outcomes = callers
        .into_iter()
        .map(|caller| caller.join().unwrap())
        .collect::<Vec<_>>();
    let job_ids = outcomes
        .iter()
        .map(|outcome| match outcome {
            EnsureOutcome::Accepted(ensured) | EnsureOutcome::Final(ensured) => {
                ensured.receipt.job_id
            }
            other => panic!("unexpected concurrent ensure outcome: {other:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(job_ids[0], job_ids[1]);

    let process_spec_path = temp.path().join("concurrent-process.json");
    let process_spec = command_spec(temp.path(), "ping -n 5 127.0.0.1 >nul");
    std::fs::write(
        &process_spec_path,
        serde_json::to_vec_pretty(&process_spec).unwrap(),
    )
    .unwrap();
    let process_key = Uuid::now_v7().to_string();
    let launch = |spec_path: &Path, key: &str| {
        Command::new(&pinned)
            .args(["--endpoint", &endpoint, "ensure", "--spec"])
            .arg(spec_path)
            .args(["--idempotency-key", key])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap()
    };
    let first_process = launch(&process_spec_path, &process_key);
    let second_process = launch(&process_spec_path, &process_key);
    let process_outcomes = [
        first_process.wait_with_output().unwrap(),
        second_process.wait_with_output().unwrap(),
    ];
    let process_jobs = process_outcomes.map(|output| {
        let report: EnsureReport<EnsuredJob> = serde_json::from_slice(&output.stdout).unwrap();
        match report.outcome {
            EnsureOutcome::Accepted(ensured) | EnsureOutcome::Final(ensured) => {
                ensured.receipt.job_id
            }
            other => panic!("unexpected process ensure outcome: {other:?}"),
        }
    });
    assert_eq!(process_jobs[0], process_jobs[1]);

    let competing_a = temp.path().join("competing-a.json");
    let competing_b = temp.path().join("competing-b.json");
    std::fs::write(
        &competing_a,
        serde_json::to_vec_pretty(&command_spec(temp.path(), "exit 0")).unwrap(),
    )
    .unwrap();
    std::fs::write(
        &competing_b,
        serde_json::to_vec_pretty(&command_spec(temp.path(), "exit 9")).unwrap(),
    )
    .unwrap();
    let competing_key = Uuid::now_v7().to_string();
    let first_process = launch(&competing_a, &competing_key);
    let second_process = launch(&competing_b, &competing_key);
    let competing = [
        first_process.wait_with_output().unwrap(),
        second_process.wait_with_output().unwrap(),
    ]
    .map(|output| {
        let report: EnsureReport<EnsuredJob> = serde_json::from_slice(&output.stdout).unwrap();
        (output.status.code(), report.outcome)
    });
    assert_eq!(
        competing
            .iter()
            .filter(|(_, outcome)| matches!(
                outcome,
                EnsureOutcome::Accepted(_) | EnsureOutcome::Final(_)
            ))
            .count(),
        1
    );
    assert_eq!(
        competing
            .iter()
            .filter(|(_, outcome)| matches!(outcome, EnsureOutcome::Conflict { .. }))
            .count(),
        1
    );
    assert!(competing.iter().any(|(code, _)| *code == Some(27)));

    let conflict = client
        .ensure_job(
            command_spec(temp.path(), "exit 7"),
            &EnsureOptions::new(key),
            Instant::now() + Duration::from_secs(5),
            None,
        )
        .unwrap();
    assert!(matches!(
        conflict,
        EnsureOutcome::Conflict {
            existing_payload_hash,
            requested_payload_hash,
        } if existing_payload_hash != requested_payload_hash
    ));

    let batch_key = Uuid::now_v7();
    let batch = BatchSpec {
        spec_version: SPEC_VERSION,
        jobs: vec![
            BatchMember {
                name: "first".into(),
                spec: command_spec(temp.path(), "exit 0"),
                dependencies: Vec::new(),
            },
            BatchMember {
                name: "second".into(),
                spec: command_spec(temp.path(), "exit 0"),
                dependencies: Vec::new(),
            },
        ],
    };
    let first_batch = client
        .ensure_batch(
            batch.clone(),
            &EnsureOptions::new(batch_key),
            Instant::now() + Duration::from_secs(5),
            None,
        )
        .unwrap();
    let replayed_batch = client
        .ensure_batch(
            batch,
            &EnsureOptions::new(batch_key),
            Instant::now() + Duration::from_secs(5),
            None,
        )
        .unwrap();
    let batch_id = |outcome: &EnsureOutcome<stillyard::EnsuredBatch>| match outcome {
        EnsureOutcome::Accepted(ensured) | EnsureOutcome::Final(ensured) => {
            ensured.receipt.batch_id
        }
        other => panic!("unexpected Batch ensure outcome: {other:?}"),
    };
    assert_eq!(batch_id(&first_batch), batch_id(&replayed_batch));
}

#[test]
fn typed_wait_and_cli_keep_terminal_root_exit_25_distinct_from_pending() {
    let temp = tempfile::tempdir().unwrap();
    let pinned = copied_daemon(temp.path());
    let store = temp.path().join("wait-store");
    let endpoint = format!(r"\\.\pipe\stillyard-wait-{}", Uuid::now_v7());
    let _daemon = spawn_daemon(&pinned, &store, &endpoint);
    let client = connect(&pinned, &endpoint);

    let slow = client
        .submit(
            command_spec(temp.path(), "ping -n 3 127.0.0.1 >nul"),
            &SubmitOptions::new(Uuid::now_v7()),
            Instant::now() + Duration::from_secs(5),
            None,
        )
        .unwrap();
    assert!(matches!(
        client.wait_outcome(
            slow.job_id,
            Instant::now() + Duration::from_millis(20),
            None,
        ),
        WaitOutcome::Pending { .. }
    ));
    assert!(matches!(
        client.wait_outcome(slow.job_id, Instant::now() + Duration::from_secs(10), None,),
        WaitOutcome::Final { .. }
    ));

    let spec_path = temp.path().join("exit-25.json");
    std::fs::write(
        &spec_path,
        serde_json::to_vec_pretty(&command_spec(temp.path(), "exit 25")).unwrap(),
    )
    .unwrap();
    let key = Uuid::now_v7().to_string();
    let cli = Command::new(&pinned)
        .args(["--endpoint", &endpoint, "ensure", "--spec"])
        .arg(&spec_path)
        .args([
            "--idempotency-key",
            &key,
            "--wait",
            "--deadline-seconds",
            "10",
        ])
        .output()
        .unwrap();
    assert_eq!(cli.status.code(), Some(20));
    let report: EnsureReport<EnsuredJob> = serde_json::from_slice(&cli.stdout).unwrap();
    assert_eq!(report.exit_source, ExitSource::Scheduler);
    assert_eq!(report.exit_code, 20);
    let EnsureOutcome::Final(ensured) = report.outcome else {
        panic!("terminal exit 25 was not final");
    };
    let job_id = ensured.receipt.job_id;
    assert_eq!(
        ensured.snapshot.expect("final snapshot").root_exit_code,
        Some(25)
    );

    for (root_code, expected_source, expected_status) in
        [(0, ExitSource::Scheduler, 0), (7, ExitSource::Process, 7)]
    {
        let spec_path = temp.path().join(format!("exit-{root_code}.json"));
        std::fs::write(
            &spec_path,
            serde_json::to_vec_pretty(&command_spec(temp.path(), &format!("exit {root_code}")))
                .unwrap(),
        )
        .unwrap();
        let key = Uuid::now_v7().to_string();
        let output = Command::new(&pinned)
            .args(["--endpoint", &endpoint, "ensure", "--spec"])
            .arg(&spec_path)
            .args([
                "--idempotency-key",
                &key,
                "--wait",
                "--deadline-seconds",
                "10",
            ])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(expected_status));
        let report: EnsureReport<EnsuredJob> = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(report.exit_source, expected_source);
        assert!(matches!(
            report.outcome,
            EnsureOutcome::Final(EnsuredJob {
                snapshot: Some(snapshot),
                ..
            }) if snapshot.root_exit_code == Some(root_code)
        ));
    }

    let waited = Command::new(&pinned)
        .args(["--endpoint", &endpoint, "wait", &job_id.to_string()])
        .output()
        .unwrap();
    assert_eq!(waited.status.code(), Some(20));
    let waited: WaitReport = serde_json::from_slice(&waited.stdout).unwrap();
    assert_eq!(waited.exit_source, ExitSource::Scheduler);
    assert_eq!(waited.exit_code, 20);
    assert!(matches!(
        waited.outcome,
        WaitOutcome::Final {
            root_exit_code: Some(25),
            ..
        }
    ));
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
