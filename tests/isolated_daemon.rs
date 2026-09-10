#![cfg(windows)]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use stillyard::{
    BatchMember, BatchSpec, Client, ConditionDeadline, ConditionDeadlineOutcome,
    ConditionObservationValue, ConditionPredicate, ConditionSpec, DaemonSnapshot,
    DoctorCheckStatus, DoctorSnapshot, EnsureOptions, EnsureOutcome, EnsureReport, EnsuredJob,
    EnvironmentSpec, Error, ExitClassification, ExitSource, GpuProviderConfig, HostConfig,
    HostObservationConfig, InvocationRole, JobId, JobSpec, LogStream, ProbeCondition, ProcessRules,
    QuietDetector, QuietPolicy, ResourceCapacities, ResourceClaims, RetryPolicy, SPEC_VERSION,
    StdinSpec, SubmitOptions, WaitOutcome, WaitReport,
};
use uuid::Uuid;

#[path = "support/cross_os_reset.rs"]
mod cross_os_reset;

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
    // Legacy fixtures deliberately create a new, empty test authority. This is
    // an owner assertion by the harness, never a production client default.
    // Repeated initialization is idempotent and cannot clear existing holds.
    let client = connect_uninitialized(executable, endpoint);
    client
        .initialize_authority_without_outstanding_work(
            Instant::now() + Duration::from_secs(10),
            None,
        )
        .unwrap();
    client
}

fn connect_uninitialized(executable: &Path, endpoint: &str) -> Client {
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

#[test]
fn persistent_machine_bridge_correlates_frames_and_disconnect_preserves_authority() {
    use stillyard::machine::bridge::*;
    let temp = tempfile::tempdir().unwrap();
    let executable = copied_daemon(temp.path());
    let endpoint = format!(r"\\.\pipe\stillyard-bridge-{}", Uuid::now_v7());
    let _daemon = spawn_daemon(&executable, &temp.path().join("store"), &endpoint);
    let client = connect(&executable, &endpoint);
    let before = client
        .authority_status(Instant::now() + Duration::from_secs(5), None)
        .unwrap();
    let mut bridge = ChildGuard::new(
        Command::new(&executable)
            .args(["--endpoint", &endpoint, "machine", "bridge"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let mut input = bridge.child_mut().stdin.take().unwrap();
    let mut output = bridge.child_mut().stdout.take().unwrap();
    let (send, receive) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        for _ in 0..3 {
            if send
                .send(stillyard::machine::read_frame::<BridgeReply>(&mut output))
                .is_err()
            {
                break;
            }
        }
    });
    for command in [
        BridgeCommand::AuthorityStatus,
        BridgeCommand::Participant {
            domain: stillyard::ExecutionDomainId(Uuid::now_v7()),
        },
        BridgeCommand::AuthorityStatus,
    ] {
        let request = BridgeRequest {
            version: BRIDGE_VERSION,
            protocol_version: 25,
            request_id: Uuid::now_v7(),
            deadline_millis: 5000,
            command,
        };
        stillyard::machine::write_frame(&mut input, &request).unwrap();
        let reply = receive
            .recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap();
        assert_eq!(reply.request_id, request.request_id);
        match request.command {
            BridgeCommand::AuthorityStatus => assert!(
                matches!(reply.outcome,BridgeOutcome::Authority{authority} if *authority==before)
            ),
            _ => assert!(matches!(reply.outcome, BridgeOutcome::Error { .. })),
        }
    }
    drop(input);
    assert!(wait_for_exit(bridge.child_mut(), Duration::from_secs(5)).success());
    reader.join().unwrap();
    assert_eq!(
        client
            .authority_status(Instant::now() + Duration::from_secs(5), None)
            .unwrap(),
        before
    );
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
        priority: stillyard::NEUTRAL_JOB_PRIORITY,
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

#[test]
fn durable_authority_blocks_release_across_restart_database_reset_and_history_loss() {
    let temp = tempfile::tempdir().unwrap();
    let pinned = copied_daemon(temp.path());
    let store = temp.path().join("authority-store");
    let endpoint = format!(r"\\.\pipe\stillyard-authority-{}", Uuid::now_v7());
    let deadline = || Instant::now() + Duration::from_secs(10);
    let mut daemon = spawn_daemon(&pinned, &store, &endpoint);
    let client = connect_uninitialized(&pinned, &endpoint);
    assert_eq!(
        client
            .authority_status(deadline(), None)
            .unwrap()
            .blocker
            .as_deref(),
        Some("authority_uninitialized")
    );
    let marker = temp.path().join("authority-canary.txt");
    let spec = command_spec(temp.path(), "echo released>authority-canary.txt");
    let submit = |client: &Client| {
        client
            .submit(
                spec.clone(),
                &SubmitOptions::new(Uuid::now_v7()),
                deadline(),
                None,
            )
            .unwrap()
            .job_id
    };
    let assert_closed = |client: &Client, id| {
        assert!(!matches!(
            client.wait_outcome(id, Instant::now() + Duration::from_millis(500), None),
            WaitOutcome::Final { .. }
        ));
        let status = client.status(id, deadline(), None).unwrap();
        assert!(
            status
                .blockers
                .iter()
                .any(|blocker| blocker.code.starts_with("authority_")),
            "{status:#?}"
        );
        assert!(
            !marker.exists(),
            "user code escaped the admission interlock"
        );
    };
    let first = submit(&client);
    assert_closed(&client, first);
    // Explicit initialization permits this known-empty fixture to run once.
    client
        .initialize_authority_without_outstanding_work(deadline(), None)
        .unwrap();
    assert!(matches!(
        client.wait_outcome(first, deadline(), None),
        WaitOutcome::Final { .. }
    ));
    assert!(marker.exists());
    std::fs::remove_file(&marker).unwrap();
    let original_domains = client
        .authority_status(deadline(), None)
        .unwrap()
        .domains
        .unwrap();
    let machine = client
        .daemon_status(deadline(), None)
        .unwrap()
        .machine_scheduling
        .unwrap();
    assert_eq!(machine.domains, original_domains);
    assert!(
        machine
            .resources
            .iter()
            .all(|resource| resource.resource.scope == original_domains.machine_scope)
    );
    assert_eq!(
        machine
            .resources
            .iter()
            .find(|resource| resource.resource.resource_id == "cargo_slots")
            .unwrap()
            .granted,
        0
    );

    let hold_id = Uuid::now_v7();
    client
        .hold_authority(hold_id, "bootstrap ownership".into(), deadline(), None)
        .unwrap();
    let second = submit(&client);
    assert_closed(&client, second);
    daemon.kill_and_wait();
    daemon = spawn_daemon(&pinned, &store, &endpoint);
    let client = connect_uninitialized(&pinned, &endpoint);
    assert_closed(&client, second);
    daemon.kill_and_wait();
    // Only this isolated test store is reset; the external obligation survives.
    for suffix in ["", "-wal", "-shm"] {
        let path = store.join(format!("stillyard.sqlite3{suffix}"));
        if path.exists() {
            std::fs::remove_file(path).unwrap();
        }
    }
    daemon = spawn_daemon(&pinned, &store, &endpoint);
    let client = connect_uninitialized(&pinned, &endpoint);
    let third = submit(&client);
    assert_closed(&client, third);
    assert_eq!(
        client.authority_status(deadline(), None).unwrap().domains,
        Some(original_domains)
    );
    assert!(
        client
            .authority_status(deadline(), None)
            .unwrap()
            .holds
            .iter()
            .any(|hold| hold.id == hold_id && !hold.released)
    );
    client
        .force_release_authority(
            hold_id,
            "fixture has no external work".into(),
            deadline(),
            None,
        )
        .unwrap();
    // Clearing one named bootstrap hold cannot clear lost coordinator history.
    assert_closed(&client, third);
    assert_eq!(
        client
            .authority_status(deadline(), None)
            .unwrap()
            .blocker
            .as_deref(),
        Some("authority_reconciliation_required")
    );
    daemon.kill_and_wait();
    std::fs::remove_file(store.join("authority/registry.json")).unwrap();
    daemon = spawn_daemon(&pinned, &store, &endpoint);
    let client = connect_uninitialized(&pinned, &endpoint);
    assert!(
        client
            .initialize_authority_without_outstanding_work(deadline(), None)
            .is_err()
    );
    assert_closed(&client, submit(&client));
    daemon.kill_and_wait();
    std::fs::remove_file(store.join("authority/anchor.json")).unwrap();
    let _daemon = spawn_daemon(&pinned, &store, &endpoint);
    let client = connect_uninitialized(&pinned, &endpoint);
    assert_closed(&client, submit(&client));
}

#[test]
fn machine_pairing_reconnect_fences_challenge_replay_and_store_reset() {
    machine_allocation_fixture(None, None, false);
}

#[test]
#[cfg(debug_assertions)]
fn machine_arm_release_crash_boundaries_recover_without_freeing_live_rights() {
    for stage in ["before_journal", "after_journal", "after_sql", "after_ack"] {
        machine_allocation_fixture(Some(stage), Some(InvocationRole::Primary), false);
    }
}

#[test]
fn machine_probe_ticket_cannot_borrow_work_or_postcondition_authority() {
    machine_allocation_fixture(None, Some(InvocationRole::Probe), false);
}

#[test]
#[ignore = "requires native system Job and prebuilt protected Linux executor subject"]
fn machine_cross_os_reset_retains_live_linux_executor_and_guest_history() {
    machine_allocation_fixture(None, None, true);
}

fn machine_allocation_fixture(
    fault_stage: Option<&str>,
    ticket_role: Option<InvocationRole>,
    live_linux: bool,
) {
    use stillyard::machine::{
        AllocationKey, AllocationOwner, Candidate, Claims, Command as MachineOperation,
        ConnectChallenge, ConnectHello, InstallationIdentity, Outcome, PairingRegistration,
        PairingSecret, ParticipantRole, ParticipantSnapshot, Reply, Request as MachineRequest,
    };
    let temp = tempfile::tempdir().unwrap();
    let pinned = copied_daemon(temp.path());
    let store = temp.path().join("machine-store");
    std::fs::create_dir_all(&store).unwrap();
    let mut config = HostConfig::default();
    config.resources.cargo_slots = 1;
    config.resources.custom.insert("side_lane".into(), 1);
    config
        .impact_incompatibilities
        .insert("cpu_heavy".into(), vec!["measurement".into()]);
    std::fs::write(
        store.join("config.json"),
        serde_json::to_vec(&config).unwrap(),
    )
    .unwrap();
    let endpoint = format!(r"\\.\pipe\stillyard-machine-{}", Uuid::now_v7());
    let spawn = || {
        let mut command = Command::new(&pinned);
        command
            .args(["--endpoint", &endpoint, "daemon", "--store"])
            .arg(&store)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        if fault_stage.is_some() {
            command.env("STILLYARD_ISOLATED_MACHINE_FAULT_ROOT", &store);
        }
        ChildGuard::new(command.spawn().unwrap())
    };
    let mut daemon = spawn();
    let client = connect(&pinned, &endpoint);
    let deadline = || Instant::now() + Duration::from_secs(10);
    let topology = client
        .authority_status(deadline(), None)
        .unwrap()
        .domains
        .unwrap();
    let mut linux = live_linux.then(cross_os_reset::Subject::start);
    let linux_identity = linux.as_ref().map(|subject| {
        let value: serde_json::Value = subject.receive("manager.json");
        (
            serde_json::from_value::<Uuid>(value["store_uuid"].clone()).unwrap(),
            serde_json::from_value::<u32>(value["uid"].clone()).unwrap(),
        )
    });
    let secret = PairingSecret::generate().unwrap();
    let registration = PairingRegistration {
        installation: InstallationIdentity {
            installation_nonce: Uuid::now_v7(),
            domain_id: stillyard::ExecutionDomainId(Uuid::now_v7()),
            owner_uid: linux_identity.map_or(1000, |value| value.1),
            runtime_registration: "isolated-protocol-fixture".into(),
            role: ParticipantRole::Executor,
        },
        manager_store_uuid: linux_identity.map_or_else(Uuid::now_v7, |value| value.0),
        parent_domain: topology.machine_scope,
        budgets: Default::default(),
        aliases: Default::default(),
        secret: *secret.anchor_bytes(),
    };
    if let Some(subject) = &linux {
        subject.publish(
            "configuration.json",
            &serde_json::json!({
                "version":1,"pairing":registration,"coordinator_installation":topology.machine_id,
                "machine_id":topology.machine_id,"bridge_executable":"/isolated-test/unused-bridge",
                "bridge_sha256":"a".repeat(64),"coordinator_endpoint":endpoint,
                "interop_socket":"/run/WSL/unused-test-socket",
                "executor_cgroup":"/sys/fs/cgroup/isolated-reset-profile",
                "journal":Uuid::now_v7()
            }),
        );
    }
    let paired = client
        .pair_machine_domain(registration.clone(), deadline())
        .unwrap();
    assert_eq!(paired.connection_epoch, 0);
    assert!(paired.reconciliation_required);
    assert_eq!(
        client
            .pair_machine_domain(registration.clone(), deadline())
            .unwrap(),
        paired
    );
    let mut conflict = registration.clone();
    conflict.secret[0] ^= 1;
    assert!(client.pair_machine_domain(conflict, deadline()).is_err());
    let hello = ConnectHello {
        installation_nonce: registration.installation.installation_nonce,
        manager_store_uuid: registration.manager_store_uuid,
        executor_incarnation: Uuid::now_v7(),
        executor_protocol: 25,
        executor_nonce: [3; 32],
    };
    // Knowing the public hello does not make an arbitrary client the installed bridge.
    assert!(
        client
            .machine_connect_begin(hello.clone(), deadline())
            .is_err()
    );
    let call = |command: &str, input: serde_json::Value| {
        let path = temp.path().join(format!("machine-{}.json", Uuid::now_v7()));
        std::fs::write(&path, serde_json::to_vec(&input).unwrap()).unwrap();
        Command::new(&pinned)
            .args(["--endpoint", &endpoint, "machine", command, "--spec"])
            .arg(path)
            .output()
            .unwrap()
    };
    let begin = call("connect-begin", serde_json::to_value(&hello).unwrap());
    assert!(
        begin.status.success(),
        "{}",
        String::from_utf8_lossy(&begin.stderr)
    );
    let challenge: ConnectChallenge = serde_json::from_slice(&begin.stdout).unwrap();
    let tag = secret.sign_challenge(&challenge).unwrap();
    let mut wrong = tag;
    wrong[0] ^= 1;
    assert!(
        !call(
            "connect-finish",
            serde_json::json!({"challenge":challenge, "tag":wrong})
        )
        .status
        .success()
    );
    let authenticated = call(
        "connect-finish",
        serde_json::json!({"challenge":challenge, "tag":tag}),
    );
    assert!(
        authenticated.status.success(),
        "{}",
        String::from_utf8_lossy(&authenticated.stderr)
    );
    let authenticated: ParticipantSnapshot = serde_json::from_slice(&authenticated.stdout).unwrap();
    assert_eq!(authenticated.connection_epoch, 1);
    assert!(authenticated.reconciliation_required);
    assert!(
        !call(
            "connect-finish",
            serde_json::json!({"challenge":challenge, "tag":tag})
        )
        .status
        .success()
    );

    let configuration = client
        .daemon_status(deadline(), None)
        .unwrap()
        .config_sha256;
    let operation = |sequence, command| {
        let mut request =
            MachineRequest::new(challenge.session.clone(), sequence, Uuid::now_v7(), command)
                .unwrap();
        secret.sign_request(&mut request).unwrap();
        request
    };
    let exchange = |request: &MachineRequest| {
        let response = call("exchange", serde_json::to_value(request).unwrap());
        assert!(
            response.status.success(),
            "{}",
            String::from_utf8_lossy(&response.stderr)
        );
        serde_json::from_slice::<Reply>(&response.stdout).unwrap()
    };
    let crash_and_restart = |daemon: &mut ChildGuard, request: &MachineRequest| {
        if let Some(stage) = fault_stage {
            use std::io::Write;
            let mut intent = std::fs::File::create(store.join("machine-fault.json")).unwrap();
            intent
                .write_all(&serde_json::to_vec(&(request.operation_id, stage)).unwrap())
                .unwrap();
            intent.sync_all().unwrap();
            drop(intent);
            let failed = call("exchange", serde_json::to_value(request).unwrap());
            assert!(!failed.status.success(), "fault did not interrupt {stage}");
            assert_eq!(
                wait_for_exit(daemon.child_mut(), Duration::from_secs(10)).code(),
                Some(86)
            );
            assert_eq!(
                std::fs::read_to_string(
                    store.join(format!("machine-fault-{}.fired", request.operation_id))
                )
                .unwrap(),
                stage
            );
            daemon.kill_and_wait();
            *daemon = spawn();
            let restarted = connect_uninitialized(&pinned, &endpoint);
            let authority = restarted.authority_status(deadline(), None).unwrap();
            assert!(
                authority.pending_machine_operation.is_none(),
                "journal recovery did not finish at {stage}"
            );
            assert!(
                authority.blocker.is_none(),
                "journal recovery retained gate at {stage}: {:?}",
                authority.blocker
            );
        }
    };
    let snapshot_id = Uuid::now_v7();
    let begin_request = operation(
        1,
        MachineOperation::ReconcileBegin {
            snapshot_id,
            begin_sequence: 0,
            end_sequence: 0,
            page_count: 0,
            digest: stillyard::machine::payload_hash(&Vec::<
                Vec<stillyard::machine::ReconcileAllocation>,
            >::new())
            .unwrap(),
            configuration_sha256: configuration.clone(),
        },
    );
    let mut unauthenticated = begin_request.clone();
    unauthenticated.authentication = [0; 32];
    assert!(
        !call("exchange", serde_json::to_value(&unauthenticated).unwrap())
            .status
            .success()
    );
    assert_eq!(
        client
            .machine_participant(registration.installation.domain_id, deadline())
            .unwrap()
            .accepted_sequence,
        0
    );
    assert!(matches!(
        exchange(&begin_request).outcome,
        Outcome::Accepted { .. }
    ));
    for bad in [
        operation(3, MachineOperation::Inspect { key: None }),
        operation(1, MachineOperation::Inspect { key: None }),
    ] {
        assert!(
            !call("exchange", serde_json::to_value(&bad).unwrap())
                .status
                .success(),
            "authenticated reordered/conflicting request advanced durable history"
        );
    }
    let mut moved = begin_request.clone();
    moved.request_sequence = 2;
    secret.sign_request(&mut moved).unwrap();
    assert!(
        !call("exchange", serde_json::to_value(&moved).unwrap())
            .status
            .success(),
        "one operation UUID acquired a second durable sequence"
    );
    assert_eq!(
        client
            .machine_participant(registration.installation.domain_id, deadline())
            .unwrap()
            .accepted_sequence,
        1
    );
    let commit_request = operation(2, MachineOperation::ReconcileCommit { snapshot_id });
    assert!(matches!(
        exchange(&commit_request).outcome,
        Outcome::Reconciled { .. }
    ));
    let manager = registration.manager_store_uuid;
    let candidate = Candidate {
        key: AllocationKey {
            machine_id: topology.machine_id,
            authority_epoch: challenge.session.authority_epoch,
            domain_id: registration.installation.domain_id,
            manager_store_uuid: manager,
            lease_id: Uuid::now_v7(),
        },
        owner: if ticket_role == Some(InvocationRole::Probe) {
            AllocationOwner::Probe {
                job_id: durable_id(manager),
                invocation_id: durable_id(manager),
            }
        } else {
            AllocationOwner::Work {
                job_id: format!("{manager}~{}", Uuid::now_v7()).parse().unwrap(),
                attempt_id: format!("{manager}~{}", Uuid::now_v7()).parse().unwrap(),
            }
        },
        revision: 1,
        priority: 0,
        claims: Claims {
            scalars: [("cargo_slots".into(), 1)].into(),
            ..Claims::default()
        },
        configuration_sha256: configuration,
        observed: None,
        quiet: None,
    };
    let advertise = operation(
        3,
        MachineOperation::CandidateUpsert {
            candidate: candidate.clone(),
        },
    );
    let accepted = exchange(&advertise);
    assert!(matches!(
        accepted.outcome,
        Outcome::Accepted { revision: 1 }
    ));
    assert_eq!(exchange(&advertise).outcome, accepted.outcome);
    let offer_deadline = deadline();
    loop {
        let status = client.daemon_status(deadline(), None).unwrap();
        if status
            .machine_scheduling
            .unwrap()
            .resources
            .iter()
            .any(|r| r.resource.resource_id == "cargo_slots" && r.offered == 1)
        {
            break;
        }
        assert!(
            Instant::now() < offer_deadline,
            "remote candidate was not offered"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    let marker = temp.path().join("after-offer.txt");
    let mut native = command_spec(temp.path(), "echo released>after-offer.txt");
    native.resources.cargo_slots = Some(1);
    let native = client
        .submit(
            native,
            &SubmitOptions::new(Uuid::now_v7()),
            deadline(),
            None,
        )
        .unwrap()
        .job_id;
    assert!(!matches!(
        client.wait_outcome(native, Instant::now() + Duration::from_millis(200), None),
        WaitOutcome::Final { .. }
    ));
    assert!(
        !marker.exists(),
        "native work ignored the offered machine token"
    );
    let inspection = exchange(&operation(
        4,
        MachineOperation::Inspect {
            key: Some(candidate.key.clone()),
        },
    ));
    let Outcome::Inspection { grants, .. } = inspection.outcome else {
        panic!("expected Grant inspection");
    };
    let offered = &grants[0];
    let arm = operation(
        5,
        MachineOperation::Arm {
            key: candidate.key.clone(),
            offer_nonce: offered.offer_nonce,
        },
    );
    crash_and_restart(&mut daemon, &arm);
    let armed = exchange(&arm);
    assert!(
        matches!(armed.outcome,Outcome::Grant {ref grant} if grant.state==stillyard::machine::GrantState::Armed)
    );
    assert_eq!(exchange(&arm).outcome, armed.outcome);
    std::thread::sleep(Duration::from_millis(5500));
    assert!(!matches!(
        client.wait_outcome(native, Instant::now() + Duration::from_millis(200), None),
        WaitOutcome::Final { .. }
    ));
    assert!(
        !marker.exists(),
        "armed Grant expired with its original Offer TTL"
    );
    let sequence = std::cell::Cell::new(6);
    let next = |command| {
        let current = sequence.get();
        sequence.set(current + 1);
        operation(current, command)
    };
    if fault_stage.is_none() && ticket_role.is_none() {
        // Capacity reduction cannot revoke a possibly used Grant or create free
        // tokens. A new ticket also needs the current configuration.
        let mut reduced = config.clone();
        reduced.resources.cargo_slots = 0;
        daemon.kill_and_wait();
        std::fs::write(
            store.join("config.json"),
            serde_json::to_vec(&reduced).unwrap(),
        )
        .unwrap();
        daemon = spawn_daemon(&pinned, &store, &endpoint);
        let reduced_client = connect_uninitialized(&pinned, &endpoint);
        let snapshot = reduced_client.daemon_status(deadline(), None).unwrap();
        let machine = snapshot.machine_scheduling.unwrap();
        assert!(
            machine
                .resources
                .iter()
                .any(|r| r.resource.resource_id == "cargo_slots"
                    && r.capacity == 0
                    && r.granted == 1),
            "capacity reduction hid the outstanding machine debit"
        );
        assert!(!marker.exists());
        let stale_intent = stillyard::machine::InvocationIntent {
            invocation_id: durable_id(manager),
            containment_id: durable_id(manager),
            role: InvocationRole::Primary,
            role_index: 0,
            release_sequence: 1,
            executable_sha256: "a".repeat(64),
            boundary_sha256: "b".repeat(64),
            readiness_challenge: Uuid::now_v7(),
            previous_cleanup: None,
        };
        assert!(
            matches!(
                exchange(&next(MachineOperation::AuthorizeInvocation {
                    key: candidate.key.clone(),
                    offer_nonce: offered.offer_nonce,
                    intent: stale_intent
                }))
                .outcome,
                Outcome::Rejected { .. }
            ),
            "new ticket used a stale configuration after capacity reduction"
        );
        assert_eq!(
            reduced_client
                .authority_status(deadline(), None)
                .unwrap()
                .machine_obligations
                .len(),
            1
        );
        daemon.kill_and_wait();
        std::fs::write(
            store.join("config.json"),
            serde_json::to_vec(&config).unwrap(),
        )
        .unwrap();
        daemon = spawn_daemon(&pinned, &store, &endpoint);
        connect_uninitialized(&pinned, &endpoint);
    }
    let proof = |intent: &stillyard::machine::InvocationIntent| stillyard::machine::TicketCleanup {
        invocation_id: intent.invocation_id,
        release_sequence: intent.release_sequence,
        boundary_sha256: intent.boundary_sha256.clone(),
        proof_sha256: stillyard::machine::payload_hash(intent).unwrap(),
        user_code_released: true,
    };
    let mut issued = Vec::new();
    if let Some(role) = ticket_role {
        use stillyard::machine::InvocationIntent;
        let primary = InvocationIntent {
            invocation_id: match candidate.owner {
                AllocationOwner::Probe { invocation_id, .. } => invocation_id,
                _ => durable_id(manager),
            },
            containment_id: durable_id(manager),
            role,
            role_index: 0,
            release_sequence: 1,
            executable_sha256: "a".repeat(64),
            boundary_sha256: "b".repeat(64),
            readiness_challenge: Uuid::now_v7(),
            previous_cleanup: None,
        };
        let authorize = |intent| MachineOperation::AuthorizeInvocation {
            key: candidate.key.clone(),
            offer_nonce: offered.offer_nonce,
            intent,
        };
        let mut wrong_role = primary.clone();
        wrong_role.role = InvocationRole::Postcondition;
        assert!(matches!(
            exchange(&next(authorize(wrong_role))).outcome,
            Outcome::Rejected { .. }
        ));
        let launch = next(authorize(primary.clone()));
        crash_and_restart(&mut daemon, &launch);
        let ticket = exchange(&launch);
        assert!(
            matches!(ticket.outcome, Outcome::Ticket { .. }),
            "{:?}",
            ticket.outcome
        );
        assert_eq!(exchange(&launch).outcome, ticket.outcome);
        assert!(matches!(
            exchange(&next(authorize(primary.clone()))).outcome,
            Outcome::Rejected { .. }
        ));
        issued.push(primary.clone());
        let mut postcondition = InvocationIntent {
            invocation_id: durable_id(manager),
            containment_id: durable_id(manager),
            role: InvocationRole::Postcondition,
            role_index: 0,
            release_sequence: 2,
            executable_sha256: "c".repeat(64),
            boundary_sha256: "d".repeat(64),
            readiness_challenge: Uuid::now_v7(),
            previous_cleanup: None,
        };
        assert!(matches!(
            exchange(&next(authorize(postcondition.clone()))).outcome,
            Outcome::Rejected { .. }
        ));
        postcondition.previous_cleanup = Some(proof(&primary));
        let launch = next(authorize(postcondition.clone()));
        if role == InvocationRole::Primary {
            crash_and_restart(&mut daemon, &launch);
            let ticket = exchange(&launch);
            assert!(
                matches!(ticket.outcome, Outcome::Ticket { .. }),
                "{:?}",
                ticket.outcome
            );
            assert_eq!(exchange(&launch).outcome, ticket.outcome);
            issued.push(postcondition);
        } else {
            assert!(matches!(
                exchange(&launch).outcome,
                Outcome::Rejected { .. }
            ));
        }
        assert!(
            !marker.exists(),
            "ticket or primary cleanup freed the work Grant"
        );
        // Issuing a Ticket must not introduce a new expiry for an Armed Grant.
        // This also passes the original Offer deadline with a durable Ticket.
        std::thread::sleep(Duration::from_millis(5500));
        assert!(!matches!(
            client.wait_outcome(native, Instant::now() + Duration::from_millis(200), None),
            WaitOutcome::Final { .. }
        ));
        assert!(!marker.exists(), "issued Ticket introduced a TTL release");
        let incomplete = next(MachineOperation::Release {
            release: stillyard::machine::SealedRelease {
                key: candidate.key.clone(),
                offer_nonce: offered.offer_nonce,
                sealed_sequence: 1,
                tickets: vec![],
            },
        });
        assert!(matches!(
            exchange(&incomplete).outcome,
            Outcome::Rejected { .. }
        ));
    }
    // The participant acknowledges only durable replies. Compaction must work
    // across the same crash boundaries while keeping the live Grant and tickets.
    let acknowledge = next(MachineOperation::Acknowledge {
        through_sequence: 3,
    });
    crash_and_restart(&mut daemon, &acknowledge);
    assert!(matches!(
        exchange(&acknowledge).outcome,
        Outcome::Acknowledged {
            through_sequence: 3
        }
    ));
    assert_eq!(
        exchange(&acknowledge).outcome,
        Outcome::Acknowledged {
            through_sequence: 3
        }
    );
    assert!(
        !call("exchange", serde_json::to_value(&advertise).unwrap())
            .status
            .success(),
        "compacted operation was reinterpreted as new work"
    );
    let retained = client.authority_status(deadline(), None).unwrap();
    assert_eq!(retained.machine_obligations.len(), 1);
    assert_eq!(retained.machine_obligations[0].tickets, issued);
    assert!(!marker.exists(), "acknowledgement freed the active Grant");

    assert!(matches!(
        exchange(&next(MachineOperation::Withdraw {
            key: candidate.key.clone(),
            revision: 2
        }))
        .outcome,
        Outcome::Accepted { .. }
    ));
    let authority = client.authority_status(deadline(), None).unwrap();
    assert_eq!(authority.machine_obligations.len(), 1);
    assert_eq!(authority.machine_obligations[0].grant_id, offered.grant_id);
    assert_eq!(authority.machine_obligations[0].tickets, issued);
    let seal = stillyard::machine::SealedRelease {
        key: candidate.key.clone(),
        offer_nonce: offered.offer_nonce,
        sealed_sequence: 1,
        tickets: issued.iter().map(proof).collect(),
    };
    let release = if fault_stage.is_some() {
        let snapshot_id = Uuid::now_v7();
        let pages = vec![vec![stillyard::machine::ReconcileAllocation {
            key: candidate.key.clone(),
            offer_nonce: offered.offer_nonce,
            tickets: issued.clone(),
            sealed_release: Some(seal),
        }]];
        let end_sequence = sequence.get() - 1;
        assert!(matches!(
            exchange(&next(MachineOperation::ReconcileBegin {
                snapshot_id,
                begin_sequence: 0,
                end_sequence,
                page_count: 1,
                digest: stillyard::machine::payload_hash(&pages).unwrap(),
                configuration_sha256: candidate.configuration_sha256.clone(),
            }))
            .outcome,
            Outcome::Accepted { .. }
        ));
        assert!(
            matches!(
                exchange(&next(MachineOperation::ReconcileCommit { snapshot_id })).outcome,
                Outcome::Rejected { .. }
            ),
            "an incomplete snapshot released outstanding rights"
        );
        let page = next(MachineOperation::ReconcilePage {
            snapshot_id,
            index: 0,
            allocations: pages[0].clone(),
        });
        let accepted = exchange(&page);
        assert!(matches!(accepted.outcome, Outcome::Accepted { .. }));
        assert_eq!(exchange(&page).outcome, accepted.outcome);
        next(MachineOperation::ReconcileCommit { snapshot_id })
    } else {
        next(MachineOperation::Release { release: seal })
    };
    crash_and_restart(&mut daemon, &release);
    let released = exchange(&release);
    assert!(
        matches!(
            released.outcome,
            Outcome::Released { .. } | Outcome::Reconciled { .. }
        ),
        "{:?}",
        released.outcome
    );
    assert_eq!(exchange(&release).outcome, released.outcome);
    // An old Arm reply is historical data, never a new debit or a fresh ticket.
    assert_eq!(exchange(&arm).outcome, armed.outcome);
    assert!(
        client
            .authority_status(deadline(), None)
            .unwrap()
            .machine_obligations
            .is_empty()
    );
    assert!(matches!(
        client.wait_outcome(native, deadline(), None),
        WaitOutcome::Final { .. }
    ));
    assert!(marker.exists());

    let mut cursor = None;
    let mut events = Vec::new();
    loop {
        let page = client.machine_events(cursor, 2, deadline()).unwrap();
        assert!(!page.gap);
        assert!(page.events.len() <= 2);
        events.extend(page.events);
        cursor = Some(page.cursor);
        if !page.more {
            break;
        }
    }
    assert!(
        events
            .windows(2)
            .all(|pair| pair[0].cursor.sequence < pair[1].cursor.sequence)
    );
    let allocation_events = events
        .iter()
        .filter(|e| e.grant_id == offered.grant_id)
        .collect::<Vec<_>>();
    assert_eq!(
        allocation_events.len(),
        3 + issued.len(),
        "replay or recovery duplicated allocation events"
    );
    assert_eq!(
        allocation_events[0].state,
        stillyard::machine::GrantState::Offered
    );
    assert_eq!(
        allocation_events[1].state,
        stillyard::machine::GrantState::Armed
    );
    let last = allocation_events.last().unwrap();
    assert_eq!(last.state, stillyard::machine::GrantState::Released);
    assert_eq!(last.tickets_issued as usize, issued.len());
    let native_grant = client.status(native, deadline(), None).unwrap().allocations[0].grant_id;
    let native_start = events
        .iter()
        .find(|e| e.grant_id == native_grant && e.state == stillyard::machine::GrantState::Armed)
        .expect("native and attached allocation events share the stream");
    assert!(native_start.native);
    assert!(
        native_start.cursor.sequence > last.cursor.sequence,
        "native token was granted before attached release committed"
    );
    assert!(client.machine_events(cursor, 257, deadline()).is_err());

    if ticket_role.is_none() && fault_stage.is_none() {
        let wait_file = |path: &Path| {
            let limit = deadline();
            while !path.exists() {
                assert!(Instant::now() < limit, "native holder did not start");
                std::thread::sleep(Duration::from_millis(20));
            }
        };
        for canceled in [true, false] {
            let mut unused = candidate.clone();
            unused.key.lease_id = Uuid::now_v7();
            unused.owner = AllocationOwner::Work {
                job_id: durable_id(manager),
                attempt_id: durable_id(manager),
            };
            unused.claims.scalars = [("side_lane".into(), 1)].into();
            assert!(matches!(
                exchange(&next(MachineOperation::CandidateUpsert {
                    candidate: unused.clone()
                }))
                .outcome,
                Outcome::Accepted { .. }
            ));
            let limit = deadline();
            let offer = loop {
                if let Outcome::Inspection { mut grants, .. } =
                    exchange(&next(MachineOperation::Inspect {
                        key: Some(unused.key.clone()),
                    }))
                    .outcome
                {
                    if grants
                        .first()
                        .is_some_and(|g| g.state == stillyard::machine::GrantState::Offered)
                    {
                        break grants.remove(0);
                    }
                }
                assert!(Instant::now() < limit, "unused candidate was not offered");
                std::thread::sleep(Duration::from_millis(20));
            };
            if canceled {
                exchange(&next(MachineOperation::CancelCandidate {
                    key: unused.key.clone(),
                    revision: 2,
                }));
            } else {
                std::thread::sleep(Duration::from_millis(5_500));
            }
            assert!(
                matches!(
                    exchange(&next(MachineOperation::Arm {
                        key: unused.key.clone(),
                        offer_nonce: offer.offer_nonce
                    }))
                    .outcome,
                    Outcome::Rejected { .. }
                ),
                "canceled/expired Offer authorized new starts"
            );
            let inspected = exchange(&next(MachineOperation::Inspect {
                key: Some(unused.key.clone()),
            }));
            assert!(
                matches!(inspected.outcome,Outcome::Inspection { ref grants, .. } if grants[0].state == stillyard::machine::GrantState::Expired)
            );
            if !canceled {
                exchange(&next(MachineOperation::CancelCandidate {
                    key: unused.key.clone(),
                    revision: 2,
                }));
            }
            unused.revision = 3;
            assert!(
                matches!(
                    exchange(&next(MachineOperation::CandidateUpsert {
                        candidate: unused
                    }))
                    .outcome,
                    Outcome::Rejected { .. }
                ),
                "cancelled allocation was revived by a higher readiness revision"
            );
            assert!(
                client
                    .authority_status(deadline(), None)
                    .unwrap()
                    .machine_obligations
                    .is_empty()
            );
        }
        let mut holder = command_spec(
            temp.path(),
            "echo started>reservation-holder.txt & C:\\Windows\\System32\\ping.exe -n 5 127.0.0.1 >nul",
        );
        holder.resources.cargo_slots = Some(1);
        let holder = client
            .submit(
                holder,
                &SubmitOptions::new(Uuid::now_v7()),
                deadline(),
                None,
            )
            .unwrap()
            .job_id;
        wait_file(&temp.path().join("reservation-holder.txt"));
        let fresh_candidate = |scalars, priority, configuration: String| {
            let mut fresh = candidate.clone();
            fresh.key.lease_id = Uuid::now_v7();
            fresh.owner = AllocationOwner::Work {
                job_id: durable_id(manager),
                attempt_id: durable_id(manager),
            };
            fresh.claims.scalars = scalars;
            fresh.priority = priority;
            fresh.configuration_sha256 = configuration;
            fresh
        };
        let high = fresh_candidate(
            [("cargo_slots".into(), 1)].into(),
            stillyard::MAX_JOB_PRIORITY,
            candidate.configuration_sha256.clone(),
        );
        assert!(matches!(
            exchange(&next(MachineOperation::CandidateUpsert {
                candidate: high.clone()
            }))
            .outcome,
            Outcome::Accepted { .. }
        ));
        let limit = deadline();
        loop {
            if client
                .daemon_status(deadline(), None)
                .unwrap()
                .machine_scheduling
                .unwrap()
                .resources
                .iter()
                .any(|r| r.resource.resource_id == "cargo_slots" && r.reserved == 1)
            {
                break;
            }
            assert!(
                Instant::now() < limit,
                "attached candidate did not reserve the busy token"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        let compatible = fresh_candidate(
            [("side_lane".into(), 1)].into(),
            0,
            candidate.configuration_sha256.clone(),
        );
        assert!(matches!(
            exchange(&next(MachineOperation::CandidateUpsert {
                candidate: compatible.clone()
            }))
            .outcome,
            Outcome::Accepted { .. }
        ));
        let inspect_offer = |key: &AllocationKey| {
            let limit = deadline();
            loop {
                let inspected = exchange(&next(MachineOperation::Inspect {
                    key: Some(key.clone()),
                }));
                if let Outcome::Inspection { grants, .. } = inspected.outcome {
                    if let Some(grant) = grants
                        .into_iter()
                        .find(|g| g.state == stillyard::machine::GrantState::Offered)
                    {
                        break grant;
                    }
                }
                assert!(
                    Instant::now() < limit,
                    "compatible candidate or reservation conversion was hidden"
                );
                std::thread::sleep(Duration::from_millis(25));
            }
        };
        inspect_offer(&compatible.key);
        assert!(
            !matches!(
                client.wait_outcome(holder, Instant::now() + Duration::from_millis(20), None),
                WaitOutcome::Final { .. }
            ),
            "compatible work started only after full serialization"
        );
        let mut waiting = command_spec(temp.path(), "echo started>reservation-waiter.txt");
        waiting.resources.cargo_slots = Some(1);
        let waiting = client
            .submit(
                waiting,
                &SubmitOptions::new(Uuid::now_v7()),
                deadline(),
                None,
            )
            .unwrap()
            .job_id;
        assert!(matches!(
            client.wait_outcome(holder, deadline(), None),
            WaitOutcome::Final { .. }
        ));
        inspect_offer(&high.key);
        assert!(
            !temp.path().join("reservation-waiter.txt").exists(),
            "native waiter stole the attached reservation on conversion"
        );
        for key in [&high.key, &compatible.key] {
            assert!(matches!(
                exchange(&next(MachineOperation::Withdraw {
                    key: key.clone(),
                    revision: 2
                }))
                .outcome,
                Outcome::Accepted { .. }
            ));
        }
        assert!(matches!(
            client.wait_outcome(waiting, deadline(), None),
            WaitOutcome::Final { .. }
        ));
        assert!(temp.path().join("reservation-waiter.txt").exists());

        // With capacity two the coordinator must permit a real native holder
        // concurrently with an attached Armed allocation.
        daemon.kill_and_wait();
        config.resources.cargo_slots = 2;
        std::fs::write(
            store.join("config.json"),
            serde_json::to_vec(&config).unwrap(),
        )
        .unwrap();
        daemon = spawn();
        let restarted = connect_uninitialized(&pinned, &endpoint);
        let configuration = restarted
            .daemon_status(deadline(), None)
            .unwrap()
            .config_sha256;
        let concurrent = fresh_candidate([("cargo_slots".into(), 1)].into(), 0, configuration);
        assert!(matches!(
            exchange(&next(MachineOperation::CandidateUpsert {
                candidate: concurrent.clone()
            }))
            .outcome,
            Outcome::Accepted { .. }
        ));
        let grant = inspect_offer(&concurrent.key);
        assert!(matches!(
            exchange(&next(MachineOperation::Arm {
                key: concurrent.key.clone(),
                offer_nonce: grant.offer_nonce
            }))
            .outcome,
            Outcome::Grant { .. }
        ));
        let mut native = command_spec(
            temp.path(),
            "echo started>two-slot-holder.txt & C:\\Windows\\System32\\ping.exe -n 3 127.0.0.1 >nul",
        );
        native.resources.cargo_slots = Some(1);
        let native = restarted
            .submit(
                native,
                &SubmitOptions::new(Uuid::now_v7()),
                deadline(),
                None,
            )
            .unwrap()
            .job_id;
        wait_file(&temp.path().join("two-slot-holder.txt"));
        let native_status = restarted.status(native, deadline(), None).unwrap();
        assert_eq!(native_status.allocations.len(), 1);
        let native_grant = &native_status.allocations[0];
        assert_eq!(native_grant.state, stillyard::machine::GrantState::Armed);
        assert_eq!(
            native_grant.key.as_ref().unwrap().domain_id,
            topology.native_domain
        );
        assert_eq!(native_grant.grant_id.entity_uuid(), native_grant.lease_id);
        assert_ne!(native_grant.grant_id, grant.grant_id);
        let native_rights = restarted
            .authority_status(deadline(), None)
            .unwrap()
            .native_obligations;
        assert_eq!(native_rights.len(), 1);
        assert_eq!(native_rights[0].allocation.grant_id, native_grant.grant_id);
        assert_eq!(
            Some(native_rights[0].invocation_id),
            native_status.invocation_id
        );
        assert_eq!(
            native_rights[0].root_identity,
            native_status.attempts[0].invocations[0]
                .root_identity
                .clone()
                .unwrap()
        );
        assert!(
            restarted
                .daemon_status(deadline(), None)
                .unwrap()
                .machine_scheduling
                .unwrap()
                .resources
                .iter()
                .any(|r| r.resource.resource_id == "cargo_slots"
                    && r.capacity == 2
                    && r.granted == 2)
        );
        assert!(matches!(
            exchange(&next(MachineOperation::Release {
                release: stillyard::machine::SealedRelease {
                    key: concurrent.key,
                    offer_nonce: grant.offer_nonce,
                    sealed_sequence: 1,
                    tickets: vec![]
                }
            }))
            .outcome,
            Outcome::Released { .. }
        ));
        assert!(matches!(
            restarted.wait_outcome(native, deadline(), None),
            WaitOutcome::Final { .. }
        ));
        let completed = restarted.status(native, deadline(), None).unwrap();
        assert_eq!(completed.allocations[0].grant_id, native_grant.grant_id);
        assert_eq!(
            completed.allocations[0].state,
            stillyard::machine::GrantState::Released
        );
        assert!(
            restarted
                .authority_status(deadline(), None)
                .unwrap()
                .native_obligations
                .is_empty()
        );

        let mut noisy = command_spec(
            temp.path(),
            "echo started>impact-holder.txt & C:\\Windows\\System32\\ping.exe -n 3 127.0.0.1 >nul",
        );
        noisy.resources.cargo_slots = Some(1);
        noisy.resources.impacts.push("cpu_heavy".into());
        let noisy = restarted
            .submit(noisy, &SubmitOptions::new(Uuid::now_v7()), deadline(), None)
            .unwrap()
            .job_id;
        wait_file(&temp.path().join("impact-holder.txt"));
        let configuration = restarted
            .daemon_status(deadline(), None)
            .unwrap()
            .config_sha256;
        let mut measurement = fresh_candidate([("cargo_slots".into(), 1)].into(), 0, configuration);
        measurement.claims.impacts.push("measurement".into());
        measurement.quiet = Some(QuietPolicy {
            stable_seconds: 1,
            max_sample_age_seconds: 1,
            wait_budget_seconds: 5,
            detectors: vec![QuietDetector::CpuUtilization { max_percent: 100 }],
        });
        assert!(matches!(
            exchange(&next(MachineOperation::CandidateUpsert {
                candidate: measurement.clone()
            }))
            .outcome,
            Outcome::Accepted { .. }
        ));
        std::thread::sleep(Duration::from_millis(200));
        let inspection = exchange(&next(MachineOperation::Inspect {
            key: Some(measurement.key.clone()),
        }));
        assert!(
            matches!(inspection.outcome, Outcome::Inspection { ref grants, .. } if grants.is_empty()),
            "managed impact did not block a measurement despite spare scalar capacity"
        );
        assert!(matches!(
            restarted.wait_outcome(noisy, deadline(), None),
            WaitOutcome::Final { .. }
        ));
        let grant = inspect_offer(&measurement.key);
        assert!(matches!(
            exchange(&next(MachineOperation::Arm {
                key: measurement.key.clone(),
                offer_nonce: grant.offer_nonce
            }))
            .outcome,
            Outcome::Grant { .. }
        ));
        let intent = stillyard::machine::InvocationIntent {
            invocation_id: durable_id(manager),
            containment_id: durable_id(manager),
            role: InvocationRole::Primary,
            role_index: 0,
            release_sequence: 1,
            executable_sha256: "e".repeat(64),
            boundary_sha256: "f".repeat(64),
            readiness_challenge: Uuid::now_v7(),
            previous_cleanup: None,
        };
        let authorize = || MachineOperation::AuthorizeInvocation {
            key: measurement.key.clone(),
            offer_nonce: grant.offer_nonce,
            intent: intent.clone(),
        };
        let first = next(authorize());
        let first_at = Instant::now();
        let waiting = exchange(&first);
        assert!(
            matches!(waiting.outcome, Outcome::Rejected { .. }),
            "one host sample incorrectly satisfied a stable interval"
        );
        let (issued_request, ticket) = loop {
            std::thread::sleep(Duration::from_millis(100));
            let request = next(authorize());
            let response = exchange(&request);
            if matches!(response.outcome, Outcome::Ticket { .. }) {
                break (request, response);
            }
            assert!(
                first_at.elapsed() < Duration::from_secs(5),
                "quiet ticket failed to converge: {:?}",
                response.outcome
            );
        };
        assert!(
            first_at.elapsed() >= Duration::from_secs(1),
            "quiet ticket bypassed the stable interval"
        );
        std::thread::sleep(Duration::from_millis(300));
        assert_eq!(
            exchange(&issued_request).outcome,
            ticket.outcome,
            "replay refreshed an expired single-use ticket"
        );
        assert_eq!(
            exchange(&first).outcome,
            waiting.outcome,
            "old rejected operation was reevaluated as new readiness"
        );
        assert!(
            matches!(
                exchange(&next(authorize())).outcome,
                Outcome::Rejected { .. }
            ),
            "another operation reused an issued Invocation"
        );
        assert!(matches!(
            exchange(&next(MachineOperation::Release {
                release: stillyard::machine::SealedRelease {
                    key: measurement.key,
                    offer_nonce: grant.offer_nonce,
                    sealed_sequence: 1,
                    tickets: vec![proof(&intent)],
                }
            }))
            .outcome,
            Outcome::Released { .. }
        ));
    }

    // Retain an independently armed fake executor alongside the native reset
    // canary. Reconstructing an empty coordinator must preserve BOTH inventories.
    let mut reset_candidate = candidate.clone();
    reset_candidate.key.lease_id = Uuid::now_v7();
    reset_candidate.owner = AllocationOwner::Work {
        job_id: durable_id(manager),
        attempt_id: durable_id(manager),
    };
    reset_candidate.claims.scalars = [("side_lane".into(), 1)].into();
    reset_candidate.configuration_sha256 = client
        .daemon_status(deadline(), None)
        .unwrap()
        .config_sha256;
    assert!(matches!(
        exchange(&next(MachineOperation::CandidateUpsert {
            candidate: reset_candidate.clone()
        }))
        .outcome,
        Outcome::Accepted { .. }
    ));
    let limit = deadline();
    let reset_grant = loop {
        let reply = exchange(&next(MachineOperation::Inspect {
            key: Some(reset_candidate.key.clone()),
        }));
        if let Outcome::Inspection { mut grants, .. } = reply.outcome {
            if grants
                .first()
                .is_some_and(|g| g.state == stillyard::machine::GrantState::Offered)
            {
                break grants.remove(0);
            }
        }
        assert!(Instant::now() < limit, "reset participant was not offered");
        std::thread::sleep(Duration::from_millis(20));
    };
    assert!(matches!(
        exchange(&next(MachineOperation::Arm {
            key: reset_candidate.key.clone(),
            offer_nonce: reset_grant.offer_nonce
        }))
        .outcome,
        Outcome::Grant { .. }
    ));
    let mut reset_intent = stillyard::machine::InvocationIntent {
        invocation_id: durable_id(manager),
        containment_id: durable_id(manager),
        role: InvocationRole::Primary,
        role_index: 0,
        release_sequence: 1,
        executable_sha256: "e".repeat(64),
        boundary_sha256: "f".repeat(64),
        readiness_challenge: Uuid::now_v7(),
        previous_cleanup: None,
    };
    if let Some(subject) = &linux {
        subject.publish("allocation.json", &reset_candidate.key);
        reset_intent = subject.receive("intent.json");
    }
    let reset_ticket = match exchange(&next(MachineOperation::AuthorizeInvocation {
        key: reset_candidate.key.clone(),
        offer_nonce: reset_grant.offer_nonce,
        intent: reset_intent.clone(),
    }))
    .outcome
    {
        Outcome::Ticket { ticket } => ticket,
        other => panic!("reset Invocation was not authorized: {other:?}"),
    };
    if let Some(subject) = &linux {
        subject.publish("ticket.json", &reset_ticket);
        let live: serde_json::Value = subject.receive("live.json");
        assert_eq!(live["populated"], true);
        assert_eq!(live["user_started"], true);
        // Lower a real live executor's claimed token, then restore it.
        // An already accepted native canary must remain pending in both cases.
        let capacity_marker = temp.path().join("capacity-overlap.txt");
        let mut canary = command_spec(temp.path(), "echo forbidden>capacity-overlap.txt");
        canary.resources.custom.insert("side_lane".into(), 1);
        let canary = client
            .submit(
                canary,
                &SubmitOptions::new(Uuid::now_v7()),
                deadline(),
                None,
            )
            .unwrap()
            .job_id;
        assert!(
            client
                .status(canary, deadline(), None)
                .unwrap()
                .started_unix_millis
                .is_none()
        );
        let mut reduced = config.clone();
        reduced.resources.custom.insert("side_lane".into(), 0);
        daemon.kill_and_wait();
        std::fs::write(
            store.join("config.json"),
            serde_json::to_vec(&reduced).unwrap(),
        )
        .unwrap();
        daemon = spawn_daemon(&pinned, &store, &endpoint);
        let reduced_client = connect_uninitialized(&pinned, &endpoint);
        let reduced_status = reduced_client.daemon_status(deadline(), None).unwrap();
        assert!(
            reduced_status
                .machine_scheduling
                .as_ref()
                .unwrap()
                .resources
                .iter()
                .any(|r| r.resource.resource_id == "side_lane"
                    && r.capacity == 0
                    && r.granted == 1)
        );
        let pending = reduced_client.status(canary, deadline(), None).unwrap();
        assert!(pending.started_unix_millis.is_none() && !pending.is_final());
        assert!(!capacity_marker.exists());
        subject.publish(
            "capacity-reduced.json",
            &serde_json::json!({"coordinator": reduced_status, "pending_canary": pending}),
        );
        let witness: serde_json::Value = subject.receive("live-after-capacity-reduction.json");
        assert_eq!(witness["populated"], true);
        assert_eq!(witness["unsealed"], true);
        daemon.kill_and_wait();
        std::fs::write(
            store.join("config.json"),
            serde_json::to_vec(&config).unwrap(),
        )
        .unwrap();
        daemon = spawn_daemon(&pinned, &store, &endpoint);
        let restored_client = connect_uninitialized(&pinned, &endpoint);
        let restored_status = restored_client.daemon_status(deadline(), None).unwrap();
        assert!(
            restored_status
                .machine_scheduling
                .as_ref()
                .unwrap()
                .resources
                .iter()
                .any(|r| r.resource.resource_id == "side_lane"
                    && r.capacity == 1
                    && r.granted == 1)
        );
        let pending = restored_client.status(canary, deadline(), None).unwrap();
        assert!(pending.started_unix_millis.is_none() && !pending.is_final());
        assert!(!capacity_marker.exists());
        subject.publish(
            "capacity-restored.json",
            &serde_json::json!({"coordinator": restored_status, "pending_canary": pending}),
        );
        assert!(restored_client.cancel(&[canary], deadline(), None).unwrap()[0].is_final());
        subject.publish("reset-guest.json", &true);
        let reset: serde_json::Value = subject.receive("guest-reset-rejected.json");
        assert_eq!(reset["populated"], true);
        assert_eq!(reset["unsealed"], true);
        assert!(
            reset["store_reset_rejection"]
                .as_str()
                .unwrap()
                .contains("admission remains fenced")
        );
        assert!(reset["missing_anchor_rejection"].as_str().is_some());
        assert!(
            reset["corrupt_anchor_rejection"]
                .as_str()
                .unwrap()
                .contains("key must be a string")
        );
    }

    let reconnect_hello = if live_linux {
        ConnectHello {
            executor_incarnation: Uuid::now_v7(),
            executor_nonce: [5; 32],
            ..hello.clone()
        }
    } else {
        hello.clone()
    };
    let abandoned = call(
        "connect-begin",
        serde_json::to_value(&reconnect_hello).unwrap(),
    );
    assert!(abandoned.status.success());
    let abandoned: ConnectChallenge = serde_json::from_slice(&abandoned.stdout).unwrap();
    let abandoned_tag = secret.sign_challenge(&abandoned).unwrap();
    let begin = call(
        "connect-begin",
        serde_json::to_value(&reconnect_hello).unwrap(),
    );
    assert!(begin.status.success());
    let second: ConnectChallenge = serde_json::from_slice(&begin.stdout).unwrap();
    assert_ne!(second.coordinator_nonce, challenge.coordinator_nonce);
    assert_eq!(abandoned.session.connection_epoch, 2);
    assert_eq!(second.session.connection_epoch, 3);
    assert!(
        !call(
            "connect-finish",
            serde_json::json!({"challenge":abandoned, "tag":abandoned_tag})
        )
        .status
        .success(),
        "abandoned challenge reused its reserved connection epoch"
    );
    let second_tag = secret.sign_challenge(&second).unwrap();
    assert!(
        call(
            "connect-finish",
            serde_json::json!({"challenge":second, "tag":second_tag})
        )
        .status
        .success()
    );
    assert!(
        !call(
            "connect-finish",
            serde_json::json!({"challenge":challenge, "tag":tag})
        )
        .status
        .success()
    );

    assert!(
        !call("exchange", serde_json::to_value(&arm).unwrap())
            .status
            .success(),
        "new connection left the old authenticated writer active"
    );
    if let Some(subject) = &linux {
        assert_ne!(
            hello.executor_incarnation,
            reconnect_hello.executor_incarnation
        );
        let retained = client
            .authority_status(deadline(), None)
            .unwrap()
            .machine_obligations;
        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].tickets, vec![reset_intent.clone()]);
        // Reconcile the live inventory before testing single-use enforcement:
        // a generic reconnect fence must not be the reason authorization fails.
        let participant = client
            .machine_participant(registration.installation.domain_id, deadline())
            .unwrap();
        let sequence = std::cell::Cell::new(participant.accepted_sequence + 1);
        let reconnected = |command| {
            let n = sequence.get();
            sequence.set(n + 1);
            let mut request =
                MachineRequest::new(second.session.clone(), n, Uuid::now_v7(), command).unwrap();
            secret.sign_request(&mut request).unwrap();
            request
        };
        let snapshot_id = Uuid::now_v7();
        let pages = vec![vec![stillyard::machine::ReconcileAllocation {
            key: reset_candidate.key.clone(),
            offer_nonce: reset_grant.offer_nonce,
            tickets: vec![reset_intent.clone()],
            sealed_release: None,
        }]];
        assert!(matches!(
            exchange(&reconnected(MachineOperation::ReconcileBegin {
                snapshot_id,
                begin_sequence: 0,
                end_sequence: participant.accepted_sequence,
                page_count: 1,
                digest: stillyard::machine::payload_hash(&pages).unwrap(),
                configuration_sha256: reset_candidate.configuration_sha256.clone(),
            }))
            .outcome,
            Outcome::Accepted { .. }
        ));
        assert!(matches!(
            exchange(&reconnected(MachineOperation::ReconcilePage {
                snapshot_id,
                index: 0,
                allocations: pages[0].clone(),
            }))
            .outcome,
            Outcome::Accepted { .. }
        ));
        assert!(matches!(
            exchange(&reconnected(MachineOperation::ReconcileCommit { snapshot_id })).outcome,
            Outcome::Reconciled { released, .. } if released.is_empty()
        ));
        assert!(
            !client
                .machine_participant(registration.installation.domain_id, deadline())
                .unwrap()
                .reconciliation_required
        );
        assert!(
            client
                .authority_status(deadline(), None)
                .unwrap()
                .blocker
                .is_none()
        );
        let duplicate = exchange(&reconnected(MachineOperation::AuthorizeInvocation {
            key: reset_candidate.key.clone(),
            offer_nonce: reset_grant.offer_nonce,
            intent: reset_intent.clone(),
        }));
        assert!(
            matches!(&duplicate.outcome, Outcome::Rejected { code, detail }
            if code == "conflict" && detail.contains("ticket identity is single-use")),
            "new executor incarnation bypassed retained Ticket: {:?}",
            duplicate.outcome
        );
        subject.publish("peer-fenced.json", &serde_json::json!({
            "old_incarnation": hello.executor_incarnation, "new_incarnation": reconnect_hello.executor_incarnation,
            "old_session": challenge.session, "new_session": second.session,
            "old_writer_rejected": true, "retained_grants": retained,
            "new_incarnation_duplicate_ticket": duplicate
        }));
        let witness: serde_json::Value = subject.receive("live-after-peer-fence.json");
        assert_eq!(witness["populated"], true);
        assert_eq!(witness["unsealed"], true);
    }
    daemon.kill_and_wait();
    daemon = spawn_daemon(&pinned, &store, &endpoint);
    let restarted = connect_uninitialized(&pinned, &endpoint);
    assert_eq!(
        restarted
            .machine_participant(registration.installation.domain_id, deadline())
            .unwrap()
            .connection_epoch,
        3
    );
    let mut live = command_spec(
        temp.path(),
        "echo started>before-native-reset.txt & C:\\Windows\\System32\\ping.exe -n 30 127.0.0.1 >nul",
    );
    live.resources.cargo_slots = Some(1);
    let live = restarted
        .submit(live, &SubmitOptions::new(Uuid::now_v7()), deadline(), None)
        .unwrap()
        .job_id;
    let limit = deadline();
    while !temp.path().join("before-native-reset.txt").exists() {
        assert!(Instant::now() < limit, "native reset canary did not start");
        std::thread::sleep(Duration::from_millis(20));
    }
    let native_before_reset = restarted
        .authority_status(deadline(), None)
        .unwrap()
        .native_obligations;
    assert_eq!(native_before_reset.len(), 1);
    assert!(
        matches!(native_before_reset[0].allocation.owner, AllocationOwner::Work { job_id, .. } if job_id == live)
    );
    daemon.kill_and_wait();
    for suffix in ["", "-wal", "-shm"] {
        let path = store.join(format!("stillyard.sqlite3{suffix}"));
        if path.exists() {
            std::fs::remove_file(path).unwrap();
        }
    }
    let mut replacement = spawn_daemon(&pinned, &store, &endpoint);
    let reset = connect_uninitialized(&pinned, &endpoint);
    assert_eq!(
        reset
            .authority_status(deadline(), None)
            .unwrap()
            .native_obligations,
        native_before_reset,
        "SQLite reset erased the native process inventory"
    );
    assert!(
        reset.machine_events(cursor, 2, deadline()).is_err(),
        "replacement event history accepted an old store cursor"
    );
    assert_eq!(
        reset
            .authority_status(deadline(), None)
            .unwrap()
            .blocker
            .as_deref(),
        Some("authority_reconciliation_required")
    );
    assert!(
        reset
            .pair_machine_domain(registration.clone(), deadline())
            .is_err()
    );
    let recovered = reset.machine_recover(deadline()).unwrap();
    assert_eq!(recovered.machine_obligations.len(), 1);
    assert_eq!(
        recovered.machine_obligations[0].grant_id,
        reset_grant.grant_id
    );
    assert_eq!(
        recovered.machine_obligations[0].tickets,
        vec![reset_intent.clone()]
    );
    assert_eq!(recovered.native_obligations, native_before_reset);
    assert_eq!(
        recovered.blocker.as_deref(),
        Some("authority_reconciliation_required")
    );
    // Repeating recovery is harmless; disappearance of the native process alone
    // cannot release the executor's possibly consumed ticket.
    assert_eq!(reset.machine_recover(deadline()).unwrap(), recovered);
    if let Some(subject) = &linux {
        subject.publish("coordinator-reset-observed.json", &true);
        let live: serde_json::Value = subject.receive("live-after-coordinator-reset.json");
        assert_eq!(live["populated"], true);
        assert_eq!(live["unsealed"], true);
    }
    let mut after_reset = command_spec(temp.path(), "echo admitted>after-coordinator-reset.txt");
    after_reset.resources.cargo_slots = Some(1);
    if live_linux {
        after_reset.resources.custom.insert("side_lane".into(), 1);
    }
    let after_reset = reset
        .submit(
            after_reset,
            &SubmitOptions::new(Uuid::now_v7()),
            deadline(),
            None,
        )
        .unwrap()
        .job_id;
    assert!(!temp.path().join("after-coordinator-reset.txt").exists());
    let begin = call(
        "connect-begin",
        serde_json::to_value(&reconnect_hello).unwrap(),
    );
    assert!(
        begin.status.success(),
        "{}",
        String::from_utf8_lossy(&begin.stderr)
    );
    let resumed: ConnectChallenge = serde_json::from_slice(&begin.stdout).unwrap();
    assert_eq!(resumed.session.connection_epoch, 4);
    let resumed_tag = secret.sign_challenge(&resumed).unwrap();
    let finish = call(
        "connect-finish",
        serde_json::json!({"challenge":resumed,"tag":resumed_tag}),
    );
    assert!(finish.status.success());
    let participant: ParticipantSnapshot = serde_json::from_slice(&finish.stdout).unwrap();
    assert!(participant.retired_sequence_floor > 0);
    let sequence = std::cell::Cell::new(participant.accepted_sequence + 1);
    let resumed_next = |command| {
        let n = sequence.get();
        sequence.set(n + 1);
        let mut r =
            MachineRequest::new(resumed.session.clone(), n, Uuid::now_v7(), command).unwrap();
        secret.sign_request(&mut r).unwrap();
        r
    };
    let missing = Uuid::now_v7();
    let end = sequence.get() - 1;
    let empty_pages = Vec::<Vec<stillyard::machine::ReconcileAllocation>>::new();
    assert!(matches!(
        exchange(&resumed_next(MachineOperation::ReconcileBegin {
            snapshot_id: missing,
            begin_sequence: 0,
            end_sequence: end,
            page_count: 0,
            digest: stillyard::machine::payload_hash(&empty_pages).unwrap(),
            configuration_sha256: reset_candidate.configuration_sha256.clone(),
        }))
        .outcome,
        Outcome::Accepted { .. }
    ));
    assert!(matches!(
        exchange(&resumed_next(MachineOperation::ReconcileCommit {
            snapshot_id: missing
        }))
        .outcome,
        Outcome::Rejected { .. }
    ));
    assert!(reset.machine_recover(deadline()).unwrap().blocker.is_some());
    assert!(!temp.path().join("after-coordinator-reset.txt").exists());
    let reset_cleanup = if let Some(subject) = &mut linux {
        subject.publish("cleanup.json", &true);
        let sealed: serde_json::Value = subject.receive("seal.json");
        let cleanup = stillyard::machine::TicketCleanup {
            invocation_id: reset_intent.invocation_id,
            release_sequence: reset_intent.release_sequence,
            boundary_sha256: reset_intent.boundary_sha256.clone(),
            proof_sha256: sealed["proof_sha256"].as_str().unwrap().to_owned(),
            user_code_released: sealed["seal"]["possibly_released"].as_bool().unwrap(),
        };
        assert_eq!(
            sealed["seal"]["boundary_sha256"],
            reset_intent.boundary_sha256
        );
        subject.finish();
        cleanup
    } else {
        proof(&reset_intent)
    };
    let pages = vec![vec![stillyard::machine::ReconcileAllocation {
        key: reset_candidate.key.clone(),
        offer_nonce: reset_grant.offer_nonce,
        tickets: vec![reset_intent.clone()],
        sealed_release: Some(stillyard::machine::SealedRelease {
            key: reset_candidate.key.clone(),
            offer_nonce: reset_grant.offer_nonce,
            sealed_sequence: 1,
            tickets: vec![reset_cleanup],
        }),
    }]];
    let complete = Uuid::now_v7();
    let end = sequence.get() - 1;
    assert!(matches!(
        exchange(&resumed_next(MachineOperation::ReconcileBegin {
            snapshot_id: complete,
            begin_sequence: 0,
            end_sequence: end,
            page_count: 1,
            digest: stillyard::machine::payload_hash(&pages).unwrap(),
            configuration_sha256: reset_candidate.configuration_sha256.clone(),
        }))
        .outcome,
        Outcome::Accepted { .. }
    ));
    assert!(matches!(
        exchange(&resumed_next(MachineOperation::ReconcilePage {
            snapshot_id: complete,
            index: 0,
            allocations: pages[0].clone(),
        }))
        .outcome,
        Outcome::Accepted { .. }
    ));
    assert!(matches!(
        exchange(&resumed_next(MachineOperation::ReconcileCommit {
            snapshot_id: complete
        }))
        .outcome,
        Outcome::Reconciled { .. }
    ));
    let cleared = reset.machine_recover(deadline()).unwrap();
    assert!(
        cleared.blocker.is_none(),
        "recovery did not converge: {:?}",
        cleared.blocker
    );
    assert_ne!(cleared.epoch, recovered.epoch);
    assert!(cleared.native_obligations.is_empty());
    assert!(cleared.machine_obligations.is_empty());
    assert!(
        !call(
            "exchange",
            serde_json::to_value(resumed_next(MachineOperation::Inspect { key: None })).unwrap()
        )
        .status
        .success(),
        "retired authority epoch accepted an old writer"
    );
    assert_eq!(
        reset.wait(after_reset, deadline(), None).unwrap().outcome,
        Some(stillyard::JobOutcome::Succeeded)
    );
    assert!(temp.path().join("after-coordinator-reset.txt").exists());
    let rollback_pending = if fault_stage.is_none() && ticket_role.is_none() {
        let hold = Uuid::now_v7();
        reset
            .hold_authority(hold, "queue before rollback".into(), deadline(), None)
            .unwrap();
        let options = SubmitOptions::new(Uuid::now_v7());
        let spec = command_spec(temp.path(), "echo forbidden>rollback-queued.txt");
        let id = reset
            .submit(spec.clone(), &options, deadline(), None)
            .unwrap()
            .job_id;
        Some((id, hold, spec, options))
    } else {
        None
    };
    replacement.kill_and_wait();
    let database = rusqlite::Connection::open(store.join("stillyard.sqlite3")).unwrap();
    database
        .execute(
            "UPDATE machine_domains SET accepted_sequence=accepted_sequence-1",
            [],
        )
        .unwrap();
    drop(database);
    let mut rollback_daemon = spawn_daemon(&pinned, &store, &endpoint);
    let rollback = connect_uninitialized(&pinned, &endpoint);
    assert_eq!(
        rollback
            .authority_status(deadline(), None)
            .unwrap()
            .blocker
            .as_deref(),
        Some("authority_reconciliation_required"),
        "partial SQL rollback bypassed the external accepted-sequence checkpoint"
    );
    assert!(
        rollback
            .machine_recover(deadline())
            .unwrap()
            .blocker
            .is_some(),
        "same-store rollback was silently treated as a reconstructed empty store"
    );
    if let Some((pending, hold, pending_spec, pending_options)) = rollback_pending {
        assert_eq!(
            rollback
                .submit(pending_spec, &pending_options, deadline(), None)
                .unwrap()
                .job_id,
            pending,
            "repair gate blocked recovery of an existing receipt"
        );
        let rejected = rollback
            .submit(
                command_spec(temp.path(), "echo forbidden>new-during-repair.txt"),
                &SubmitOptions::new(Uuid::now_v7()),
                deadline(),
                None,
            )
            .unwrap_err();
        assert!(
            rejected.to_string().contains("authority_repair_pending"),
            "{rejected}"
        );
        rollback
            .force_release_authority(
                hold,
                "fixture queue retained by rollback gate".into(),
                deadline(),
                None,
            )
            .unwrap();
        assert!(
            rollback
                .machine_recover(deadline())
                .unwrap()
                .blocker
                .is_some()
        );
        assert!(!temp.path().join("rollback-queued.txt").exists());
        // Ordinary explicit cancellation resolves possibly resurrected native
        // history. Recovery itself does not drop or silently cancel any Job.
        rollback.cancel(&[pending], deadline(), None).unwrap();
        assert_eq!(
            rollback.wait(pending, deadline(), None).unwrap().outcome,
            Some(stillyard::JobOutcome::Canceled)
        );
        // A new client cannot replenish the queue after cancellation, including a batch.
        let batch = BatchSpec {
            spec_version: SPEC_VERSION,
            jobs: vec![BatchMember {
                name: "new".into(),
                spec: command_spec(temp.path(), "exit 0"),
                dependencies: vec![],
            }],
        };
        let rejected = rollback
            .submit_batch(batch, &SubmitOptions::new(Uuid::now_v7()), deadline(), None)
            .unwrap_err();
        assert!(
            rejected.to_string().contains("authority_repair_pending"),
            "{rejected}"
        );
        let p = rollback
            .machine_clearance_preview(registration.installation.domain_id, deadline())
            .unwrap();
        assert!(p.inventory.grants.is_empty());
        rollback
            .retire_machine_domain(
                stillyard::machine::DomainRetirementRequest {
                    operation_id: Uuid::now_v7(),
                    domain_id: registration.installation.domain_id,
                    expected_inventory_sha256: p.sha256,
                    reason: "empty fixture manager retired during rollback repair".into(),
                    accept_risk: false,
                },
                deadline(),
            )
            .unwrap();
        // Detection pinned the detecting daemon identity; a restart establishes
        // its death rather than treating its own current generation as proof.
        assert!(
            rollback
                .machine_recover(deadline())
                .unwrap()
                .blocker
                .is_some()
        );
        rollback_daemon.kill_and_wait();
        rollback_daemon = spawn_daemon(&pinned, &store, &endpoint);
        let repaired = connect_uninitialized(&pinned, &endpoint);
        let snapshot = repaired.machine_recover(deadline()).unwrap();
        assert!(snapshot.blocker.is_none(), "{:?}", snapshot.blocker);
        assert_ne!(snapshot.epoch, cleared.epoch);
        assert_eq!(
            repaired
                .status(after_reset, deadline(), None)
                .unwrap()
                .outcome,
            Some(stillyard::JobOutcome::Succeeded)
        );
        assert_eq!(
            repaired.status(pending, deadline(), None).unwrap().outcome,
            Some(stillyard::JobOutcome::Canceled)
        );
        assert!(!temp.path().join("rollback-queued.txt").exists());
        let fresh = repaired
            .submit(
                command_spec(temp.path(), "echo repaired>after-rollback-repair.txt"),
                &SubmitOptions::new(Uuid::now_v7()),
                deadline(),
                None,
            )
            .unwrap()
            .job_id;
        assert_eq!(fresh.store_uuid(), after_reset.store_uuid());
        assert_eq!(
            repaired.wait(fresh, deadline(), None).unwrap().outcome,
            Some(stillyard::JobOutcome::Succeeded)
        );
        assert!(temp.path().join("after-rollback-repair.txt").exists());
        rollback_daemon.kill_and_wait();
    }
}

#[test]
fn authority_hold_rejects_active_work_and_managed_administration() {
    let temp = tempfile::tempdir().unwrap();
    let pinned = copied_daemon(temp.path());
    let store = temp.path().join("authority-admin-store");
    let endpoint = format!(r"\\.\pipe\stillyard-authority-admin-{}", Uuid::now_v7());
    let _daemon = spawn_daemon(&pinned, &store, &endpoint);
    let client = connect(&pinned, &endpoint);
    let deadline = || Instant::now() + Duration::from_secs(10);
    let marker = temp.path().join("active.txt");
    let spec = command_spec(
        temp.path(),
        "echo active>active.txt & C:\\Windows\\System32\\ping.exe -n 4 127.0.0.1 >nul",
    );
    let receipt = client
        .submit(spec, &SubmitOptions::new(Uuid::now_v7()), deadline(), None)
        .unwrap();
    let start_deadline = deadline();
    while !marker.exists() {
        assert!(Instant::now() < start_deadline);
        std::thread::sleep(Duration::from_millis(10));
    }
    let error = client
        .hold_authority(
            Uuid::now_v7(),
            "unsafe maintenance request".into(),
            deadline(),
            None,
        )
        .unwrap_err();
    assert!(error.to_string().contains("Lease"), "{error}");
    assert!(
        client
            .authority_status(deadline(), None)
            .unwrap()
            .blocker
            .is_none()
    );
    assert!(matches!(
        client.wait_outcome(receipt.job_id, deadline(), None),
        WaitOutcome::Final { .. }
    ));

    let mut spec = command_spec(temp.path(), "unused");
    spec.executable = pinned;
    spec.args = vec![
        "--endpoint".into(),
        endpoint,
        "authority".into(),
        "hold".into(),
        Uuid::now_v7().to_string(),
        "--reason".into(),
        "managed self-administration".into(),
    ];
    let receipt = client
        .submit(spec, &SubmitOptions::new(Uuid::now_v7()), deadline(), None)
        .unwrap();
    assert!(matches!(
        client.wait_outcome(receipt.job_id, deadline(), None),
        WaitOutcome::Final { .. }
    ));
    let status = client.status(receipt.job_id, deadline(), None).unwrap();
    assert_eq!(status.outcome, Some(stillyard::JobOutcome::Failed));
    let log = client
        .logs(
            receipt.job_id,
            LogStream::Stderr,
            0,
            65536,
            deadline(),
            None,
        )
        .unwrap();
    assert!(String::from_utf8_lossy(&log.bytes).contains("managed work cannot"));
    assert!(
        client
            .authority_status(deadline(), None)
            .unwrap()
            .holds
            .is_empty()
    );
}

#[test]
#[ignore = "requires explicit WSL distribution/user and must be scheduled by the system daemon"]
fn wsl_bootstrap_nested_delegation_cannot_escape_the_outer_cleanup() {
    let temp = tempfile::tempdir().unwrap();
    let pinned = copied_daemon(temp.path());
    let endpoint = format!(r"\\.\pipe\stillyard-nested-bootstrap-{}", Uuid::now_v7());
    let _daemon = spawn_daemon(&pinned, &temp.path().join("store"), &endpoint);
    let client = connect(&pinned, &endpoint);
    let code = r#"
import errno, os, pathlib, subprocess
root = pathlib.Path(os.environ['STILLYARD_TEST_CGROUP_ROOT'])
assert root.is_dir()
assert (root / 'cgroup.max.depth').read_text().strip() == '16'
child = root / 'nested-control'
child.mkdir()
worker = subprocess.Popen(['/usr/bin/python3', '-c', 'import time; time.sleep(60)'])
(child / 'cgroup.procs').write_text(str(worker.pid))
assert worker.pid in [int(p) for p in (child / 'cgroup.procs').read_text().split()]
try:
    pathlib.Path('/sys/fs/cgroup/cgroup.procs').write_text(str(worker.pid))
    raise AssertionError('host cgroup hierarchy became writable')
except OSError as error:
    assert error.errno in (errno.EROFS, errno.EACCES, errno.EPERM), error
assert 'populated 1' in (child / 'cgroup.events').read_text()
print('nested-live-descendant-and-readonly-host-proven', flush=True)
# Deliberately leave both a live descendant and its cgroup. Only the outer
# supervisor can seal this Job; root exit by itself is not a cleanup proof.
"#;
    let work = stillyard::BootstrapWork {
        operation_id: Uuid::nil(),
        distribution: std::env::var("MR_WSL_TEST_DISTRO").unwrap(),
        user: std::env::var("MR_WSL_TEST_USER").unwrap(),
        executable: "/usr/bin/python3".into(),
        args: vec!["-c".into(), code.into()],
        working_directory: "/tmp".into(),
        environment: [("PATH".into(), "/usr/bin:/bin".into())].into(),
        timeout_seconds: 30,
        delegate_test_cgroup: true,
    };
    let work_path = temp.path().join("nested.json");
    std::fs::write(&work_path, serde_json::to_vec(&work).unwrap()).unwrap();
    let mut spec = command_spec(temp.path(), "unused");
    spec.executable = pinned;
    spec.args = vec![
        "--endpoint".into(),
        endpoint,
        "bootstrap".into(),
        "run".into(),
        "--spec".into(),
        work_path.display().to_string(),
    ];
    spec.timeout_seconds = Some(90);
    let until = || Instant::now() + Duration::from_secs(120);
    let receipt = client
        .submit(spec, &SubmitOptions::new(Uuid::now_v7()), until(), None)
        .unwrap();
    let result = client.wait(receipt.job_id, until(), None).unwrap();
    assert_eq!(
        result.outcome,
        Some(stillyard::JobOutcome::Succeeded),
        "{result:?}"
    );
    let log = client
        .logs(receipt.job_id, LogStream::Stdout, 0, 65536, until(), None)
        .unwrap();
    assert!(
        String::from_utf8_lossy(&log.bytes)
            .contains("nested-live-descendant-and-readonly-host-proven")
    );
    let authority = client.authority_status(until(), None).unwrap();
    assert_eq!(authority.holds.len(), 1);
    assert!(authority.holds[0].released);
    assert_eq!(
        authority.holds[0].cleanup_proof.as_ref().unwrap().phase,
        "sealed_empty"
    );
}

#[test]
#[ignore = "requires explicit WSL distribution/user and must be scheduled by the system daemon"]
fn wsl_bootstrap_public_path_preserves_outputs_exit_timeout_and_bridge_loss_obligation() {
    let distribution = std::env::var("MR_WSL_TEST_DISTRO").expect("explicit WSL distro");
    let user = std::env::var("MR_WSL_TEST_USER").expect("explicit WSL user");
    let temp = tempfile::tempdir().unwrap();
    let pinned = copied_daemon(temp.path());
    let store = temp.path().join("bootstrap-store");
    let endpoint = format!(r"\\.\pipe\stillyard-bootstrap-{}", Uuid::now_v7());
    let mut daemon = spawn_daemon(&pinned, &store, &endpoint);
    let client = connect(&pinned, &endpoint);
    let deadline = || Instant::now() + Duration::from_secs(30);
    let submit = |code: &str, timeout_seconds| {
        let work = stillyard::BootstrapWork {
            operation_id: Uuid::nil(),
            distribution: distribution.clone(),
            user: user.clone(),
            executable: "/usr/bin/python3".into(),
            args: vec!["-c".into(), code.into()],
            working_directory: "/tmp".into(),
            environment: [("PATH".into(), "/usr/bin:/bin".into())].into(),
            timeout_seconds,
            delegate_test_cgroup: false,
        };
        let work_path = temp
            .path()
            .join(format!("bootstrap-{}.json", Uuid::now_v7()));
        std::fs::write(&work_path, serde_json::to_vec(&work).unwrap()).unwrap();
        let mut spec = command_spec(temp.path(), "unused");
        spec.executable = pinned.clone();
        spec.args = vec![
            "--endpoint".into(),
            endpoint.clone(),
            "bootstrap".into(),
            "run".into(),
            "--spec".into(),
            work_path.display().to_string(),
        ];
        spec.timeout_seconds = Some(60);
        client
            .submit(spec, &SubmitOptions::new(Uuid::now_v7()), deadline(), None)
            .unwrap()
            .job_id
    };
    let await_final = |id| {
        let outcome = client.wait_outcome(id, deadline(), None);
        assert!(
            matches!(outcome, WaitOutcome::Final { .. }),
            "bootstrap wait: {outcome:#?}; status: {:#?}; authority: {:#?}; stderr: {:#?}",
            client.status(id, deadline(), None),
            client.authority_status(deadline(), None),
            client
                .logs(id, LogStream::Stderr, 0, 65536, deadline(), None)
                .map(|log| String::from_utf8_lossy(&log.bytes).into_owned())
        );
        client.status(id, deadline(), None).unwrap()
    };
    let first = submit(
        "import errno,os,subprocess,sys\ntry:\n subprocess.run(['/mnt/c/Windows/System32/cmd.exe','/d','/c','exit 0'],check=False)\nexcept OSError as error:\n assert error.errno == errno.EACCES\nelse:\n raise RuntimeError('Windows interop escaped the Linux boundary')\nprint('linux-stdout:'+os.getcwd(),flush=True)\nprint('linux-stderr',file=sys.stderr,flush=True)",
        10,
    );
    let first = await_final(first);
    assert_eq!(
        first.outcome,
        Some(stillyard::JobOutcome::Succeeded),
        "{first:#?}"
    );
    let stdout = client
        .logs(first.job_id, LogStream::Stdout, 0, 65536, deadline(), None)
        .unwrap();
    let stderr = client
        .logs(first.job_id, LogStream::Stderr, 0, 65536, deadline(), None)
        .unwrap();
    assert_eq!(
        String::from_utf8_lossy(&stdout.bytes).trim(),
        "linux-stdout:/tmp"
    );
    assert_eq!(
        String::from_utf8_lossy(&stderr.bytes).trim(),
        "linux-stderr"
    );
    let second = await_final(submit("import sys; sys.exit(25)", 10));
    assert_eq!(second.root_exit_code, Some(25), "{second:#?}");
    let third = await_final(submit(
        "import subprocess,time; subprocess.Popen(['/usr/bin/python3','-c','import time; time.sleep(10)'],start_new_session=True); time.sleep(20)",
        1,
    ));
    assert_eq!(third.root_exit_code, Some(124), "{third:#?}");
    let state = client.authority_status(deadline(), None).unwrap();
    assert!(state.blocker.is_none(), "{state:#?}");
    assert_eq!(state.holds.len(), 3);
    println!(
        "bootstrap normal/exit/timeout evidence: {}",
        serde_json::to_string(&state).unwrap()
    );
    assert!(
        state
            .holds
            .iter()
            .all(|hold| hold.released && hold.cleanup_proof.is_some())
    );

    let fourth = submit(
        "import time; print('long-work-started',flush=True); time.sleep(15)",
        20,
    );
    let startup_deadline = deadline();
    let operation_id = loop {
        let state = client.authority_status(deadline(), None).unwrap();
        if let Some(hold) = state.holds.iter().find(|hold| !hold.released) {
            let log = client
                .logs(fourth, LogStream::Stdout, 0, 65536, deadline(), None)
                .unwrap();
            if String::from_utf8_lossy(&log.bytes).contains("long-work-started") {
                break hold.id;
            }
        }
        assert!(
            Instant::now() < startup_deadline,
            "Linux command did not start"
        );
        std::thread::sleep(Duration::from_millis(50));
    };
    client.cancel(&[fourth], deadline(), None).unwrap();
    let fourth = await_final(fourth);
    assert_eq!(fourth.outcome, Some(stillyard::JobOutcome::Canceled));
    let state = client.authority_status(deadline(), None).unwrap();
    assert_eq!(state.blocker.as_deref(), Some("authority_held"));
    println!(
        "bootstrap after native cancel: {}",
        serde_json::to_string(&state).unwrap()
    );
    let marker = temp.path().join("after-bootstrap.txt");
    let canary = client
        .submit(
            command_spec(temp.path(), "echo allowed>after-bootstrap.txt"),
            &SubmitOptions::new(Uuid::now_v7()),
            deadline(),
            None,
        )
        .unwrap()
        .job_id;
    assert!(!marker.exists());
    let recovery_deadline = Instant::now() + Duration::from_secs(35);
    loop {
        let result = Command::new(&pinned)
            .args([
                "--endpoint",
                &endpoint,
                "bootstrap",
                "reconcile",
                &operation_id.to_string(),
            ])
            .output()
            .unwrap();
        if result.status.success() {
            break;
        }
        assert!(
            !marker.exists(),
            "canary released before authenticated cleanup proof"
        );
        assert!(
            Instant::now() < recovery_deadline,
            "reconciliation did not converge: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    let canary = await_final(canary);
    assert_eq!(canary.outcome, Some(stillyard::JobOutcome::Succeeded));
    assert!(marker.exists());

    let fifth = submit(
        "import time; print('reset-control-running',flush=True); time.sleep(20)",
        25,
    );
    let startup_deadline = deadline();
    let operation_id = loop {
        let state = client.authority_status(deadline(), None).unwrap();
        if let Some(hold) = state.holds.iter().find(|hold| !hold.released) {
            let log = client
                .logs(fifth, LogStream::Stdout, 0, 65536, deadline(), None)
                .unwrap();
            if String::from_utf8_lossy(&log.bytes).contains("reset-control-running") {
                println!(
                    "bootstrap before coordinator loss: {}",
                    serde_json::to_string(&state).unwrap()
                );
                break hold.id;
            }
        }
        assert!(Instant::now() < startup_deadline);
        std::thread::sleep(Duration::from_millis(50));
    };
    daemon.kill_and_wait();
    daemon = spawn_daemon(&pinned, &store, &endpoint);
    let restarted = connect_uninitialized(&pinned, &endpoint);
    assert_eq!(
        restarted
            .authority_status(deadline(), None)
            .unwrap()
            .blocker
            .as_deref(),
        Some("authority_held")
    );
    daemon.kill_and_wait();
    for suffix in ["", "-wal", "-shm"] {
        let path = store.join(format!("stillyard.sqlite3{suffix}"));
        if path.exists() {
            std::fs::remove_file(path).unwrap();
        }
    }
    let _replacement = spawn_daemon(&pinned, &store, &endpoint);
    let reset = connect_uninitialized(&pinned, &endpoint);
    let state = reset.authority_status(deadline(), None).unwrap();
    println!(
        "bootstrap after coordinator database reset: {}",
        serde_json::to_string(&state).unwrap()
    );
    assert_eq!(
        state.blocker.as_deref(),
        Some("authority_reconciliation_required")
    );
    assert!(
        state
            .holds
            .iter()
            .any(|hold| hold.id == operation_id && !hold.released)
    );
    std::fs::remove_file(&marker).unwrap();
    let canary = reset
        .submit(
            command_spec(temp.path(), "echo recovered>after-bootstrap.txt"),
            &SubmitOptions::new(Uuid::now_v7()),
            deadline(),
            None,
        )
        .unwrap()
        .job_id;
    assert!(!matches!(
        reset.wait_outcome(canary, Instant::now() + Duration::from_millis(500), None),
        WaitOutcome::Final { .. }
    ));
    assert!(
        !marker.exists(),
        "new SQLite issued capacity before Linux reconciliation"
    );
    let recovery_deadline = Instant::now() + Duration::from_secs(35);
    loop {
        let result = Command::new(&pinned)
            .args([
                "--endpoint",
                &endpoint,
                "bootstrap",
                "reconcile",
                &operation_id.to_string(),
            ])
            .output()
            .unwrap();
        if result.status.success() {
            break;
        }
        assert!(!marker.exists());
        assert!(
            Instant::now() < recovery_deadline,
            "reset recovery failed: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    // Sealing the Linux bootstrap boundary cannot erase the coordinator's
    // independent lost-history fence introduced by MR-2.
    assert_eq!(
        reset
            .authority_status(deadline(), None)
            .unwrap()
            .blocker
            .as_deref(),
        Some("authority_reconciliation_required")
    );
    assert!(
        !marker.exists(),
        "bootstrap cleanup incorrectly cleared coordinator history loss"
    );
    let restored = reset.machine_recover(deadline()).unwrap();
    assert!(restored.blocker.is_none(), "{restored:#?}");
    assert!(matches!(
        reset.wait_outcome(canary, deadline(), None),
        WaitOutcome::Final { .. }
    ));
    assert_eq!(
        reset.status(canary, deadline(), None).unwrap().outcome,
        Some(stillyard::JobOutcome::Succeeded)
    );
    assert!(marker.exists());
    println!(
        "bootstrap reconciled after reset: {}",
        serde_json::to_string(&reset.authority_status(deadline(), None).unwrap()).unwrap()
    );
}

#[test]
#[ignore = "native Windows installer acceptance inside a system Job"]
fn wsl_bootstrap_installer_refuses_busy_work_and_preserves_queue_and_authority() {
    use sha2::{Digest, Sha256};
    let temp = tempfile::tempdir().unwrap();
    let pinned = copied_daemon(temp.path());
    let store = temp.path().join("install-store");
    let endpoint = format!(r"\\.\pipe\stillyard-install-{}", Uuid::now_v7());
    let _daemon = spawn_daemon(&pinned, &store, &endpoint);
    let client = connect(&pinned, &endpoint);
    let deadline = || Instant::now() + Duration::from_secs(20);
    let before = client.daemon_status(deadline(), None).unwrap();
    let candidate = PathBuf::from(env!("CARGO_BIN_EXE_stillyard"));
    let hash = format!("{:x}", Sha256::digest(std::fs::read(&candidate).unwrap()));
    let python = std::env::var("MR_WINDOWS_PYTHON").expect("explicit native Windows Python");
    let build_job =
        std::env::var("STILLYARD_JOB_ID").expect("system Job schedules installer acceptance");
    let install = || {
        Command::new(&python)
            .arg(
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scripts/install-windows-daemon.py"),
            )
            .arg("--candidate")
            .arg(&candidate)
            .args(["--candidate-sha256", &hash, "--build-job-id", &build_job])
            .arg("--evidence-directory")
            .arg(temp.path().join("install-evidence"))
            .arg("--installed")
            .arg(&pinned)
            .args(["--endpoint", &endpoint, "--wait-seconds", "0", "--apply"])
            .output()
            .unwrap()
    };
    let busy = client.submit(command_spec(temp.path(), "echo active>install-busy.txt & C:\\Windows\\System32\\ping.exe -n 8 127.0.0.1 >nul"), &SubmitOptions::new(Uuid::now_v7()), deadline(), None).unwrap().job_id;
    let startup_deadline = deadline();
    while !temp.path().join("install-busy.txt").exists() {
        assert!(Instant::now() < startup_deadline);
        std::thread::sleep(Duration::from_millis(20));
    }
    let refused = install();
    assert!(!refused.status.success(), "installer stopped busy work");
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("active or uncertain work"),
        "{}",
        String::from_utf8_lossy(&refused.stderr)
    );
    assert_eq!(
        client.daemon_status(deadline(), None).unwrap().pid,
        before.pid
    );
    assert!(matches!(
        client.wait_outcome(busy, deadline(), None),
        WaitOutcome::Final { .. }
    ));
    // Reproduce an already resolved historical incident without changing any
    // production data. The completed fixture's boundary was proven empty above.
    let completed = client.status(busy, deadline(), None).unwrap();
    let containment = completed.containment_id.unwrap().entity_uuid().to_string();
    let audit = serde_json::json!({"resolved_unix_millis": completed.finished_unix_millis.unwrap(),
        "daemon_generation": before.daemon_generation, "resolution": "proven_empty",
        "last_reconciliation": "proven_empty", "origin": "automatic", "forced": null, "lease_released": true});
    rusqlite::Connection::open(store.join("stillyard.sqlite3")).unwrap().execute(
        "UPDATE containments SET state='cleared', resolution='proven_empty', resolution_audit_json=?2 WHERE id=?1 AND state='empty'",
        rusqlite::params![containment, audit.to_string()],
    ).unwrap();
    let hold = Uuid::now_v7();
    client
        .hold_authority(hold, "installation fixture".into(), deadline(), None)
        .unwrap();
    let queued = client
        .submit(
            command_spec(temp.path(), "echo preserved>install-canary.txt"),
            &SubmitOptions::new(Uuid::now_v7()),
            deadline(),
            None,
        )
        .unwrap()
        .job_id;
    let installed = install();
    assert!(
        installed.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&installed.stdout),
        String::from_utf8_lossy(&installed.stderr)
    );
    let after_client = connect_uninitialized(&pinned, &endpoint);
    let after = after_client.daemon_status(deadline(), None).unwrap();
    let _cleanup = ExternalDaemonGuard::from_snapshot(&after);
    assert_ne!(after.daemon_generation, before.daemon_generation);
    assert_eq!(after.store_uuid, before.store_uuid);
    assert_eq!(
        after_client
            .authority_status(deadline(), None)
            .unwrap()
            .blocker
            .as_deref(),
        Some("authority_held")
    );
    assert!(!temp.path().join("install-canary.txt").exists());
    after_client
        .force_release_authority(
            hold,
            "fixture contains no external work".into(),
            deadline(),
            None,
        )
        .unwrap();
    assert!(matches!(
        after_client.wait_outcome(queued, deadline(), None),
        WaitOutcome::Final { .. }
    ));
    assert!(temp.path().join("install-canary.txt").exists());
    println!(
        "installer preserved store and queue: {}",
        String::from_utf8_lossy(&installed.stdout)
    );
}

struct ExternalDaemonGuard(windows_sys::Win32::Foundation::HANDLE);

impl ExternalDaemonGuard {
    fn from_snapshot(snapshot: &DaemonSnapshot) -> Self {
        use windows_sys::Win32::Foundation::{CloseHandle, FILETIME};
        use windows_sys::Win32::System::Threading::{
            GetProcessTimes, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_TERMINATE,
        };
        const SYNCHRONIZE_ACCESS: u32 = 0x0010_0000;
        let Some(stillyard::ProcessIdentity::Windows {
            creation_filetime_100ns,
            ..
        }) = snapshot.process_identity.as_ref()
        else {
            panic!("test daemon identity missing");
        };
        // SAFETY: OpenProcess returns a new owned handle; subsequent calls use
        // writable FILETIME buffers. Identity is verified before ownership allows kill.
        unsafe {
            let handle = OpenProcess(
                PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_TERMINATE | SYNCHRONIZE_ACCESS,
                0,
                snapshot.pid,
            );
            assert!(!handle.is_null());
            let mut created: FILETIME = std::mem::zeroed();
            let mut exited: FILETIME = std::mem::zeroed();
            let mut kernel: FILETIME = std::mem::zeroed();
            let mut user: FILETIME = std::mem::zeroed();
            let queried =
                GetProcessTimes(handle, &mut created, &mut exited, &mut kernel, &mut user);
            let actual =
                (u64::from(created.dwHighDateTime) << 32) | u64::from(created.dwLowDateTime);
            if queried == 0 || actual != *creation_filetime_100ns {
                CloseHandle(handle);
                panic!("test daemon process identity changed");
            }
            Self(handle)
        }
    }
}

impl Drop for ExternalDaemonGuard {
    fn drop(&mut self) {
        // SAFETY: the guard owns a verified handle to the isolated test daemon.
        unsafe {
            windows_sys::Win32::System::Threading::TerminateProcess(self.0, 0);
            windows_sys::Win32::System::Threading::WaitForSingleObject(self.0, 10000);
            windows_sys::Win32::Foundation::CloseHandle(self.0);
        }
    }
}

#[test]
fn probe_condition_runs_in_its_own_invocation_before_primary_release() {
    let temp = tempfile::tempdir().unwrap();
    let pinned = copied_daemon(temp.path());
    let store = temp.path().join("condition-store");
    let endpoint = format!(r"\\.\pipe\stillyard-condition-{}", Uuid::now_v7());
    let _daemon = spawn_daemon(&pinned, &store, &endpoint);
    let client = connect(&pinned, &endpoint);
    let marker = temp.path().join("primary-ran.txt");
    let mut spec = command_spec(temp.path(), &format!("echo primary>{}", marker.display()));
    spec.conditions.push(ConditionSpec {
        predicate: ConditionPredicate::Probe {
            probe: Box::new(ProbeCondition {
                executable: PathBuf::from(r"C:\Windows\System32\cmd.exe"),
                args: vec!["/d".into(), "/c".into(), "echo probe-ready & exit 0".into()],
                working_directory: temp.path().to_path_buf(),
                environment: EnvironmentSpec::default(),
                resources: ResourceClaims::default(),
                timeout_seconds: 5,
                interval_seconds: 1,
                accepted_exit_codes: vec![0],
            }),
        },
        deadline: ConditionDeadline::Relative { seconds: 10 },
        on_deadline: ConditionDeadlineOutcome::Failed,
    });
    let receipt = client
        .submit(
            spec,
            &SubmitOptions::new(Uuid::now_v7()),
            Instant::now() + Duration::from_secs(5),
            None,
        )
        .unwrap();
    assert!(matches!(
        client.wait_outcome(
            receipt.job_id,
            Instant::now() + Duration::from_secs(15),
            None,
        ),
        WaitOutcome::Final { .. }
    ));
    let snapshot = client
        .status(
            receipt.job_id,
            Instant::now() + Duration::from_secs(2),
            None,
        )
        .unwrap();
    assert_eq!(
        snapshot.outcome,
        Some(stillyard::JobOutcome::Succeeded),
        "{snapshot:#?}"
    );
    assert!(marker.is_file(), "primary command was never released");
    assert!(matches!(
        snapshot.conditions[0]
            .last_observation
            .as_ref()
            .map(|observation| &observation.value),
        Some(ConditionObservationValue::Probe {
            exit_code: Some(0),
            timed_out: false,
            accepted: true,
        })
    ));
    let probe = snapshot.attempts[0]
        .invocations
        .iter()
        .find(|invocation| invocation.role == InvocationRole::Probe)
        .expect("probe Invocation is public");
    assert_eq!(
        probe.exit_classification,
        Some(ExitClassification::Accepted)
    );
    assert!(probe.stdout_tail.contains("probe-ready"));
    let probe_grant = snapshot.allocations.iter().find(|g|matches!(g.owner,
        stillyard::machine::AllocationOwner::Probe { invocation_id, .. } if invocation_id == probe.invocation_id)).expect("probe has its own public allocation");
    let work_grant = snapshot
        .allocations
        .iter()
        .find(|g| matches!(g.owner, stillyard::machine::AllocationOwner::Work { .. }))
        .expect("primary has a public work allocation");
    assert_ne!(probe_grant.grant_id, work_grant.grant_id);
    assert_eq!(probe_grant.state, stillyard::machine::GrantState::Released);
    assert_eq!(work_grant.state, stillyard::machine::GrantState::Released);
    assert_eq!(
        snapshot.attempts[0]
            .invocations
            .last()
            .map(|invocation| invocation.role),
        Some(InvocationRole::Primary)
    );
}

#[test]
fn path_condition_freshness_wakes_scheduler_and_rescans_without_manual_signal() {
    let temp = tempfile::tempdir().unwrap();
    let pinned = copied_daemon(temp.path());
    let store = temp.path().join("path-condition-store");
    let endpoint = format!(r"\\.\pipe\stillyard-path-condition-{}", Uuid::now_v7());
    let _daemon = spawn_daemon(&pinned, &store, &endpoint);
    let client = connect(&pinned, &endpoint);
    let ready = temp.path().join("ready.flag");
    let marker = temp.path().join("path-primary-ran.txt");
    let mut spec = command_spec(temp.path(), &format!("echo primary>{}", marker.display()));
    spec.conditions.push(ConditionSpec {
        predicate: ConditionPredicate::PathExists {
            path: ready.clone(),
        },
        deadline: ConditionDeadline::Relative { seconds: 5 },
        on_deadline: ConditionDeadlineOutcome::Failed,
    });
    let receipt = client
        .submit(
            spec,
            &SubmitOptions::new(Uuid::now_v7()),
            Instant::now() + Duration::from_secs(5),
            None,
        )
        .unwrap();
    assert!(matches!(
        receipt.conditions[0]
            .last_observation
            .as_ref()
            .map(|observation| &observation.value),
        Some(ConditionObservationValue::Path { exists: false })
    ));
    std::fs::write(&ready, b"ready").unwrap();
    assert!(matches!(
        client.wait_outcome(
            receipt.job_id,
            Instant::now() + Duration::from_secs(10),
            None,
        ),
        WaitOutcome::Final { .. }
    ));
    let snapshot = client
        .status(
            receipt.job_id,
            Instant::now() + Duration::from_secs(2),
            None,
        )
        .unwrap();
    assert_eq!(snapshot.outcome, Some(stillyard::JobOutcome::Succeeded));
    assert!(marker.is_file());
    assert!(matches!(
        snapshot.conditions[0]
            .last_observation
            .as_ref()
            .map(|observation| &observation.value),
        Some(ConditionObservationValue::Path { exists: true })
    ));
}

#[test]
fn timed_out_probe_is_observable_and_condition_deadline_prevents_primary_release() {
    let temp = tempfile::tempdir().unwrap();
    let pinned = copied_daemon(temp.path());
    let store = temp.path().join("probe-timeout-store");
    let endpoint = format!(r"\\.\pipe\stillyard-probe-timeout-{}", Uuid::now_v7());
    let _daemon = spawn_daemon(&pinned, &store, &endpoint);
    let client = connect(&pinned, &endpoint);
    let marker = temp.path().join("must-not-run.txt");
    let mut spec = command_spec(temp.path(), &format!("echo forbidden>{}", marker.display()));
    spec.conditions.push(ConditionSpec {
        predicate: ConditionPredicate::Probe {
            probe: Box::new(ProbeCondition {
                executable: PathBuf::from(r"C:\Windows\System32\cmd.exe"),
                args: vec![
                    "/d".into(),
                    "/c".into(),
                    r"C:\Windows\System32\ping.exe -n 10 127.0.0.1 >nul".into(),
                ],
                working_directory: temp.path().to_path_buf(),
                environment: EnvironmentSpec::default(),
                resources: ResourceClaims::default(),
                timeout_seconds: 1,
                interval_seconds: 1,
                accepted_exit_codes: vec![0],
            }),
        },
        deadline: ConditionDeadline::Relative { seconds: 2 },
        on_deadline: ConditionDeadlineOutcome::Failed,
    });
    let receipt = client
        .submit(
            spec,
            &SubmitOptions::new(Uuid::now_v7()),
            Instant::now() + Duration::from_secs(5),
            None,
        )
        .unwrap();
    assert!(matches!(
        client.wait_outcome(
            receipt.job_id,
            Instant::now() + Duration::from_secs(10),
            None,
        ),
        WaitOutcome::Final { .. }
    ));
    let snapshot = client
        .status(
            receipt.job_id,
            Instant::now() + Duration::from_secs(2),
            None,
        )
        .unwrap();
    assert_eq!(snapshot.outcome, Some(stillyard::JobOutcome::Failed));
    assert_eq!(
        snapshot.reason_code.as_deref(),
        Some("condition_deadline_expired")
    );
    assert!(!marker.exists(), "primary ran after Condition deadline");
    assert!(
        matches!(
            snapshot.conditions[0]
                .last_observation
                .as_ref()
                .map(|observation| &observation.value),
            Some(ConditionObservationValue::Probe {
                timed_out: true,
                accepted: false,
                ..
            })
        ),
        "{snapshot:#?}"
    );
}

fn seed_unresolved_incidents(store: &Path, count: u64) {
    let connection = rusqlite::Connection::open(store.join("stillyard.sqlite3")).unwrap();
    connection
        .pragma_update(None, "foreign_keys", true)
        .unwrap();
    let spec = JobSpec {
        spec_version: SPEC_VERSION,
        priority: stillyard::NEUTRAL_JOB_PRIORITY,
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
    // Independent observations have distinct capture times, while all identity,
    // resource and runtime state must still agree for this idle instance.
    let mut comparable_daemon = doctor.daemon.clone();
    let observed = comparable_daemon.machine_scheduling.as_mut().unwrap();
    assert!(observed.observed_unix_millis > 0);
    observed.observed_unix_millis = status_b
        .machine_scheduling
        .as_ref()
        .unwrap()
        .observed_unix_millis;
    assert_eq!(comparable_daemon, status_b);
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
    let mut cli_doctor: DoctorSnapshot = serde_json::from_slice(&cli_doctor.stdout).unwrap();
    let observed = cli_doctor.daemon.machine_scheduling.as_mut().unwrap();
    assert!(observed.observed_unix_millis > 0);
    observed.observed_unix_millis = doctor
        .daemon
        .machine_scheduling
        .as_ref()
        .unwrap()
        .observed_unix_millis;
    assert_eq!(cli_doctor.daemon, doctor.daemon);
    assert_eq!(cli_doctor.store, doctor.store);

    let cli_context = Command::new(&pinned)
        .args(["--endpoint", &endpoint_b, "context", "--json"])
        .env_remove("STILLYARD_ENDPOINT")
        .env_remove("STILLYARD_JOB_ID")
        .env_remove("STILLYARD_ATTEMPT")
        .env_remove("STILLYARD_INVOCATION_ID")
        .env_remove("STILLYARD_ROLE")
        .output()
        .unwrap();
    assert!(
        cli_context.status.success(),
        "context CLI failed: {}",
        String::from_utf8_lossy(&cli_context.stderr)
    );
    let cli_context: stillyard::SubmissionContext =
        serde_json::from_slice(&cli_context.stdout).unwrap();
    assert_eq!(cli_context.store_uuid, status_b.store_uuid);
    assert_eq!(cli_context.parent, None);

    let nested = JobSpec {
        spec_version: SPEC_VERSION,
        priority: stillyard::NEUTRAL_JOB_PRIORITY,
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

    let mut nested_context = command_spec(temp.path(), "exit 1");
    nested_context.executable = pinned.clone();
    nested_context.args = vec!["context".into(), "--json".into()];
    nested_context.child_submission_policy = Some(stillyard::ChildSubmissionPolicy::default());
    let context_receipt = client_b
        .submit(
            nested_context,
            &SubmitOptions::new(Uuid::now_v7()),
            Instant::now() + Duration::from_secs(5),
            None,
        )
        .unwrap();
    let context_snapshot = client_b
        .wait(
            context_receipt.job_id,
            Instant::now() + Duration::from_secs(10),
            None,
        )
        .unwrap();
    assert_eq!(
        context_snapshot.outcome,
        Some(stillyard::JobOutcome::Succeeded)
    );
    let context_output = client_b
        .logs(
            context_receipt.job_id,
            LogStream::Stdout,
            0,
            64 * 1024,
            Instant::now() + Duration::from_secs(2),
            None,
        )
        .unwrap();
    let nested_context: stillyard::SubmissionContext =
        serde_json::from_slice(&context_output.bytes).unwrap();
    let nested_parent = nested_context.parent.expect("managed CLI caller");
    assert_eq!(nested_context.store_uuid, status_b.store_uuid);
    assert_eq!(nested_parent.job_id, context_receipt.job_id);
    assert_eq!(
        nested_parent.attempt_id,
        context_snapshot.attempts[0].attempt_id
    );
    assert_eq!(
        nested_parent.invocation_id,
        context_snapshot.attempts[0].invocations[0].invocation_id
    );

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
    let cli = Command::new(std::env::var_os("ISOLATED_DAEMON_EXECUTABLE").unwrap())
        .args(["context", "--json"])
        .output()
        .unwrap();
    assert!(!cli.status.success());
    assert!(cli.stdout.is_empty(), "failed attestation emitted JSON");
    assert!(
        String::from_utf8_lossy(&cli.stderr)
            .contains("claimed managed parent does not match daemon-held OS containment")
    );
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
        priority: stillyard::NEUTRAL_JOB_PRIORITY,
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

#[path = "support/machine_clearance.rs"]
mod machine_clearance;
#[path = "support/machine_manager.rs"]
mod machine_manager;

#[test]
fn bootstrap_native_controller_rejects_unmanaged_unbounded_and_failed_spawn_before_arm() {
    let temp = tempfile::tempdir().unwrap();
    let pinned = copied_daemon(temp.path());
    let endpoint = format!(
        r"\\.\pipe\stillyard-bootstrap-controller-{}",
        Uuid::now_v7()
    );
    let _daemon = spawn_daemon(&pinned, &temp.path().join("store"), &endpoint);
    let client = connect(&pinned, &endpoint);
    let deadline = || Instant::now() + Duration::from_secs(20);
    let work = stillyard::BootstrapWork {
        operation_id: Uuid::nil(),
        distribution: "must-not-be-called".into(),
        user: "must-not-be-called".into(),
        executable: "/usr/bin/true".into(),
        args: vec![],
        working_directory: "/tmp".into(),
        environment: Default::default(),
        timeout_seconds: 1,
        delegate_test_cgroup: false,
    };
    let work_path = temp.path().join("work.json");
    std::fs::write(&work_path, serde_json::to_vec(&work).unwrap()).unwrap();
    let marker = temp.path().join("controller-started.txt");
    let arguments = vec![
        "--endpoint".into(),
        endpoint.clone(),
        "bootstrap".into(),
        "run".into(),
        "--spec".into(),
        work_path.display().to_string(),
        "--native-controller".into(),
        r"C:\Windows\System32\cmd.exe".into(),
        "--".into(),
        "/d".into(),
        "/c".into(),
        format!("echo escaped>\"{}\"", marker.display()),
    ];
    let outside = Command::new(&pinned).args(&arguments).output().unwrap();
    assert!(!outside.status.success());
    assert!(String::from_utf8_lossy(&outside.stderr).contains("bounded native Stillyard Job"));
    assert!(!marker.exists());
    assert!(
        client
            .authority_status(deadline(), None)
            .unwrap()
            .holds
            .is_empty()
    );

    let mut spec = command_spec(temp.path(), "unused");
    spec.executable = pinned.clone();
    spec.args = arguments.clone();
    spec.timeout_seconds = None;
    let receipt = client
        .submit(
            spec.clone(),
            &SubmitOptions::new(Uuid::now_v7()),
            deadline(),
            None,
        )
        .unwrap();
    assert_eq!(
        client
            .wait(receipt.job_id, deadline(), None)
            .unwrap()
            .outcome,
        Some(stillyard::JobOutcome::Failed)
    );
    let log = client
        .logs(
            receipt.job_id,
            LogStream::Stderr,
            0,
            65536,
            deadline(),
            None,
        )
        .unwrap();
    assert!(String::from_utf8_lossy(&log.bytes).contains("finite Job timeout"));
    assert!(!marker.exists());
    assert!(
        client
            .authority_status(deadline(), None)
            .unwrap()
            .holds
            .is_empty()
    );

    // Spawn failure must precede Arm: there can be no new Linux record/seal to
    // reconcile when a nonexistent native controller never started.
    spec.timeout_seconds = Some(10);
    spec.args[7] = temp
        .path()
        .join("missing-controller.exe")
        .display()
        .to_string();
    let receipt = client
        .submit(spec, &SubmitOptions::new(Uuid::now_v7()), deadline(), None)
        .unwrap();
    assert_eq!(
        client
            .wait(receipt.job_id, deadline(), None)
            .unwrap()
            .outcome,
        Some(stillyard::JobOutcome::Failed)
    );
    assert!(
        client
            .authority_status(deadline(), None)
            .unwrap()
            .holds
            .is_empty()
    );
    // A postcondition is a different Invocation role, even though its PID is
    // the current root and it shares the finite work Lease.
    let mut postcondition = command_spec(temp.path(), "exit 0");
    postcondition.postconditions.push(
        serde_json::from_value(serde_json::json!({
            "executable":pinned,"args":arguments
        }))
        .unwrap(),
    );
    let receipt = client
        .submit(
            postcondition,
            &SubmitOptions::new(Uuid::now_v7()),
            deadline(),
            None,
        )
        .unwrap();
    let status = client.wait(receipt.job_id, deadline(), None).unwrap();
    assert_eq!(status.outcome, Some(stillyard::JobOutcome::Failed));
    assert_eq!(status.attempts[0].invocations.len(), 2);
    assert!(!marker.exists());
    assert!(
        client
            .authority_status(deadline(), None)
            .unwrap()
            .holds
            .is_empty()
    );
}
