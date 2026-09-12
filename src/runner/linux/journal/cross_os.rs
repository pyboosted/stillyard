//! Real Linux executor subject for the native coordinator reset acceptance.
//! Runs only inside the default Windows Job's protected bootstrap delegation.
use super::*;
use crate::store::{Store, StorePaths};
use std::os::unix::fs::PermissionsExt;
use std::time::Duration;

fn receive<T: serde::de::DeserializeOwned>(mailbox: &Path, name: &str) -> T {
    let until = Instant::now() + Duration::from_secs(120);
    loop {
        assert!(
            !mailbox.join("abort.json").exists(),
            "native controller aborted before {name}"
        );
        match std::fs::read(mailbox.join(name)) {
            Ok(bytes) => return serde_json::from_slice(&bytes).unwrap(),
            Err(error) if error.kind() == io::ErrorKind::NotFound && Instant::now() < until => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("cross-OS mailbox {name}: {error}"),
        }
    }
}
fn publish(mailbox: &Path, name: &str, value: &impl Serialize) {
    let temporary = mailbox.join(format!(".{}", Uuid::now_v7()));
    std::fs::write(&temporary, serde_json::to_vec(value).unwrap()).unwrap();
    std::fs::rename(temporary, mailbox.join(name)).unwrap();
}

#[test]
#[ignore = "native system Job supplies mailbox and protected Linux delegation"]
fn linux_executor_cross_os_reset_subject() {
    let mailbox = PathBuf::from(std::env::var_os("MR_WSL_FAULT_MAILBOX").unwrap());
    let _: bool = receive(&mailbox, "controller-ready.json");
    let parent = PathBuf::from(std::env::var_os("STILLYARD_TEST_CGROUP_ROOT").unwrap());
    let temp = tempfile::tempdir_in(env!("CARGO_MANIFEST_DIR")).unwrap();
    std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let paths = StorePaths::new(temp.path().join("manager"));
    let mut manager = Store::open(paths.clone()).unwrap();
    // SAFETY: geteuid has no preconditions.
    let uid = unsafe { libc::geteuid() };
    std::fs::set_permissions(&paths.root, std::fs::Permissions::from_mode(0o700)).unwrap();
    publish(
        &mailbox,
        "manager.json",
        &serde_json::json!({"store_uuid":manager.store_uuid(),"uid":uid}),
    );
    let configuration: serde_json::Value = receive(&mailbox, "configuration.json");
    let configuration_path = temp.path().join("configuration.json");
    std::fs::write(
        &configuration_path,
        serde_json::to_vec(&configuration).unwrap(),
    )
    .unwrap();
    std::fs::set_permissions(&configuration_path, std::fs::Permissions::from_mode(0o600)).unwrap();
    manager
        .install_attached_from_file(&configuration_path)
        .unwrap();
    let anchor = Anchor {
        journal: serde_json::from_value(configuration["journal"].clone()).unwrap(),
        store: manager.store_uuid(),
        domain: serde_json::from_value(
            configuration["pairing"]["installation"]["domain_id"].clone(),
        )
        .unwrap(),
    };
    let journal_path = paths.root.join("attachment/executor");
    let mut journal = Journal::open(&journal_path, &anchor).unwrap();
    let allocation: crate::machine::AllocationKey = receive(&mailbox, "allocation.json");
    assert_eq!(allocation.manager_store_uuid, anchor.store);
    assert_eq!(allocation.domain_id, anchor.domain);
    let invocation = InvocationId::from_parts(anchor.store, Uuid::now_v7());
    let containment = ContainmentId::from_parts(anchor.store, Uuid::now_v7());
    // SAFETY: geteuid has no preconditions.
    let identity =
        crate::identity::linux::Process::open(std::process::id(), unsafe { libc::geteuid() })
            .unwrap();
    let boundary = journal
        .create(
            &parent,
            invocation,
            containment,
            allocation.lease_id,
            Uuid::now_v7(),
            identity.identity,
        )
        .unwrap();
    let helper = temp.path().join("stillyard");
    std::fs::copy(
        std::env::var_os("STILLYARD_TEST_EXECUTABLE").unwrap(),
        &helper,
    )
    .unwrap();
    let marker = temp.path().join("user-started");
    let code = format!(
        "import pathlib,subprocess,time; subprocess.Popen(['/usr/bin/python3','-c','import time;time.sleep(120)']); pathlib.Path({}).write_text('live'); time.sleep(120)",
        serde_json::to_string(&marker.display().to_string()).unwrap()
    );
    let spec = super::super::launch::LaunchSpec {
        executable: "/usr/bin/python3".into(),
        args: vec!["-c".into(), code],
        working_directory: temp.path().into(),
        environment: BTreeMap::new(),
    };
    let mut launch = super::super::launch::PreparedLaunch::prepare(
        &boundary,
        &helper,
        &spec,
        File::open("/dev/null").unwrap(),
        Instant::now() + Duration::from_secs(15),
    )
    .unwrap();
    journal.ready(invocation, &launch).unwrap();
    let intent = crate::machine::InvocationIntent {
        invocation_id: invocation,
        containment_id: containment,
        role: crate::InvocationRole::Primary,
        role_index: 0,
        release_sequence: 1,
        executable_sha256: launch.requested_sha256.clone(),
        boundary_sha256: journal.records().unwrap()[&invocation]
            .boundary_sha256()
            .unwrap(),
        readiness_challenge: Uuid::now_v7(),
        previous_cleanup: None,
    };
    publish(&mailbox, "intent.json", &intent);
    let ticket: crate::machine::InvocationTicket = receive(&mailbox, "ticket.json");
    assert_eq!(ticket.intent, intent);
    assert_eq!(ticket.key, allocation);
    journal.release_intent(&ticket).unwrap();
    launch.release().unwrap();
    let until = Instant::now() + Duration::from_secs(10);
    while !marker.exists() {
        assert!(
            Instant::now() < until,
            "actual Linux user code did not start"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    publish(
        &mailbox,
        "live.json",
        &serde_json::json!({"record":journal.records().unwrap()[&invocation],"populated":boundary.populated().unwrap(),"user_started":marker.exists()}),
    );
    let _: serde_json::Value = receive(&mailbox, "capacity-reduced.json");
    assert!(boundary.populated().unwrap());
    assert!(journal.records().unwrap()[&invocation].seal.is_none());
    publish(
        &mailbox,
        "live-after-capacity-reduction.json",
        &serde_json::json!({
            "populated":boundary.populated().unwrap(), "unsealed":journal.records().unwrap()[&invocation].seal.is_none(),
            "record":journal.records().unwrap()[&invocation]
        }),
    );
    let _: bool = receive(&mailbox, "reset-guest.json");
    assert!(boundary.populated().unwrap());
    assert!(journal.records().unwrap()[&invocation].seal.is_none());
    drop(manager);
    for suffix in ["", "-wal", "-shm"] {
        let source = paths.root.join(format!("stillyard.sqlite3{suffix}"));
        if source.exists() {
            std::fs::rename(&source, source.with_extension(format!("prior{suffix}"))).unwrap();
        }
    }
    let reset_error = Store::open(paths.clone())
        .err()
        .expect("empty local Store bypassed retained pairing history");
    assert!(
        matches!(&reset_error, crate::store::StoreError::InvalidState(message) if message.contains("admission remains fenced")),
        "unexpected reset rejection: {reset_error}"
    );
    let anchor_path = paths.root.join("attachment/anchor.json");
    let saved_anchor = std::fs::read(&anchor_path).unwrap();
    std::fs::remove_file(&anchor_path).unwrap();
    let missing_error = Store::open(paths.clone())
        .err()
        .expect("lost anchor became standalone");
    assert!(
        matches!(&missing_error, crate::store::StoreError::Io(error) if error.kind()==io::ErrorKind::NotFound),
        "unexpected missing-anchor rejection: {missing_error}"
    );
    std::fs::write(&anchor_path, b"{corrupt").unwrap();
    std::fs::set_permissions(&anchor_path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let corrupt_error = Store::open(paths.clone())
        .err()
        .expect("corrupt anchor became standalone");
    assert!(
        corrupt_error.to_string().contains("key must be a string"),
        "unexpected corrupt-anchor rejection: {corrupt_error}"
    );
    std::fs::write(&anchor_path, &saved_anchor).unwrap();
    let executor_anchor = journal_path.join("anchor.json");
    let executor_anchor_backup = journal_path.join("anchor.test-backup");
    let executor_anchor_bytes = std::fs::read(&executor_anchor).unwrap();
    std::fs::rename(&executor_anchor, &executor_anchor_backup).unwrap();
    let missing_executor_error = journal
        .records()
        .err()
        .expect("live executor accepted missing anchor");
    std::fs::rename(&executor_anchor_backup, &executor_anchor).unwrap();
    std::fs::write(&executor_anchor, b"{corrupt").unwrap();
    let corrupt_executor_error = journal
        .records()
        .err()
        .expect("live executor accepted corrupt anchor");
    std::fs::write(&executor_anchor, executor_anchor_bytes).unwrap();
    drop(journal);
    journal = Journal::open(&journal_path, &anchor).unwrap();
    assert!(journal.records().unwrap()[&invocation].seal.is_none());
    assert!(boundary.populated().unwrap());
    publish(
        &mailbox,
        "guest-reset-rejected.json",
        &serde_json::json!({"populated":boundary.populated().unwrap(),"unsealed":journal.records().unwrap()[&invocation].seal.is_none(),"store_reset_rejection":reset_error.to_string(),"missing_anchor_rejection":missing_error.to_string(),"corrupt_anchor_rejection":corrupt_error.to_string(),"missing_executor_anchor_rejection":missing_executor_error.to_string(),"corrupt_executor_anchor_rejection":corrupt_executor_error.to_string(),"record":journal.records().unwrap()[&invocation]}),
    );
    let _: serde_json::Value = receive(&mailbox, "peer-fenced.json");
    assert!(boundary.populated().unwrap());
    assert!(journal.records().unwrap()[&invocation].seal.is_none());
    publish(
        &mailbox,
        "live-after-peer-fence.json",
        &serde_json::json!({
            "populated":boundary.populated().unwrap(), "unsealed":journal.records().unwrap()[&invocation].seal.is_none(),
            "record":journal.records().unwrap()[&invocation]
        }),
    );
    let _: bool = receive(&mailbox, "coordinator-reset-observed.json");
    assert!(boundary.populated().unwrap());
    publish(
        &mailbox,
        "live-after-coordinator-reset.json",
        &serde_json::json!({"populated":boundary.populated().unwrap(),"unsealed":journal.records().unwrap()[&invocation].seal.is_none()}),
    );
    let _: bool = receive(&mailbox, "cleanup.json");
    let seal = journal
        .cleanup(invocation, Instant::now() + Duration::from_secs(5))
        .unwrap();
    assert!(seal.possibly_released);
    assert!(!Path::new(&boundary.identity.path).exists());
    publish(
        &mailbox,
        "seal.json",
        &serde_json::json!({"seal":seal,"proof_sha256":seal.sha256().unwrap(),"record":journal.records().unwrap()[&invocation]}),
    );
}
