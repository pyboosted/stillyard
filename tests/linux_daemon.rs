#![cfg(target_os = "linux")]
//! Isolated subjects; Cargo itself is scheduled by the system default daemon.
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use stillyard::{Client, Error, JobSpec, SubmitOptions};

// A fork in another libtest thread can temporarily inherit the writable fd
// used by fs::copy, even with CLOEXEC, causing exec of that fresh file to fail
// with ETXTBSY (rust-lang/rust#114554). Serialize these two fixture lifetimes;
// each test still runs the actual competing daemon processes concurrently.
static FIXTURE_LIFETIME: std::sync::Mutex<()> = std::sync::Mutex::new(());

struct Daemon(Child);
impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn spawn(exe: &Path, store: &Path, socket: &Path) -> Daemon {
    Daemon(
        Command::new(exe)
            .arg("--endpoint")
            .arg(socket)
            .args(["daemon", "--store"])
            .arg(store)
            .env_remove("STILLYARD_STORE")
            .env_remove("STILLYARD_ENDPOINT")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    )
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(10)
}
fn connect(exe: &Path, socket: &Path) -> Client {
    let until = deadline();
    loop {
        match Client::builder()
            .endpoint(socket.to_str().unwrap())
            .daemon_executable(exe)
            .auto_start(false)
            .connect(Instant::now() + Duration::from_millis(200), None)
        {
            Ok(client) => return client,
            Err(Error::Unavailable(_) | Error::DeadlineElapsed) if Instant::now() < until => {
                std::thread::sleep(Duration::from_millis(20))
            }
            Err(error) => panic!("Linux daemon failed to become ready: {error}"),
        }
    }
}
fn pinned(root: &Path) -> PathBuf {
    let path = root.join("stillyard");
    std::fs::copy(env!("CARGO_BIN_EXE_stillyard"), &path).unwrap();
    path
}
fn assert_exits(daemon: &mut Daemon) {
    let until = deadline();
    loop {
        if let Some(status) = daemon.0.try_wait().unwrap() {
            assert!(!status.success());
            return;
        }
        assert!(Instant::now() < until, "contending daemon did not exit");
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn linux_daemon_singletons_and_crash_recovery_preserve_store_and_queue() {
    let _fixture = FIXTURE_LIFETIME
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    // The WSL contract requires ext4. /tmp may be tmpfs even when the checkout
    // and real store are ext4, so place this isolated store in the source volume.
    let temp = tempfile::tempdir_in(env!("CARGO_MANIFEST_DIR")).unwrap();
    std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let exe = pinned(temp.path());
    let socket = temp.path().join("a.sock");
    let store = temp.path().join("store-a");
    let daemon = spawn(&exe, &store, &socket);
    let client = connect(&exe, &socket);
    let first = client.daemon_status(deadline(), None).unwrap();
    assert_eq!(first.pid, daemon.0.id());
    assert_eq!(std::fs::metadata(&socket).unwrap().mode() & 0o777, 0o600);
    assert!(
        client
            .submission_context(deadline(), None)
            .unwrap()
            .parent
            .is_none()
    );
    // Runtime authority cannot be initialized before actual execution capability exists.
    assert!(
        client
            .initialize_authority_without_outstanding_work(deadline(), None)
            .is_err()
    );
    let spec: JobSpec = serde_json::from_value(serde_json::json!({
        "spec_version": stillyard::SPEC_VERSION,
        "executable": "/usr/bin/true", "args": [], "working_directory": temp.path(),
        "stdin": {"kind": "eof"}
    }))
    .unwrap();
    let receipt = client
        .submit(
            spec,
            &SubmitOptions::new(uuid::Uuid::now_v7()),
            deadline(),
            None,
        )
        .unwrap();
    let waiting = client.status(receipt.job_id, deadline(), None).unwrap();
    assert!(waiting.started_unix_millis.is_none());
    assert!(
        waiting
            .blockers
            .iter()
            .any(|b| b.code == "host_capability_unavailable")
    );

    let mut duplicate = spawn(&exe, &temp.path().join("unused"), &socket);
    assert_exits(&mut duplicate);
    assert_eq!(
        client
            .daemon_status(deadline(), None)
            .unwrap()
            .daemon_generation,
        first.daemon_generation
    );
    let other_socket = temp.path().join("b.sock");
    let mut shared_store = spawn(&exe, &store, &other_socket);
    assert_exits(&mut shared_store);
    assert!(
        !other_socket.exists(),
        "failed store claim must remove only its own socket"
    );
    let other = spawn(&exe, &temp.path().join("store-b"), &other_socket);
    let other_client = connect(&exe, &other_socket);
    assert_ne!(
        other_client
            .daemon_status(deadline(), None)
            .unwrap()
            .store_uuid,
        first.store_uuid
    );
    drop(other);

    drop(daemon); // SIGKILL leaves the socket; next owner must inspect and reclaim it.
    assert!(socket.exists());
    let restarted = spawn(&exe, &store, &socket);
    let recovered = connect(&exe, &socket);
    let second = recovered.daemon_status(deadline(), None).unwrap();
    assert_eq!(second.store_uuid, first.store_uuid);
    assert_ne!(second.daemon_generation, first.daemon_generation);
    assert_eq!(second.pid, restarted.0.id());
    let queued = recovered.status(receipt.job_id, deadline(), None).unwrap();
    assert!(queued.started_unix_millis.is_none());
    assert!(
        recovered
            .cancel(&[receipt.job_id], deadline(), None)
            .unwrap()[0]
            .is_final()
    );
}

#[test]
fn linux_daemon_rejects_unsafe_paths_and_never_autostarts_explicit_endpoint() {
    let _fixture = FIXTURE_LIFETIME
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let temp = tempfile::tempdir_in(env!("CARGO_MANIFEST_DIR")).unwrap();
    std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let exe = pinned(temp.path());
    let control_socket = temp.path().join("ok.sock");
    let _control = spawn(&exe, &temp.path().join("control"), &control_socket);
    let _client = connect(&exe, &control_socket);
    let socket = temp.path().join("ipc.sock");
    // Even auto_start(true) cannot turn an explicit coordinate into a spawn request.
    assert!(
        Client::builder()
            .endpoint(socket.to_str().unwrap())
            .daemon_executable(&exe)
            .auto_start(true)
            .connect(Instant::now() + Duration::from_millis(100), None)
            .is_err()
    );
    assert!(!socket.exists());
    std::fs::write(&socket, b"foreign file").unwrap();
    let mut collision = spawn(&exe, &temp.path().join("store"), &socket);
    assert_exits(&mut collision);
    assert_eq!(std::fs::read(&socket).unwrap(), b"foreign file");
    let unsafe_dir = temp.path().join("public");
    std::fs::create_dir(&unsafe_dir).unwrap();
    std::fs::set_permissions(&unsafe_dir, std::fs::Permissions::from_mode(0o777)).unwrap();
    let mut unsafe_owner = spawn(
        &exe,
        &temp.path().join("other"),
        &unsafe_dir.join("ipc.sock"),
    );
    assert_exits(&mut unsafe_owner);
    assert!(!unsafe_dir.join("ipc.sock").exists());

    let active_socket = temp.path().join("foreign.sock");
    let listener = std::os::unix::net::UnixListener::bind(&active_socket).unwrap();
    std::fs::set_permissions(&active_socket, std::fs::Permissions::from_mode(0o600)).unwrap();
    let inode = std::fs::metadata(&active_socket).unwrap().ino();
    let mut active_collision = spawn(&exe, &temp.path().join("third"), &active_socket);
    assert_exits(&mut active_collision);
    assert_eq!(std::fs::metadata(&active_socket).unwrap().ino(), inode);
    drop(listener);
}
