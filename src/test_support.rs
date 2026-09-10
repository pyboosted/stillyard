//! Durable model fixtures use the checkout filesystem, not Linux's tmpfs /tmp.
//! IPC-only fixtures retain short temporary socket paths independently.

pub(crate) fn durable_tempdir() -> std::io::Result<tempfile::TempDir> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::PermissionsExt;
        let temp = tempfile::tempdir_in(env!("CARGO_MANIFEST_DIR"))?;
        std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700))?;
        Ok(temp)
    }
    #[cfg(not(target_os = "linux"))]
    tempfile::tempdir()
}

/// Test-binary-only rendezvous. The owning test controller kills this exact
/// child process; installed release binaries contain neither hook nor switch.
#[cfg(target_os = "linux")]
pub(crate) fn linux_runtime_checkpoint(stage: &str) {
    use std::os::unix::fs::MetadataExt;
    if std::env::var("STILLYARD_TEST_RUNTIME_CRASH").as_deref() != Ok(stage) {
        return;
    }
    let root =
        std::path::PathBuf::from(std::env::var_os("STILLYARD_TEST_RUNTIME_CRASH_ROOT").unwrap());
    let metadata = std::fs::symlink_metadata(&root).unwrap();
    assert!(metadata.is_dir() && metadata.mode() & 0o077 == 0);
    // SAFETY: geteuid has no preconditions.
    assert_eq!(metadata.uid(), unsafe { libc::geteuid() });
    assert!(
        root.canonicalize()
            .unwrap()
            .starts_with(env!("CARGO_MANIFEST_DIR"))
    );
    let file = root.join("checkpoint.json");
    std::fs::write(
        root.join("checkpoint.next"),
        serde_json::to_vec(&serde_json::json!({
            "stage": stage, "pid": std::process::id()
        }))
        .unwrap(),
    )
    .unwrap();
    std::fs::rename(root.join("checkpoint.next"), file).unwrap();
    loop {
        std::thread::park();
    }
}
