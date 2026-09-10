use super::*;
use crate::{
    BatchMember, DependencyKind, DependencySpec, EnvironmentSpec, EstimateConfidence,
    PostconditionSpec, ResourceClaims, RetryPolicy, SPEC_VERSION, StdinSpec,
};

// These controls exercise the durable Store model without launching OS code.
// Platform/runtime controls supply their own real identities and executor proof.
fn model_tempdir() -> std::io::Result<tempfile::TempDir> {
    crate::test_support::durable_tempdir()
}

#[cfg(target_os = "linux")]
fn model_identity() -> StartupIdentity {
    let host = HostId("store-model-fixture".into());
    let boot = BootId("store-model-boot".into());
    StartupIdentity {
        host_id: Some(host.clone()),
        boot_id: Some(boot.clone()),
        daemon_process: Some(ProcessIdentity::Windows {
            host_id: host,
            boot_id: boot,
            pid: 123,
            creation_filetime_100ns: 456,
        }),
        failures: vec![],
    }
}
#[cfg(not(target_os = "linux"))]
fn model_identity() -> StartupIdentity {
    probe_startup_identity()
}

fn open_model_store(paths: StorePaths) -> StoreResult<Store> {
    #[cfg(target_os = "linux")]
    {
        let config = load_host_config(&paths.config)?;
        Store::open_with_config(paths, config, model_identity())
    }
    #[cfg(not(target_os = "linux"))]
    Store::open(paths)
}

fn open_model_store_with_capacities(
    paths: StorePaths,
    capacities: ResourceCapacities,
) -> StoreResult<Store> {
    #[cfg(target_os = "linux")]
    {
        Store::open_with_config(
            paths,
            HostConfig {
                resources: capacities,
                impact_incompatibilities: Default::default(),
                observation: Default::default(),
            },
            model_identity(),
        )
    }
    #[cfg(not(target_os = "linux"))]
    Store::open_with_capacities(paths, capacities)
}

fn model_probe_executable() -> PathBuf {
    #[cfg(windows)]
    {
        PathBuf::from(r"C:\Windows\System32\cmd.exe")
    }
    #[cfg(not(windows))]
    {
        PathBuf::from("/usr/bin/true")
    }
}

fn spec(root: &Path) -> JobSpec {
    JobSpec {
        spec_version: SPEC_VERSION,
        priority: crate::NEUTRAL_JOB_PRIORITY,
        executable: root.join("tool.exe"),
        args: Vec::new(),
        working_directory: root.to_path_buf(),
        stdin: StdinSpec::Eof,
        environment: EnvironmentSpec::default(),
        resources: ResourceClaims::default(),
        observed: None,
        conditions: Vec::new(),
        retry: RetryPolicy::default(),
        postconditions: Vec::new(),
        labels: Vec::new(),
        expected_duration_seconds: None,
        timeout_seconds: None,
        quiet: None,
        artifacts: Vec::new(),
        child_submission_policy: None,
    }
}

fn capacities() -> ResourceCapacities {
    ResourceCapacities {
        cpu_units: 4,
        ram_mb: 16_384,
        cargo_slots: 1,
        gpu_slots: 1,
        custom: [("review_slots".into(), 2)].into(),
    }
}

fn member(name: &str, spec: JobSpec, dependencies: Vec<DependencySpec>) -> BatchMember {
    BatchMember {
        name: name.into(),
        spec,
        dependencies,
    }
}

fn stage_bytes(store: &Store, bytes: &[u8]) -> StagedInputRef {
    let input = StagedInputRef {
        sha256: format!("{:x}", Sha256::digest(bytes)),
        length: bytes.len() as u64,
    };
    let upload_id = Uuid::now_v7();
    assert_eq!(
        store
            .stage_begin(upload_id, &input.sha256, input.length)
            .unwrap(),
        0
    );
    let mut offset = 0_u64;
    for chunk in bytes.chunks(17_003) {
        offset = store.stage_chunk(upload_id, offset, chunk).unwrap();
    }
    assert_eq!(store.stage_commit(upload_id).unwrap(), input);
    input
}

mod admission_safety;
mod attempt_lifecycle;
mod conditions;
mod containment_safety;
mod input_submission;
mod managed_submission;
mod observation;
mod priority_reservations;
mod store_recovery;
mod tree;
