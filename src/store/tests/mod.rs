use super::*;
use crate::{
    BatchMember, DependencyKind, DependencySpec, EnvironmentSpec, EstimateConfidence,
    PostconditionSpec, ResourceClaims, RetryPolicy, SPEC_VERSION, StdinSpec,
};

fn spec(root: &Path) -> JobSpec {
    JobSpec {
        spec_version: SPEC_VERSION,
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
mod containment_safety;
mod input_submission;
mod managed_submission;
mod observation;
mod store_recovery;
mod tree;
