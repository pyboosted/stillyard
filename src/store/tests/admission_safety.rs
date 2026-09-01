use super::*;
use crate::host_observation::{
    ComponentEvidence, ComponentValue, GpuEvidence, HostSample, MemoryEvidence, ObservationMoment,
    ProcessEvidence,
};
use crate::{
    AdmissionDecisionState, AttemptVerdict, HostObservationConfig, JobOutcome, PostconditionSpec,
    QuietDetector, QuietPolicy,
};

const GPU_UUID: &str = "GPU-a1144c26-a15c-cba1-3b7a-870c755ef08a";

fn observation_config() -> HostConfig {
    HostConfig {
        resources: ResourceCapacities {
            ram_mb: 32_768,
            ..Default::default()
        },
        impact_incompatibilities: Default::default(),
        observation: HostObservationConfig {
            ram_safety_margin_mb: 1_024,
            pre_release_backoff_millis: 100,
            ..Default::default()
        },
    }
}

fn sample(generation: Uuid, captured: u64, cpu: u8) -> HostSample {
    HostSample {
        observation_generation: generation,
        captured_unix_millis: i64::try_from(captured).unwrap(),
        captured_monotonic_millis: captured,
        memory: ComponentEvidence::available(
            i64::try_from(captured).unwrap(),
            captured,
            MemoryEvidence {
                available_physical_mb: 64_000,
                commit_headroom_mb: 64_000,
            },
        ),
        cpu_utilization: ComponentEvidence::available(
            i64::try_from(captured).unwrap(),
            captured,
            cpu,
        ),
        disk_utilization: ComponentEvidence::available(
            i64::try_from(captured).unwrap(),
            captured,
            0,
        ),
        processes: ComponentEvidence::available(
            i64::try_from(captured).unwrap(),
            captured,
            Vec::<ProcessEvidence>::new(),
        ),
        gpus: ComponentEvidence::available(
            i64::try_from(captured).unwrap(),
            captured,
            std::collections::BTreeMap::<String, GpuEvidence>::new(),
        ),
    }
}

fn gpu_sample(generation: Uuid, captured: u64) -> HostSample {
    let mut result = sample(generation, captured, 0);
    let uuid = GPU_UUID.to_ascii_lowercase();
    result.gpus = ComponentEvidence::available(
        i64::try_from(captured).unwrap(),
        captured,
        [(
            uuid.clone(),
            GpuEvidence {
                uuid,
                driver_version: "999.42".into(),
                free_memory_mb: 16_384,
                utilization_percent: 0,
                compute_processes: Vec::new(),
            },
        )]
        .into(),
    );
    result
}

fn at(sample: &HostSample, now: u64) -> ObservationMoment<'_> {
    ObservationMoment {
        sample,
        now_unix_millis: i64::try_from(now).unwrap(),
        now_monotonic_millis: now,
    }
}

fn quiet_job(root: &Path) -> JobSpec {
    let mut job = spec(root);
    job.quiet = Some(QuietPolicy {
        stable_seconds: 1,
        max_sample_age_seconds: 2,
        wait_budget_seconds: 10,
        detectors: vec![QuietDetector::CpuUtilization { max_percent: 0 }],
    });
    job.postconditions.push(PostconditionSpec {
        executable: root.join("validator.exe"),
        args: Vec::new(),
        working_directory: None,
        accepted_exit_codes: vec![0],
        retryable_exit_codes: Vec::new(),
    });
    job
}

#[test]
fn stale_memory_and_low_commit_headroom_never_create_a_lease_or_invocation() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open_with_config(
        StorePaths::new(temp.path().to_path_buf()),
        observation_config(),
        probe_startup_identity(),
    )
    .unwrap();
    let mut job = spec(temp.path());
    job.resources.ram_mb = Some(24_000);
    let hash = normalized_payload_hash(&job).unwrap();
    let receipt = store.submit(Uuid::now_v7(), &hash, &job).unwrap().receipt;

    let generation = Uuid::now_v7();
    let mut stale = sample(generation, 1_000, 0);
    stale.memory = ComponentEvidence::available(
        1_000,
        1_000,
        MemoryEvidence {
            available_physical_mb: 64_000,
            commit_headroom_mb: 64_000,
        },
    );
    assert!(
        store
            .prepare_next_job_with_observation(Some(at(&stale, 4_000)))
            .unwrap()
            .job
            .is_none()
    );
    let mut low_commit = sample(generation, 5_000, 0);
    low_commit.memory = ComponentEvidence::available(
        5_000,
        5_000,
        MemoryEvidence {
            available_physical_mb: 64_000,
            commit_headroom_mb: 20_000,
        },
    );
    assert!(
        store
            .prepare_next_job_with_observation(Some(at(&low_commit, 5_000)))
            .unwrap()
            .job
            .is_none()
    );
    let leases: u64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM leases", [], |row| row.get(0))
        .unwrap();
    let invocations: u64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM invocations", [], |row| row.get(0))
        .unwrap();
    assert_eq!((leases, invocations), (0, 0));
    let admission = store
        .status(receipt.job_id)
        .unwrap()
        .admission
        .expect("RAM admission evidence");
    assert_eq!(admission.state, AdmissionDecisionState::Waiting);
    assert_eq!(admission.evaluated_unix_millis, Some(5_000));
    assert!(
        admission
            .blockers
            .iter()
            .any(|blocker| blocker.code == "observed_resource_busy")
    );
    assert_eq!(admission.operands.len(), 1);
    assert_eq!(admission.operands[0].name, "ram_mb");
    assert_eq!(admission.operands[0].observed, Some(20_000));
    assert!(!admission.operands[0].satisfied);
}

#[test]
fn sidecar_gpu_and_vram_claims_fail_closed_then_preserve_grant_provenance() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = observation_config();
    config.resources.gpu_slots = 1;
    config
        .resources
        .custom
        .insert(format!("vram_mb:{GPU_UUID}"), 16_384);
    config.observation.gpu_slot_uuid = Some(GPU_UUID.into());
    config.observation.vram_safety_margin_mb = 1_024;
    let mut store = Store::open_with_config(
        StorePaths::new(temp.path().to_path_buf()),
        config,
        probe_startup_identity(),
    )
    .unwrap();
    let mut job = spec(temp.path());
    job.resources.gpu_slots = Some(1);
    job.resources
        .custom
        .insert(format!("vram_mb:{GPU_UUID}"), 8_192);
    let hash = normalized_payload_hash(&job).unwrap();
    let receipt = store.submit(Uuid::now_v7(), &hash, &job).unwrap().receipt;
    let generation = Uuid::now_v7();
    let stale = gpu_sample(generation, 1_000);
    assert!(
        store
            .prepare_next_job_with_observation(Some(at(&stale, 4_000)))
            .unwrap()
            .job
            .is_none()
    );
    let blocked = store.status(receipt.job_id).unwrap();
    assert!(
        blocked
            .blockers
            .iter()
            .any(|blocker| blocker.code == "observation_stale")
    );
    let counts: (u64, u64) = store
        .connection
        .query_row(
            "SELECT (SELECT COUNT(*) FROM leases),
                    (SELECT COUNT(*) FROM invocations)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(counts, (0, 0));
    let blocked_admission = blocked.admission.expect("blocked GPU admission evidence");
    assert_eq!(blocked_admission.state, AdmissionDecisionState::Waiting);
    assert!(
        blocked_admission
            .blockers
            .iter()
            .any(|blocker| blocker.code == "observation_stale")
    );

    let mut provider_down = gpu_sample(generation, 4_500);
    provider_down.gpus.value = ComponentValue::Error("NVML provider is down".into());
    assert!(
        store
            .prepare_next_job_with_observation(Some(at(&provider_down, 4_500)))
            .unwrap()
            .job
            .is_none()
    );
    let unavailable = store
        .status(receipt.job_id)
        .unwrap()
        .admission
        .expect("unavailable GPU admission evidence");
    assert!(
        unavailable
            .blockers
            .iter()
            .any(|blocker| blocker.code == "observation_missing")
    );

    let other_uuid = "gpu-b2255d37-b26d-dcb2-4c8b-981d866ff19b";
    let mut wrong_device = gpu_sample(generation, 4_750);
    wrong_device.gpus = ComponentEvidence::available(
        4_750,
        4_750,
        [(
            other_uuid.into(),
            GpuEvidence {
                uuid: other_uuid.into(),
                driver_version: "999.42".into(),
                free_memory_mb: 16_384,
                utilization_percent: 0,
                compute_processes: Vec::new(),
            },
        )]
        .into(),
    );
    assert!(
        store
            .prepare_next_job_with_observation(Some(at(&wrong_device, 4_750)))
            .unwrap()
            .job
            .is_none(),
        "a different enumerated GPU supplied placement provenance"
    );
    assert!(
        store
            .status(receipt.job_id)
            .unwrap()
            .blockers
            .iter()
            .any(|blocker| blocker.detail.contains("absent from current NVML topology"))
    );

    let fresh = gpu_sample(generation, 5_000);
    let prepared = store
        .prepare_next_job_with_observation(Some(at(&fresh, 5_000)))
        .unwrap()
        .job
        .unwrap();
    assert_eq!(
        prepared.spec.quiet, None,
        "sidecar must not gain quiet wait"
    );
    let expected = crate::GpuProvenance {
        uuid: GPU_UUID.to_ascii_lowercase(),
        driver_version: "999.42".into(),
    };
    let reserved = store
        .status(receipt.job_id)
        .unwrap()
        .admission
        .expect("reserved GPU admission evidence");
    assert_eq!(reserved.state, AdmissionDecisionState::Reserved);
    assert_eq!(reserved.gpu_provenance, Some(expected.clone()));
    assert!(!reserved.final_sample);
    assert_eq!(reserved.operands.len(), 1);
    assert!(reserved.operands[0].satisfied);
    assert_eq!(
        store.status(receipt.job_id).unwrap().gpu_provenance,
        Some(expected.clone())
    );
    let refreshed_receipt = store
        .receipt(receipt.submission_id, receipt.job_id)
        .unwrap();
    assert_eq!(refreshed_receipt.gpu_provenance, Some(expected));
    assert_eq!(refreshed_receipt.admission, Some(reserved));
}

#[test]
fn quiet_wait_holds_no_lease_and_final_recheck_reuses_the_attempt_without_running_code() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open_with_config(
        StorePaths::new(temp.path().to_path_buf()),
        observation_config(),
        probe_startup_identity(),
    )
    .unwrap();
    let mut job = quiet_job(temp.path());
    job.timeout_seconds = Some(10);
    let hash = normalized_payload_hash(&job).unwrap();
    let receipt = store.submit(Uuid::now_v7(), &hash, &job).unwrap().receipt;
    let generation = Uuid::now_v7();
    let first = sample(generation, 1_000, 0);
    assert!(
        store
            .prepare_next_job_with_observation(Some(at(&first, 1_000)))
            .unwrap()
            .job
            .is_none()
    );
    let attempt_key: String = store
        .connection
        .query_row(
            "SELECT attempt_id FROM jobs WHERE id = ?1",
            [receipt.job_id.entity_uuid().to_string()],
            |row| row.get(0),
        )
        .unwrap();
    let leases: u64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM leases", [], |row| row.get(0))
        .unwrap();
    assert_eq!(leases, 0, "quiet stability must not hold the work Lease");

    let second = sample(generation, 2_000, 0);
    let prepared = store
        .prepare_next_job_with_observation(Some(at(&second, 2_000)))
        .unwrap()
        .job
        .unwrap();
    let before_release = store.status(receipt.job_id).unwrap();
    assert_eq!(before_release.attempts[0].started_unix_millis, None);
    assert_eq!(before_release.attempts[0].deadline_unix_millis, None);
    assert_eq!(prepared.attempt_id.entity_uuid().to_string(), attempt_key);
    let identity = ProcessIdentity::Windows {
        host_id: prepared.host_id.clone().unwrap(),
        boot_id: prepared.boot_id.clone().unwrap(),
        pid: 42,
        creation_filetime_100ns: 1,
    };
    store
        .record_suspended_root(&prepared, 42, "test-image", &identity)
        .unwrap();
    let contaminated = sample(generation, 2_100, 100);
    let authorization = store
        .authorize_release(&prepared, at(&contaminated, 2_100))
        .unwrap();
    let crate::store::ReleaseAuthorization::Deferred { reason } = authorization else {
        panic!("contaminated final sample authorized release")
    };
    store.replan_never_run(&prepared, &reason).unwrap();
    let state: (String, String, String, u64) = store
        .connection
        .query_row(
            "SELECT jobs.state, attempts.state, invocations.state,
                    (SELECT COUNT(*) FROM leases WHERE state = 'granted')
             FROM jobs JOIN attempts ON attempts.id = jobs.attempt_id
             JOIN invocations ON invocations.attempt_id = attempts.id
             WHERE jobs.id = ?1",
            [receipt.job_id.entity_uuid().to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(
        state,
        ("pending".into(), "planned".into(), "resolved".into(), 0)
    );
    let replanned = store
        .status(receipt.job_id)
        .unwrap()
        .admission
        .expect("replanned admission evidence");
    assert_eq!(replanned.state, AdmissionDecisionState::Replanned);
    assert_eq!(replanned.deferral_count, 1);

    std::thread::sleep(std::time::Duration::from_millis(110));
    let third = sample(generation, 3_000, 0);
    assert!(
        store
            .prepare_next_job_with_observation(Some(at(&third, 3_000)))
            .unwrap()
            .job
            .is_none()
    );
    let fourth = sample(generation, 4_000, 0);
    let replacement = store
        .prepare_next_job_with_observation(Some(at(&fourth, 4_000)))
        .unwrap()
        .job
        .unwrap();
    assert_eq!(replacement.attempt_id, prepared.attempt_id);
    assert_ne!(replacement.invocation_id, prepared.invocation_id);
    let role_index: u32 = store
        .connection
        .query_row(
            "SELECT role_index FROM invocations WHERE id = ?1",
            [replacement.invocation_id.entity_uuid().to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(role_index, 1);

    let replacement_identity = ProcessIdentity::Windows {
        host_id: replacement.host_id.clone().unwrap(),
        boot_id: replacement.boot_id.clone().unwrap(),
        pid: 43,
        creation_filetime_100ns: 2,
    };
    store
        .record_suspended_root(&replacement, 43, "test-image", &replacement_identity)
        .unwrap();
    let cached = sample(generation, 4_100, 0);
    let stale = store
        .authorize_release(&replacement, at(&cached, 6_101))
        .unwrap();
    assert!(matches!(
        stale,
        crate::store::ReleaseAuthorization::Deferred { .. }
    ));
    let invocation_started: Option<i64> = store
        .connection
        .query_row(
            "SELECT started_ms FROM invocations WHERE id = ?1",
            [replacement.invocation_id.entity_uuid().to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        invocation_started, None,
        "stale final evidence marked code as started"
    );

    store
        .replan_never_run(&replacement, "stale final evidence")
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(110));
    let fifth = sample(generation, 7_000, 0);
    assert!(
        store
            .prepare_next_job_with_observation(Some(at(&fifth, 7_000)))
            .unwrap()
            .job
            .is_none()
    );
    let sixth = sample(generation, 8_000, 0);
    let final_primary = store
        .prepare_next_job_with_observation(Some(at(&sixth, 8_000)))
        .unwrap()
        .job
        .unwrap();
    let final_identity = ProcessIdentity::Windows {
        host_id: final_primary.host_id.clone().unwrap(),
        boot_id: final_primary.boot_id.clone().unwrap(),
        pid: 44,
        creation_filetime_100ns: 3,
    };
    store
        .record_suspended_root(&final_primary, 44, "test-image", &final_identity)
        .unwrap();
    let release = sample(generation, 8_100, 0);
    assert!(matches!(
        store
            .authorize_release(&final_primary, at(&release, 8_100))
            .unwrap(),
        crate::store::ReleaseAuthorization::Authorized { .. }
    ));
    let released = store
        .status(receipt.job_id)
        .unwrap()
        .admission
        .expect("release admission evidence");
    assert_eq!(released.state, AdmissionDecisionState::Released);
    assert!(released.final_sample);
    assert!(released.detectors.iter().all(|detector| detector.satisfied));
    let released_attempt = &store.status(receipt.job_id).unwrap().attempts[0];
    assert!(released_attempt.started_unix_millis.is_some());
    assert!(released_attempt.deadline_unix_millis.is_some());
    store
        .mark_invocation_resolved(
            &final_primary,
            Some(0),
            Some(crate::ExitClassification::Accepted),
        )
        .unwrap();
    store
        .record_primary_result(
            &final_primary,
            InvocationVerdict::Succeeded,
            TerminationReason::Exited,
        )
        .unwrap();
    let postcondition = store.prepare_postcondition(&final_primary, 0).unwrap();
    let indices: (u32, u32) = store
        .connection
        .query_row(
            "SELECT role_index, postcondition_index FROM invocations WHERE id = ?1",
            [postcondition.invocation_id.entity_uuid().to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(indices, (3, 0));
}

#[test]
fn cancel_that_wins_during_contaminated_cleanup_cannot_replan_or_release_code() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open_with_config(
        StorePaths::new(temp.path().to_path_buf()),
        observation_config(),
        probe_startup_identity(),
    )
    .unwrap();
    let job = quiet_job(temp.path());
    let hash = normalized_payload_hash(&job).unwrap();
    let receipt = store.submit(Uuid::now_v7(), &hash, &job).unwrap().receipt;
    let generation = Uuid::now_v7();
    let first = sample(generation, 1_000, 0);
    assert!(
        store
            .prepare_next_job_with_observation(Some(at(&first, 1_000)))
            .unwrap()
            .job
            .is_none()
    );
    let second = sample(generation, 2_000, 0);
    let prepared = store
        .prepare_next_job_with_observation(Some(at(&second, 2_000)))
        .unwrap()
        .job
        .unwrap();
    let identity = ProcessIdentity::Windows {
        host_id: prepared.host_id.clone().unwrap(),
        boot_id: prepared.boot_id.clone().unwrap(),
        pid: 46,
        creation_filetime_100ns: 5,
    };
    store
        .record_suspended_root(&prepared, 46, "test-image", &identity)
        .unwrap();
    let contaminated = sample(generation, 2_100, 100);
    let crate::store::ReleaseAuthorization::Deferred { reason } = store
        .authorize_release(&prepared, at(&contaminated, 2_100))
        .unwrap()
    else {
        panic!("contaminated final sample authorized release")
    };

    let requested = store.cancel_jobs(&[receipt.job_id]).unwrap();
    assert!(requested[0].cancel_requested);
    store.replan_never_run(&prepared, &reason).unwrap();
    let final_snapshot = store.status(receipt.job_id).unwrap();
    assert_eq!(final_snapshot.outcome, Some(JobOutcome::Canceled));
    assert_eq!(
        final_snapshot.attempts[0].verdict,
        Some(AttemptVerdict::Canceled)
    );
    assert_eq!(final_snapshot.attempts[0].invocations.len(), 1);
    let granted: u64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM leases WHERE state = 'granted'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(granted, 0);
}

#[test]
fn quiet_stability_resets_on_sample_gap_and_observation_generation_change() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = observation_config();
    config.observation.quiet_max_sample_gap_millis = 1_000;
    let mut store = Store::open_with_config(
        StorePaths::new(temp.path().to_path_buf()),
        config,
        probe_startup_identity(),
    )
    .unwrap();
    let mut job = quiet_job(temp.path());
    job.quiet.as_mut().unwrap().stable_seconds = 2;
    let hash = normalized_payload_hash(&job).unwrap();
    store.submit(Uuid::now_v7(), &hash, &job).unwrap();
    let first_generation = Uuid::now_v7();

    for captured in [1_000, 2_001, 3_001] {
        let observation = sample(first_generation, captured, 0);
        assert!(
            store
                .prepare_next_job_with_observation(Some(at(&observation, captured)))
                .unwrap()
                .job
                .is_none(),
            "sample gap did not reset the stable interval at {captured}ms"
        );
    }

    let second_generation = Uuid::now_v7();
    for captured in [4_001, 5_001] {
        let observation = sample(second_generation, captured, 0);
        assert!(
            store
                .prepare_next_job_with_observation(Some(at(&observation, captured)))
                .unwrap()
                .job
                .is_none(),
            "generation change did not reset the stable interval at {captured}ms"
        );
    }
    let final_sample = sample(second_generation, 6_001, 0);
    assert!(
        store
            .prepare_next_job_with_observation(Some(at(&final_sample, 6_001)))
            .unwrap()
            .job
            .is_some(),
        "two seconds in one generation did not complete quiet stability"
    );
}

#[test]
fn final_observed_ram_check_excludes_only_its_own_granted_lease() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open_with_config(
        StorePaths::new(temp.path().to_path_buf()),
        observation_config(),
        probe_startup_identity(),
    )
    .unwrap();
    let mut job = quiet_job(temp.path());
    job.resources.ram_mb = Some(24_000);
    let hash = normalized_payload_hash(&job).unwrap();
    store.submit(Uuid::now_v7(), &hash, &job).unwrap();
    let generation = Uuid::now_v7();
    let first = sample(generation, 1_000, 0);
    assert!(
        store
            .prepare_next_job_with_observation(Some(at(&first, 1_000)))
            .unwrap()
            .job
            .is_none()
    );
    let second = sample(generation, 2_000, 0);
    let prepared = store
        .prepare_next_job_with_observation(Some(at(&second, 2_000)))
        .unwrap()
        .job
        .unwrap();
    let identity = ProcessIdentity::Windows {
        host_id: prepared.host_id.clone().unwrap(),
        boot_id: prepared.boot_id.clone().unwrap(),
        pid: 45,
        creation_filetime_100ns: 4,
    };
    store
        .record_suspended_root(&prepared, 45, "test-image", &identity)
        .unwrap();

    let mut final_sample = sample(generation, 2_100, 0);
    final_sample.memory = ComponentEvidence::available(
        2_100,
        2_100,
        MemoryEvidence {
            available_physical_mb: 26_000,
            commit_headroom_mb: 26_000,
        },
    );
    assert!(matches!(
        store
            .authorize_release(&prepared, at(&final_sample, 2_100))
            .unwrap(),
        crate::store::ReleaseAuthorization::Authorized { .. }
    ));
}

#[test]
fn restart_preserves_admitting_attempt_budget_resets_stability_and_allows_cancel() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StorePaths::new(temp.path().to_path_buf());
    let config = observation_config();
    let (job_id, attempt_id, wall_deadline) = {
        let mut store =
            Store::open_with_config(paths.clone(), config.clone(), probe_startup_identity())
                .unwrap();
        let job = quiet_job(temp.path());
        let hash = normalized_payload_hash(&job).unwrap();
        let receipt = store.submit(Uuid::now_v7(), &hash, &job).unwrap().receipt;
        let generation = Uuid::now_v7();
        let first = sample(generation, 1_000, 0);
        assert!(
            store
                .prepare_next_job_with_observation(Some(at(&first, 1_000)))
                .unwrap()
                .job
                .is_none()
        );
        let second = sample(generation, 1_500, 0);
        assert!(
            store
                .prepare_next_job_with_observation(Some(at(&second, 1_500)))
                .unwrap()
                .job
                .is_none()
        );
        let durable: (String, String, u64, i64) = store
            .connection
            .query_row(
                "SELECT attempts.id, attempts.state, admissions.quiet_consumed_ms,
                        admissions.wall_deadline_ms
                 FROM attempts JOIN admissions ON admissions.attempt_id = attempts.id
                 WHERE attempts.job_id = ?1",
                [receipt.job_id.entity_uuid().to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(durable.1, "admitting");
        assert_eq!(durable.2, 500);
        (
            receipt.job_id,
            AttemptId::from_parts(store.store_uuid, Uuid::parse_str(&durable.0).unwrap()),
            durable.3,
        )
    };

    let mut reopened = Store::open_with_config(paths, config, probe_startup_identity()).unwrap();
    let snapshot = reopened.status(job_id).unwrap();
    assert_eq!(snapshot.state, JobState::Pending);
    assert_eq!(snapshot.attempt_id, Some(attempt_id));
    assert_eq!(snapshot.attempts[0].verdict, None);
    let new_generation = Uuid::now_v7();
    let after_restart = sample(new_generation, 2_000, 0);
    assert!(
        reopened
            .prepare_next_job_with_observation(Some(at(&after_restart, 2_000)))
            .unwrap()
            .job
            .is_none()
    );
    let durable: (String, u64, Option<u64>, i64) = reopened
        .connection
        .query_row(
            "SELECT attempts.state, admissions.quiet_consumed_ms,
                    admissions.quiet_first_monotonic_ms, admissions.wall_deadline_ms
             FROM attempts JOIN admissions ON admissions.attempt_id = attempts.id
             WHERE attempts.id = ?1",
            [attempt_id.entity_uuid().to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(
        durable,
        ("admitting".into(), 500, Some(2_000), wall_deadline)
    );

    let canceled = reopened.cancel_jobs(&[job_id]).unwrap();
    assert_eq!(canceled[0].outcome, Some(JobOutcome::Canceled));
    assert_eq!(
        canceled[0].attempts[0].verdict,
        Some(AttemptVerdict::Canceled)
    );
}

#[test]
fn incompatible_impact_consumes_the_admission_wall_clock_and_fails_finitely() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = observation_config();
    config.observation.admission_wall_clock_limit_seconds = 1;
    config
        .impact_incompatibilities
        .insert("measurement".into(), vec!["cpu_heavy".into()]);
    let mut store = Store::open_with_config(
        StorePaths::new(temp.path().to_path_buf()),
        config,
        probe_startup_identity(),
    )
    .unwrap();

    let mut cargo = spec(temp.path());
    cargo.resources.impacts.push("cpu_heavy".into());
    let hash = normalized_payload_hash(&cargo).unwrap();
    store.submit(Uuid::now_v7(), &hash, &cargo).unwrap();
    let cargo = store
        .prepare_next_job()
        .unwrap()
        .expect("cargo reservation");

    let mut strict = quiet_job(temp.path());
    strict.resources.impacts.push("measurement".into());
    let hash = normalized_payload_hash(&strict).unwrap();
    let receipt = store
        .submit(Uuid::now_v7(), &hash, &strict)
        .unwrap()
        .receipt;
    let observation = sample(Uuid::now_v7(), 1_000, 0);
    assert!(
        store
            .prepare_next_job_with_observation(Some(at(&observation, 1_000)))
            .unwrap()
            .job
            .is_none()
    );
    let admission_rows: u64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM admissions JOIN attempts
             ON attempts.id = admissions.attempt_id WHERE attempts.job_id = ?1",
            [receipt.job_id.entity_uuid().to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        admission_rows, 1,
        "impact wait did not start its finite deadline"
    );
    store
        .connection
        .execute(
            "UPDATE admissions SET wall_deadline_ms = 0 WHERE attempt_id =
             (SELECT attempt_id FROM jobs WHERE id = ?1)",
            [receipt.job_id.entity_uuid().to_string()],
        )
        .unwrap();
    assert!(
        store
            .prepare_next_job_with_observation(Some(at(&observation, 1_000)))
            .unwrap()
            .job
            .is_none()
    );
    let snapshot = store.status(receipt.job_id).unwrap();
    assert_eq!(snapshot.outcome, Some(JobOutcome::Failed));
    assert_eq!(
        snapshot.attempts[0].verdict,
        Some(AttemptVerdict::SafetyFailed)
    );
    assert_eq!(
        snapshot.attempts[0].reason_code.as_deref(),
        Some("admission_starved")
    );
    assert!(snapshot.attempts[0].invocations.is_empty());

    let _cargo = cargo;
}

#[test]
fn missing_observation_pauses_quiet_budget_and_rebuilds_stability() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open_with_config(
        StorePaths::new(temp.path().to_path_buf()),
        observation_config(),
        probe_startup_identity(),
    )
    .unwrap();
    let job = quiet_job(temp.path());
    let hash = normalized_payload_hash(&job).unwrap();
    store.submit(Uuid::now_v7(), &hash, &job).unwrap();
    let generation = Uuid::now_v7();

    let first = sample(generation, 1_000, 0);
    assert!(
        store
            .prepare_next_job_with_observation(Some(at(&first, 1_000)))
            .unwrap()
            .job
            .is_none()
    );
    assert!(
        store
            .prepare_next_job_with_observation(None)
            .unwrap()
            .job
            .is_none()
    );

    let after_gap = sample(generation, 9_000, 0);
    assert!(
        store
            .prepare_next_job_with_observation(Some(at(&after_gap, 9_000)))
            .unwrap()
            .job
            .is_none(),
        "missing evidence must not count toward stability"
    );
    let progress: (u64, Option<u64>) = store
        .connection
        .query_row(
            "SELECT quiet_consumed_ms, quiet_first_monotonic_ms FROM admissions",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(progress, (0, Some(9_000)));

    let stable = sample(generation, 10_000, 0);
    assert!(
        store
            .prepare_next_job_with_observation(Some(at(&stable, 10_000)))
            .unwrap()
            .job
            .is_some()
    );
}

#[test]
fn static_contention_pauses_quiet_budget_and_rebuilds_stability() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = observation_config();
    config
        .impact_incompatibilities
        .insert("measurement".into(), vec!["cpu_heavy".into()]);
    let mut store = Store::open_with_config(
        StorePaths::new(temp.path().to_path_buf()),
        config,
        probe_startup_identity(),
    )
    .unwrap();
    let mut strict = quiet_job(temp.path());
    strict.resources.impacts.push("measurement".into());
    let hash = normalized_payload_hash(&strict).unwrap();
    store.submit(Uuid::now_v7(), &hash, &strict).unwrap();
    let generation = Uuid::now_v7();
    let first = sample(generation, 1_000, 0);
    assert!(
        store
            .prepare_next_job_with_observation(Some(at(&first, 1_000)))
            .unwrap()
            .job
            .is_none()
    );

    let mut cargo = spec(temp.path());
    cargo.resources.impacts.push("cpu_heavy".into());
    let hash = normalized_payload_hash(&cargo).unwrap();
    let cargo_receipt = store.submit(Uuid::now_v7(), &hash, &cargo).unwrap().receipt;
    let cargo = store
        .prepare_job(cargo_receipt.job_id)
        .unwrap()
        .expect("orthogonal waiter may reserve while quiet has no Lease");

    let blocked = sample(generation, 9_000, 0);
    assert!(
        store
            .prepare_next_job_with_observation(Some(at(&blocked, 9_000)))
            .unwrap()
            .job
            .is_none()
    );
    store
        .connection
        .execute(
            "UPDATE leases SET state = 'released' WHERE attempt_id = ?1",
            [cargo.attempt_id.entity_uuid().to_string()],
        )
        .unwrap();

    let eligible_again = sample(generation, 9_500, 0);
    assert!(
        store
            .prepare_next_job_with_observation(Some(at(&eligible_again, 9_500)))
            .unwrap()
            .job
            .is_none(),
        "resource contention must reset the stable interval"
    );
    let progress: (u64, Option<u64>) = store
        .connection
        .query_row(
            "SELECT quiet_consumed_ms, quiet_first_monotonic_ms FROM admissions
             WHERE attempt_id != ?1",
            [cargo.attempt_id.entity_uuid().to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(progress, (0, Some(9_500)));

    let stable = sample(generation, 10_500, 0);
    assert!(
        store
            .prepare_next_job_with_observation(Some(at(&stable, 10_500)))
            .unwrap()
            .job
            .is_some()
    );
}

#[test]
fn starting_strict_job_keeps_sampler_demand_until_release() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open_with_config(
        StorePaths::new(temp.path().to_path_buf()),
        observation_config(),
        probe_startup_identity(),
    )
    .unwrap();
    let job = quiet_job(temp.path());
    let hash = normalized_payload_hash(&job).unwrap();
    store.submit(Uuid::now_v7(), &hash, &job).unwrap();
    let generation = Uuid::now_v7();
    let first = sample(generation, 1_000, 0);
    assert!(
        store
            .prepare_next_job_with_observation(Some(at(&first, 1_000)))
            .unwrap()
            .job
            .is_none()
    );
    let second = sample(generation, 2_000, 0);
    let prepared = store
        .prepare_next_job_with_observation(Some(at(&second, 2_000)))
        .unwrap()
        .job
        .expect("stable quiet window reserves");
    assert!(store.host_observation_demand().unwrap());
    let identity = ProcessIdentity::Windows {
        host_id: prepared.host_id.clone().unwrap(),
        boot_id: prepared.boot_id.clone().unwrap(),
        pid: 62,
        creation_filetime_100ns: 6,
    };
    store
        .record_suspended_root(&prepared, 62, "test-image", &identity)
        .unwrap();
    assert!(store.host_observation_demand().unwrap());

    let final_sample = sample(generation, 2_100, 0);
    assert!(matches!(
        store
            .authorize_release(&prepared, at(&final_sample, 2_100))
            .unwrap(),
        crate::store::ReleaseAuthorization::Authorized { .. }
    ));
    assert!(!store.host_observation_demand().unwrap());
}

#[test]
fn clean_deferral_exhaustion_uses_the_job_retry_policy() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = observation_config();
    config.observation.pre_release_max_deferrals = 1;
    let mut store = Store::open_with_config(
        StorePaths::new(temp.path().to_path_buf()),
        config,
        probe_startup_identity(),
    )
    .unwrap();
    let mut job = quiet_job(temp.path());
    job.retry.max_attempts = 2;
    job.retry.retryable.push("safety_failed".into());
    let hash = normalized_payload_hash(&job).unwrap();
    let receipt = store.submit(Uuid::now_v7(), &hash, &job).unwrap().receipt;
    let generation = Uuid::now_v7();
    let first = sample(generation, 1_000, 0);
    assert!(
        store
            .prepare_next_job_with_observation(Some(at(&first, 1_000)))
            .unwrap()
            .job
            .is_none()
    );
    let second = sample(generation, 2_000, 0);
    let prepared = store
        .prepare_next_job_with_observation(Some(at(&second, 2_000)))
        .unwrap()
        .job
        .unwrap();
    let identity = ProcessIdentity::Windows {
        host_id: prepared.host_id.clone().unwrap(),
        boot_id: prepared.boot_id.clone().unwrap(),
        pid: 51,
        creation_filetime_100ns: 5,
    };
    store
        .record_suspended_root(&prepared, 51, "test-image", &identity)
        .unwrap();
    store.replan_never_run(&prepared, "contaminated").unwrap();

    let state: (String, Option<String>, u64, u64) = store
        .connection
        .query_row(
            "SELECT jobs.state, jobs.attempt_id,
                    (SELECT COUNT(*) FROM attempts WHERE job_id = jobs.id),
                    (SELECT COUNT(*) FROM leases WHERE state = 'granted')
             FROM jobs WHERE jobs.id = ?1",
            [receipt.job_id.entity_uuid().to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(state, ("pending".into(), None, 1, 0));
    let prior = store.status(receipt.job_id).unwrap();
    assert_eq!(
        prior.attempts[0].verdict,
        Some(AttemptVerdict::SafetyFailed)
    );
    assert_eq!(
        prior.attempts[0].reason_code.as_deref(),
        Some("quiet_unattainable")
    );
    assert_eq!(prior.admission.unwrap().deferral_count, 1);

    let retry_sample = sample(generation, 3_000, 0);
    assert!(
        store
            .prepare_next_job_with_observation(Some(at(&retry_sample, 3_000)))
            .unwrap()
            .job
            .is_none()
    );
    let attempts: u64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM attempts", [], |row| row.get(0))
        .unwrap();
    assert_eq!(attempts, 2, "retry must create a fresh Attempt and budget");
}

#[test]
fn uncertain_pre_release_cleanup_is_final_and_retains_the_lease() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = observation_config();
    config.resources.cargo_slots = 1;
    let mut store = Store::open_with_config(
        StorePaths::new(temp.path().to_path_buf()),
        config,
        probe_startup_identity(),
    )
    .unwrap();
    let mut job = quiet_job(temp.path());
    job.resources.cargo_slots = Some(1);
    job.retry.max_attempts = 2;
    job.retry.retryable.push("safety_failed".into());
    let hash = normalized_payload_hash(&job).unwrap();
    let receipt = store.submit(Uuid::now_v7(), &hash, &job).unwrap().receipt;
    let generation = Uuid::now_v7();
    let first = sample(generation, 1_000, 0);
    assert!(
        store
            .prepare_next_job_with_observation(Some(at(&first, 1_000)))
            .unwrap()
            .job
            .is_none()
    );
    let second = sample(generation, 2_000, 0);
    let prepared = store
        .prepare_next_job_with_observation(Some(at(&second, 2_000)))
        .unwrap()
        .job
        .unwrap();
    let identity = ProcessIdentity::Windows {
        host_id: prepared.host_id.clone().unwrap(),
        boot_id: prepared.boot_id.clone().unwrap(),
        pid: 52,
        creation_filetime_100ns: 6,
    };
    store
        .record_suspended_root(&prepared, 52, "test-image", &identity)
        .unwrap();
    store
        .mark_pre_release_cleanup_uncertain(&prepared, None)
        .unwrap();

    let snapshot = store.status(receipt.job_id).unwrap();
    assert_eq!(snapshot.state, JobState::Final);
    assert_eq!(snapshot.outcome, Some(JobOutcome::Failed));
    assert_eq!(
        snapshot.attempts[0].verdict,
        Some(AttemptVerdict::SafetyFailed)
    );
    assert_eq!(
        snapshot.attempts[0].reason_code.as_deref(),
        Some("pre_release_cleanup_uncertain")
    );
    assert_eq!(
        snapshot.attempts[0].invocations[0].containment.state,
        ContainmentState::Uncertain
    );
    let granted: u64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM leases WHERE state = 'granted'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(granted, 1, "unproven containment must retain its Lease");
    let resources = store
        .daemon_status("test")
        .unwrap()
        .resources
        .expect("current daemon resource snapshot");
    assert_eq!(resources.cargo_slots.capacity, 1);
    assert_eq!(resources.cargo_slots.granted, 1);
    assert_eq!(resources.cargo_slots.reserved, 0);
}

#[test]
fn restart_refuses_host_policy_that_invalidates_a_retained_job() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StorePaths::new(temp.path().to_path_buf());
    let mut original = observation_config();
    original.observation.pre_release_max_deferrals = 1;
    let mut job = quiet_job(temp.path());
    job.retry.max_attempts = 16;
    job.postconditions = (0..14)
        .map(|_| PostconditionSpec {
            executable: temp.path().join("validator.exe"),
            args: Vec::new(),
            working_directory: None,
            accepted_exit_codes: vec![0],
            retryable_exit_codes: Vec::new(),
        })
        .collect();
    {
        let mut store =
            Store::open_with_config(paths.clone(), original.clone(), probe_startup_identity())
                .unwrap();
        let hash = normalized_payload_hash(&job).unwrap();
        store.submit(Uuid::now_v7(), &hash, &job).unwrap();
    }

    let mut incompatible = original;
    incompatible.observation.pre_release_max_deferrals = 2;
    let error = Store::open_with_config(paths, incompatible, probe_startup_identity())
        .err()
        .expect("incompatible retained Job must reject daemon startup");
    assert!(
        error.to_string().contains("retained Job")
            && error.to_string().contains("exceed 256 Invocations"),
        "unexpected startup error: {error}"
    );
}

#[test]
fn restart_refuses_gpu_placement_change_for_a_retained_job() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StorePaths::new(temp.path().to_path_buf());
    let mut original = observation_config();
    original.resources.gpu_slots = 1;
    original
        .resources
        .custom
        .insert(format!("vram_mb:{GPU_UUID}"), 16_384);
    original.observation.gpu_slot_uuid = Some(GPU_UUID.into());
    original.observation.vram_safety_margin_mb = 1_024;
    let mut job = spec(temp.path());
    job.resources.gpu_slots = Some(1);
    job.resources
        .custom
        .insert(format!("vram_mb:{GPU_UUID}"), 8_192);
    {
        let mut store =
            Store::open_with_config(paths.clone(), original.clone(), probe_startup_identity())
                .unwrap();
        let hash = normalized_payload_hash(&job).unwrap();
        store.submit(Uuid::now_v7(), &hash, &job).unwrap();
    }

    let replacement_uuid = "GPU-b2255d37-b26d-dcb2-4c8b-981d866ff19b";
    let mut incompatible = original;
    incompatible.observation.gpu_slot_uuid = Some(replacement_uuid.into());
    incompatible.observation.vram_safety_margin_mb = 1_024;
    incompatible.resources.custom = [(format!("vram_mb:{replacement_uuid}"), 24_000)].into();
    let error = Store::open_with_config(paths, incompatible, probe_startup_identity())
        .err()
        .expect("GPU placement change must reject daemon startup");
    assert!(
        error.to_string().contains("retained Job")
            && error
                .to_string()
                .contains("differs from host gpu_slot_uuid"),
        "unexpected startup error: {error}"
    );
}
