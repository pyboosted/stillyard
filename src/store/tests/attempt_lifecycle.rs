use super::*;

#[test]
fn postcondition_retry_keeps_one_job_and_exposes_ordered_attempts() {
    let temp = tempfile::tempdir().unwrap();
    let mut store =
        Store::open_with_capacities(StorePaths::new(temp.path().to_path_buf()), capacities())
            .unwrap();
    let mut job = spec(temp.path());
    job.resources.cargo_slots = Some(1);
    job.retry = RetryPolicy {
        max_attempts: 2,
        backoff_seconds: 0,
        retryable: vec!["postcondition_retryable".into()],
    };
    job.postconditions.push(PostconditionSpec {
        executable: temp.path().join("validate.exe"),
        args: vec!["--result".into()],
        working_directory: None,
        accepted_exit_codes: vec![0],
        retryable_exit_codes: vec![10],
    });
    let hash = normalized_payload_hash(&job).unwrap();
    let receipt = store.submit(Uuid::now_v7(), &hash, &job).unwrap().receipt;
    let first = store.prepare_job(receipt.job_id).unwrap().unwrap();
    store
        .mark_invocation_resolved(&first, Some(0), None)
        .unwrap();

    let mut contender = spec(temp.path());
    contender.resources.cargo_slots = Some(1);
    let contender_hash = normalized_payload_hash(&contender).unwrap();
    let contender = store
        .submit(Uuid::now_v7(), &contender_hash, &contender)
        .unwrap()
        .receipt;
    assert!(
        contender
            .blockers
            .iter()
            .any(|blocker| blocker.code == "resource_busy"),
        "primary cleanup must not release the Attempt Lease before postconditions"
    );

    let validator = store.prepare_postcondition(&first, 0).unwrap();
    assert!(
        store
            .status(contender.job_id)
            .unwrap()
            .blockers
            .iter()
            .any(|blocker| blocker.code == "resource_busy"),
        "preparing a postcondition must retain the complete Attempt Lease"
    );
    store
        .mark_invocation_resolved(&validator, Some(10), Some(ExitClassification::Retryable))
        .unwrap();
    assert!(
        store
            .status(contender.job_id)
            .unwrap()
            .blockers
            .iter()
            .any(|blocker| blocker.code == "resource_busy"),
        "resolving the validator must not release the Lease before Attempt settlement"
    );
    assert!(
        store
            .settle_attempt(&first, AttemptVerdict::PostconditionRetryable)
            .unwrap()
    );
    let between = store.status(receipt.job_id).unwrap();
    assert_eq!(between.state, JobState::Pending);
    assert_eq!(between.attempts.len(), 1);
    assert_eq!(
        between.attempts[0].verdict,
        Some(AttemptVerdict::PostconditionRetryable)
    );
    assert_eq!(between.attempts[0].invocations.len(), 2);
    assert_eq!(
        between.attempts[0].invocations[1].exit_classification,
        Some(ExitClassification::Retryable)
    );

    let second = store.prepare_job(receipt.job_id).unwrap().unwrap();
    assert_ne!(first.attempt_id, second.attempt_id);
    store
        .mark_invocation_resolved(&second, Some(0), None)
        .unwrap();
    let validator = store.prepare_postcondition(&second, 0).unwrap();
    store
        .mark_invocation_resolved(&validator, Some(0), Some(ExitClassification::Accepted))
        .unwrap();
    assert!(
        !store
            .settle_attempt(&second, AttemptVerdict::Succeeded)
            .unwrap()
    );
    let final_snapshot = store.status(receipt.job_id).unwrap();
    assert_eq!(final_snapshot.outcome, Some(JobOutcome::Succeeded));
    assert_eq!(
        final_snapshot
            .attempts
            .iter()
            .map(|attempt| attempt.attempt_index)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
}

#[test]
fn plain_cancel_covers_queued_active_and_backoff_without_successors() {
    let temp = tempfile::tempdir().unwrap();
    let mut store =
        Store::open_with_capacities(StorePaths::new(temp.path().to_path_buf()), capacities())
            .unwrap();
    let batch = BatchSpec {
        spec_version: SPEC_VERSION,
        jobs: vec![
            member("reviewer", spec(temp.path()), vec![]),
            member(
                "collect",
                spec(temp.path()),
                vec![DependencySpec {
                    job: "reviewer".into(),
                    on: DependencyKind::Terminal,
                }],
            ),
        ],
    };
    let hash = normalized_batch_payload_hash(&batch).unwrap();
    let receipt = store
        .submit_batch(Uuid::now_v7(), &hash, &batch)
        .unwrap()
        .receipt;
    let reviewer = receipt.jobs[0].receipt.job_id;
    let collect = receipt.jobs[1].receipt.job_id;
    assert!(matches!(
        store.cancel_jobs(&vec![reviewer; MAX_CANCEL_JOBS + 1]),
        Err(StoreError::InvalidSpec(_))
    ));
    let canceled = store.cancel_jobs(&[reviewer]).unwrap();
    assert_eq!(canceled[0].outcome, Some(JobOutcome::Canceled));
    assert!(canceled[0].cancel_requested);
    let prepared_collect = store.prepare_next_job().unwrap().unwrap();
    assert_eq!(
        prepared_collect.job_id, collect,
        "plain cancel must not select collect"
    );

    let mut active_spec = spec(temp.path());
    active_spec.retry = RetryPolicy {
        max_attempts: 2,
        backoff_seconds: 60,
        retryable: vec!["process_failed".into()],
    };
    let hash = normalized_payload_hash(&active_spec).unwrap();
    let active = store
        .submit(Uuid::now_v7(), &hash, &active_spec)
        .unwrap()
        .receipt
        .job_id;
    let prepared = store.prepare_job(active).unwrap().unwrap();
    let canceling = store.cancel_jobs(&[active]).unwrap();
    assert_eq!(canceling[0].state, JobState::Active);
    assert!(canceling[0].cancel_requested);
    assert!(store.cancel_requested(active).unwrap());
    store
        .mark_invocation_resolved(&prepared, Some(1), None)
        .unwrap();
    assert!(
        !store
            .settle_attempt(&prepared, AttemptVerdict::ProcessFailed)
            .unwrap()
    );
    let active = store.status(active).unwrap();
    assert_eq!(active.outcome, Some(JobOutcome::Canceled));
    assert_eq!(active.attempts[0].verdict, Some(AttemptVerdict::Canceled));
}

#[test]
fn expired_blocked_retry_does_not_spin_and_backoff_cancel_is_terminal() {
    let temp = tempfile::tempdir().unwrap();
    let mut store =
        Store::open_with_capacities(StorePaths::new(temp.path().to_path_buf()), capacities())
            .unwrap();
    let mut retry_spec = spec(temp.path());
    retry_spec.resources.cargo_slots = Some(1);
    retry_spec.retry = RetryPolicy {
        max_attempts: 2,
        backoff_seconds: 60,
        retryable: vec!["process_failed".into()],
    };
    let hash = normalized_payload_hash(&retry_spec).unwrap();
    let retry_job = store
        .submit(Uuid::now_v7(), &hash, &retry_spec)
        .unwrap()
        .receipt
        .job_id;
    let first = store.prepare_job(retry_job).unwrap().unwrap();
    store
        .mark_invocation_resolved(&first, Some(1), None)
        .unwrap();
    assert!(
        store
            .settle_attempt(&first, AttemptVerdict::ProcessFailed)
            .unwrap()
    );

    let mut holder_spec = spec(temp.path());
    holder_spec.resources.cargo_slots = Some(1);
    let hash = normalized_payload_hash(&holder_spec).unwrap();
    let holder = store
        .submit(Uuid::now_v7(), &hash, &holder_spec)
        .unwrap()
        .receipt
        .job_id;
    store.prepare_job(holder).unwrap().unwrap();
    let boundary = now_millis();
    store
        .connection
        .execute(
            "UPDATE jobs SET retry_not_before_ms = ?2 WHERE id = ?1",
            params![retry_job.entity_uuid().to_string(), boundary],
        )
        .unwrap();
    assert!(
        store.next_retry_delay(boundary - 1).unwrap().is_some(),
        "a retry that expires during a scheduling pass needs one immediate rescan"
    );
    store
        .connection
        .execute(
            "UPDATE jobs SET retry_not_before_ms = ?2 WHERE id = ?1",
            params![retry_job.entity_uuid().to_string(), now_millis() - 1],
        )
        .unwrap();

    assert!(store.prepare_job(retry_job).unwrap().is_none());
    let after_expiry = now_millis();
    assert_eq!(
        store.next_retry_delay(after_expiry).unwrap(),
        None,
        "an expired retry blocked on a Lease must wait for the Lease-release wakeup"
    );
    let canceled = store.cancel_jobs(&[retry_job]).unwrap();
    assert_eq!(canceled[0].outcome, Some(JobOutcome::Canceled));
    assert_eq!(canceled[0].attempts.len(), 1);
    assert!(store.prepare_job(retry_job).unwrap().is_none());
}

#[test]
fn impact_rules_block_admission_and_ancestor_waits_symmetrically() {
    let temp = tempfile::tempdir().unwrap();
    let config = HostConfig {
        resources: capacities(),
        profiles: Default::default(),
        impact_incompatibilities: [(
            "measurement".into(),
            vec!["cpu_heavy".into(), "gpu_heavy".into()],
        )]
        .into(),
    };
    let mut store = Store::open_with_config(
        StorePaths::new(temp.path().to_path_buf()),
        config,
        probe_startup_identity(),
    )
    .unwrap();
    let mut cpu = spec(temp.path());
    cpu.resources.impacts = vec!["cpu_heavy".into()];
    let hash = normalized_payload_hash(&cpu).unwrap();
    let cpu = store.submit(Uuid::now_v7(), &hash, &cpu).unwrap().receipt;
    store.prepare_job(cpu.job_id).unwrap().unwrap();

    let mut measurement = spec(temp.path());
    measurement.resources.impacts = vec!["measurement".into()];
    let hash = normalized_payload_hash(&measurement).unwrap();
    let measurement = store
        .submit(Uuid::now_v7(), &hash, &measurement)
        .unwrap()
        .receipt;
    assert!(
        measurement
            .blockers
            .iter()
            .any(|blocker| blocker.code == "impact_busy")
    );
    let daemon = store
        .daemon_status(r"\\.\pipe\stillyard-store-test")
        .unwrap();
    assert_eq!(daemon.daemon_generation, cpu.daemon_generation);
    assert!(!daemon.config_sha256.is_empty());
}

#[test]
fn receipt_preserves_accepting_generation_across_daemon_restart() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StorePaths::new(temp.path().to_path_buf());
    let key = Uuid::now_v7();
    let job = spec(temp.path());
    let hash = normalized_payload_hash(&job).unwrap();
    let accepted_generation = {
        let mut store = Store::open(paths.clone()).unwrap();
        store
            .submit(key, &hash, &job)
            .unwrap()
            .receipt
            .daemon_generation
    };
    let mut reopened = Store::open(paths).unwrap();
    assert_ne!(reopened.daemon_generation, accepted_generation);
    let replay = reopened.submit(key, &hash, &job).unwrap();
    assert!(!replay.should_schedule);
    assert_eq!(replay.receipt.daemon_generation, accepted_generation);
}

#[test]
fn root_exit_is_visible_before_containment_resolution() {
    let temp = tempfile::tempdir().unwrap();
    let mut store =
        Store::open_with_capacities(StorePaths::new(temp.path().to_path_buf()), capacities())
            .unwrap();
    let job = spec(temp.path());
    let hash = normalized_payload_hash(&job).unwrap();
    let receipt = store.submit(Uuid::now_v7(), &hash, &job).unwrap().receipt;
    let prepared = store.prepare_job(receipt.job_id).unwrap().unwrap();
    store.mark_started(&prepared, 42, "test-hash").unwrap();
    store.mark_root_exited(&prepared, 0).unwrap();

    let snapshot = store.status(receipt.job_id).unwrap();
    let invocation = &snapshot.attempts[0].invocations[0];
    assert_eq!(invocation.state, InvocationState::Exited);
    assert_eq!(invocation.root_exit_code, Some(0));
    assert_eq!(invocation.containment.state, ContainmentState::Live);
}

#[test]
fn snapshot_diagnostic_budget_keeps_newest_utf8_suffixes() {
    let mut newest = "new".to_owned();
    let mut older = "éolder".to_owned();
    let mut remaining = 5;
    keep_tail_within_budget(&mut newest, &mut remaining);
    keep_tail_within_budget(&mut older, &mut remaining);

    assert_eq!(newest, "new");
    assert_eq!(older, "er");
    assert_eq!(remaining, 0);
}
