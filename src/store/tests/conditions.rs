use super::*;
use crate::{ConditionSpec, PathConditionState, ProbeCondition};

fn none_deadline(predicate: ConditionPredicate) -> ConditionSpec {
    ConditionSpec {
        predicate,
        deadline: ConditionDeadline::None,
        on_deadline: ConditionDeadlineOutcome::Failed,
    }
}

fn probe_condition(root: &Path) -> ConditionSpec {
    none_deadline(ConditionPredicate::Probe {
        probe: Box::new(ProbeCondition {
            executable: PathBuf::from(r"C:\Windows\System32\cmd.exe"),
            args: vec!["/d".into(), "/c".into(), "exit 0".into()],
            working_directory: root.to_path_buf(),
            environment: EnvironmentSpec::default(),
            resources: ResourceClaims::default(),
            timeout_seconds: 5,
            interval_seconds: 1,
            accepted_exit_codes: vec![0],
        }),
    })
}

fn submit_condition_job(store: &mut Store, spec: &JobSpec) -> SubmitResult {
    let hash = normalized_payload_hash(spec).unwrap();
    store.submit(Uuid::now_v7(), &hash, spec).unwrap()
}

fn expire_condition_evidence(store: &Store, job_id: JobId) {
    store
        .connection
        .execute(
            "UPDATE observations SET fresh_until_ms = 0
             WHERE condition_id IN (SELECT id FROM conditions WHERE job_id = ?1)",
            [job_id.entity_uuid().to_string()],
        )
        .unwrap();
}

fn prepare_until_ready(store: &mut Store, job_id: JobId) -> PreparedJob {
    for _ in 0..8 {
        if let Some(job) = store.prepare_job(job_id).unwrap() {
            return job;
        }
    }
    panic!("Job did not become ready after bounded scheduler progress");
}

#[test]
fn path_transition_is_anchored_at_acceptance_and_rescanned_before_lease() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("ready.flag");
    let mut store =
        Store::open_with_capacities(StorePaths::new(temp.path().join("store")), capacities())
            .unwrap();
    let mut spec = spec(temp.path());
    spec.conditions
        .push(none_deadline(ConditionPredicate::PathTransition {
            path: path.clone(),
            from: PathConditionState::Absent,
            to: PathConditionState::Present,
        }));
    let receipt = submit_condition_job(&mut store, &spec);
    assert_eq!(receipt.receipt.conditions.len(), 1);
    assert_eq!(receipt.receipt.conditions[0].state, ConditionState::Waiting);
    assert!(store.prepare_job(receipt.receipt.job_id).unwrap().is_none());

    std::fs::write(&path, b"ready").unwrap();
    expire_condition_evidence(&store, receipt.receipt.job_id);
    let prepared = prepare_until_ready(&mut store, receipt.receipt.job_id);
    assert_eq!(prepared.role, InvocationRole::Primary);
    std::fs::remove_file(&path).unwrap();
    assert!(matches!(
        store.authorize_condition_release(&prepared, None).unwrap(),
        ReleaseAuthorization::Authorized { .. }
    ));
    let snapshot = store.status(receipt.receipt.job_id).unwrap();
    assert_eq!(snapshot.conditions[0].state, ConditionState::Satisfied);
    assert!(matches!(
        snapshot.conditions[0]
            .last_observation
            .as_ref()
            .map(|observation| &observation.value),
        Some(ConditionObservationValue::Path { exists: false })
    ));
    assert_eq!(snapshot.conditions[0].state, ConditionState::Satisfied);
}

#[test]
fn path_absent_waits_for_authoritative_absence() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("present.flag");
    std::fs::write(&path, b"present").unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().join("store"))).unwrap();
    let mut job = spec(temp.path());
    job.conditions
        .push(none_deadline(ConditionPredicate::PathAbsent {
            path: path.clone(),
        }));
    let receipt = submit_condition_job(&mut store, &job);
    assert_eq!(receipt.receipt.conditions[0].state, ConditionState::Waiting);
    std::fs::remove_file(path).unwrap();
    expire_condition_evidence(&store, receipt.receipt.job_id);
    assert_eq!(
        prepare_until_ready(&mut store, receipt.receipt.job_id).role,
        InvocationRole::Primary
    );
}

#[test]
fn eta_is_unknown_for_external_predicates_and_lower_bound_for_not_before() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().join("store"))).unwrap();
    let mut path_spec = spec(temp.path());
    path_spec
        .conditions
        .push(none_deadline(ConditionPredicate::PathExists {
            path: temp.path().join("missing"),
        }));
    let path = submit_condition_job(&mut store, &path_spec);
    assert_eq!(
        path.receipt.estimate.confidence,
        EstimateConfidence::Unknown
    );
    assert_eq!(path.receipt.estimate.start_in_millis, None);

    let target = now_millis() + 10_000;
    let mut time_spec = spec(temp.path());
    time_spec
        .conditions
        .push(none_deadline(ConditionPredicate::NotBefore {
            unix_millis: target,
        }));
    let time = submit_condition_job(&mut store, &time_spec);
    assert_eq!(
        time.receipt.estimate.confidence,
        EstimateConfidence::LowerBoundOnly
    );
    assert!(
        time.receipt
            .estimate
            .start_in_millis
            .is_some_and(|delay| { (9_000..=10_000).contains(&delay) })
    );
}

#[test]
fn relative_deadline_is_durable_and_does_not_create_an_attempt() {
    let temp = tempfile::tempdir().unwrap();
    let missing = temp.path().join("never-created");
    let paths = StorePaths::new(temp.path().join("store"));
    let mut store = Store::open(paths.clone()).unwrap();
    let mut spec = spec(temp.path());
    spec.conditions.push(ConditionSpec {
        predicate: ConditionPredicate::PathExists { path: missing },
        deadline: ConditionDeadline::Relative { seconds: 10 },
        on_deadline: ConditionDeadlineOutcome::Canceled,
    });
    let receipt = submit_condition_job(&mut store, &spec);
    let accepted = receipt.receipt.accepted_unix_millis;
    let deadline = receipt.receipt.conditions[0]
        .deadline_unix_millis
        .expect("relative deadline is resolved at acceptance");
    assert_eq!(deadline, accepted + 10_000);
    store
        .connection
        .execute(
            "UPDATE conditions SET deadline_ms = ?2 WHERE job_id = ?1",
            params![
                receipt.receipt.job_id.entity_uuid().to_string(),
                now_millis() - 1
            ],
        )
        .unwrap();
    drop(store);

    let mut reopened = Store::open(paths).unwrap();
    assert!(
        reopened
            .prepare_job(receipt.receipt.job_id)
            .unwrap()
            .is_none()
    );
    let snapshot = reopened.status(receipt.receipt.job_id).unwrap();
    assert_eq!(snapshot.state, JobState::Final);
    assert_eq!(snapshot.outcome, Some(JobOutcome::Canceled));
    assert_eq!(
        snapshot.reason_code.as_deref(),
        Some("condition_deadline_expired")
    );
    assert!(!snapshot.cancel_requested);
    assert!(snapshot.attempts.is_empty());
}

#[test]
fn probe_uses_its_own_lease_and_releases_it_before_primary_work() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = HostConfig::default();
    config.resources.cargo_slots = 2;
    let mut store = Store::open_with_config(
        StorePaths::new(temp.path().join("store")),
        config,
        probe_startup_identity(),
    )
    .unwrap();
    let mut spec = spec(temp.path());
    spec.resources.cargo_slots = Some(2);
    spec.conditions
        .push(none_deadline(ConditionPredicate::Probe {
            probe: Box::new(ProbeCondition {
                executable: PathBuf::from(r"C:\Windows\System32\cmd.exe"),
                args: vec!["/d".into(), "/c".into(), "exit 0".into()],
                working_directory: PathBuf::from(r"C:\"),
                environment: EnvironmentSpec::default(),
                resources: ResourceClaims {
                    cargo_slots: Some(1),
                    ..ResourceClaims::default()
                },
                timeout_seconds: 5,
                interval_seconds: 1,
                accepted_exit_codes: vec![0],
            }),
        }));
    let receipt = submit_condition_job(&mut store, &spec);
    let probe = prepare_until_ready(&mut store, receipt.receipt.job_id);
    assert_eq!(probe.role, InvocationRole::Probe);
    let granted: Vec<String> = store
        .connection
        .prepare("SELECT claims_json FROM leases WHERE state = 'granted'")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(granted.len(), 1);
    assert_eq!(
        serde_json::from_str::<ResolvedClaims>(&granted[0])
            .unwrap()
            .cargo_slots,
        1
    );

    store.mark_started(&probe, 42, "probe-image").unwrap();
    store.mark_root_exited(&probe, 0).unwrap();
    store.settle_probe(&probe, Some(0), false).unwrap();
    let primary = prepare_until_ready(&mut store, receipt.receipt.job_id);
    assert_eq!(primary.role, InvocationRole::Primary);
    let granted: Vec<String> = store
        .connection
        .prepare("SELECT claims_json FROM leases WHERE state = 'granted'")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        granted.len(),
        1,
        "probe Lease must be released before work Lease"
    );
    assert_eq!(
        serde_json::from_str::<ResolvedClaims>(&granted[0])
            .unwrap()
            .cargo_slots,
        2
    );
    let snapshot = store.status(receipt.receipt.job_id).unwrap();
    assert_eq!(snapshot.conditions[0].state, ConditionState::Satisfied);
    assert!(
        snapshot.attempts[0]
            .invocations
            .iter()
            .any(|invocation| invocation.role == InvocationRole::Probe
                && invocation.exit_classification == Some(ExitClassification::Accepted))
    );
}

#[test]
fn deadline_that_wins_during_a_probe_remains_terminal_after_probe_cleanup() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().join("store"))).unwrap();
    let mut spec = spec(temp.path());
    spec.conditions.push(ConditionSpec {
        predicate: ConditionPredicate::Probe {
            probe: Box::new(ProbeCondition {
                executable: PathBuf::from(r"C:\Windows\System32\cmd.exe"),
                args: vec!["/d".into(), "/c".into(), "exit 9".into()],
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
    let receipt = submit_condition_job(&mut store, &spec);
    let probe = prepare_until_ready(&mut store, receipt.receipt.job_id);
    store.mark_started(&probe, 42, "probe-image").unwrap();
    store
        .connection
        .execute(
            "UPDATE conditions SET deadline_ms = ?2 WHERE job_id = ?1",
            params![
                receipt.receipt.job_id.entity_uuid().to_string(),
                now_millis() - 1
            ],
        )
        .unwrap();
    assert!(store.prepare_job(receipt.receipt.job_id).unwrap().is_none());
    let pending = store.status(receipt.receipt.job_id).unwrap();
    assert_eq!(pending.state, JobState::Pending);
    assert_eq!(pending.attempts[0].verdict, None);
    assert_eq!(pending.attempts[0].finished_unix_millis, None);
    assert_eq!(
        pending.attempts[0].invocations[0].state,
        InvocationState::Started
    );
    assert_eq!(
        pending.attempts[0].invocations[0].containment.state,
        ContainmentState::Live
    );
    store.mark_root_exited(&probe, 9).unwrap();
    store.settle_probe(&probe, Some(9), false).unwrap();

    let snapshot = store.status(receipt.receipt.job_id).unwrap();
    assert_eq!(snapshot.outcome, Some(JobOutcome::Failed));
    assert_eq!(snapshot.conditions[0].state, ConditionState::Failed);
    assert_eq!(
        snapshot.reason_code.as_deref(),
        Some("condition_deadline_expired")
    );
    assert!(!snapshot.cancel_requested);
    let (next_probe, granted): (Option<i64>, bool) = store
        .connection
        .query_row(
            "SELECT conditions.next_probe_ms,
                    EXISTS(SELECT 1 FROM leases WHERE state = 'granted')
             FROM conditions WHERE conditions.job_id = ?1",
            [receipt.receipt.job_id.entity_uuid().to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(next_probe, None);
    assert!(!granted);
}

#[test]
fn pre_release_path_change_replans_same_attempt_then_exhausts_as_readiness_unstable() {
    let temp = tempfile::tempdir().unwrap();
    let ready = temp.path().join("ready.flag");
    std::fs::write(&ready, b"ready").unwrap();
    let mut config = HostConfig::default();
    config.observation.pre_release_max_deferrals = 1;
    let mut store = Store::open_with_config(
        StorePaths::new(temp.path().join("store")),
        config,
        probe_startup_identity(),
    )
    .unwrap();
    let mut spec = spec(temp.path());
    spec.conditions
        .push(none_deadline(ConditionPredicate::PathExists {
            path: ready.clone(),
        }));
    let receipt = submit_condition_job(&mut store, &spec);
    let primary = prepare_until_ready(&mut store, receipt.receipt.job_id);
    let attempt_id = primary.attempt_id;
    std::fs::remove_file(&ready).unwrap();
    let authorization = store.authorize_condition_release(&primary, None).unwrap();
    let ReleaseAuthorization::Deferred { reason } = authorization else {
        panic!("changed path evidence must defer release");
    };
    store
        .connection
        .execute(
            "UPDATE containments SET state = 'live' WHERE id = ?1",
            [primary.containment_id.entity_uuid().to_string()],
        )
        .unwrap();
    store.replan_never_run(&primary, &reason).unwrap();
    let snapshot = store.status(receipt.receipt.job_id).unwrap();
    assert_eq!(snapshot.state, JobState::Final);
    assert_eq!(snapshot.outcome, Some(JobOutcome::Failed));
    assert_eq!(snapshot.reason_code.as_deref(), Some("readiness_unstable"));
    assert_eq!(snapshot.attempts[0].attempt_id, attempt_id);
    assert_eq!(
        snapshot.attempts[0].reason_code.as_deref(),
        Some("readiness_unstable")
    );
}

#[test]
fn first_pre_release_deferral_replans_the_same_attempt_and_preserves_budget() {
    let temp = tempfile::tempdir().unwrap();
    let ready = temp.path().join("ready.flag");
    std::fs::write(&ready, b"ready").unwrap();
    let mut config = HostConfig::default();
    config.observation.pre_release_max_deferrals = 2;
    config.observation.pre_release_backoff_millis = 100;
    let mut store = Store::open_with_config(
        StorePaths::new(temp.path().join("store")),
        config,
        probe_startup_identity(),
    )
    .unwrap();
    let mut job = spec(temp.path());
    job.conditions
        .push(none_deadline(ConditionPredicate::PathExists {
            path: ready.clone(),
        }));
    let receipt = submit_condition_job(&mut store, &job);
    let first = prepare_until_ready(&mut store, receipt.receipt.job_id);
    std::fs::remove_file(&ready).unwrap();
    let ReleaseAuthorization::Deferred { reason } =
        store.authorize_condition_release(&first, None).unwrap()
    else {
        panic!("changed path evidence must defer release");
    };
    store
        .connection
        .execute(
            "UPDATE containments SET state = 'live' WHERE id = ?1",
            [first.containment_id.entity_uuid().to_string()],
        )
        .unwrap();
    store.replan_never_run(&first, &reason).unwrap();
    let replanned = store.status(receipt.receipt.job_id).unwrap();
    assert_eq!(replanned.state, JobState::Pending);
    assert_eq!(replanned.attempts.len(), 1);
    assert_eq!(replanned.attempts[0].attempt_id, first.attempt_id);
    assert_eq!(replanned.admission.unwrap().deferral_count, 1);

    std::fs::write(&ready, b"ready-again").unwrap();
    store
        .connection
        .execute(
            "UPDATE jobs SET retry_not_before_ms = 0 WHERE id = ?1",
            [receipt.receipt.job_id.entity_uuid().to_string()],
        )
        .unwrap();
    expire_condition_evidence(&store, receipt.receipt.job_id);
    let replacement = prepare_until_ready(&mut store, receipt.receipt.job_id);
    assert_eq!(replacement.attempt_id, first.attempt_id);
    assert_ne!(replacement.invocation_id, first.invocation_id);
}

#[test]
fn post_commit_freshness_expiry_replans_the_never_resumed_attempt() {
    let temp = tempfile::tempdir().unwrap();
    let ready = temp.path().join("ready.flag");
    std::fs::write(&ready, b"ready").unwrap();
    let mut config = HostConfig::default();
    config.observation.pre_release_max_deferrals = 2;
    config.observation.pre_release_backoff_millis = 0;
    let mut store = Store::open_with_config(
        StorePaths::new(temp.path().join("store")),
        config,
        probe_startup_identity(),
    )
    .unwrap();
    let mut job = spec(temp.path());
    job.conditions
        .push(none_deadline(ConditionPredicate::PathExists {
            path: ready,
        }));
    let receipt = submit_condition_job(&mut store, &job);
    let first = prepare_until_ready(&mut store, receipt.receipt.job_id);
    store
        .connection
        .execute(
            "UPDATE containments SET state = 'live' WHERE id = ?1",
            [first.containment_id.entity_uuid().to_string()],
        )
        .unwrap();
    assert!(matches!(
        store.authorize_condition_release(&first, None).unwrap(),
        ReleaseAuthorization::Authorized { .. }
    ));
    let authorized: (String, String) = store
        .connection
        .query_row(
            "SELECT attempts.state, invocations.state FROM attempts
             JOIN invocations ON invocations.attempt_id = attempts.id
             WHERE attempts.id = ?1 AND invocations.id = ?2",
            params![
                first.attempt_id.entity_uuid().to_string(),
                first.invocation_id.entity_uuid().to_string()
            ],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(authorized, ("running".into(), "started".into()));

    store
        .replan_never_run(&first, "Condition release evidence expired before resume")
        .unwrap();
    let replanned = store.status(receipt.receipt.job_id).unwrap();
    assert_eq!(replanned.state, JobState::Pending);
    assert_eq!(replanned.started_unix_millis, None);
    assert_eq!(replanned.attempts[0].attempt_id, first.attempt_id);
    assert_eq!(replanned.attempts[0].started_unix_millis, None);
    assert_eq!(
        replanned.attempts[0].invocations[0].started_unix_millis,
        None
    );
    expire_condition_evidence(&store, receipt.receipt.job_id);
    let replacement = prepare_until_ready(&mut store, receipt.receipt.job_id);
    assert_eq!(replacement.attempt_id, first.attempt_id);
    assert_ne!(replacement.invocation_id, first.invocation_id);
}

#[test]
fn pre_release_cancellation_is_never_projected_as_released() {
    for cause in ["cancel", "deadline_canceled"] {
        let temp = tempfile::tempdir().unwrap();
        let ready = temp.path().join("ready.flag");
        std::fs::write(&ready, b"ready").unwrap();
        let mut store = Store::open(StorePaths::new(temp.path().join("store"))).unwrap();
        let mut job = spec(temp.path());
        job.conditions.push(ConditionSpec {
            predicate: ConditionPredicate::PathExists { path: ready },
            deadline: ConditionDeadline::Relative { seconds: 60 },
            on_deadline: ConditionDeadlineOutcome::Canceled,
        });
        let receipt = submit_condition_job(&mut store, &job).receipt;
        let primary = prepare_until_ready(&mut store, receipt.job_id);
        store
            .connection
            .execute(
                "UPDATE containments SET state = 'live' WHERE id = ?1",
                [primary.containment_id.entity_uuid().to_string()],
            )
            .unwrap();

        if cause == "cancel" {
            store.cancel_jobs(&[receipt.job_id]).unwrap();
        } else {
            store
                .connection
                .execute(
                    "UPDATE conditions SET deadline_ms = ?2 WHERE job_id = ?1",
                    params![receipt.job_id.entity_uuid().to_string(), now_millis() - 1],
                )
                .unwrap();
        }
        let reason = store
            .pre_resume_defer_reason(receipt.job_id)
            .unwrap()
            .expect("pre-release terminal intent must stop resume");
        store.replan_never_run(&primary, &reason).unwrap();

        let snapshot = store.status(receipt.job_id).unwrap();
        assert_eq!(snapshot.outcome, Some(JobOutcome::Canceled), "case {cause}");
        let admission = snapshot.attempts[0]
            .admission
            .as_ref()
            .expect("Condition attempt has admission history");
        assert_eq!(
            admission.state,
            AdmissionDecisionState::Reserved,
            "case {cause}"
        );
        assert!(!admission.final_sample, "case {cause}");
    }
}

#[test]
fn condition_release_is_publicly_released_while_running_and_after_success() {
    let temp = tempfile::tempdir().unwrap();
    let ready = temp.path().join("ready.flag");
    std::fs::write(&ready, b"ready").unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().join("store"))).unwrap();
    let mut job = spec(temp.path());
    job.conditions
        .push(none_deadline(ConditionPredicate::PathExists {
            path: ready,
        }));
    let receipt = submit_condition_job(&mut store, &job).receipt;
    let primary = prepare_until_ready(&mut store, receipt.job_id);
    store
        .connection
        .execute(
            "UPDATE containments SET state = 'live' WHERE id = ?1",
            [primary.containment_id.entity_uuid().to_string()],
        )
        .unwrap();
    assert!(matches!(
        store.authorize_condition_release(&primary, None).unwrap(),
        ReleaseAuthorization::Authorized { .. }
    ));
    assert_eq!(
        store.status(receipt.job_id).unwrap().attempts[0]
            .admission
            .as_ref()
            .map(|admission| admission.state),
        Some(AdmissionDecisionState::Released)
    );

    store.mark_root_exited(&primary, 0).unwrap();
    store
        .mark_invocation_resolved(&primary, Some(0), None)
        .unwrap();
    store
        .record_primary_result(
            &primary,
            InvocationVerdict::Succeeded,
            TerminationReason::Exited,
        )
        .unwrap();
    store
        .settle_attempt(&primary, AttemptVerdict::Succeeded)
        .unwrap();
    let final_snapshot = store.status(receipt.job_id).unwrap();
    assert_eq!(final_snapshot.outcome, Some(JobOutcome::Succeeded));
    assert_eq!(
        final_snapshot.attempts[0]
            .admission
            .as_ref()
            .map(|admission| admission.state),
        Some(AdmissionDecisionState::Released)
    );
}

#[test]
fn postcondition_does_not_inherit_primary_conditions_after_release() {
    let temp = tempfile::tempdir().unwrap();
    let ready = temp.path().join("ready.flag");
    std::fs::write(&ready, b"ready").unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().join("store"))).unwrap();
    let mut job = spec(temp.path());
    job.conditions
        .push(none_deadline(ConditionPredicate::PathExists {
            path: ready,
        }));
    job.postconditions.push(PostconditionSpec {
        executable: temp.path().join("validator.exe"),
        args: Vec::new(),
        working_directory: None,
        accepted_exit_codes: vec![0],
        retryable_exit_codes: Vec::new(),
    });
    let receipt = submit_condition_job(&mut store, &job).receipt;
    let primary = prepare_until_ready(&mut store, receipt.job_id);
    store
        .connection
        .execute(
            "UPDATE containments SET state = 'live' WHERE id = ?1",
            [primary.containment_id.entity_uuid().to_string()],
        )
        .unwrap();
    assert!(matches!(
        store.authorize_condition_release(&primary, None).unwrap(),
        ReleaseAuthorization::Authorized { .. }
    ));
    store.mark_root_exited(&primary, 0).unwrap();
    store
        .mark_invocation_resolved(&primary, Some(0), None)
        .unwrap();
    store
        .record_primary_result(
            &primary,
            InvocationVerdict::Succeeded,
            TerminationReason::Exited,
        )
        .unwrap();
    store
        .connection
        .execute(
            "UPDATE conditions SET state = 'failed', deadline_ms = ?2
             WHERE job_id = ?1",
            params![receipt.job_id.entity_uuid().to_string(), now_millis() - 1],
        )
        .unwrap();

    let postcondition = store.prepare_postcondition(&primary, 0).unwrap();
    assert!(postcondition.spec.conditions.is_empty());
    assert_eq!(postcondition.role, InvocationRole::Postcondition);
    store
        .mark_started(&postcondition, 81, "postcondition-image")
        .unwrap();
    assert_eq!(
        store.status(receipt.job_id).unwrap().attempts[0]
            .invocations
            .last()
            .map(|invocation| invocation.state),
        Some(InvocationState::Started)
    );
}

#[test]
fn postcondition_start_failure_preserves_primary_start_and_ignores_condition_deadline() {
    let temp = tempfile::tempdir().unwrap();
    let ready = temp.path().join("ready.flag");
    std::fs::write(&ready, b"ready").unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().join("store"))).unwrap();
    let mut job = spec(temp.path());
    job.conditions.push(ConditionSpec {
        predicate: ConditionPredicate::PathExists { path: ready },
        deadline: ConditionDeadline::Relative { seconds: 60 },
        on_deadline: ConditionDeadlineOutcome::Canceled,
    });
    job.postconditions.push(PostconditionSpec {
        executable: temp.path().join("validator.exe"),
        args: Vec::new(),
        working_directory: None,
        accepted_exit_codes: vec![0],
        retryable_exit_codes: Vec::new(),
    });
    let receipt = submit_condition_job(&mut store, &job).receipt;
    let primary = prepare_until_ready(&mut store, receipt.job_id);
    store
        .connection
        .execute(
            "UPDATE containments SET state = 'live' WHERE id = ?1",
            [primary.containment_id.entity_uuid().to_string()],
        )
        .unwrap();
    assert!(matches!(
        store.authorize_condition_release(&primary, None).unwrap(),
        ReleaseAuthorization::Authorized { .. }
    ));
    store.mark_root_exited(&primary, 0).unwrap();
    store
        .mark_invocation_resolved(&primary, Some(0), None)
        .unwrap();
    let primary_result = store
        .record_primary_result(
            &primary,
            InvocationVerdict::Succeeded,
            TerminationReason::Exited,
        )
        .unwrap();
    let primary_started = primary_result
        .started_unix_millis
        .expect("released primary has a start timestamp");
    let postcondition = store.prepare_postcondition(&primary, 0).unwrap();
    store
        .connection
        .execute(
            "UPDATE conditions SET state = 'failed', deadline_ms = ?2
             WHERE job_id = ?1",
            params![receipt.job_id.entity_uuid().to_string(), now_millis() - 1],
        )
        .unwrap();

    store
        .mark_finished(
            &postcondition,
            None,
            JobOutcome::Failed,
            "postcondition_failed",
        )
        .unwrap();

    let snapshot = store.status(receipt.job_id).unwrap();
    assert_eq!(snapshot.state, JobState::Final);
    assert_eq!(snapshot.outcome, Some(JobOutcome::Failed));
    assert_eq!(snapshot.reason_code, None);
    assert_eq!(snapshot.root_exit_code, Some(0));
    assert_eq!(snapshot.started_unix_millis, Some(primary_started));
    assert_eq!(
        snapshot.attempts[0].started_unix_millis,
        Some(primary_started)
    );
    assert_eq!(
        snapshot.attempts[0].verdict,
        Some(AttemptVerdict::PostconditionFailed)
    );
    assert_eq!(
        snapshot.attempts[0]
            .primary_result
            .as_ref()
            .and_then(|result| result.started_unix_millis),
        Some(primary_started)
    );
    assert_eq!(
        snapshot.attempts[0].invocations[1].started_unix_millis,
        None
    );
}

#[test]
fn post_commit_cancel_and_deadline_are_ordered_before_primary_resume() {
    let temp = tempfile::tempdir().unwrap();
    for cause in ["cancel", "deadline", "deadline_during_cleanup"] {
        let root = temp.path().join(cause);
        std::fs::create_dir_all(&root).unwrap();
        let ready = root.join("ready.flag");
        std::fs::write(&ready, b"ready").unwrap();
        let mut store = Store::open(StorePaths::new(root.join("store"))).unwrap();
        let mut job = spec(&root);
        job.conditions
            .push(none_deadline(ConditionPredicate::PathExists {
                path: ready,
            }));
        let receipt = submit_condition_job(&mut store, &job);
        let primary = prepare_until_ready(&mut store, receipt.receipt.job_id);
        store
            .connection
            .execute(
                "UPDATE containments SET state = 'live' WHERE id = ?1",
                [primary.containment_id.entity_uuid().to_string()],
            )
            .unwrap();
        assert!(matches!(
            store.authorize_condition_release(&primary, None).unwrap(),
            ReleaseAuthorization::Authorized { .. }
        ));

        if cause == "cancel" {
            store.cancel_jobs(&[receipt.receipt.job_id]).unwrap();
        } else {
            store
                .connection
                .execute(
                    "UPDATE conditions SET deadline_ms = ?2 WHERE job_id = ?1",
                    params![
                        receipt.receipt.job_id.entity_uuid().to_string(),
                        now_millis() - 1
                    ],
                )
                .unwrap();
        }
        let reason = if cause == "deadline_during_cleanup" {
            "Condition release evidence expired before resume".into()
        } else {
            store
                .pre_resume_defer_reason(receipt.receipt.job_id)
                .unwrap()
                .expect("a terminal intent that commits before resume must defer it")
        };
        assert_eq!(
            reason.starts_with("condition_deadline_expired:"),
            cause == "deadline"
        );
        store.replan_never_run(&primary, &reason).unwrap();

        let final_snapshot = store.status(receipt.receipt.job_id).unwrap();
        assert_eq!(final_snapshot.state, JobState::Final, "case {cause}");
        assert_eq!(
            final_snapshot.outcome,
            Some(if cause == "cancel" {
                JobOutcome::Canceled
            } else {
                JobOutcome::Failed
            }),
            "case {cause}"
        );
        if cause != "cancel" {
            assert_eq!(
                final_snapshot.reason_code.as_deref(),
                Some("condition_deadline_expired")
            );
            assert!(
                final_snapshot
                    .conditions
                    .iter()
                    .all(|condition| condition.state == ConditionState::Failed)
            );
        }
    }
}

#[test]
fn restart_preserves_the_durable_pre_resume_terminal_latch() {
    for (case, deadline_outcome, expected_outcome, expected_verdict) in [
        (
            "cancel",
            ConditionDeadlineOutcome::Failed,
            JobOutcome::Canceled,
            AttemptVerdict::Canceled,
        ),
        (
            "downtime_cancel",
            ConditionDeadlineOutcome::Failed,
            JobOutcome::Canceled,
            AttemptVerdict::Canceled,
        ),
        (
            "deadline_failed",
            ConditionDeadlineOutcome::Failed,
            JobOutcome::Failed,
            AttemptVerdict::SafetyFailed,
        ),
        (
            "deadline_canceled",
            ConditionDeadlineOutcome::Canceled,
            JobOutcome::Canceled,
            AttemptVerdict::Canceled,
        ),
        (
            "downtime_deadline_failed",
            ConditionDeadlineOutcome::Failed,
            JobOutcome::Failed,
            AttemptVerdict::SafetyFailed,
        ),
        (
            "downtime_deadline_canceled",
            ConditionDeadlineOutcome::Canceled,
            JobOutcome::Canceled,
            AttemptVerdict::Canceled,
        ),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let ready = temp.path().join("ready.flag");
        std::fs::write(&ready, b"ready").unwrap();
        let paths = StorePaths::new(temp.path().join("store"));
        let job_id = {
            let mut store = Store::open(paths.clone()).unwrap();
            let mut job = spec(temp.path());
            job.conditions.push(ConditionSpec {
                predicate: ConditionPredicate::PathExists { path: ready },
                deadline: ConditionDeadline::Relative { seconds: 60 },
                on_deadline: deadline_outcome,
            });
            let receipt = submit_condition_job(&mut store, &job).receipt;
            let primary = prepare_until_ready(&mut store, receipt.job_id);
            store
                .connection
                .execute(
                    "UPDATE containments SET state = 'live' WHERE id = ?1",
                    [primary.containment_id.entity_uuid().to_string()],
                )
                .unwrap();
            let downtime_expiry = case.starts_with("downtime_");
            if !downtime_expiry {
                assert!(matches!(
                    store.authorize_condition_release(&primary, None).unwrap(),
                    ReleaseAuthorization::Authorized { .. }
                ));
            }
            if matches!(case, "cancel" | "downtime_cancel") {
                store.cancel_jobs(&[receipt.job_id]).unwrap();
            } else {
                store
                    .connection
                    .execute(
                        "UPDATE conditions SET deadline_ms = ?2 WHERE job_id = ?1",
                        params![receipt.job_id.entity_uuid().to_string(), now_millis() - 1],
                    )
                    .unwrap();
            }
            if !downtime_expiry {
                assert!(
                    store
                        .pre_resume_defer_reason(receipt.job_id)
                        .unwrap()
                        .is_some()
                );
            }
            let latch: Option<String> = store
                .connection
                .query_row(
                    "SELECT pre_resume_defer_reason FROM admissions
                     WHERE attempt_id = ?1",
                    [primary.attempt_id.entity_uuid().to_string()],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(latch.is_some(), !downtime_expiry);
            receipt.job_id
        };

        let reopened = Store::open(paths).unwrap();
        let snapshot = reopened.status(job_id).unwrap();
        assert_eq!(snapshot.state, JobState::Final, "case {case}");
        assert_eq!(snapshot.outcome, Some(expected_outcome), "case {case}");
        assert_eq!(
            snapshot.attempts[0].verdict,
            Some(expected_verdict),
            "case {case}"
        );
        assert_eq!(snapshot.started_unix_millis, None, "case {case}");
        assert_eq!(
            snapshot.attempts[0].started_unix_millis, None,
            "case {case}"
        );
        assert_eq!(
            snapshot.attempts[0].invocations[0].started_unix_millis, None,
            "case {case}"
        );
        assert_eq!(
            snapshot.attempts[0].invocations[0].containment.state,
            ContainmentState::Uncertain,
            "case {case}"
        );
        let admission = snapshot.attempts[0]
            .admission
            .as_ref()
            .expect("Condition attempt has admission history");
        assert_eq!(
            admission.state,
            if expected_verdict == AttemptVerdict::SafetyFailed {
                AdmissionDecisionState::Failed
            } else {
                AdmissionDecisionState::Reserved
            },
            "case {case}"
        );
        assert_eq!(admission.final_sample, !case.starts_with("downtime_"));
        if matches!(case, "cancel" | "downtime_cancel") {
            assert_eq!(snapshot.reason_code, None);
        } else {
            assert_eq!(
                snapshot.reason_code.as_deref(),
                Some("condition_deadline_expired")
            );
            assert_eq!(snapshot.conditions[0].state, ConditionState::Failed);
        }
        let lease: String = reopened
            .connection
            .query_row(
                "SELECT leases.state FROM leases
                 JOIN attempts ON attempts.id = leases.attempt_id
                 WHERE attempts.job_id = ?1",
                [job_id.entity_uuid().to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(lease, "granted", "case {case}");
    }
}

#[test]
fn worker_start_failure_keeps_condition_deadline_precedence() {
    let temp = tempfile::tempdir().unwrap();
    let ready = temp.path().join("ready.flag");
    std::fs::write(&ready, b"ready").unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().join("store"))).unwrap();
    let mut job = spec(temp.path());
    job.conditions.push(ConditionSpec {
        predicate: ConditionPredicate::PathExists { path: ready },
        deadline: ConditionDeadline::Relative { seconds: 60 },
        on_deadline: ConditionDeadlineOutcome::Canceled,
    });
    let receipt = submit_condition_job(&mut store, &job).receipt;
    let primary = prepare_until_ready(&mut store, receipt.job_id);
    store
        .connection
        .execute(
            "UPDATE conditions SET deadline_ms = ?2 WHERE job_id = ?1",
            params![receipt.job_id.entity_uuid().to_string(), now_millis() - 1],
        )
        .unwrap();

    store
        .mark_finished(&primary, None, JobOutcome::Failed, "start_failed")
        .unwrap();

    let snapshot = store.status(receipt.job_id).unwrap();
    assert_eq!(snapshot.state, JobState::Final);
    assert_eq!(snapshot.outcome, Some(JobOutcome::Canceled));
    assert_eq!(
        snapshot.reason_code.as_deref(),
        Some("condition_deadline_expired")
    );
    assert_eq!(snapshot.attempts[0].verdict, Some(AttemptVerdict::Canceled));
    assert_eq!(snapshot.started_unix_millis, None);
}

#[test]
fn restart_invalidates_generation_local_path_evidence_and_rescans() {
    let temp = tempfile::tempdir().unwrap();
    let ready = temp.path().join("ready.flag");
    std::fs::write(&ready, b"ready").unwrap();
    let paths = StorePaths::new(temp.path().join("store"));
    let mut store = Store::open(paths.clone()).unwrap();
    let mut spec = spec(temp.path());
    spec.conditions
        .push(none_deadline(ConditionPredicate::PathExists {
            path: ready.clone(),
        }));
    let receipt = submit_condition_job(&mut store, &spec);
    let first_generation = receipt.receipt.conditions[0]
        .last_observation
        .as_ref()
        .unwrap()
        .daemon_generation;
    drop(store);
    std::fs::remove_file(&ready).unwrap();

    let mut reopened = Store::open(paths).unwrap();
    assert!(
        reopened
            .prepare_job(receipt.receipt.job_id)
            .unwrap()
            .is_none()
    );
    let snapshot = reopened.status(receipt.receipt.job_id).unwrap();
    let observation = snapshot.conditions[0].last_observation.as_ref().unwrap();
    assert_ne!(observation.daemon_generation, first_generation);
    assert!(matches!(
        observation.value,
        ConditionObservationValue::Path { exists: false }
    ));
    assert_eq!(snapshot.conditions[0].state, ConditionState::Waiting);
}

#[test]
fn restart_retains_only_the_unresolved_probe_claims_and_blocks_reprobe_until_clearance() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StorePaths::new(temp.path().join("store"));
    let mut config = HostConfig::default();
    config.resources.cargo_slots = 3;
    let (job_id, probe_invocation) = {
        let mut store =
            Store::open_with_config(paths.clone(), config.clone(), probe_startup_identity())
                .unwrap();
        let mut spec = spec(temp.path());
        spec.resources.cargo_slots = Some(3);
        spec.conditions
            .push(none_deadline(ConditionPredicate::Probe {
                probe: Box::new(ProbeCondition {
                    executable: PathBuf::from(r"C:\Windows\System32\cmd.exe"),
                    args: vec!["/d".into(), "/c".into(), "exit 0".into()],
                    working_directory: temp.path().to_path_buf(),
                    environment: EnvironmentSpec::default(),
                    resources: ResourceClaims {
                        cargo_slots: Some(1),
                        ..Default::default()
                    },
                    timeout_seconds: 5,
                    interval_seconds: 1,
                    accepted_exit_codes: vec![0],
                }),
            }));
        let receipt = submit_condition_job(&mut store, &spec);
        let probe = prepare_until_ready(&mut store, receipt.receipt.job_id);
        store.mark_started(&probe, 42, "probe-image").unwrap();
        (receipt.receipt.job_id, probe.invocation_id)
    };

    let mut reopened = Store::open_with_config(paths, config, probe_startup_identity()).unwrap();
    let snapshot = reopened.status(job_id).unwrap();
    assert_eq!(snapshot.state, JobState::Pending);
    let probe = snapshot.attempts[0]
        .invocations
        .iter()
        .find(|invocation| invocation.invocation_id == probe_invocation)
        .unwrap();
    assert_eq!(probe.containment.state, ContainmentState::Uncertain);
    assert_eq!(
        probe
            .containment
            .incident
            .as_ref()
            .unwrap()
            .retained_claims
            .cargo_slots,
        Some(1),
        "restart must not attribute the primary's three-slot claim to a one-slot probe"
    );
    assert!(reopened.prepare_job(job_id).unwrap().is_none());
    assert_eq!(
        reopened.status(job_id).unwrap().conditions[0].probe_invocation_id,
        Some(probe_invocation),
        "an unresolved probe must prevent a replacement probe"
    );
}

#[test]
fn restart_repairs_the_legacy_split_probe_settlement_window() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StorePaths::new(temp.path().join("store"));
    let job_id = {
        let mut store = Store::open(paths.clone()).unwrap();
        let mut job = spec(temp.path());
        job.conditions
            .push(none_deadline(ConditionPredicate::Probe {
                probe: Box::new(ProbeCondition {
                    executable: PathBuf::from(r"C:\Windows\System32\cmd.exe"),
                    args: vec!["/d".into(), "/c".into(), "exit 0".into()],
                    working_directory: temp.path().to_path_buf(),
                    environment: EnvironmentSpec::default(),
                    resources: ResourceClaims::default(),
                    timeout_seconds: 5,
                    interval_seconds: 1,
                    accepted_exit_codes: vec![0],
                }),
            }));
        let receipt = submit_condition_job(&mut store, &job);
        let probe = prepare_until_ready(&mut store, receipt.receipt.job_id);
        store.mark_started(&probe, 42, "probe-image").unwrap();
        store.mark_root_exited(&probe, 0).unwrap();
        // This recreates the pre-fix crash point: lifecycle resolution committed, while the
        // Condition pointer and probe Lease were not yet settled.
        store
            .mark_invocation_resolved(&probe, Some(0), None)
            .unwrap();
        let condition_key: String = store
            .connection
            .query_row(
                "SELECT id FROM conditions WHERE job_id = ?1",
                [receipt.receipt.job_id.entity_uuid().to_string()],
                |row| row.get(0),
            )
            .unwrap();
        for index in 0..MAX_RETAINED_OBSERVATIONS_PER_CONDITION {
            store
                .connection
                .execute(
                    "INSERT INTO observations(
                         id, condition_id, observed_ms, observed_monotonic_ms,
                         daemon_generation, fresh_until_ms, source, value_json
                     ) VALUES (?1, ?2, ?3, 0, ?4, ?3, 'invalidation', ?5)",
                    params![
                        Uuid::now_v7().to_string(),
                        &condition_key,
                        index as i64,
                        store.daemon_generation.to_string(),
                        serde_json::to_string(&ConditionObservationValue::Invalidated {
                            reason: format!("legacy-{index}")
                        })
                        .unwrap()
                    ],
                )
                .unwrap();
        }
        receipt.receipt.job_id
    };

    let mut reopened = Store::open(paths).unwrap();
    let snapshot = reopened.status(job_id).unwrap();
    assert_eq!(snapshot.conditions[0].state, ConditionState::Waiting);
    assert_eq!(snapshot.conditions[0].probe_invocation_id, None);
    let granted: bool = reopened
        .connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM leases WHERE state = 'granted')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(!granted);
    let observations: u64 = reopened
        .connection
        .query_row(
            "SELECT COUNT(*) FROM observations
             WHERE condition_id IN (SELECT id FROM conditions WHERE job_id = ?1)",
            [job_id.entity_uuid().to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(observations, MAX_RETAINED_OBSERVATIONS_PER_CONDITION as u64);
    reopened
        .connection
        .execute(
            "UPDATE conditions SET next_probe_ms = 0 WHERE job_id = ?1",
            [job_id.entity_uuid().to_string()],
        )
        .unwrap();
    assert_eq!(
        prepare_until_ready(&mut reopened, job_id).role,
        InvocationRole::Probe,
        "legacy split settlement lacks durable timeout evidence and must fail closed"
    );
}

#[test]
fn cancel_waits_for_live_probe_boundary_before_publishing_final() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().join("store"))).unwrap();
    let mut job = spec(temp.path());
    job.conditions
        .push(none_deadline(ConditionPredicate::Probe {
            probe: Box::new(ProbeCondition {
                executable: PathBuf::from(r"C:\Windows\System32\cmd.exe"),
                args: vec!["/d".into(), "/c".into(), "exit 0".into()],
                working_directory: temp.path().to_path_buf(),
                environment: EnvironmentSpec::default(),
                resources: ResourceClaims::default(),
                timeout_seconds: 5,
                interval_seconds: 1,
                accepted_exit_codes: vec![0],
            }),
        }));
    let receipt = submit_condition_job(&mut store, &job);
    let probe = prepare_until_ready(&mut store, receipt.receipt.job_id);
    store.mark_started(&probe, 42, "probe-image").unwrap();

    let canceled = store.cancel_jobs(&[receipt.receipt.job_id]).unwrap();
    assert_eq!(canceled[0].state, JobState::Pending);
    assert!(canceled[0].cancel_requested);
    assert_eq!(
        canceled[0].attempts[0].invocations[0].state,
        InvocationState::Started
    );

    store.mark_root_exited(&probe, 0).unwrap();
    store.settle_probe(&probe, Some(0), false).unwrap();
    let final_snapshot = store.status(receipt.receipt.job_id).unwrap();
    assert_eq!(final_snapshot.state, JobState::Final);
    assert_eq!(final_snapshot.outcome, Some(JobOutcome::Canceled));
    assert_eq!(
        final_snapshot.attempts[0].invocations[0].containment.state,
        ContainmentState::Empty
    );
}

#[test]
fn terminal_intent_waits_for_every_live_probe_and_cancel_never_starts_another() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().join("store"))).unwrap();
    let mut job = spec(temp.path());
    job.conditions = vec![
        probe_condition(temp.path()),
        probe_condition(temp.path()),
        probe_condition(temp.path()),
    ];
    let receipt = submit_condition_job(&mut store, &job);
    let first = prepare_until_ready(&mut store, receipt.receipt.job_id);
    store.mark_started(&first, 41, "probe-one").unwrap();
    let second = prepare_until_ready(&mut store, receipt.receipt.job_id);
    store.mark_started(&second, 42, "probe-two").unwrap();

    let canceled = store.cancel_jobs(&[receipt.receipt.job_id]).unwrap();
    assert_eq!(canceled[0].state, JobState::Pending);
    assert!(store.prepare_job(receipt.receipt.job_id).unwrap().is_none());
    let invocation_count: u64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM invocations", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        invocation_count, 2,
        "cancel must not prepare the third probe"
    );

    store.mark_root_exited(&first, 0).unwrap();
    store.settle_probe(&first, Some(0), false).unwrap();
    let pending = store.status(receipt.receipt.job_id).unwrap();
    assert_eq!(pending.state, JobState::Pending);
    assert_eq!(
        pending
            .attempts
            .iter()
            .flat_map(|attempt| &attempt.invocations)
            .find(|invocation| invocation.invocation_id == second.invocation_id)
            .unwrap()
            .containment
            .state,
        ContainmentState::Live
    );

    store.mark_root_exited(&second, 0).unwrap();
    store.settle_probe(&second, Some(0), false).unwrap();
    let final_snapshot = store.status(receipt.receipt.job_id).unwrap();
    assert_eq!(final_snapshot.state, JobState::Final);
    assert_eq!(final_snapshot.outcome, Some(JobOutcome::Canceled));
}

#[test]
fn failed_deadline_latch_survives_refresh_until_probe_cleanup() {
    let temp = tempfile::tempdir().unwrap();
    let ready = temp.path().join("ready.flag");
    std::fs::write(&ready, b"ready").unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().join("store"))).unwrap();
    let mut job = spec(temp.path());
    job.conditions
        .push(none_deadline(ConditionPredicate::PathExists {
            path: ready,
        }));
    job.conditions.push(probe_condition(temp.path()));
    let receipt = submit_condition_job(&mut store, &job);
    let probe = prepare_until_ready(&mut store, receipt.receipt.job_id);
    store.mark_started(&probe, 43, "deadline-probe").unwrap();
    store
        .connection
        .execute(
            "UPDATE conditions SET deadline_ms = ?2
             WHERE job_id = ?1 AND condition_index = 0",
            params![
                receipt.receipt.job_id.entity_uuid().to_string(),
                now_millis() - 1
            ],
        )
        .unwrap();
    assert!(
        store
            .expire_job_condition_deadline(receipt.receipt.job_id, now_millis())
            .unwrap()
    );
    store
        .connection
        .execute(
            "UPDATE conditions SET deadline_ms = ?2
             WHERE job_id = ?1 AND condition_index = 0",
            params![
                receipt.receipt.job_id.entity_uuid().to_string(),
                now_millis() + 60_000
            ],
        )
        .unwrap();
    assert!(
        store
            .invocation_stop_requested(receipt.receipt.job_id)
            .unwrap(),
        "the failed deadline latch must survive a backward wall-clock discontinuity"
    );
    assert!(store.prepare_job(receipt.receipt.job_id).unwrap().is_none());
    let pending = store.status(receipt.receipt.job_id).unwrap();
    assert_eq!(pending.state, JobState::Pending);
    assert!(
        pending
            .conditions
            .iter()
            .all(|condition| condition.state == ConditionState::Failed)
    );

    store.mark_root_exited(&probe, 0).unwrap();
    store.settle_probe(&probe, Some(0), false).unwrap();
    let final_snapshot = store.status(receipt.receipt.job_id).unwrap();
    assert_eq!(final_snapshot.state, JobState::Final);
    assert_eq!(
        final_snapshot.reason_code.as_deref(),
        Some("condition_deadline_expired")
    );
}

#[test]
fn due_deadline_wins_over_cancel_before_and_during_probe_cleanup() {
    let temp = tempfile::tempdir().unwrap();

    let mut direct_store = Store::open(StorePaths::new(temp.path().join("direct"))).unwrap();
    let mut direct_job = spec(temp.path());
    direct_job
        .conditions
        .push(none_deadline(ConditionPredicate::PathExists {
            path: temp.path().join("not-ready.flag"),
        }));
    let direct = submit_condition_job(&mut direct_store, &direct_job);
    direct_store
        .connection
        .execute(
            "UPDATE conditions SET deadline_ms = ?2 WHERE job_id = ?1",
            params![
                direct.receipt.job_id.entity_uuid().to_string(),
                now_millis() - 1
            ],
        )
        .unwrap();
    let direct_snapshot = direct_store.cancel_jobs(&[direct.receipt.job_id]).unwrap();
    assert_eq!(direct_snapshot[0].state, JobState::Final);
    assert_eq!(direct_snapshot[0].outcome, Some(JobOutcome::Failed));
    assert_eq!(
        direct_snapshot[0].reason_code.as_deref(),
        Some("condition_deadline_expired")
    );

    let mut probe_store = Store::open(StorePaths::new(temp.path().join("probe"))).unwrap();
    let mut probe_job = spec(temp.path());
    probe_job.conditions.push(probe_condition(temp.path()));
    let probe_receipt = submit_condition_job(&mut probe_store, &probe_job);
    let probe = prepare_until_ready(&mut probe_store, probe_receipt.receipt.job_id);
    probe_store
        .mark_started(&probe, 45, "deadline-vs-cancel-probe")
        .unwrap();
    probe_store
        .connection
        .execute(
            "UPDATE conditions SET deadline_ms = ?2 WHERE job_id = ?1",
            params![
                probe_receipt.receipt.job_id.entity_uuid().to_string(),
                now_millis() - 1
            ],
        )
        .unwrap();
    assert_eq!(
        probe_store
            .cancel_jobs(&[probe_receipt.receipt.job_id])
            .unwrap()[0]
            .state,
        JobState::Pending
    );
    probe_store.mark_root_exited(&probe, 0).unwrap();
    probe_store.settle_probe(&probe, Some(0), false).unwrap();
    let probe_snapshot = probe_store.status(probe_receipt.receipt.job_id).unwrap();
    assert_eq!(probe_snapshot.state, JobState::Final);
    assert_eq!(probe_snapshot.outcome, Some(JobOutcome::Failed));
    assert_eq!(
        probe_snapshot.reason_code.as_deref(),
        Some("condition_deadline_expired")
    );
}

#[test]
fn restart_finalizes_cancel_and_deadline_after_probe_becomes_uncertain() {
    let temp = tempfile::tempdir().unwrap();
    for case in ["cancel", "deadline_latched", "deadline_during_downtime"] {
        let cancel = case == "cancel";
        let paths = StorePaths::new(temp.path().join(case));
        let job_id = {
            let mut store = Store::open(paths.clone()).unwrap();
            let mut job = spec(temp.path());
            job.conditions.push(probe_condition(temp.path()));
            let receipt = submit_condition_job(&mut store, &job);
            let probe = prepare_until_ready(&mut store, receipt.receipt.job_id);
            store.mark_started(&probe, 44, "restart-probe").unwrap();
            if cancel {
                assert_eq!(
                    store.cancel_jobs(&[receipt.receipt.job_id]).unwrap()[0].state,
                    JobState::Pending
                );
            } else if case == "deadline_latched" {
                store
                    .connection
                    .execute(
                        "UPDATE conditions SET deadline_ms = ?2 WHERE job_id = ?1",
                        params![
                            receipt.receipt.job_id.entity_uuid().to_string(),
                            now_millis() - 1
                        ],
                    )
                    .unwrap();
                assert!(
                    store
                        .expire_job_condition_deadline(receipt.receipt.job_id, now_millis())
                        .unwrap()
                );
                store
                    .connection
                    .execute(
                        "UPDATE conditions SET deadline_ms = ?2 WHERE job_id = ?1",
                        params![
                            receipt.receipt.job_id.entity_uuid().to_string(),
                            now_millis() + 60_000
                        ],
                    )
                    .unwrap();
            } else {
                store
                    .connection
                    .execute(
                        "UPDATE conditions SET deadline_ms = ?2 WHERE job_id = ?1",
                        params![
                            receipt.receipt.job_id.entity_uuid().to_string(),
                            now_millis() + 25
                        ],
                    )
                    .unwrap();
            }
            receipt.receipt.job_id
        };

        if case == "deadline_during_downtime" {
            std::thread::sleep(Duration::from_millis(50));
        }

        let reopened = Store::open(paths).unwrap();
        let snapshot = reopened.status(job_id).unwrap();
        assert_eq!(snapshot.state, JobState::Final, "restart case {case}");
        assert_eq!(
            snapshot.outcome,
            Some(if cancel {
                JobOutcome::Canceled
            } else {
                JobOutcome::Failed
            })
        );
        assert_eq!(
            snapshot.attempts[0].invocations[0].containment.state,
            ContainmentState::Uncertain
        );
        let granted: bool = reopened
            .connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM leases WHERE state = 'granted')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(granted, "uncertain containment must retain its Lease");
    }
}

#[test]
fn deadline_is_enforced_before_retry_backoff_and_dependency_filters() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().join("store"))).unwrap();
    let mut backoff = spec(temp.path());
    backoff
        .conditions
        .push(none_deadline(ConditionPredicate::PathExists {
            path: temp.path().join("backoff-ready"),
        }));
    let backoff = submit_condition_job(&mut store, &backoff);
    store
        .connection
        .execute(
            "UPDATE jobs SET retry_not_before_ms = ?2 WHERE id = ?1",
            params![
                backoff.receipt.job_id.entity_uuid().to_string(),
                now_millis() + 60_000
            ],
        )
        .unwrap();
    store
        .connection
        .execute(
            "UPDATE conditions SET deadline_ms = ?2 WHERE job_id = ?1",
            params![
                backoff.receipt.job_id.entity_uuid().to_string(),
                now_millis() - 1
            ],
        )
        .unwrap();

    let mut predecessor = spec(temp.path());
    predecessor
        .conditions
        .push(none_deadline(ConditionPredicate::PathExists {
            path: temp.path().join("predecessor-ready"),
        }));
    let mut successor = spec(temp.path());
    successor
        .conditions
        .push(none_deadline(ConditionPredicate::PathExists {
            path: temp.path().join("successor-ready"),
        }));
    let batch = BatchSpec {
        spec_version: SPEC_VERSION,
        jobs: vec![
            member("predecessor", predecessor, vec![]),
            member(
                "successor",
                successor,
                vec![DependencySpec {
                    job: "predecessor".into(),
                    on: DependencyKind::Success,
                }],
            ),
        ],
    };
    let hash = normalized_batch_payload_hash(&batch).unwrap();
    let receipt = store
        .submit_batch(Uuid::now_v7(), &hash, &batch)
        .unwrap()
        .receipt;
    let successor_id = receipt.jobs[1].receipt.job_id;
    store
        .connection
        .execute(
            "UPDATE conditions SET deadline_ms = ?2 WHERE job_id = ?1",
            params![successor_id.entity_uuid().to_string(), now_millis() - 1],
        )
        .unwrap();

    let progress = store.prepare_next_job_with_progress().unwrap();
    assert!(progress.state_changed);
    for job_id in [backoff.receipt.job_id, successor_id] {
        let snapshot = store.status(job_id).unwrap();
        assert_eq!(snapshot.state, JobState::Final);
        assert_eq!(
            snapshot.reason_code.as_deref(),
            Some("condition_deadline_expired")
        );
    }
}

#[test]
fn expired_monotonic_evidence_does_not_create_a_zero_delay_retry_spin() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().join("store"))).unwrap();
    let mut job = spec(temp.path());
    job.conditions
        .push(none_deadline(ConditionPredicate::PathExists {
            path: temp.path().join("blocked.flag"),
        }));
    let receipt = submit_condition_job(&mut store, &job);
    let scheduling_pass_started = now_millis();
    store
        .connection
        .execute(
            "UPDATE jobs SET retry_not_before_ms = ?2 WHERE id = ?1",
            params![
                receipt.receipt.job_id.entity_uuid().to_string(),
                scheduling_pass_started + 60_000
            ],
        )
        .unwrap();
    store
        .connection
        .execute(
            "UPDATE observations SET fresh_until_ms = ?2, observed_monotonic_ms = 0
             WHERE condition_id IN (SELECT id FROM conditions WHERE job_id = ?1)",
            params![receipt.receipt.job_id.entity_uuid().to_string(), i64::MAX],
        )
        .unwrap();

    let delay = store
        .next_retry_delay(scheduling_pass_started)
        .unwrap()
        .expect("retry backoff supplies a future wake");
    assert!(
        delay >= Duration::from_secs(30),
        "expired monotonic evidence must not dominate the future retry with zero delay: {delay:?}"
    );
}

#[test]
fn freshness_that_expires_during_a_long_pass_yields_before_rescan() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().join("store"))).unwrap();
    let mut job = spec(temp.path());
    job.conditions
        .push(none_deadline(ConditionPredicate::PathExists {
            path: temp.path().join("blocked.flag"),
        }));
    let receipt = submit_condition_job(&mut store, &job);
    let pass_started = now_millis().saturating_sub(1_000);
    store
        .connection
        .execute(
            "UPDATE observations SET fresh_until_ms = ?2, observed_monotonic_ms = 0
             WHERE condition_id IN (SELECT id FROM conditions WHERE job_id = ?1)",
            params![
                receipt.receipt.job_id.entity_uuid().to_string(),
                pass_started + 1
            ],
        )
        .unwrap();

    let delay = store
        .next_retry_delay(pass_started)
        .unwrap()
        .expect("expired freshness schedules a bounded follow-up rescan");
    assert_eq!(delay, Duration::from_millis(25));
}

#[test]
fn monotonic_expiry_yields_despite_a_future_wall_deadline() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().join("store"))).unwrap();
    let mut job = spec(temp.path());
    job.conditions
        .push(none_deadline(ConditionPredicate::PathExists {
            path: temp.path().join("blocked.flag"),
        }));
    let receipt = submit_condition_job(&mut store, &job);
    let pass_started = now_millis();
    store
        .connection
        .execute(
            "UPDATE observations SET fresh_until_ms = ?2, observed_monotonic_ms = 0
             WHERE condition_id IN (SELECT id FROM conditions WHERE job_id = ?1)",
            params![
                receipt.receipt.job_id.entity_uuid().to_string(),
                pass_started + 60_000
            ],
        )
        .unwrap();

    assert_eq!(
        store.next_retry_delay(pass_started).unwrap(),
        Some(Duration::from_millis(25)),
        "monotonic expiry must fail closed even when wall time rolled back"
    );
}

#[test]
fn already_stale_refresh_blocks_once_then_yields_instead_of_replanning() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = HostConfig::default();
    config.observation.condition_rescan_interval_millis = 100;
    let mut store = Store::open_with_config(
        StorePaths::new(temp.path().join("store")),
        config,
        probe_startup_identity(),
    )
    .unwrap();
    let mut job = spec(temp.path());
    job.resources.ram_mb = Some(1);
    job.conditions
        .push(none_deadline(ConditionPredicate::PathExists {
            path: temp.path().join("stillyard-test-slow-condition"),
        }));
    let receipt = submit_condition_job(&mut store, &job);
    let pass_started = now_millis().saturating_sub(1);

    assert!(matches!(
        store
            .prepare_job_inner(receipt.receipt.job_id, None)
            .unwrap(),
        PrepareJob::Blocked
    ));
    assert_eq!(
        store.next_retry_delay(pass_started).unwrap(),
        Some(Duration::from_millis(25))
    );
}

#[test]
fn deadline_crossing_during_scan_wins_over_impossible_dependency() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().join("store"))).unwrap();
    let predecessor_spec = spec(temp.path());
    let predecessor = submit_condition_job(&mut store, &predecessor_spec).receipt;
    store
        .connection
        .execute(
            "UPDATE jobs SET state = 'final', outcome = 'failed', finished_ms = ?2
             WHERE id = ?1",
            params![predecessor.job_id.entity_uuid().to_string(), now_millis()],
        )
        .unwrap();
    let mut successor_spec = spec(temp.path());
    successor_spec.conditions.push(ConditionSpec {
        predicate: ConditionPredicate::PathAbsent {
            path: temp.path().join("stillyard-test-slow-condition"),
        },
        deadline: ConditionDeadline::Relative { seconds: 60 },
        on_deadline: ConditionDeadlineOutcome::Canceled,
    });
    let successor = submit_condition_job(&mut store, &successor_spec).receipt;
    store
        .connection
        .execute(
            "INSERT INTO dependencies(predecessor_id, successor_id, kind)
             VALUES (?1, ?2, 'success')",
            params![
                predecessor.job_id.entity_uuid().to_string(),
                successor.job_id.entity_uuid().to_string(),
            ],
        )
        .unwrap();
    store
        .connection
        .execute(
            "UPDATE conditions SET deadline_ms = ?2 WHERE job_id = ?1",
            params![
                successor.job_id.entity_uuid().to_string(),
                now_millis() + 50,
            ],
        )
        .unwrap();

    assert!(matches!(
        store.prepare_job_inner(successor.job_id, None).unwrap(),
        PrepareJob::StateChanged
    ));
    let snapshot = store.status(successor.job_id).unwrap();
    assert_eq!(snapshot.state, JobState::Final);
    assert_eq!(snapshot.outcome, Some(JobOutcome::Canceled));
    assert_eq!(
        snapshot.reason_code.as_deref(),
        Some("condition_deadline_expired")
    );
}

#[test]
fn prepared_probe_start_failure_atomically_releases_its_lease_and_requeues() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().join("store"))).unwrap();
    let mut job = spec(temp.path());
    job.conditions
        .push(none_deadline(ConditionPredicate::Probe {
            probe: Box::new(ProbeCondition {
                executable: PathBuf::from(r"C:\Windows\System32\cmd.exe"),
                args: vec!["/d".into(), "/c".into(), "exit 0".into()],
                working_directory: temp.path().to_path_buf(),
                environment: EnvironmentSpec::default(),
                resources: ResourceClaims::default(),
                timeout_seconds: 5,
                interval_seconds: 1,
                accepted_exit_codes: vec![0],
            }),
        }));
    let receipt = submit_condition_job(&mut store, &job);
    let probe = prepare_until_ready(&mut store, receipt.receipt.job_id);
    store.settle_probe(&probe, None, false).unwrap();

    let snapshot = store.status(receipt.receipt.job_id).unwrap();
    assert_eq!(snapshot.state, JobState::Pending);
    assert_eq!(snapshot.conditions[0].state, ConditionState::Waiting);
    assert_eq!(snapshot.conditions[0].probe_invocation_id, None);
    let (next_probe, granted): (Option<i64>, bool) = store
        .connection
        .query_row(
            "SELECT conditions.next_probe_ms,
                    EXISTS(SELECT 1 FROM leases WHERE state = 'granted')
             FROM conditions WHERE conditions.job_id = ?1",
            [receipt.receipt.job_id.entity_uuid().to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert!(next_probe.is_some());
    assert!(!granted);
}

#[test]
fn monotonic_expiry_forces_rescan_despite_a_future_wall_freshness_and_history_is_bounded() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().join("store"))).unwrap();
    let mut job = spec(temp.path());
    job.conditions
        .push(none_deadline(ConditionPredicate::PathExists {
            path: temp.path().join("still-missing"),
        }));
    let receipt = submit_condition_job(&mut store, &job);
    let job_key = receipt.receipt.job_id.entity_uuid().to_string();
    let initial_observation = receipt.receipt.conditions[0]
        .last_observation
        .as_ref()
        .unwrap()
        .observation_id;
    store
        .connection
        .execute(
            "UPDATE observations SET fresh_until_ms = ?2, observed_monotonic_ms = 0
             WHERE condition_id IN (SELECT id FROM conditions WHERE job_id = ?1)",
            params![job_key, i64::MAX],
        )
        .unwrap();
    assert!(store.prepare_job(receipt.receipt.job_id).unwrap().is_none());
    let rescanned = store.status(receipt.receipt.job_id).unwrap();
    assert_ne!(
        rescanned.conditions[0]
            .last_observation
            .as_ref()
            .unwrap()
            .observation_id,
        initial_observation
    );

    let baseline_events: u64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .unwrap();
    for _ in 0..12 {
        expire_condition_evidence(&store, receipt.receipt.job_id);
        assert!(store.prepare_job(receipt.receipt.job_id).unwrap().is_none());
    }
    let observations: u64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM observations
             WHERE condition_id IN (SELECT id FROM conditions WHERE job_id = ?1)",
            [&job_key],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(observations, 8);
    let events: u64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        events, baseline_events,
        "freshness-only rescans must not storm events"
    );
}

#[test]
fn probe_history_is_pruned_after_its_pinning_events_leave_the_ring() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().join("store"))).unwrap();
    let mut job = spec(temp.path());
    job.conditions.push(probe_condition(temp.path()));
    let receipt = submit_condition_job(&mut store, &job);
    let job_key = receipt.receipt.job_id.entity_uuid().to_string();
    let mut oldest_logs = None;
    for index in 0..70_u32 {
        store
            .connection
            .execute(
                "UPDATE conditions SET next_probe_ms = 0 WHERE job_id = ?1",
                [&job_key],
            )
            .unwrap();
        let probe = prepare_until_ready(&mut store, receipt.receipt.job_id);
        if index == 0 {
            std::fs::create_dir(&probe.stdout_path).unwrap();
            std::fs::write(&probe.stderr_path, b"old stderr").unwrap();
            oldest_logs = Some((probe.stdout_path.clone(), probe.stderr_path.clone()));
        }
        store
            .mark_started(&probe, 1_000 + index, "history-probe")
            .unwrap();
        store.mark_root_exited(&probe, 1).unwrap();
        store.settle_probe(&probe, Some(1), false).unwrap();
    }
    let before: u64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM invocations WHERE condition_id IS NOT NULL",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(
        before > MAX_RETAINED_PROBE_INVOCATIONS_PER_CONDITION as u64,
        "live event references should temporarily pin probe history"
    );
    store
        .connection
        .execute(
            "UPDATE conditions SET state = 'satisfied', next_probe_ms = NULL
             WHERE job_id = ?1",
            [&job_key],
        )
        .unwrap();
    let transaction = store.connection.transaction().unwrap();
    for offset in 0..=(MAX_EVENT_ROWS + 8) {
        transaction
            .execute(
                "INSERT INTO events(kind, job_id, committed_ms)
                 VALUES ('job_changed', ?1, ?2)",
                params![&job_key, now_millis().saturating_add(offset as i64)],
            )
            .unwrap();
    }
    transaction.commit().unwrap();
    store.prune_condition_history().unwrap();
    let after: u64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM invocations WHERE condition_id IS NOT NULL",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        after, MAX_RETAINED_PROBE_INVOCATIONS_PER_CONDITION as u64,
        "history must converge to its bound after old event references disappear"
    );
    let retained_gc: (u64, u64) = store
        .connection
        .query_row(
            "SELECT COUNT(*), MAX(attempt_count) FROM probe_log_gc",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        retained_gc,
        (1, 1),
        "failed log deletion must remain retryable without starving newer tombstones"
    );
    let (oldest_stdout, oldest_stderr) = oldest_logs.unwrap();
    assert!(
        oldest_stdout.is_dir(),
        "the deliberately undeletable log remains until the next GC pass"
    );
    assert!(!oldest_stderr.exists(), "pruning must remove probe stderr");
    std::fs::remove_dir(&oldest_stdout).unwrap();
    store.prune_condition_history().unwrap();
    let retained_gc: u64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM probe_log_gc", [], |row| row.get(0))
        .unwrap();
    assert_eq!(retained_gc, 0, "successful retry must clear its tombstone");
    assert!(!oldest_stdout.exists());
}
