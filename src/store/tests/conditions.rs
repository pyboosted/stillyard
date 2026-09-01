use super::*;
use crate::{ConditionSpec, PathConditionState, ProbeCondition};

fn none_deadline(predicate: ConditionPredicate) -> ConditionSpec {
    ConditionSpec {
        predicate,
        deadline: ConditionDeadline::None,
        on_deadline: ConditionDeadlineOutcome::Failed,
    }
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
        store.authorize_condition_release(&prepared).unwrap(),
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
    store
        .mark_invocation_resolved(&probe, Some(0), None)
        .unwrap();
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
    store.mark_root_exited(&probe, 9).unwrap();
    store
        .mark_invocation_resolved(&probe, Some(9), None)
        .unwrap();
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
    let authorization = store.authorize_condition_release(&primary).unwrap();
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
