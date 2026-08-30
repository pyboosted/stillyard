use super::*;

fn observation_scheduler() -> Arc<DaemonReactor> {
    let store_uuid = uuid::Uuid::now_v7();
    let daemon_generation = uuid::Uuid::now_v7();
    Arc::new(DaemonReactor {
        signal: Arc::new((Mutex::new(false), Condvar::new())),
        events: Arc::new((Mutex::new(0), Condvar::new())),
        endpoint: Arc::from(r"\\.\pipe\stillyard-daemon-test"),
        live_containments: crate::runner::LiveContainments::default(),
        reconciliation_observations: Mutex::new(Default::default()),
        doctor_snapshots: Mutex::new(crate::store::DoctorSnapshotCache::new(
            store_uuid,
            daemon_generation,
        )),
        host_observation: Arc::new(crate::host_observation::HostObservationService::new(
            Default::default(),
        )),
    })
}

#[test]
fn endpoint_lease_is_exclusive_and_released_with_its_handle() {
    let endpoint = format!(r"\\.\pipe\stillyard-lease-test-{}", uuid::Uuid::now_v7());
    let first = acquire_endpoint_lease(&endpoint).unwrap();
    assert!(matches!(
        acquire_endpoint_lease(&endpoint),
        Err(Error::Unavailable(_))
    ));
    drop(first);
    acquire_endpoint_lease(&endpoint).unwrap();
}

#[test]
fn explicit_instance_tuple_accepts_only_both_coordinates_or_neither() {
    for store_cli in [false, true] {
        for endpoint_cli in [false, true] {
            for store_env in [false, true] {
                for endpoint_env in [false, true] {
                    let store_selected = store_cli || store_env;
                    let endpoint_selected = endpoint_cli || endpoint_env;
                    let result = validate_instance_tuple(store_selected, endpoint_selected);
                    assert_eq!(
                        result.is_ok(),
                        store_selected == endpoint_selected,
                        "store_cli={store_cli}, endpoint_cli={endpoint_cli}, store_env={store_env}, endpoint_env={endpoint_env}"
                    );
                }
            }
        }
    }
}

#[test]
fn first_pipe_instance_is_exclusive_and_released_with_its_handle() {
    let endpoint = format!(r"\\.\pipe\stillyard-pipe-test-{}", uuid::Uuid::now_v7());
    let first = create_pipe_instance(&endpoint, true).unwrap();
    assert!(matches!(
        create_pipe_instance(&endpoint, true),
        Err(Error::Unavailable(_))
    ));
    drop(first);
    create_pipe_instance(&endpoint, true).unwrap();
}

fn candidate(store: uuid::Uuid, _enabled: bool) -> ManagedCandidate {
    ManagedCandidate {
        parent: crate::ManagedParent {
            job_id: crate::JobId::from_parts(store, uuid::Uuid::now_v7()),
            attempt_id: crate::AttemptId::from_parts(store, uuid::Uuid::now_v7()),
            invocation_id: crate::InvocationId::from_parts(store, uuid::Uuid::now_v7()),
        },
        parent_job_id: None,
        current: true,
    }
}

#[test]
fn peer_membership_derives_one_enabled_parent_and_rejects_ambiguity() {
    let store = uuid::Uuid::now_v7();
    let first = candidate(store, true);
    let second = candidate(store, true);
    assert_eq!(
        resolve_managed_membership(&[first], |id| {
            Ok(Some(id == first.parent.invocation_id))
        })
        .unwrap(),
        Some(first.parent)
    );
    assert!(matches!(
        resolve_managed_membership(&[first, second], |_| Ok(Some(true))),
        Err(StoreError::Rejected(_))
    ));
}

#[test]
fn nested_membership_selects_the_unique_immediate_containment() {
    let store = uuid::Uuid::now_v7();
    let outer = candidate(store, true);
    let mut inner = candidate(store, true);
    inner.parent_job_id = Some(outer.parent.job_id);
    assert_eq!(
        resolve_managed_membership(&[outer, inner], |_| Ok(Some(true))).unwrap(),
        Some(inner.parent)
    );

    assert_eq!(
        resolve_managed_membership(&[outer, inner], |_| Ok(Some(true))).unwrap(),
        Some(inner.parent)
    );
}

#[test]
fn peer_inside_disabled_primary_is_authenticated_not_downgraded_to_unmanaged() {
    let candidate = candidate(uuid::Uuid::now_v7(), false);
    assert_eq!(
        resolve_managed_membership(&[candidate], |_| Ok(Some(true))).unwrap(),
        Some(candidate.parent)
    );
}

#[test]
fn peer_inside_root_exited_or_uncertain_containment_is_rejected() {
    let mut candidate = candidate(uuid::Uuid::now_v7(), true);
    candidate.current = false;
    assert!(matches!(
        resolve_managed_membership(&[candidate], |_| Ok(Some(true))),
        Err(StoreError::Rejected(_))
    ));
}

#[test]
fn missing_handle_never_downgrades_a_possible_managed_peer_to_unmanaged() {
    let mut candidate = candidate(uuid::Uuid::now_v7(), true);
    candidate.current = false;
    assert!(matches!(
        resolve_managed_membership(&[candidate], |_| Ok(None)),
        Err(StoreError::InvalidState(_))
    ));
}

#[test]
fn singleton_lock_is_acquired_before_destructive_store_open() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StorePaths::new(temp.path().to_path_buf());
    paths.ensure().unwrap();
    let connection = rusqlite::Connection::open(&paths.database).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
                 INSERT INTO meta(key, value) VALUES ('schema_epoch', 'obsolete-schema');",
        )
        .unwrap();
    drop(connection);

    let held_lock = open_lock(&paths.lock).unwrap();
    held_lock.try_lock_exclusive().unwrap();
    assert!(matches!(
        open_store_under_lock(StorePaths::new(temp.path().to_path_buf())),
        Err(Error::Unavailable(message)) if message.contains("daemon already running")
    ));

    let connection = rusqlite::Connection::open(&paths.database).unwrap();
    let epoch: String = connection
        .query_row(
            "SELECT value FROM meta WHERE key = 'schema_epoch'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(epoch, "obsolete-schema");
}

#[test]
fn commit_at_wait_boundary_wakes_from_durable_event() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StorePaths::new(temp.path().to_path_buf());
    let mut store = Store::open_with_capacities(
        paths,
        crate::ResourceCapacities {
            cpu_units: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let spec = crate::JobSpec {
        spec_version: crate::SPEC_VERSION,
        executable: temp.path().join("tool.exe"),
        args: Vec::new(),
        working_directory: temp.path().to_path_buf(),
        stdin: crate::StdinSpec::Eof,
        environment: Default::default(),
        resources: Default::default(),
        observed: None,
        conditions: Vec::new(),
        retry: Default::default(),
        postconditions: Vec::new(),
        labels: Vec::new(),
        expected_duration_seconds: None,
        timeout_seconds: None,
        quiet: None,
        artifacts: Vec::new(),
        child_submission_policy: None,
    };
    let hash = crate::store::normalized_payload_hash(&spec).unwrap();
    let receipt = store
        .submit(uuid::Uuid::now_v7(), &hash, &spec)
        .unwrap()
        .receipt;
    let cursor = store
        .list_jobs(&crate::JobSelector::All, None, 1)
        .unwrap()
        .event_cursor;
    let scheduler = observation_scheduler();
    let notifier = Arc::downgrade(&scheduler);
    store.set_change_notifier(Arc::new(move || {
        if let Some(notifier) = notifier.upgrade() {
            notifier.notify_change();
        }
    }));
    let store = Arc::new(Mutex::new(store));
    let waiting_store = Arc::clone(&store);
    let waiting_scheduler = Arc::clone(&scheduler);
    let waiter = std::thread::spawn(move || {
        waiting_scheduler.wait_observation(
            &waiting_store,
            &crate::JobSelector::All,
            Some(cursor),
            16,
            Duration::from_secs(2),
        )
    });
    std::thread::sleep(Duration::from_millis(25));
    let committed_at = Instant::now();
    store
        .lock()
        .unwrap()
        .commit_log_offset(receipt.job_id, crate::LogStream::Stdout, 7)
        .unwrap();
    let frame = waiter.join().unwrap().unwrap();
    assert!(
        committed_at.elapsed() < Duration::from_millis(500),
        "waiter slept until its timeout instead of consuming the notification"
    );
    assert!(matches!(
        &frame,
        crate::ObservationFrame::Events { events, .. }
            if events.iter().any(|event| event.kind == crate::SchedulerEventKind::LogCommitted)
    ));

    let before_second = frame.cursor();
    store
        .lock()
        .unwrap()
        .commit_log_offset(receipt.job_id, crate::LogStream::Stdout, 8)
        .unwrap();
    let started = Instant::now();
    let already_committed = scheduler
        .wait_observation(
            &store,
            &crate::JobSelector::All,
            Some(before_second),
            16,
            Duration::from_secs(2),
        )
        .unwrap();
    assert!(started.elapsed() < Duration::from_millis(100));
    assert!(matches!(
        already_committed,
        crate::ObservationFrame::Events { ref events, .. }
            if events.iter().any(|event| event.kind == crate::SchedulerEventKind::LogCommitted)
    ));

    let invalidation_cursor = store
        .lock()
        .unwrap()
        .list_jobs(&crate::JobSelector::All, None, 1)
        .unwrap()
        .event_cursor;
    let other = crate::JobSpec {
        labels: vec![crate::Label {
            key: "other".into(),
            value: "job".into(),
        }],
        ..spec
    };
    let other_hash = crate::store::normalized_payload_hash(&other).unwrap();
    store
        .lock()
        .unwrap()
        .submit(uuid::Uuid::now_v7(), &other_hash, &other)
        .unwrap();
    let started = Instant::now();
    let invalidation = scheduler
        .wait_observation(
            &store,
            &crate::JobSelector::Jobs {
                job_ids: vec![receipt.job_id],
            },
            Some(invalidation_cursor),
            16,
            Duration::from_secs(2),
        )
        .unwrap();
    assert!(started.elapsed() < Duration::from_millis(100));
    assert!(matches!(
        invalidation,
        crate::ObservationFrame::Events { ref events, cursor }
            if events.is_empty() && cursor.sequence > invalidation_cursor.sequence
    ));
}

#[test]
fn wait_snapshot_includes_daemon_reconciliation_evidence() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    let spec = crate::JobSpec {
        spec_version: crate::SPEC_VERSION,
        executable: temp.path().join("tool.exe"),
        args: Vec::new(),
        working_directory: temp.path().to_path_buf(),
        stdin: crate::StdinSpec::Eof,
        environment: Default::default(),
        resources: Default::default(),
        observed: None,
        conditions: Vec::new(),
        retry: Default::default(),
        postconditions: Vec::new(),
        labels: Vec::new(),
        expected_duration_seconds: None,
        timeout_seconds: None,
        quiet: None,
        artifacts: Vec::new(),
        child_submission_policy: None,
    };
    let hash = crate::store::normalized_payload_hash(&spec).unwrap();
    let receipt = store
        .submit(uuid::Uuid::now_v7(), &hash, &spec)
        .unwrap()
        .receipt;
    let prepared = store.prepare_job(receipt.job_id).unwrap().unwrap();
    store
        .mark_uncertain(&prepared, None, "interrupted")
        .unwrap();

    let scheduler = observation_scheduler();
    scheduler
        .reconciliation_observations
        .lock()
        .unwrap()
        .record(
            prepared.containment_id,
            crate::ReconciliationResult::BoundaryNotEmpty,
        );
    let snapshot = scheduler
        .wait_snapshot(&Arc::new(Mutex::new(store)), receipt.job_id, Duration::ZERO)
        .unwrap();

    assert_eq!(
        snapshot.attempts[0].invocations[0]
            .containment
            .incident
            .as_ref()
            .and_then(|incident| incident.last_reconciliation.clone()),
        Some(crate::ReconciliationResult::BoundaryNotEmpty)
    );
}

#[test]
fn boot_change_is_proof_only_for_a_prior_generation() {
    let live_containments = crate::runner::LiveContainments::default();
    let store_uuid = uuid::Uuid::now_v7();
    let generation = uuid::Uuid::now_v7();
    let host = crate::HostId("host".into());
    let candidate = crate::store::ReconciliationCandidate {
        containment_id: crate::ContainmentId::new(store_uuid),
        invocation_id: crate::InvocationId::new(store_uuid),
        attempt_id: crate::AttemptId::new(store_uuid),
        version: 1,
        host_id: Some(host.clone()),
        boot_id: Some(crate::BootId("prior-boot".into())),
        daemon_generation: Some(generation),
        root_pid_recorded: false,
        root_identity: None,
        prior_daemon_identity: None,
        incident_sequence: 1,
    };
    let current = (host, crate::BootId("current-boot".into()), generation);
    assert_eq!(
        probe_reconciliation_candidate(&live_containments, &candidate, &current),
        (None, crate::ReconciliationResult::IdentityUnavailable)
    );
    let mut prior = candidate;
    prior.daemon_generation = Some(uuid::Uuid::now_v7());
    assert_eq!(
        probe_reconciliation_candidate(&live_containments, &prior, &current),
        (
            Some(crate::ContainmentResolution::Reboot),
            crate::ReconciliationResult::PriorBoot
        )
    );
}

#[test]
fn prior_generation_requires_absent_daemon_and_exact_root_evidence() {
    let live_containments = crate::runner::LiveContainments::default();
    let startup = crate::identity::probe_startup_identity();
    let host = startup.host_id.unwrap();
    let boot = startup.boot_id.unwrap();
    let current_process = startup.daemon_process.unwrap();
    let absent_daemon = match current_process.clone() {
        crate::ProcessIdentity::Windows {
            host_id,
            boot_id,
            pid,
            creation_filetime_100ns,
        } => crate::ProcessIdentity::Windows {
            host_id,
            boot_id,
            pid,
            creation_filetime_100ns: creation_filetime_100ns.saturating_add(1),
        },
        _ => unreachable!("Windows test requires Windows process identity"),
    };
    let store_uuid = uuid::Uuid::now_v7();
    let current_generation = uuid::Uuid::now_v7();
    let current = (host.clone(), boot.clone(), current_generation);
    let candidate = crate::store::ReconciliationCandidate {
        containment_id: crate::ContainmentId::new(store_uuid),
        invocation_id: crate::InvocationId::new(store_uuid),
        attempt_id: crate::AttemptId::new(store_uuid),
        version: 1,
        host_id: Some(host.clone()),
        boot_id: Some(boot),
        daemon_generation: Some(uuid::Uuid::now_v7()),
        root_pid_recorded: false,
        root_identity: None,
        prior_daemon_identity: Some(absent_daemon),
        incident_sequence: 1,
    };
    assert_eq!(
        probe_reconciliation_candidate(&live_containments, &candidate, &current),
        (
            Some(crate::ContainmentResolution::ProvenEmpty),
            crate::ReconciliationResult::IdentityAbsent
        )
    );

    let mut live_root = candidate.clone();
    live_root.root_pid_recorded = true;
    live_root.root_identity = Some(current_process);
    assert_eq!(
        probe_reconciliation_candidate(&live_containments, &live_root, &current),
        (None, crate::ReconciliationResult::StillResolves)
    );

    let mut pid_only = candidate.clone();
    pid_only.root_pid_recorded = true;
    assert_eq!(
        probe_reconciliation_candidate(&live_containments, &pid_only, &current),
        (None, crate::ReconciliationResult::IdentityUnavailable)
    );

    let mut foreign_host = candidate;
    foreign_host.host_id = Some(crate::HostId("foreign".into()));
    assert_eq!(
        probe_reconciliation_candidate(&live_containments, &foreign_host, &current),
        (None, crate::ReconciliationResult::IdentityUnavailable)
    );
}

#[test]
fn force_authorization_fails_closed_for_roots_and_missing_handles() {
    let live_containments = crate::runner::LiveContainments::default();
    let startup = crate::identity::probe_startup_identity();
    let requester = startup.daemon_process.unwrap();
    let peer = PeerProcess {
        handle: 0,
        pid: std::process::id(),
        identity: Some(requester.clone()),
    };
    assert!(matches!(
        authorize_force_peer(
            &live_containments,
            &peer,
            &requester,
            &[],
            std::slice::from_ref(&requester),
        ),
        Err(StoreError::OperationRejected { code, .. })
            if code == "containment_caller_managed"
    ));
    assert!(matches!(
        authorize_force_peer(
            &live_containments,
            &peer,
            &requester,
            &[crate::InvocationId::new(uuid::Uuid::now_v7())],
            &[],
        ),
        Err(StoreError::OperationRejected { code, .. })
            if code == "containment_authorization_unavailable"
    ));
}
