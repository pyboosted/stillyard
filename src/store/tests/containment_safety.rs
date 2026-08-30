use super::*;

#[test]
fn creating_and_started_records_capture_exact_identity() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    let job_spec = spec(temp.path());
    let hash = normalized_payload_hash(&job_spec).unwrap();
    let receipt = store
        .submit(Uuid::now_v7(), &hash, &job_spec)
        .unwrap()
        .receipt;
    let prepared = store.prepare_job(receipt.job_id).unwrap().unwrap();
    let creating: (String, String, String, String) = store
        .connection
        .query_row(
            "SELECT host_id, boot_id, daemon_generation, strength
                 FROM containments WHERE id = ?1",
            [prepared.containment_id.entity_uuid().to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(
        creating.0,
        store.startup_identity.host_id.as_ref().unwrap().0
    );
    assert_eq!(
        creating.1,
        store.startup_identity.boot_id.as_ref().unwrap().0
    );
    assert_eq!(creating.2, store.daemon_generation.to_string());
    assert_eq!(creating.3, "windows_job_object");

    let root = ProcessIdentity::Windows {
        host_id: store.startup_identity.host_id.clone().unwrap(),
        boot_id: store.startup_identity.boot_id.clone().unwrap(),
        pid: 4242,
        creation_filetime_100ns: 123_456_789,
    };
    store
        .mark_started_with_identity(&prepared, 4242, "containment-image-hash", Some(&root))
        .unwrap();
    let snapshot = store.status(receipt.job_id).unwrap();
    assert_eq!(
        snapshot.attempts[0].invocations[0].root_identity,
        Some(root)
    );
    assert_eq!(
        store.daemon_status("test").unwrap().process_identity,
        store.startup_identity.daemon_process
    );
}

#[test]
fn clearance_is_idempotent_and_audited_through_status() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    let mut doctor_cache = DoctorSnapshotCache::new(store.store_uuid(), store.daemon_generation());
    let job_spec = spec(temp.path());
    let hash = normalized_payload_hash(&job_spec).unwrap();
    let receipt = store
        .submit(Uuid::now_v7(), &hash, &job_spec)
        .unwrap()
        .receipt;
    let prepared = store.prepare_job(receipt.job_id).unwrap().unwrap();
    store
        .mark_uncertain(&prepared, None, "interrupted")
        .unwrap();
    let before = store.doctor("test", None, None, &mut doctor_cache).unwrap();
    assert_eq!(before.incidents.total_unresolved, 1);
    assert_eq!(
        before.incidents.incidents[0].containment_id,
        prepared.containment_id
    );
    let candidate = store
        .reconciliation_candidate(prepared.containment_id)
        .unwrap();
    assert!(matches!(
        store.commit_containment_resolution(
            &candidate,
            ContainmentResolution::ProvenEmpty,
            ReconciliationResult::Unknown("future_proof".into()),
            ClearanceOrigin::Automatic,
            None,
            None,
        ),
        Err(StoreError::InvalidState(_))
    ));
    let requester = store.startup_identity.daemon_process.clone().unwrap();
    let forced = ForcedClearanceAudit {
        requested_unix_millis: now_millis(),
        requester,
    };
    let result = store
        .commit_containment_resolution(
            &candidate,
            ContainmentResolution::ForcedRiskAcceptance,
            ReconciliationResult::IdentityAbsent,
            ClearanceOrigin::Forced,
            Some(forced),
            None,
        )
        .unwrap()
        .unwrap();
    assert!(result.audit.lease_released);
    assert_eq!(
        store.persisted_clearance(prepared.containment_id).unwrap(),
        Some(result.clone())
    );
    assert!(
        store
            .doctor("test", None, None, &mut doctor_cache)
            .unwrap()
            .incidents
            .incidents
            .is_empty()
    );
    let status = store.status(receipt.job_id).unwrap();
    let containment = &status.attempts[0].invocations[0].containment;
    assert_eq!(containment.state, ContainmentState::Cleared);
    assert_eq!(containment.resolution_audit, Some(result.audit));
}

#[test]
fn attempt_wide_predicate_waits_for_every_containment() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    let job_spec = spec(temp.path());
    let hash = normalized_payload_hash(&job_spec).unwrap();
    let receipt = store
        .submit(Uuid::now_v7(), &hash, &job_spec)
        .unwrap()
        .receipt;
    let prepared = store.prepare_job(receipt.job_id).unwrap().unwrap();
    store
        .mark_uncertain(&prepared, None, "interrupted")
        .unwrap();
    let sibling_invocation = InvocationId::new(store.store_uuid);
    let sibling_containment = ContainmentId::new(store.store_uuid);
    store
        .connection
        .execute(
            "INSERT INTO invocations(id, attempt_id, role, role_index, state, finished_ms)
                 VALUES (?1, ?2, 'postcondition', 1, 'resolved', ?3)",
            params![
                sibling_invocation.entity_uuid().to_string(),
                prepared.attempt_id.entity_uuid().to_string(),
                now_millis(),
            ],
        )
        .unwrap();
    store
        .connection
        .execute(
            "INSERT INTO containments(
                    id, invocation_id, state, host_id, boot_id, daemon_generation, strength,
                    version, incident_sequence, reason_code, detail, opened_ms,
                    retained_claims_json
                 ) VALUES (?1, ?2, 'uncertain', ?3, ?4, ?5, 'windows_job_object',
                           1, 2, 'fixture', 'sibling blocker', ?6, ?7)",
            params![
                sibling_containment.entity_uuid().to_string(),
                sibling_invocation.entity_uuid().to_string(),
                store.startup_identity.host_id.as_ref().unwrap().0,
                store.startup_identity.boot_id.as_ref().unwrap().0,
                Uuid::now_v7().to_string(),
                now_millis(),
                serde_json::to_string(&job_spec.resources).unwrap(),
            ],
        )
        .unwrap();
    let first = store
        .reconciliation_candidate(prepared.containment_id)
        .unwrap();
    let first = store
        .commit_containment_resolution(
            &first,
            ContainmentResolution::ProvenEmpty,
            ReconciliationResult::ProvenEmpty,
            ClearanceOrigin::Automatic,
            None,
            None,
        )
        .unwrap()
        .unwrap();
    assert!(!first.audit.lease_released);
    let second = store.reconciliation_candidate(sibling_containment).unwrap();
    let second = store
        .commit_containment_resolution(
            &second,
            ContainmentResolution::ProvenEmpty,
            ReconciliationResult::IdentityAbsent,
            ClearanceOrigin::Automatic,
            None,
            None,
        )
        .unwrap()
        .unwrap();
    assert!(second.audit.lease_released);
}

#[test]
fn force_commit_rejects_a_changed_authorization_set() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    let job_spec = spec(temp.path());
    let hash = normalized_payload_hash(&job_spec).unwrap();
    let first_receipt = store
        .submit(Uuid::now_v7(), &hash, &job_spec)
        .unwrap()
        .receipt;
    let first = store.prepare_job(first_receipt.job_id).unwrap().unwrap();
    store.mark_uncertain(&first, None, "fixture").unwrap();
    let candidate = store
        .reconciliation_candidate(first.containment_id)
        .unwrap();
    let (authorized, _) = store.clearance_authorization_evidence().unwrap();

    let second_receipt = store
        .submit(Uuid::now_v7(), &hash, &job_spec)
        .unwrap()
        .receipt;
    store.prepare_job(second_receipt.job_id).unwrap().unwrap();
    let requester = store.startup_identity.daemon_process.clone().unwrap();
    let result = store
        .commit_containment_resolution(
            &candidate,
            ContainmentResolution::ForcedRiskAcceptance,
            ReconciliationResult::IdentityAbsent,
            ClearanceOrigin::Forced,
            Some(ForcedClearanceAudit {
                requested_unix_millis: now_millis(),
                requester,
            }),
            Some(&authorized),
        )
        .unwrap();
    assert!(result.is_none());
    let lease: String = store
        .connection
        .query_row(
            "SELECT state FROM leases WHERE attempt_id = ?1",
            [first.attempt_id.entity_uuid().to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(lease, "granted");
}

#[test]
fn missing_host_capability_blocks_before_lease_grant() {
    let temp = tempfile::tempdir().unwrap();
    let config = HostConfig {
        resources: capacities(),
        impact_incompatibilities: Default::default(),
        observation: Default::default(),
    };
    let unavailable = StartupIdentity {
        host_id: None,
        boot_id: None,
        daemon_process: None,
        failures: vec!["fixture identity failure".into()],
    };
    let mut store = Store::open_with_config(
        StorePaths::new(temp.path().to_path_buf()),
        config,
        unavailable,
    )
    .unwrap();
    let job_spec = spec(temp.path());
    let hash = normalized_payload_hash(&job_spec).unwrap();
    let receipt = store
        .submit(Uuid::now_v7(), &hash, &job_spec)
        .unwrap()
        .receipt;
    assert!(store.prepare_job(receipt.job_id).unwrap().is_none());
    let snapshot = store.status(receipt.job_id).unwrap();
    assert_eq!(snapshot.blockers[0].code, "host_capability_unavailable");
    let granted: bool = store
        .connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM leases WHERE state = 'granted')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(!granted);
}

#[test]
fn doctor_reports_loaded_config_evidence() {
    let temp = tempfile::tempdir().unwrap();
    let config = HostConfig {
        resources: capacities(),
        impact_incompatibilities: [("measurement".into(), vec!["cpu_heavy".into()])].into(),
        observation: Default::default(),
    };
    let expected_hash = format!("{:x}", Sha256::digest(serde_json::to_vec(&config).unwrap()));
    let store = Store::open_with_config(
        StorePaths::new(temp.path().to_path_buf()),
        config,
        probe_startup_identity(),
    )
    .unwrap();
    let mut doctor_cache = DoctorSnapshotCache::new(store.store_uuid(), store.daemon_generation());
    let doctor = store.doctor("test", None, None, &mut doctor_cache).unwrap();
    assert_eq!(doctor.daemon.capacities, capacities());
    assert_eq!(doctor.daemon.config_sha256, expected_hash);
}

#[test]
fn unbound_pending_store_binds_without_reset() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StorePaths::new(temp.path().to_path_buf());
    let (store_uuid, job_id) = {
        let mut store = Store::open(paths).unwrap();
        let job_spec = spec(temp.path());
        let hash = normalized_payload_hash(&job_spec).unwrap();
        let job_id = store
            .submit(Uuid::now_v7(), &hash, &job_spec)
            .unwrap()
            .receipt
            .job_id;
        store
            .connection
            .execute("DELETE FROM meta WHERE key = 'bound_host_id'", [])
            .unwrap();
        (store.store_uuid, job_id)
    };
    let store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    assert_eq!(store.store_uuid, store_uuid);
    assert_eq!(store.status(job_id).unwrap().state, JobState::Pending);
    assert_eq!(store.bound_host_id, store.startup_identity.host_id);
}

#[test]
fn unbound_store_with_containment_is_replaced() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StorePaths::new(temp.path().to_path_buf());
    let original_uuid = {
        let mut store = Store::open(paths).unwrap();
        let job_spec = spec(temp.path());
        let hash = normalized_payload_hash(&job_spec).unwrap();
        let receipt = store
            .submit(Uuid::now_v7(), &hash, &job_spec)
            .unwrap()
            .receipt;
        store.prepare_job(receipt.job_id).unwrap().unwrap();
        store
            .connection
            .execute("DELETE FROM meta WHERE key = 'bound_host_id'", [])
            .unwrap();
        store.store_uuid
    };
    let store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    assert_ne!(store.store_uuid, original_uuid);
    let containments: u64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM containments", [], |row| row.get(0))
        .unwrap();
    assert_eq!(containments, 0);
}

#[cfg(windows)]
#[test]
fn foreign_host_binding_resets_the_whole_store() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StorePaths::new(temp.path().to_path_buf());
    let original_uuid = Store::open(paths.clone()).unwrap().store_uuid;
    let observed = probe_startup_identity();
    let boot_id = observed.boot_id.unwrap();
    let (pid, creation_filetime_100ns) = match observed.daemon_process.unwrap() {
        ProcessIdentity::Windows {
            pid,
            creation_filetime_100ns,
            ..
        } => (pid, creation_filetime_100ns),
        ProcessIdentity::Unknown { .. } => panic!("Windows test requires Windows identity"),
    };
    let foreign_host = HostId("sha256:fixture-foreign-host".into());
    let startup = StartupIdentity {
        host_id: Some(foreign_host.clone()),
        boot_id: Some(boot_id.clone()),
        daemon_process: Some(ProcessIdentity::Windows {
            host_id: foreign_host.clone(),
            boot_id,
            pid,
            creation_filetime_100ns,
        }),
        failures: Vec::new(),
    };
    let store = Store::open_with_config(
        paths,
        HostConfig {
            resources: capacities(),
            impact_incompatibilities: Default::default(),
            observation: Default::default(),
        },
        startup,
    )
    .unwrap();
    assert_ne!(store.store_uuid, original_uuid);
    assert_eq!(store.bound_host_id, Some(foreign_host));
}

#[test]
fn doctor_incidents_are_frozen_across_pages() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StorePaths::new(temp.path().to_path_buf());
    let mut store = Store::open(paths.clone()).unwrap();
    let mut doctor_cache = DoctorSnapshotCache::new(store.store_uuid(), store.daemon_generation());
    let mut original = Vec::new();
    for _ in 0..3 {
        let job_spec = spec(temp.path());
        let hash = normalized_payload_hash(&job_spec).unwrap();
        let receipt = store
            .submit(Uuid::now_v7(), &hash, &job_spec)
            .unwrap()
            .receipt;
        let prepared = store.prepare_job(receipt.job_id).unwrap().unwrap();
        store.mark_uncertain(&prepared, None, "fixture").unwrap();
        original.push(prepared.containment_id);
    }
    let first = store
        .doctor("test", None, Some(2), &mut doctor_cache)
        .unwrap();
    assert_eq!(first.incidents.total_unresolved, 3);
    assert_eq!(first.incidents.incidents.len(), 2);
    assert!(first.incidents.truncated);

    store
        .connection
        .execute(
            "UPDATE containments SET state = 'cleared', resolution = 'forced_risk_acceptance',
             resolved_ms = ?2 WHERE id = ?1",
            params![original[2].entity_uuid().to_string(), now_millis()],
        )
        .unwrap();
    let replacement_spec = spec(temp.path());
    let replacement_hash = normalized_payload_hash(&replacement_spec).unwrap();
    let replacement_receipt = store
        .submit(Uuid::now_v7(), &replacement_hash, &replacement_spec)
        .unwrap()
        .receipt;
    let replacement = store
        .prepare_job(replacement_receipt.job_id)
        .unwrap()
        .unwrap();
    store
        .mark_uncertain(&replacement, None, "replacement")
        .unwrap();

    let cursor = first.incidents.next_cursor;
    let second = store
        .doctor("test", cursor, Some(2), &mut doctor_cache)
        .unwrap();
    assert_eq!(second.incidents.total_unresolved, 3);
    assert_eq!(second.incidents.incidents.len(), 1);
    assert!(!second.incidents.truncated);
    let old_inventory = first
        .incidents
        .incidents
        .iter()
        .chain(&second.incidents.incidents)
        .map(|incident| incident.incident_id)
        .collect::<Vec<_>>();
    assert_eq!(old_inventory, original);
    assert_eq!(
        second.incidents.incidents[0].state,
        ContainmentState::Uncertain
    );

    let current = store
        .doctor("test", None, Some(10), &mut doctor_cache)
        .unwrap();
    let current_ids = current
        .incidents
        .incidents
        .iter()
        .map(|incident| incident.incident_id)
        .collect::<Vec<_>>();
    assert_eq!(current.incidents.total_unresolved, 3);
    assert!(!current_ids.contains(&original[2]));
    assert!(current_ids.contains(&replacement.containment_id));
}

#[test]
fn doctor_rejects_foreign_unknown_tampered_and_expired_cursors() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    let mut doctor_cache = DoctorSnapshotCache::new(store.store_uuid(), store.daemon_generation());
    for _ in 0..2 {
        let job_spec = spec(temp.path());
        let hash = normalized_payload_hash(&job_spec).unwrap();
        let receipt = store
            .submit(Uuid::now_v7(), &hash, &job_spec)
            .unwrap()
            .receipt;
        let prepared = store.prepare_job(receipt.job_id).unwrap().unwrap();
        store.mark_uncertain(&prepared, None, "fixture").unwrap();
    }
    let first = store
        .doctor("test", None, Some(1), &mut doctor_cache)
        .unwrap();
    let cursor = first.incidents.next_cursor.unwrap();

    let foreign = ContainmentIncidentCursor {
        store_uuid: Uuid::now_v7(),
        ..cursor
    };
    assert!(matches!(
        store.doctor("test", Some(foreign), Some(1), &mut doctor_cache),
        Err(StoreError::DoctorCursorStale(_))
    ));
    let unknown = ContainmentIncidentCursor {
        snapshot_uuid: Uuid::now_v7(),
        token_uuid: Uuid::now_v7(),
        ..cursor
    };
    assert!(matches!(
        store.doctor("test", Some(unknown), Some(1), &mut doctor_cache),
        Err(StoreError::DoctorCursorStale(_))
    ));
    let tampered = ContainmentIncidentCursor {
        offset: cursor.offset + 1,
        ..cursor
    };
    assert!(matches!(
        store.doctor("test", Some(tampered), Some(1), &mut doctor_cache),
        Err(StoreError::DoctorCursorStale(_))
    ));

    doctor_cache.expire(cursor.snapshot_uuid);
    assert!(matches!(
        store.doctor("test", Some(cursor), Some(1), &mut doctor_cache),
        Err(StoreError::DoctorCursorStale(_))
    ));
}

#[test]
fn doctor_rejects_cursor_from_previous_daemon_generation() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StorePaths::new(temp.path().to_path_buf());
    let mut store = Store::open(paths.clone()).unwrap();
    let mut cache = DoctorSnapshotCache::new(store.store_uuid(), store.daemon_generation());
    for _ in 0..2 {
        let job_spec = spec(temp.path());
        let hash = normalized_payload_hash(&job_spec).unwrap();
        let receipt = store
            .submit(Uuid::now_v7(), &hash, &job_spec)
            .unwrap()
            .receipt;
        let prepared = store.prepare_job(receipt.job_id).unwrap().unwrap();
        store.mark_uncertain(&prepared, None, "fixture").unwrap();
    }
    let cursor = store
        .doctor("test", None, Some(1), &mut cache)
        .unwrap()
        .incidents
        .next_cursor
        .unwrap();
    drop(store);

    let reopened = Store::open(paths).unwrap();
    let mut restarted_cache =
        DoctorSnapshotCache::new(reopened.store_uuid(), reopened.daemon_generation());
    assert!(matches!(
        reopened.doctor("test", Some(cursor), Some(1), &mut restarted_cache),
        Err(StoreError::DoctorCursorStale(detail)) if detail.contains("generation")
    ));
}

#[test]
fn doctor_snapshot_stress_resists_parallel_incident_turnover() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StorePaths::new(temp.path().to_path_buf());
    let mut store = Store::open(paths.clone()).unwrap();
    let mut doctor_cache = DoctorSnapshotCache::new(store.store_uuid(), store.daemon_generation());
    for _ in 0..40 {
        let job_spec = spec(temp.path());
        let hash = normalized_payload_hash(&job_spec).unwrap();
        let receipt = store
            .submit(Uuid::now_v7(), &hash, &job_spec)
            .unwrap()
            .receipt;
        let prepared = store.prepare_job(receipt.job_id).unwrap().unwrap();
        store.mark_uncertain(&prepared, None, "fixture").unwrap();
    }
    let expected = store
        .doctor(
            "test",
            None,
            Some(crate::MAX_DOCTOR_PAGE),
            &mut doctor_cache,
        )
        .unwrap()
        .incidents
        .incidents
        .into_iter()
        .map(|incident| incident.incident_id)
        .collect::<Vec<_>>();
    let first = store
        .doctor("test", None, Some(3), &mut doctor_cache)
        .unwrap();
    let mut observed = first
        .incidents
        .incidents
        .into_iter()
        .map(|incident| incident.incident_id)
        .collect::<Vec<_>>();
    let mut cursor = first.incidents.next_cursor;

    let database = paths.database.clone();
    let turnover = std::thread::spawn(move || {
        let connection = rusqlite::Connection::open(database).unwrap();
        connection
            .busy_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        for _ in 0..101 {
            connection
                .execute(
                    "UPDATE containments
                     SET state = CASE state WHEN 'uncertain' THEN 'cleared' ELSE 'uncertain' END",
                    [],
                )
                .unwrap();
            std::thread::yield_now();
        }
    });
    while let Some(next) = cursor {
        let page = store
            .doctor("test", Some(next), Some(3), &mut doctor_cache)
            .unwrap();
        observed.extend(
            page.incidents
                .incidents
                .into_iter()
                .map(|incident| incident.incident_id),
        );
        cursor = page.incidents.next_cursor;
        std::thread::yield_now();
    }
    turnover.join().unwrap();
    assert_eq!(observed, expected);
    assert_eq!(
        observed
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        expected.len()
    );
}

#[test]
fn doctor_default_page_is_bounded_below_protocol_limit() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    let job_spec = spec(temp.path());
    let hash = normalized_payload_hash(&job_spec).unwrap();
    let receipt = store
        .submit(Uuid::now_v7(), &hash, &job_spec)
        .unwrap()
        .receipt;
    let prepared = store.prepare_job(receipt.job_id).unwrap().unwrap();
    store.mark_uncertain(&prepared, None, "fixture").unwrap();
    let host = store.startup_identity.host_id.as_ref().unwrap().0.clone();
    let boot = store.startup_identity.boot_id.as_ref().unwrap().0.clone();
    let generation = store.daemon_generation.to_string();
    let claims = serde_json::to_string(&job_spec.resources).unwrap();
    let oversized_code = "x".repeat(DOCTOR_CODE_MAX_BYTES + 1);
    let oversized_detail = "é".repeat(DOCTOR_DETAIL_MAX_BYTES + 1);
    let transaction = store.connection.transaction().unwrap();
    for role_index in 1..257_u32 {
        let invocation = InvocationId::new(store.store_uuid);
        let containment = ContainmentId::new(store.store_uuid);
        transaction
            .execute(
                "INSERT INTO invocations(id, attempt_id, role, role_index, state, finished_ms)
                     VALUES (?1, ?2, 'postcondition', ?3, 'resolved', ?4)",
                params![
                    invocation.entity_uuid().to_string(),
                    prepared.attempt_id.entity_uuid().to_string(),
                    role_index,
                    now_millis(),
                ],
            )
            .unwrap();
        transaction
            .execute(
                "INSERT INTO containments(
                        id, invocation_id, state, host_id, boot_id, daemon_generation, strength,
                        version, incident_sequence, reason_code, detail, opened_ms,
                        retained_claims_json
                     ) VALUES (?1, ?2, 'uncertain', ?3, ?4, ?5, 'windows_job_object',
                               1, ?6, ?9, ?10, ?7, ?8)",
                params![
                    containment.entity_uuid().to_string(),
                    invocation.entity_uuid().to_string(),
                    host,
                    boot,
                    generation,
                    role_index + 1,
                    now_millis(),
                    claims,
                    oversized_code,
                    oversized_detail,
                ],
            )
            .unwrap();
    }
    transaction.commit().unwrap();
    let changes_before_observation = store.connection.total_changes();
    let first_turn = store.reconciliation_candidates(0, u32::MAX).unwrap();
    assert_eq!(first_turn.len(), 32);
    let turn_cursor = first_turn.last().unwrap().incident_sequence;
    let second_turn = store
        .reconciliation_candidates(turn_cursor, u32::MAX)
        .unwrap();
    assert_eq!(second_turn.len(), 32);
    assert!(
        second_turn
            .first()
            .is_some_and(|candidate| candidate.incident_sequence > turn_cursor)
    );
    let mut observations = ReconciliationObservations::default();
    observations.record(
        first_turn[0].containment_id,
        ReconciliationResult::BoundaryNotEmpty,
    );
    assert_eq!(store.connection.total_changes(), changes_before_observation);
    let mut doctor_cache = DoctorSnapshotCache::new(store.store_uuid(), store.daemon_generation());
    let first_page = doctor_cache
        .begin(
            store.capture_doctor_incidents(&observations).unwrap(),
            crate::MAX_DOCTOR_PAGE as usize,
        )
        .unwrap();
    let first = store.doctor_with_incident_page("test", first_page).unwrap();
    assert_eq!(first.incidents.total_unresolved, 257);
    assert_eq!(first.incidents.incidents.len(), 256);
    assert!(first.incidents.truncated);
    assert!(first.incidents.incidents.iter().all(|incident| {
        incident.reason_code.len() <= DOCTOR_CODE_MAX_BYTES
            && incident.reason_code.is_ascii()
            && incident.detail.len() <= DOCTOR_DETAIL_MAX_BYTES
            && incident.detail.is_char_boundary(incident.detail.len())
    }));
    assert!(first.checks.iter().all(|check| {
        check.code.len() <= DOCTOR_CODE_MAX_BYTES
            && check.code.is_ascii()
            && check.summary.len() <= DOCTOR_SUMMARY_MAX_BYTES
            && check
                .remediation
                .as_ref()
                .is_none_or(|text| text.len() <= DOCTOR_DETAIL_MAX_BYTES)
    }));
    assert!(serde_json::to_vec(&first).unwrap().len() < 16 * 1024 * 1024);
    let tail_page = doctor_cache
        .next(
            first.incidents.next_cursor.unwrap(),
            crate::MAX_DOCTOR_PAGE as usize,
        )
        .unwrap();
    let tail = store.doctor_with_incident_page("test", tail_page).unwrap();
    assert_eq!(tail.incidents.incidents.len(), 1);
    assert!(!tail.incidents.truncated);
    assert_eq!(store.connection.total_changes(), changes_before_observation);
}
