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
    let before = store.doctor("test", None, None).unwrap();
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
            .doctor("test", None, None)
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
        profiles: Default::default(),
        impact_incompatibilities: Default::default(),
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
fn doctor_reports_loaded_config_evidence_without_profile_values() {
    let temp = tempfile::tempdir().unwrap();
    let sentinel = "SECRET_PROFILE_SENTINEL";
    let config = HostConfig {
        resources: capacities(),
        profiles: [(
            "reviewer".into(),
            EnvironmentProfile {
                set: [("PRIVATE_VALUE".into(), sentinel.into())].into(),
                ..EnvironmentProfile::default()
            },
        )]
        .into(),
        impact_incompatibilities: Default::default(),
    };
    let expected_hash = format!("{:x}", Sha256::digest(serde_json::to_vec(&config).unwrap()));
    let store = Store::open_with_config(
        StorePaths::new(temp.path().to_path_buf()),
        config,
        probe_startup_identity(),
    )
    .unwrap();
    let doctor = store.doctor("test", None, None).unwrap();
    assert_eq!(doctor.daemon.profile_names, vec!["reviewer"]);
    assert_eq!(doctor.daemon.capacities, capacities());
    assert_eq!(doctor.daemon.config_sha256, expected_hash);
    assert!(!serde_json::to_string(&doctor).unwrap().contains(sentinel));
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
            profiles: Default::default(),
            impact_incompatibilities: Default::default(),
        },
        startup,
    )
    .unwrap();
    assert_ne!(store.store_uuid, original_uuid);
    assert_eq!(store.bound_host_id, Some(foreign_host));
}

#[test]
fn doctor_incidents_page_by_durable_sequence() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    for _ in 0..3 {
        let job_spec = spec(temp.path());
        let hash = normalized_payload_hash(&job_spec).unwrap();
        let receipt = store
            .submit(Uuid::now_v7(), &hash, &job_spec)
            .unwrap()
            .receipt;
        let prepared = store.prepare_job(receipt.job_id).unwrap().unwrap();
        store.mark_uncertain(&prepared, None, "fixture").unwrap();
    }
    let first = store.doctor("test", None, Some(2)).unwrap();
    assert_eq!(first.incidents.total_unresolved, 3);
    assert_eq!(first.incidents.incidents.len(), 2);
    assert!(first.incidents.truncated);
    let second = store
        .doctor("test", first.incidents.next_cursor, Some(2))
        .unwrap();
    assert_eq!(second.incidents.incidents.len(), 1);
    assert!(!second.incidents.truncated);
    assert!(
        first.incidents.incidents[1].incident_sequence
            < second.incidents.incidents[0].incident_sequence
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
    let first = store
        .doctor_with_reconciliation("test", None, None, &observations)
        .unwrap();
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
    let tail = store
        .doctor_with_reconciliation("test", first.incidents.next_cursor, None, &observations)
        .unwrap();
    assert_eq!(tail.incidents.incidents.len(), 1);
    assert!(!tail.incidents.truncated);
}
