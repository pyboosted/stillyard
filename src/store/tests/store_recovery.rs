use super::*;

#[test]
fn duplicate_key_returns_one_job() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    let key = Uuid::now_v7();
    let spec = spec(temp.path());
    let hash = normalized_payload_hash(&spec).unwrap();
    let first = store.submit(key, &hash, &spec).unwrap();
    let second = store.submit(key, &hash, &spec).unwrap();
    assert_eq!(first.receipt.job_id, second.receipt.job_id);
    assert!(first.should_schedule);
    assert!(!second.should_schedule);
}

#[test]
fn foreign_store_id_rejects_even_if_entity_uuid_collides() {
    let first_dir = tempfile::tempdir().unwrap();
    let second_dir = tempfile::tempdir().unwrap();
    let mut first = Store::open(StorePaths::new(first_dir.path().to_path_buf())).unwrap();
    let job_spec = spec(first_dir.path());
    let hash = normalized_payload_hash(&job_spec).unwrap();
    let receipt = first
        .submit(Uuid::now_v7(), &hash, &job_spec)
        .unwrap()
        .receipt;
    let second = Store::open(StorePaths::new(second_dir.path().to_path_buf())).unwrap();
    let foreign = JobId::from_parts(second.store_uuid, receipt.job_id.entity_uuid());
    assert!(matches!(
        first.status(foreign),
        Err(StoreError::NotFound(message)) if message.contains("foreign durable ID")
    ));
}

#[test]
fn same_key_different_payload_conflicts() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    let key = Uuid::now_v7();
    let first = spec(temp.path());
    let first_hash = normalized_payload_hash(&first).unwrap();
    store.submit(key, &first_hash, &first).unwrap();
    let mut second = first.clone();
    second.args.push("different".into());
    let second_hash = normalized_payload_hash(&second).unwrap();
    assert!(matches!(
        store.submit(key, &second_hash, &second),
        Err(StoreError::IdempotencyConflict)
    ));
}

#[test]
fn recovery_never_creates_work_and_distinguishes_conflict() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    let key = Uuid::now_v7();
    assert_eq!(
        store.recover_submission(key, "hash").unwrap(),
        RecoveryResult::Unknown
    );
    let spec = spec(temp.path());
    let hash = normalized_payload_hash(&spec).unwrap();
    let submitted = store.submit(key, &hash, &spec).unwrap();
    assert_eq!(
        store.recover_submission(key, "other").unwrap(),
        RecoveryResult::Conflict
    );
    match store.recover_submission(key, &hash).unwrap() {
        RecoveryResult::Accepted(receipt) => {
            assert_eq!(receipt.job_id, submitted.receipt.job_id);
        }
        recovery => panic!("unexpected recovery: {recovery:?}"),
    }
    assert_eq!(store.pending_jobs().unwrap().len(), 1);
}

#[test]
fn rejected_idempotency_decision_replays_as_rejected() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    let key = Uuid::now_v7();
    let spec = spec(temp.path());
    let hash = normalized_payload_hash(&spec).unwrap();
    store
        .connection
        .execute(
            "INSERT INTO submissions(
                    id, scope, idempotency_key, payload_hash, state, spec_json, kind, created_ms
                 ) VALUES (?1, 'unmanaged', ?2, ?3, 'rejected', ?4, 'job', ?5)",
            params![
                Uuid::now_v7().to_string(),
                key.to_string(),
                hash,
                serde_json::to_string(&spec).unwrap(),
                now_millis(),
            ],
        )
        .unwrap();

    assert!(matches!(
        store.submit(key, &normalized_payload_hash(&spec).unwrap(), &spec),
        Err(StoreError::Rejected(_))
    ));
}

#[test]
fn restart_interrupts_active_job_without_requeueing_it() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StorePaths::new(temp.path().to_path_buf());
    let job_id = {
        let mut store = Store::open(paths).unwrap();
        let spec = spec(temp.path());
        let hash = normalized_payload_hash(&spec).unwrap();
        let submitted = store.submit(Uuid::now_v7(), &hash, &spec).unwrap();
        let prepared = store
            .prepare_job(submitted.receipt.job_id)
            .unwrap()
            .unwrap();
        store
            .mark_started(&prepared, std::process::id(), "exe-hash")
            .unwrap();
        prepared.job_id
    };
    let store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    let snapshot = store.status(job_id).unwrap();
    assert_eq!(snapshot.state, JobState::Final);
    assert_eq!(snapshot.outcome, Some(JobOutcome::Interrupted));
    assert!(store.pending_jobs().unwrap().is_empty());
    let (containment, lease): (String, String) = store
        .connection
        .query_row(
            "SELECT containments.state, leases.state
                 FROM containments
                 JOIN invocations ON invocations.id = containments.invocation_id
                 JOIN attempts ON attempts.id = invocations.attempt_id
                 JOIN leases ON leases.attempt_id = attempts.id
                 WHERE attempts.job_id = ?1",
            [job_id.entity_uuid().to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(containment, "uncertain");
    assert_eq!(lease, "granted");
}

#[cfg(windows)]
#[test]
fn restart_never_uses_pid_only_root_disappearance_as_proof() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StorePaths::new(temp.path().to_path_buf());
    let job_id = {
        let mut store = Store::open(paths).unwrap();
        let job_spec = spec(temp.path());
        let hash = normalized_payload_hash(&job_spec).unwrap();
        let submitted = store.submit(Uuid::now_v7(), &hash, &job_spec).unwrap();
        let prepared = store
            .prepare_job(submitted.receipt.job_id)
            .unwrap()
            .unwrap();
        store.mark_started(&prepared, u32::MAX, "exe-hash").unwrap();
        store.mark_root_exited(&prepared, 0).unwrap();
        prepared.job_id
    };
    let store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    let (containment, lease): (String, String) = store
        .connection
        .query_row(
            "SELECT containments.state, leases.state
                 FROM containments
                 JOIN invocations ON invocations.id = containments.invocation_id
                 JOIN attempts ON attempts.id = invocations.attempt_id
                 JOIN leases ON leases.attempt_id = attempts.id
                 WHERE attempts.job_id = ?1",
            [job_id.entity_uuid().to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(containment, "uncertain");
    assert_eq!(lease, "granted");
    assert_eq!(
        store.status(job_id).unwrap().attempts[0].invocations[0].state,
        InvocationState::Resolved
    );
}

#[test]
fn restart_before_root_retains_boundary_until_reconciled() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StorePaths::new(temp.path().to_path_buf());
    let job_id = {
        let mut store = Store::open(paths).unwrap();
        let job_spec = spec(temp.path());
        let hash = normalized_payload_hash(&job_spec).unwrap();
        let submitted = store.submit(Uuid::now_v7(), &hash, &job_spec).unwrap();
        store
            .prepare_job(submitted.receipt.job_id)
            .unwrap()
            .unwrap()
            .job_id
    };
    let store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    let snapshot = store.status(job_id).unwrap();
    assert_eq!(snapshot.outcome, Some(JobOutcome::Failed));
    let (verdict, containment, lease): (String, String, String) = store
        .connection
        .query_row(
            "SELECT attempts.verdict, containments.state, leases.state
                 FROM attempts
                 JOIN invocations ON invocations.attempt_id = attempts.id
                 JOIN containments ON containments.invocation_id = invocations.id
                 JOIN leases ON leases.attempt_id = attempts.id
                 WHERE attempts.job_id = ?1",
            [job_id.entity_uuid().to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(verdict, "start_failed");
    assert_eq!(containment, "uncertain");
    assert_eq!(lease, "granted");
}

#[test]
fn restart_during_prepared_postcondition_retains_attempt_lease() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StorePaths::new(temp.path().to_path_buf());
    let job_id = {
        let mut store = Store::open_with_capacities(paths, capacities()).unwrap();
        let mut job_spec = spec(temp.path());
        job_spec.resources.cargo_slots = Some(1);
        job_spec.postconditions.push(PostconditionSpec {
            executable: temp.path().join("validate.exe"),
            args: Vec::new(),
            working_directory: None,
            accepted_exit_codes: vec![0],
            retryable_exit_codes: Vec::new(),
        });
        let hash = normalized_payload_hash(&job_spec).unwrap();
        let receipt = store
            .submit(Uuid::now_v7(), &hash, &job_spec)
            .unwrap()
            .receipt;
        let primary = store.prepare_job(receipt.job_id).unwrap().unwrap();
        store
            .mark_started(&primary, u32::MAX, "primary-hash")
            .unwrap();
        store.mark_root_exited(&primary, 0).unwrap();
        store
            .mark_invocation_resolved(&primary, Some(0), None)
            .unwrap();
        store.prepare_postcondition(&primary, 0).unwrap();
        receipt.job_id
    };

    let store =
        Store::open_with_capacities(StorePaths::new(temp.path().to_path_buf()), capacities())
            .unwrap();
    let snapshot = store.status(job_id).unwrap();
    assert_eq!(snapshot.outcome, Some(JobOutcome::Interrupted));
    assert_eq!(
        snapshot.attempts[0].verdict,
        Some(AttemptVerdict::Interrupted)
    );
    assert_eq!(snapshot.attempts[0].invocations.len(), 2);
    assert_eq!(
        snapshot.attempts[0].invocations[1].state,
        InvocationState::Resolved
    );
    assert_eq!(
        snapshot.attempts[0].invocations[1].containment.state,
        ContainmentState::Uncertain
    );
    let lease: String = store
        .connection
        .query_row(
            "SELECT leases.state FROM leases JOIN attempts ON attempts.id = leases.attempt_id
                 WHERE attempts.job_id = ?1",
            [job_id.entity_uuid().to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(lease, "granted");
}

#[test]
fn restart_after_resolved_postcondition_releases_empty_attempt_lease() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StorePaths::new(temp.path().to_path_buf());
    let job_id = {
        let mut store = Store::open_with_capacities(paths, capacities()).unwrap();
        let mut job_spec = spec(temp.path());
        job_spec.resources.cargo_slots = Some(1);
        job_spec.postconditions.push(PostconditionSpec {
            executable: temp.path().join("validate.exe"),
            args: Vec::new(),
            working_directory: None,
            accepted_exit_codes: vec![0],
            retryable_exit_codes: Vec::new(),
        });
        let hash = normalized_payload_hash(&job_spec).unwrap();
        let receipt = store
            .submit(Uuid::now_v7(), &hash, &job_spec)
            .unwrap()
            .receipt;
        let primary = store.prepare_job(receipt.job_id).unwrap().unwrap();
        store
            .mark_started(&primary, u32::MAX, "primary-hash")
            .unwrap();
        store.mark_root_exited(&primary, 0).unwrap();
        store
            .mark_invocation_resolved(&primary, Some(0), None)
            .unwrap();
        let validator = store.prepare_postcondition(&primary, 0).unwrap();
        store
            .mark_started(&validator, u32::MAX, "validator-hash")
            .unwrap();
        store.mark_root_exited(&validator, 0).unwrap();
        store
            .mark_invocation_resolved(&validator, Some(0), Some(ExitClassification::Accepted))
            .unwrap();
        receipt.job_id
    };

    let store =
        Store::open_with_capacities(StorePaths::new(temp.path().to_path_buf()), capacities())
            .unwrap();
    let snapshot = store.status(job_id).unwrap();
    assert_eq!(snapshot.outcome, Some(JobOutcome::Interrupted));
    assert_eq!(
        snapshot.attempts[0].verdict,
        Some(AttemptVerdict::Interrupted)
    );
    let lease: String = store
        .connection
        .query_row(
            "SELECT leases.state FROM leases JOIN attempts ON attempts.id = leases.attempt_id
                 WHERE attempts.job_id = ?1",
            [job_id.entity_uuid().to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(lease, "released");
}

#[test]
fn uncertain_settlement_retains_lease_and_is_terminal() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    let job_spec = spec(temp.path());
    let hash = normalized_payload_hash(&job_spec).unwrap();
    let submitted = store.submit(Uuid::now_v7(), &hash, &job_spec).unwrap();
    let prepared = store
        .prepare_job(submitted.receipt.job_id)
        .unwrap()
        .unwrap();
    store.mark_started(&prepared, 1234, "exe-hash").unwrap();
    assert_eq!(store.managed_containment_candidates().unwrap().len(), 1);
    store
        .mark_uncertain(&prepared, None, "interrupted")
        .unwrap();
    assert!(store.managed_containment_candidates().unwrap().is_empty());
    let (containment, lease): (String, String) = store
        .connection
        .query_row(
            "SELECT containments.state, leases.state
                 FROM containments
                 JOIN invocations ON invocations.id = containments.invocation_id
                 JOIN attempts ON attempts.id = invocations.attempt_id
                 JOIN leases ON leases.attempt_id = attempts.id
                 WHERE attempts.job_id = ?1",
            [prepared.job_id.entity_uuid().to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(containment, "uncertain");
    assert_eq!(lease, "granted");
    assert!(matches!(
        store.mark_finished(&prepared, None, JobOutcome::Failed, "start_failed"),
        Err(StoreError::InvalidState(_))
    ));
}

#[test]
fn logs_publish_only_flushed_committed_prefix() {
    use std::io::Write;

    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    let spec = spec(temp.path());
    let hash = normalized_payload_hash(&spec).unwrap();
    let submitted = store.submit(Uuid::now_v7(), &hash, &spec).unwrap();
    let prepared = store
        .prepare_job(submitted.receipt.job_id)
        .unwrap()
        .unwrap();
    let mut output = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&prepared.stdout_path)
        .unwrap();
    output.write_all(b"committed-tail").unwrap();
    output.sync_data().unwrap();
    store
        .commit_log_offset(prepared.job_id, LogStream::Stdout, 9)
        .unwrap();
    let chunk = store
        .logs(prepared.job_id, LogStream::Stdout, 0, 1024)
        .unwrap();
    assert_eq!(chunk.bytes, b"committed");
    assert_eq!(chunk.next_offset, 9);

    drop(output);
    std::fs::remove_file(&prepared.stdout_path).unwrap();
    let gap = store
        .logs(prepared.job_id, LogStream::Stdout, 0, 1024)
        .unwrap();
    assert!(gap.gap.is_some());
    assert!(gap.bytes.is_empty());
}

#[test]
fn diagnostic_tail_io_failure_cannot_block_invocation_resolution() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    let job_spec = spec(temp.path());
    let hash = normalized_payload_hash(&job_spec).unwrap();
    let receipt = store
        .submit(Uuid::now_v7(), &hash, &job_spec)
        .unwrap()
        .receipt;
    let prepared = store.prepare_job(receipt.job_id).unwrap().unwrap();
    std::fs::create_dir(&prepared.stdout_path).unwrap();

    store
        .mark_invocation_resolved(&prepared, Some(0), None)
        .unwrap();
    let snapshot = store.status(receipt.job_id).unwrap();
    assert_eq!(
        snapshot.attempts[0].invocations[0].state,
        InvocationState::Resolved
    );
    assert!(
        snapshot.attempts[0].invocations[0]
            .stdout_tail
            .contains("tail unavailable")
    );
}

#[test]
fn startup_resumes_durable_received_submission() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StorePaths::new(temp.path().to_path_buf());
    let job_spec = spec(temp.path());
    let hash = normalized_payload_hash(&job_spec).unwrap();
    let key = Uuid::now_v7();
    {
        let store = Store::open(paths).unwrap();
        let submission_id = SubmissionId::new(store.store_uuid);
        store
            .connection
            .execute(
                "INSERT INTO submissions(
                        id, scope, idempotency_key, payload_hash, state, spec_json, created_ms
                     ) VALUES (?1, 'unmanaged', ?2, ?3, 'received', ?4, ?5)",
                params![
                    submission_id.entity_uuid().to_string(),
                    key.to_string(),
                    hash,
                    serde_json::to_string(&job_spec).unwrap(),
                    now_millis(),
                ],
            )
            .unwrap();
    }
    let store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    assert!(matches!(
        store.recover_submission(key, &hash).unwrap(),
        RecoveryResult::Accepted(_)
    ));
    assert_eq!(store.pending_jobs().unwrap().len(), 1);
}

#[test]
fn schema_epoch_mismatch_resets_database_and_preserves_other_files() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StorePaths::new(temp.path().to_path_buf());
    let mut old_store = Store::open_with_capacities(paths, capacities()).unwrap();
    let old_uuid = old_store.store_uuid;
    let job_spec = spec(temp.path());
    let hash = normalized_payload_hash(&job_spec).unwrap();
    let accepted_key = Uuid::now_v7();
    let old_job_id = old_store
        .submit(accepted_key, &hash, &job_spec)
        .unwrap()
        .receipt
        .job_id;
    let received_key = Uuid::now_v7();
    old_store
        .connection
        .execute(
            "INSERT INTO submissions(
                    id, scope, idempotency_key, payload_hash, state, spec_json, kind, created_ms
                 ) VALUES (?1, 'unmanaged', ?2, ?3, 'received', ?4, 'job', ?5)",
            params![
                Uuid::now_v7().to_string(),
                received_key.to_string(),
                hash,
                serde_json::to_string(&job_spec).unwrap(),
                now_millis(),
            ],
        )
        .unwrap();
    old_store
        .connection
        .execute_batch(
            "CREATE TABLE obsolete_rows(value TEXT NOT NULL);
                 INSERT INTO obsolete_rows(value) VALUES ('must not survive');
                 UPDATE meta SET value = 'obsolete-schema' WHERE key = 'schema_epoch';",
        )
        .unwrap();
    drop(old_store);

    let paths = StorePaths::new(temp.path().to_path_buf());
    let log_marker = paths.logs.join("orphaned.log");
    std::fs::write(&log_marker, b"preserve me").unwrap();
    let config = HostConfig {
        resources: capacities(),
        profiles: Default::default(),
        impact_incompatibilities: Default::default(),
    };
    std::fs::write(&paths.config, serde_json::to_vec(&config).unwrap()).unwrap();
    let store = Store::open(paths).unwrap();
    assert_ne!(store.store_uuid, old_uuid);
    assert_eq!(std::fs::read(&log_marker).unwrap(), b"preserve me");
    assert_eq!(
        std::fs::read(&store.paths.config).unwrap(),
        serde_json::to_vec(&config).unwrap()
    );
    let epoch: String = store
        .connection
        .query_row(
            "SELECT value FROM meta WHERE key = 'schema_epoch'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(epoch, STORE_SCHEMA_EPOCH);
    let obsolete_exists: bool = store
        .connection
        .query_row(
            "SELECT EXISTS(
                    SELECT 1 FROM sqlite_master
                    WHERE type = 'table' AND name = 'obsolete_rows'
                 )",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(!obsolete_exists, "reset must not import old rows or tables");
    for table in ["jobs", "submissions"] {
        let count: u64 = store
            .connection
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0, "reset must not import old {table}");
    }
    assert!(store.pending_jobs().unwrap().is_empty());
    assert!(matches!(
        store.recover_submission(accepted_key, &hash).unwrap(),
        RecoveryResult::Unknown
    ));
    assert!(matches!(
        store.recover_submission(received_key, &hash).unwrap(),
        RecoveryResult::Unknown
    ));
    assert!(matches!(
        store.status(old_job_id),
        Err(StoreError::NotFound(message)) if message.contains("foreign durable ID")
    ));
}

#[test]
fn damaged_schema_and_identity_each_reset_the_whole_database() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StorePaths::new(temp.path().to_path_buf());
    let store = Store::open(paths).unwrap();
    let first_uuid = store.store_uuid;
    store.connection.execute("DROP TABLE batches", []).unwrap();
    drop(store);

    let store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    assert_ne!(store.store_uuid, first_uuid);
    let second_uuid = store.store_uuid;
    store
        .connection
        .execute("DELETE FROM meta WHERE key = 'store_uuid'", [])
        .unwrap();
    drop(store);

    let store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    assert_ne!(store.store_uuid, second_uuid);
}

#[test]
fn corrupt_or_empty_database_is_replaced_with_current_schema() {
    let corrupt = tempfile::tempdir().unwrap();
    let corrupt_paths = StorePaths::new(corrupt.path().to_path_buf());
    corrupt_paths.ensure().unwrap();
    std::fs::write(&corrupt_paths.database, b"not a sqlite database").unwrap();
    let corrupt_store = Store::open(corrupt_paths).unwrap();
    assert!(schema_is_current(&corrupt_store.connection).unwrap());

    let temp = tempfile::tempdir().unwrap();
    let paths = StorePaths::new(temp.path().to_path_buf());
    paths.ensure().unwrap();
    File::create(&paths.database).unwrap();

    let store = Store::open(paths).unwrap();
    let stored: String = store
        .connection
        .query_row(
            "SELECT value FROM meta WHERE key = 'store_uuid'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(Uuid::parse_str(&stored).unwrap(), store.store_uuid);
}

#[test]
fn corruption_discovered_during_recovery_resets_once() {
    use std::io::Write as _;

    let temp = tempfile::tempdir().unwrap();
    let paths = StorePaths::new(temp.path().to_path_buf());
    let mut store = Store::open(paths).unwrap();
    let old_uuid = store.store_uuid;
    let job_spec = spec(temp.path());
    let hash = normalized_payload_hash(&job_spec).unwrap();
    store.submit(Uuid::now_v7(), &hash, &job_spec).unwrap();
    drop(store);

    let paths = StorePaths::new(temp.path().to_path_buf());
    let connection = Connection::open(&paths.database).unwrap();
    let page_size: u64 = connection
        .query_row("PRAGMA page_size", [], |row| row.get(0))
        .unwrap();
    let jobs_root_page: u64 = connection
        .query_row(
            "SELECT rootpage FROM sqlite_master WHERE type = 'table' AND name = 'jobs'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    drop(connection);
    let mut database = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&paths.database)
        .unwrap();
    database
        .seek(SeekFrom::Start((jobs_root_page - 1) * page_size))
        .unwrap();
    database.write_all(&[0xff; 128]).unwrap();
    database.sync_all().unwrap();
    drop(database);

    let reopened = Store::open(paths).unwrap();
    assert_ne!(reopened.store_uuid, old_uuid);
    assert!(reopened.pending_jobs().unwrap().is_empty());
}

#[test]
fn only_corruption_errors_authorize_destructive_reset() {
    fn sqlite_error(code: i32) -> StoreError {
        StoreError::Sqlite(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(code),
            None,
        ))
    }

    assert!(matches!(
        schema_probe_error(sqlite_error(rusqlite::ffi::SQLITE_BUSY)),
        Err(StoreError::Sqlite(_))
    ));
    assert!(matches!(
        schema_probe_error(sqlite_error(rusqlite::ffi::SQLITE_IOERR)),
        Err(StoreError::Sqlite(_))
    ));
    assert!(!schema_probe_error(sqlite_error(rusqlite::ffi::SQLITE_CORRUPT)).unwrap());
    assert!(!schema_probe_error(sqlite_error(rusqlite::ffi::SQLITE_NOTADB)).unwrap());
}

#[test]
fn current_schema_reopens_without_changing_store_identity() {
    let temp = tempfile::tempdir().unwrap();
    let store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    let store_uuid = store.store_uuid;
    drop(store);

    let reopened = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    assert_eq!(reopened.store_uuid, store_uuid);
}
