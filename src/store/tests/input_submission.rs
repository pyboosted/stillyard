use super::*;

#[test]
fn staged_stdin_is_pre_received_immutable_and_idempotent() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StorePaths::new(temp.path().to_path_buf());
    let mut store = Store::open_with_capacities(paths, capacities()).unwrap();
    let bytes = (0..90_000)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let upload_id = Uuid::now_v7();
    let input = StagedInputRef {
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        length: bytes.len() as u64,
    };
    store
        .stage_begin(upload_id, &input.sha256, input.length)
        .unwrap();
    let midpoint = 41_000;
    assert_eq!(
        store.stage_chunk(upload_id, 0, &bytes[..midpoint]).unwrap(),
        midpoint as u64
    );
    let submissions: u64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM submissions", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        submissions, 0,
        "partial upload must not create a Submission"
    );
    store
        .stage_chunk(upload_id, midpoint as u64, &bytes[midpoint..])
        .unwrap();
    assert_eq!(store.stage_commit(upload_id).unwrap(), input);

    let source = temp.path().join("client-source.bin");
    let mut job = spec(temp.path());
    job.stdin = StdinSpec::File { path: source };
    let key = Uuid::now_v7();
    let hash = normalized_payload_hash_with_input(&job, Some(&input)).unwrap();
    let accepted = store
        .submit_with_stdin(key, &hash, &job, Some(&input))
        .unwrap();
    let prepared = store.prepare_job(accepted.receipt.job_id).unwrap().unwrap();
    assert_eq!(prepared.stdin, Some(input.clone()));
    assert_eq!(
        std::fs::read(prepared.stdin_path.as_ref().unwrap()).unwrap(),
        bytes
    );
    assert!(
        std::fs::metadata(prepared.stdin_path.unwrap())
            .unwrap()
            .permissions()
            .readonly(),
        "published stdin blob must be immutable to ordinary writers"
    );

    let changed = stage_bytes(&store, b"different immutable input");
    let changed_hash = normalized_payload_hash_with_input(&job, Some(&changed)).unwrap();
    assert!(matches!(
        store.submit_with_stdin(key, &changed_hash, &job, Some(&changed)),
        Err(StoreError::IdempotencyConflict)
    ));
}

#[test]
fn corrupt_staged_input_rejects_before_received() {
    let temp = tempfile::tempdir().unwrap();
    let mut store =
        Store::open_with_capacities(StorePaths::new(temp.path().to_path_buf()), capacities())
            .unwrap();
    let input = stage_bytes(&store, b"trusted");
    let blob = store.paths.blob_path(&input.sha256);
    let mut permissions = std::fs::metadata(&blob).unwrap().permissions();
    make_file_writable(&blob, &mut permissions).unwrap();
    std::fs::write(&blob, b"altered").unwrap();
    let mut job = spec(temp.path());
    job.stdin = StdinSpec::File {
        path: temp.path().join("client-source.bin"),
    };
    let hash = normalized_payload_hash_with_input(&job, Some(&input)).unwrap();
    assert!(matches!(
        store.submit_with_stdin(Uuid::now_v7(), &hash, &job, Some(&input)),
        Err(StoreError::InvalidSpec(_))
    ));
    let submissions: u64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM submissions", [], |row| row.get(0))
        .unwrap();
    assert_eq!(submissions, 0);
}

#[test]
fn restart_collects_partial_upload_without_submission() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StorePaths::new(temp.path().to_path_buf());
    let key = Uuid::now_v7();
    {
        let store = Store::open_with_capacities(paths.clone(), capacities()).unwrap();
        let bytes = b"never committed";
        let hash = format!("{:x}", Sha256::digest(bytes));
        let upload_id = Uuid::now_v7();
        store
            .stage_begin(upload_id, &hash, bytes.len() as u64)
            .unwrap();
        store.stage_chunk(upload_id, 0, bytes).unwrap();
        assert!(matches!(
            store.recover_submission(key, &hash).unwrap(),
            RecoveryResult::Unknown
        ));
    }
    let store = Store::open_with_capacities(paths, capacities()).unwrap();
    assert_eq!(std::fs::read_dir(&store.paths.uploads).unwrap().count(), 0);
    assert_eq!(std::fs::read_dir(&store.paths.blobs).unwrap().count(), 0);
    let submissions: u64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM submissions", [], |row| row.get(0))
        .unwrap();
    assert_eq!(submissions, 0);
}

#[test]
fn partial_batch_input_map_is_atomic() {
    let temp = tempfile::tempdir().unwrap();
    let mut store =
        Store::open_with_capacities(StorePaths::new(temp.path().to_path_buf()), capacities())
            .unwrap();
    let mut first = spec(temp.path());
    first.stdin = StdinSpec::File {
        path: temp.path().join("first.in"),
    };
    let mut second = spec(temp.path());
    second.stdin = StdinSpec::File {
        path: temp.path().join("second.in"),
    };
    let batch = BatchSpec {
        spec_version: SPEC_VERSION,
        jobs: vec![
            member("first", first, vec![]),
            member("second", second, vec![]),
        ],
    };
    let stdins = [("first".to_owned(), stage_bytes(&store, b"first"))].into();
    let hash = normalized_batch_payload_hash_with_inputs(&batch, &stdins).unwrap();
    assert!(matches!(
        store.submit_batch_with_stdins(Uuid::now_v7(), &hash, &batch, &stdins),
        Err(StoreError::InvalidSpec(_))
    ));
    for table in ["submissions", "batches", "jobs"] {
        let count: u64 = store
            .connection
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0, "partial Batch stdin must create no {table}");
    }
}

#[test]
fn received_batch_revalidates_staged_inputs_before_acceptance() {
    let temp = tempfile::tempdir().unwrap();
    let mut store =
        Store::open_with_capacities(StorePaths::new(temp.path().to_path_buf()), capacities())
            .unwrap();
    let mut job = spec(temp.path());
    job.stdin = StdinSpec::File {
        path: temp.path().join("member.in"),
    };
    let batch = BatchSpec {
        spec_version: SPEC_VERSION,
        jobs: vec![member("member", job, vec![])],
    };
    let input = stage_bytes(&store, b"trusted");
    let stdins = [("member".to_owned(), input.clone())].into();
    let hash = normalized_batch_payload_hash_with_inputs(&batch, &stdins).unwrap();
    let key = Uuid::now_v7();
    let submission_id = SubmissionId::new(store.store_uuid);
    store
        .connection
        .execute(
            "INSERT INTO submissions(
                    id, scope, idempotency_key, payload_hash, state, spec_json, stdin_json,
                    kind, created_ms
                 ) VALUES (?1, 'unmanaged', ?2, ?3, 'received', ?4, ?5, 'batch', ?6)",
            params![
                submission_id.entity_uuid().to_string(),
                key.to_string(),
                hash,
                serde_json::to_string(&batch).unwrap(),
                serde_json::to_string(&stdins).unwrap(),
                now_millis(),
            ],
        )
        .unwrap();
    let blob = store.paths.blob_path(&input.sha256);
    let mut permissions = std::fs::metadata(&blob).unwrap().permissions();
    make_file_writable(&blob, &mut permissions).unwrap();
    std::fs::write(&blob, b"altered").unwrap();

    store.resume_received().unwrap();
    assert!(matches!(
        store.recover_submission(key, &hash).unwrap(),
        RecoveryResult::Rejected { .. }
    ));
    let jobs: u64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM jobs", [], |row| row.get(0))
        .unwrap();
    assert_eq!(jobs, 0, "corrupt staged Batch input must never create Jobs");
}

#[test]
fn profile_expands_at_acceptance_and_enforces_precedence() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StorePaths::new(temp.path().to_path_buf());
    let config = HostConfig {
        resources: capacities(),
        impact_incompatibilities: Default::default(),
        profiles: [(
            "codex".to_owned(),
            EnvironmentProfile {
                set: [("PATH".to_owned(), r"C:\Tools".to_owned())].into(),
                unset: vec!["ANTHROPIC_API_KEY".into()],
                locked_set: [("CODEX_HOME".to_owned(), r"C:\Accounts\codex2".to_owned())].into(),
                locked_unset: vec!["XAI_API_KEY".into()],
            },
        )]
        .into(),
    };
    std::fs::write(&paths.config, serde_json::to_vec(&config).unwrap()).unwrap();
    let mut store = Store::open(paths).unwrap();
    let mut job = spec(temp.path());
    job.environment.profile = Some("codex".into());
    job.environment
        .set
        .insert("ANTHROPIC_API_KEY".into(), "must-not-leak".into());
    job.environment.set.insert("ROUND".into(), "2".into());
    let hash = normalized_payload_hash(&job).unwrap();
    let accepted = store.submit(Uuid::now_v7(), &hash, &job).unwrap();
    let effective = store
        .status(accepted.receipt.job_id)
        .unwrap()
        .spec
        .environment;
    assert_eq!(effective.profile.as_deref(), Some("codex"));
    assert_eq!(effective.set.get("PATH").unwrap(), r"C:\Tools");
    assert_eq!(
        effective.set.get("CODEX_HOME").unwrap(),
        r"C:\Accounts\codex2"
    );
    assert_eq!(effective.set.get("ROUND").unwrap(), "2");
    assert!(!effective.set.contains_key("ANTHROPIC_API_KEY"));
    assert!(
        effective
            .unset
            .iter()
            .any(|name| name == "ANTHROPIC_API_KEY")
    );
    assert!(effective.unset.iter().any(|name| name == "XAI_API_KEY"));

    let mut override_locked = job;
    override_locked
        .environment
        .set
        .insert("CODEX_HOME".into(), "wrong".into());
    let hash = normalized_payload_hash(&override_locked).unwrap();
    assert!(matches!(
        store.submit(Uuid::now_v7(), &hash, &override_locked),
        Err(StoreError::Rejected(_))
    ));
    let jobs: u64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM jobs", [], |row| row.get(0))
        .unwrap();
    assert_eq!(jobs, 1, "locked override must never create a Job");
}

#[test]
fn batch_is_atomic_and_dependencies_use_final_outcomes() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StorePaths::new(temp.path().to_path_buf());
    let mut store = Store::open_with_capacities(paths, capacities()).unwrap();
    let mut invalid = BatchSpec {
        spec_version: SPEC_VERSION,
        jobs: vec![member(
            "only",
            spec(temp.path()),
            vec![DependencySpec {
                job: "missing".into(),
                on: DependencyKind::Success,
            }],
        )],
    };
    let hash = normalized_batch_payload_hash(&invalid).unwrap();
    assert!(matches!(
        store.submit_batch(Uuid::now_v7(), &hash, &invalid),
        Err(StoreError::InvalidSpec(_))
    ));
    let count: u64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM jobs", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 0, "invalid atomic batch must create no members");

    invalid.jobs = vec![
        member("root", spec(temp.path()), vec![]),
        member(
            "successor",
            spec(temp.path()),
            vec![DependencySpec {
                job: "root".into(),
                on: DependencyKind::Success,
            }],
        ),
        member(
            "finally",
            spec(temp.path()),
            vec![DependencySpec {
                job: "root".into(),
                on: DependencyKind::Terminal,
            }],
        ),
    ];
    let hash = normalized_batch_payload_hash(&invalid).unwrap();
    let receipt = store
        .submit_batch(Uuid::now_v7(), &hash, &invalid)
        .unwrap()
        .receipt;
    assert_eq!(receipt.jobs.len(), 3);
    assert_eq!(
        receipt.jobs[1].receipt.blockers[0].code,
        "dependency_pending"
    );
    let root = store.prepare_next_job().unwrap().unwrap();
    assert_eq!(root.job_id, receipt.jobs[0].receipt.job_id);
    store
        .mark_finished(&root, Some(1), JobOutcome::Failed, "process_failed")
        .unwrap();
    let finally = store.prepare_next_job().unwrap().unwrap();
    assert_eq!(finally.job_id, receipt.jobs[2].receipt.job_id);
    let skipped = store.status(receipt.jobs[1].receipt.job_id).unwrap();
    assert_eq!(skipped.outcome, Some(JobOutcome::Skipped));
}

#[test]
fn reverse_order_skip_closure_reaches_terminal_state() {
    let temp = tempfile::tempdir().unwrap();
    let mut store =
        Store::open_with_capacities(StorePaths::new(temp.path().to_path_buf()), capacities())
            .unwrap();
    let batch = BatchSpec {
        spec_version: SPEC_VERSION,
        jobs: vec![
            member(
                "c",
                spec(temp.path()),
                vec![DependencySpec {
                    job: "b".into(),
                    on: DependencyKind::Success,
                }],
            ),
            member(
                "b",
                spec(temp.path()),
                vec![DependencySpec {
                    job: "a".into(),
                    on: DependencyKind::Success,
                }],
            ),
            member("a", spec(temp.path()), vec![]),
        ],
    };
    let hash = normalized_batch_payload_hash(&batch).unwrap();
    let receipt = store
        .submit_batch(Uuid::now_v7(), &hash, &batch)
        .unwrap()
        .receipt;
    let root = store.prepare_next_job().unwrap().unwrap();
    assert_eq!(root.job_id, receipt.jobs[2].receipt.job_id);
    store
        .mark_finished(&root, Some(1), JobOutcome::Failed, "process_failed")
        .unwrap();

    let progress = store.prepare_next_job_with_progress().unwrap();
    assert!(progress.job.is_none());
    assert!(
        progress.state_changed,
        "skip-only passes must notify waiters"
    );
    assert_eq!(
        store
            .status(receipt.jobs[1].receipt.job_id)
            .unwrap()
            .outcome,
        Some(JobOutcome::Skipped)
    );
    assert_eq!(
        store
            .status(receipt.jobs[0].receipt.job_id)
            .unwrap()
            .outcome,
        Some(JobOutcome::Skipped)
    );
}

#[test]
fn sqlite_failure_rolls_back_every_batch_member() {
    let temp = tempfile::tempdir().unwrap();
    let mut store =
        Store::open_with_capacities(StorePaths::new(temp.path().to_path_buf()), capacities())
            .unwrap();
    store
        .connection
        .execute_batch(
            "CREATE TRIGGER fail_second_batch_member
                 BEFORE INSERT ON jobs WHEN NEW.batch_member = 'second'
                 BEGIN SELECT RAISE(ABORT, 'forced batch fault'); END;",
        )
        .unwrap();
    let batch = BatchSpec {
        spec_version: SPEC_VERSION,
        jobs: vec![
            member("first", spec(temp.path()), vec![]),
            member("second", spec(temp.path()), vec![]),
        ],
    };
    let key = Uuid::now_v7();
    let hash = normalized_batch_payload_hash(&batch).unwrap();
    assert!(matches!(
        store.submit_batch(key, &hash, &batch),
        Err(StoreError::Sqlite(_))
    ));
    for table in ["batches", "jobs", "dependencies"] {
        let count: u64 = store
            .connection
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0, "{table} must roll back atomically");
    }
    let state: String = store
        .connection
        .query_row(
            "SELECT state FROM submissions WHERE idempotency_key = ?1",
            [key.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(state, "received");

    store
        .connection
        .execute_batch("DROP TRIGGER fail_second_batch_member")
        .unwrap();
    store.resume_received().unwrap();
    let recovered = store.recover_submission(key, &hash).unwrap();
    assert!(matches!(recovered, RecoveryResult::AcceptedBatch(_)));
}

#[test]
fn complete_leases_serialize_conflicts_but_allow_orthogonal_work() {
    let temp = tempfile::tempdir().unwrap();
    let mut store =
        Store::open_with_capacities(StorePaths::new(temp.path().to_path_buf()), capacities())
            .unwrap();
    let mut cpu = spec(temp.path());
    cpu.resources.cpu_units = Some(3);
    cpu.resources.ram_mb = Some(8_000);
    cpu.expected_duration_seconds = Some(30);
    let mut blocked = spec(temp.path());
    blocked.resources.cpu_units = Some(2);
    blocked.resources.ram_mb = Some(1_000);
    let mut gpu = spec(temp.path());
    gpu.resources.gpu_slots = Some(1);
    let mut ram = spec(temp.path());
    ram.resources.ram_mb = Some(8_000);
    let batch = BatchSpec {
        spec_version: SPEC_VERSION,
        jobs: vec![
            member("cpu", cpu, vec![]),
            member("blocked", blocked, vec![]),
            member("gpu", gpu, vec![]),
            member("ram", ram, vec![]),
        ],
    };
    let hash = normalized_batch_payload_hash(&batch).unwrap();
    let receipt = store
        .submit_batch(Uuid::now_v7(), &hash, &batch)
        .unwrap()
        .receipt;
    assert!(
        receipt.jobs[1]
            .receipt
            .blockers
            .iter()
            .any(|blocker| blocker.code == "resource_busy"),
        "receipt must account for an earlier compatible queue reservation"
    );
    assert!(receipt.jobs[2].receipt.blockers.is_empty());
    assert!(
        receipt.jobs[3].receipt.blockers.is_empty(),
        "a non-fitting earlier claim must not reserve only its RAM portion"
    );
    let cpu = store.prepare_next_job().unwrap().unwrap();
    assert_eq!(cpu.job_id, receipt.jobs[0].receipt.job_id);
    let gpu = store.prepare_next_job().unwrap().unwrap();
    assert_eq!(
        gpu.job_id, receipt.jobs[2].receipt.job_id,
        "a partially fitting CPU claim must not reserve RAM or block orthogonal GPU work"
    );
    let ram = store.prepare_next_job().unwrap().unwrap();
    assert_eq!(ram.job_id, receipt.jobs[3].receipt.job_id);
    let blocked = store.status(receipt.jobs[1].receipt.job_id).unwrap();
    assert!(
        blocked
            .blockers
            .iter()
            .any(|item| item.code == "resource_busy")
    );
    store
        .mark_finished(&cpu, Some(0), JobOutcome::Succeeded, "succeeded")
        .unwrap();
    let admitted = store.prepare_next_job().unwrap().unwrap();
    assert_eq!(admitted.job_id, receipt.jobs[1].receipt.job_id);
}

#[test]
fn receipt_reports_rank_blocker_and_honest_estimate() {
    let temp = tempfile::tempdir().unwrap();
    let mut store =
        Store::open_with_capacities(StorePaths::new(temp.path().to_path_buf()), capacities())
            .unwrap();
    let mut first = spec(temp.path());
    first.resources.cargo_slots = Some(1);
    first.expected_duration_seconds = Some(60);
    let hash = normalized_payload_hash(&first).unwrap();
    let first = store.submit(Uuid::now_v7(), &hash, &first).unwrap();
    let running = store.prepare_next_job().unwrap().unwrap();
    assert_eq!(running.job_id, first.receipt.job_id);

    let mut waiting = spec(temp.path());
    waiting.resources.cargo_slots = Some(1);
    let hash = normalized_payload_hash(&waiting).unwrap();
    let waiting = store.submit(Uuid::now_v7(), &hash, &waiting).unwrap();
    assert_eq!(waiting.receipt.queue_rank, Some(1));
    assert!(waiting.receipt.blockers.iter().any(|blocker| {
        blocker.code == "resource_busy" && blocker.detail.contains("cargo_slots")
    }));
    assert_eq!(
        waiting.receipt.estimate.confidence,
        EstimateConfidence::Estimated
    );
    assert!(waiting.receipt.estimate.start_in_millis.is_some());
}

#[test]
fn missing_path_fence_identity_survives_later_creation() {
    let temp = tempfile::tempdir().unwrap();
    let mut store =
        Store::open_with_capacities(StorePaths::new(temp.path().to_path_buf()), capacities())
            .unwrap();
    let fenced = temp.path().join("future-slot");
    let mut first = spec(temp.path());
    first.resources.exclusive_fences = vec![fenced.to_string_lossy().into_owned()];
    let fence_spec = first.clone();
    let first_hash = normalized_payload_hash(&first).unwrap();
    let first = store
        .submit(Uuid::now_v7(), &first_hash, &first)
        .unwrap()
        .receipt;
    let second_hash = normalized_payload_hash(&fence_spec).unwrap();
    let second = store
        .submit(Uuid::now_v7(), &second_hash, &fence_spec)
        .unwrap()
        .receipt;
    std::fs::create_dir(&fenced).unwrap();
    let admitted = store.prepare_next_job().unwrap().unwrap();
    assert_eq!(admitted.job_id, first.job_id);
    let after_creation_hash = normalized_payload_hash(&fence_spec).unwrap();
    let after_creation = store
        .submit(Uuid::now_v7(), &after_creation_hash, &fence_spec)
        .unwrap()
        .receipt;
    let snapshot = store.status(second.job_id).unwrap();
    assert!(
        snapshot
            .blockers
            .iter()
            .any(|blocker| blocker.code == "path_fence_busy")
    );
    assert!(
        store
            .status(after_creation.job_id)
            .unwrap()
            .blockers
            .iter()
            .any(|blocker| blocker.code == "path_fence_busy"),
        "creating the leaf between acceptances must not evade the incumbent fence"
    );
}

#[test]
fn dependency_outside_fifo_prefix_is_unknown() {
    let temp = tempfile::tempdir().unwrap();
    let mut store =
        Store::open_with_capacities(StorePaths::new(temp.path().to_path_buf()), capacities())
            .unwrap();
    let mut short = spec(temp.path());
    short.expected_duration_seconds = Some(5);
    let mut long = spec(temp.path());
    long.expected_duration_seconds = Some(3_600);
    let batch = BatchSpec {
        spec_version: SPEC_VERSION,
        jobs: vec![
            member("short", short, vec![]),
            member(
                "dependent",
                spec(temp.path()),
                vec![DependencySpec {
                    job: "long".into(),
                    on: DependencyKind::Success,
                }],
            ),
            member("long", long, vec![]),
        ],
    };
    let hash = normalized_batch_payload_hash(&batch).unwrap();
    let receipt = store
        .submit_batch(Uuid::now_v7(), &hash, &batch)
        .unwrap()
        .receipt;
    assert_eq!(
        receipt.jobs[1].receipt.estimate.confidence,
        EstimateConfidence::Unknown
    );
    assert_eq!(receipt.jobs[1].receipt.estimate.start_in_millis, None);
}
