use super::*;

fn start_managed_parent(store: &mut Store, root: &Path, enabled: bool) -> PreparedJob {
    let mut parent_spec = spec(root);
    parent_spec.allow_child_submissions = enabled;
    start_managed_parent_with_spec(store, parent_spec)
}

fn start_managed_parent_with_spec(store: &mut Store, parent_spec: JobSpec) -> PreparedJob {
    let hash = normalized_payload_hash(&parent_spec).unwrap();
    let receipt = store
        .submit(Uuid::now_v7(), &hash, &parent_spec)
        .unwrap()
        .receipt;
    let prepared = store.prepare_job(receipt.job_id).unwrap().unwrap();
    store.mark_started(&prepared, 4242, "parent-image").unwrap();
    prepared
}

fn start_managed_child(
    store: &mut Store,
    parent: SubmissionScope,
    child_spec: &JobSpec,
) -> PreparedJob {
    let hash = normalized_payload_hash(child_spec).unwrap();
    let receipt = store
        .submit_with_stdin_scoped(parent, Uuid::now_v7(), &hash, child_spec, None)
        .unwrap()
        .receipt;
    let prepared = store.prepare_job(receipt.job_id).unwrap().unwrap();
    store.mark_started(&prepared, 4343, "child-image").unwrap();
    prepared
}

fn scope_for(prepared: &PreparedJob) -> SubmissionScope {
    SubmissionScope::Managed(ManagedParent {
        job_id: prepared.job_id,
        attempt_id: prepared.attempt_id,
        invocation_id: prepared.invocation_id,
    })
}

#[test]
fn managed_not_received_is_provable_only_for_the_live_current_parent() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    let parent = start_managed_parent(&mut store, temp.path(), true);
    let scope = scope_for(&parent);
    let key = Uuid::now_v7();

    assert_eq!(
        store
            .recover_submission_scoped(scope, key, "child-payload")
            .unwrap(),
        RecoveryResult::NotReceived
    );

    store
        .mark_finished(&parent, Some(0), JobOutcome::Succeeded, "succeeded")
        .unwrap();
    assert_eq!(
        store
            .recover_submission_scoped(scope, key, "child-payload")
            .unwrap(),
        RecoveryResult::Unknown
    );
}

#[test]
fn managed_exact_replay_is_idempotent_and_commits_parentage() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    let parent = start_managed_parent(&mut store, temp.path(), true);
    let scope = scope_for(&parent);
    let child = spec(temp.path());
    let hash = normalized_payload_hash(&child).unwrap();
    let key = Uuid::now_v7();

    assert_eq!(
        store.recover_submission_scoped(scope, key, &hash).unwrap(),
        RecoveryResult::NotReceived
    );
    let first = store
        .submit_with_stdin_scoped(scope, key, &hash, &child, None)
        .unwrap();
    let replay = store
        .submit_with_stdin_scoped(scope, key, &hash, &child, None)
        .unwrap();
    assert_eq!(first.receipt.job_id, replay.receipt.job_id);
    assert_eq!(first.receipt.parent, scope.parent());
    assert_eq!(
        store.status(first.receipt.job_id).unwrap().parent,
        scope.parent()
    );
    assert!(matches!(
        store
            .recover_submission_scoped(scope, key, &hash)
            .unwrap(),
        RecoveryResult::Accepted(receipt) if receipt.job_id == first.receipt.job_id
    ));

    let mut changed = child.clone();
    changed.args.push("different".into());
    let changed_hash = normalized_payload_hash(&changed).unwrap();
    assert!(matches!(
        store.submit_with_stdin_scoped(scope, key, &changed_hash, &changed, None),
        Err(StoreError::IdempotencyConflict)
    ));
}

#[test]
fn managed_combined_wait_rejects_ancestor_scalar_but_detached_submit_survives() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StorePaths::new(temp.path().to_path_buf());
    let mut store = Store::open_with_capacities(paths, capacities()).unwrap();
    let mut parent_spec = spec(temp.path());
    parent_spec.allow_child_submissions = true;
    parent_spec.resources.cargo_slots = Some(1);
    let parent = start_managed_parent_with_spec(&mut store, parent_spec);
    let scope = scope_for(&parent);
    let mut child = spec(temp.path());
    child.resources.cargo_slots = Some(1);
    let hash = normalized_payload_hash(&child).unwrap();
    let wait_key = Uuid::now_v7();

    assert!(matches!(
        store.submit_with_stdin_scoped_for_wait(scope, wait_key, &hash, &child, None, true,),
        Err(StoreError::BlockedByAncestor(_))
    ));
    let (state, wait_intent): (String, bool) = store
        .connection
        .query_row(
            "SELECT state, wait_intent FROM submissions WHERE idempotency_key = ?1",
            [wait_key.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(state, "rejected");
    assert!(wait_intent);
    assert!(matches!(
        store.submit_with_stdin_scoped_for_wait(
            scope,
            wait_key,
            &hash,
            &child,
            None,
            true,
        ),
        Err(StoreError::BlockedByAncestor(detail)) if detail.contains("cargo_slots")
    ));
    assert!(matches!(
        store.recover_submission_scoped(scope, wait_key, &hash).unwrap(),
        RecoveryResult::Rejected { code, detail }
            if code == "blocked_by_ancestor" && detail.contains("cargo_slots")
    ));
    let jobs: u64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM jobs", [], |row| row.get(0))
        .unwrap();
    assert_eq!(jobs, 1, "unsafe combined wait must create no child Job");

    let detached_key = Uuid::now_v7();
    let detached = store
        .submit_with_stdin_scoped_for_wait(scope, detached_key, &hash, &child, None, false)
        .unwrap();
    assert!(matches!(
        store.validate_managed_wait(scope, &[detached.receipt.job_id]),
        Err(StoreError::BlockedByAncestor(_))
    ));
    assert!(
        detached
            .receipt
            .blockers
            .iter()
            .any(|blocker| blocker.code == "resource_busy")
    );
    let replay = store
        .submit_with_stdin_scoped_for_wait(scope, detached_key, &hash, &child, None, true)
        .unwrap();
    assert_eq!(replay.receipt.job_id, detached.receipt.job_id);
    assert!(!replay.should_schedule);
}

#[test]
fn managed_wait_rejects_a_claim_that_exceeds_host_capacity() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StorePaths::new(temp.path().to_path_buf());
    let mut store = Store::open_with_capacities(paths, capacities()).unwrap();
    let mut parent_spec = spec(temp.path());
    parent_spec.allow_child_submissions = true;
    let parent = start_managed_parent_with_spec(&mut store, parent_spec);
    let scope = scope_for(&parent);
    let mut child = spec(temp.path());
    child.resources.cargo_slots = Some(2);
    let hash = normalized_payload_hash(&child).unwrap();

    assert!(matches!(
        store.submit_with_stdin_scoped_for_wait(
            scope,
            Uuid::now_v7(),
            &hash,
            &child,
            None,
            true,
        ),
        Err(StoreError::ManagedWaitRejected { code, detail })
            if code == "resource_capacity" && detail.contains("configured capacity 1")
    ));
    let children: u64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM jobs WHERE parent_job_id = ?1",
            [parent.job_id.entity_uuid().to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(children, 0);
}

#[test]
fn received_wait_intent_survives_resume_and_cannot_accept_an_unsafe_child() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StorePaths::new(temp.path().to_path_buf());
    let mut store = Store::open_with_capacities(paths, capacities()).unwrap();
    let mut parent_spec = spec(temp.path());
    parent_spec.allow_child_submissions = true;
    parent_spec.resources.cargo_slots = Some(1);
    let parent = start_managed_parent_with_spec(&mut store, parent_spec);
    let scope = scope_for(&parent);
    let mut child = spec(temp.path());
    child.resources.cargo_slots = Some(1);
    let hash = normalized_payload_hash(&child).unwrap();
    let key = Uuid::now_v7();
    let submission_id = SubmissionId::new(store.store_uuid);
    let managed = scope.parent().unwrap();
    store
        .connection
        .execute(
            "INSERT INTO submissions(
                    id, scope, idempotency_key, payload_hash, state, spec_json, kind,
                    parent_job_id, parent_attempt_id, parent_invocation_id, wait_intent, created_ms
                 ) VALUES (?1, ?2, ?3, ?4, 'received', ?5, 'job', ?6, ?7, ?8, 1, ?9)",
            params![
                submission_id.entity_uuid().to_string(),
                scope.key(),
                key.to_string(),
                hash,
                serde_json::to_string(&child).unwrap(),
                managed.job_id.entity_uuid().to_string(),
                managed.attempt_id.entity_uuid().to_string(),
                managed.invocation_id.entity_uuid().to_string(),
                now_millis(),
            ],
        )
        .unwrap();

    store.resume_received().unwrap();
    assert!(matches!(
        store.recover_submission_scoped(scope, key, &hash).unwrap(),
        RecoveryResult::Rejected { .. }
    ));
    let children: u64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM jobs WHERE parent_job_id = ?1",
            [managed.job_id.entity_uuid().to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(children, 0);
}

#[test]
fn managed_wait_allows_orthogonal_child_and_checks_the_full_ancestor_chain() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StorePaths::new(temp.path().to_path_buf());
    let mut store = Store::open_with_capacities(paths, capacities()).unwrap();
    let mut grandparent_spec = spec(temp.path());
    grandparent_spec.allow_child_submissions = true;
    grandparent_spec.resources.cargo_slots = Some(1);
    let grandparent = start_managed_parent_with_spec(&mut store, grandparent_spec);

    let mut waiter_spec = spec(temp.path());
    waiter_spec.allow_child_submissions = true;
    let waiter = start_managed_child(&mut store, scope_for(&grandparent), &waiter_spec);
    let waiter_scope = scope_for(&waiter);
    let mut orthogonal = spec(temp.path());
    orthogonal.resources.gpu_slots = Some(1);
    let orthogonal_hash = normalized_payload_hash(&orthogonal).unwrap();
    let accepted = store
        .submit_with_stdin_scoped_for_wait(
            waiter_scope,
            Uuid::now_v7(),
            &orthogonal_hash,
            &orthogonal,
            None,
            true,
        )
        .unwrap();
    store
        .validate_managed_wait(waiter_scope, &[accepted.receipt.job_id])
        .unwrap();

    let mut conflicting = spec(temp.path());
    conflicting.resources.cargo_slots = Some(1);
    let conflicting_hash = normalized_payload_hash(&conflicting).unwrap();
    assert!(matches!(
        store.submit_with_stdin_scoped_for_wait(
            waiter_scope,
            Uuid::now_v7(),
            &conflicting_hash,
            &conflicting,
            None,
            true,
        ),
        Err(StoreError::BlockedByAncestor(detail)) if detail.contains("cargo_slots")
    ));
}

#[test]
fn managed_wait_walks_unfinished_predecessors_and_rejects_self_or_foreign_targets() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StorePaths::new(temp.path().to_path_buf());
    let mut store = Store::open_with_capacities(paths, capacities()).unwrap();
    let mut parent_spec = spec(temp.path());
    parent_spec.allow_child_submissions = true;
    parent_spec.resources.cargo_slots = Some(1);
    let parent = start_managed_parent_with_spec(&mut store, parent_spec);
    let scope = scope_for(&parent);

    let mut predecessor = spec(temp.path());
    predecessor.resources.cargo_slots = Some(1);
    let mut successor = spec(temp.path());
    successor.resources.gpu_slots = Some(1);
    let batch = BatchSpec {
        spec_version: SPEC_VERSION,
        jobs: vec![
            member("predecessor", predecessor, Vec::new()),
            member(
                "successor",
                successor,
                vec![DependencySpec {
                    job: "predecessor".into(),
                    on: DependencyKind::Terminal,
                }],
            ),
        ],
    };
    let hash = normalized_batch_payload_hash(&batch).unwrap();
    let receipt = store
        .submit_batch_with_stdins_scoped(scope, Uuid::now_v7(), &hash, &batch, &Default::default())
        .unwrap()
        .receipt;
    let successor_id = receipt
        .jobs
        .iter()
        .find(|member| member.name == "successor")
        .unwrap()
        .receipt
        .job_id;
    assert!(matches!(
        store.validate_managed_wait(scope, &[successor_id]),
        Err(StoreError::BlockedByAncestor(detail)) if detail.contains("predecessor") && detail.contains("cargo_slots")
    ));

    let foreign_spec = spec(temp.path());
    let foreign_hash = normalized_payload_hash(&foreign_spec).unwrap();
    let foreign = store
        .submit(Uuid::now_v7(), &foreign_hash, &foreign_spec)
        .unwrap();
    assert!(matches!(
        store.validate_managed_wait(scope, &[foreign.receipt.job_id]),
        Err(StoreError::Rejected(_))
    ));

    let direct_child = receipt.jobs[0].receipt.job_id;
    store
        .connection
        .execute(
            "INSERT INTO dependencies(predecessor_id, successor_id, kind)
                 VALUES (?1, ?2, 'terminal')",
            params![
                parent.job_id.entity_uuid().to_string(),
                direct_child.entity_uuid().to_string(),
            ],
        )
        .unwrap();
    assert!(matches!(
        store.validate_managed_wait(scope, &[direct_child]),
        Err(StoreError::BlockedByAncestor(detail)) if detail.contains("waiting Job itself")
    ));
}

#[test]
fn managed_batch_wait_rejects_atomically_when_one_member_conflicts() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StorePaths::new(temp.path().to_path_buf());
    let mut store = Store::open_with_capacities(paths, capacities()).unwrap();
    let mut parent_spec = spec(temp.path());
    parent_spec.allow_child_submissions = true;
    parent_spec.resources.cargo_slots = Some(1);
    let parent = start_managed_parent_with_spec(&mut store, parent_spec);
    let scope = scope_for(&parent);
    let mut safe = spec(temp.path());
    safe.resources.gpu_slots = Some(1);
    let mut blocked = spec(temp.path());
    blocked.resources.cargo_slots = Some(1);
    let batch = BatchSpec {
        spec_version: SPEC_VERSION,
        jobs: vec![
            member("safe", safe, Vec::new()),
            member("blocked", blocked, Vec::new()),
        ],
    };
    let hash = normalized_batch_payload_hash(&batch).unwrap();
    let key = Uuid::now_v7();
    assert!(matches!(
        store.submit_batch_with_stdins_scoped_for_wait(
            scope,
            key,
            &hash,
            &batch,
            &Default::default(),
            true,
        ),
        Err(StoreError::BlockedByAncestor(_))
    ));
    let child_jobs: u64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM jobs WHERE parent_job_id = ?1",
            [parent.job_id.entity_uuid().to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(child_jobs, 0);
    let batches: u64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM batches", [], |row| row.get(0))
        .unwrap();
    assert_eq!(batches, 0);
    let state: String = store
        .connection
        .query_row(
            "SELECT state FROM submissions WHERE idempotency_key = ?1",
            [key.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(state, "rejected");
}

#[test]
fn managed_acceptance_rechecks_parent_and_disabled_primary_never_proves_absence() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    let disabled = start_managed_parent(&mut store, temp.path(), false);
    let disabled_scope = scope_for(&disabled);
    let child = spec(temp.path());
    let hash = normalized_payload_hash(&child).unwrap();
    assert_eq!(
        store
            .recover_submission_scoped(disabled_scope, Uuid::now_v7(), &hash)
            .unwrap(),
        RecoveryResult::Unknown
    );
    assert!(matches!(
        store.submit_with_stdin_scoped(disabled_scope, Uuid::now_v7(), &hash, &child, None,),
        Err(StoreError::Rejected(_))
    ));

    let enabled = start_managed_parent(&mut store, temp.path(), true);
    let enabled_scope = scope_for(&enabled);
    store.mark_root_exited(&enabled, 0).unwrap();
    assert_eq!(
        store
            .recover_submission_scoped(enabled_scope, Uuid::now_v7(), &hash)
            .unwrap(),
        RecoveryResult::Unknown,
        "a live descendant cannot submit after the primary root exited"
    );
    store
        .mark_finished(&enabled, Some(0), JobOutcome::Succeeded, "succeeded")
        .unwrap();
    let key = Uuid::now_v7();
    assert!(matches!(
        store.submit_with_stdin_scoped(enabled_scope, key, &hash, &child, None),
        Err(StoreError::Rejected(_))
    ));
    let retained: u64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM submissions WHERE idempotency_key = ?1",
            [key.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(retained, 0, "late child rejection must create no work");
}

#[test]
fn restart_rejects_managed_received_work_from_the_previous_daemon_generation() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StorePaths::new(temp.path().to_path_buf());
    let mut store = Store::open(paths.clone()).unwrap();
    let previous_generation = store.daemon_generation;
    let parent = start_managed_parent(&mut store, temp.path(), true);
    let scope = scope_for(&parent);
    let child = spec(temp.path());
    let hash = normalized_payload_hash(&child).unwrap();
    let key = Uuid::now_v7();
    let submission_id = SubmissionId::new(store.store_uuid);
    store
        .connection
        .execute(
            "INSERT INTO submissions(
                    id, scope, idempotency_key, payload_hash, state, spec_json, kind,
                    parent_job_id, parent_attempt_id, parent_invocation_id, created_ms
                 ) VALUES (?1, ?2, ?3, ?4, 'received', ?5, 'job', ?6, ?7, ?8, ?9)",
            params![
                submission_id.entity_uuid().to_string(),
                scope.key(),
                key.to_string(),
                hash,
                serde_json::to_string(&child).unwrap(),
                parent.job_id.entity_uuid().to_string(),
                parent.attempt_id.entity_uuid().to_string(),
                parent.invocation_id.entity_uuid().to_string(),
                now_millis(),
            ],
        )
        .unwrap();
    drop(store);

    let reopened = Store::open(paths).unwrap();
    assert_ne!(reopened.daemon_generation, previous_generation);
    let state: String = reopened
        .connection
        .query_row(
            "SELECT state FROM submissions WHERE id = ?1",
            [submission_id.entity_uuid().to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(state, "rejected");
    let child_jobs: u64 = reopened
        .connection
        .query_row(
            "SELECT COUNT(*) FROM jobs WHERE submission_id = ?1",
            [submission_id.entity_uuid().to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(child_jobs, 0, "restart must not accept a late child");
}
