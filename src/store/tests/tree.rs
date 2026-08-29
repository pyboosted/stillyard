use super::*;
use crate::store::database::validate_schema;

fn permissive_policy() -> crate::ChildSubmissionPolicy {
    crate::ChildSubmissionPolicy {
        max_claims: crate::ResourceClaimLimits {
            cpu_units: Some(u32::MAX),
            ram_mb: Some(u64::MAX),
            cargo_slots: Some(u32::MAX),
            gpu_slots: Some(u32::MAX),
            custom: Default::default(),
        },
        allowed_impacts: Vec::new(),
        required_labels: Vec::new(),
        fences: Default::default(),
        allow_observed: true,
        allow_quiet: true,
        allow_delegation: true,
    }
}

fn submit_root(store: &mut Store, mut job: JobSpec, managed: bool) -> (JobId, Option<PreparedJob>) {
    job.child_submission_policy = managed.then(permissive_policy);
    let hash = normalized_payload_hash(&job).unwrap();
    let receipt = store.submit(Uuid::now_v7(), &hash, &job).unwrap().receipt;
    let prepared = managed.then(|| {
        let prepared = store.prepare_job(receipt.job_id).unwrap().unwrap();
        store.mark_started(&prepared, 8100, "tree-parent").unwrap();
        prepared
    });
    (receipt.job_id, prepared)
}

fn submit_child(
    store: &mut Store,
    parent: &PreparedJob,
    mut job: JobSpec,
    managed: bool,
) -> (JobId, Option<PreparedJob>) {
    job.child_submission_policy = managed.then(permissive_policy);
    let hash = normalized_payload_hash(&job).unwrap();
    let scope = SubmissionScope::Managed(ManagedParent {
        job_id: parent.job_id,
        attempt_id: parent.attempt_id,
        invocation_id: parent.invocation_id,
    });
    let receipt = store
        .submit_with_stdin_scoped(scope, Uuid::now_v7(), &hash, &job, None)
        .unwrap()
        .receipt;
    let prepared = managed.then(|| {
        let prepared = store.prepare_job(receipt.job_id).unwrap().unwrap();
        store.mark_started(&prepared, 8200, "tree-child").unwrap();
        prepared
    });
    (receipt.job_id, prepared)
}

#[test]
fn tree_depth_cursor_and_for_job_preserve_the_complete_ancestor_path() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    let (root, parent) = submit_root(&mut store, spec(temp.path()), true);
    let parent = parent.unwrap();
    let (child, child_parent) = submit_child(&mut store, &parent, spec(temp.path()), true);
    let (grandchild, _) =
        submit_child(&mut store, &child_parent.unwrap(), spec(temp.path()), false);

    store
        .connection
        .execute(
            "UPDATE jobs SET state = 'final', outcome = 'succeeded' WHERE id = ?1",
            [root.entity_uuid().to_string()],
        )
        .unwrap();

    let shallow = store
        .tree(&JobSelector::All, None, 16, 16, Some(0))
        .unwrap();
    assert_eq!(shallow.nodes.len(), 1);
    assert_eq!(shallow.nodes[0].summary.job_id, root);
    assert_eq!(
        shallow.nodes[0].family_attention,
        Some(TreeAttentionBucket::Running),
        "an active child keeps its finished parent's family in Running"
    );
    assert!(shallow.nodes[0].descendants_truncated);
    let children = store
        .tree_children(
            shallow.nodes[0].next_children_cursor.as_ref().unwrap(),
            16,
            Some(1),
        )
        .unwrap();
    assert_eq!(
        children
            .nodes
            .iter()
            .map(|node| (node.summary.job_id, node.depth))
            .collect::<Vec<_>>(),
        vec![(child, 1), (grandchild, 2)]
    );

    assert!(matches!(
        store.tree_for_job(grandchild, 2, Some(0)),
        Err(StoreError::InvalidSpec(detail)) if detail.contains("requires 3")
    ));
    let focused = store.tree_for_job(grandchild, 3, Some(0)).unwrap();
    assert_eq!(focused.selected_job_id, Some(grandchild));
    assert_eq!(
        focused
            .nodes
            .iter()
            .map(|node| (node.summary.job_id, node.context_only))
            .collect::<Vec<_>>(),
        vec![(root, true), (child, true), (grandchild, false)]
    );
}

#[test]
fn filtered_tree_retains_only_connecting_ancestors_and_selector_bound_expansion() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    let (root, parent) = submit_root(&mut store, spec(temp.path()), true);
    let parent = parent.unwrap();
    let mut matching = spec(temp.path());
    matching.labels.push(crate::Label {
        key: "project".into(),
        value: "match".into(),
    });
    let (matching, _) = submit_child(&mut store, &parent, matching, false);
    let (unrelated, _) = submit_child(&mut store, &parent, spec(temp.path()), false);
    let selector = JobSelector::Labels {
        labels: vec![crate::Label {
            key: "project".into(),
            value: "match".into(),
        }],
    };

    let page = store.tree(&selector, None, 1, 1, None).unwrap();
    assert_eq!(page.nodes[0].summary.job_id, root);
    assert!(page.nodes[0].context_only);
    let cursor = page.nodes[0].next_children_cursor.clone().unwrap();
    let expanded = store.tree_children(&cursor, 8, None).unwrap();
    assert_eq!(expanded.nodes.len(), 1);
    assert_eq!(expanded.nodes[0].summary.job_id, matching);
    assert_ne!(expanded.nodes[0].summary.job_id, unrelated);

    let mut tampered = cursor;
    tampered.selector_hash.push('0');
    assert!(matches!(
        store.tree_children(&tampered, 8, None),
        Err(StoreError::ViewStale(_))
    ));
}

#[test]
fn depth_cut_before_a_connector_keeps_an_inclusive_child_continuation() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    let mut root_spec = spec(temp.path());
    root_spec.labels.push(crate::Label {
        key: "project".into(),
        value: "match".into(),
    });
    let (root, root_parent) = submit_root(&mut store, root_spec, true);
    let (branch, branch_parent) =
        submit_child(&mut store, &root_parent.unwrap(), spec(temp.path()), true);
    let branch_parent = branch_parent.unwrap();
    let (depth_cut, _) = submit_child(&mut store, &branch_parent, spec(temp.path()), false);
    let (connector, connector_parent) =
        submit_child(&mut store, &branch_parent, spec(temp.path()), true);
    let mut anchor_spec = spec(temp.path());
    anchor_spec.labels.push(crate::Label {
        key: "project".into(),
        value: "match".into(),
    });
    let (deep_anchor, _) = submit_child(&mut store, &connector_parent.unwrap(), anchor_spec, false);
    let selector = JobSelector::Labels {
        labels: vec![crate::Label {
            key: "project".into(),
            value: "match".into(),
        }],
    };

    let page = store.tree(&selector, None, 8, 16, Some(1)).unwrap();
    assert_eq!(
        page.nodes
            .iter()
            .map(|node| node.summary.job_id)
            .collect::<Vec<_>>(),
        vec![root, branch]
    );
    let branch_node = page
        .nodes
        .iter()
        .find(|node| node.summary.job_id == branch)
        .unwrap();
    assert!(branch_node.descendants_truncated);
    let cursor = branch_node.next_children_cursor.clone().unwrap();
    assert_eq!(cursor.child_job_id, depth_cut);

    let continued = store.tree_children(&cursor, 16, None).unwrap();
    assert_eq!(
        continued
            .nodes
            .iter()
            .map(|node| node.summary.job_id)
            .collect::<Vec<_>>(),
        vec![depth_cut, connector]
    );
    let connector_node = continued
        .nodes
        .iter()
        .find(|node| node.summary.job_id == connector)
        .unwrap();
    assert!(connector_node.descendants_truncated);
    assert_eq!(
        connector_node
            .next_children_cursor
            .as_ref()
            .map(|cursor| cursor.child_job_id),
        Some(deep_anchor)
    );
}

#[test]
fn root_cursor_survives_logs_but_fails_closed_when_attention_order_changes() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    let (running, _) = submit_root(&mut store, spec(temp.path()), true);
    let (_queued, _) = submit_root(&mut store, spec(temp.path()), false);
    let first = store.tree(&JobSelector::All, None, 1, 1, None).unwrap();
    assert_eq!(
        first.nodes[0].family_attention,
        Some(TreeAttentionBucket::Running)
    );
    let cursor = first.next_root_cursor.clone().unwrap();

    store
        .connection
        .execute(
            "UPDATE jobs SET stdout_len = stdout_len + 1 WHERE id = ?1",
            [running.entity_uuid().to_string()],
        )
        .unwrap();
    assert!(
        store
            .tree(&JobSelector::All, Some(cursor.clone()), 1, 8, None)
            .is_ok()
    );

    store
        .connection
        .execute(
            "UPDATE jobs SET state = 'final', outcome = 'succeeded' WHERE id = ?1",
            [running.entity_uuid().to_string()],
        )
        .unwrap();
    assert!(matches!(
        store.tree(&JobSelector::All, Some(cursor), 1, 8, None),
        Err(StoreError::ViewStale(_))
    ));
}

#[test]
fn tree_observation_includes_future_descendants_and_wrong_store_gap_snapshot() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    let (root, parent) = submit_root(&mut store, spec(temp.path()), true);
    let head = store.event_head().unwrap();
    let (child, _) = submit_child(&mut store, &parent.unwrap(), spec(temp.path()), false);
    let selector = JobTreeSelector {
        root_job_ids: vec![root],
    };
    let events = store
        .observe_trees(&selector, Some(head), 32, 1, 32, None)
        .unwrap();
    assert!(matches!(
        events,
        TreeObservationFrame::Events { events, .. }
            if events.iter().any(|event| event.job_id == child)
    ));

    let foreign = EventCursor {
        store_uuid: Uuid::now_v7(),
        sequence: 0,
    };
    let gap = store
        .observe_trees(&selector, Some(foreign), 32, 1, 32, None)
        .unwrap();
    assert!(matches!(
        gap,
        TreeObservationFrame::Gap { snapshot, .. }
            if snapshot.nodes.iter().any(|node| node.summary.job_id == root)
                && snapshot.nodes.iter().any(|node| node.summary.job_id == child)
    ));
}

#[test]
fn schema_rejects_a_noncanonical_tree_order_trigger() {
    let temp = tempfile::tempdir().unwrap();
    let store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    store
        .connection
        .execute_batch(
            "DROP TRIGGER jobs_tree_order_insert;
             CREATE TRIGGER jobs_tree_order_insert AFTER INSERT ON jobs BEGIN
                 SELECT 1;
             END;",
        )
        .unwrap();
    assert!(matches!(
        validate_schema(&store.connection),
        Err(StoreError::InvalidState(detail)) if detail.contains("canonical definition")
    ));
}

#[test]
fn schema_requires_tree_query_indexes() {
    for index in [
        "jobs_parent_accepted",
        "jobs_state_accepted",
        "jobs_accepted_order",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
        store
            .connection
            .execute_batch(&format!("DROP INDEX {index}"))
            .unwrap();
        assert!(matches!(
            validate_schema(&store.connection),
            Err(StoreError::InvalidState(detail)) if detail.contains(index)
        ));
    }
}

#[test]
fn orphan_is_explicit_and_a_parent_cycle_fails_closed() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StorePaths::new(temp.path().to_path_buf());
    let mut store = Store::open(paths).unwrap();
    let (root, parent) = submit_root(&mut store, spec(temp.path()), true);
    let (child, _) = submit_child(&mut store, &parent.unwrap(), spec(temp.path()), false);

    store
        .connection
        .pragma_update(None, "foreign_keys", false)
        .unwrap();
    store
        .connection
        .execute(
            "DELETE FROM jobs WHERE id = ?1",
            [root.entity_uuid().to_string()],
        )
        .unwrap();
    let orphan = store.tree(&JobSelector::All, None, 8, 8, None).unwrap();
    assert_eq!(orphan.nodes[0].summary.job_id, child);
    assert_eq!(orphan.nodes[0].parent_retained, Some(false));
    let focused_orphan = store.tree_for_job(child, 1, None).unwrap();
    assert_eq!(focused_orphan.nodes[0].summary.job_id, child);
    assert_eq!(focused_orphan.nodes[0].parent_retained, Some(false));
    drop(store);

    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    let (root, parent) = submit_root(&mut store, spec(temp.path()), true);
    let (child, _) = submit_child(&mut store, &parent.unwrap(), spec(temp.path()), false);
    store
        .connection
        .execute(
            "UPDATE jobs SET parent_job_id = ?1 WHERE id = ?2",
            params![
                child.entity_uuid().to_string(),
                root.entity_uuid().to_string()
            ],
        )
        .unwrap();
    assert!(matches!(
        store.tree(&JobSelector::All, None, 8, 8, None),
        Err(StoreError::InvalidState(detail)) if detail.contains("cycle")
    ));
}

#[test]
fn managed_ancestry_accepts_sixty_four_jobs_and_rejects_the_sixty_fifth() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    let (_, parent) = submit_root(&mut store, spec(temp.path()), true);
    let mut parent = parent.unwrap();
    for _ in 1..64 {
        let (_, next) = submit_child(&mut store, &parent, spec(temp.path()), true);
        parent = next.unwrap();
    }
    let child = spec(temp.path());
    let hash = normalized_payload_hash(&child).unwrap();
    let scope = SubmissionScope::Managed(ManagedParent {
        job_id: parent.job_id,
        attempt_id: parent.attempt_id,
        invocation_id: parent.invocation_id,
    });
    assert!(matches!(
        store.submit_with_stdin_scoped(scope, Uuid::now_v7(), &hash, &child, None),
        Err(StoreError::OperationRejected { code, .. })
            if code == "child_policy_depth_exceeded"
    ));
    assert!(store.tree(&JobSelector::All, None, 8, 256, None).is_ok());
}

fn insert_fixture_roots(store: &mut Store, count: usize, spec_json: &str) {
    let store_uuid = store.store_uuid;
    store
        .connection
        .pragma_update(None, "foreign_keys", false)
        .unwrap();
    let transaction = store.connection.transaction().unwrap();
    {
        let mut insert = transaction
            .prepare(
                "INSERT INTO jobs(
                    id, submission_id, state, spec_json, claims_json, accepted_ms
                 ) VALUES (?1, ?2, 'final', ?3, '{}', ?4)",
            )
            .unwrap();
        for index in 0..count {
            insert
                .execute(params![
                    JobId::new(store_uuid).entity_uuid().to_string(),
                    Uuid::now_v7().to_string(),
                    spec_json,
                    i64::try_from(index).unwrap(),
                ])
                .unwrap();
        }
    }
    transaction.commit().unwrap();
}

#[test]
fn row_sixteen_thousand_three_hundred_eighty_five_fails_before_classification() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    let spec_json = serde_json::to_string(&spec(temp.path())).unwrap();
    insert_fixture_roots(&mut store, 16_385, &spec_json);
    assert!(matches!(
        store.tree(&JobSelector::All, None, 1, 1, None),
        Err(StoreError::ViewUnavailable(detail)) if detail.contains("tree_scan_limit")
    ));
}

#[test]
fn encoded_budget_truncates_with_a_root_continuation_and_makes_progress() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    let mut oversized_fixture = spec(temp.path());
    oversized_fixture.labels.push(crate::Label {
        key: "budget-fixture".into(),
        value: "x".repeat(48 * 1024),
    });
    let spec_json = serde_json::to_string(&oversized_fixture).unwrap();
    insert_fixture_roots(&mut store, 256, &spec_json);

    let first = store.tree(&JobSelector::All, None, 256, 256, None).unwrap();
    assert!(!first.nodes.is_empty());
    assert!(first.nodes.len() < 256);
    assert!(first.next_root_cursor.is_some());
    assert!(serde_json::to_vec(&first).unwrap().len() <= 8 * 1024 * 1024);
    let second = store
        .tree(&JobSelector::All, first.next_root_cursor, 256, 256, None)
        .unwrap();
    assert!(!second.nodes.is_empty());
    assert!(serde_json::to_vec(&second).unwrap().len() <= 8 * 1024 * 1024);
}
