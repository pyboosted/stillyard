use super::*;

#[test]
fn list_events_and_cursor_are_one_public_observation_path() {
    let temp = tempfile::tempdir().unwrap();
    let mut store =
        Store::open_with_capacities(StorePaths::new(temp.path().to_path_buf()), capacities())
            .unwrap();
    let mut job = spec(temp.path());
    job.args = vec!["audit".into(), "two words".into()];
    job.labels.push(crate::Label {
        key: "round".into(),
        value: "seven".into(),
    });
    job.resources.cargo_slots = Some(1);
    let hash = normalized_payload_hash(&job).unwrap();
    let receipt = store.submit(Uuid::now_v7(), &hash, &job).unwrap().receipt;

    let page = store
        .list_jobs(
            &JobSelector::Labels {
                labels: job.labels.clone(),
            },
            None,
            10,
        )
        .unwrap();
    assert_eq!(page.jobs.len(), 1);
    assert_eq!(page.jobs[0].job_id, receipt.job_id);
    assert_eq!(
        page.jobs[0].command_preview,
        r#"tool.exe audit "two words""#
    );
    assert_eq!(page.jobs[0].queue_rank, Some(1));
    assert_eq!(page.jobs[0].claims.cargo_slots, Some(1));
    assert_eq!(page.jobs[0].labels, job.labels);
    assert!(page.event_cursor.sequence > 0);

    let frame = store
        .observe(
            &JobSelector::Jobs {
                job_ids: vec![receipt.job_id],
            },
            Some(EventCursor {
                store_uuid: store.store_uuid,
                sequence: 0,
            }),
            100,
        )
        .unwrap();
    let ObservationFrame::Events { events, cursor } = frame else {
        panic!("fresh retained history must not produce Gap");
    };
    assert!(!events.is_empty());
    assert_eq!(events.last().unwrap().cursor, cursor);
    assert!(events.iter().all(|event| event.job_id == receipt.job_id));

    store
        .commit_log_offset(receipt.job_id, LogStream::Stdout, 1)
        .unwrap();
    store
        .commit_log_offset(receipt.job_id, LogStream::Stdout, 2)
        .unwrap();
    let first_events = store
        .observe(
            &JobSelector::Jobs {
                job_ids: vec![receipt.job_id],
            },
            Some(cursor),
            1,
        )
        .unwrap();
    let ObservationFrame::Events {
        events: first_events,
        cursor: first_cursor,
    } = first_events
    else {
        panic!("paged event read must stay on the Events branch");
    };
    assert_eq!(first_events.len(), 1);
    assert_eq!(first_events[0].cursor, first_cursor);
    let second_events = store
        .observe(
            &JobSelector::Jobs {
                job_ids: vec![receipt.job_id],
            },
            Some(first_cursor),
            10,
        )
        .unwrap();
    let ObservationFrame::Events {
        events: second_events,
        ..
    } = second_events
    else {
        panic!("remaining events must be readable after a truncated page");
    };
    assert_eq!(second_events.len(), 1);
    assert!(second_events[0].cursor.sequence > first_cursor.sequence);

    let replaced = store
        .observe(
            &JobSelector::All,
            Some(EventCursor {
                store_uuid: Uuid::now_v7(),
                sequence: cursor.sequence,
            }),
            10,
        )
        .unwrap();
    assert!(matches!(replaced, ObservationFrame::Gap { .. }));
    let exact_replaced = store
        .observe(
            &JobSelector::Jobs {
                job_ids: vec![receipt.job_id],
            },
            Some(EventCursor {
                store_uuid: Uuid::now_v7(),
                sequence: cursor.sequence,
            }),
            10,
        )
        .unwrap();
    assert!(matches!(
        exact_replaced,
        ObservationFrame::Gap {
            ref snapshot,
            cursor: replacement_cursor,
            ..
        } if snapshot.jobs.is_empty()
            && snapshot.event_cursor == replacement_cursor
            && replacement_cursor.store_uuid == store.store_uuid
    ));

    let before = store.event_head().unwrap();
    let transaction = store.connection.transaction().unwrap();
    transaction
        .execute(
            "UPDATE jobs SET stdout_len = 99 WHERE id = ?1",
            [receipt.job_id.entity_uuid().to_string()],
        )
        .unwrap();
    drop(transaction);
    assert_eq!(store.event_head().unwrap(), before);
    assert_eq!(
        store.job_summary(receipt.job_id).unwrap().stdout_committed,
        2
    );
}

#[test]
fn event_ring_reports_gap_and_resynchronizes() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    let job = spec(temp.path());
    let hash = normalized_payload_hash(&job).unwrap();
    let receipt = store.submit(Uuid::now_v7(), &hash, &job).unwrap().receipt;
    let transaction = store.connection.transaction().unwrap();
    for offset in 1..=(MAX_EVENT_ROWS + 8) {
        transaction
            .execute(
                "UPDATE jobs SET stdout_len = ?2 WHERE id = ?1",
                params![receipt.job_id.entity_uuid().to_string(), offset],
            )
            .unwrap();
    }
    transaction.commit().unwrap();
    let retained: u64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(retained, MAX_EVENT_ROWS);

    let frame = store
        .observe(
            &JobSelector::All,
            Some(EventCursor {
                store_uuid: store.store_uuid,
                sequence: 0,
            }),
            16,
        )
        .unwrap();
    let ObservationFrame::Gap {
        gap,
        snapshot,
        cursor,
    } = frame
    else {
        panic!("expired cursor must produce Gap");
    };
    assert_eq!(
        gap.oldest_available.sequence,
        cursor.sequence - MAX_EVENT_ROWS + 1
    );
    assert_eq!(snapshot.jobs[0].job_id, receipt.job_id);
    assert_eq!(snapshot.jobs[0].stdout_committed, MAX_EVENT_ROWS + 8);
    assert_eq!(snapshot.event_cursor, cursor);

    let continuous = store
        .observe(
            &JobSelector::All,
            Some(EventCursor {
                store_uuid: store.store_uuid,
                sequence: gap.oldest_available.sequence - 1,
            }),
            1,
        )
        .unwrap();
    assert!(matches!(continuous, ObservationFrame::Events { .. }));
    let expired_by_one = store
        .observe(
            &JobSelector::All,
            Some(EventCursor {
                store_uuid: store.store_uuid,
                sequence: gap.oldest_available.sequence - 2,
            }),
            1,
        )
        .unwrap();
    assert!(matches!(expired_by_one, ObservationFrame::Gap { .. }));
}

#[test]
fn lifecycle_and_cancellation_transitions_emit_named_events() {
    let temp = tempfile::tempdir().unwrap();
    let mut store =
        Store::open_with_capacities(StorePaths::new(temp.path().to_path_buf()), capacities())
            .unwrap();
    let job = spec(temp.path());
    let hash = normalized_payload_hash(&job).unwrap();
    let receipt = store.submit(Uuid::now_v7(), &hash, &job).unwrap().receipt;
    let before = store.event_head().unwrap();
    let prepared = store.prepare_job(receipt.job_id).unwrap().unwrap();
    store
        .mark_started(&prepared, 42, "observation-image-hash")
        .unwrap();
    store.mark_root_exited(&prepared, 0).unwrap();
    store.cancel_jobs(&[receipt.job_id]).unwrap();
    let frame = store
        .observe(
            &JobSelector::Jobs {
                job_ids: vec![receipt.job_id],
            },
            Some(before),
            MAX_OBSERVATION_PAGE,
        )
        .unwrap();
    let ObservationFrame::Events { events, .. } = frame else {
        panic!("retained lifecycle events must not Gap");
    };
    let kinds = events.iter().map(|event| event.kind).collect::<Vec<_>>();
    assert!(kinds.contains(&SchedulerEventKind::AttemptChanged));
    assert!(kinds.contains(&SchedulerEventKind::InvocationChanged));
    assert!(kinds.contains(&SchedulerEventKind::ContainmentChanged));
    assert!(kinds.contains(&SchedulerEventKind::CancellationRequested));
    let invocation_events = events
        .iter()
        .filter(|event| event.kind == SchedulerEventKind::InvocationChanged)
        .collect::<Vec<_>>();
    assert_eq!(invocation_events.len(), 2);
    assert_eq!(
        invocation_events
            .iter()
            .map(|event| event.transition)
            .collect::<Vec<_>>(),
        vec![
            Some(InvocationTransition::Started),
            Some(InvocationTransition::Exited)
        ]
    );
    assert!(invocation_events.iter().all(|event| {
        event.attempt_id == Some(prepared.attempt_id)
            && event.invocation_id == Some(prepared.invocation_id)
    }));
}

#[test]
fn invocation_event_identity_is_atomic_and_required() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    let job = spec(temp.path());
    let hash = normalized_payload_hash(&job).unwrap();
    let receipt = store.submit(Uuid::now_v7(), &hash, &job).unwrap().receipt;
    let prepared = store.prepare_job(receipt.job_id).unwrap().unwrap();
    let error = store.connection.execute(
        "INSERT INTO events(kind, job_id, committed_ms)
         VALUES ('invocation_changed', ?1, ?2)",
        params![receipt.job_id.entity_uuid().to_string(), now_millis()],
    );
    assert!(
        error.is_err(),
        "partial InvocationChanged must fail its CHECK"
    );

    let before = store.event_head().unwrap();
    store
        .mark_started(&prepared, 42, "observation-image-hash")
        .unwrap();
    let frame = store
        .observe(
            &JobSelector::Jobs {
                job_ids: vec![receipt.job_id],
            },
            Some(before),
            MAX_OBSERVATION_PAGE,
        )
        .unwrap();
    let ObservationFrame::Events { events, .. } = frame else {
        panic!("fresh transition must remain observable");
    };
    assert!(events.iter().any(|event| {
        event.kind == SchedulerEventKind::InvocationChanged
            && event.attempt_id == Some(prepared.attempt_id)
            && event.invocation_id == Some(prepared.invocation_id)
            && event.transition == Some(InvocationTransition::Started)
    }));

    let other_job = spec(temp.path());
    let other_hash = normalized_payload_hash(&other_job).unwrap();
    let other_receipt = store
        .submit(Uuid::now_v7(), &other_hash, &other_job)
        .unwrap()
        .receipt;
    let before_mismatch = store.event_head().unwrap();
    store
        .connection
        .execute(
            "INSERT INTO events(
                 kind, job_id, attempt_id, invocation_id, transition, committed_ms
             ) VALUES ('invocation_changed', ?1, ?2, ?3, 'started', ?4)",
            params![
                other_receipt.job_id.entity_uuid().to_string(),
                prepared.attempt_id.entity_uuid().to_string(),
                prepared.invocation_id.entity_uuid().to_string(),
                now_millis()
            ],
        )
        .unwrap();
    let error = store
        .observe(
            &JobSelector::Jobs {
                job_ids: vec![other_receipt.job_id],
            },
            Some(before_mismatch),
            MAX_OBSERVATION_PAGE,
        )
        .unwrap_err();
    assert!(matches!(error, StoreError::InvalidState(_)));
}

#[test]
fn list_cursor_is_stable_and_store_scoped() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
    let mut submitted = Vec::new();
    for _ in 0..3 {
        let job = spec(temp.path());
        let hash = normalized_payload_hash(&job).unwrap();
        submitted.push(
            store
                .submit(Uuid::now_v7(), &hash, &job)
                .unwrap()
                .receipt
                .job_id,
        );
    }
    let first = store.list_jobs(&JobSelector::All, None, 1).unwrap();
    store
        .connection
        .execute(
            "UPDATE jobs SET state = 'active' WHERE id = ?1",
            [first.jobs[0].job_id.entity_uuid().to_string()],
        )
        .unwrap();
    let second = store
        .list_jobs(&JobSelector::All, first.next_cursor, 1)
        .unwrap();
    assert_ne!(first.jobs[0].job_id, second.jobs[0].job_id);
    let third = store
        .list_jobs(&JobSelector::All, second.next_cursor, 1)
        .unwrap();
    assert!(third.next_cursor.is_none());
    let mut paged = vec![
        first.jobs[0].job_id,
        second.jobs[0].job_id,
        third.jobs[0].job_id,
    ];
    paged.sort();
    submitted.sort();
    assert_eq!(paged, submitted);
    let mut foreign = first.next_cursor.unwrap();
    foreign.store_uuid = Uuid::now_v7();
    assert!(matches!(
        store.list_jobs(&JobSelector::All, Some(foreign), 1),
        Err(StoreError::Rejected(_))
    ));
}
