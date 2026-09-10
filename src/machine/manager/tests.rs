use super::*;

fn id<T: std::str::FromStr>(store: Uuid) -> T
where
    T::Err: std::fmt::Debug,
{
    format!("{store}~{}", Uuid::now_v7()).parse().unwrap()
}
fn open(path: &std::path::Path) -> Connection {
    let c = Connection::open(path).unwrap();
    c.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;
        CREATE TABLE IF NOT EXISTS fixture_leases(id TEXT PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS fixture_starts(id TEXT PRIMARY KEY);",
    )
    .unwrap();
    c
}
fn response(r: &Request, outcome: Outcome) -> Reply {
    Reply {
        session: r.session.clone(),
        request_sequence: r.request_sequence,
        operation_id: r.operation_id,
        coordinator_revision: r.request_sequence,
        outcome,
    }
}
#[test]
fn local_outbox_and_start_intent_survive_commit_gaps_without_relaunch() {
    local_commit_fixture(false);
}
#[test]
fn lost_ticket_then_coordinator_reset_and_epoch_rotation_recover_without_launch() {
    local_commit_fixture(true);
}
fn local_commit_fixture(coordinator_reset: bool) {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("manager.sqlite3");
    let mut c = open(&path);
    let session = SessionIdentity {
        machine_id: Uuid::now_v7(),
        authority_epoch: Uuid::now_v7(),
        domain_id: crate::ExecutionDomainId(Uuid::now_v7()),
        manager_store_uuid: Uuid::now_v7(),
        executor_incarnation: Uuid::now_v7(),
        connection_epoch: 1,
    };
    let secret = PairingSecret::generate().unwrap();
    let participant = ParticipantSnapshot {
        installation: InstallationIdentity {
            installation_nonce: Uuid::now_v7(),
            domain_id: session.domain_id,
            owner_uid: 1000,
            runtime_registration: "fixture".into(),
            role: ParticipantRole::Executor,
        },
        manager_store_uuid: session.manager_store_uuid,
        connection_epoch: 1,
        executor_incarnation: Some(session.executor_incarnation),
        reconciliation_required: true,
        retired_sequence_floor: 0,
        accepted_sequence: 0,
    };
    let tx = c.transaction().unwrap();
    initialize(&tx, session.manager_store_uuid).unwrap();
    bind(&tx, &session, &participant).unwrap();
    tx.commit().unwrap();
    let key = AllocationKey {
        machine_id: session.machine_id,
        authority_epoch: session.authority_epoch,
        domain_id: session.domain_id,
        manager_store_uuid: session.manager_store_uuid,
        lease_id: Uuid::now_v7(),
    };
    let candidate = Candidate {
        key: key.clone(),
        owner: AllocationOwner::Work {
            job_id: id(session.manager_store_uuid),
            attempt_id: id(session.manager_store_uuid),
        },
        revision: 1,
        priority: 0,
        claims: Claims {
            scalars: [("cargo_slots".into(), 1)].into(),
            ..Claims::default()
        },
        configuration_sha256: "a".repeat(64),
        observed: None,
        quiet: None,
    };
    let grant = GrantSnapshot {
        risk_clearance: None,
        uncertainty_reason: None,
        queue_accepted_unix_millis: 1,
        queue_sequence: 1,
        grant_id: id(Uuid::now_v7()),
        candidate,
        offer_nonce: Uuid::now_v7(),
        state: GrantState::Armed,
        offered_unix_millis: 1,
        offer_deadline_unix_millis: 5001,
        armed_unix_millis: Some(2),
        released_unix_millis: None,
        tickets: vec![],
        sealed_release: None,
    };
    let operation = Uuid::now_v7();
    let arm = Command::Arm {
        key: key.clone(),
        offer_nonce: grant.offer_nonce,
    };
    {
        let tx = c.transaction().unwrap();
        tx.execute(
            "INSERT INTO fixture_leases VALUES (?1)",
            [key.lease_id.to_string()],
        )
        .unwrap();
        enqueue(&tx, operation, &arm).unwrap();
        // Crash before the local transaction: neither Lease nor outbox survives.
    }
    assert!(pending(&c, &secret).unwrap().is_none());
    assert_eq!(
        c.query_row("SELECT COUNT(*) FROM fixture_leases", [], |r| r
            .get::<_, u64>(0))
            .unwrap(),
        0
    );
    let tx = c.transaction().unwrap();
    tx.execute(
        "INSERT INTO fixture_leases VALUES (?1)",
        [key.lease_id.to_string()],
    )
    .unwrap();
    assert_eq!(enqueue(&tx, operation, &arm).unwrap(), 1);
    assert_eq!(enqueue(&tx, operation, &arm).unwrap(), 1);
    assert!(enqueue(&tx, operation, &Command::Inspect { key: None }).is_err());
    tx.commit().unwrap();
    let arm_request = pending(&c, &secret).unwrap().unwrap();
    drop(c);
    c = open(&path);
    assert_eq!(pending(&c, &secret).unwrap(), Some(arm_request.clone()));
    let arm_reply = response(
        &arm_request,
        Outcome::Grant {
            grant: Box::new(grant.clone()),
        },
    );
    let tx = c.transaction().unwrap();
    assert!(accept(&tx, &arm_request, &arm_reply).unwrap());
    tx.commit().unwrap();
    let tx = c.transaction().unwrap();
    assert!(!accept(&tx, &arm_request, &arm_reply).unwrap());
    tx.commit().unwrap();
    let intent = InvocationIntent {
        invocation_id: id(session.manager_store_uuid),
        containment_id: id(session.manager_store_uuid),
        role: InvocationRole::Primary,
        role_index: 0,
        release_sequence: 1,
        executable_sha256: "b".repeat(64),
        boundary_sha256: "c".repeat(64),
        readiness_challenge: Uuid::now_v7(),
        previous_cleanup: None,
    };
    let authorize = Command::AuthorizeInvocation {
        key: key.clone(),
        offer_nonce: grant.offer_nonce,
        intent: intent.clone(),
    };
    let tx = c.transaction().unwrap();
    enqueue(&tx, Uuid::now_v7(), &authorize).unwrap();
    tx.commit().unwrap();
    let ticket_request = pending(&c, &secret).unwrap().unwrap();
    let tx = c.transaction().unwrap();
    assert!(seal_release(&tx, &key, Uuid::now_v7()).is_err());
    tx.rollback().unwrap();
    let ticket = InvocationTicket {
        grant_id: grant.grant_id,
        key: key.clone(),
        offer_nonce: grant.offer_nonce,
        intent: intent.clone(),
        configuration_sha256: "a".repeat(64),
        issued_unix_millis: 3,
        host_observation_generation: Uuid::now_v7(),
        host_sample_unix_millis: 3,
        session: session.clone(),
    };
    let ticket_reply = response(
        &ticket_request,
        Outcome::Ticket {
            ticket: Box::new(ticket.clone()),
        },
    );
    {
        let tx = c.transaction().unwrap();
        enqueue(
            &tx,
            Uuid::now_v7(),
            &Command::ReportUncertain {
                key: key.clone(),
                offer_nonce: grant.offer_nonce,
                reason: "local proof temporarily unavailable".into(),
            },
        )
        .unwrap();
        // An older in-flight ticket still becomes a cleanup obligation, but
        // cannot be consumed after the local uncertainty fence was persisted.
        accept(&tx, &ticket_request, &ticket_reply).unwrap();
        assert!(consume_ticket(&tx, &ticket).is_err());
        // Lost local commit retains the exact outstanding request.
    }
    assert_eq!(pending(&c, &secret).unwrap(), Some(ticket_request.clone()));
    if coordinator_reset {
        let mut recovered_session = session.clone();
        recovered_session.connection_epoch += 1;
        recovered_session.executor_incarnation = Uuid::now_v7();
        let mut peer = participant.clone();
        peer.connection_epoch = recovered_session.connection_epoch;
        peer.executor_incarnation = Some(recovered_session.executor_incarnation);
        peer.accepted_sequence = ticket_request.request_sequence;
        peer.retired_sequence_floor = peer.accepted_sequence;
        let tx = c.transaction().unwrap();
        assert!(bind(&tx, &recovered_session, &peer).is_err());
        recovery::begin(&tx, &recovered_session, &peer).unwrap();
        assert!(consume_ticket(&tx, &ticket).is_err());
        assert!(enqueue(&tx, Uuid::now_v7(), &authorize).is_err());
        tx.commit().unwrap();
        drop(c);
        c = open(&path);
        assert_eq!(recovery::abandoned(&c).unwrap().len(), 1);
        let tx = c.transaction().unwrap();
        enqueue(
            &tx,
            Uuid::now_v7(),
            &recovery::next_page(&tx).unwrap().unwrap(),
        )
        .unwrap();
        tx.commit().unwrap();
        let request = pending(&c, &secret).unwrap().unwrap();
        let mut recovered_grant = grant.clone();
        recovered_grant.tickets.push(intent.clone());
        let reply = response(
            &request,
            Outcome::InventoryPage {
                grants: vec![recovered_grant],
                next: None,
                configuration_sha256: "a".repeat(64),
            },
        );
        {
            let tx = c.transaction().unwrap();
            accept(&tx, &request, &reply).unwrap();
        }
        assert!(recovery::next_page(&c).unwrap().is_some());
        let tx = c.transaction().unwrap();
        accept(&tx, &request, &reply).unwrap();
        tx.commit().unwrap();
        assert!(recovery::next_page(&c).unwrap().is_none());
        let tx = c.transaction().unwrap();
        assert!(seal_release(&tx, &key, Uuid::now_v7()).is_err());
        assert!(consume_ticket(&tx, &ticket).is_err());
        tx.rollback().unwrap();
        let tx = c.transaction().unwrap();
        record_cleanup(
            &tx,
            &TicketCleanup {
                invocation_id: intent.invocation_id,
                release_sequence: intent.release_sequence,
                boundary_sha256: intent.boundary_sha256.clone(),
                proof_sha256: "d".repeat(64),
                user_code_released: false,
            },
        )
        .unwrap();
        seal_release(&tx, &key, Uuid::now_v7()).unwrap();
        tx.commit().unwrap();
        for command in recovery::reconciliation(&c).unwrap() {
            let outcome = match &command {
                Command::ReconcileCommit { .. } => Outcome::Reconciled {
                    end_sequence: request.request_sequence,
                    released: vec![key.clone()],
                },
                _ => Outcome::Accepted { revision: 0 },
            };
            let tx = c.transaction().unwrap();
            enqueue(&tx, Uuid::now_v7(), &command).unwrap();
            tx.commit().unwrap();
            let request = pending(&c, &secret).unwrap().unwrap();
            let tx = c.transaction().unwrap();
            accept(&tx, &request, &response(&request, outcome)).unwrap();
            tx.commit().unwrap();
        }
        assert!(!recovery::active(&c).unwrap());
        // Explicitly rotate the authenticated authority after all old rights
        // have been sealed; the same local store can then advertise new work.
        recovered_session.authority_epoch = Uuid::now_v7();
        recovered_session.connection_epoch += 1;
        peer.connection_epoch = recovered_session.connection_epoch;
        peer.accepted_sequence = c
            .query_row("SELECT applied_sequence FROM attached_peer", [], |r| {
                r.get(0)
            })
            .unwrap();
        let tx = c.transaction().unwrap();
        assert!(bind(&tx, &recovered_session, &peer).is_err());
        recovery::begin(&tx, &recovered_session, &peer).unwrap();
        tx.commit().unwrap();
        let command = recovery::next_page(&c).unwrap().unwrap();
        let tx = c.transaction().unwrap();
        enqueue(&tx, Uuid::now_v7(), &command).unwrap();
        tx.commit().unwrap();
        let request = pending(&c, &secret).unwrap().unwrap();
        let tx = c.transaction().unwrap();
        accept(
            &tx,
            &request,
            &response(
                &request,
                Outcome::InventoryPage {
                    grants: vec![],
                    next: None,
                    configuration_sha256: "a".repeat(64),
                },
            ),
        )
        .unwrap();
        tx.commit().unwrap();
        for command in recovery::reconciliation(&c).unwrap() {
            let outcome = match &command {
                Command::ReconcileCommit { .. } => Outcome::Reconciled {
                    end_sequence: request.request_sequence,
                    released: vec![],
                },
                _ => Outcome::Accepted { revision: 0 },
            };
            let tx = c.transaction().unwrap();
            enqueue(&tx, Uuid::now_v7(), &command).unwrap();
            tx.commit().unwrap();
            let request = pending(&c, &secret).unwrap().unwrap();
            let tx = c.transaction().unwrap();
            accept(&tx, &request, &response(&request, outcome)).unwrap();
            tx.commit().unwrap();
        }
        let mut candidate = grant.candidate.clone();
        candidate.key.authority_epoch = recovered_session.authority_epoch;
        candidate.key.lease_id = Uuid::now_v7();
        let tx = c.transaction().unwrap();
        enqueue(&tx, Uuid::now_v7(), &Command::CandidateUpsert { candidate }).unwrap();
        assert!(consume_ticket(&tx, &ticket).is_err());
        tx.commit().unwrap();
        assert_eq!(
            c.query_row("SELECT COUNT(*) FROM attached_tickets", [], |r| r
                .get::<_, u64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            c.query_row("SELECT COUNT(*) FROM fixture_starts", [], |r| r
                .get::<_, u64>(0))
                .unwrap(),
            0
        );
        return;
    }
    let tx = c.transaction().unwrap();
    accept(&tx, &ticket_request, &ticket_reply).unwrap();
    tx.commit().unwrap();
    {
        let tx = c.transaction().unwrap();
        assert!(consume_ticket(&tx, &ticket).unwrap());
        tx.execute(
            "INSERT INTO fixture_starts VALUES (?1)",
            [intent.invocation_id.to_string()],
        )
        .unwrap();
        // No OS release is legal before this commit; rollback leaves both absent.
    }
    assert_eq!(
        c.query_row("SELECT COUNT(*) FROM fixture_starts", [], |r| r
            .get::<_, u64>(0))
            .unwrap(),
        0
    );
    let tx = c.transaction().unwrap();
    assert!(consume_ticket(&tx, &ticket).unwrap());
    tx.execute(
        "INSERT INTO fixture_starts VALUES (?1)",
        [intent.invocation_id.to_string()],
    )
    .unwrap();
    tx.commit().unwrap();
    drop(c);
    c = open(&path);
    let tx = c.transaction().unwrap();
    assert!(
        !consume_ticket(&tx, &ticket).unwrap(),
        "restart repeated a possibly released start"
    );
    assert!(seal_release(&tx, &key, Uuid::now_v7()).is_err());
    tx.rollback().unwrap();
    let cleanup = TicketCleanup {
        invocation_id: intent.invocation_id,
        release_sequence: 1,
        boundary_sha256: intent.boundary_sha256,
        proof_sha256: "d".repeat(64),
        user_code_released: true,
    };
    let tx = c.transaction().unwrap();
    record_cleanup(&tx, &cleanup).unwrap();
    assert!(
        consume_ticket(&tx, &ticket).is_err(),
        "empty sealed boundary was relaunched"
    );
    let release_operation = Uuid::now_v7();
    seal_release(&tx, &key, release_operation).unwrap();
    assert!(enqueue(&tx, Uuid::now_v7(), &authorize).is_err());
    tx.commit().unwrap();
    let release_request = pending(&c, &secret).unwrap().unwrap();
    drop(c);
    c = open(&path);
    assert_eq!(pending(&c, &secret).unwrap(), Some(release_request.clone()));
    let Command::Release { release } = &release_request.command else {
        panic!("release outbox");
    };
    let release_reply = response(
        &release_request,
        Outcome::Released {
            grant_id: grant.grant_id,
            sealed_sequence: release.sealed_sequence,
        },
    );
    let tx = c.transaction().unwrap();
    assert!(accept(&tx, &release_request, &release_reply).unwrap());
    tx.commit().unwrap();
    let tx = c.transaction().unwrap();
    assert!(!accept(&tx, &release_request, &release_reply).unwrap());
    enqueue(
        &tx,
        Uuid::now_v7(),
        &Command::Acknowledge {
            through_sequence: 3,
        },
    )
    .unwrap();
    tx.commit().unwrap();
    let ack = pending(&c, &secret).unwrap().unwrap();
    let tx = c.transaction().unwrap();
    accept(
        &tx,
        &ack,
        &response(
            &ack,
            Outcome::Acknowledged {
                through_sequence: 3,
            },
        ),
    )
    .unwrap();
    tx.commit().unwrap();
    assert!(pending(&c, &secret).unwrap().is_none());
    assert_eq!(
        c.query_row("SELECT COUNT(*) FROM attached_outbox", [], |r| r
            .get::<_, u64>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        c.query_row(
            "SELECT COUNT(*) FROM attached_tickets WHERE consumed=1",
            [],
            |r| r.get::<_, u64>(0)
        )
        .unwrap(),
        1
    );
    let tx = c.transaction().unwrap();
    assert!(
        accept(&tx, &arm_request, &arm_reply).is_err(),
        "compacted reply recreated a Lease"
    );
}

#[test]
fn outbox_counts_utf8_bytes_and_reserves_room_for_acknowledgement() {
    let mut c = Connection::open_in_memory().unwrap();
    let session = SessionIdentity {
        machine_id: Uuid::now_v7(),
        authority_epoch: Uuid::now_v7(),
        domain_id: crate::ExecutionDomainId(Uuid::now_v7()),
        manager_store_uuid: Uuid::now_v7(),
        executor_incarnation: Uuid::now_v7(),
        connection_epoch: 1,
    };
    let secret = PairingSecret::generate().unwrap();
    let tx = c.transaction().unwrap();
    initialize(&tx, session.manager_store_uuid).unwrap();
    // Binding authentication is exercised above; seed only this capacity fixture.
    tx.execute(
        "UPDATE attached_peer SET session_json=?1",
        [serde_json::to_string(&session).unwrap()],
    )
    .unwrap();
    tx.commit().unwrap();
    let mut applied = 0;
    for _ in 0..100 {
        let tx = c.transaction().unwrap();
        if enqueue(&tx, Uuid::now_v7(), &Command::Inspect { key: None }).is_err() {
            break;
        }
        tx.commit().unwrap();
        let request = pending(&c, &secret).unwrap().unwrap();
        let reply = response(
            &request,
            Outcome::Rejected {
                code: "fixture".into(),
                detail: "я".repeat(350_000),
            },
        );
        let tx = c.transaction().unwrap();
        accept(&tx, &request, &reply).unwrap();
        tx.commit().unwrap();
        applied = request.request_sequence;
        let bytes:u64 = c.query_row("SELECT SUM(length(CAST(command_json AS BLOB))+length(CAST(outcome_json AS BLOB))) FROM attached_outbox",[],|r|r.get(0)).unwrap();
        assert!(
            bytes <= 16 * 1024 * 1024,
            "text character counts bypassed the durable byte budget"
        );
    }
    assert!(applied > 0 && applied < 100);
    let tx = c.transaction().unwrap();
    enqueue(
        &tx,
        Uuid::now_v7(),
        &Command::Acknowledge {
            through_sequence: applied,
        },
    )
    .unwrap();
    tx.commit().unwrap();
    let request = pending(&c, &secret).unwrap().unwrap();
    let tx = c.transaction().unwrap();
    accept(
        &tx,
        &request,
        &response(
            &request,
            Outcome::Acknowledged {
                through_sequence: applied,
            },
        ),
    )
    .unwrap();
    tx.commit().unwrap();
    assert_eq!(
        c.query_row("SELECT COUNT(*) FROM attached_outbox", [], |r| r
            .get::<_, u64>(0))
            .unwrap(),
        1
    );
}
