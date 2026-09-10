use super::*;
use stillyard::machine::{self as m, manager as journal};

fn manager_database(path: &std::path::Path) -> rusqlite::Connection {
    let c = rusqlite::Connection::open(path).unwrap();
    c.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;
        CREATE TABLE IF NOT EXISTS local_lease(id TEXT PRIMARY KEY,state TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS local_invocation(id TEXT PRIMARY KEY,starts INTEGER NOT NULL);",
    )
    .unwrap();
    c
}

#[test]
fn durable_manager_outbox_recovers_lost_arm_and_release_without_a_second_start() {
    manager_fixture(false);
}
#[test]
fn durable_manager_recovers_retired_ticket_reply_and_rotated_coordinator_authority() {
    manager_fixture(true);
}
fn manager_fixture(reset_after_ticket: bool) {
    let temp = tempfile::tempdir().unwrap();
    let pinned = copied_daemon(temp.path());
    let store = temp.path().join("coordinator");
    std::fs::create_dir_all(&store).unwrap();
    let mut config = HostConfig::default();
    config.resources.cargo_slots = 1;
    std::fs::write(
        store.join("config.json"),
        serde_json::to_vec(&config).unwrap(),
    )
    .unwrap();
    let endpoint = format!(r"\\.\pipe\stillyard-manager-outbox-{}", Uuid::now_v7());
    let mut daemon = spawn_daemon(&pinned, &store, &endpoint);
    let client = connect(&pinned, &endpoint);
    let deadline = || Instant::now() + Duration::from_secs(10);
    let authority = client.authority_status(deadline(), None).unwrap();
    let domains = authority.domains.unwrap();
    let secret = m::PairingSecret::generate().unwrap();
    let registration = m::PairingRegistration {
        installation: m::InstallationIdentity {
            installation_nonce: Uuid::now_v7(),
            domain_id: stillyard::ExecutionDomainId(Uuid::now_v7()),
            owner_uid: 1000,
            runtime_registration: "durable-test-manager".into(),
            role: m::ParticipantRole::Executor,
        },
        manager_store_uuid: Uuid::now_v7(),
        parent_domain: domains.machine_scope,
        budgets: Default::default(),
        aliases: Default::default(),
        secret: *secret.anchor_bytes(),
    };
    client
        .pair_machine_domain(registration.clone(), deadline())
        .unwrap();
    let call = |command: &str, input: serde_json::Value| {
        let path = temp.path().join(format!("request-{}.json", Uuid::now_v7()));
        std::fs::write(&path, serde_json::to_vec(&input).unwrap()).unwrap();
        let result = Command::new(&pinned)
            .args(["--endpoint", &endpoint, "machine", command, "--spec"])
            .arg(path)
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        serde_json::from_slice::<serde_json::Value>(&result.stdout).unwrap()
    };
    let connect_manager = || {
        let hello = m::ConnectHello {
            installation_nonce: registration.installation.installation_nonce,
            manager_store_uuid: registration.manager_store_uuid,
            executor_incarnation: Uuid::now_v7(),
            executor_protocol: 25,
            executor_nonce: [7; 32],
        };
        let challenge: m::ConnectChallenge =
            serde_json::from_value(call("connect-begin", serde_json::to_value(hello).unwrap()))
                .unwrap();
        let tag = secret.sign_challenge(&challenge).unwrap();
        let participant: m::ParticipantSnapshot = serde_json::from_value(call(
            "connect-finish",
            serde_json::json!({"challenge":challenge,"tag":tag}),
        ))
        .unwrap();
        (challenge, participant)
    };
    let (challenge, participant) = connect_manager();
    let path = temp.path().join("manager.sqlite3");
    let mut local = manager_database(&path);
    let tx = local.transaction().unwrap();
    journal::initialize(&tx, registration.manager_store_uuid).unwrap();
    journal::bind(&tx, &challenge.session, &participant).unwrap();
    tx.commit().unwrap();
    let send = |request: &m::Request| -> m::Reply {
        serde_json::from_value(call("exchange", serde_json::to_value(request).unwrap())).unwrap()
    };
    let apply = |local: &mut rusqlite::Connection, request: &m::Request, reply: &m::Reply| {
        let tx = local.transaction().unwrap();
        let changed = journal::accept(&tx, request, reply).unwrap();
        if changed && matches!(reply.outcome, m::Outcome::Grant { .. }) {
            tx.execute("UPDATE local_lease SET state='armed'", [])
                .unwrap();
        }
        if changed && matches!(reply.outcome, m::Outcome::Released { .. }) {
            tx.execute("UPDATE local_lease SET state='released'", [])
                .unwrap();
        }
        tx.commit().unwrap();
        changed
    };
    let exchange = |local: &mut rusqlite::Connection, command: m::Command| -> m::Reply {
        let tx = local.transaction().unwrap();
        journal::enqueue(&tx, Uuid::now_v7(), &command).unwrap();
        tx.commit().unwrap();
        let request = journal::pending(local, &secret).unwrap().unwrap();
        let reply = send(&request);
        assert!(apply(local, &request, &reply));
        reply
    };
    let configuration = client
        .daemon_status(deadline(), None)
        .unwrap()
        .config_sha256;
    let cut = Uuid::now_v7();
    exchange(
        &mut local,
        m::Command::ReconcileBegin {
            snapshot_id: cut,
            begin_sequence: 0,
            end_sequence: 0,
            page_count: 0,
            digest: m::payload_hash(&Vec::<Vec<m::ReconcileAllocation>>::new()).unwrap(),
            configuration_sha256: configuration.clone(),
        },
    );
    assert!(matches!(
        exchange(&mut local, m::Command::ReconcileCommit { snapshot_id: cut }).outcome,
        m::Outcome::Reconciled { .. }
    ));
    assert!(
        matches!(exchange(&mut local,m::Command::InspectPage { after:None,limit:1 }).outcome,
        m::Outcome::InventoryPage { grants,next:None,configuration_sha256 } if grants.is_empty() && configuration_sha256 == configuration)
    );
    assert!(
        matches!(exchange(&mut local,m::Command::InspectPage { after:None,limit:257 }).outcome,
        m::Outcome::Rejected { code,.. } if code == "limit_exceeded")
    );
    let key = m::AllocationKey {
        machine_id: domains.machine_id,
        authority_epoch: challenge.session.authority_epoch,
        domain_id: registration.installation.domain_id,
        manager_store_uuid: registration.manager_store_uuid,
        lease_id: Uuid::now_v7(),
    };
    let candidate = m::Candidate {
        key: key.clone(),
        owner: m::AllocationOwner::Work {
            job_id: durable_id(key.manager_store_uuid),
            attempt_id: durable_id(key.manager_store_uuid),
        },
        revision: 1,
        priority: 0,
        claims: m::Claims {
            scalars: [("cargo_slots".into(), 1)].into(),
            ..Default::default()
        },
        configuration_sha256: configuration.clone(),
        observed: None,
        quiet: None,
    };
    exchange(&mut local, m::Command::CandidateUpsert { candidate });
    let until = deadline();
    let grant = loop {
        let reply = exchange(
            &mut local,
            m::Command::Inspect {
                key: Some(key.clone()),
            },
        );
        if let m::Outcome::Inspection { mut grants, .. } = reply.outcome {
            if grants
                .first()
                .is_some_and(|g| g.state == m::GrantState::Offered)
            {
                break grants.remove(0);
            }
        }
        assert!(Instant::now() < until);
        std::thread::sleep(Duration::from_millis(20));
    };
    let tx = local.transaction().unwrap();
    tx.execute(
        "INSERT INTO local_lease VALUES (?1,'arming')",
        [key.lease_id.to_string()],
    )
    .unwrap();
    journal::enqueue(
        &tx,
        Uuid::now_v7(),
        &m::Command::Arm {
            key: key.clone(),
            offer_nonce: grant.offer_nonce,
        },
    )
    .unwrap();
    tx.commit().unwrap();
    let arm = journal::pending(&local, &secret).unwrap().unwrap();
    let lost = send(&arm);
    assert!(matches!(lost.outcome, m::Outcome::Grant { .. }));
    daemon.kill_and_wait();
    daemon = spawn_daemon(&pinned, &store, &endpoint);
    connect_uninitialized(&pinned, &endpoint);
    drop(local);
    local = manager_database(&path);
    assert_eq!(
        journal::pending(&local, &secret).unwrap(),
        Some(arm.clone())
    );
    let replay = send(&arm);
    assert_eq!(replay.outcome, lost.outcome);
    assert!(apply(&mut local, &arm, &replay));
    assert!(!apply(&mut local, &arm, &replay));
    let mut native = command_spec(temp.path(), "echo admitted>after-manager-release.txt");
    native.resources.cargo_slots = Some(1);
    let native = client
        .submit(
            native,
            &SubmitOptions::new(Uuid::now_v7()),
            deadline(),
            None,
        )
        .unwrap()
        .job_id;
    let intent = m::InvocationIntent {
        invocation_id: durable_id(key.manager_store_uuid),
        containment_id: durable_id(key.manager_store_uuid),
        role: InvocationRole::Primary,
        role_index: 0,
        release_sequence: 1,
        executable_sha256: "a".repeat(64),
        boundary_sha256: "b".repeat(64),
        readiness_challenge: Uuid::now_v7(),
        previous_cleanup: None,
    };
    if reset_after_ticket {
        let tx = local.transaction().unwrap();
        journal::enqueue(
            &tx,
            Uuid::now_v7(),
            &m::Command::AuthorizeInvocation {
                key: key.clone(),
                offer_nonce: grant.offer_nonce,
                intent: intent.clone(),
            },
        )
        .unwrap();
        tx.commit().unwrap();
        let request = journal::pending(&local, &secret).unwrap().unwrap();
        let lost = send(&request);
        assert!(matches!(lost.outcome, m::Outcome::Ticket { .. }));
        daemon.kill_and_wait();
        for name in [
            "stillyard.sqlite3",
            "stillyard.sqlite3-wal",
            "stillyard.sqlite3-shm",
        ] {
            let file = store.join(name);
            if file.exists() {
                std::fs::remove_file(file).unwrap();
            }
        }
        daemon = spawn_daemon(&pinned, &store, &endpoint);
        let reset = connect_uninitialized(&pinned, &endpoint);
        let reconstructed = reset.machine_recover(deadline()).unwrap();
        assert!(reconstructed.blocker.is_some());
        let mut canary = command_spec(temp.path(), "echo recovered>after-retired-ticket.txt");
        canary.resources.cargo_slots = Some(1);
        let waiter = reset
            .submit(
                canary,
                &SubmitOptions::new(Uuid::now_v7()),
                deadline(),
                None,
            )
            .unwrap()
            .job_id;
        let (session, peer) = connect_manager();
        assert_eq!(peer.retired_sequence_floor, request.request_sequence);
        let tx = local.transaction().unwrap();
        assert!(journal::bind(&tx, &session.session, &peer).is_err());
        journal::recovery::begin(&tx, &session.session, &peer).unwrap();
        assert!(journal::recovery::acknowledge_abandoned(&tx, &[request.operation_id]).is_err());
        tx.commit().unwrap();
        drop(local);
        local = manager_database(&path);
        while let Some(command) = journal::recovery::next_page(&local).unwrap() {
            let reply = exchange(&mut local, command);
            let m::Outcome::InventoryPage { grants, .. } = reply.outcome else {
                panic!("recovery inventory page");
            };
            let retained = grants
                .iter()
                .find(|g| g.grant_id == grant.grant_id)
                .unwrap();
            assert_eq!(
                retained.queue_accepted_unix_millis,
                grant.queue_accepted_unix_millis
            );
            assert_eq!(retained.queue_sequence, grant.queue_sequence);
        }
        assert!(!temp.path().join("after-retired-ticket.txt").exists());
        assert_eq!(
            local
                .query_row("SELECT COUNT(*) FROM attached_tickets", [], |r| r
                    .get::<_, u64>(0))
                .unwrap(),
            0
        );
        let tx = local.transaction().unwrap();
        assert!(journal::seal_release(&tx, &key, Uuid::now_v7()).is_err());
        tx.rollback().unwrap();
        let tx = local.transaction().unwrap();
        journal::record_cleanup(
            &tx,
            &m::TicketCleanup {
                invocation_id: intent.invocation_id,
                release_sequence: intent.release_sequence,
                boundary_sha256: intent.boundary_sha256.clone(),
                proof_sha256: "c".repeat(64),
                user_code_released: false,
            },
        )
        .unwrap();
        journal::seal_release(&tx, &key, Uuid::now_v7()).unwrap();
        tx.commit().unwrap();
        for command in journal::recovery::reconciliation(&local).unwrap() {
            let result = exchange(&mut local, command);
            assert!(
                !matches!(result.outcome, m::Outcome::Rejected { .. }),
                "{result:?}"
            );
        }
        let cleared = reset.machine_recover(deadline()).unwrap();
        assert!(cleared.blocker.is_none());
        assert_ne!(cleared.epoch, reconstructed.epoch);
        let abandoned = journal::recovery::abandoned(&local).unwrap();
        assert!(abandoned.iter().any(|(id, _)| *id == request.operation_id));
        let tx = local.transaction().unwrap();
        // The fixture has no started local Invocation; its superseded pending
        // lifecycle intent is settled together with this archive acknowledgement.
        assert_eq!(
            journal::recovery::acknowledge_abandoned(&tx, &[request.operation_id]).unwrap(),
            1
        );
        assert_eq!(
            journal::recovery::acknowledge_abandoned(&tx, &[request.operation_id]).unwrap(),
            0
        );
        tx.commit().unwrap();
        assert_eq!(
            reset.wait(waiter, deadline(), None).unwrap().outcome,
            Some(stillyard::JobOutcome::Succeeded)
        );
        let (session, peer) = connect_manager();
        let tx = local.transaction().unwrap();
        assert!(journal::bind(&tx, &session.session, &peer).is_err());
        journal::recovery::begin(&tx, &session.session, &peer).unwrap();
        tx.commit().unwrap();
        while let Some(command) = journal::recovery::next_page(&local).unwrap() {
            exchange(&mut local, command);
        }
        for command in journal::recovery::reconciliation(&local).unwrap() {
            let result = exchange(&mut local, command);
            assert!(
                !matches!(result.outcome, m::Outcome::Rejected { .. }),
                "{result:?}"
            );
        }
        let mut new_candidate = grant.candidate.clone();
        new_candidate.key.authority_epoch = session.session.authority_epoch;
        new_candidate.key.lease_id = Uuid::now_v7();
        assert!(matches!(
            exchange(
                &mut local,
                m::Command::CandidateUpsert {
                    candidate: new_candidate
                }
            )
            .outcome,
            m::Outcome::Accepted { .. }
        ));
        assert_eq!(
            local
                .query_row("SELECT COUNT(*) FROM local_invocation", [], |r| r
                    .get::<_, u64>(0))
                .unwrap(),
            0
        );
        daemon.kill_and_wait();
        return;
    }
    let reply = exchange(
        &mut local,
        m::Command::AuthorizeInvocation {
            key: key.clone(),
            offer_nonce: grant.offer_nonce,
            intent: intent.clone(),
        },
    );
    let m::Outcome::Ticket { ticket } = reply.outcome else {
        panic!("ticket not authorized");
    };
    let tx = local.transaction().unwrap();
    assert!(journal::consume_ticket(&tx, &ticket).unwrap());
    tx.execute(
        "INSERT INTO local_invocation VALUES (?1,1)",
        [intent.invocation_id.to_string()],
    )
    .unwrap();
    tx.commit().unwrap();
    drop(local);
    local = manager_database(&path);
    let tx = local.transaction().unwrap();
    assert!(!journal::consume_ticket(&tx, &ticket).unwrap());
    tx.commit().unwrap();
    // A reset manager has a new SQLite identity. Its empty local database
    // cannot authenticate the old manager's release rights or free the token.
    let reset_store = Uuid::now_v7();
    let mut empty_manager = manager_database(&temp.path().join("reset-manager.sqlite3"));
    let tx = empty_manager.transaction().unwrap();
    journal::initialize(&tx, reset_store).unwrap();
    assert!(journal::bind(&tx, &challenge.session, &participant).is_err());
    tx.rollback().unwrap();
    let changed_hello = m::ConnectHello {
        installation_nonce: registration.installation.installation_nonce,
        manager_store_uuid: reset_store,
        executor_incarnation: Uuid::now_v7(),
        executor_protocol: 25,
        executor_nonce: [8; 32],
    };
    let changed_path = temp.path().join("reset-manager-hello.json");
    std::fs::write(&changed_path, serde_json::to_vec(&changed_hello).unwrap()).unwrap();
    assert!(
        !Command::new(&pinned)
            .args([
                "--endpoint",
                &endpoint,
                "machine",
                "connect-begin",
                "--spec"
            ])
            .arg(changed_path)
            .output()
            .unwrap()
            .status
            .success()
    );
    assert_eq!(
        client
            .authority_status(deadline(), None)
            .unwrap()
            .machine_obligations
            .len(),
        1
    );
    assert!(!temp.path().join("after-manager-release.txt").exists());

    let (reconnected, participant) = connect_manager();
    let tx = local.transaction().unwrap();
    journal::bind(&tx, &reconnected.session, &participant).unwrap();
    tx.commit().unwrap();
    let records = vec![vec![m::ReconcileAllocation {
        key: key.clone(),
        offer_nonce: grant.offer_nonce,
        tickets: vec![intent.clone()],
        sealed_release: None,
    }]];
    let cut = Uuid::now_v7();
    exchange(
        &mut local,
        m::Command::ReconcileBegin {
            snapshot_id: cut,
            begin_sequence: 0,
            end_sequence: participant.accepted_sequence,
            page_count: 1,
            digest: m::payload_hash(&records).unwrap(),
            configuration_sha256: configuration,
        },
    );
    exchange(
        &mut local,
        m::Command::ReconcilePage {
            snapshot_id: cut,
            index: 0,
            allocations: records[0].clone(),
        },
    );
    assert!(matches!(
        exchange(&mut local, m::Command::ReconcileCommit { snapshot_id: cut }).outcome,
        m::Outcome::Reconciled { .. }
    ));
    assert!(!temp.path().join("after-manager-release.txt").exists());
    let tx = local.transaction().unwrap();
    assert!(
        journal::consume_ticket(&tx, &ticket).is_err(),
        "old session could release the ticket after reconnect"
    );
    assert!(journal::seal_release(&tx, &key, Uuid::now_v7()).is_err());
    tx.rollback().unwrap();
    let uncertain = exchange(
        &mut local,
        m::Command::ReportUncertain {
            key: key.clone(),
            offer_nonce: grant.offer_nonce,
            reason: "fixture boundary requires explicit empty proof".into(),
        },
    );
    assert!(
        matches!(uncertain.outcome,m::Outcome::Grant { grant } if grant.state==m::GrantState::Uncertain && grant.uncertainty_reason.is_some())
    );
    assert_eq!(
        client
            .authority_status(deadline(), None)
            .unwrap()
            .machine_obligations[0]
            .state,
        m::GrantState::Uncertain
    );
    assert!(!temp.path().join("after-manager-release.txt").exists());
    let preview = client
        .machine_clearance_preview(key.domain_id, deadline())
        .unwrap();
    assert_eq!(preview.sha256, m::payload_hash(&preview.inventory).unwrap());
    assert_eq!(preview.inventory.manager_store_uuid, key.manager_store_uuid);
    assert_eq!(preview.inventory.grants.len(), 1);
    assert_eq!(preview.inventory.grants[0].tickets, vec![intent.clone()]);
    assert_eq!(
        preview,
        client
            .machine_clearance_preview(key.domain_id, deadline())
            .unwrap()
    );
    let cleanup = m::TicketCleanup {
        invocation_id: intent.invocation_id,
        release_sequence: 1,
        boundary_sha256: intent.boundary_sha256,
        proof_sha256: "c".repeat(64),
        user_code_released: true,
    };
    let tx = local.transaction().unwrap();
    journal::record_cleanup(&tx, &cleanup).unwrap();
    journal::seal_release(&tx, &key, Uuid::now_v7()).unwrap();
    tx.execute("UPDATE local_lease SET state='releasing'", [])
        .unwrap();
    tx.commit().unwrap();
    let release = journal::pending(&local, &secret).unwrap().unwrap();
    let lost = send(&release);
    assert!(matches!(lost.outcome, m::Outcome::Released { .. }));
    assert_eq!(
        client.wait(native, deadline(), None).unwrap().outcome,
        Some(stillyard::JobOutcome::Succeeded)
    );
    daemon.kill_and_wait();
    let _replacement = spawn_daemon(&pinned, &store, &endpoint);
    connect_uninitialized(&pinned, &endpoint);
    drop(local);
    local = manager_database(&path);
    assert_eq!(
        journal::pending(&local, &secret).unwrap(),
        Some(release.clone())
    );
    let repeated = send(&release);
    assert_eq!(repeated.outcome, lost.outcome);
    assert!(apply(&mut local, &release, &repeated));
    assert!(!apply(&mut local, &release, &repeated));
    assert_eq!(
        client.wait(native, deadline(), None).unwrap().outcome,
        Some(stillyard::JobOutcome::Succeeded)
    );
    assert_eq!(
        local
            .query_row("SELECT SUM(starts) FROM local_invocation", [], |r| r
                .get::<_, u64>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        local
            .query_row("SELECT state FROM local_lease", [], |r| r
                .get::<_, String>(0))
            .unwrap(),
        "released"
    );
    assert!(matches!(
        exchange(
            &mut local,
            m::Command::Acknowledge {
                through_sequence: release.request_sequence
            }
        )
        .outcome,
        m::Outcome::Acknowledged { .. }
    ));
    assert!(journal::pending(&local, &secret).unwrap().is_none());
    let mut reused = grant.candidate.clone();
    reused.revision += 1;
    assert!(
        matches!(exchange(&mut local,m::Command::CandidateUpsert { candidate:reused }).outcome,
        m::Outcome::Rejected { code,.. } if code=="stale"),
        "compaction allowed a retired Lease key to acquire another Grant"
    );
    assert!(
        client
            .authority_status(deadline(), None)
            .unwrap()
            .machine_obligations
            .is_empty()
    );
}
