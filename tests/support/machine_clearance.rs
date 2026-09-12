use super::*;
use stillyard::machine as m;

#[test]
fn pending_pairing_can_be_abandoned_without_inventing_cleanup() {
    pending_pairing_fixture(false);
    pending_pairing_fixture(true);
}

fn pending_pairing_fixture(reset: bool) {
    let temp = tempfile::tempdir().unwrap();
    let pinned = copied_daemon(temp.path());
    let store = temp.path().join("pending-pairing");
    std::fs::create_dir_all(&store).unwrap();
    let endpoint = format!(r"\\.\pipe\stillyard-abandon-{}", Uuid::now_v7());
    let spawn = || {
        ChildGuard::new(
            Command::new(&pinned)
                .args(["--endpoint", &endpoint, "daemon", "--store"])
                .arg(&store)
                .env("STILLYARD_ISOLATED_MACHINE_FAULT_ROOT", &store)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        )
    };
    let mut daemon = spawn();
    let client = connect(&pinned, &endpoint);
    let deadline = || Instant::now() + Duration::from_secs(10);
    let domains = client
        .authority_status(deadline(), None)
        .unwrap()
        .domains
        .unwrap();
    let secret = m::PairingSecret::generate().unwrap();
    let registration = m::PairingRegistration {
        installation: m::InstallationIdentity {
            installation_nonce: Uuid::now_v7(),
            domain_id: stillyard::ExecutionDomainId(Uuid::now_v7()),
            owner_uid: 1000,
            runtime_registration: "interrupted-pairing".into(),
            role: m::ParticipantRole::Executor,
        },
        manager_store_uuid: Uuid::now_v7(),
        parent_domain: domains.machine_scope,
        budgets: Default::default(),
        aliases: Default::default(),
        secret: *secret.anchor_bytes(),
    };
    use std::io::Write;
    let mut f = std::fs::File::create(store.join("machine-fault.json")).unwrap();
    f.write_all(
        &serde_json::to_vec(&(
            registration.installation.domain_id.0,
            "after_pairing_journal",
        ))
        .unwrap(),
    )
    .unwrap();
    f.sync_all().unwrap();
    drop(f);
    assert!(
        client
            .pair_machine_domain(registration.clone(), deadline())
            .is_err()
    );
    assert_eq!(
        wait_for_exit(daemon.child_mut(), Duration::from_secs(10)).code(),
        Some(86)
    );
    daemon.kill_and_wait();
    if reset {
        for name in [
            "stillyard.sqlite3",
            "stillyard.sqlite3-wal",
            "stillyard.sqlite3-shm",
        ] {
            let path = store.join(name);
            if path.exists() {
                std::fs::remove_file(path).unwrap();
            }
        }
    }
    daemon = spawn();
    connect_uninitialized(&pinned, &endpoint);
    assert!(
        client
            .authority_status(deadline(), None)
            .unwrap()
            .blocker
            .is_some()
    );
    let p = client
        .machine_clearance_preview(registration.installation.domain_id, deadline())
        .unwrap();
    assert!(!p.inventory.committed);
    assert!(p.inventory.session.is_none());
    assert!(p.inventory.grants.is_empty());
    let r = client
        .retire_machine_domain(
            m::DomainRetirementRequest {
                operation_id: Uuid::now_v7(),
                domain_id: registration.installation.domain_id,
                expected_inventory_sha256: p.sha256,
                reason: "abandon interrupted registration".into(),
                accept_risk: false,
            },
            deadline(),
        )
        .unwrap();
    assert!(!r.risk_accepted);
    assert_eq!(r.tickets_retired, 0);
    if reset {
        assert!(
            client
                .machine_recover(deadline())
                .unwrap()
                .blocker
                .is_none()
        );
    }
    assert!(
        client
            .authority_status(deadline(), None)
            .unwrap()
            .blocker
            .is_none()
    );
    assert!(
        client
            .pair_machine_domain(registration, deadline())
            .is_err()
    );
    let job = client
        .submit(
            command_spec(temp.path(), "echo recovered>after-abandon.txt"),
            &SubmitOptions::new(Uuid::now_v7()),
            deadline(),
            None,
        )
        .unwrap()
        .job_id;
    assert_eq!(
        client.wait(job, deadline(), None).unwrap().outcome,
        Some(stillyard::JobOutcome::Succeeded)
    );
    assert!(temp.path().join("after-abandon.txt").exists());
    daemon.kill_and_wait();
}

#[test]
fn owner_retirement_fences_lost_manager_and_recovers_every_durable_boundary() {
    for stage in [
        None,
        Some("before_retirement_journal"),
        Some("after_retirement_journal"),
        Some("after_retirement_sql"),
        Some("after_retirement_ack"),
    ] {
        retirement_fixture(stage, false, false);
    }
    retirement_fixture(None, true, false);
    retirement_fixture(Some("after_retirement_journal"), false, true);
}

fn retirement_fixture(stage: Option<&str>, reset: bool, divergent: bool) {
    let temp = tempfile::tempdir().unwrap();
    let pinned = copied_daemon(temp.path());
    let store = temp.path().join("retirement-store");
    std::fs::create_dir_all(&store).unwrap();
    let mut config = HostConfig::default();
    config.resources.cargo_slots = 1;
    std::fs::write(
        store.join("config.json"),
        serde_json::to_vec(&config).unwrap(),
    )
    .unwrap();
    let endpoint = format!(r"\\.\pipe\stillyard-retirement-{}", Uuid::now_v7());
    let spawn = || {
        ChildGuard::new(
            Command::new(&pinned)
                .args(["--endpoint", &endpoint, "daemon", "--store"])
                .arg(&store)
                .env("STILLYARD_ISOLATED_MACHINE_FAULT_ROOT", &store)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        )
    };
    let mut daemon = spawn();
    let client = connect(&pinned, &endpoint);
    let deadline = || Instant::now() + Duration::from_secs(10);
    let original = client.authority_status(deadline(), None).unwrap();
    let domains = original.domains.unwrap();
    let secret = m::PairingSecret::generate().unwrap();
    let registration = m::PairingRegistration {
        installation: m::InstallationIdentity {
            installation_nonce: Uuid::now_v7(),
            domain_id: stillyard::ExecutionDomainId(Uuid::now_v7()),
            owner_uid: 1000,
            runtime_registration: "lost-manager-fixture".into(),
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
    // Another participant must survive retirement and keep its exact session.
    let mut other = registration.clone();
    other.installation.domain_id = stillyard::ExecutionDomainId(Uuid::now_v7());
    other.installation.installation_nonce = Uuid::now_v7();
    other.manager_store_uuid = Uuid::now_v7();
    let mut other_before = client
        .pair_machine_domain(other.clone(), deadline())
        .unwrap();
    let call = |command: &str, input: serde_json::Value| {
        let path = temp.path().join(format!("request-{}.json", Uuid::now_v7()));
        std::fs::write(&path, serde_json::to_vec(&input).unwrap()).unwrap();
        Command::new(&pinned)
            .args(["--endpoint", &endpoint, "machine", command, "--spec"])
            .arg(path)
            .output()
            .unwrap()
    };
    let hello = m::ConnectHello {
        installation_nonce: registration.installation.installation_nonce,
        manager_store_uuid: registration.manager_store_uuid,
        executor_incarnation: Uuid::now_v7(),
        executor_protocol: 25,
        executor_nonce: [9; 32],
    };
    let begin = call("connect-begin", serde_json::to_value(&hello).unwrap());
    assert!(
        begin.status.success(),
        "{}",
        String::from_utf8_lossy(&begin.stderr)
    );
    let challenge: m::ConnectChallenge = serde_json::from_slice(&begin.stdout).unwrap();
    let tag = secret.sign_challenge(&challenge).unwrap();
    assert!(
        call(
            "connect-finish",
            serde_json::json!({"challenge":challenge,"tag":tag})
        )
        .status
        .success()
    );
    let sequence = std::cell::Cell::new(0);
    let exchange = |command: m::Command| {
        sequence.set(sequence.get() + 1);
        let mut request = m::Request::new(
            challenge.session.clone(),
            sequence.get(),
            Uuid::now_v7(),
            command,
        )
        .unwrap();
        secret.sign_request(&mut request).unwrap();
        let output = call("exchange", serde_json::to_value(&request).unwrap());
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        (
            request,
            serde_json::from_slice::<m::Reply>(&output.stdout).unwrap(),
        )
    };
    let configuration = client
        .daemon_status(deadline(), None)
        .unwrap()
        .config_sha256;
    let cut = Uuid::now_v7();
    exchange(m::Command::ReconcileBegin {
        snapshot_id: cut,
        begin_sequence: 0,
        end_sequence: 0,
        page_count: 0,
        digest: m::payload_hash(&Vec::<Vec<m::ReconcileAllocation>>::new()).unwrap(),
        configuration_sha256: configuration.clone(),
    });
    assert!(matches!(
        exchange(m::Command::ReconcileCommit { snapshot_id: cut })
            .1
            .outcome,
        m::Outcome::Reconciled { .. }
    ));
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
    exchange(m::Command::CandidateUpsert { candidate });
    let until = deadline();
    let offer = loop {
        if let m::Outcome::Inspection { mut grants, .. } = exchange(m::Command::Inspect {
            key: Some(key.clone()),
        })
        .1
        .outcome
        {
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
    let old_preview = client
        .machine_clearance_preview(key.domain_id, deadline())
        .unwrap();
    assert!(old_preview.inventory.grants.is_empty());
    let (old_arm, armed) = exchange(m::Command::Arm {
        key: key.clone(),
        offer_nonce: offer.offer_nonce,
    });
    assert!(matches!(armed.outcome, m::Outcome::Grant { .. }));
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
    assert!(matches!(
        exchange(m::Command::AuthorizeInvocation {
            key: key.clone(),
            offer_nonce: offer.offer_nonce,
            intent
        })
        .1
        .outcome,
        m::Outcome::Ticket { .. }
    ));
    let other_rights = if (stage.is_none() && !reset) || divergent {
        let hello = m::ConnectHello {
            installation_nonce: other.installation.installation_nonce,
            manager_store_uuid: other.manager_store_uuid,
            executor_incarnation: Uuid::now_v7(),
            executor_protocol: 25,
            executor_nonce: [8; 32],
        };
        let output = call("connect-begin", serde_json::to_value(hello).unwrap());
        let challenge: m::ConnectChallenge = serde_json::from_slice(&output.stdout).unwrap();
        let tag = secret.sign_challenge(&challenge).unwrap();
        assert!(
            call(
                "connect-finish",
                serde_json::json!({"challenge":challenge,"tag":tag})
            )
            .status
            .success()
        );
        let sequence = std::cell::Cell::new(0);
        let send = |command: m::Command| {
            sequence.set(sequence.get() + 1);
            let mut r = m::Request::new(
                challenge.session.clone(),
                sequence.get(),
                Uuid::now_v7(),
                command,
            )
            .unwrap();
            secret.sign_request(&mut r).unwrap();
            let out = call("exchange", serde_json::to_value(r).unwrap());
            assert!(
                out.status.success(),
                "{}",
                String::from_utf8_lossy(&out.stderr)
            );
            serde_json::from_slice::<m::Reply>(&out.stdout).unwrap()
        };
        let cut = Uuid::now_v7();
        send(m::Command::ReconcileBegin {
            snapshot_id: cut,
            begin_sequence: 0,
            end_sequence: 0,
            page_count: 0,
            digest: m::payload_hash(&Vec::<Vec<m::ReconcileAllocation>>::new()).unwrap(),
            configuration_sha256: configuration.clone(),
        });
        send(m::Command::ReconcileCommit { snapshot_id: cut });
        let other_key = m::AllocationKey {
            machine_id: domains.machine_id,
            authority_epoch: challenge.session.authority_epoch,
            domain_id: other.installation.domain_id,
            manager_store_uuid: other.manager_store_uuid,
            lease_id: Uuid::now_v7(),
        };
        send(m::Command::CandidateUpsert {
            candidate: m::Candidate {
                key: other_key.clone(),
                owner: m::AllocationOwner::Work {
                    job_id: durable_id(other.manager_store_uuid),
                    attempt_id: durable_id(other.manager_store_uuid),
                },
                revision: 1,
                priority: 0,
                claims: Default::default(),
                configuration_sha256: configuration.clone(),
                observed: None,
                quiet: None,
            },
        });
        let until = deadline();
        let offered = loop {
            if let m::Outcome::Inspection { mut grants, .. } = send(m::Command::Inspect {
                key: Some(other_key.clone()),
            })
            .outcome
            {
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
        assert!(matches!(
            send(m::Command::Arm {
                key: other_key.clone(),
                offer_nonce: offered.offer_nonce
            })
            .outcome,
            m::Outcome::Grant { .. }
        ));
        assert!(matches!(
            send(m::Command::AuthorizeInvocation {
                key: other_key,
                offer_nonce: offered.offer_nonce,
                intent: m::InvocationIntent {
                    invocation_id: durable_id(other.manager_store_uuid),
                    containment_id: durable_id(other.manager_store_uuid),
                    role: InvocationRole::Primary,
                    role_index: 0,
                    release_sequence: 1,
                    executable_sha256: "c".repeat(64),
                    boundary_sha256: "d".repeat(64),
                    readiness_challenge: Uuid::now_v7(),
                    previous_cleanup: None
                }
            })
            .outcome,
            m::Outcome::Ticket { .. }
        ));
        other_before = client
            .machine_participant(other.installation.domain_id, deadline())
            .unwrap();
        Some(
            client
                .machine_clearance_preview(other.installation.domain_id, deadline())
                .unwrap(),
        )
    } else {
        None
    };
    if reset {
        // Simultaneous manager loss and coordinator SQL replacement retains the
        // external inventory; only this isolated test store is removed.
        daemon.kill_and_wait();
        for name in [
            "stillyard.sqlite3",
            "stillyard.sqlite3-wal",
            "stillyard.sqlite3-shm",
        ] {
            let p = store.join(name);
            if p.exists() {
                std::fs::remove_file(p).unwrap();
            }
        }
        daemon = spawn();
        connect_uninitialized(&pinned, &endpoint);
        assert!(
            client
                .machine_recover(deadline())
                .unwrap()
                .blocker
                .is_some()
        );
    }
    let live_native = if stage.is_none() && !reset {
        let job=client.submit(command_spec(temp.path(),"echo live>unrelated-native.txt & C:\\Windows\\System32\\ping.exe -n 30 127.0.0.1 >nul"),&SubmitOptions::new(Uuid::now_v7()),deadline(),None).unwrap().job_id;
        let until = deadline();
        while !temp.path().join("unrelated-native.txt").exists() {
            assert!(Instant::now() < until);
            std::thread::sleep(Duration::from_millis(20));
        }
        let permission=client.authority_status(deadline(),None).unwrap().native_obligations.into_iter().find(|p|matches!(p.allocation.owner,m::AllocationOwner::Work {job_id,..} if job_id==job)).unwrap();
        Some((job, permission))
    } else {
        None
    };
    let mut native = command_spec(temp.path(), "echo admitted>after-retirement.txt");
    native.resources.cargo_slots = Some(1);
    let mut waiter = client
        .submit(
            native,
            &SubmitOptions::new(Uuid::now_v7()),
            deadline(),
            None,
        )
        .unwrap()
        .job_id;
    let preview = client
        .machine_clearance_preview(key.domain_id, deadline())
        .unwrap();
    assert_eq!(preview.inventory.grants.len(), 1);
    assert_eq!(preview.inventory.grants[0].tickets.len(), 1);
    let mut request = m::DomainRetirementRequest {
        operation_id: Uuid::now_v7(),
        domain_id: key.domain_id,
        expected_inventory_sha256: old_preview.sha256,
        reason: "isolated lost manager store; no cleanup evidence".into(),
        accept_risk: true,
    };
    assert!(
        client
            .retire_machine_domain(request.clone(), deadline())
            .is_err()
    );
    request.expected_inventory_sha256 = preview.sha256.clone();
    request.accept_risk = false;
    assert!(
        client
            .retire_machine_domain(request.clone(), deadline())
            .is_err()
    );
    assert_eq!(
        client
            .authority_status(deadline(), None)
            .unwrap()
            .machine_obligations
            .len(),
        1 + usize::from(other_rights.is_some())
    );
    assert!(!temp.path().join("after-retirement.txt").exists());
    request.accept_risk = true;
    if let Some(stage) = stage {
        use std::io::Write;
        let mut file = std::fs::File::create(store.join("machine-fault.json")).unwrap();
        file.write_all(&serde_json::to_vec(&(request.operation_id, stage)).unwrap())
            .unwrap();
        file.sync_all().unwrap();
        drop(file);
        assert!(
            client
                .retire_machine_domain(request.clone(), deadline())
                .is_err()
        );
        assert_eq!(
            wait_for_exit(daemon.child_mut(), Duration::from_secs(10)).code(),
            Some(86)
        );
        let registry: serde_json::Value =
            serde_json::from_slice(&std::fs::read(store.join("authority/registry.json")).unwrap())
                .unwrap();
        let charged = registry["payload"]["machine_permissions"]
            .as_object()
            .map_or(0, |v| v.len());
        assert_eq!(
            charged,
            usize::from(stage != "after_retirement_ack") + usize::from(other_rights.is_some())
        );
        daemon.kill_and_wait();
        if stage == "after_retirement_journal" && !divergent {
            let audit = store
                .join("authority")
                .join(format!("domain-retirement-{}.json", request.operation_id));
            let backup = audit.with_extension("fixture-backup");
            std::fs::rename(&audit, &backup).unwrap();
            daemon = spawn();
            let broken = connect_uninitialized(&pinned, &endpoint);
            let state = broken.authority_status(deadline(), None).unwrap();
            assert_eq!(state.blocker.as_deref(), Some("authority_history_unknown"));
            assert!(state.detail.unwrap().contains("domain-retirement-"));
            assert!(
                broken
                    .machine_recover(deadline())
                    .unwrap()
                    .blocker
                    .is_some()
            );
            assert!(!temp.path().join("after-retirement.txt").exists());
            daemon.kill_and_wait();
            // Restore the exact durable audit, never synthesize a replacement
            // clearance or treat the missing file as a proof of empty work.
            std::fs::rename(backup, audit).unwrap();
        }
        if divergent {
            // A rolled-back/corrupt SQL projection contains an Armed row absent
            // from the accepted current external inventory. It must gate repair
            // without making the daemon itself unavailable for diagnosis.
            let db = rusqlite::Connection::open(store.join("stillyard.sqlite3")).unwrap();
            let original_key = serde_json::to_string(&key).unwrap();
            let mut extra = preview.inventory.grants[0].clone();
            extra.grant_id = durable_id(extra.grant_id.store_uuid());
            extra.candidate.key.lease_id = Uuid::now_v7();
            let extra_key = serde_json::to_string(&extra.candidate.key).unwrap();
            db.execute("INSERT INTO machine_candidates SELECT ?1,domain_id,?2,queue_owner,'withdrawn',expires_ms,revision,NULL,NULL FROM machine_candidates WHERE allocation_key=?3",rusqlite::params![extra_key,serde_json::to_string(&extra.candidate).unwrap(),original_key]).unwrap();
            db.execute("INSERT INTO machine_grants SELECT ?1,?2,physical_claims_json,'armed',deadline_ms FROM machine_grants WHERE allocation_key=?3",rusqlite::params![extra_key,serde_json::to_string(&extra).unwrap(),original_key]).unwrap();
        }
        daemon = spawn();
        connect_uninitialized(&pinned, &endpoint);
        if divergent {
            let gated = client.machine_recover(deadline()).unwrap();
            assert_eq!(gated.pending_machine_operation, Some(request.operation_id));
            assert_eq!(gated.machine_obligations.len(), 2);
            let pending_peer = call("connect-begin", serde_json::to_value(&hello).unwrap());
            assert!(!pending_peer.status.success());
            let diagnostic = String::from_utf8_lossy(&pending_peer.stderr);
            assert!(
                diagnostic.contains("retirement_pending")
                    && diagnostic.contains(&request.operation_id.to_string()),
                "{diagnostic}"
            );
            assert!(
                client
                    .machine_clearance_preview(key.domain_id, deadline())
                    .is_ok()
            );
            assert!(!temp.path().join("after-retirement.txt").exists());
            client.cancel(&[waiter], deadline(), None).unwrap();
            assert_eq!(
                client.wait(waiter, deadline(), None).unwrap().outcome,
                Some(stillyard::JobOutcome::Canceled)
            );
            let refused = client
                .submit(
                    command_spec(temp.path(), "exit 0"),
                    &SubmitOptions::new(Uuid::now_v7()),
                    deadline(),
                    None,
                )
                .unwrap_err();
            assert!(
                refused.to_string().contains("authority_repair_pending"),
                "{refused}"
            );
            let repaired = client.machine_recover(deadline()).unwrap();
            assert!(repaired.pending_machine_operation.is_none());
            assert!(
                repaired.blocker.is_some(),
                "unrelated participant still requires reconciliation"
            );
            let p = client
                .machine_clearance_preview(other.installation.domain_id, deadline())
                .unwrap();
            assert_eq!(
                &p,
                other_rights.as_ref().unwrap(),
                "repair changed the surviving manager's issued inventory"
            );
            let grant = p.inventory.grants[0].clone();
            let db = rusqlite::Connection::open(store.join("stillyard.sqlite3")).unwrap();
            for intent in &grant.tickets {
                let stored: (String, String) = db.query_row("SELECT containment_id,intent_json FROM machine_ticket_identities WHERE invocation_id=?1", [intent.invocation_id.to_string()], |r| Ok((r.get(0)?,r.get(1)?))).unwrap();
                assert_eq!(stored.0, intent.containment_id.to_string());
                assert_eq!(
                    serde_json::from_str::<m::InvocationIntent>(&stored.1).unwrap(),
                    *intent
                );
            }
            drop(db);
            // The surviving manager authenticates, reconciles and seals its own
            // rights after the SQL rebuild; no risk retirement is used for it.
            let hello = m::ConnectHello {
                installation_nonce: other.installation.installation_nonce,
                manager_store_uuid: other.manager_store_uuid,
                executor_incarnation: Uuid::now_v7(),
                executor_protocol: 25,
                executor_nonce: [6; 32],
            };
            let out = call("connect-begin", serde_json::to_value(hello).unwrap());
            assert!(
                out.status.success(),
                "{}",
                String::from_utf8_lossy(&out.stderr)
            );
            let peer: m::ConnectChallenge = serde_json::from_slice(&out.stdout).unwrap();
            let tag = secret.sign_challenge(&peer).unwrap();
            let out = call(
                "connect-finish",
                serde_json::json!({"challenge":peer,"tag":tag}),
            );
            assert!(
                out.status.success(),
                "{}",
                String::from_utf8_lossy(&out.stderr)
            );
            let participant: m::ParticipantSnapshot = serde_json::from_slice(&out.stdout).unwrap();
            let sequence = std::cell::Cell::new(participant.accepted_sequence);
            let send = |command: m::Command| {
                sequence.set(sequence.get() + 1);
                let mut request = m::Request::new(
                    peer.session.clone(),
                    sequence.get(),
                    Uuid::now_v7(),
                    command,
                )
                .unwrap();
                secret.sign_request(&mut request).unwrap();
                let out = call("exchange", serde_json::to_value(request).unwrap());
                assert!(
                    out.status.success(),
                    "{}",
                    String::from_utf8_lossy(&out.stderr)
                );
                serde_json::from_slice::<m::Reply>(&out.stdout)
                    .unwrap()
                    .outcome
            };
            let mut new_candidate = grant.candidate.clone();
            new_candidate.key.lease_id = Uuid::now_v7();
            assert!(
                matches!(
                    send(m::Command::CandidateUpsert {
                        candidate: new_candidate
                    }),
                    m::Outcome::Rejected { .. }
                ),
                "reset gate admitted fresh attached work"
            );
            let page = vec![m::ReconcileAllocation {
                key: grant.candidate.key.clone(),
                offer_nonce: grant.offer_nonce,
                tickets: grant.tickets.clone(),
                sealed_release: Some(m::SealedRelease {
                    key: grant.candidate.key.clone(),
                    offer_nonce: grant.offer_nonce,
                    sealed_sequence: 1,
                    tickets: grant
                        .tickets
                        .iter()
                        .map(|intent| m::TicketCleanup {
                            invocation_id: intent.invocation_id,
                            release_sequence: intent.release_sequence,
                            boundary_sha256: intent.boundary_sha256.clone(),
                            proof_sha256: m::payload_hash(intent).unwrap(),
                            user_code_released: true,
                        })
                        .collect(),
                }),
            }];
            let cut = Uuid::now_v7();
            let end_sequence = sequence.get();
            assert!(matches!(
                send(m::Command::ReconcileBegin {
                    snapshot_id: cut,
                    begin_sequence: 0,
                    end_sequence,
                    page_count: 1,
                    digest: m::payload_hash(&vec![page.clone()]).unwrap(),
                    configuration_sha256: configuration.clone(),
                }),
                m::Outcome::Accepted { .. }
            ));
            assert!(matches!(
                send(m::Command::ReconcilePage {
                    snapshot_id: cut,
                    index: 0,
                    allocations: page
                }),
                m::Outcome::Accepted { .. }
            ));
            assert!(matches!(
                send(m::Command::ReconcileCommit { snapshot_id: cut }),
                m::Outcome::Reconciled { .. }
            ));
            assert!(
                client
                    .authority_status(deadline(), None)
                    .unwrap()
                    .machine_obligations
                    .is_empty()
            );
            assert!(
                client
                    .machine_clearance_preview(other.installation.domain_id, deadline())
                    .unwrap()
                    .inventory
                    .grants
                    .is_empty()
            );
            daemon.kill_and_wait();
            daemon = spawn();
            connect_uninitialized(&pinned, &endpoint);
            assert!(
                client
                    .machine_recover(deadline())
                    .unwrap()
                    .blocker
                    .is_none()
            );
            let mut replacement = command_spec(temp.path(), "echo repaired>after-retirement.txt");
            replacement.resources.cargo_slots = Some(1);
            waiter = client
                .submit(
                    replacement,
                    &SubmitOptions::new(Uuid::now_v7()),
                    deadline(),
                    None,
                )
                .unwrap()
                .job_id;
        } else if stage != "before_retirement_journal" {
            // No clearance replay or machine RPC may be needed for startup to
            // finish the durable decision and unblock ordinary native work.
            assert_eq!(
                client.wait(waiter, deadline(), None).unwrap().outcome,
                Some(stillyard::JobOutcome::Succeeded)
            );
            assert!(
                client
                    .authority_status(deadline(), None)
                    .unwrap()
                    .pending_machine_operation
                    .is_none()
            );
        }
    }
    let receipt = client
        .retire_machine_domain(request.clone(), deadline())
        .unwrap();
    assert_eq!(
        client
            .retire_machine_domain(request.clone(), deadline())
            .unwrap(),
        receipt
    );
    assert!(receipt.risk_accepted);
    assert_eq!(receipt.grants_retired, 1);
    assert_eq!(receipt.tickets_retired, 1);
    assert!(receipt.requester_principal.starts_with("S-1-"));
    let audit: m::DomainRetirementAudit = serde_json::from_slice(
        &std::fs::read(
            store
                .join("authority")
                .join(format!("domain-retirement-{}.json", request.operation_id)),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(audit.preview, preview);
    assert_eq!(audit.receipt().unwrap(), receipt);
    let mut conflict = request.clone();
    conflict.reason.push('!');
    assert!(client.retire_machine_domain(conflict, deadline()).is_err());
    assert!(
        !call("exchange", serde_json::to_value(old_arm).unwrap())
            .status
            .success()
    );
    let retired = call("connect-begin", serde_json::to_value(hello).unwrap());
    assert!(!retired.status.success());
    assert!(String::from_utf8_lossy(&retired.stderr).contains(&request.operation_id.to_string()));
    assert!(
        !call(
            "connect-finish",
            serde_json::json!({"challenge":challenge,"tag":tag})
        )
        .status
        .success()
    );
    let mut fresh_operation = request.clone();
    fresh_operation.operation_id = Uuid::now_v7();
    let rejected = client
        .retire_machine_domain(fresh_operation, deadline())
        .unwrap_err()
        .to_string();
    assert!(rejected.contains(&request.operation_id.to_string()));
    for reused in 0..3 {
        let mut r = registration.clone();
        if reused != 0 {
            r.installation.domain_id = stillyard::ExecutionDomainId(Uuid::now_v7());
        }
        if reused != 1 {
            r.installation.installation_nonce = Uuid::now_v7();
        }
        if reused != 2 {
            r.manager_store_uuid = Uuid::now_v7();
        }
        assert!(client.pair_machine_domain(r, deadline()).is_err());
    }
    if reset {
        // The second, empty participant still owns reconciliation. Its explicit
        // no-risk retirement demonstrates that the lost domain clears no peer.
        assert!(
            client
                .machine_recover(deadline())
                .unwrap()
                .blocker
                .is_some()
        );
        assert!(!temp.path().join("after-retirement.txt").exists());
        let p = client
            .machine_clearance_preview(other.installation.domain_id, deadline())
            .unwrap();
        assert!(p.inventory.grants.is_empty());
        client
            .retire_machine_domain(
                m::DomainRetirementRequest {
                    operation_id: Uuid::now_v7(),
                    domain_id: other.installation.domain_id,
                    expected_inventory_sha256: p.sha256,
                    reason: "empty isolated participant retired".into(),
                    accept_risk: false,
                },
                deadline(),
            )
            .unwrap();
        let recovered = client.machine_recover(deadline()).unwrap();
        assert!(recovered.blocker.is_none(), "{:?}", recovered.blocker);
        assert_ne!(recovered.epoch, original.epoch);
    } else if !divergent {
        assert_eq!(
            client
                .machine_participant(other.installation.domain_id, deadline())
                .unwrap(),
            other_before
        );
        if let Some(expected) = other_rights {
            assert_eq!(
                client
                    .machine_clearance_preview(other.installation.domain_id, deadline())
                    .unwrap(),
                expected,
                "retirement changed another manager's issued rights"
            );
        }
    }
    let result = client.wait(waiter, deadline(), None).unwrap();
    assert_eq!(result.outcome, Some(stillyard::JobOutcome::Succeeded));
    if let Some((job, permission)) = live_native {
        assert!(
            client
                .authority_status(deadline(), None)
                .unwrap()
                .native_obligations
                .contains(&permission),
            "domain retirement cleared an unrelated native root"
        );
        assert!(
            client
                .status(job, deadline(), None)
                .unwrap()
                .outcome
                .is_none()
        );
        client.cancel(&[job], deadline(), None).unwrap();
        assert_eq!(
            client.wait(job, deadline(), None).unwrap().outcome,
            Some(stillyard::JobOutcome::Canceled)
        );
    }
    assert!(temp.path().join("after-retirement.txt").exists());
    let events = client.machine_events(None, 256, deadline()).unwrap();
    assert!(events.events.iter().any(|e| e.grant_id == offer.grant_id
        && e.state == m::GrantState::Released
        && e.risk_clearance == Some(request.operation_id)));
    let sql = rusqlite::Connection::open(store.join("stillyard.sqlite3")).unwrap();
    let value: String = sql
        .query_row(
            "SELECT snapshot_json FROM machine_grants WHERE allocation_key=?1",
            [serde_json::to_string(&key).unwrap()],
            |r| r.get(0),
        )
        .unwrap();
    let released: m::GrantSnapshot = serde_json::from_str(&value).unwrap();
    assert!(released.sealed_release.is_none());
    assert_eq!(released.risk_clearance, Some(request.operation_id));
    drop(sql);
    daemon.kill_and_wait();
}
