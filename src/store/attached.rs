//! Local lifecycle binding for an attached executor. SQLite guards cover every
//! Lease insertion/release path, including probes and reconciliation.
use super::*;
mod cleanup;
#[cfg(target_os = "linux")]
pub(crate) mod driver;
#[cfg(target_os = "linux")]
pub(super) mod installation;
pub(super) mod invocation;
#[cfg(target_os = "linux")]
mod observation;
mod queue;
mod recovery;
#[cfg(target_os = "linux")]
mod runtime;
use crate::machine::{
    AllocationKey, AllocationOwner, Candidate, Command, GrantState, SessionIdentity,
};

pub(super) fn initialize_schema(c: &Connection) -> StoreResult<()> {
    c.execute_batch("CREATE TABLE IF NOT EXISTS attached_local_mode(
        singleton INTEGER PRIMARY KEY CHECK(singleton=1), store_uuid TEXT NOT NULL,
        session_json TEXT, configuration_sha256 TEXT, connected INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS attached_local_plans(
        slot TEXT PRIMARY KEY, lease_id TEXT UNIQUE NOT NULL, job_id TEXT NOT NULL,
        attempt_id TEXT NOT NULL, invocation_id TEXT UNIQUE NOT NULL,
        containment_id TEXT UNIQUE NOT NULL, allocation_key TEXT UNIQUE NOT NULL,
        candidate_json TEXT NOT NULL, claims_json TEXT NOT NULL,
        armed INTEGER NOT NULL DEFAULT 0, committed INTEGER NOT NULL DEFAULT 0,
        release_pending INTEGER NOT NULL DEFAULT 0, released INTEGER NOT NULL DEFAULT 0);
        CREATE TRIGGER IF NOT EXISTS attached_lease_insert BEFORE INSERT ON leases
        WHEN EXISTS(SELECT 1 FROM attached_local_mode) BEGIN
          SELECT CASE WHEN NOT EXISTS(
            SELECT 1 FROM attached_local_plans p JOIN attached_local_mode m
            WHERE p.lease_id=NEW.id AND p.attempt_id=NEW.attempt_id
              AND p.claims_json=NEW.claims_json AND p.armed=1 AND p.committed=1
              AND p.release_pending=0 AND p.released=0 AND m.connected=1
              AND ((NEW.invocation_id IS NULL AND json_extract(p.candidate_json,'$.owner.kind')='work')
                OR (NEW.invocation_id=p.invocation_id AND json_extract(p.candidate_json,'$.owner.kind')='probe'))
          ) THEN RAISE(ABORT,'attached Lease has no committed Armed Grant') END;
        END;
        CREATE TRIGGER IF NOT EXISTS attached_lease_release BEFORE UPDATE OF state ON leases
        WHEN NEW.state='released' AND OLD.state='granted'
          AND EXISTS(SELECT 1 FROM attached_local_mode) BEGIN
          SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM attached_local_plans WHERE lease_id=OLD.id)
            THEN RAISE(ABORT,'attached Lease lost its Grant binding') END;
          UPDATE attached_local_plans SET release_pending=1 WHERE lease_id=OLD.id;
          SELECT CASE WHEN NOT EXISTS(
            SELECT 1 FROM attached_local_plans WHERE lease_id=OLD.id AND released=1
          ) THEN RAISE(IGNORE) END;
        END;")?;
    queue::initialize(c)?;
    invocation::initialize(c)?;
    #[cfg(target_os = "linux")]
    observation::initialize(c)?;
    cleanup::initialize(c)
}

fn protocol_error(e: crate::machine::manager::JournalError) -> StoreError {
    StoreError::InvalidState(e.to_string())
}

#[derive(Clone, Copy)]
pub(super) struct PlannedIds {
    pub invocation: InvocationId,
    pub containment: ContainmentId,
    pub lease: Uuid,
}
pub(super) struct Admission {
    pub ids: PlannedIds,
    pub ready: bool,
    pub changed: bool,
    pub persist: bool,
}

type ModeRow = (String, Option<String>, Option<String>, bool);
type PlanRow = (String, String, String, String, bool, bool, bool);

/// Called only AFTER local dependencies, conditions and capacity have been
/// checked. A pending remote reply commits the plan, not a local Lease.
pub(super) fn prepare(
    tx: &Transaction<'_>,
    job: JobId,
    attempt: AttemptId,
    condition: Option<ConditionId>,
    spec: &JobSpec,
    claims: &ResolvedClaims,
) -> StoreResult<Admission> {
    let fresh = || PlannedIds {
        invocation: InvocationId::new(job.store_uuid()),
        containment: ContainmentId::new(job.store_uuid()),
        lease: Uuid::now_v7(),
    };
    let mode: Option<ModeRow> = tx.query_row(
        "SELECT store_uuid,session_json,configuration_sha256,connected FROM attached_local_mode WHERE singleton=1",
        [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
    let Some((store, session, configuration, connected)) = mode else {
        return Ok(Admission {
            ids: fresh(),
            ready: true,
            changed: false,
            persist: false,
        });
    };
    if store != job.store_uuid().to_string() {
        return Err(StoreError::InvalidState(
            "attached store differs from its installed binding".into(),
        ));
    }
    if !connected {
        return Ok(Admission {
            ids: fresh(),
            ready: false,
            changed: false,
            persist: false,
        });
    }
    let session: SessionIdentity = serde_json::from_str(
        &session.ok_or_else(|| StoreError::InvalidState("attached session missing".into()))?,
    )?;
    let configuration = configuration
        .ok_or_else(|| StoreError::InvalidState("attached configuration missing".into()))?;
    let probe = condition.is_some();
    let role = if probe { "probe" } else { "primary" };
    let index: u64 = tx.query_row(
        "SELECT COALESCE(MAX(role_index),-1)+1 FROM invocations WHERE attempt_id=?1 AND role=?2",
        params![attempt.entity_uuid().to_string(), role],
        |r| r.get(0),
    )?;
    let slot = format!(
        "{attempt}:{role}:{index}:{}",
        condition.map(|id| id.to_string()).unwrap_or_default()
    );
    let previous: Option<PlanRow> = tx.query_row(
        "SELECT invocation_id,containment_id,lease_id,candidate_json,armed,release_pending,released FROM attached_local_plans WHERE slot=?1",
        [&slot], |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?))).optional()?;
    let ids = match &previous {
        Some((invocation, containment, lease, ..)) => PlannedIds {
            invocation: InvocationId::from_parts(job.store_uuid(), Uuid::parse_str(invocation)?),
            containment: ContainmentId::from_parts(job.store_uuid(), Uuid::parse_str(containment)?),
            lease: Uuid::parse_str(lease)?,
        },
        None => fresh(),
    };
    let scalars = crate::admission::scalar_claim_entries(claims);
    let candidate = Candidate {
        key: AllocationKey {
            machine_id: session.machine_id,
            authority_epoch: session.authority_epoch,
            domain_id: session.domain_id,
            manager_store_uuid: job.store_uuid(),
            lease_id: ids.lease,
        },
        owner: if probe {
            AllocationOwner::Probe {
                job_id: job,
                invocation_id: ids.invocation,
            }
        } else {
            AllocationOwner::Work {
                job_id: job,
                attempt_id: attempt,
            }
        },
        revision: 1,
        priority: spec.priority,
        claims: crate::machine::Claims {
            scalars,
            shared_fences: claims.shared_fences.iter().cloned().collect(),
            exclusive_fences: claims.exclusive_fences.iter().cloned().collect(),
            impacts: claims.impacts.iter().cloned().collect(),
        },
        configuration_sha256: configuration,
        observed: spec.observed.clone(),
        quiet: spec.quiet.clone(),
    };
    let json = serde_json::to_string(&candidate)?;
    if let Some((_, _, _, old, armed, pending, released)) = previous {
        if old != json || pending || released {
            return Err(StoreError::InvalidState(
                "attached candidate changed or was sealed; reconcile before replanning".into(),
            ));
        }
        queue::touch(tx, ids.lease, false)?;
        if armed {
            tx.execute(
                "UPDATE attached_local_plans SET committed=1 WHERE slot=?1",
                [&slot],
            )?;
        }
        return Ok(Admission {
            ids,
            ready: armed,
            changed: false,
            persist: true,
        });
    }
    let count: u64 = tx.query_row(
        "SELECT COUNT(*) FROM attached_local_plans WHERE released=0",
        [],
        |r| r.get(0),
    )?;
    if count >= crate::machine::MAX_DOMAIN_CANDIDATES as u64 {
        return Ok(Admission {
            ids,
            ready: false,
            changed: false,
            persist: false,
        });
    }
    tx.execute("INSERT INTO attached_local_plans(slot,lease_id,job_id,attempt_id,invocation_id,containment_id,allocation_key,candidate_json,claims_json)
        VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",params![slot,ids.lease.to_string(),job.entity_uuid().to_string(),attempt.entity_uuid().to_string(),
            ids.invocation.entity_uuid().to_string(),ids.containment.entity_uuid().to_string(),serde_json::to_string(&candidate.key)?,json,serde_json::to_string(claims)?])?;
    tx.execute(
        "UPDATE jobs SET attempt_id=?2 WHERE id=?1 AND state='pending'",
        params![
            job.entity_uuid().to_string(),
            attempt.entity_uuid().to_string()
        ],
    )?;
    queue::touch(tx, ids.lease, true)?;
    crate::machine::manager::enqueue(tx, Uuid::now_v7(), &Command::CandidateUpsert { candidate })
        .map_err(protocol_error)?;
    Ok(Admission {
        ids,
        ready: false,
        changed: true,
        persist: true,
    })
}

/// Apply the remote acknowledgement and local lifecycle effect in ONE commit.
/// An unanswered Release leaves both the local Lease and machine Grant debited.
#[allow(dead_code)] // Called by the attached bridge worker in the next runtime slice.
pub(super) fn accept(
    tx: &Transaction<'_>,
    request: &crate::machine::Request,
    reply: &crate::machine::Reply,
) -> StoreResult<bool> {
    let changed = crate::machine::manager::accept(tx, request, reply).map_err(protocol_error)?;
    if !changed {
        return Ok(false);
    }
    match &reply.outcome {
        crate::machine::Outcome::Grant { grant } if grant.state == GrantState::Armed => {
            let count = tx.execute("UPDATE attached_local_plans SET armed=1 WHERE allocation_key=?1 AND candidate_json=?2 AND released=0 AND release_pending=0",
                params![serde_json::to_string(&grant.candidate.key)?,serde_json::to_string(&grant.candidate)?])?;
            if count != 1 {
                return Err(StoreError::InvalidState(
                    "Armed Grant has no matching local candidate".into(),
                ));
            }
        }
        crate::machine::Outcome::Released { .. } => {
            let Command::Release { release } = &request.command else {
                unreachable!()
            };
            let key = serde_json::to_string(&release.key)?;
            tx.execute(
                "UPDATE attached_local_plans SET released=1 WHERE allocation_key=?1",
                [&key],
            )?;
            tx.execute("UPDATE leases SET state='released' WHERE state='granted' AND id IN (SELECT lease_id FROM attached_local_plans WHERE allocation_key=?1 AND release_pending=1 AND released=1)",[key])?;
        }
        crate::machine::Outcome::Reconciled { released, .. } => {
            for key in released {
                let key = serde_json::to_string(key)?;
                tx.execute(
                    "UPDATE attached_local_plans SET released=1 WHERE allocation_key=?1",
                    [&key],
                )?;
                tx.execute("UPDATE leases SET state='released' WHERE state='granted' AND id IN (SELECT lease_id FROM attached_local_plans WHERE allocation_key=?1 AND release_pending=1 AND released=1)",[key])?;
            }
        }
        _ => {}
    }
    queue::accepted(tx, request, &reply.outcome)?;
    cleanup::apply(tx)?;
    if matches!(
        reply.outcome,
        crate::machine::Outcome::InventoryPage { next: None, .. }
            | crate::machine::Outcome::Reconciled { .. }
    ) {
        recovery::synchronize(tx)?;
    }
    Ok(changed)
}

#[cfg(test)]
mod tests {
    #[cfg(target_os = "linux")]
    mod crash;
    use super::*;
    use crate::machine::{
        InstallationIdentity, Outcome, PairingSecret, ParticipantRole, ParticipantSnapshot, manager,
    };

    fn reply(tx: &Transaction<'_>, secret: &PairingSecret, outcome: Outcome) {
        let request = manager::pending(tx, secret).unwrap().unwrap();
        let reply = crate::machine::Reply {
            coordinator_revision: 1,
            session: request.session.clone(),
            operation_id: request.operation_id,
            request_sequence: request.request_sequence,
            outcome,
        };
        accept(tx, &request, &reply).unwrap();
    }

    #[cfg(target_os = "linux")]
    fn kernel_runtime(
        store: Store,
        prepared: PreparedJob,
        session: SessionIdentity,
        secret: PairingSecret,
        paths: &StorePaths,
        scenario: &str,
    ) {
        use std::sync::{Arc, Mutex};
        use std::time::{Duration, Instant};
        let config = installation::load(&paths.root).unwrap().unwrap();
        let history = paths.root.join("attachment/executor");
        crate::runner::linux::initialize_history(
            &history,
            config.journal,
            session.manager_store_uuid,
            session.domain_id,
        )
        .unwrap();
        let registry = crate::runner::linux::installed_registry(
            &history,
            config.journal,
            session.manager_store_uuid,
            session.domain_id,
            session.executor_incarnation,
            "/tmp/attached-fixture.sock".into(),
        )
        .unwrap();
        if let Some(root) = std::env::var_os("STILLYARD_TEST_RUNTIME_CRASH_ROOT") {
            std::fs::write(
                std::path::Path::new(&root).join("fixture.json"),
                serde_json::to_vec(&serde_json::json!({
                    "store": paths.root, "job": prepared.job_id,
                    "invocation": prepared.invocation_id, "stdout": prepared.stdout_path,
                    "containment": prepared.containment_id,
                }))
                .unwrap(),
            )
            .unwrap();
        }
        let releases = Arc::new(driver::ReleaseState::default());
        let live = crate::runner::LiveContainments::attached_linux(registry, Arc::clone(&releases));
        let store = Arc::new(Mutex::new(store));
        let peer_store = Arc::clone(&store);
        let peer_secret = PairingSecret::from_anchor(*secret.anchor_bytes());
        let retry_waiting = scenario == "ticket_wait";
        let waiting_stdout = prepared.stdout_path.clone();
        let peer = std::thread::spawn(move || {
            let until = Instant::now() + Duration::from_secs(35);
            let mut rejected_operation = None;
            loop {
                let mut store = peer_store.lock().unwrap();
                let tx = store.connection.transaction().unwrap();
                if let Some(request) = manager::pending(&tx, &peer_secret).unwrap() {
                    let Command::AuthorizeInvocation {
                        key,
                        offer_nonce,
                        intent,
                    } = &request.command
                    else {
                        panic!("unexpected runtime request");
                    };
                    releases
                        .challenge(&request, session.executor_incarnation, "c".repeat(64))
                        .unwrap();
                    if retry_waiting && rejected_operation.is_none() {
                        assert!(
                            std::fs::read_to_string(&waiting_stdout)
                                .unwrap_or_default()
                                .is_empty()
                        );
                        let outcome = Outcome::Rejected {
                            code: "detector_unavailable".into(),
                            detail: "warming_up".into(),
                        };
                        reply(&tx, &peer_secret, outcome.clone());
                        tx.commit().unwrap();
                        releases.accepted_rejection(&request, &outcome).unwrap();
                        rejected_operation = Some(request.operation_id);
                        drop(store);
                        continue;
                    }
                    if let Some(operation) = rejected_operation {
                        assert_ne!(operation, request.operation_id);
                        assert!(
                            std::fs::read_to_string(&waiting_stdout)
                                .unwrap_or_default()
                                .is_empty(),
                            "waiting response released user code without a Ticket"
                        );
                    }
                    let json: String = tx
                        .query_row(
                            "SELECT grant_json FROM attached_grants WHERE allocation_key=?1",
                            [serde_json::to_string(key).unwrap()],
                            |r| r.get(0),
                        )
                        .unwrap();
                    let grant: crate::machine::GrantSnapshot = serde_json::from_str(&json).unwrap();
                    reply(
                        &tx,
                        &peer_secret,
                        Outcome::Ticket {
                            ticket: Box::new(crate::machine::InvocationTicket {
                                grant_id: grant.grant_id,
                                key: key.clone(),
                                offer_nonce: *offer_nonce,
                                intent: intent.clone(),
                                configuration_sha256: "c".repeat(64),
                                issued_unix_millis: now_millis(),
                                host_observation_generation: Uuid::now_v7(),
                                host_sample_unix_millis: now_millis(),
                                session: session.clone(),
                            }),
                        },
                    );
                    tx.commit().unwrap();
                    return;
                }
                tx.rollback().unwrap();
                drop(store);
                assert!(
                    Instant::now() < until,
                    "prepared executor never requested Ticket"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
        });
        let job_id = prepared.job_id;
        let invocation = prepared.invocation_id;
        let stdout = prepared.stdout_path.clone();
        let cancel = (scenario == "cancel").then(|| {
            let store = Arc::clone(&store);
            let stdout = stdout.clone();
            std::thread::spawn(move || {
                let until = Instant::now() + Duration::from_secs(30);
                loop {
                    if std::fs::read_to_string(&stdout)
                        .is_ok_and(|s| s.contains("attached-kernel-runtime"))
                    {
                        store.lock().unwrap().cancel_jobs(&[job_id]).unwrap();
                        break;
                    }
                    assert!(
                        Instant::now() < until,
                        "user code never reached cancellation marker"
                    );
                    std::thread::sleep(Duration::from_millis(5));
                }
            })
        });
        crate::runner::run(
            prepared,
            Arc::clone(&store),
            "/tmp/attached-fixture.sock".into(),
            live,
            Arc::new(crate::host_observation::HostObservationService::new(
                Default::default(),
            )),
            Arc::new(|| {}),
        );
        peer.join().unwrap();
        if let Some(cancel) = cancel {
            cancel.join().unwrap();
        }
        let mut store = store.lock().unwrap();
        let snapshot = store.status(job_id).unwrap();
        assert_eq!(
            snapshot.outcome,
            Some(match scenario {
                "cancel" => crate::JobOutcome::Canceled,
                "timeout" => crate::JobOutcome::TimedOut,
                _ => crate::JobOutcome::Succeeded,
            }),
            "{snapshot:?}"
        );
        assert!(
            std::fs::read_to_string(stdout)
                .unwrap()
                .contains("attached-kernel-runtime")
        );
        let (consumed, cleanup): (bool, String) = store
            .connection
            .query_row(
                "SELECT consumed,cleanup_json FROM attached_tickets WHERE invocation_id=?1",
                [invocation.to_string()],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert!(consumed);
        assert!(
            serde_json::from_str::<crate::machine::TicketCleanup>(&cleanup)
                .unwrap()
                .user_code_released
        );
        let retained: u64 = store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM leases WHERE state='granted'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            retained, 1,
            "kernel cleanup bypassed coordinator acknowledgement"
        );
        let tx = store.connection.transaction().unwrap();
        assert!(queue::maintain(&tx).unwrap());
        let request = manager::pending(&tx, &secret).unwrap().unwrap();
        let Command::Release { release } = request.command else {
            panic!("sealed runtime did not request Release");
        };
        let grant: String = tx
            .query_row(
                "SELECT grant_json FROM attached_grants WHERE allocation_key=?1",
                [serde_json::to_string(&release.key).unwrap()],
                |r| r.get(0),
            )
            .unwrap();
        reply(
            &tx,
            &secret,
            Outcome::Released {
                grant_id: serde_json::from_str::<crate::machine::GrantSnapshot>(&grant)
                    .unwrap()
                    .grant_id,
                sealed_sequence: release.sealed_sequence,
            },
        );
        tx.commit().unwrap();
        assert_eq!(
            store
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM leases WHERE state='granted'",
                    [],
                    |r| r.get::<_, u64>(0)
                )
                .unwrap(),
            0
        );
    }

    #[test]
    fn attached_admission_persists_candidates_and_holds_lease_until_release_ack() {
        attached_fixture(false, "model");
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "requires protected nested cgroup delegation"]
    fn linux_attached_runtime_consumes_ticket_and_seals_live_descendant() {
        attached_fixture(true, "root_exit");
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "requires protected nested cgroup delegation"]
    fn linux_attached_runtime_cancel_seals_live_descendant() {
        attached_fixture(true, "cancel");
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "requires protected nested cgroup delegation"]
    fn linux_attached_runtime_timeout_seals_live_descendant() {
        attached_fixture(true, "timeout");
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "requires protected nested cgroup delegation"]
    fn linux_attached_runtime_waiting_ticket_does_not_release_user_code() {
        attached_fixture(true, "ticket_wait");
    }

    fn attached_fixture(kernel: bool, scenario: &str) {
        let temp = tempfile::tempdir_in(
            std::env::var_os("STILLYARD_TEST_RUNTIME_CRASH_ROOT")
                .unwrap_or_else(|| env!("CARGO_MANIFEST_DIR").into()),
        )
        .unwrap();
        let host = HostId("attached-store-fixture".into());
        let boot = BootId("attached-store-boot".into());
        let identity = StartupIdentity {
            host_id: Some(host.clone()),
            boot_id: Some(boot.clone()),
            failures: vec![],
            daemon_process: Some(ProcessIdentity::Windows {
                host_id: host,
                boot_id: boot,
                pid: 99,
                creation_filetime_100ns: 99,
            }),
        };
        #[cfg(target_os = "linux")]
        let identity = if kernel {
            crate::identity::probe_attached_linux_identity()
        } else {
            identity
        };
        let paths = StorePaths::new(temp.path().join("store"));
        let mut config = HostConfig::default();
        config.resources.cargo_slots = 1;
        let mut store =
            Store::open_with_config(paths.clone(), config.clone(), identity.clone()).unwrap();
        let session = SessionIdentity {
            machine_id: Uuid::now_v7(),
            authority_epoch: Uuid::now_v7(),
            domain_id: crate::ExecutionDomainId(Uuid::now_v7()),
            manager_store_uuid: store.store_uuid(),
            executor_incarnation: store.daemon_generation(),
            connection_epoch: 1,
        };
        let secret = PairingSecret::generate().unwrap();
        let tx = store.connection.transaction().unwrap();
        manager::initialize(&tx, session.manager_store_uuid).unwrap();
        #[cfg(target_os = "linux")]
        let owner_uid =
            std::os::unix::fs::MetadataExt::uid(&std::fs::metadata(&paths.root).unwrap());
        #[cfg(not(target_os = "linux"))]
        let owner_uid = 1000;
        let peer = ParticipantSnapshot {
            installation: InstallationIdentity {
                installation_nonce: Uuid::now_v7(),
                domain_id: session.domain_id,
                owner_uid,
                runtime_registration: "fixture-only".into(),
                role: ParticipantRole::Executor,
            },
            manager_store_uuid: session.manager_store_uuid,
            connection_epoch: 1,
            executor_incarnation: Some(session.executor_incarnation),
            reconciliation_required: false,
            retired_sequence_floor: 0,
            accepted_sequence: 0,
        };
        manager::bind(&tx, &session, &peer).unwrap();
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&paths.root, std::fs::Permissions::from_mode(0o700)).unwrap();
            installation::create(
                &paths.root,
                &installation::Configuration {
                    version: 1,
                    pairing: crate::machine::PairingRegistration {
                        installation: peer.installation.clone(),
                        manager_store_uuid: session.manager_store_uuid,
                        parent_domain: crate::ExecutionDomainId(Uuid::now_v7()),
                        budgets: Default::default(),
                        aliases: Default::default(),
                        secret: *secret.anchor_bytes(),
                    },
                    coordinator_installation: Uuid::now_v7(),
                    machine_id: session.machine_id,
                    bridge_executable: "/fixture/stillyard.exe".into(),
                    bridge_sha256: "b".repeat(64),
                    coordinator_endpoint: "fixture-pipe".into(),
                    interop_socket: "/run/WSL/fixture_interop".into(),
                    executor_cgroup: "/sys/fs/cgroup/fixture".into(),
                    journal: Uuid::now_v7(),
                },
            )
            .unwrap();
            let installed = installation::load(&paths.root).unwrap().unwrap();
            tx.execute(
                "INSERT INTO attached_meta VALUES ('installation_sha256',?1)",
                [installation::fingerprint(&installed).unwrap()],
            )
            .unwrap();
        }
        tx.execute(
            "INSERT INTO attached_local_mode VALUES (1,?1,?2,?3,1)",
            params![
                session.manager_store_uuid.to_string(),
                serde_json::to_string(&session).unwrap(),
                "c".repeat(64)
            ],
        )
        .unwrap();
        tx.commit().unwrap();
        let mut spec: JobSpec = serde_json::from_value(serde_json::json!({
            "spec_version": crate::SPEC_VERSION, "executable": temp.path().join("tool.exe"),
            "working_directory": temp.path(), "resources": {"cargo_slots":1}
        }))
        .unwrap();
        if kernel {
            spec.executable = "/usr/bin/python3".into();
            let tail = if matches!(scenario, "root_exit" | "ticket_wait") {
                ""
            } else {
                "; import time; time.sleep(60)"
            };
            spec.args = vec![
                "-c".into(),
                format!(
                    "import subprocess; subprocess.Popen(['/usr/bin/python3','-c','import time;time.sleep(60)']); print('attached-kernel-runtime',flush=True){tail}"
                ),
            ];
            if scenario == "timeout" {
                spec.timeout_seconds = Some(2);
            }
        }
        let mut jobs = Vec::new();
        for _ in 0..if kernel { 1 } else { 2 } {
            let job = store
                .submit(
                    Uuid::now_v7(),
                    &normalized_payload_hash(&spec).unwrap(),
                    &spec,
                )
                .unwrap()
                .receipt
                .job_id;
            if jobs.is_empty() {
                store
                    .connection
                    .execute("UPDATE attached_local_mode SET connected=0", [])
                    .unwrap();
                for _ in 0..3 {
                    assert!(store.prepare_job(job).unwrap().is_none());
                }
                assert_eq!(
                    store
                        .connection
                        .query_row("SELECT COUNT(*) FROM attempts", [], |r| r.get::<_, u64>(0))
                        .unwrap(),
                    0,
                    "disconnected scans leaked unbound planned Attempts"
                );
                store
                    .connection
                    .execute("UPDATE attached_local_mode SET connected=1", [])
                    .unwrap();
            }
            assert!(store.prepare_job(job).unwrap().is_none());
            jobs.push(job);
        }
        assert_eq!(
            store
                .connection
                .query_row("SELECT COUNT(*) FROM attached_local_plans", [], |r| r
                    .get::<_, u64>(0))
                .unwrap(),
            if kernel { 1 } else { 2 },
            "one domain head hid another ready candidate"
        );
        assert_eq!(
            store
                .connection
                .query_row("SELECT COUNT(*) FROM leases", [], |r| r.get::<_, u64>(0))
                .unwrap(),
            0,
            "local Lease committed before remote Arm"
        );
        let tx = store.connection.transaction().unwrap();
        let request = manager::pending(&tx, &secret).unwrap().unwrap();
        let Command::CandidateUpsert { candidate } = request.command else {
            panic!("candidate missing")
        };
        reply(&tx, &secret, Outcome::Accepted { revision: 1 });
        if !kernel {
            reply(&tx, &secret, Outcome::Accepted { revision: 1 });
        }
        let grant = crate::machine::GrantSnapshot {
            queue_accepted_unix_millis: 1,
            queue_sequence: 1,
            uncertainty_reason: None,
            risk_clearance: None,
            grant_id: crate::GrantId::from_parts(Uuid::now_v7(), Uuid::now_v7()),
            candidate: candidate.clone(),
            offer_nonce: session.machine_id,
            state: GrantState::Armed,
            offered_unix_millis: 1,
            offer_deadline_unix_millis: 5000,
            armed_unix_millis: Some(2),
            released_unix_millis: None,
            tickets: vec![],
            sealed_release: None,
        };
        assert!(queue::maintain(&tx).unwrap());
        assert!(
            matches!(manager::pending(&tx,&secret).unwrap().unwrap().command,
            Command::Inspect { key:Some(key) } if key==candidate.key)
        );
        let mut offer = grant.clone();
        offer.state = GrantState::Offered;
        offer.armed_unix_millis = None;
        reply(
            &tx,
            &secret,
            Outcome::Inspection {
                grants: vec![offer],
                truncated: false,
            },
        );
        assert!(
            matches!(manager::pending(&tx,&secret).unwrap().unwrap().command,
            Command::Arm { key,offer_nonce } if key==candidate.key && offer_nonce==grant.offer_nonce)
        );
        reply(
            &tx,
            &secret,
            Outcome::Grant {
                grant: Box::new(grant.clone()),
            },
        );
        tx.commit().unwrap();
        let prepared = store.prepare_job(jobs[0]).unwrap().unwrap();
        #[cfg(target_os = "linux")]
        {
            let allocations = store.status(jobs[0]).unwrap().allocations;
            assert_eq!(allocations.len(), 1);
            assert_eq!(allocations[0].grant_id, grant.grant_id);
            assert_eq!(allocations[0].lease_id, candidate.key.lease_id);
            assert_eq!(allocations[0].key.as_ref(), Some(&candidate.key));
        }
        #[cfg(target_os = "linux")]
        if kernel {
            kernel_runtime(store, prepared, session, secret, &paths, scenario);
            return;
        }
        assert_eq!(
            prepared.invocation_id.entity_uuid().to_string(),
            store
                .connection
                .query_row(
                    "SELECT invocation_id FROM attached_local_plans WHERE lease_id=?1",
                    [candidate.key.lease_id.to_string()],
                    |r| r.get::<_, String>(0)
                )
                .unwrap()
        );
        assert!(store.prepare_job(jobs[1]).unwrap().is_none());
        // These are synthetic protocol replies and typed roots: this control
        // proves the SQLite boundary, not a Linux kernel release.
        let root = ProcessIdentity::Linux {
            host_id: HostId("fixture-linux".into()),
            boot_id: BootId("fixture-boot".into()),
            pid: 123,
            start_ticks: 456,
            pid_namespace_inode: 789,
            uid: 1000,
        };
        // Probe/deferral Invocations can precede the primary in the same SQL
        // Attempt. The Grant's primary Ticket still has wire role_index zero.
        store
            .connection
            .execute(
                "UPDATE invocations SET role_index=7 WHERE id=?1",
                [prepared.invocation_id.entity_uuid().to_string()],
            )
            .unwrap();
        let intent = crate::machine::InvocationIntent {
            invocation_id: prepared.invocation_id,
            containment_id: prepared.containment_id,
            role: prepared.role,
            role_index: 0,
            release_sequence: 1,
            executable_sha256: "a".repeat(64),
            boundary_sha256: "b".repeat(64),
            readiness_challenge: Uuid::now_v7(),
            previous_cleanup: None,
        };
        let ticket = crate::machine::InvocationTicket {
            grant_id: grant.grant_id,
            key: candidate.key.clone(),
            offer_nonce: grant.offer_nonce,
            intent: intent.clone(),
            configuration_sha256: "c".repeat(64),
            issued_unix_millis: 10,
            host_observation_generation: Uuid::now_v7(),
            host_sample_unix_millis: 10,
            session: session.clone(),
        };
        let tx = store.connection.transaction().unwrap();
        manager::enqueue(
            &tx,
            Uuid::now_v7(),
            &Command::AuthorizeInvocation {
                key: candidate.key.clone(),
                offer_nonce: grant.offer_nonce,
                intent,
            },
        )
        .unwrap();
        reply(
            &tx,
            &secret,
            Outcome::Ticket {
                ticket: Box::new(ticket.clone()),
            },
        );
        tx.commit().unwrap();
        let state = |store: &Store| -> (String, bool, u64) {
            store
                .connection
                .query_row(
                    "SELECT i.state,t.consumed,
                (SELECT COUNT(*) FROM invocation_process_identities r WHERE r.invocation_id=i.id)
                FROM invocations i JOIN attached_tickets t ON t.invocation_id=?1 WHERE i.id=?2",
                    params![
                        prepared.invocation_id.to_string(),
                        prepared.invocation_id.entity_uuid().to_string()
                    ],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .unwrap()
        };
        assert!(
            store
                .mark_started_with_identity(
                    &prepared,
                    123,
                    &ticket.intent.executable_sha256,
                    Some(&root)
                )
                .is_err()
        );
        assert_eq!(state(&store), ("prepared".into(), false, 0));
        let mut wrong = ticket.clone();
        wrong.intent.boundary_sha256 = "d".repeat(64);
        assert!(
            store
                .mark_started_with_ticket(
                    &prepared,
                    123,
                    &ticket.intent.executable_sha256,
                    Some(&root),
                    Some(&wrong)
                )
                .is_err()
        );
        assert_eq!(state(&store), ("prepared".into(), false, 0));
        store
            .mark_started_with_ticket(
                &prepared,
                123,
                &ticket.intent.executable_sha256,
                Some(&root),
                Some(&ticket),
            )
            .unwrap();
        assert_eq!(state(&store), ("started".into(), true, 1));
        assert!(
            store
                .mark_started_with_ticket(
                    &prepared,
                    123,
                    &ticket.intent.executable_sha256,
                    Some(&root),
                    Some(&ticket)
                )
                .is_err()
        );
        assert_eq!(state(&store), ("started".into(), true, 1));
        let tx = store.connection.transaction().unwrap();
        manager::record_cleanup(
            &tx,
            &crate::machine::TicketCleanup {
                invocation_id: prepared.invocation_id,
                release_sequence: 1,
                boundary_sha256: ticket.intent.boundary_sha256.clone(),
                proof_sha256: "e".repeat(64),
                user_code_released: true,
            },
        )
        .unwrap();
        tx.commit().unwrap();
        // Ordinary lifecycle cleanup cannot retire the shared debit until the
        // coordinator acknowledges the manager's sealed Release.
        store
            .mark_invocation_resolved(&prepared, Some(0), None)
            .unwrap();
        store
            .settle_attempt(&prepared, AttemptVerdict::Succeeded)
            .unwrap();
        assert_eq!(
            store
                .connection
                .query_row(
                    "SELECT state FROM leases WHERE id=?1",
                    [candidate.key.lease_id.to_string()],
                    |r| r.get::<_, String>(0)
                )
                .unwrap(),
            "granted"
        );
        assert_eq!(
            store
                .connection
                .query_row(
                    "SELECT release_pending FROM attached_local_plans WHERE lease_id=?1",
                    [candidate.key.lease_id.to_string()],
                    |r| r.get::<_, u64>(0)
                )
                .unwrap(),
            1
        );
        let tx = store.connection.transaction().unwrap();
        // Regression: a stale unarmed candidate used to enqueue Cancel revision
        // 1 forever, ahead of this cleaned Invocation's sealed Release.
        tx.execute(
            "UPDATE attached_local_liveness SET ready_ms=0 WHERE lease_id!=?1",
            [candidate.key.lease_id.to_string()],
        )
        .unwrap();
        assert!(queue::maintain(&tx).unwrap());
        let request = manager::pending(&tx, &secret).unwrap().unwrap();
        let Command::Release { release } = request.command else {
            panic!("Release missing")
        };
        tx.commit().unwrap();
        assert!(
            store.prepare_job(jobs[1]).unwrap().is_none(),
            "unanswered Release freed local token"
        );
        let tx = store.connection.transaction().unwrap();
        reply(
            &tx,
            &secret,
            Outcome::Released {
                grant_id: grant.grant_id,
                sealed_sequence: release.sealed_sequence,
            },
        );
        tx.commit().unwrap();
        assert_eq!(
            store
                .connection
                .query_row(
                    "SELECT state FROM leases WHERE id=?1",
                    [candidate.key.lease_id.to_string()],
                    |r| r.get::<_, String>(0)
                )
                .unwrap(),
            "released"
        );
        let tx = store.connection.transaction().unwrap();
        tx.execute(
            "UPDATE attached_local_liveness SET ready_ms=0 WHERE lease_id!=?1",
            [candidate.key.lease_id.to_string()],
        )
        .unwrap();
        assert!(queue::maintain(&tx).unwrap());
        let cancel = manager::pending(&tx, &secret).unwrap().unwrap();
        let Command::CancelCandidate { key, revision } = cancel.command else {
            panic!("stale candidate cancellation missing")
        };
        assert_ne!(key, candidate.key);
        assert_eq!(
            revision, 2,
            "cancellation must advance the advertised revision"
        );
        reply(&tx, &secret, Outcome::Accepted { revision });
        assert!(
            !queue::maintain(&tx).unwrap(),
            "retired candidate produced another cancellation"
        );
        tx.commit().unwrap();
        // Ordinary startup must preserve mode but fence the old connection.
        drop(store);
        let mut store =
            Store::open_with_config(paths.clone(), config.clone(), identity.clone()).unwrap();
        assert_eq!(
            store
                .connection
                .query_row("SELECT connected FROM attached_local_mode", [], |r| r
                    .get::<_, u64>(0))
                .unwrap(),
            0
        );
        assert!(
            store.attach_authority().is_err(),
            "attached mode fell back to a standalone authority"
        );
        #[cfg(target_os = "linux")]
        {
            let anchor = paths.root.join("attachment/anchor.json");
            let bytes = std::fs::read(&anchor).unwrap();
            std::fs::write(&anchor, b"{}").unwrap();
            assert!(
                store.authority_blocker().is_err(),
                "corrupt anchor allowed admission"
            );
            std::fs::write(&anchor, &bytes).unwrap();
            std::fs::rename(&anchor, anchor.with_extension("saved")).unwrap();
            assert!(
                store.authority_blocker().is_err(),
                "missing anchor allowed admission"
            );
            std::fs::rename(anchor.with_extension("saved"), &anchor).unwrap();
            drop(store);
            std::fs::remove_file(&paths.database).unwrap();
            assert!(
                Store::open_with_config(paths, config, identity).is_err(),
                "replacement SQLite history silently created a free attached store"
            );
        }
    }
}
