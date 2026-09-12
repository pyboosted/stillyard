//! Real manager-process deaths around durable/kernel release boundaries.
//! Protocol replies are the existing authenticated model fixture; native
//! coordinator crash/replay controls and installed bridge loss are separate.
use super::*;
use std::path::Path;
use std::process::{Child, Command as ProcessCommand};
use std::time::{Duration, Instant};

struct Subject(Child);
impl Drop for Subject {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn read(path: &Path) -> serde_json::Value {
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

#[test]
#[ignore = "only the protected crash matrix controller launches this child"]
fn linux_attached_runtime_crash_subject() {
    assert!(std::env::var_os("STILLYARD_TEST_RUNTIME_CRASH_ROOT").is_some());
    attached_fixture(true, "root_exit");
    panic!("selected real runtime checkpoint was not reached");
}

#[test]
#[ignore = "requires protected nested cgroup delegation and prebuilt Linux stub"]
fn linux_attached_runtime_crash_commit_boundaries() {
    assert!(std::env::var_os("STILLYARD_TEST_CGROUP_ROOT").is_some());
    for stage in ["ready", "release-intent", "consumed", "released", "sealed"] {
        let root = crate::test_support::durable_tempdir().unwrap();
        let output = std::fs::File::create(root.path().join("subject.log")).unwrap();
        let mut subject = Subject(
            ProcessCommand::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "store::attached::tests::crash::linux_attached_runtime_crash_subject",
                    "--ignored",
                    "--nocapture",
                ])
                .env("STILLYARD_TEST_RUNTIME_CRASH_ROOT", root.path())
                .env("STILLYARD_TEST_RUNTIME_CRASH", stage)
                .stdout(output.try_clone().unwrap())
                .stderr(output)
                .spawn()
                .unwrap(),
        );
        let until = Instant::now() + Duration::from_secs(45);
        while !root.path().join("checkpoint.json").exists() {
            assert!(
                subject.0.try_wait().unwrap().is_none(),
                "subject exited before {stage}: {}",
                std::fs::read_to_string(root.path().join("subject.log")).unwrap()
            );
            assert!(Instant::now() < until, "subject missed {stage}");
            std::thread::sleep(Duration::from_millis(10));
        }
        let checkpoint = read(&root.path().join("checkpoint.json"));
        assert_eq!(checkpoint["pid"], subject.0.id());
        assert_eq!(checkpoint["stage"], stage);
        let fixture = read(&root.path().join("fixture.json"));
        let paths = StorePaths::new(std::path::PathBuf::from(fixture["store"].as_str().unwrap()));
        assert!(paths.root.starts_with(root.path()));
        let stdout = Path::new(fixture["stdout"].as_str().unwrap());
        if matches!(stage, "released" | "sealed") {
            while !std::fs::read_to_string(stdout)
                .unwrap_or_default()
                .contains("attached-kernel-runtime")
            {
                assert!(
                    Instant::now() < until,
                    "released code never produced its real marker"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
        } else {
            assert!(
                !std::fs::read_to_string(stdout)
                    .unwrap_or_default()
                    .contains("attached-kernel-runtime")
            );
        }
        // A real SIGKILL of the exact owned Child, not an injected Result error.
        subject.0.kill().unwrap();
        assert!(!subject.0.wait().unwrap().success());
        let journal_path = paths.root.join("attachment/executor");
        let before = read(&journal_path.join("state.json"));
        let invocation: InvocationId =
            serde_json::from_value(fixture["invocation"].clone()).unwrap();
        let key = invocation.to_string();
        let record = &before["state"]["records"][&key];
        assert_eq!(record["seal"].is_null(), stage != "sealed");
        assert_eq!(record["release_intent"].is_null(), stage == "ready");
        let sql = Connection::open(&paths.database).unwrap();
        let consumed: Option<bool> = sql
            .query_row(
                "SELECT consumed FROM attached_tickets WHERE invocation_id=?1",
                [&key],
                |row| row.get(0),
            )
            .optional()
            .unwrap();
        assert_eq!(
            consumed,
            match stage {
                "ready" => None,
                "release-intent" => Some(false),
                _ => Some(true),
            }
        );
        assert_eq!(
            sql.query_row(
                "SELECT COUNT(*) FROM leases WHERE state='granted'",
                [],
                |r| r.get::<_, u64>(0)
            )
            .unwrap(),
            1
        );
        drop(sql);
        let mut store = Store::open(paths.clone()).unwrap();
        let config = installation::load(&paths.root).unwrap().unwrap();
        let registry = crate::runner::linux::installed_registry(
            &journal_path,
            config.journal,
            store.store_uuid(),
            config.pairing.installation.domain_id,
            store.daemon_generation(),
            root.path().join("unused.sock").display().to_string(),
        )
        .unwrap();
        let candidates = store.reconciliation_candidates(0, 32).unwrap();
        assert_eq!(
            candidates.len(),
            1,
            "crashed launch must remain uncertain until actual cleanup: {stage}"
        );
        let candidate = &candidates[0];
        assert_eq!(candidate.invocation_id, invocation);
        assert_eq!(
            registry
                .reconcile(candidate, Instant::now() + Duration::from_secs(15))
                .unwrap(),
            crate::ReconciliationResult::ProvenEmpty
        );
        registry.persist_cleanup(&mut store, invocation).unwrap();
        store
            .commit_containment_resolution(
                candidate,
                crate::ContainmentResolution::ProvenEmpty,
                crate::ReconciliationResult::ProvenEmpty,
                crate::ClearanceOrigin::Automatic,
                None,
                None,
            )
            .unwrap()
            .unwrap();
        let seals = registry.durable_seals().unwrap();
        assert_eq!(seals.len(), 1);
        let after = read(&journal_path.join("state.json"));
        if stage == "sealed" {
            assert_eq!(record["seal"], after["state"]["records"][&key]["seal"]);
        }
        assert!(!Path::new(record["boundary"]["path"].as_str().unwrap()).exists());
        assert_eq!(
            store
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM leases WHERE state='granted'",
                    [],
                    |r| r.get::<_, u64>(0)
                )
                .unwrap(),
            1,
            "kernel cleanup must not bypass coordinator acknowledgement"
        );
        let secret = PairingSecret::from_anchor(config.pairing.secret);
        let tx = store.connection.transaction().unwrap();
        assert!(queue::maintain(&tx).unwrap());
        let pending = manager::pending(&tx, &secret).unwrap().unwrap();
        let Command::Release { release } = &pending.command else {
            panic!(
                "crashed launch did not request sealed release at {stage}: {:?}",
                pending.command
            );
        };
        assert_eq!(
            after["state"]["records"][&key]["seal"]["possibly_released"],
            stage != "ready"
        );
        if let Some(consumed) = consumed {
            assert_eq!(release.tickets.len(), 1);
            let cleanup = &release.tickets[0];
            assert_eq!(cleanup.invocation_id, invocation);
            assert_eq!(cleanup.boundary_sha256, seals[0].1);
            assert_eq!(cleanup.proof_sha256, seals[0].2);
            // The executor seal is conservative about the intent/SQL gap.
            // Continuous manager SQL independently proves whether consumption
            // committed; no kernel resume is possible before that commit.
            assert_eq!(cleanup.user_code_released, consumed);
        } else {
            assert!(release.tickets.is_empty());
        }
        let grant: String = tx
            .query_row(
                "SELECT grant_json FROM attached_grants WHERE allocation_key=?1",
                [serde_json::to_string(&release.key).unwrap()],
                |r| r.get(0),
            )
            .unwrap();
        let outcome = Outcome::Released {
            grant_id: serde_json::from_str::<crate::machine::GrantSnapshot>(&grant)
                .unwrap()
                .grant_id,
            sealed_sequence: release.sealed_sequence,
        };
        reply(&tx, &secret, outcome.clone());
        // The same authenticated reply is replayed without another transition.
        assert!(
            !accept(
                &tx,
                &pending,
                &crate::machine::Reply {
                    coordinator_revision: 1,
                    session: pending.session.clone(),
                    operation_id: pending.operation_id,
                    request_sequence: pending.request_sequence,
                    outcome
                }
            )
            .unwrap()
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
        assert_eq!(
            store
                .connection
                .query_row("SELECT COUNT(*) FROM invocations", [], |r| r
                    .get::<_, u64>(0))
                .unwrap(),
            1
        );
        let starts = std::fs::read_to_string(stdout)
            .unwrap_or_default()
            .matches("attached-kernel-runtime")
            .count();
        assert_eq!(starts, usize::from(matches!(stage, "released" | "sealed")));
        println!(
            "{}",
            serde_json::json!({"stage":stage,"actual_process_killed":true,"user_starts":starts,
            "consumed":consumed,"seal":after["state"]["records"][&key]["seal"],"released_after_ack":true})
        );
    }
}
