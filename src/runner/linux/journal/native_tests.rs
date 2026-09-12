use super::*;
use crate::machine::{
    AllocationKey, AllocationOwner, Claims, GrantState, NativeAllocationSnapshot,
    NativeStartPermission,
};

fn fixture() -> (Anchor, InvocationId, Record, NativeStartPermission) {
    let anchor = Anchor {
        journal: Uuid::now_v7(),
        store: Uuid::now_v7(),
        domain: crate::ExecutionDomainId(Uuid::now_v7()),
    };
    let invocation = InvocationId::from_parts(anchor.store, Uuid::now_v7());
    let identity =
        crate::identity::linux::Process::open(std::process::id(), unsafe { libc::geteuid() })
            .unwrap()
            .identity;
    let record = Record {
        containment: ContainmentId::from_parts(anchor.store, Uuid::now_v7()),
        lease: Uuid::now_v7(),
        daemon_generation: Uuid::now_v7(),
        creator: identity.clone(),
        boundary: Some(Identity {
            boot_id: Uuid::now_v7().to_string(),
            path: "/sys/fs/cgroup/test-only".into(),
            device: 1,
            inode: 2,
        }),
        root: Some(identity.clone()),
        executable_sha256: Some("a".repeat(64)),
        release_intent: None,
        native_release_intent: None,
        seal: None,
    };
    let permission = NativeStartPermission {
        allocation: NativeAllocationSnapshot {
            grant_id: crate::GrantId::from_parts(anchor.store, record.lease),
            lease_id: record.lease,
            key: Some(AllocationKey {
                machine_id: Uuid::now_v7(),
                authority_epoch: Uuid::now_v7(),
                domain_id: anchor.domain,
                manager_store_uuid: anchor.store,
                lease_id: record.lease,
            }),
            owner: AllocationOwner::Work {
                job_id: crate::JobId::from_parts(anchor.store, Uuid::now_v7()),
                attempt_id: crate::AttemptId::from_parts(anchor.store, Uuid::now_v7()),
            },
            state: GrantState::Armed,
            claims: Claims::default(),
        },
        invocation_id: invocation,
        containment_id: record.containment,
        root_identity: identity.clone(),
        creator_identity: identity,
        daemon_generation: record.daemon_generation,
        executable_sha256: "a".repeat(64),
        boundary_kind: "linux_cgroup_v2".into(),
    };
    (anchor, invocation, record, permission)
}

#[test]
fn absent_native_intent_preserves_legacy_record_bytes_and_checksum() {
    // Exact pre-MR4 field order: State checksums serialize these typed records.
    #[derive(Serialize)]
    struct LegacyRecord<'a> {
        containment: ContainmentId,
        lease: Uuid,
        daemon_generation: Uuid,
        creator: &'a ProcessIdentity,
        boundary: &'a Option<Identity>,
        root: &'a Option<ProcessIdentity>,
        executable_sha256: &'a Option<String>,
        release_intent: &'a Option<crate::machine::InvocationTicket>,
        seal: &'a Option<Seal>,
    }
    let (_, _, record, _) = fixture();
    let legacy = LegacyRecord {
        containment: record.containment,
        lease: record.lease,
        daemon_generation: record.daemon_generation,
        creator: &record.creator,
        boundary: &record.boundary,
        root: &record.root,
        executable_sha256: &record.executable_sha256,
        release_intent: &record.release_intent,
        seal: &record.seal,
    };
    let bytes = serde_json::to_vec(&legacy).unwrap();
    assert_eq!(bytes, serde_json::to_vec(&record).unwrap());
    let recovered: Record = serde_json::from_slice(&bytes).unwrap();
    assert!(recovered.native_release_intent.is_none());
    assert_eq!(digest(&legacy).unwrap(), digest(&recovered).unwrap());
}

#[test]
fn native_permission_is_bound_to_exact_prepared_obligation() {
    let (anchor, invocation, record, permission) = fixture();
    record
        .validate_native_permission(&anchor, invocation, &permission)
        .unwrap();
    let mutants: [fn(&mut NativeStartPermission); 10] = [
        |p| p.invocation_id = InvocationId::from_parts(Uuid::now_v7(), Uuid::now_v7()),
        |p| p.containment_id = ContainmentId::from_parts(Uuid::now_v7(), Uuid::now_v7()),
        |p| p.daemon_generation = Uuid::now_v7(),
        |p| p.executable_sha256 = "b".repeat(64),
        |p| p.boundary_kind = "windows_job_object".into(),
        |p| p.allocation.key.as_mut().unwrap().domain_id.0 = Uuid::now_v7(),
        |p| p.allocation.key.as_mut().unwrap().manager_store_uuid = Uuid::now_v7(),
        |p| p.allocation.lease_id = Uuid::now_v7(),
        |p| p.allocation.state = GrantState::Released,
        |p| p.allocation.key = None,
    ];
    for mutate in mutants {
        let mut wrong = permission.clone();
        mutate(&mut wrong);
        assert!(
            record
                .validate_native_permission(&anchor, invocation, &wrong)
                .is_err()
        );
    }
    let mut missing_boundary = record;
    missing_boundary.boundary = None;
    assert!(
        missing_boundary
            .validate_native_permission(&anchor, invocation, &permission)
            .is_err()
    );
}

#[test]
fn native_intent_survives_reopen_and_cannot_be_replayed_or_sealed_by_absence() {
    let (anchor, invocation, record, permission) = fixture();
    let temp = crate::test_support::durable_tempdir().unwrap();
    let path = temp.path().join("executor");
    let mut journal = Journal::initialize(&path, anchor.clone()).unwrap();
    let mut state = journal.state.clone();
    state.records.insert(invocation, record);
    journal.commit(state).unwrap();
    let hash = journal.records().unwrap()[&invocation]
        .boundary_sha256()
        .unwrap();
    journal.native_release_intent(&permission).unwrap();
    assert!(journal.native_release_intent(&permission).is_err());
    drop(journal);
    let mut recovered = Journal::open(&path, &anchor).unwrap();
    let record = &recovered.records().unwrap()[&invocation];
    assert!(record.possibly_released());
    assert_eq!(record.native_release_intent.as_ref(), Some(&permission));
    assert_eq!(record.boundary_sha256().unwrap(), hash);
    assert!(recovered.cleanup(invocation, Instant::now()).is_err());
    assert!(recovered.records().unwrap()[&invocation].seal.is_none());
}
