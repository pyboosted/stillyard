use std::time::Instant;

use stillyard::{
    CancellationToken, Client, CompleteDoctorSnapshot, ContainmentId, ContainmentIncidentCursor,
    DefaultInstance, DoctorSnapshot, JobChildrenCursor, JobId, JobSelector, JobTreeSelector,
};

type ExternalConsumerSignature = for<'client, 'cancellation> fn(
    &'client Client,
    Option<ContainmentIncidentCursor>,
    ContainmentId,
    JobId,
    JobChildrenCursor,
    Instant,
    Option<&'cancellation CancellationToken>,
);

#[allow(dead_code)]
fn external_consumer_compiles(
    client: &Client,
    cursor: Option<ContainmentIncidentCursor>,
    containment_id: ContainmentId,
    job_id: JobId,
    children_cursor: JobChildrenCursor,
    deadline: Instant,
    cancellation: Option<&CancellationToken>,
) {
    let _: stillyard::Result<DoctorSnapshot> =
        client.doctor(cursor, Some(256), deadline, cancellation);
    let _: stillyard::Result<CompleteDoctorSnapshot> =
        client.doctor_complete(deadline, cancellation);
    let _: stillyard::Result<stillyard::ClearContainmentResult> =
        client.force_clear_containment(containment_id, deadline, cancellation);
    let _: stillyard::Result<stillyard::JobTreePage> = client.tree(
        JobSelector::Jobs {
            job_ids: vec![job_id],
        },
        None,
        1,
        256,
        None,
        deadline,
        cancellation,
    );
    let _: stillyard::Result<stillyard::JobTreePage> =
        client.tree_for_job(job_id, 256, None, deadline, cancellation);
    let _: stillyard::Result<stillyard::JobChildrenPage> =
        client.tree_children(children_cursor, 256, None, deadline, cancellation);
    let _: stillyard::Result<stillyard::TreeObservationFrame> = client.observe_trees(
        JobTreeSelector {
            root_job_ids: vec![job_id],
        },
        None,
        256,
        1,
        256,
        None,
        std::time::Duration::ZERO,
        deadline,
        cancellation,
    );
}

#[test]
fn public_methods_are_callable_from_an_external_crate() {
    let _: ExternalConsumerSignature = external_consumer_compiles;
    let coordinates: DefaultInstance = stillyard::default_instance().unwrap();
    assert!(coordinates.store_path.is_absolute());
    assert!(!coordinates.endpoint.is_empty());
    assert!(std::mem::size_of::<DoctorSnapshot>() > 0);
}

#[test]
fn default_instance_ignores_ambient_coordinates() {
    let expected = stillyard::default_instance().unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            "default_instance_ambient_helper",
            "--nocapture",
        ])
        .env("STILLYARD_ENDPOINT", r"\\.\pipe\definitely-not-the-default")
        .env("STILLYARD_STORE", r"C:\definitely-not-the-default")
        .env("STILLYARD_JOB_ID", "malformed-managed-coordinate")
        .env("EXPECTED_DEFAULT_ENDPOINT", &expected.endpoint)
        .env("EXPECTED_DEFAULT_STORE", &expected.store_path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "helper failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
#[ignore = "launched as an ambient-coordinate helper"]
fn default_instance_ambient_helper() {
    let actual = stillyard::default_instance().unwrap();
    assert_eq!(
        actual.endpoint,
        std::env::var("EXPECTED_DEFAULT_ENDPOINT").unwrap()
    );
    assert_eq!(
        actual.store_path,
        std::path::PathBuf::from(std::env::var_os("EXPECTED_DEFAULT_STORE").unwrap())
    );
}
