use std::time::Instant;

use stillyard::{
    CancellationToken, Client, ContainmentId, ContainmentIncidentCursor, DoctorSnapshot,
    JobChildrenCursor, JobId, JobSelector, JobTreeSelector,
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
    assert!(std::mem::size_of::<DoctorSnapshot>() > 0);
}
