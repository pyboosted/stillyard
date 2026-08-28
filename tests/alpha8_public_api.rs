use std::time::Instant;

use stillyard::{
    CancellationToken, Client, ContainmentId, ContainmentIncidentCursor, DoctorSnapshot,
};

#[allow(dead_code)]
fn external_consumer_compiles(
    client: &Client,
    cursor: Option<ContainmentIncidentCursor>,
    containment_id: ContainmentId,
    deadline: Instant,
    cancellation: Option<&CancellationToken>,
) {
    let _: stillyard::Result<DoctorSnapshot> =
        client.doctor(cursor, Some(256), deadline, cancellation);
    let _: stillyard::Result<stillyard::ClearContainmentResult> =
        client.force_clear_containment(containment_id, deadline, cancellation);
}

#[test]
fn alpha8_public_methods_are_callable_from_an_external_crate() {
    assert!(std::mem::size_of::<DoctorSnapshot>() > 0);
}
