use super::*;

const CLOSED_CONTAINMENT_STATES: [&str; 2] = ["empty", "cleared"];

pub(super) fn release_attempt_lease_if_safe(
    transaction: &Transaction<'_>,
    attempt_id: &str,
) -> StoreResult<bool> {
    let eligible: bool = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM attempts WHERE id = ?1 AND state = 'settled')
            AND NOT EXISTS(
                SELECT 1 FROM containments
                JOIN invocations ON invocations.id = containments.invocation_id
                WHERE invocations.attempt_id = ?1
                  AND containments.state NOT IN (?2, ?3)
            )",
        params![
            attempt_id,
            CLOSED_CONTAINMENT_STATES[0],
            CLOSED_CONTAINMENT_STATES[1]
        ],
        |row| row.get(0),
    )?;
    if !eligible {
        return Ok(false);
    }
    let released = transaction.execute(
        "UPDATE leases SET state = 'released'
         WHERE attempt_id = ?1 AND state = 'granted'",
        [attempt_id],
    )? > 0;
    Ok(released)
}

pub(super) fn release_never_run_attempt_lease_if_safe(
    transaction: &Transaction<'_>,
    attempt_id: &str,
) -> StoreResult<bool> {
    let eligible: bool = transaction.query_row(
        "SELECT EXISTS(
                SELECT 1 FROM attempts
                WHERE id = ?1 AND state IN ('starting', 'running')
            )
            AND NOT EXISTS(
                SELECT 1 FROM invocations
                WHERE attempt_id = ?1 AND state != 'resolved'
            )
            AND NOT EXISTS(
                SELECT 1 FROM containments
                JOIN invocations ON invocations.id = containments.invocation_id
                WHERE invocations.attempt_id = ?1
                  AND containments.state NOT IN (?2, ?3)
            )",
        params![
            attempt_id,
            CLOSED_CONTAINMENT_STATES[0],
            CLOSED_CONTAINMENT_STATES[1]
        ],
        |row| row.get(0),
    )?;
    if !eligible {
        return Ok(false);
    }
    let released = transaction.execute(
        "UPDATE leases SET state = 'released'
         WHERE attempt_id = ?1 AND state = 'granted'",
        [attempt_id],
    )? > 0;
    Ok(released)
}

pub(super) fn attempt_lease_release_eligible_after_target(
    transaction: &Transaction<'_>,
    attempt_id: &str,
    target_containment_id: &str,
) -> StoreResult<bool> {
    transaction
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM attempts WHERE id = ?1 AND state = 'settled')
                AND EXISTS(SELECT 1 FROM leases
                           WHERE attempt_id = ?1 AND state = 'granted')
                AND NOT EXISTS(
                    SELECT 1 FROM containments
                    JOIN invocations ON invocations.id = containments.invocation_id
                    WHERE invocations.attempt_id = ?1
                      AND containments.id != ?2
                      AND containments.state NOT IN (?3, ?4)
                )",
            params![
                attempt_id,
                target_containment_id,
                CLOSED_CONTAINMENT_STATES[0],
                CLOSED_CONTAINMENT_STATES[1]
            ],
            |row| row.get(0),
        )
        .map_err(Into::into)
}

pub(super) fn release_all_safe_attempt_leases(transaction: &Transaction<'_>) -> StoreResult<usize> {
    transaction
        .execute(
            "UPDATE leases SET state = 'released'
             WHERE state = 'granted'
               AND EXISTS(SELECT 1 FROM attempts
                          WHERE attempts.id = leases.attempt_id
                            AND attempts.state = 'settled')
               AND NOT EXISTS(
                    SELECT 1 FROM containments
                    JOIN invocations ON invocations.id = containments.invocation_id
                    WHERE invocations.attempt_id = leases.attempt_id
                      AND containments.state NOT IN (?1, ?2)
               )",
            params![CLOSED_CONTAINMENT_STATES[0], CLOSED_CONTAINMENT_STATES[1]],
        )
        .map_err(Into::into)
}
