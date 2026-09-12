use super::*;
use crate::admission::{ScheduleKey, effective_priority_at, outranks};

pub(super) struct RemoteReservation {
    pub(super) candidate: crate::machine::Candidate,
    pub(super) rank: ScheduleKey,
    pub(super) physical: ResolvedClaims,
}

pub(super) fn remote_reservations(
    connection: &Connection,
    now: i64,
) -> StoreResult<Vec<RemoteReservation>> {
    let mut statement = connection.prepare("SELECT c.candidate_json,q.accepted_ms,q.sequence,r.physical_claims_json FROM machine_reservations r JOIN machine_candidates c USING(allocation_key) JOIN machine_queue q ON q.owner=c.queue_owner JOIN machine_domains d USING(domain_id) WHERE c.state='ready' AND c.expires_ms>?1 AND c.reservation_deadline_ms>?1 AND d.reconciliation_required=0")?;
    statement
        .query_map([now], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, String>(3)?,
            ))
        })?
        .map(|row| {
            let (candidate, accepted_ms, rowid, physical) = row?;
            let candidate: crate::machine::Candidate = serde_json::from_str(&candidate)?;
            Ok(RemoteReservation {
                rank: ScheduleKey {
                    effective_priority: effective_priority_at(candidate.priority, accepted_ms, now),
                    accepted_ms,
                    rowid,
                },
                candidate,
                physical: serde_json::from_str(&physical)?,
            })
        })
        .collect()
}

pub(super) fn remote_debits(
    connection: &Connection,
    now: i64,
    higher_than: Option<ScheduleKey>,
) -> StoreResult<Vec<ResolvedClaims>> {
    Ok(remote_reservations(connection, now)?
        .into_iter()
        .filter(|r| higher_than.is_none_or(|rank| outranks(r.rank, rank)))
        .map(|r| r.physical)
        .collect())
}

pub(super) fn expire(connection: &Connection, now: i64) -> StoreResult<bool> {
    let changed=connection.execute("UPDATE machine_candidates SET reservation_deadline_ms=NULL,not_before_ms=?2 WHERE reservation_deadline_ms IS NOT NULL AND (reservation_deadline_ms<=?1 OR expires_ms<=?1 OR state!='ready')",params![now,now.saturating_add(5000)])?;
    connection.execute("DELETE FROM machine_reservations WHERE allocation_key IN (SELECT allocation_key FROM machine_candidates WHERE reservation_deadline_ms IS NULL OR state!='ready' OR expires_ms<=?1)",[now])?;
    Ok(changed > 0)
}

pub(super) fn drop_reservation(connection: &Connection, key: &str) -> StoreResult<bool> {
    connection.execute(
        "UPDATE machine_candidates SET reservation_deadline_ms=NULL WHERE allocation_key=?1",
        [key],
    )?;
    Ok(connection.execute(
        "DELETE FROM machine_reservations WHERE allocation_key=?1",
        [key],
    )? > 0)
}
