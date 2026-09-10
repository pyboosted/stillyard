use super::*;

#[derive(Clone, Debug)]
pub(super) struct StoredReservation {
    pub(super) job_id: String,
    pub(super) claims: ResolvedClaims,
    pub(super) hold_deadline_ms: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ScalarDisposition {
    Grant,
    Hold,
    Created,
    StateChanged,
}

pub(crate) use crate::admission::effective_priority_at;
use crate::admission::{ScheduleKey, outranks};

impl Store {
    pub(super) fn reservation_for_job(
        &self,
        job_id: JobId,
    ) -> StoreResult<Option<crate::ScalarReservation>> {
        let now = now_millis();
        self.connection
            .query_row(
                "SELECT reservations.id, reservations.claims_json,
                        reservations.created_ms, reservations.hold_deadline_ms
                 FROM reservations JOIN jobs ON jobs.id = reservations.job_id
                 WHERE reservations.job_id = ?1 AND reservations.hold_deadline_ms > ?2
                   AND jobs.state = 'pending' AND jobs.cancel_requested = 0",
                params![self.local_id(job_id)?, now],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .optional()?
            .map(|(id, claims, created_ms, hold_deadline_ms)| {
                let claims: ResolvedClaims = serde_json::from_str(&claims)?;
                Ok(crate::ScalarReservation {
                    reservation_id: crate::ReservationId::from_parts(
                        self.store_uuid,
                        Uuid::parse_str(&id)?,
                    ),
                    claims: claims.public_scalars(),
                    created_unix_millis: created_ms,
                    hold_deadline_unix_millis: hold_deadline_ms,
                })
            })
            .transpose()
    }

    pub(super) fn active_reservations(&self) -> StoreResult<Vec<StoredReservation>> {
        let mut statement = self.connection.prepare(
            "SELECT reservations.job_id, reservations.claims_json,
                    reservations.hold_deadline_ms
             FROM reservations JOIN jobs ON jobs.id = reservations.job_id
             WHERE reservations.hold_deadline_ms > ?1
               AND jobs.state = 'pending' AND jobs.cancel_requested = 0",
        )?;
        let rows = statement.query_map([now_millis()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?;
        rows.map(|row| {
            let (job_id, claims, hold_deadline_ms) = row?;
            Ok(StoredReservation {
                job_id,
                claims: serde_json::from_str(&claims)?,
                hold_deadline_ms,
            })
        })
        .collect()
    }

    pub(super) fn expire_due_reservations(&mut self, now: i64) -> StoreResult<bool> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = expire_due_reservations_tx(&transaction, now)?;
        if changed {
            transaction.commit()?;
        } else {
            transaction.rollback()?;
        }
        Ok(changed)
    }

    pub(super) fn reservation_debits_for_job(
        &self,
        job_key: &str,
    ) -> StoreResult<Vec<ResolvedClaims>> {
        let now = now_millis();
        let reservations = self.active_reservations()?;
        if !reservations
            .iter()
            .any(|reservation| reservation.job_id == job_key)
        {
            let mut debits: Vec<_> = reservations
                .into_iter()
                .map(|reservation| reservation.claims)
                .collect();
            debits.extend(super::machine_reservation::remote_debits(
                &self.connection,
                now,
                None,
            )?);
            return Ok(debits);
        }
        let candidate = schedule_key(&self.connection, job_key, now)?;
        let mut blocking = Vec::new();
        blocking.extend(super::machine_reservation::remote_debits(
            &self.connection,
            now,
            Some(candidate),
        )?);
        for reservation in reservations {
            if reservation.job_id != job_key
                && outranks(
                    schedule_key(&self.connection, &reservation.job_id, now)?,
                    candidate,
                )
            {
                blocking.push(reservation.claims);
            }
        }
        Ok(blocking)
    }
}

pub(super) fn normalize_reservations_for_capacities(
    connection: &mut Connection,
    capacities: &ResourceCapacities,
    now: i64,
) -> StoreResult<bool> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let changed = normalize_reservations_for_capacities_tx(&transaction, capacities, now)?;
    if changed {
        transaction.commit()?;
    } else {
        transaction.rollback()?;
    }
    Ok(changed)
}

fn normalize_reservations_for_capacities_tx(
    transaction: &Transaction<'_>,
    capacities: &ResourceCapacities,
    now: i64,
) -> StoreResult<bool> {
    let mut reservations = {
        let mut statement = transaction.prepare(
            "SELECT reservations.job_id, reservations.claims_json,
                    jobs.accepted_ms, jobs.rowid, jobs.spec_json
             FROM reservations JOIN jobs ON jobs.id = reservations.job_id
             WHERE reservations.hold_deadline_ms > ?1
               AND jobs.state = 'pending' AND jobs.cancel_requested = 0",
        )?;
        let rows = statement.query_map([now], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, String>(4)?,
            ))
        })?;
        rows.map(|row| {
            let (job_id, claims_json, accepted_ms, rowid, spec_json) = row?;
            let claims: ResolvedClaims = serde_json::from_str(&claims_json)?;
            let spec: JobSpec = serde_json::from_str(&spec_json)?;
            Ok((
                ScheduleKey {
                    effective_priority: effective_priority_at(spec.priority, accepted_ms, now),
                    accepted_ms,
                    rowid,
                },
                job_id,
                claims,
            ))
        })
        .collect::<StoreResult<Vec<_>>>()?
    };
    reservations.sort_by(|left, right| crate::admission::schedule_order(left.0, right.0));

    let mut retained = Vec::new();
    let mut suffix_start = None;
    for (index, (_, _, claims)) in reservations.iter().enumerate() {
        if claims.scalar_blockers(capacities, &retained).is_empty() {
            retained.push(claims.clone());
        } else {
            suffix_start = Some(index);
            break;
        }
    }
    let Some(suffix_start) = suffix_start else {
        return Ok(false);
    };

    let backoff = now.saturating_add(
        i64::try_from(crate::SCALAR_RESERVATION_BACKOFF_MILLIS).unwrap_or(i64::MAX),
    );
    for (_, job_id, _) in &reservations[suffix_start..] {
        transaction.execute(
            "UPDATE jobs SET reservation_not_before_ms =
                 MAX(COALESCE(reservation_not_before_ms, ?2), ?2)
             WHERE id = ?1 AND state = 'pending'",
            params![job_id, backoff],
        )?;
        release_reservation_tx(transaction, job_id)?;
    }
    Ok(true)
}

pub(super) fn expire_due_reservations_tx(
    transaction: &Transaction<'_>,
    now: i64,
) -> StoreResult<bool> {
    let backoff = now.saturating_add(
        i64::try_from(crate::SCALAR_RESERVATION_BACKOFF_MILLIS).unwrap_or(i64::MAX),
    );
    let updated = transaction.execute(
        "UPDATE jobs SET reservation_not_before_ms = ?2
         WHERE state = 'pending' AND id IN (
             SELECT job_id FROM reservations WHERE hold_deadline_ms <= ?1
         )",
        params![now, backoff],
    )?;
    if updated > 0 {
        transaction.execute(
            "DELETE FROM reservations WHERE hold_deadline_ms <= ?1",
            [now],
        )?;
    }
    Ok(updated > 0)
}

pub(super) fn release_reservation_tx(
    transaction: &Transaction<'_>,
    job_key: &str,
) -> StoreResult<bool> {
    Ok(transaction.execute("DELETE FROM reservations WHERE job_id = ?1", [job_key])? > 0)
}

pub(super) fn scalar_disposition_tx(
    transaction: &Transaction<'_>,
    store_uuid: Uuid,
    capacities: &ResourceCapacities,
    job_key: &str,
    claims: &ResolvedClaims,
    active: &[ResolvedClaims],
    now: i64,
) -> StoreResult<ScalarDisposition> {
    use crate::admission::ReservationDecision;
    let own = reservation_tx(transaction, job_key)?;
    let not_before: Option<i64> = transaction.query_row(
        "SELECT reservation_not_before_ms FROM jobs WHERE id = ?1",
        [job_key],
        |row| row.get(0),
    )?;
    let reservations = reservation_claims_tx(transaction, now, None)?;
    let higher = own.is_some() && higher_reservation_overlaps(transaction, job_key, claims, now)?;
    match crate::admission::local_reservation_decision(
        claims,
        capacities,
        active,
        &reservations,
        (
            own.as_ref().map(|reservation| reservation.hold_deadline_ms),
            higher,
            not_before,
            now,
        ),
    ) {
        ReservationDecision::Drop => {
            return Ok(if release_reservation_tx(transaction, job_key)? {
                ScalarDisposition::StateChanged
            } else {
                ScalarDisposition::Hold
            });
        }
        ReservationDecision::Expire => {
            let backoff = now.saturating_add(
                i64::try_from(crate::SCALAR_RESERVATION_BACKOFF_MILLIS).unwrap_or(i64::MAX),
            );
            transaction.execute(
                "UPDATE jobs SET reservation_not_before_ms = ?2 WHERE id = ?1",
                params![job_key, backoff],
            )?;
            release_reservation_tx(transaction, job_key)?;
            return Ok(ScalarDisposition::StateChanged);
        }
        ReservationDecision::Grant => {
            if own.is_some() {
                release_reservation_tx(transaction, job_key)?;
            }
            return Ok(ScalarDisposition::Grant);
        }
        ReservationDecision::Hold => return Ok(ScalarDisposition::Hold),
        ReservationDecision::Create => {}
    }
    let reservation_id = crate::ReservationId::new(store_uuid);
    let deadline = now
        .saturating_add(i64::try_from(crate::SCALAR_RESERVATION_HOLD_MILLIS).unwrap_or(i64::MAX));
    transaction.execute(
        "INSERT INTO reservations(id, job_id, claims_json, created_ms, hold_deadline_ms)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            reservation_id.entity_uuid().to_string(),
            job_key,
            serde_json::to_string(&claims.scalar_only())?,
            now,
            deadline,
        ],
    )?;
    Ok(ScalarDisposition::Created)
}

fn reservation_tx(
    transaction: &Transaction<'_>,
    job_key: &str,
) -> StoreResult<Option<StoredReservation>> {
    transaction
        .query_row(
            "SELECT job_id, claims_json, hold_deadline_ms
             FROM reservations WHERE job_id = ?1",
            [job_key],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )
        .optional()?
        .map(|(job_id, claims, hold_deadline_ms)| {
            Ok(StoredReservation {
                job_id,
                claims: serde_json::from_str(&claims)?,
                hold_deadline_ms,
            })
        })
        .transpose()
}

pub(super) fn reservation_claims_tx(
    transaction: &Transaction<'_>,
    now: i64,
    excluded_job: Option<&str>,
) -> StoreResult<Vec<ResolvedClaims>> {
    let mut statement = transaction.prepare(
        "SELECT reservations.claims_json FROM reservations
         JOIN jobs ON jobs.id = reservations.job_id
         WHERE reservations.hold_deadline_ms > ?1
           AND jobs.state = 'pending' AND jobs.cancel_requested = 0
           AND (?2 IS NULL OR reservations.job_id != ?2)",
    )?;
    let rows = statement.query_map(params![now, excluded_job], |row| row.get::<_, String>(0))?;
    let mut debits = rows
        .map(|row| Ok(serde_json::from_str(&row?)?))
        .collect::<StoreResult<Vec<_>>>()?;
    debits.extend(super::machine_reservation::remote_debits(
        transaction,
        now,
        None,
    )?);
    Ok(debits)
}

fn higher_reservation_overlaps(
    transaction: &Transaction<'_>,
    job_key: &str,
    claims: &ResolvedClaims,
    now: i64,
) -> StoreResult<bool> {
    let candidate = schedule_key(transaction, job_key, now)?;
    if super::machine_reservation::remote_debits(transaction, now, Some(candidate))?
        .iter()
        .any(|held| claims.overlaps_scalars(held))
    {
        return Ok(true);
    }
    let mut statement = transaction.prepare(
        "SELECT reservations.job_id, reservations.claims_json
         FROM reservations JOIN jobs ON jobs.id = reservations.job_id
         WHERE reservations.job_id != ?1 AND reservations.hold_deadline_ms > ?2
           AND jobs.state = 'pending' AND jobs.cancel_requested = 0",
    )?;
    let rows = statement.query_map(params![job_key, now], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    for row in rows {
        let (other_job, other_claims) = row?;
        if outranks(schedule_key(transaction, &other_job, now)?, candidate)
            && claims.overlaps_scalars(&serde_json::from_str(&other_claims)?)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(super) fn schedule_key(
    connection: &Connection,
    job_key: &str,
    now: i64,
) -> StoreResult<ScheduleKey> {
    let (accepted_ms, rowid, spec_json) = connection.query_row(
        "SELECT q.accepted_ms, q.sequence, j.spec_json FROM jobs j JOIN machine_queue q ON q.owner='native:' || j.id WHERE j.id = ?1",
        [job_key],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
            ))
        },
    )?;
    let spec: JobSpec = serde_json::from_str(&spec_json)?;
    Ok(ScheduleKey {
        effective_priority: effective_priority_at(spec.priority, accepted_ms, now),
        accepted_ms,
        rowid,
    })
}
