//! Refresh every locally ready candidate and retire unused remote rights when
//! local readiness disappears. The coordinator remains the sole global chooser.
use super::*;
use crate::machine::{Outcome, manager};

const READY_MILLIS: i64 = 2_000;

pub(super) fn initialize(c: &Connection) -> StoreResult<()> {
    c.execute_batch(
        "CREATE TABLE IF NOT EXISTS attached_local_liveness(
        lease_id TEXT PRIMARY KEY REFERENCES attached_local_plans(lease_id),
        ready_ms INTEGER NOT NULL, poll_ms INTEGER NOT NULL, advertised_ms INTEGER NOT NULL);",
    )?;
    Ok(())
}
pub(super) fn touch(tx: &Transaction<'_>, lease: Uuid, first: bool) -> StoreResult<()> {
    let now = now_millis();
    if first {
        tx.execute(
            "INSERT INTO attached_local_liveness VALUES (?1,?2,0,?2)",
            params![lease.to_string(), now],
        )?;
    } else {
        tx.execute(
            "UPDATE attached_local_liveness SET ready_ms=?2 WHERE lease_id=?1",
            params![lease.to_string(), now],
        )?;
    }
    Ok(())
}
fn pending(tx: &Transaction<'_>, command: Command) -> StoreResult<()> {
    manager::enqueue(tx, Uuid::now_v7(), &command).map_err(protocol_error)?;
    Ok(())
}
fn retire_slot(tx: &Transaction<'_>, key: &AllocationKey) -> StoreResult<()> {
    tx.execute(
        "UPDATE attached_local_plans SET released=1,slot='retired:'||slot||':'||lease_id
        WHERE allocation_key=?1 AND committed=0 AND released=0",
        [serde_json::to_string(key)?],
    )?;
    Ok(())
}

/// Caller has already drained the outbox. At most one maintenance operation is
/// added per turn; polling order rotates over all candidates, never one head.
#[cfg_attr(windows, allow(dead_code))]
pub(super) fn maintain(tx: &Transaction<'_>) -> StoreResult<bool> {
    super::recovery::acknowledge_settled(tx)?;
    let (count,applied): (u64,u64) = tx.query_row("SELECT (SELECT COUNT(*) FROM attached_outbox),applied_sequence FROM attached_peer WHERE singleton=1",[],|r|Ok((r.get(0)?,r.get(1)?)))?;
    if count >= 64 {
        pending(
            tx,
            Command::Acknowledge {
                through_sequence: applied,
            },
        )?;
        return Ok(true);
    }
    // Completed cleanup must make progress even if another candidate repeatedly
    // loses readiness or its cancellation is rejected by the coordinator.
    let ready_release: Option<String> = tx.query_row("SELECT p.allocation_key FROM attached_local_plans p
        JOIN attached_grants g USING(allocation_key)
        WHERE p.release_pending=1 AND p.released=0 AND g.seal_json IS NULL
          AND NOT EXISTS(SELECT 1 FROM attached_tickets t WHERE t.allocation_key=p.allocation_key AND t.cleanup_json IS NULL)
        ORDER BY p.rowid LIMIT 1",[],|r|r.get(0)).optional()?;
    if let Some(json) = ready_release {
        manager::seal_release(tx, &serde_json::from_str(&json)?, Uuid::now_v7())
            .map_err(protocol_error)?;
        return Ok(true);
    }
    let now = now_millis();
    // A canceled/stale candidate may already own Arm, but no Invocation has
    // committed yet. Seal the empty set of start rights before retiring it.
    let stale: Option<(String, bool, u64)> = tx
        .query_row(
            "SELECT p.allocation_key,p.armed,json_extract(p.candidate_json,'$.revision') FROM attached_local_plans p
        JOIN jobs j ON j.id=p.job_id JOIN attached_local_liveness t USING(lease_id)
        WHERE p.committed=0 AND p.released=0 AND p.release_pending=0
          AND (j.state!='pending' OR j.cancel_requested!=0 OR j.attempt_id!=p.attempt_id
            OR t.ready_ms>?1 OR t.ready_ms<?2)
        ORDER BY t.ready_ms,p.rowid LIMIT 1",
            params![now, now.saturating_sub(READY_MILLIS)],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;
    if let Some((json, armed, revision)) = stale {
        let key: AllocationKey = serde_json::from_str(&json)?;
        if armed {
            tx.execute(
                "UPDATE attached_local_plans SET release_pending=1 WHERE allocation_key=?1",
                [&json],
            )?;
            manager::seal_release(tx, &key, Uuid::now_v7()).map_err(protocol_error)?;
        } else {
            let revision = revision
                .checked_add(1)
                .filter(|n| *n <= i64::MAX as u64)
                .ok_or_else(|| StoreError::InvalidState("candidate revision exhausted".into()))?;
            pending(tx, Command::CancelCandidate { key, revision })?;
        }
        return Ok(true);
    }
    let candidate: Option<(String, String, i64)> = tx
        .query_row(
            "SELECT p.lease_id,p.candidate_json,t.advertised_ms
        FROM attached_local_plans p JOIN attached_local_liveness t USING(lease_id)
        WHERE p.committed=0 AND p.armed=0 AND p.released=0 AND p.release_pending=0
          AND t.ready_ms BETWEEN ?1 AND ?2 AND t.poll_ms<=?3
        ORDER BY t.poll_ms,p.rowid LIMIT 1",
            params![
                now.saturating_sub(READY_MILLIS),
                now,
                now.saturating_sub(100)
            ],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;
    if let Some((lease, json, advertised)) = candidate {
        let candidate: Candidate = serde_json::from_str(&json)?;
        let refresh = now.saturating_sub(advertised) >= 10_000 || advertised > now;
        pending(
            tx,
            if refresh {
                Command::CandidateUpsert { candidate }
            } else {
                Command::Inspect {
                    key: Some(candidate.key),
                }
            },
        )?;
        tx.execute("UPDATE attached_local_liveness SET poll_ms=?2,advertised_ms=CASE WHEN ?3 THEN ?2 ELSE advertised_ms END WHERE lease_id=?1",
            params![lease,now,refresh])?;
        return Ok(true);
    }
    Ok(false)
}

pub(super) fn accepted(
    tx: &Transaction<'_>,
    request: &crate::machine::Request,
    outcome: &Outcome,
) -> StoreResult<()> {
    match outcome {
        Outcome::Inspection { grants, truncated } => {
            let Command::Inspect { key: Some(key) } = &request.command else {
                return Ok(());
            };
            if *truncated || grants.len() > 1 || grants.iter().any(|g| &g.candidate.key != key) {
                return Err(StoreError::InvalidState(
                    "candidate inspection returned another allocation".into(),
                ));
            }
            for grant in grants.iter().filter(|g| g.state == GrantState::Offered) {
                let now = now_millis();
                let ready:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM attached_local_plans p
                    JOIN attached_local_liveness t USING(lease_id) JOIN jobs j ON j.id=p.job_id
                    WHERE p.allocation_key=?1 AND p.candidate_json=?2 AND p.armed=0 AND p.committed=0
                      AND p.released=0 AND p.release_pending=0 AND j.state='pending' AND j.cancel_requested=0
                      AND j.attempt_id=p.attempt_id AND t.ready_ms BETWEEN ?3 AND ?4)",
                    params![serde_json::to_string(key)?,serde_json::to_string(&grant.candidate)?,now.saturating_sub(READY_MILLIS),now],|r|r.get(0))?;
                if ready {
                    pending(
                        tx,
                        Command::Arm {
                            key: key.clone(),
                            offer_nonce: grant.offer_nonce,
                        },
                    )?;
                }
            }
        }
        Outcome::Accepted { .. } => {
            if let Command::CancelCandidate { key, .. } | Command::Withdraw { key, .. } =
                &request.command
            {
                let armed: bool = tx.query_row(
                    "SELECT armed FROM attached_local_plans WHERE allocation_key=?1",
                    [serde_json::to_string(key)?],
                    |r| r.get(0),
                )?;
                if armed {
                    return Err(StoreError::InvalidState(
                        "candidate cancellation cannot release an Armed Grant".into(),
                    ));
                }
                retire_slot(tx, key)?;
            }
        }
        Outcome::Released { .. } => {
            if let Command::Release { release } = &request.command {
                // Retire the planning slot while preserving every durable ID.
                // A deferred primary can then request a fresh Grant/Invocation.
                tx.execute(
                    "UPDATE attached_local_plans SET slot='retired:'||slot||':'||lease_id
                    WHERE allocation_key=?1 AND released=1 AND slot NOT LIKE 'retired:%'",
                    [serde_json::to_string(&release.key)?],
                )?;
            }
        }
        Outcome::Reconciled { released, .. } => {
            for key in released {
                tx.execute(
                    "UPDATE attached_local_plans SET slot='retired:'||slot||':'||lease_id
                    WHERE allocation_key=?1 AND released=1 AND slot NOT LIKE 'retired:%'",
                    [serde_json::to_string(key)?],
                )?;
            }
        }
        _ => {}
    }
    Ok(())
}
