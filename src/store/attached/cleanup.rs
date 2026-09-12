//! Retain executor seals across delayed Ticket replies and inventory recovery.
use super::*;
use crate::machine::{InvocationTicket, TicketCleanup, manager};

pub(super) fn initialize(c: &Connection) -> StoreResult<()> {
    c.execute_batch(
        "CREATE TABLE IF NOT EXISTS attached_local_cleanup(
        invocation_id TEXT PRIMARY KEY, boundary_sha256 TEXT NOT NULL,
        proof_sha256 TEXT NOT NULL);",
    )?;
    Ok(())
}

#[cfg(target_os = "linux")]
impl Store {
    pub(crate) fn record_attached_cleanup(
        &mut self,
        invocation: InvocationId,
        boundary: &str,
        proof: &str,
        never: Option<&manager::release::NeverReleased>,
    ) -> StoreResult<()> {
        let tx = self.connection.transaction()?;
        tx.execute(
            "INSERT OR IGNORE INTO attached_local_cleanup VALUES (?1,?2,?3)",
            params![invocation.to_string(), boundary, proof],
        )?;
        let prior:(String,String)=tx.query_row("SELECT boundary_sha256,proof_sha256 FROM attached_local_cleanup WHERE invocation_id=?1",
            [invocation.to_string()],|r|Ok((r.get(0)?,r.get(1)?)))?;
        if prior != (boundary.into(), proof.into()) {
            return Err(StoreError::InvalidState(
                "executor cleanup seal changed".into(),
            ));
        }
        if let Some(never) = never {
            let json: String = tx.query_row(
                "SELECT ticket_json FROM attached_tickets WHERE invocation_id=?1",
                [invocation.to_string()],
                |r| r.get(0),
            )?;
            let ticket: InvocationTicket = serde_json::from_str(&json)?;
            manager::release::record_unused_cleanup(
                &tx,
                never,
                &TicketCleanup {
                    invocation_id: invocation,
                    release_sequence: ticket.intent.release_sequence,
                    boundary_sha256: boundary.into(),
                    proof_sha256: proof.into(),
                    user_code_released: false,
                },
            )
            .map_err(protocol_error)?;
        }
        apply(&tx)?;
        tx.commit()?;
        Ok(())
    }
}

pub(super) fn apply(tx: &Transaction<'_>) -> StoreResult<()> {
    let rows = {
        let mut stmt = tx.prepare(
            "SELECT t.ticket_json,t.consumed,c.boundary_sha256,c.proof_sha256
            FROM attached_tickets t JOIN attached_local_cleanup c USING(invocation_id)
            WHERE t.cleanup_json IS NULL",
        )?;
        stmt.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, bool>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?
    };
    for (json, consumed, boundary, proof) in rows {
        let ticket: InvocationTicket = serde_json::from_str(&json)?;
        manager::record_cleanup(
            tx,
            &TicketCleanup {
                invocation_id: ticket.intent.invocation_id,
                release_sequence: ticket.intent.release_sequence,
                boundary_sha256: boundary,
                proof_sha256: proof,
                user_code_released: consumed,
            },
        )
        .map_err(protocol_error)?;
    }
    // Inventory can recover a Ticket that never reached the continuous local
    // Store. Its durable request and executor seal prove an unused obligation.
    let missing = {
        let mut statement=tx.prepare("SELECT intent.value,c.boundary_sha256,c.proof_sha256
            FROM attached_grants g JOIN json_each(g.grant_json,'$.tickets') intent
            JOIN attached_local_cleanup c ON c.invocation_id=json_extract(intent.value,'$.invocation_id')
            WHERE NOT EXISTS(SELECT 1 FROM attached_tickets t WHERE t.invocation_id=c.invocation_id)
              AND NOT EXISTS(SELECT 1 FROM attached_recovered_cleanup r WHERE r.invocation_id=c.invocation_id)")?;
        statement
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    for (json, boundary, proof) in missing {
        let intent: crate::machine::InvocationIntent = serde_json::from_str(&json)?;
        manager::record_cleanup(
            tx,
            &TicketCleanup {
                invocation_id: intent.invocation_id,
                release_sequence: intent.release_sequence,
                boundary_sha256: boundary,
                proof_sha256: proof,
                user_code_released: false,
            },
        )
        .map_err(protocol_error)?;
    }
    Ok(())
}
