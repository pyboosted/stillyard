//! Durable manager-side protocol primitives. The caller owns the SQLite
//! transaction and commits these changes WITH its Lease/Invocation lifecycle.
//! No function performs IPC or releases user code. A successful SQL call is not
//! a committed permission until the caller has committed its transaction.
use super::*;
use rusqlite::{Connection, OptionalExtension, Transaction, params};

pub mod recovery;
pub mod release;

#[derive(Debug, thiserror::Error)]
pub enum JournalError {
    #[error("manager protocol history is unavailable: {0}")]
    History(String),
    #[error(transparent)]
    Database(#[from] rusqlite::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
pub type Result<T> = std::result::Result<T, JournalError>;
fn invalid(detail: &str) -> JournalError {
    JournalError::History(detail.into())
}

/// Schema creation belongs to explicit installation, never reconnect recovery.
/// The host manager's reset-independent pairing anchor must match `store`.
pub fn initialize(tx: &Transaction<'_>, store: Uuid) -> Result<()> {
    if store.is_nil() {
        return Err(invalid("nil manager store"));
    }
    tx.execute_batch("CREATE TABLE IF NOT EXISTS attached_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        INSERT OR IGNORE INTO attached_meta VALUES ('schema_version','2');
        CREATE TABLE IF NOT EXISTS attached_abandoned(operation_id TEXT PRIMARY KEY,sequence INTEGER NOT NULL,payload_sha256 TEXT NOT NULL,command_json TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS attached_inventory(allocation_key TEXT PRIMARY KEY,grant_json TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS attached_recovered_cleanup(invocation_id TEXT PRIMARY KEY,cleanup_json TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS attached_peer(
        singleton INTEGER PRIMARY KEY CHECK(singleton=1), store_uuid TEXT NOT NULL,
        session_json TEXT, next_sequence INTEGER NOT NULL, applied_sequence INTEGER NOT NULL,
        retired_floor INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS attached_outbox(
            sequence INTEGER PRIMARY KEY, operation_id TEXT UNIQUE NOT NULL,
            payload_sha256 TEXT NOT NULL, command_json TEXT NOT NULL, outcome_json TEXT);
        CREATE TABLE IF NOT EXISTS attached_grants(
            allocation_key TEXT PRIMARY KEY, grant_json TEXT NOT NULL, seal_json TEXT);
        CREATE TABLE IF NOT EXISTS attached_tickets(
            invocation_id TEXT PRIMARY KEY, allocation_key TEXT NOT NULL REFERENCES attached_grants(allocation_key),
            ticket_json TEXT NOT NULL, consumed INTEGER NOT NULL DEFAULT 0, cleanup_json TEXT);")?;
    let version: String = tx.query_row(
        "SELECT value FROM attached_meta WHERE key='schema_version'",
        [],
        |r| r.get(0),
    )?;
    if version != "2" {
        return Err(invalid("unsupported manager journal schema"));
    }
    tx.execute(
        "INSERT OR IGNORE INTO attached_peer VALUES (1,?1,NULL,1,0,0)",
        [store.to_string()],
    )?;
    let existing: String = tx.query_row(
        "SELECT store_uuid FROM attached_peer WHERE singleton=1",
        [],
        |r| r.get(0),
    )?;
    if existing != store.to_string() {
        return Err(invalid("manager store differs from the pairing anchor"));
    }
    Ok(())
}

/// Bind an authenticated handshake. Lost response rows cannot be invented from
/// a remote watermark: the caller must first run full inventory recovery.
pub fn bind(
    tx: &Transaction<'_>,
    session: &SessionIdentity,
    participant: &ParticipantSnapshot,
) -> Result<()> {
    if recovery::active(tx)? {
        return Err(invalid("resume inventory recovery before normal binding"));
    }
    let (store, prior, next, applied, floor):(String,Option<String>,u64,u64,u64) = tx.query_row(
        "SELECT store_uuid,session_json,next_sequence,applied_sequence,retired_floor FROM attached_peer WHERE singleton=1",[],
        |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)))?;
    if store != session.manager_store_uuid.to_string()
        || participant.manager_store_uuid != session.manager_store_uuid
        || participant.installation.domain_id != session.domain_id
        || participant.connection_epoch != session.connection_epoch
        || participant.executor_incarnation != Some(session.executor_incarnation)
        || participant.accepted_sequence < applied
        || participant.accepted_sequence >= next
        || participant.retired_sequence_floor > applied
        || participant.retired_sequence_floor < floor
    {
        return Err(invalid(
            "peer watermark/session differs from durable manager history",
        ));
    }
    if let Some(prior) = prior {
        let prior: SessionIdentity = serde_json::from_str(&prior)?;
        if prior.machine_id != session.machine_id
            || prior.domain_id != session.domain_id
            || prior.authority_epoch != session.authority_epoch
            || prior.connection_epoch > session.connection_epoch
            || (prior.connection_epoch == session.connection_epoch && prior != *session)
        {
            return Err(invalid(
                "authority change requires inventory reconciliation",
            ));
        }
    }
    tx.execute(
        "UPDATE attached_peer SET session_json=?1 WHERE singleton=1",
        [serde_json::to_string(session)?],
    )?;
    Ok(())
}

/// Enqueue before sending, in the same transaction as the local lifecycle intent.
/// Reusing an operation ID with another command never creates a second operation.
pub fn enqueue(tx: &Transaction<'_>, operation: Uuid, command: &Command) -> Result<u64> {
    if operation.is_nil() {
        return Err(invalid("nil operation id"));
    }
    let hash = payload_hash(command)?;
    let prior: Option<(u64, String)> = tx
        .query_row(
            "SELECT sequence,payload_sha256 FROM attached_outbox WHERE operation_id=?1",
            [operation.to_string()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    if let Some((sequence, old)) = prior {
        return if old == hash {
            Ok(sequence)
        } else {
            Err(invalid("operation payload changed"))
        };
    }
    let abandoned: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM attached_abandoned WHERE operation_id=?1)",
        [operation.to_string()],
        |r| r.get(0),
    )?;
    if abandoned {
        return Err(invalid(
            "operation was superseded by recovery; reconcile its local lifecycle",
        ));
    }
    recovery::allow(tx, command)?;
    if let Command::AuthorizeInvocation { key, .. } = command {
        let seal: Option<String> = tx.query_row(
            "SELECT seal_json FROM attached_grants WHERE allocation_key=?1",
            [serde_json::to_string(key)?],
            |r| r.get(0),
        )?;
        if seal.is_some() {
            return Err(invalid(
                "allocation was already sealed against future starts",
            ));
        }
    }
    let (next,count,bytes):(u64,u64,u64) = tx.query_row("SELECT next_sequence,(SELECT COUNT(*) FROM attached_outbox),(SELECT COALESCE(SUM(length(CAST(command_json AS BLOB))+COALESCE(length(CAST(outcome_json AS BLOB)),1048576)),0) FROM attached_outbox) FROM attached_peer WHERE singleton=1",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
    let json = serde_json::to_string(command)?;
    let is_ack = matches!(command, Command::Acknowledge { .. });
    if next >= i64::MAX as u64
        || count >= if is_ack { 16_384 } else { 16_383 }
        || json.len() > MAX_FRAME_BYTES
        || bytes
            .saturating_add(json.len() as u64)
            .saturating_add(MAX_FRAME_BYTES as u64)
            > if is_ack {
                16 * 1024 * 1024
            } else {
                15 * 1024 * 1024
            }
    {
        return Err(invalid(
            "outbox capacity reached; retain obligations and compact acknowledged replies",
        ));
    }
    if let Command::ReportUncertain {
        key,
        offer_nonce,
        reason,
    } = command
    {
        let encoded = serde_json::to_string(key)?;
        let (json, seal): (String, Option<String>) = tx.query_row(
            "SELECT grant_json,seal_json FROM attached_grants WHERE allocation_key=?1",
            [&encoded],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let mut grant: GrantSnapshot = serde_json::from_str(&json)?;
        if seal.is_some()
            || grant.offer_nonce != *offer_nonce
            || !matches!(grant.state, GrantState::Armed | GrantState::Uncertain)
            || reason.is_empty()
            || reason.len() > 1024
            || reason.contains('\0')
        {
            return Err(invalid(
                "uncertainty requires an outstanding local Grant and bounded reason",
            ));
        }
        grant.state = GrantState::Uncertain;
        grant.uncertainty_reason = Some(reason.clone());
        tx.execute(
            "UPDATE attached_grants SET grant_json=?2 WHERE allocation_key=?1",
            params![encoded, serde_json::to_string(&grant)?],
        )?;
    }
    tx.execute(
        "INSERT INTO attached_outbox VALUES (?1,?2,?3,?4,NULL)",
        params![next, operation.to_string(), hash, json],
    )?;
    tx.execute(
        "UPDATE attached_peer SET next_sequence=next_sequence+1 WHERE singleton=1",
        [],
    )?;
    Ok(next)
}

/// Read only the earliest unapplied operation. Reconnect signs the same logical
/// operation with the new session; it never allocates another request sequence.
pub fn pending(connection: &Connection, secret: &PairingSecret) -> Result<Option<Request>> {
    let session: Option<String> = connection.query_row(
        "SELECT session_json FROM attached_peer WHERE singleton=1",
        [],
        |r| r.get(0),
    )?;
    let session: SessionIdentity =
        serde_json::from_str(&session.ok_or_else(|| invalid("peer is not authenticated"))?)?;
    let row:Option<(u64,String,String,String)> = connection.query_row("SELECT sequence,operation_id,command_json,payload_sha256 FROM attached_outbox WHERE outcome_json IS NULL ORDER BY sequence LIMIT 1",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
    row.map(|(sequence, id, json, hash)| {
        let mut request = Request::new(
            session,
            sequence,
            Uuid::parse_str(&id).map_err(|_| invalid("invalid operation UUID"))?,
            serde_json::from_str(&json)?,
        )?;
        if request.payload_sha256 != hash {
            return Err(invalid(
                "stored command changed across serialization versions; retain pending obligations",
            ));
        }
        secret.sign_request(&mut request)?;
        Ok(request)
    })
    .transpose()
}

/// Apply a response atomically with the manager lifecycle. Duplicates return
/// false and cannot re-apply lifecycle effects. The response remains replayable.
pub fn accept(tx: &Transaction<'_>, request: &Request, reply: &Reply) -> Result<bool> {
    request.validate()?;
    if reply.session != request.session
        || reply.operation_id != request.operation_id
        || reply.request_sequence != request.request_sequence
    {
        return Err(invalid("reply does not identify the outstanding request"));
    }
    let (session, applied): (Option<String>, u64) = tx.query_row(
        "SELECT session_json,applied_sequence FROM attached_peer WHERE singleton=1",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    if session.as_deref() != Some(serde_json::to_string(&request.session)?.as_str()) {
        return Err(invalid("reply belongs to a fenced connection"));
    }
    let row:Option<(String,String,Option<String>)> = tx.query_row("SELECT operation_id,payload_sha256,outcome_json FROM attached_outbox WHERE sequence=?1",[request.request_sequence],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
    let (id, hash, prior) = row.ok_or_else(|| invalid("reply has no durable outbox intent"))?;
    if id != request.operation_id.to_string() || hash != request.payload_sha256 {
        return Err(invalid("reply operation conflicts with the outbox"));
    }
    super::write_frame(&mut std::io::sink(), reply)?;
    let json = serde_json::to_string(&reply.outcome)?;
    if let Some(prior) = prior {
        return if prior == json {
            Ok(false)
        } else {
            Err(invalid("replayed response changed"))
        };
    }
    if applied.checked_add(1) != Some(request.request_sequence) {
        return Err(invalid("reply is out of order"));
    }
    match &reply.outcome {
        Outcome::Grant { grant } => {
            if !matches!(&request.command,Command::Arm { key,offer_nonce } if key == &grant.candidate.key && offer_nonce == &grant.offer_nonce)
                && !matches!(&request.command,Command::ReportUncertain { key,offer_nonce,reason } if key==&grant.candidate.key && offer_nonce==&grant.offer_nonce && grant.state==GrantState::Uncertain && grant.uncertainty_reason.as_ref()==Some(reason))
            {
                return Err(invalid("unsolicited Grant"));
            }
            if !request.session.owns(&grant.candidate.key) {
                return Err(invalid("Grant belongs to another manager"));
            }
            let key = serde_json::to_string(&grant.candidate.key)?;
            let previous: Option<String> = tx
                .query_row(
                    "SELECT grant_json FROM attached_grants WHERE allocation_key=?1",
                    [&key],
                    |r| r.get(0),
                )
                .optional()?;
            if let Some(previous) = previous {
                let previous: GrantSnapshot = serde_json::from_str(&previous)?;
                if previous.grant_id != grant.grant_id || previous.offer_nonce != grant.offer_nonce
                {
                    return Err(invalid("allocation acquired a different Grant"));
                }
            }
            tx.execute("INSERT INTO attached_grants VALUES (?1,?2,NULL) ON CONFLICT(allocation_key) DO UPDATE SET grant_json=excluded.grant_json",params![key,serde_json::to_string(grant)?])?;
        }
        Outcome::Ticket { ticket } => {
            let Command::AuthorizeInvocation {
                key,
                offer_nonce,
                intent,
            } = &request.command
            else {
                return Err(invalid("unsolicited invocation ticket"));
            };
            if &ticket.key != key || &ticket.offer_nonce != offer_nonce || &ticket.intent != intent
            {
                return Err(invalid("ticket differs from the durable Invocation intent"));
            }
            let key = serde_json::to_string(key)?;
            let (grant, seal): (String, Option<String>) = tx.query_row(
                "SELECT grant_json,seal_json FROM attached_grants WHERE allocation_key=?1",
                [&key],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            let mut grant: GrantSnapshot = serde_json::from_str(&grant)?;
            if seal.is_some()
                || grant.grant_id != ticket.grant_id
                || !matches!(grant.state, GrantState::Armed | GrantState::Uncertain)
            {
                return Err(invalid("ticket has no unsealed local Armed Grant"));
            }
            let encoded = serde_json::to_string(ticket)?;
            tx.execute("INSERT OR IGNORE INTO attached_tickets(invocation_id,allocation_key,ticket_json) VALUES (?1,?2,?3)",params![intent.invocation_id.to_string(),key,encoded])?;
            let old: String = tx.query_row(
                "SELECT ticket_json FROM attached_tickets WHERE invocation_id=?1",
                [intent.invocation_id.to_string()],
                |r| r.get(0),
            )?;
            if old != encoded {
                return Err(invalid("Invocation ticket was replaced"));
            }
            if !grant.tickets.contains(intent) {
                grant.tickets.push(intent.clone());
                tx.execute(
                    "UPDATE attached_grants SET grant_json=?2 WHERE allocation_key=?1",
                    params![key, serde_json::to_string(&grant)?],
                )?;
            }
        }
        Outcome::Released {
            grant_id,
            sealed_sequence,
        } => {
            let Command::Release { release } = &request.command else {
                return Err(invalid("unsolicited release"));
            };
            let key = serde_json::to_string(&release.key)?;
            let (grant, seal): (String, Option<String>) = tx.query_row(
                "SELECT grant_json,seal_json FROM attached_grants WHERE allocation_key=?1",
                [&key],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            let mut grant: GrantSnapshot = serde_json::from_str(&grant)?;
            if *sealed_sequence != release.sealed_sequence
                || grant.grant_id != *grant_id
                || seal.as_deref() != Some(serde_json::to_string(release)?.as_str())
            {
                return Err(invalid("release acknowledgement has no local seal"));
            }
            grant.state = GrantState::Released;
            grant.sealed_release = Some(release.clone());
            tx.execute(
                "UPDATE attached_grants SET grant_json=?2 WHERE allocation_key=?1",
                params![key, serde_json::to_string(&grant)?],
            )?;
        }
        Outcome::Acknowledged { through_sequence } => {
            if !matches!(request.command,Command::Acknowledge { through_sequence: n } if n == *through_sequence)
                || *through_sequence > applied
            {
                return Err(invalid("coordinator acknowledged unapplied responses"));
            }
            tx.execute(
                "DELETE FROM attached_outbox WHERE sequence<=?1",
                [through_sequence],
            )?;
            tx.execute(
                "UPDATE attached_peer SET retired_floor=MAX(retired_floor,?1) WHERE singleton=1",
                [through_sequence],
            )?;
        }
        Outcome::InventoryPage {
            grants,
            next,
            configuration_sha256,
        } => {
            recovery::accept_page(tx, request, grants, next, configuration_sha256)?;
        }
        Outcome::Reconciled { released, .. } => {
            recovery::committed(tx, request, released)?;
        }
        _ => {}
    }
    tx.execute(
        "UPDATE attached_outbox SET outcome_json=?2 WHERE sequence=?1",
        params![request.request_sequence, json],
    )?;
    tx.execute(
        "UPDATE attached_peer SET applied_sequence=?1 WHERE singleton=1",
        [request.request_sequence],
    )?;
    Ok(true)
}

/// Persist the no-replay start intent under the caller's cancel/release barrier.
/// The runtime must ALSO check ticket session/freshness and local readiness while
/// holding that barrier through actual OS release. A restart never repeats this.
pub fn consume_ticket(tx: &Transaction<'_>, ticket: &InvocationTicket) -> Result<bool> {
    if recovery::active(tx)? {
        return Err(invalid("start is fenced during inventory recovery"));
    }
    let session: Option<String> = tx.query_row(
        "SELECT session_json FROM attached_peer WHERE singleton=1",
        [],
        |r| r.get(0),
    )?;
    if session.as_deref() != Some(serde_json::to_string(&ticket.session)?.as_str()) {
        return Err(invalid("ticket session was fenced"));
    }
    let (encoded,consumed,seal,cleanup):(String,bool,Option<String>,Option<String>) = tx.query_row("SELECT t.ticket_json,t.consumed,g.seal_json,t.cleanup_json FROM attached_tickets t JOIN attached_grants g USING(allocation_key) WHERE invocation_id=?1",[ticket.intent.invocation_id.to_string()],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
    let state: String = tx.query_row(
        "SELECT json_extract(grant_json,'$.state') FROM attached_grants WHERE allocation_key=?1",
        [serde_json::to_string(&ticket.key)?],
        |r| r.get(0),
    )?;
    if state != "armed"
        || encoded != serde_json::to_string(ticket)?
        || seal.is_some()
        || cleanup.is_some()
    {
        return Err(invalid("ticket is sealed or changed"));
    }
    if consumed {
        return Ok(false);
    }
    tx.execute(
        "UPDATE attached_tickets SET consumed=1 WHERE invocation_id=?1",
        [ticket.intent.invocation_id.to_string()],
    )?;
    Ok(true)
}

/// Call only after the platform executor proves the entire boundary empty.
pub fn record_cleanup(tx: &Transaction<'_>, cleanup: &TicketCleanup) -> Result<()> {
    let known: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM attached_tickets WHERE invocation_id=?1)",
        [cleanup.invocation_id.to_string()],
        |r| r.get(0),
    )?;
    if !known {
        // A ticket reply absent from this continuous store was never consumed.
        // Import retained its intent as a cleanup obligation, not a permission.
        let mut grants = tx.prepare("SELECT grant_json FROM attached_grants")?;
        let intents = grants
            .query_map([], |r| r.get::<_, String>(0))?
            .map(|r| Ok(serde_json::from_str::<GrantSnapshot>(&r?)?))
            .collect::<Result<Vec<_>>>()?;
        let intent = intents
            .iter()
            .flat_map(|g| &g.tickets)
            .find(|i| i.invocation_id == cleanup.invocation_id)
            .ok_or_else(|| invalid("cleanup has no recovered ticket obligation"))?;
        if cleanup.user_code_released
            || cleanup.release_sequence != intent.release_sequence
            || cleanup.boundary_sha256 != intent.boundary_sha256
            || cleanup.proof_sha256.len() != 64
            || !cleanup.proof_sha256.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(invalid(
                "cleanup does not cover the recovered unused ticket",
            ));
        }
        let encoded = serde_json::to_string(cleanup)?;
        let old: Option<String> = tx
            .query_row(
                "SELECT cleanup_json FROM attached_recovered_cleanup WHERE invocation_id=?1",
                [cleanup.invocation_id.to_string()],
                |r| r.get(0),
            )
            .optional()?;
        if old.as_ref().is_some_and(|s| s != &encoded) {
            return Err(invalid("recovered cleanup proof changed"));
        }
        tx.execute(
            "INSERT OR IGNORE INTO attached_recovered_cleanup VALUES (?1,?2)",
            params![cleanup.invocation_id.to_string(), encoded],
        )?;
        return Ok(());
    }
    let (ticket, consumed, prior): (String, bool, Option<String>) = tx.query_row(
        "SELECT ticket_json,consumed,cleanup_json FROM attached_tickets WHERE invocation_id=?1",
        [cleanup.invocation_id.to_string()],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )?;
    let ticket: InvocationTicket = serde_json::from_str(&ticket)?;
    let encoded = serde_json::to_string(cleanup)?;
    if cleanup.release_sequence != ticket.intent.release_sequence
        || cleanup.boundary_sha256 != ticket.intent.boundary_sha256
        || cleanup.user_code_released != consumed
        || cleanup.proof_sha256.len() != 64
        || !cleanup.proof_sha256.bytes().all(|c| c.is_ascii_hexdigit())
        || prior.as_ref().is_some_and(|old| old != &encoded)
    {
        return Err(invalid(
            "cleanup does not cover the immutable local start intent",
        ));
    }
    tx.execute(
        "UPDATE attached_tickets SET cleanup_json=?2 WHERE invocation_id=?1",
        params![cleanup.invocation_id.to_string(), encoded],
    )?;
    Ok(())
}

/// Seal the complete ticket inventory and enqueue Release in ONE transaction.
/// An unanswered ticket request blocks sealing even if no local ticket exists.
pub fn seal_release(tx: &Transaction<'_>, key: &AllocationKey, operation: Uuid) -> Result<u64> {
    recovery::require_complete(tx)?;
    let key_json = serde_json::to_string(key)?;
    let mut requests =
        tx.prepare("SELECT command_json FROM attached_outbox WHERE outcome_json IS NULL")?;
    for row in requests.query_map([], |r| r.get::<_, String>(0))? {
        if matches!(serde_json::from_str::<Command>(&row?)?, Command::AuthorizeInvocation { key: pending, .. } if pending == *key)
        {
            return Err(invalid(
                "unanswered ticket request prevents release sealing",
            ));
        }
    }
    let (grant, seal): (String, Option<String>) = tx.query_row(
        "SELECT grant_json,seal_json FROM attached_grants WHERE allocation_key=?1",
        [&key_json],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    if let Some(seal) = seal {
        let release: SealedRelease = serde_json::from_str(&seal)?;
        if recovery::active(tx)? {
            return Ok(release.sealed_sequence);
        }
        return enqueue(
            tx,
            operation,
            &Command::Release {
                release: serde_json::from_str(&seal)?,
            },
        );
    }
    let grant: GrantSnapshot = serde_json::from_str(&grant)?;
    let mut tickets = tx.prepare(
        "SELECT cleanup_json FROM attached_tickets WHERE allocation_key=?1 ORDER BY invocation_id",
    )?;
    let mut cleanup = tickets
        .query_map([&key_json], |r| r.get::<_, Option<String>>(0))?
        .map(|r| {
            Ok(serde_json::from_str::<TicketCleanup>(&r?.ok_or_else(
                || invalid("ticket boundary is not proven empty"),
            )?)?)
        })
        .collect::<Result<Vec<_>>>()?;
    for intent in &grant.tickets {
        if cleanup
            .iter()
            .any(|c| c.invocation_id == intent.invocation_id)
        {
            continue;
        }
        let json: Option<String> = tx
            .query_row(
                "SELECT cleanup_json FROM attached_recovered_cleanup WHERE invocation_id=?1",
                [intent.invocation_id.to_string()],
                |r| r.get(0),
            )
            .optional()?;
        cleanup.push(serde_json::from_str(&json.ok_or_else(|| {
            invalid("recovered ticket boundary is not proven empty")
        })?)?);
    }
    let sequence: u64 = tx.query_row(
        "SELECT next_sequence FROM attached_peer WHERE singleton=1",
        [],
        |r| r.get(0),
    )?;
    let release = SealedRelease {
        key: key.clone(),
        offer_nonce: grant.offer_nonce,
        sealed_sequence: sequence,
        tickets: cleanup,
    };
    tx.execute(
        "UPDATE attached_grants SET seal_json=?2 WHERE allocation_key=?1",
        params![key_json, serde_json::to_string(&release)?],
    )?;
    if recovery::active(tx)? {
        // The complete reconciliation snapshot carries this seal; a separate
        // Release operation would disturb its sequence watermark.
        Ok(sequence)
    } else {
        enqueue(tx, operation, &Command::Release { release })
    }
}

#[cfg(test)]
mod tests;
