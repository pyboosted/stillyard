//! Recovery of a continuous manager store after the coordinator retires replies.
//! Inventory is imported as cleanup obligations, never reconstructed tickets.
use super::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Recovery {
    after: Option<AllocationKey>,
    complete: bool,
    configuration: Option<String>,
}

pub(super) fn active(c: &Connection) -> Result<bool> {
    Ok(c.query_row(
        "SELECT EXISTS(SELECT 1 FROM attached_meta WHERE key='recovery')",
        [],
        |r| r.get(0),
    )?)
}

/// Enter a fenced recovery session using the authenticated handshake. The local
/// store must remain continuous. Superseded operations are retained for the
/// caller's lifecycle reconciliation; advancing a watermark does not apply them.
pub fn begin(
    tx: &Transaction<'_>,
    session: &SessionIdentity,
    peer: &ParticipantSnapshot,
) -> Result<()> {
    let (store, prior, next, applied, floor):(String,Option<String>,u64,u64,u64) = tx.query_row(
        "SELECT store_uuid,session_json,next_sequence,applied_sequence,retired_floor FROM attached_peer WHERE singleton=1",[],
        |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)))?;
    let prior: SessionIdentity = serde_json::from_str(
        &prior.ok_or_else(|| invalid("recovery requires continuous paired manager history"))?,
    )?;
    if store != session.manager_store_uuid.to_string()
        || prior.machine_id != session.machine_id
        || prior.domain_id != session.domain_id
        || prior.manager_store_uuid != session.manager_store_uuid
        || prior.connection_epoch > session.connection_epoch
        || (prior.connection_epoch == session.connection_epoch && prior != *session)
        || peer.manager_store_uuid != session.manager_store_uuid
        || peer.installation.domain_id != session.domain_id
        || peer.connection_epoch != session.connection_epoch
        || peer.executor_incarnation != Some(session.executor_incarnation)
        || !peer.reconciliation_required
        || peer.accepted_sequence < applied
        || peer.accepted_sequence >= next
        || peer.retired_sequence_floor < floor
        || peer.retired_sequence_floor > peer.accepted_sequence
    {
        return Err(invalid(
            "recovery handshake does not cover continuous manager history",
        ));
    }
    // Every existing pending command remains identifiable. Never resend a lost
    // AuthorizeInvocation as new work after the coordinator has retired its ack.
    let count:u64 = tx.query_row("SELECT (SELECT COUNT(*) FROM attached_abandoned)+(SELECT COUNT(*) FROM attached_outbox WHERE outcome_json IS NULL)",[],|r|r.get(0))?;
    let bytes:u64=tx.query_row("SELECT (SELECT COALESCE(SUM(length(CAST(command_json AS BLOB))),0) FROM attached_abandoned)+(SELECT COALESCE(SUM(length(CAST(command_json AS BLOB))),0) FROM attached_outbox WHERE outcome_json IS NULL)",[],|r|r.get(0))?;
    if count > 16384 || bytes > MAX_RECONCILE_BYTES as u64 {
        return Err(invalid(
            "unresolved lifecycle intents exceed recovery budget",
        ));
    }
    tx.execute("INSERT OR IGNORE INTO attached_abandoned SELECT operation_id,sequence,payload_sha256,command_json FROM attached_outbox WHERE outcome_json IS NULL",[])?;
    tx.execute("DELETE FROM attached_outbox WHERE outcome_json IS NULL", [])?;
    tx.execute("UPDATE attached_peer SET session_json=?1,next_sequence=?2,applied_sequence=?3,retired_floor=?4 WHERE singleton=1",
        params![serde_json::to_string(session)?,peer.accepted_sequence+1,peer.accepted_sequence,peer.retired_sequence_floor])?;
    tx.execute("DELETE FROM attached_inventory", [])?;
    save(
        tx,
        &Recovery {
            after: None,
            complete: false,
            configuration: None,
        },
    )
}

fn save(tx: &Transaction<'_>, state: &Recovery) -> Result<()> {
    tx.execute("INSERT INTO attached_meta VALUES ('recovery',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value",[serde_json::to_string(state)?])?;
    Ok(())
}
fn state(c: &Connection) -> Result<Recovery> {
    let json: String = c.query_row(
        "SELECT value FROM attached_meta WHERE key='recovery'",
        [],
        |r| r.get(0),
    )?;
    Ok(serde_json::from_str(&json)?)
}

pub(super) fn require_complete(c: &Connection) -> Result<()> {
    if active(c)? && !state(c)?.complete {
        return Err(invalid("cannot seal before complete recovery inventory"));
    }
    Ok(())
}

/// The next page request is durable through the ordinary outbox. Repeated calls
/// use the caller's same operation UUID until its response is committed.
pub fn next_page(c: &Connection) -> Result<Option<Command>> {
    let s = state(c)?;
    Ok((!s.complete).then_some(Command::InspectPage {
        after: s.after,
        limit: 256,
    }))
}

pub(super) fn allow(c: &Connection, command: &Command) -> Result<()> {
    if !active(c)? {
        return Ok(());
    }
    let s = state(c)?;
    let allowed = match command {
        Command::InspectPage { after, .. } => !s.complete && *after == s.after,
        Command::ReconcileBegin { .. }
        | Command::ReconcilePage { .. }
        | Command::ReconcileCommit { .. } => s.complete,
        Command::Acknowledge { .. } => true,
        _ => false,
    };
    if allowed {
        Ok(())
    } else {
        Err(invalid(
            "new work is fenced until full inventory reconciliation completes",
        ))
    }
}

pub(super) fn accept_page(
    tx: &Transaction<'_>,
    request: &Request,
    grants: &[GrantSnapshot],
    next: &Option<AllocationKey>,
    configuration: &str,
) -> Result<()> {
    if !active(tx)? {
        return Ok(());
    }
    let mut s = state(tx)?;
    let Command::InspectPage { after, limit } = &request.command else {
        return Err(invalid("unsolicited recovery page"));
    };
    if s.complete
        || *after != s.after
        || grants.len() > *limit as usize
        || configuration.len() != 64
        || !configuration.bytes().all(|c| c.is_ascii_hexdigit())
        || s.configuration
            .as_ref()
            .is_some_and(|old| old != configuration)
    {
        return Err(invalid("recovery page cursor or configuration changed"));
    }
    let mut previous = after.as_ref().map(serde_json::to_string).transpose()?;
    for grant in grants {
        let key = &grant.candidate.key;
        let encoded = serde_json::to_string(key)?;
        if !request.session.owns(key)
            || previous.as_ref().is_some_and(|p| p >= &encoded)
            || !matches!(
                grant.state,
                GrantState::Offered | GrantState::Armed | GrantState::Uncertain
            )
        {
            return Err(invalid(
                "inventory is unordered or belongs to another authority",
            ));
        }
        previous = Some(encoded.clone());
        tx.execute(
            "INSERT INTO attached_inventory VALUES (?1,?2)",
            params![encoded, serde_json::to_string(grant)?],
        )?;
    }
    if next.is_some()
        && (grants.is_empty() || grants.last().map(|g| &g.candidate.key) != next.as_ref())
    {
        return Err(invalid(
            "inventory cursor does not identify the last record",
        ));
    }
    let (count, bytes): (u64, u64) = tx.query_row(
        "SELECT COUNT(*),COALESCE(SUM(length(CAST(grant_json AS BLOB))),0) FROM attached_inventory",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    if count > MAX_DOMAIN_CANDIDATES as u64 || bytes > MAX_RECONCILE_BYTES as u64 {
        return Err(invalid("recovery inventory exceeds durable bounds"));
    }
    s.after = next.clone();
    s.complete = next.is_none();
    s.configuration = Some(configuration.into());
    if s.complete {
        import_inventory(tx)?;
    }
    save(tx, &s)
}

fn grants(c: &Connection, table: &str) -> Result<Vec<GrantSnapshot>> {
    let sql = match table {
        "local" => "SELECT grant_json FROM attached_grants ORDER BY allocation_key",
        _ => "SELECT grant_json FROM attached_inventory ORDER BY allocation_key",
    };
    let mut stmt = c.prepare(sql)?;
    stmt.query_map([], |r| r.get::<_, String>(0))?
        .map(|r| Ok(serde_json::from_str(&r?)?))
        .collect()
}

fn import_inventory(tx: &Transaction<'_>) -> Result<()> {
    let remote = grants(tx, "remote")?;
    let local = grants(tx, "local")?;
    let mut intents=tx.prepare("SELECT command_json FROM attached_abandoned UNION ALL SELECT command_json FROM attached_outbox")?;
    let commands = intents
        .query_map([], |r| r.get::<_, String>(0))?
        .map(|r| Ok(serde_json::from_str::<Command>(&r?)?))
        .collect::<Result<Vec<_>>>()?;
    for grant in &remote {
        let key = &grant.candidate.key;
        let old = local.iter().find(|g| g.candidate.key == *key);
        if let Some(old)=old {
            if old.grant_id!=grant.grant_id || old.offer_nonce!=grant.offer_nonce
                || old.state==GrantState::Released
                || (matches!(old.state,GrantState::Armed|GrantState::Uncertain) && grant.state==GrantState::Offered)
                || old.tickets.iter().any(|t|!grant.tickets.contains(t))
            { return Err(invalid("coordinator inventory conflicts with local start history")); }
        } else if grant.state!=GrantState::Offered && !commands.iter().any(|c|matches!(c,Command::Arm { key:k,offer_nonce } if k==key && *offer_nonce==grant.offer_nonce)) {
            return Err(invalid("unknown Armed grant has no durable local Arm intent"));
        }
        for intent in &grant.tickets {
            let known: Option<String> = tx
                .query_row(
                    "SELECT ticket_json FROM attached_tickets WHERE invocation_id=?1",
                    [intent.invocation_id.to_string()],
                    |r| r.get(0),
                )
                .optional()?;
            if let Some(known)=known {
                let ticket:InvocationTicket=serde_json::from_str(&known)?;
                if ticket.key!=*key || ticket.intent!=*intent || ticket.grant_id!=grant.grant_id { return Err(invalid("issued ticket conflicts with local Invocation history")); }
            } else if !commands.iter().any(|c|matches!(c,Command::AuthorizeInvocation { key:k,offer_nonce,intent:i } if k==key && *offer_nonce==grant.offer_nonce && i==intent)) {
                return Err(invalid("missing local ticket has no continuous durable request intent"));
            }
        }
        let mut imported = grant.clone();
        if let Some(old) = old.filter(|g| g.state == GrantState::Uncertain) {
            imported.state = GrantState::Uncertain;
            imported.uncertainty_reason = old.uncertainty_reason.clone();
        }
        tx.execute("INSERT INTO attached_grants VALUES (?1,?2,NULL) ON CONFLICT(allocation_key) DO UPDATE SET grant_json=excluded.grant_json",params![serde_json::to_string(key)?,serde_json::to_string(&imported)?])?;
    }
    // Absence from the completed authenticated inventory can acknowledge an
    // already locally sealed release. It cannot erase an unsealed obligation.
    for mut old in local {
        if old.state == GrantState::Released
            || remote.iter().any(|g| g.candidate.key == old.candidate.key)
        {
            continue;
        }
        let key = serde_json::to_string(&old.candidate.key)?;
        let seal: Option<String> = tx.query_row(
            "SELECT seal_json FROM attached_grants WHERE allocation_key=?1",
            [&key],
            |r| r.get(0),
        )?;
        let seal =
            seal.ok_or_else(|| invalid("coordinator omitted an unsealed local allocation"))?;
        old.state = GrantState::Released;
        old.sealed_release = Some(serde_json::from_str(&seal)?);
        tx.execute(
            "UPDATE attached_grants SET grant_json=?2 WHERE allocation_key=?1",
            params![key, serde_json::to_string(&old)?],
        )?;
    }
    Ok(())
}

/// Build the complete snapshot after inventory import. Enqueue one command at a
/// time and commit its acknowledgement before sending the next one.
pub fn reconciliation(c: &Connection) -> Result<Vec<Command>> {
    let s = state(c)?;
    if !s.complete {
        return Err(invalid("inventory is incomplete"));
    }
    let mut pages: Vec<Vec<ReconcileAllocation>> = Vec::new();
    let mut page = Vec::new();
    for grant in grants(c, "local")? {
        if grant.state == GrantState::Released {
            continue;
        }
        let seal: Option<String> = c.query_row(
            "SELECT seal_json FROM attached_grants WHERE allocation_key=?1",
            [serde_json::to_string(&grant.candidate.key)?],
            |r| r.get(0),
        )?;
        let record = ReconcileAllocation {
            key: grant.candidate.key,
            offer_nonce: grant.offer_nonce,
            tickets: grant.tickets,
            sealed_release: seal.map(|s| serde_json::from_str(&s)).transpose()?,
        };
        page.push(record);
        if page.len() > MAX_RECONCILE_PAGE
            || serde_json::to_vec(&page)?.len() > MAX_FRAME_BYTES - 4096
        {
            let last = page.pop().unwrap();
            if page.is_empty() {
                return Err(invalid(
                    "single reconciliation allocation exceeds frame budget",
                ));
            }
            pages.push(std::mem::take(&mut page));
            page.push(last);
        }
    }
    if !page.is_empty() {
        pages.push(page);
    }
    if serde_json::to_vec(&pages)?.len() > MAX_RECONCILE_BYTES {
        return Err(invalid("reconciliation exceeds byte budget"));
    }
    let end: u64 = c.query_row(
        "SELECT applied_sequence FROM attached_peer WHERE singleton=1",
        [],
        |r| r.get(0),
    )?;
    let pending: bool = c.query_row(
        "SELECT EXISTS(SELECT 1 FROM attached_outbox WHERE outcome_json IS NULL)",
        [],
        |r| r.get(0),
    )?;
    if pending {
        return Err(invalid(
            "drain outstanding replies before taking a reconciliation watermark",
        ));
    }
    let snapshot_id = Uuid::now_v7();
    let mut commands = vec![Command::ReconcileBegin {
        snapshot_id,
        begin_sequence: 0,
        end_sequence: end,
        page_count: pages.len() as u32,
        digest: payload_hash(&pages)?,
        configuration_sha256: s.configuration.unwrap(),
    }];
    commands.extend(pages.into_iter().enumerate().map(|(index, allocations)| {
        Command::ReconcilePage {
            snapshot_id,
            index: index as u32,
            allocations,
        }
    }));
    commands.push(Command::ReconcileCommit { snapshot_id });
    Ok(commands)
}

pub(super) fn committed(
    tx: &Transaction<'_>,
    request: &Request,
    released: &[AllocationKey],
) -> Result<()> {
    if !active(tx)? {
        return Ok(());
    }
    if !state(tx)?.complete || !matches!(request.command, Command::ReconcileCommit { .. }) {
        return Err(invalid("unsolicited recovery completion"));
    }
    for key in released {
        let encoded = serde_json::to_string(key)?;
        let (json, seal): (String, Option<String>) = tx.query_row(
            "SELECT grant_json,seal_json FROM attached_grants WHERE allocation_key=?1",
            [&encoded],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let mut grant: GrantSnapshot = serde_json::from_str(&json)?;
        grant.sealed_release =
            Some(serde_json::from_str(&seal.ok_or_else(|| {
                invalid("reconciled release has no local seal")
            })?)?);
        grant.state = GrantState::Released;
        tx.execute(
            "UPDATE attached_grants SET grant_json=?2 WHERE allocation_key=?1",
            params![encoded, serde_json::to_string(&grant)?],
        )?;
    }
    tx.execute("DELETE FROM attached_inventory", [])?;
    tx.execute("DELETE FROM attached_meta WHERE key='recovery'", [])?;
    Ok(())
}

/// Return abandoned requests for explicit reconciliation with local Job states.
/// These records are not launch permissions and must not be resent blindly.
pub fn abandoned(c: &Connection) -> Result<Vec<(Uuid, Command)>> {
    let mut stmt =
        c.prepare("SELECT operation_id,command_json FROM attached_abandoned ORDER BY sequence")?;
    stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
        .map(|r| {
            let (id, json) = r?;
            Ok((
                Uuid::parse_str(&id).map_err(|_| invalid("invalid archived operation UUID"))?,
                serde_json::from_str(&json)?,
            ))
        })
        .collect()
}

/// Acknowledge superseded intents after the caller has settled their local
/// lifecycle in this same transaction. A drained, fully reconciled manager is
/// required: archives may still be the only proof of an issued missing Ticket.
/// This does not delete Grant, Ticket, cleanup or caller-owned Job history.
pub fn acknowledge_abandoned(tx: &Transaction<'_>, operations: &[Uuid]) -> Result<usize> {
    if operations.is_empty() || operations.len() > 256 || operations.iter().any(Uuid::is_nil) {
        return Err(invalid(
            "acknowledge 1..256 explicit abandoned operation identities",
        ));
    }
    if active(tx)?
        || grants(tx, "local")?
            .iter()
            .any(|g| g.state != GrantState::Released)
    {
        return Err(invalid(
            "abandoned intents remain necessary until all allocations reconcile released",
        ));
    }
    let mut changed = 0;
    for operation in operations {
        changed += tx.execute(
            "DELETE FROM attached_abandoned WHERE operation_id=?1",
            [operation.to_string()],
        )?;
    }
    Ok(changed)
}
