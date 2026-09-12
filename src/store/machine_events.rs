use super::*;
use crate::machine::{
    AllocationEvent, AllocationKey, AllocationOwner, Claims, EventCursor as MachineCursor,
    EventPage, GrantState,
};

pub(super) fn initialize_schema(connection: &Connection) -> StoreResult<()> {
    connection.execute_batch("CREATE TABLE IF NOT EXISTS machine_events(
        sequence INTEGER PRIMARY KEY AUTOINCREMENT, payload_json TEXT NOT NULL,
        committed_ms INTEGER NOT NULL);
        CREATE TRIGGER IF NOT EXISTS machine_events_retention AFTER INSERT ON machine_events BEGIN
            DELETE FROM machine_events WHERE sequence <= NEW.sequence - 4096;
            DELETE FROM machine_events WHERE sequence IN (
                SELECT sequence FROM (
                    SELECT sequence, SUM(length(CAST(payload_json AS BLOB))) OVER (ORDER BY sequence DESC) AS total_bytes
                    FROM machine_events) WHERE total_bytes > 16777216);
        END;")?;
    let now = "CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)";
    for (name, action, predicate) in [
        ("insert", "INSERT", "1"),
        ("update", "UPDATE OF state", "OLD.state != NEW.state"),
    ] {
        connection.execute_batch(&format!("CREATE TRIGGER IF NOT EXISTS machine_native_event_{name}
            AFTER {action} ON leases WHEN {predicate} BEGIN
                INSERT INTO machine_events(payload_json,committed_ms) VALUES (
                    json_object('kind','native','lease',NEW.id,'attempt',NEW.attempt_id,
                        'invocation',NEW.invocation_id,'job',(SELECT job_id FROM attempts WHERE id=NEW.attempt_id),
                        'state',NEW.state,'claims',json(NEW.claims_json)), {now});
            END;"))?;
    }
    for (name, action, predicate) in [
        ("insert", "INSERT", "1"),
        (
            "update",
            "UPDATE OF state,snapshot_json",
            "OLD.state != NEW.state OR json_array_length(OLD.snapshot_json,'$.tickets') != json_array_length(NEW.snapshot_json,'$.tickets')",
        ),
    ] {
        connection.execute_batch(&format!("CREATE TRIGGER IF NOT EXISTS machine_attached_event_{name}
            AFTER {action} ON machine_grants WHEN {predicate} BEGIN
                INSERT INTO machine_events(payload_json,committed_ms) VALUES (
                    json_object('kind','attached','grant_id',json_extract(NEW.snapshot_json,'$.grant_id'),
                        'key',json_extract(NEW.snapshot_json,'$.candidate.key'),
                        'owner',json_extract(NEW.snapshot_json,'$.candidate.owner'),
                        'state',NEW.state,'tickets_issued',json_array_length(NEW.snapshot_json,'$.tickets'),
                        'risk_clearance',json_extract(NEW.snapshot_json,'$.risk_clearance'),
                        'claims',json_extract(NEW.snapshot_json,'$.candidate.claims')), {now});
            END;"))?;
    }
    Ok(())
}

#[derive(serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Record {
    Native {
        lease: Uuid,
        attempt: Uuid,
        invocation: Option<Uuid>,
        job: Uuid,
        state: String,
        claims: ResolvedClaims,
    },
    Attached {
        grant_id: crate::GrantId,
        key: AllocationKey,
        owner: AllocationOwner,
        state: GrantState,
        tickets_issued: u32,
        #[serde(default)]
        risk_clearance: Option<Uuid>,
        claims: Claims,
    },
}

impl Store {
    pub(crate) fn machine_events(
        &self,
        cursor: Option<MachineCursor>,
        limit: u32,
    ) -> StoreResult<EventPage> {
        if limit == 0
            || limit > 256
            || cursor.is_some_and(|c| c.coordinator_store_uuid != self.store_uuid)
        {
            return Err(super::machine::rejected(
                "history_unknown",
                "machine event cursor store or page bounds are invalid",
            ));
        }
        let authority = self
            .authority
            .as_ref()
            .map(|_| self.authority_snapshot())
            .transpose()?;
        let identity = authority.and_then(|a| a.epoch.zip(a.domains));
        let tx = self.connection.unchecked_transaction()?;
        let (first, last): (Option<u64>, Option<u64>) = tx.query_row(
            "SELECT MIN(sequence),MAX(sequence) FROM machine_events",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let after = cursor.map_or(0, |c| c.sequence);
        if after > last.unwrap_or(0) {
            return Err(super::machine::rejected(
                "history_unknown",
                "machine event cursor is ahead of durable history",
            ));
        }
        let gap = first.is_some_and(|first| after.saturating_add(1) < first);
        let mut statement = tx.prepare("SELECT sequence,payload_json,committed_ms FROM machine_events WHERE sequence>?1 ORDER BY sequence LIMIT ?2")?;
        let mut events = Vec::new();
        let mut bytes = 0;
        let mut next = after;
        for row in statement.query_map(params![after, limit], |r| {
            Ok((
                r.get::<_, u64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)?,
            ))
        })? {
            let (sequence, payload, time) = row?;
            let cursor = MachineCursor {
                coordinator_store_uuid: self.store_uuid,
                sequence,
            };
            let event = match serde_json::from_str::<Record>(&payload)? {
                Record::Native {
                    lease,
                    attempt,
                    invocation,
                    job,
                    state,
                    claims,
                } => {
                    let owner = match invocation {
                        Some(invocation) => AllocationOwner::Probe {
                            job_id: JobId::from_parts(self.store_uuid, job),
                            invocation_id: InvocationId::from_parts(self.store_uuid, invocation),
                        },
                        None => AllocationOwner::Work {
                            job_id: JobId::from_parts(self.store_uuid, job),
                            attempt_id: AttemptId::from_parts(self.store_uuid, attempt),
                        },
                    };
                    AllocationEvent {
                        cursor,
                        native: true,
                        grant_id: crate::GrantId::from_parts(self.store_uuid, lease),
                        key: identity.as_ref().map(|(epoch, domains)| AllocationKey {
                            machine_id: domains.machine_id,
                            authority_epoch: *epoch,
                            domain_id: domains.native_domain,
                            manager_store_uuid: self.store_uuid,
                            lease_id: lease,
                        }),
                        owner,
                        state: match state.as_str() {
                            "granted" => GrantState::Armed,
                            "released" => GrantState::Released,
                            _ => {
                                return Err(StoreError::InvalidState(
                                    "invalid native event state".into(),
                                ));
                            }
                        },
                        tickets_issued: 0,
                        risk_clearance: None,
                        claims: Claims {
                            scalars: crate::admission::scalar_claim_entries(&claims),
                            shared_fences: claims.shared_fences.into_iter().collect(),
                            exclusive_fences: claims.exclusive_fences.into_iter().collect(),
                            impacts: claims.impacts.into_iter().collect(),
                        },
                        committed_unix_millis: time,
                    }
                }
                Record::Attached {
                    grant_id,
                    key,
                    owner,
                    state,
                    tickets_issued,
                    risk_clearance,
                    claims,
                } => AllocationEvent {
                    cursor,
                    native: false,
                    grant_id,
                    key: Some(key),
                    owner,
                    state,
                    tickets_issued,
                    risk_clearance,
                    claims,
                    committed_unix_millis: time,
                },
            };
            let size = serde_json::to_vec(&event)?.len();
            if bytes + size > crate::machine::MAX_FRAME_BYTES - 4096 {
                if events.is_empty() {
                    return Err(super::machine::rejected(
                        "limit_exceeded",
                        "single machine event exceeds transport budget",
                    ));
                }
                break;
            }
            bytes += size;
            next = sequence;
            events.push(event);
        }
        Ok(EventPage {
            events,
            cursor: MachineCursor {
                coordinator_store_uuid: self.store_uuid,
                sequence: next,
            },
            oldest_available: MachineCursor {
                coordinator_store_uuid: self.store_uuid,
                sequence: first.unwrap_or(0),
            },
            gap,
            more: last.is_some_and(|last| next < last),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_retention_bounds_rows_and_bytes_without_touching_grant_history() {
        let mut connection = Connection::open_in_memory().unwrap();
        connection.execute_batch("CREATE TABLE leases(id TEXT,attempt_id TEXT,invocation_id TEXT,state TEXT,claims_json TEXT);
            CREATE TABLE attempts(id TEXT,job_id TEXT);
            CREATE TABLE machine_grants(allocation_key TEXT,state TEXT,snapshot_json TEXT);").unwrap();
        initialize_schema(&connection).unwrap();
        let tx = connection.transaction().unwrap();
        for _ in 0..4098 {
            tx.execute(
                "INSERT INTO machine_events(payload_json,committed_ms) VALUES ('{}',1)",
                [],
            )
            .unwrap();
        }
        let (first, count): (u64, u64) = tx
            .query_row(
                "SELECT MIN(sequence),COUNT(*) FROM machine_events",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!((first, count), (3, 4096));
        let lease = Uuid::now_v7().to_string();
        let attempt = Uuid::now_v7().to_string();
        tx.execute(
            "INSERT INTO attempts VALUES (?1,?2)",
            params![attempt, Uuid::now_v7().to_string()],
        )
        .unwrap();
        tx.execute(
            "INSERT INTO leases VALUES (?1,?2,NULL,'granted','{}')",
            params![lease, attempt],
        )
        .unwrap();
        let large = "x".repeat(1024 * 1024);
        for _ in 0..17 {
            tx.execute(
                "INSERT INTO machine_events(payload_json,committed_ms) VALUES (?1,2)",
                [&large],
            )
            .unwrap();
        }
        let (count, bytes): (u64, u64) = tx
            .query_row(
                "SELECT COUNT(*),SUM(length(CAST(payload_json AS BLOB))) FROM machine_events",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(count, 16);
        assert_eq!(bytes, 16 * 1024 * 1024);
        assert_eq!(
            tx.query_row("SELECT state FROM leases WHERE id=?1", [lease], |r| r
                .get::<_, String>(0))
                .unwrap(),
            "granted"
        );
        tx.commit().unwrap();
    }
}
