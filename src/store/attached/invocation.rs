//! The platform's prepared-root identity, durable Ticket intent and lifecycle
//! authorization share the Store transaction boundary. No function releases OS code.
use super::*;
#[cfg(target_os = "linux")]
use crate::machine::{InvocationIntent, TicketCleanup};
use crate::machine::{InvocationTicket, manager};

pub(super) fn initialize(c: &Connection) -> StoreResult<()> {
    c.execute_batch(
        "CREATE TABLE IF NOT EXISTS attached_local_invocations(
        invocation_id TEXT PRIMARY KEY, operation_id TEXT UNIQUE NOT NULL,
        allocation_key TEXT NOT NULL, intent_json TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS attached_local_ticket_outcomes(
        invocation_id TEXT PRIMARY KEY, operation_id TEXT NOT NULL, outcome_json TEXT NOT NULL);",
    )?;
    let paired: bool = c.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='attached_outbox')",
        [],
        |r| r.get(0),
    )?;
    if paired {
        c.execute_batch(
            "CREATE TRIGGER IF NOT EXISTS attached_ticket_wait_outcome
        AFTER UPDATE OF outcome_json ON attached_outbox
        WHEN json_extract(NEW.outcome_json,'$.result')='rejected'
        BEGIN
          INSERT INTO attached_local_ticket_outcomes
          SELECT invocation_id,operation_id,NEW.outcome_json FROM attached_local_invocations
          WHERE operation_id=NEW.operation_id
          ON CONFLICT(invocation_id) DO UPDATE SET operation_id=excluded.operation_id,
            outcome_json=excluded.outcome_json;
        END;",
        )?;
    }
    Ok(())
}

/// Called immediately before the SAME transaction commits the started state.
/// Native callers pass None; an attached store can never use that native path.
pub(in crate::store) fn consume(
    tx: &Transaction<'_>,
    job: &PreparedJob,
    generation: Uuid,
    ticket: Option<&InvocationTicket>,
) -> StoreResult<()> {
    let connected: Option<bool> = tx
        .query_row(
            "SELECT connected FROM attached_local_mode WHERE singleton=1",
            [],
            |r| r.get(0),
        )
        .optional()?;
    if connected.is_none() && ticket.is_none() {
        return Ok(());
    }
    let ticket = ticket.ok_or_else(|| {
        StoreError::InvalidState(
            "attached Invocation requires its one-use coordinator Ticket".into(),
        )
    })?;
    if connected != Some(true)
        || ticket.intent.invocation_id != job.invocation_id
        || ticket.intent.containment_id != job.containment_id
        || ticket.intent.role != job.role
        || ticket.session.executor_incarnation != generation
    {
        return Err(StoreError::InvalidState(
            "Ticket belongs to a disconnected or different Invocation generation".into(),
        ));
    }
    let matches:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM attached_local_plans p
        JOIN leases l ON l.id=p.lease_id JOIN invocations i ON i.attempt_id=p.attempt_id
        JOIN invocation_process_identities r ON r.invocation_id=i.id
        WHERE p.allocation_key=?1 AND p.committed=1 AND p.armed=1 AND p.released=0 AND p.release_pending=0
          AND l.state='granted' AND i.id=?2 AND i.executable_hash=?4
          AND (CASE WHEN i.role='postcondition' THEN i.postcondition_index ELSE 0 END)=?3
          AND i.state='started' AND json_extract(r.identity_json,'$.platform')='linux'
          AND json_extract(r.identity_json,'$.pid')=i.root_pid
          AND ((i.role='probe' AND l.invocation_id=i.id)
            OR (i.role IN ('primary','postcondition') AND l.invocation_id IS NULL)))",
        params![serde_json::to_string(&ticket.key)?,job.invocation_id.entity_uuid().to_string(),ticket.intent.role_index,ticket.intent.executable_sha256],|r|r.get(0))?;
    if !matches || !manager::consume_ticket(tx, ticket).map_err(protocol_error)? {
        return Err(StoreError::InvalidState(
            "Ticket has no matching start transaction or was already consumed".into(),
        ));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
impl Store {
    pub(crate) fn attached_observation_ready(
        &self,
        job: &PreparedJob,
        lease: Uuid,
        sample: Option<&crate::host_observation::HostSample>,
    ) -> StoreResult<bool> {
        if !job.spec.requires_host_observation() {
            return Ok(true);
        }
        let Some(sample) = sample else {
            return Ok(false);
        };
        let mut statement = self
            .connection
            .prepare("SELECT claims_json FROM leases WHERE state='granted' AND id!=?1")?;
        let active = statement
            .query_map([lease.to_string()], |r| r.get::<_, String>(0))?
            .map(|row| Ok(serde_json::from_str::<ResolvedClaims>(&row?)?))
            .collect::<StoreResult<Vec<_>>>()?;
        let (wall, monotonic) = crate::host_observation::observation_clock()?;
        Ok(crate::host_observation::evaluate_admission(
            &job.spec,
            &self.host_config(),
            sample,
            &active,
            wall,
            monotonic,
        )
        .blockers
        .is_empty())
    }
    pub(crate) fn attached_launch_identity(
        &self,
        job: &PreparedJob,
    ) -> StoreResult<(Uuid, PathBuf, Uuid)> {
        self.check_authority_release()?;
        let config = super::installation::load(&self.paths.root)?.ok_or_else(|| {
            StoreError::InvalidState("installed executor configuration missing".into())
        })?;
        let lease:String=self.connection.query_row("SELECT p.lease_id FROM attached_local_plans p
            JOIN leases l ON l.id=p.lease_id WHERE p.job_id=?1 AND p.attempt_id=?2
            AND p.armed=1 AND p.committed=1 AND p.released=0 AND p.release_pending=0 AND l.state='granted'
            AND ((?3='probe' AND l.invocation_id=?4) OR (?3!='probe' AND l.invocation_id IS NULL))",
            params![job.job_id.entity_uuid().to_string(),job.attempt_id.entity_uuid().to_string(),role_text(job.role),job.invocation_id.entity_uuid().to_string()],|r|r.get(0))?;
        Ok((
            Uuid::parse_str(&lease)?,
            config.executor_cgroup,
            self.daemon_generation,
        ))
    }
    pub(crate) fn request_attached_ticket(
        &mut self,
        job: &PreparedJob,
        root: &ProcessIdentity,
        executable_sha256: &str,
        boundary_sha256: &str,
    ) -> StoreResult<Uuid> {
        self.check_authority_release()?;
        // Pairing may have been explicitly installed after this Store opened.
        initialize(&self.connection)?;
        let ProcessIdentity::Linux { pid, .. } = root else {
            return Err(StoreError::InvalidState(
                "attached executor requires a pinned Linux root".into(),
            ));
        };
        let store_uuid = self.store_uuid;
        let generation = self.daemon_generation;
        let tx = self.connection.transaction()?;
        let key_json:String=tx.query_row("SELECT p.allocation_key FROM attached_local_plans p JOIN leases l ON l.id=p.lease_id
            WHERE p.job_id=?1 AND p.attempt_id=?2 AND p.committed=1 AND p.armed=1 AND p.released=0 AND p.release_pending=0
              AND l.state='granted' AND ((?3='probe' AND l.invocation_id=?4) OR (?3!='probe' AND l.invocation_id IS NULL))",
            params![job.job_id.entity_uuid().to_string(),job.attempt_id.entity_uuid().to_string(),role_text(job.role),job.invocation_id.entity_uuid().to_string()],|r|r.get(0))?;
        let key: AllocationKey = serde_json::from_str(&key_json)?;
        let prior:Option<(String,String)> = tx.query_row("SELECT operation_id,intent_json FROM attached_local_invocations WHERE invocation_id=?1",[job.invocation_id.to_string()],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
        if let Some((operation, json)) = prior {
            let recorded: String = tx.query_row(
                "SELECT identity_json FROM invocation_process_identities WHERE invocation_id=?1",
                [job.invocation_id.entity_uuid().to_string()],
                |r| r.get(0),
            )?;
            if serde_json::from_str::<ProcessIdentity>(&recorded)? != *root {
                return Err(StoreError::InvalidState(
                    "prepared root identity changed".into(),
                ));
            }
            let prior: InvocationIntent = serde_json::from_str(&json)?;
            if prior.containment_id != job.containment_id
                || prior.executable_sha256 != executable_sha256
                || prior.boundary_sha256 != boundary_sha256
            {
                return Err(StoreError::InvalidState(
                    "prepared Invocation intent changed".into(),
                ));
            }
            return Ok(Uuid::parse_str(&operation)?);
        }
        let (json, seal): (String, Option<String>) = tx.query_row(
            "SELECT grant_json,seal_json FROM attached_grants WHERE allocation_key=?1",
            [&key_json],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let grant: crate::machine::GrantSnapshot = serde_json::from_str(&json)?;
        if seal.is_some()
            || grant.state != GrantState::Armed
            || key.manager_store_uuid != store_uuid
        {
            return Err(StoreError::InvalidState(
                "prepared Invocation has no unsealed Armed Grant".into(),
            ));
        }
        let previous_cleanup = grant
            .tickets
            .last()
            .map(|intent| -> StoreResult<TicketCleanup> {
                let json: Option<String> = tx
                    .query_row(
                        "SELECT cleanup_json FROM attached_tickets WHERE invocation_id=?1",
                        [intent.invocation_id.to_string()],
                        |r| r.get(0),
                    )
                    .optional()?
                    .flatten();
                serde_json::from_str(&json.ok_or_else(|| {
                    StoreError::InvalidState("previous Invocation has no committed cleanup".into())
                })?)
                .map_err(Into::into)
            })
            .transpose()?;
        // SQL role_index orders every Invocation in an Attempt (including
        // probes and host deferrals). The wire index identifies a role within
        // its allocation: primary/probe are zero; postconditions use their
        // specification index, preserved across pre-release deferrals.
        let (role, index, state): (String, Option<u32>, String) = tx.query_row(
            "SELECT role,postcondition_index,state FROM invocations WHERE id=?1 AND attempt_id=?2",
            params![
                job.invocation_id.entity_uuid().to_string(),
                job.attempt_id.entity_uuid().to_string()
            ],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        if role != role_text(job.role) || state != "prepared" {
            return Err(StoreError::InvalidState(
                "Invocation root is not awaiting its first release".into(),
            ));
        }
        let index = match job.role {
            InvocationRole::Postcondition => index.ok_or_else(|| {
                StoreError::InvalidState("postcondition has no specification index".into())
            })?,
            InvocationRole::Primary | InvocationRole::Probe => 0,
        };
        let intent = InvocationIntent {
            invocation_id: job.invocation_id,
            containment_id: job.containment_id,
            role: job.role,
            role_index: index,
            release_sequence: grant
                .tickets
                .last()
                .map_or(1, |i| i.release_sequence.saturating_add(1)),
            executable_sha256: executable_sha256.into(),
            boundary_sha256: boundary_sha256.into(),
            readiness_challenge: Uuid::now_v7(),
            previous_cleanup,
        };
        let (host, boot, creation) = process_records::legacy_columns(root, *pid)?;
        process_records::record(&tx, job.invocation_id, root, *pid)?;
        tx.execute("UPDATE invocations SET root_pid=?2,executable_hash=?3,daemon_generation=?4,root_host_id=?5,root_boot_id=?6,root_creation_filetime_100ns=?7 WHERE id=?1 AND state='prepared'",
            params![job.invocation_id.entity_uuid().to_string(),pid,executable_sha256,generation.to_string(),host,boot,creation])?;
        tx.execute(
            "UPDATE containments SET state='live' WHERE id=?1 AND state='creating'",
            [job.containment_id.entity_uuid().to_string()],
        )?;
        let operation = Uuid::now_v7();
        tx.execute(
            "INSERT INTO attached_local_invocations VALUES (?1,?2,?3,?4)",
            params![
                job.invocation_id.to_string(),
                operation.to_string(),
                key_json,
                serde_json::to_string(&intent)?
            ],
        )?;
        manager::enqueue(
            &tx,
            operation,
            &Command::AuthorizeInvocation {
                key,
                offer_nonce: grant.offer_nonce,
                intent,
            },
        )
        .map_err(protocol_error)?;
        tx.commit()?;
        Ok(operation)
    }

    pub(crate) fn attached_ticket(
        &self,
        invocation: InvocationId,
    ) -> StoreResult<Option<InvocationTicket>> {
        let ticket: Option<String> = self
            .connection
            .query_row(
                "SELECT ticket_json FROM attached_tickets WHERE invocation_id=?1",
                [invocation.to_string()],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(json) = ticket {
            return Ok(Some(serde_json::from_str(&json)?));
        }
        let outcome:Option<String>=self.connection.query_row("SELECT o.outcome_json FROM attached_local_invocations i JOIN attached_local_ticket_outcomes o ON o.operation_id=i.operation_id WHERE i.invocation_id=?1",[invocation.to_string()],|r|r.get(0)).optional()?;
        if let Some(json) = outcome {
            if waiting_outcome(&json)? {
                return Ok(None);
            }
            return Err(StoreError::InvalidState(format!(
                "Invocation Ticket request did not grant release: {json}"
            )));
        }
        Ok(None)
    }

    /// Retry only an authenticated, durably completed waiting response. An
    /// unknown/lost response keeps its original operation and cannot authorize
    /// a second command. The same prepared root/intent remains kernel-stopped.
    pub(crate) fn retry_waiting_attached_ticket(
        &mut self,
        invocation: InvocationId,
    ) -> StoreResult<bool> {
        let tx = self.connection.transaction()?;
        let prior: Option<(String, String, String)> = tx.query_row(
            "SELECT i.allocation_key,i.intent_json,o.outcome_json FROM attached_local_invocations i
             JOIN attached_local_ticket_outcomes o ON o.operation_id=i.operation_id
             WHERE i.invocation_id=?1
             AND NOT EXISTS(SELECT 1 FROM attached_tickets t WHERE t.invocation_id=i.invocation_id)",
            [invocation.to_string()],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        ).optional()?;
        let Some((key, intent, outcome)) = prior else {
            return Ok(false);
        };
        if !waiting_outcome(&outcome)? {
            return Ok(false);
        }
        let json: String = tx.query_row(
            "SELECT grant_json FROM attached_grants WHERE allocation_key=?1 AND seal_json IS NULL",
            [&key],
            |r| r.get(0),
        )?;
        let grant: crate::machine::GrantSnapshot = serde_json::from_str(&json)?;
        if grant.state != GrantState::Armed {
            return Ok(false);
        }
        let operation = Uuid::now_v7();
        manager::enqueue(
            &tx,
            operation,
            &Command::AuthorizeInvocation {
                key: serde_json::from_str(&key)?,
                offer_nonce: grant.offer_nonce,
                intent: serde_json::from_str(&intent)?,
            },
        )
        .map_err(protocol_error)?;
        tx.execute(
            "UPDATE attached_local_invocations SET operation_id=?2 WHERE invocation_id=?1",
            params![invocation.to_string(), operation.to_string()],
        )?;
        tx.commit()?;
        Ok(true)
    }
}

#[cfg(target_os = "linux")]
fn waiting_outcome(json: &str) -> StoreResult<bool> {
    Ok(
        matches!(serde_json::from_str::<crate::machine::Outcome>(json)?,
        crate::machine::Outcome::Rejected { code, .. } if matches!(code.as_str(),
            "quiet_waiting" | "quiet_contaminated" | "detector_unavailable" |
            "observation_stale" | "observation_missing" | "observation_unusable" |
            "observed_resource_busy" | "unavailable")),
    )
}

#[cfg(target_os = "linux")]
fn role_text(role: InvocationRole) -> &'static str {
    match role {
        InvocationRole::Primary => "primary",
        InvocationRole::Probe => "probe",
        InvocationRole::Postcondition => "postcondition",
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    #[test]
    fn waiting_ticket_reply_survives_outbox_compaction_without_replaying_unknown_operation() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch("CREATE TABLE attached_outbox(operation_id TEXT, outcome_json TEXT);")
            .unwrap();
        initialize(&connection).unwrap();
        connection.execute_batch("INSERT INTO attached_local_invocations VALUES ('invocation','first','key','intent');
            INSERT INTO attached_outbox VALUES ('first',NULL);").unwrap();
        let waiting = serde_json::to_string(&crate::machine::Outcome::Rejected {
            code: "quiet_waiting".into(),
            detail: "not yet stable".into(),
        })
        .unwrap();
        connection
            .execute(
                "UPDATE attached_outbox SET outcome_json=?1 WHERE operation_id='first'",
                [&waiting],
            )
            .unwrap();
        connection
            .execute("DELETE FROM attached_outbox", [])
            .unwrap();
        let lookup = "SELECT o.outcome_json FROM attached_local_invocations i
            JOIN attached_local_ticket_outcomes o ON o.operation_id=i.operation_id";
        let retained: String = connection.query_row(lookup, [], |r| r.get(0)).unwrap();
        assert!(waiting_outcome(&retained).unwrap());
        connection
            .execute(
                "UPDATE attached_local_invocations SET operation_id='second'",
                [],
            )
            .unwrap();
        let unknown: Option<String> = connection
            .query_row(lookup, [], |r| r.get(0))
            .optional()
            .unwrap();
        assert!(
            unknown.is_none(),
            "old waiting reply authorized replay of an unanswered operation"
        );
        for code in ["conflict", "quiet_unattainable", "stale", "unauthorized"] {
            let fatal = serde_json::to_string(&crate::machine::Outcome::Rejected {
                code: code.into(),
                detail: "final".into(),
            })
            .unwrap();
            assert!(!waiting_outcome(&fatal).unwrap());
        }
    }
}
