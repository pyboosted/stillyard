//! Coordinator-owned durable protocol data. Mutations use the same SQLite
//! transaction boundary as native admission; no transport call occurs here.

use super::*;
use crate::machine::{
    ConnectChallenge, ConnectHello, PairingRegistration, ParticipantSnapshot, SessionIdentity,
};

pub(super) fn initialize_schema(connection: &Connection) -> StoreResult<()> {
    // This separately versioned extension preserves the baseline Job tables.
    // Older executables reject the extended authority registry before admission.
    connection.execute_batch(
        "BEGIN IMMEDIATE;
        CREATE TABLE IF NOT EXISTS machine_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
        INSERT OR IGNORE INTO machine_meta VALUES ('schema_version', '1');
        CREATE TABLE IF NOT EXISTS machine_domain_retirements(
            operation_id TEXT PRIMARY KEY,domain_id TEXT UNIQUE NOT NULL,receipt_json TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS machine_domains(
            domain_id TEXT PRIMARY KEY, registration_sha256 TEXT NOT NULL,
            installation_json TEXT NOT NULL, manager_store_uuid TEXT NOT NULL,
            connection_epoch INTEGER NOT NULL DEFAULT 0, executor_incarnation TEXT,
            reconciliation_required INTEGER NOT NULL DEFAULT 1,
            retired_sequence_floor INTEGER NOT NULL DEFAULT 0,
            accepted_sequence INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS machine_challenges(
            domain_id TEXT PRIMARY KEY REFERENCES machine_domains(domain_id),
            challenge_json TEXT NOT NULL, expires_ms INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS machine_operations(
            domain_id TEXT NOT NULL REFERENCES machine_domains(domain_id),
            sequence INTEGER NOT NULL, operation_id TEXT NOT NULL,
            payload_sha256 TEXT NOT NULL, response_json TEXT NOT NULL,
            PRIMARY KEY(domain_id, sequence), UNIQUE(domain_id, operation_id));
        INSERT OR IGNORE INTO machine_meta VALUES ('revision', '0');
        INSERT OR IGNORE INTO machine_meta VALUES ('schedule_ms', '0');
        CREATE TABLE IF NOT EXISTS machine_queue(
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            owner TEXT NOT NULL UNIQUE, accepted_ms INTEGER NOT NULL, priority INTEGER);
        CREATE TRIGGER IF NOT EXISTS machine_native_accepted AFTER INSERT ON jobs BEGIN
            INSERT INTO machine_queue(owner, accepted_ms) VALUES ('native:' || NEW.id, NEW.accepted_ms);
        END;
        INSERT OR IGNORE INTO machine_queue(owner, accepted_ms)
            SELECT 'native:' || id, accepted_ms FROM jobs ORDER BY accepted_ms, rowid;
        CREATE TABLE IF NOT EXISTS machine_candidates(
            allocation_key TEXT PRIMARY KEY, domain_id TEXT NOT NULL REFERENCES machine_domains(domain_id),
            candidate_json TEXT NOT NULL, queue_owner TEXT NOT NULL REFERENCES machine_queue(owner),
            state TEXT NOT NULL, expires_ms INTEGER NOT NULL, revision INTEGER NOT NULL,
            reservation_deadline_ms INTEGER, not_before_ms INTEGER);
        CREATE TABLE IF NOT EXISTS machine_grants(
            allocation_key TEXT PRIMARY KEY REFERENCES machine_candidates(allocation_key),
            snapshot_json TEXT NOT NULL, physical_claims_json TEXT NOT NULL,
            state TEXT NOT NULL, deadline_ms INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS machine_reconciles(
            domain_id TEXT PRIMARY KEY REFERENCES machine_domains(domain_id), snapshot_id TEXT NOT NULL,
            manifest_json TEXT NOT NULL, pages_json TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS machine_readiness(
            allocation_key TEXT PRIMARY KEY REFERENCES machine_candidates(allocation_key),
            progress_json TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS machine_ticket_identities(
            invocation_id TEXT PRIMARY KEY, containment_id TEXT UNIQUE NOT NULL,
            allocation_key TEXT NOT NULL REFERENCES machine_grants(allocation_key), intent_json TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS machine_reservations(
            allocation_key TEXT PRIMARY KEY REFERENCES machine_candidates(allocation_key),
            physical_claims_json TEXT NOT NULL);
        COMMIT;",
    )?;
    let version: String = connection.query_row(
        "SELECT value FROM machine_meta WHERE key='schema_version'",
        [],
        |row| row.get(0),
    )?;
    if version != "1" {
        return Err(StoreError::InvalidState(
            "unsupported coordinator schema version".into(),
        ));
    }
    super::machine_events::initialize_schema(connection)?;
    Ok(())
}

pub(super) fn rejected(code: &str, detail: &str) -> StoreError {
    StoreError::OperationRejected {
        code: code.into(),
        detail: detail.into(),
    }
}

impl Store {
    pub(crate) fn machine_clearance_preview(
        &mut self,
        domain: crate::ExecutionDomainId,
    ) -> StoreResult<crate::machine::DomainClearancePreview> {
        self.reconcile_pending_domain_retirement()?;
        self.reconcile_pending_machine_commit()?;
        Ok(self.authority_lock()?.domain_clearance_preview(domain)?)
    }

    pub(super) fn validate_machine_history(&self) -> StoreResult<()> {
        let mut authority = self.authority_lock()?;
        // Unknown/uninitialized authorities are already closed and inspectable.
        let Ok(anchors) = authority.participants() else {
            return Ok(());
        };
        if authority
            .snapshot()
            .coordinator
            .as_ref()
            .is_some_and(|c| c.pending_reset.is_some())
        {
            return Ok(());
        }
        for anchor in anchors.into_iter().filter(|a| a.committed) {
            let persisted: Option<String> = self
                .connection
                .query_row(
                    "SELECT registration_sha256 FROM machine_domains WHERE domain_id=?1",
                    [anchor.registration.installation.domain_id.0.to_string()],
                    |row| row.get(0),
                )
                .optional()?;
            if persisted.as_deref()
                != Some(crate::machine::payload_hash(&anchor.registration)?.as_str())
            {
                authority.record_reset(
                    "coordinator participant rows do not match durable pairing anchors",
                )?;
                break;
            }
            let participant =
                self.machine_participant(anchor.registration.installation.domain_id)?;
            if participant.accepted_sequence < anchor.accepted_sequence
                || participant.retired_sequence_floor < anchor.retired_sequence_floor
                || participant.connection_epoch > anchor.next_connection_epoch
                || anchor.session.as_ref().is_some_and(|session| {
                    participant.connection_epoch > session.connection_epoch
                        || (participant.connection_epoch == session.connection_epoch
                            && participant.executor_incarnation
                                != Some(session.executor_incarnation))
                })
            {
                authority.record_reset("coordinator session/sequence rows rolled back across durable participant checkpoints")?;
                return Ok(());
            }
            authority.checkpoint_machine_sequence(
                participant.installation.domain_id,
                participant.accepted_sequence,
            )?;
        }
        let snapshot = authority.snapshot();
        if snapshot
            .coordinator
            .as_ref()
            .is_some_and(|c| c.pending_reset.is_some())
        {
            return Ok(());
        }
        let expected = snapshot
            .machine_obligations
            .into_iter()
            .map(|g| (g.grant_id, g))
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut statement = self.connection.prepare(
            "SELECT snapshot_json FROM machine_grants WHERE state IN ('armed','uncertain')",
        )?;
        let actual = statement
            .query_map([], |r| r.get::<_, String>(0))?
            .map(|row| {
                let grant: crate::machine::GrantSnapshot = serde_json::from_str(&row?)?;
                Ok((grant.grant_id, grant))
            })
            .collect::<StoreResult<std::collections::BTreeMap<_, _>>>()?;
        if actual != expected {
            authority.record_reset(
                "SQLite Grants and external start permissions are not the same inventory",
            )?;
            return Ok(());
        }
        if snapshot.native_coverage_store == Some(self.store_uuid) {
            let expected = snapshot
                .native_obligations
                .iter()
                .map(|p| p.invocation_id.entity_uuid().to_string())
                .collect::<std::collections::BTreeSet<_>>();
            let mut statement = self.connection.prepare("SELECT i.id FROM invocations i JOIN containments c ON c.invocation_id=i.id WHERE i.root_pid IS NOT NULL AND c.state NOT IN ('empty','cleared')")?;
            for row in statement.query_map([], |r| r.get::<_, String>(0))? {
                if !expected.contains(&row?) {
                    authority
                        .record_reset("native root exists without its external start permission")?;
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    pub(crate) fn pair_machine_domain(
        &mut self,
        registration: PairingRegistration,
    ) -> StoreResult<ParticipantSnapshot> {
        self.reconcile_pending_domain_retirement()?;
        self.require_machine_registration(&registration)?;
        let id = registration.installation.domain_id;
        let hash = crate::machine::payload_hash(&registration)?;
        // Registry first: an interrupted SQLite commit leaves an explicit gate.
        self.authority_lock()?
            .prepare_pairing(registration.clone())?;
        super::machine_allocation::crash_boundary(&self.paths.root, id.0, "after_pairing_journal");
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let prior: Option<String> = tx
            .query_row(
                "SELECT registration_sha256 FROM machine_domains WHERE domain_id=?1",
                [id.0.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        match prior {
            Some(prior) if prior != hash => {
                return Err(rejected("conflict", "domain registration payload changed"));
            }
            Some(_) => {}
            None => {
                tx.execute("INSERT INTO machine_domains(domain_id, registration_sha256, installation_json, manager_store_uuid) VALUES (?1, ?2, ?3, ?4)",
                    params![id.0.to_string(), hash, serde_json::to_string(&registration.installation)?, registration.manager_store_uuid.to_string()])?;
            }
        }
        tx.commit()?;
        self.authority_lock()?.commit_pairing(id)?;
        self.machine_participant(id)
    }

    fn require_machine_registration(&self, registration: &PairingRegistration) -> StoreResult<()> {
        let snapshot = self.authority_snapshot()?;
        let domains = snapshot
            .domains
            .ok_or_else(|| rejected("history_unknown", "authority topology unavailable"))?;
        if snapshot
            .coordinator
            .as_ref()
            .is_none_or(|c| c.store_uuid != self.store_uuid || c.pending_reset.is_some())
        {
            return Err(rejected(
                "history_unknown",
                "coordinator history must reconcile before pairing",
            ));
        }
        if registration.installation.domain_id.0.is_nil()
            || registration.installation.installation_nonce.is_nil()
            || registration.manager_store_uuid.is_nil()
            || registration.installation.domain_id == domains.machine_scope
            || registration.installation.domain_id == domains.native_domain
            || registration.secret == [0; 32]
            || registration.installation.runtime_registration.is_empty()
            || registration.installation.runtime_registration.len() > 1024
            || registration.budgets.len() > 256
            || registration.aliases.len() > 256
        {
            return Err(rejected(
                "invalid_registration",
                "invalid or excessive participant identity/mapping",
            ));
        }
        // The full tree is validated by the same core as admission. No new
        // physical resources or disconnected roots can be manufactured by a peer.
        let anchors = self.authority_lock()?.participants()?;
        let root_capacities = crate::admission::machine_capacities(&self.capacities);
        let mut budgets = vec![
            crate::admission::DomainBudget {
                id: domains.machine_scope,
                parent: None,
                capacities: root_capacities,
            },
            crate::admission::DomainBudget {
                id: domains.native_domain,
                parent: Some(domains.machine_scope),
                capacities: Default::default(),
            },
        ];
        let mut aliases = std::collections::BTreeMap::new();
        for r in anchors
            .iter()
            .map(|a| &a.registration)
            .filter(|r| r.installation.domain_id != registration.installation.domain_id)
            .chain(std::iter::once(registration))
        {
            if r.budgets
                .keys()
                .chain(r.aliases.keys())
                .chain(r.aliases.values())
                .any(|s| s.is_empty() || s.len() > 256 || s.contains('\0'))
            {
                return Err(rejected(
                    "invalid_registration",
                    "invalid resource alias or budget name",
                ));
            }
            budgets.push(crate::admission::DomainBudget {
                id: r.installation.domain_id,
                parent: Some(r.parent_domain),
                capacities: r.budgets.clone(),
            });
            aliases.extend(r.aliases.iter().map(|(alias, physical)| {
                ((r.installation.domain_id, alias.clone()), physical.clone())
            }));
        }
        crate::admission::ResourceTopology::new(domains.machine_scope, budgets, aliases)
            .map_err(StoreError::InvalidSpec)?;
        Ok(())
    }

    pub(crate) fn machine_participant(
        &self,
        domain: crate::ExecutionDomainId,
    ) -> StoreResult<ParticipantSnapshot> {
        let row = self.connection.query_row("SELECT installation_json, manager_store_uuid, connection_epoch, executor_incarnation, reconciliation_required, retired_sequence_floor, accepted_sequence FROM machine_domains WHERE domain_id=?1", [domain.0.to_string()], |row| {
            Ok((row.get::<_,String>(0)?, row.get::<_,String>(1)?, row.get::<_,u64>(2)?, row.get::<_,Option<String>>(3)?, row.get::<_,bool>(4)?, row.get::<_,u64>(5)?, row.get::<_,u64>(6)?))
        }).optional()?.ok_or_else(|| rejected("not_found", "participant is not registered"))?;
        Ok(ParticipantSnapshot {
            installation: serde_json::from_str(&row.0)?,
            manager_store_uuid: Uuid::parse_str(&row.1)?,
            connection_epoch: row.2,
            executor_incarnation: row.3.map(|s| Uuid::parse_str(&s)).transpose()?,
            reconciliation_required: row.4,
            retired_sequence_floor: row.5,
            accepted_sequence: row.6,
        })
    }

    pub(crate) fn machine_connect_begin(
        &mut self,
        hello: ConnectHello,
    ) -> StoreResult<ConnectChallenge> {
        self.reconcile_pending_domain_retirement()?;
        self.reconcile_pending_machine_commit()?;
        if let Some(operation) = self
            .authority_lock()?
            .pending_installation_retirement(hello.installation_nonce, hello.manager_store_uuid)
        {
            return Err(rejected(
                "retirement_pending",
                &format!(
                    "domain remains fenced by pending retirement operation {operation}; inspect authority status and repair coordinator history"
                ),
            ));
        }
        if let Some(receipt) = self
            .authority_lock()?
            .retired_installation_receipt(hello.installation_nonce, hello.manager_store_uuid)
        {
            return Err(rejected(
                "retired_domain",
                &serde_json::to_string(&receipt)?,
            ));
        }
        if hello.executor_protocol != crate::protocol::PROTOCOL_VERSION
            || hello.executor_incarnation.is_nil()
            || hello.executor_nonce == [0; 32]
        {
            return Err(rejected(
                "unsupported",
                "executor protocol or incarnation is not supported",
            ));
        }
        let registration = self
            .authority_lock()?
            .participants()?
            .into_iter()
            .find(|a| {
                a.committed
                    && a.registration.installation.installation_nonce == hello.installation_nonce
            })
            .ok_or_else(|| rejected("history_unknown", "continuous pairing anchor not found"))?
            .registration;
        let participant = self.machine_participant(registration.installation.domain_id)?;
        if participant.manager_store_uuid != hello.manager_store_uuid
            || registration.manager_store_uuid != hello.manager_store_uuid
        {
            return Err(rejected(
                "history_unknown",
                "manager store changed; its outstanding rights require inventory reconciliation",
            ));
        }
        let snapshot = self.authority_snapshot()?;
        let domains = snapshot
            .domains
            .ok_or_else(|| rejected("history_unknown", "machine identity unavailable"))?;
        let mut nonce = [0; 32];
        getrandom::fill(&mut nonce).map_err(std::io::Error::other)?;
        let epoch = self.authority_lock()?.reserve_machine_connection(
            participant.installation.domain_id,
            participant.connection_epoch,
        )?;
        let challenge = ConnectChallenge {
            wire_version: crate::machine::WIRE_VERSION,
            coordinator_protocol: crate::protocol::PROTOCOL_VERSION,
            executor_protocol: hello.executor_protocol,
            coordinator_installation: domains.machine_id,
            installation: registration.installation,
            session: SessionIdentity {
                machine_id: domains.machine_id,
                authority_epoch: snapshot
                    .epoch
                    .ok_or_else(|| rejected("history_unknown", "authority epoch unavailable"))?,
                domain_id: participant.installation.domain_id,
                manager_store_uuid: hello.manager_store_uuid,
                executor_incarnation: hello.executor_incarnation,
                connection_epoch: epoch,
            },
            coordinator_nonce: nonce,
            executor_nonce: hello.executor_nonce,
        };
        self.connection.execute("INSERT INTO machine_challenges VALUES (?1, ?2, ?3) ON CONFLICT(domain_id) DO UPDATE SET challenge_json=excluded.challenge_json, expires_ms=excluded.expires_ms",
            params![participant.installation.domain_id.0.to_string(), serde_json::to_string(&challenge)?, now_millis().saturating_add(30_000)])?;
        Ok(challenge)
    }

    pub(crate) fn machine_connect_finish(
        &mut self,
        challenge: ConnectChallenge,
        tag: [u8; 32],
    ) -> StoreResult<ParticipantSnapshot> {
        self.reconcile_pending_domain_retirement()?;
        self.reconcile_pending_machine_commit()?;
        let anchor = self
            .authority_lock()?
            .participants()?
            .into_iter()
            .find(|a| a.committed && a.registration.installation == challenge.installation)
            .ok_or_else(|| rejected("history_unknown", "pairing anchor missing or changed"))?;
        crate::machine::PairingSecret::from_anchor(anchor.registration.secret)
            .verify_challenge(&challenge, &tag)
            .map_err(|_| rejected("unauthorized", "pairing authentication failed"))?;
        let id = challenge.installation.domain_id;
        let journal = self
            .authority
            .clone()
            .ok_or_else(|| rejected("history_unknown", "pairing journal unavailable"))?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let pending: Option<(String, i64)> = tx
            .query_row(
                "SELECT challenge_json, expires_ms FROM machine_challenges WHERE domain_id=?1",
                [id.0.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((pending, expires)) = pending else {
            return Err(rejected("stale", "challenge was consumed or replaced"));
        };
        if pending != serde_json::to_string(&challenge)? || expires <= now_millis() {
            return Err(rejected(
                "stale",
                "challenge was consumed, expired or replaced",
            ));
        }
        journal
            .lock()
            .map_err(|_| StoreError::InvalidState("authority mutex poisoned".into()))?
            .accept_machine_session(challenge.session.clone())?;
        let changed = tx.execute("UPDATE machine_domains SET connection_epoch=?2, executor_incarnation=?3, reconciliation_required=1 WHERE domain_id=?1 AND connection_epoch<?2 AND manager_store_uuid=?4",
            params![id.0.to_string(), challenge.session.connection_epoch, challenge.session.executor_incarnation.to_string(), challenge.session.manager_store_uuid.to_string()])?;
        if changed != 1 {
            return Err(rejected(
                "stale",
                "connection epoch or manager store changed",
            ));
        }
        tx.execute(
            "DELETE FROM machine_challenges WHERE domain_id=?1",
            [id.0.to_string()],
        )?;
        tx.execute(
            "DELETE FROM machine_reconciles WHERE domain_id=?1",
            [id.0.to_string()],
        )?;
        tx.execute(
            "UPDATE machine_candidates SET reservation_deadline_ms=NULL WHERE domain_id=?1",
            [id.0.to_string()],
        )?;
        tx.execute("DELETE FROM machine_reservations WHERE allocation_key IN (SELECT allocation_key FROM machine_candidates WHERE domain_id=?1)",[id.0.to_string()])?;
        tx.commit()?;
        self.machine_participant(id)
    }
}
