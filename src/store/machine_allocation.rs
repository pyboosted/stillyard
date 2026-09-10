use super::machine::rejected;
use super::*;
use crate::machine::{
    AllocationKey, AllocationOwner, Candidate, Command, GrantSnapshot, Outcome, Reply, Request,
};

pub(super) fn key_string(key: &AllocationKey) -> StoreResult<String> {
    Ok(serde_json::to_string(key)?)
}

fn rejection(error: StoreError) -> std::result::Result<Outcome, StoreError> {
    match error {
        StoreError::OperationRejected { code, detail } => Ok(Outcome::Rejected { code, detail }),
        StoreError::InvalidSpec(detail) => Ok(Outcome::Rejected {
            code: "invalid_spec".into(),
            detail,
        }),
        other => Err(other),
    }
}

impl Store {
    fn compact_machine_retirements(&self) -> StoreResult<()> {
        let candidates = self
            .authority_lock()?
            .retirement_candidates(self.store_uuid());
        if candidates.is_empty() {
            return Ok(());
        }
        let candidates: std::collections::BTreeSet<_> = candidates.into_iter().collect();
        let mut proven = std::collections::BTreeSet::new();
        let mut rows = self
            .connection
            .prepare("SELECT allocation_key FROM machine_grants WHERE state='released'")?;
        for row in rows.query_map([], |r| r.get::<_, String>(0))? {
            let key: AllocationKey = serde_json::from_str(&row?)?;
            let hash = crate::machine::payload_hash(&key)?;
            if candidates.contains(&hash) {
                proven.insert(hash);
            }
        }
        self.authority_lock()?
            .compact_machine_retirements(self.store_uuid(), &proven)?;
        Ok(())
    }

    pub(crate) fn machine_exchange(
        &mut self,
        request: Request,
        sample: Option<&crate::host_observation::HostSample>,
    ) -> StoreResult<Reply> {
        self.reconcile_pending_domain_retirement()?;
        self.reconcile_pending_machine_commit()?;
        self.compact_machine_retirements()?;
        if self
            .authority_lock()?
            .is_retired_domain(request.session.domain_id)
        {
            return Err(rejected(
                "retired_domain",
                "the owner retired this manager registration",
            ));
        }
        request.validate()?;
        crate::machine::write_frame(&mut std::io::sink(), &request)?;
        let anchor = self
            .authority_lock()?
            .participants()?
            .into_iter()
            .find(|a| {
                a.committed && a.registration.installation.domain_id == request.session.domain_id
            })
            .ok_or_else(|| rejected("unauthorized", "paired executor not found"))?;
        if anchor.registration.installation.role != crate::machine::ParticipantRole::Executor {
            return Err(rejected(
                "unauthorized",
                "runtime-adapter role cannot issue executor operations",
            ));
        }
        crate::machine::PairingSecret::from_anchor(anchor.registration.secret)
            .verify_request(&request)
            .map_err(|_| rejected("unauthorized", "machine operation authentication failed"))?;
        if anchor.session.as_ref() != Some(&request.session) {
            return Err(rejected(
                "stale",
                "session was fenced by the durable pairing anchor",
            ));
        }
        let authority = self.authority_snapshot()?;
        let domains = authority
            .domains
            .as_ref()
            .ok_or_else(|| rejected("history_unknown", "machine identity is unavailable"))?;
        if authority.epoch != Some(request.session.authority_epoch)
            || domains.machine_id != request.session.machine_id
        {
            return Err(rejected("stale", "machine authority changed"));
        }
        let participant = self.machine_participant(request.session.domain_id)?;
        if participant.connection_epoch != request.session.connection_epoch
            || participant.manager_store_uuid != request.session.manager_store_uuid
            || participant.executor_incarnation != Some(request.session.executor_incarnation)
        {
            return Err(rejected(
                "stale",
                "participant connection or store was fenced",
            ));
        }
        let config_sha256 = self.config_sha256.clone();
        let host_config = self.host_config();
        let journal = self
            .authority
            .as_ref()
            .cloned()
            .ok_or_else(|| rejected("history_unknown", "authority journal unavailable"))?;
        let mut new_operation = false;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let domain = request.session.domain_id.0.to_string();
        let prior: Option<(u64, String, String, String)> = tx.query_row(
            "SELECT sequence, operation_id, payload_sha256, response_json FROM machine_operations WHERE domain_id=?1 AND (sequence=?2 OR operation_id=?3)",
            params![domain, request.request_sequence, request.operation_id.to_string()],
            |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?))).optional()?;
        let outcome = if let Some((sequence, operation, hash, response)) = prior {
            if sequence != request.request_sequence
                || operation != request.operation_id.to_string()
                || hash != request.payload_sha256
            {
                return Err(rejected(
                    "conflict",
                    "operation or sequence has another payload",
                ));
            }
            serde_json::from_str(&response)?
        } else {
            new_operation = true;
            if request.request_sequence <= participant.retired_sequence_floor {
                return Err(rejected(
                    "history_unknown",
                    "operation is below the retired sequence floor",
                ));
            }
            if participant.accepted_sequence.checked_add(1) != Some(request.request_sequence)
                || request.request_sequence > i64::MAX as u64
            {
                return Err(rejected(
                    "stale",
                    "request sequence is not the next durable operation",
                ));
            }
            let pending: u64 = tx.query_row(
                "SELECT COUNT(*) FROM machine_operations WHERE domain_id=?1",
                [&domain],
                |row| row.get(0),
            )?;
            if pending >= 16_384 && !matches!(request.command, Command::Acknowledge { .. }) {
                return Err(rejected(
                    "limit_exceeded",
                    "operation history requires bilateral acknowledgement before compaction",
                ));
            }
            let prior_now: i64 = tx
                .query_row(
                    "SELECT value FROM machine_meta WHERE key='schedule_ms'",
                    [],
                    |row| row.get::<_, String>(0),
                )?
                .parse()
                .map_err(|_| StoreError::InvalidState("invalid coordinator clock".into()))?;
            let now = now_millis().max(prior_now);
            tx.execute(
                "UPDATE machine_meta SET value=?1 WHERE key='schedule_ms'",
                [now.to_string()],
            )?;
            tx.execute_batch("SAVEPOINT machine_command")?;
            let result = apply_command(
                &tx,
                &request,
                &config_sha256,
                participant.reconciliation_required,
                authority.blocker.is_some(),
                now,
                &super::machine_ticket::Readiness {
                    config: &host_config,
                    sample,
                },
            );
            // A response must be deliverable BEFORE committing its effects or
            // durable replay record. Otherwise the oldest outbox entry wedges.
            let result = result.and_then(|outcome| {
                let preview = Reply {
                    session: request.session.clone(),
                    request_sequence: request.request_sequence,
                    operation_id: request.operation_id,
                    coordinator_revision: u64::MAX,
                    outcome,
                };
                crate::machine::write_frame(&mut std::io::sink(), &preview).map_err(|_| {
                    rejected(
                        "limit_exceeded",
                        "machine response exceeds the wire byte budget",
                    )
                })?;
                let grants = commit_grants(&tx, &request, &preview.outcome)?;
                if !grants.is_empty() || matches!(preview.outcome, Outcome::Acknowledged { .. }) {
                    journal
                        .lock()
                        .map_err(|_| StoreError::InvalidState("authority mutex poisoned".into()))?
                        .check_machine_commit(crate::authority::MachineCommitIntent {
                            request: request.clone(),
                            outcome: preview.outcome.clone(),
                            grants,
                        })
                        .map_err(|error| {
                            if error.kind() == std::io::ErrorKind::InvalidInput {
                                rejected("limit_exceeded", &error.to_string())
                            } else {
                                StoreError::Io(error)
                            }
                        })?;
                }
                Ok(preview.outcome)
            });
            let outcome = match result {
                Ok(outcome) => {
                    tx.execute_batch("RELEASE machine_command")?;
                    outcome
                }
                Err(error) => {
                    tx.execute_batch("ROLLBACK TO machine_command; RELEASE machine_command")?;
                    rejection(error)?
                }
            };
            tx.execute(
                "INSERT INTO machine_operations VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    domain,
                    request.request_sequence,
                    request.operation_id.to_string(),
                    request.payload_sha256,
                    serde_json::to_string(&outcome)?
                ],
            )?;
            tx.execute(
                "UPDATE machine_domains SET accepted_sequence=?2 WHERE domain_id=?1",
                params![domain, request.request_sequence],
            )?;
            tx.execute(
                "UPDATE machine_meta SET value=CAST(value AS INTEGER)+1 WHERE key='revision'",
                [],
            )?;
            outcome
        };
        let revision: String = tx.query_row(
            "SELECT value FROM machine_meta WHERE key='revision'",
            [],
            |row| row.get(0),
        )?;
        let coordinator_revision = revision
            .parse()
            .map_err(|_| StoreError::InvalidState("invalid coordinator revision".into()))?;
        let grants = if new_operation {
            commit_grants(&tx, &request, &outcome)?
        } else {
            vec![]
        };
        let prepared = !grants.is_empty() || matches!(outcome, Outcome::Acknowledged { .. });
        if prepared {
            crash_boundary(&self.paths.root, request.operation_id, "before_journal");
            journal
                .lock()
                .map_err(|_| StoreError::InvalidState("authority mutex poisoned".into()))?
                .prepare_machine_commit(crate::authority::MachineCommitIntent {
                    request: request.clone(),
                    outcome: outcome.clone(),
                    grants,
                })?;
            crash_boundary(&self.paths.root, request.operation_id, "after_journal");
        }
        tx.commit()?;
        if prepared {
            crash_boundary(&self.paths.root, request.operation_id, "after_sql");
            journal
                .lock()
                .map_err(|_| StoreError::InvalidState("authority mutex poisoned".into()))?
                .finish_machine_commit(request.operation_id)?;
            crash_boundary(&self.paths.root, request.operation_id, "after_ack");
        }
        journal
            .lock()
            .map_err(|_| StoreError::InvalidState("authority mutex poisoned".into()))?
            .checkpoint_machine_sequence(request.session.domain_id, request.request_sequence)?;
        Ok(Reply {
            session: request.session,
            request_sequence: request.request_sequence,
            operation_id: request.operation_id,
            coordinator_revision,
            outcome,
        })
    }
}

fn commit_grants(
    c: &Connection,
    request: &Request,
    outcome: &Outcome,
) -> StoreResult<Vec<GrantSnapshot>> {
    match outcome {
        Outcome::Grant { grant }
            if matches!(
                grant.state,
                crate::machine::GrantState::Armed | crate::machine::GrantState::Uncertain
            ) =>
        {
            Ok(vec![grant.as_ref().clone()])
        }
        Outcome::Released { grant_id, .. } => {
            let Command::Release { release } = &request.command else {
                return Err(StoreError::InvalidState(
                    "release outcome has no allocation".into(),
                ));
            };
            let grant = load_grant(c, &release.key)?;
            if grant.grant_id != *grant_id {
                return Err(StoreError::InvalidState(
                    "release Grant identity changed".into(),
                ));
            }
            Ok(vec![grant])
        }
        Outcome::Ticket { ticket } => Ok(vec![load_grant(c, &ticket.key)?]),
        Outcome::Reconciled { released, .. } => {
            released.iter().map(|key| load_grant(c, key)).collect()
        }
        _ => Ok(vec![]),
    }
}

/// Abrupt-exit injection is compiled out of release builds and cannot target
/// the system store. The integration harness must opt in to its exact root and
/// pin an executable beside that root; a file selects one operation and boundary.
pub(super) fn crash_boundary(root: &std::path::Path, operation: Uuid, stage: &str) {
    #[cfg(debug_assertions)]
    {
        use std::io::Write;
        let Some(selected) = std::env::var_os("STILLYARD_ISOLATED_MACHINE_FAULT_ROOT") else {
            return;
        };
        let Ok(root) = root.canonicalize() else {
            return;
        };
        if std::path::PathBuf::from(selected)
            .canonicalize()
            .ok()
            .as_ref()
            != Some(&root)
        {
            return;
        }
        let Ok(default) = crate::instance::default_instance() else {
            return;
        };
        let default_root = default
            .store_path
            .canonicalize()
            .unwrap_or(default.store_path);
        if root == default_root {
            return;
        }
        let Ok(exe) = std::env::current_exe() else {
            return;
        };
        let Some(pinned) = exe.parent() else {
            return;
        };
        if pinned.file_name() != Some(std::ffi::OsStr::new("pinned-revision"))
            || pinned.parent().and_then(|p| p.canonicalize().ok())
                != root.parent().map(std::path::Path::to_path_buf)
        {
            return;
        }
        let path = root.join("machine-fault.json");
        let Ok(file) = std::fs::File::open(&path) else {
            return;
        };
        use std::io::Read;
        let Ok((selected_operation, selected_stage)) =
            serde_json::from_reader::<_, (Uuid, String)>(file.take(1024))
        else {
            return;
        };
        if selected_operation != operation || selected_stage != stage {
            return;
        }
        let mut fired = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(root.join(format!("machine-fault-{operation}.fired")))
            .expect("create fault evidence");
        fired
            .write_all(stage.as_bytes())
            .expect("write fault evidence");
        fired.sync_all().expect("flush fault evidence");
        std::fs::remove_file(path).expect("consume fault intent");
        std::process::exit(86);
    }
    #[cfg(not(debug_assertions))]
    let _ = (root, operation, stage);
}

pub(super) fn load_grant(
    connection: &Connection,
    key: &AllocationKey,
) -> StoreResult<GrantSnapshot> {
    let row: Option<(String, String)> = connection
        .query_row(
            "SELECT snapshot_json,state FROM machine_grants WHERE allocation_key=?1",
            [key_string(key)?],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    let (json, state) =
        row.ok_or_else(|| rejected("not_found", "allocation has no Offer or Grant"))?;
    let grant: GrantSnapshot = serde_json::from_str(&json)?;
    if &grant.candidate.key != key
        || serde_json::to_value(&grant.state)?.as_str() != Some(state.as_str())
    {
        return Err(StoreError::InvalidState(
            "Grant state or allocation identity is inconsistent".into(),
        ));
    }
    Ok(grant)
}

pub(super) fn owned(request: &Request, key: &AllocationKey) -> StoreResult<()> {
    if !request.session.owns(key) || key.lease_id.is_nil() {
        return Err(rejected(
            "unauthorized",
            "allocation belongs to another authority/domain/store",
        ));
    }
    Ok(())
}

pub(super) fn is_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit())
}

fn validate_candidate(
    request: &Request,
    candidate: &Candidate,
    configuration: &str,
) -> StoreResult<()> {
    owned(request, &candidate.key)?;
    if serde_json::to_vec(candidate)?.len() > crate::machine::MAX_FRAME_BYTES / 2 {
        return Err(rejected(
            "limit_exceeded",
            "candidate exceeds the allocation byte budget",
        ));
    }
    if let Some(policy) = &candidate.observed {
        policy
            .validate()
            .map_err(|e| StoreError::InvalidSpec(e.to_string()))?;
    }
    if let Some(policy) = &candidate.quiet {
        policy
            .validate()
            .map_err(|e| StoreError::InvalidSpec(e.to_string()))?;
    }
    let owner_store_matches = match candidate.owner {
        AllocationOwner::Work { job_id, attempt_id } => {
            job_id.store_uuid() == candidate.key.manager_store_uuid
                && attempt_id.store_uuid() == job_id.store_uuid()
        }
        AllocationOwner::Probe {
            job_id,
            invocation_id,
        } => {
            job_id.store_uuid() == candidate.key.manager_store_uuid
                && invocation_id.store_uuid() == job_id.store_uuid()
        }
    };
    if !owner_store_matches
        || !(-3..=3).contains(&candidate.priority)
        || candidate.revision == 0
        || candidate.revision > i64::MAX as u64
        || candidate.configuration_sha256 != configuration
    {
        return Err(rejected(
            "invalid_candidate",
            "candidate owner, priority, revision or configuration is invalid",
        ));
    }
    let c = &candidate.claims;
    if c.scalars.len() > 256
        || c.shared_fences.len() + c.exclusive_fences.len() > 512
        || c.impacts.len() > 64
        || c.scalars
            .keys()
            .chain(c.shared_fences.iter())
            .chain(c.exclusive_fences.iter())
            .chain(c.impacts.iter())
            .any(|s| s.is_empty() || s.len() > 1024 || s.contains('\0'))
    {
        return Err(rejected(
            "limit_exceeded",
            "candidate vector exceeds shape bounds",
        ));
    }
    Ok(())
}

fn apply_release(
    tx: &Transaction<'_>,
    request: &Request,
    release: &crate::machine::SealedRelease,
    now: i64,
) -> StoreResult<GrantSnapshot> {
    owned(request, &release.key)?;
    let mut grant = load_grant(tx, &release.key)?;
    if grant
        .sealed_release
        .as_ref()
        .is_some_and(|old| old != release)
    {
        return Err(rejected(
            "conflict",
            "a sealed release cannot change its payload",
        ));
    }
    if grant.offer_nonce != release.offer_nonce || release.sealed_sequence == 0 {
        return Err(rejected(
            "conflict",
            "release seal does not name the allocated Offer",
        ));
    }
    if !matches!(
        grant.state,
        crate::machine::GrantState::Armed
            | crate::machine::GrantState::Uncertain
            | crate::machine::GrantState::Released
    ) {
        return Err(rejected(
            "stale",
            "release does not cover a potentially used Grant",
        ));
    }
    if release.tickets.len() != grant.tickets.len() {
        return Err(rejected(
            "cleanup_incomplete",
            "release does not cover every issued ticket",
        ));
    }
    let mut covered = std::collections::BTreeSet::new();
    for ticket in &grant.tickets {
        let proof = release
            .tickets
            .iter()
            .find(|p| {
                p.invocation_id == ticket.invocation_id
                    && p.release_sequence == ticket.release_sequence
            })
            .ok_or_else(|| {
                rejected(
                    "cleanup_incomplete",
                    "issued Invocation ticket has no sealed proof",
                )
            })?;
        if !covered.insert(proof.invocation_id)
            || proof.boundary_sha256 != ticket.boundary_sha256
            || !is_digest(&proof.proof_sha256)
            || grant
                .tickets
                .iter()
                .filter_map(|t| t.previous_cleanup.as_ref())
                .any(|prior| prior.invocation_id == proof.invocation_id && prior != proof)
        {
            return Err(rejected(
                "cleanup_incomplete",
                "cleanup proof identity is invalid or duplicated",
            ));
        }
    }
    grant.state = crate::machine::GrantState::Released;
    grant.released_unix_millis.get_or_insert(now);
    grant.sealed_release = Some(release.clone());
    super::machine_queue::save_grant(tx, &grant, None)?;
    tx.execute("UPDATE machine_candidates SET state='released',reservation_deadline_ms=NULL WHERE allocation_key=?1",[key_string(&release.key)?])?;
    Ok(grant)
}

fn apply_command(
    tx: &Transaction<'_>,
    request: &Request,
    configuration: &str,
    recovering: bool,
    authority_closed: bool,
    now: i64,
    readiness: &super::machine_ticket::Readiness<'_>,
) -> StoreResult<Outcome> {
    match &request.command {
        Command::Acknowledge { through_sequence } => {
            let (accepted, floor):(u64,u64) = tx.query_row(
                "SELECT accepted_sequence,retired_sequence_floor FROM machine_domains WHERE domain_id=?1",
                [request.session.domain_id.0.to_string()],|r|Ok((r.get(0)?,r.get(1)?)))?;
            if *through_sequence > accepted || *through_sequence < floor {
                return Err(rejected(
                    "stale",
                    "acknowledgement is outside the continuous accepted interval",
                ));
            }
            compact_operations(tx, request.session.domain_id, *through_sequence)?;
            Ok(Outcome::Acknowledged {
                through_sequence: *through_sequence,
            })
        }
        Command::AuthorizeInvocation {
            key,
            offer_nonce,
            intent,
        } => {
            owned(request, key)?;
            let grant = load_grant(tx, key)?;
            if recovering
                || authority_closed
                || grant.state != crate::machine::GrantState::Armed
                || grant.offer_nonce != *offer_nonce
                || grant.candidate.configuration_sha256 != configuration
            {
                return Err(rejected(
                    "stale",
                    "Invocation authorization needs a current reconciled Armed Grant",
                ));
            }
            super::machine_ticket::authorize(tx, request, grant, intent, readiness)
        }
        Command::Arm { key, offer_nonce } => {
            owned(request, key)?;
            let mut grant = load_grant(tx, key)?;
            if grant.offer_nonce != *offer_nonce {
                return Err(rejected("stale", "Offer nonce changed"));
            }
            match grant.state {
                crate::machine::GrantState::Armed | crate::machine::GrantState::Uncertain => {}
                crate::machine::GrantState::Offered => {
                    if recovering
                        || authority_closed
                        || grant.offer_deadline_unix_millis <= now
                        || grant.candidate.configuration_sha256 != configuration
                    {
                        return Err(rejected(
                            "stale",
                            "Offer expired or authority/readiness needs reconciliation",
                        ));
                    }
                    let (state,revision,expires):(String,u64,i64)=tx.query_row("SELECT state,revision,expires_ms FROM machine_candidates WHERE allocation_key=?1",[key_string(key)?],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
                    if state != "ready" || revision != grant.candidate.revision || expires <= now {
                        return Err(rejected("stale", "candidate readiness was withdrawn"));
                    }
                    grant.state = crate::machine::GrantState::Armed;
                    grant.armed_unix_millis = Some(now);
                    super::machine_queue::save_grant(tx, &grant, None)?;
                }
                _ => {
                    return Err(rejected(
                        "stale",
                        "allocation is retired or no longer offered",
                    ));
                }
            }
            Ok(Outcome::Grant {
                grant: Box::new(grant),
            })
        }
        Command::Release { release } => {
            let grant = apply_release(tx, request, release, now)?;
            Ok(Outcome::Released {
                grant_id: grant.grant_id,
                sealed_sequence: release.sealed_sequence,
            })
        }
        Command::ReportUncertain {
            key,
            offer_nonce,
            reason,
        } => {
            owned(request, key)?;
            if reason.is_empty() || reason.len() > 1024 || reason.contains('\0') {
                return Err(rejected(
                    "invalid_spec",
                    "uncertainty needs a bounded reason",
                ));
            }
            let mut grant = load_grant(tx, key)?;
            if grant.offer_nonce != *offer_nonce
                || !matches!(
                    grant.state,
                    crate::machine::GrantState::Armed | crate::machine::GrantState::Uncertain
                )
            {
                return Err(rejected(
                    "stale",
                    "uncertainty requires an outstanding Armed allocation",
                ));
            }
            grant.state = crate::machine::GrantState::Uncertain;
            grant.uncertainty_reason = Some(reason.clone());
            super::machine_queue::save_grant(tx, &grant, None)?;
            Ok(Outcome::Grant {
                grant: Box::new(grant),
            })
        }
        Command::CandidateUpsert { candidate } => {
            validate_candidate(request, candidate, configuration)?;
            if recovering || authority_closed {
                return Err(rejected(
                    "history_unknown",
                    "candidate admission requires completed reconciliation",
                ));
            }
            let key = key_string(&candidate.key)?;
            // Retained expired/withdrawn revision history is not a ready slot.
            // Count armed obligations even if their candidate refresh expired.
            let (domain_count, machine_count) =
                active_candidate_counts(tx, candidate.key.domain_id, &key, now)?;
            if domain_count >= crate::machine::MAX_DOMAIN_CANDIDATES as u64
                || machine_count >= crate::machine::MAX_MACHINE_CANDIDATES as u64
            {
                return Err(rejected(
                    "limit_exceeded",
                    "ready candidate budget exhausted",
                ));
            }
            let previous: Option<(String, String, u64)> = tx.query_row("SELECT candidate_json, state, revision FROM machine_candidates WHERE allocation_key=?1", [&key], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).optional()?;
            if let Some((json, state, revision)) = previous {
                let prior: Candidate = serde_json::from_str(&json)?;
                if prior.claims != candidate.claims
                    || prior.owner != candidate.owner
                    || prior.priority != candidate.priority
                    || prior.observed != candidate.observed
                    || prior.quiet != candidate.quiet
                {
                    return Err(rejected(
                        "conflict",
                        "allocation owner, claims and priority are immutable",
                    ));
                }
                if candidate.revision < revision || state == "canceled" || state == "released" {
                    return Err(rejected(
                        "stale",
                        "candidate revision was withdrawn or retired",
                    ));
                }
                if candidate.revision == revision && prior != *candidate {
                    return Err(rejected("conflict", "candidate revision payload changed"));
                }
                if candidate.revision == revision && state != "ready" {
                    return Err(rejected(
                        "stale",
                        "withdrawn revision cannot become ready again",
                    ));
                }
                tx.execute("UPDATE machine_candidates SET candidate_json=?2, revision=?3, state='ready', expires_ms=?4 WHERE allocation_key=?1", params![key, serde_json::to_string(candidate)?, candidate.revision, now.saturating_add(crate::machine::CANDIDATE_MILLIS)])?;
            } else {
                let job = match candidate.owner {
                    AllocationOwner::Work { job_id, .. }
                    | AllocationOwner::Probe { job_id, .. } => job_id,
                };
                let owner = format!("remote:{job}");
                tx.execute("INSERT OR IGNORE INTO machine_queue(owner, accepted_ms, priority) VALUES (?1,?2,?3)", params![owner, now, candidate.priority])?;
                let accepted_priority: i8 = tx.query_row(
                    "SELECT priority FROM machine_queue WHERE owner=?1",
                    [&owner],
                    |row| row.get(0),
                )?;
                if accepted_priority != candidate.priority {
                    return Err(rejected(
                        "conflict",
                        "retry changed the original accepted priority",
                    ));
                }
                tx.execute("INSERT INTO machine_candidates(allocation_key,domain_id,candidate_json,queue_owner,state,expires_ms,revision) VALUES (?1,?2,?3,?4,'ready',?5,?6)", params![key,candidate.key.domain_id.0.to_string(),serde_json::to_string(candidate)?,owner,now.saturating_add(crate::machine::CANDIDATE_MILLIS),candidate.revision])?;
            }
            Ok(Outcome::Accepted {
                revision: candidate.revision,
            })
        }
        Command::Withdraw { key, revision } | Command::CancelCandidate { key, revision } => {
            owned(request, key)?;
            let key = key_string(key)?;
            let old: Option<u64> = tx
                .query_row(
                    "SELECT revision FROM machine_candidates WHERE allocation_key=?1",
                    [&key],
                    |row| row.get(0),
                )
                .optional()?;
            if old.is_none_or(|old| *revision <= old) || *revision > i64::MAX as u64 {
                return Err(rejected(
                    "stale",
                    "withdrawal needs a newer candidate revision",
                ));
            }
            let state = if matches!(request.command, Command::CancelCandidate { .. }) {
                "canceled"
            } else {
                "withdrawn"
            };
            tx.execute("UPDATE machine_candidates SET revision=?2,state=?3,reservation_deadline_ms=NULL WHERE allocation_key=?1", params![key,revision,state])?;
            let offered: Option<String> = tx.query_row("SELECT snapshot_json FROM machine_grants WHERE allocation_key=?1 AND state='offered'",[&key],|row|row.get(0)).optional()?;
            if let Some(json) = offered {
                let mut grant: GrantSnapshot = serde_json::from_str(&json)?;
                grant.state = crate::machine::GrantState::Expired;
                super::machine_queue::save_grant(tx, &grant, None)?;
            }
            Ok(Outcome::Accepted {
                revision: *revision,
            })
        }
        Command::InspectPage { after, limit } => {
            if let Some(after) = after {
                owned(request, after)?;
            }
            let (grants, truncated) = inventory_page(
                tx,
                request.session.domain_id,
                None,
                after.as_ref(),
                *limit,
                true,
            )?;
            let next = truncated.then(|| grants.last().unwrap().candidate.key.clone());
            Ok(Outcome::InventoryPage {
                grants,
                next,
                configuration_sha256: configuration.into(),
            })
        }
        Command::Inspect { key } => {
            if let Some(key) = key {
                owned(request, key)?;
            }
            let (grants, truncated) = inventory_page(
                tx,
                request.session.domain_id,
                key.as_ref(),
                None,
                256,
                false,
            )?;
            Ok(Outcome::Inspection { grants, truncated })
        }
        Command::ReconcileBegin {
            snapshot_id,
            begin_sequence,
            end_sequence,
            page_count,
            configuration_sha256,
            ..
        } => {
            let accepted: u64 = tx.query_row(
                "SELECT accepted_sequence FROM machine_domains WHERE domain_id=?1",
                [request.session.domain_id.0.to_string()],
                |row| row.get(0),
            )?;
            if snapshot_id.is_nil()
                || *page_count > crate::machine::MAX_DOMAIN_CANDIDATES as u32
                || begin_sequence > end_sequence
                || *end_sequence != accepted
                || *begin_sequence != 0
                || configuration_sha256 != configuration
            {
                return Err(rejected(
                    "history_unknown",
                    "snapshot sequence, configuration or page bounds are discontinuous",
                ));
            }
            let pages: Vec<Option<Vec<crate::machine::ReconcileAllocation>>> =
                vec![None; *page_count as usize];
            tx.execute("INSERT INTO machine_reconciles VALUES (?1,?2,?3,?4) ON CONFLICT(domain_id) DO UPDATE SET snapshot_id=excluded.snapshot_id, manifest_json=excluded.manifest_json, pages_json=excluded.pages_json",
                params![request.session.domain_id.0.to_string(),snapshot_id.to_string(),serde_json::to_string(&request.command)?,serde_json::to_string(&pages)?])?;
            tx.execute(
                "UPDATE machine_domains SET reconciliation_required=1 WHERE domain_id=?1",
                [request.session.domain_id.0.to_string()],
            )?;
            Ok(Outcome::Accepted {
                revision: *end_sequence,
            })
        }
        Command::ReconcilePage {
            snapshot_id,
            index,
            allocations,
        } => {
            let document: Option<String> = tx.query_row("SELECT pages_json FROM machine_reconciles WHERE domain_id=?1 AND snapshot_id=?2", params![request.session.domain_id.0.to_string(),snapshot_id.to_string()], |row| row.get(0)).optional()?;
            let mut pages: Vec<Option<Vec<crate::machine::ReconcileAllocation>>> =
                serde_json::from_str(
                    &document.ok_or_else(|| rejected("stale", "snapshot not begun"))?,
                )?;
            if allocations.len() > crate::machine::MAX_RECONCILE_PAGE
                || *index as usize >= pages.len()
                || pages[..*index as usize].iter().any(Option::is_none)
            {
                return Err(rejected(
                    "history_unknown",
                    "snapshot page is oversized or out of order",
                ));
            }
            for allocation in allocations {
                owned(request, &allocation.key)?;
            }
            if let Some(old) = &pages[*index as usize] {
                if old != allocations {
                    return Err(rejected("conflict", "snapshot page changed"));
                }
            }
            pages[*index as usize] = Some(allocations.clone());
            if pages
                .iter()
                .filter_map(|p| p.as_ref())
                .map(Vec::len)
                .sum::<usize>()
                > crate::machine::MAX_DOMAIN_CANDIDATES
                || serde_json::to_vec(&pages)?.len() > crate::machine::MAX_RECONCILE_BYTES
            {
                return Err(rejected(
                    "limit_exceeded",
                    "reconciliation inventory exceeds durable count/byte bounds",
                ));
            }
            tx.execute(
                "UPDATE machine_reconciles SET pages_json=?2 WHERE domain_id=?1",
                params![
                    request.session.domain_id.0.to_string(),
                    serde_json::to_string(&pages)?
                ],
            )?;
            Ok(Outcome::Accepted {
                revision: u64::from(*index),
            })
        }
        Command::ReconcileCommit { snapshot_id } => {
            let row: Option<(String,String)> = tx.query_row("SELECT manifest_json,pages_json FROM machine_reconciles WHERE domain_id=?1 AND snapshot_id=?2", params![request.session.domain_id.0.to_string(),snapshot_id.to_string()], |row| Ok((row.get(0)?,row.get(1)?))).optional()?;
            let (manifest, pages) = row.ok_or_else(|| rejected("stale", "snapshot not begun"))?;
            let Command::ReconcileBegin {
                end_sequence,
                digest,
                ..
            } = serde_json::from_str(&manifest)?
            else {
                return Err(StoreError::InvalidState(
                    "invalid reconciliation manifest".into(),
                ));
            };
            let pages: Vec<Option<Vec<crate::machine::ReconcileAllocation>>> =
                serde_json::from_str(&pages)?;
            let pages = pages
                .into_iter()
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| rejected("history_unknown", "snapshot has missing pages"))?;
            if crate::machine::payload_hash(&pages)? != digest {
                return Err(rejected("history_unknown", "snapshot digest mismatch"));
            }
            let mut inventory = std::collections::BTreeMap::new();
            for allocation in pages.into_iter().flatten() {
                if inventory
                    .insert(key_string(&allocation.key)?, allocation)
                    .is_some()
                {
                    return Err(rejected("conflict", "duplicate allocation in snapshot"));
                }
            }
            let mut stmt = tx.prepare("SELECT allocation_key,snapshot_json FROM machine_grants JOIN machine_candidates USING(allocation_key) WHERE domain_id=?1 AND machine_grants.state IN ('offered','armed','uncertain')")?;
            let expected = stmt
                .query_map([request.session.domain_id.0.to_string()], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            for (key, grant) in expected {
                let grant: GrantSnapshot = serde_json::from_str(&grant)?;
                let recorded = inventory.get(&key).ok_or_else(|| {
                    rejected(
                        "history_unknown",
                        "snapshot omitted an outstanding allocation",
                    )
                })?;
                if recorded.offer_nonce != grant.offer_nonce || recorded.tickets != grant.tickets {
                    return Err(rejected(
                        "history_unknown",
                        "snapshot does not cover all issued start rights",
                    ));
                }
            }
            let mut released = Vec::new();
            for (key, record) in inventory {
                let known: bool = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM machine_grants WHERE allocation_key=?1)",
                    [&key],
                    |row| row.get(0),
                )?;
                if !known {
                    if !record.offer_nonce.is_nil()
                        || !record.tickets.is_empty()
                        || record.sealed_release.is_some()
                    {
                        return Err(rejected(
                            "history_unknown",
                            "manager has start rights absent from coordinator history",
                        ));
                    }
                    continue;
                }
                let grant = load_grant(tx, &record.key)?;
                if record.offer_nonce != grant.offer_nonce || record.tickets != grant.tickets {
                    return Err(rejected(
                        "history_unknown",
                        "snapshot changed a retained allocation identity or ticket inventory",
                    ));
                }
                if let Some(seal) = &record.sealed_release {
                    if seal.key != record.key || seal.offer_nonce != record.offer_nonce {
                        return Err(rejected(
                            "conflict",
                            "snapshot seal belongs to another allocation",
                        ));
                    }
                    apply_release(tx, request, seal, now)?;
                    released.push(record.key);
                }
            }
            tx.execute(
                "UPDATE machine_domains SET reconciliation_required=0 WHERE domain_id=?1",
                [request.session.domain_id.0.to_string()],
            )?;
            tx.execute(
                "DELETE FROM machine_reconciles WHERE domain_id=?1",
                [request.session.domain_id.0.to_string()],
            )?;
            Ok(Outcome::Reconciled {
                end_sequence,
                released,
            })
        }
    }
}

pub(super) fn compact_operations(
    connection: &Connection,
    domain: crate::ExecutionDomainId,
    floor: u64,
) -> StoreResult<()> {
    connection.execute(
        "DELETE FROM machine_operations WHERE domain_id=?1 AND sequence<=?2",
        params![domain.0.to_string(), floor],
    )?;
    connection.execute("UPDATE machine_domains SET retired_sequence_floor=MAX(retired_sequence_floor,?2) WHERE domain_id=?1",params![domain.0.to_string(),floor])?;
    Ok(())
}

fn active_candidate_counts(
    c: &Connection,
    domain: crate::ExecutionDomainId,
    except: &str,
    now: i64,
) -> StoreResult<(u64, u64)> {
    Ok(c.query_row("SELECT COALESCE(SUM(c.domain_id=?1),0),COUNT(*) FROM machine_candidates c LEFT JOIN machine_grants g USING(allocation_key) WHERE c.allocation_key!=?2 AND ((c.state='ready' AND c.expires_ms>?3) OR g.state IN ('offered','armed','uncertain'))",params![domain.0.to_string(),except,now],|r|Ok((r.get(0)?,r.get(1)?)))?)
}

fn inventory_page(
    connection: &Connection,
    domain: crate::ExecutionDomainId,
    exact: Option<&AllocationKey>,
    after: Option<&AllocationKey>,
    limit: u32,
    outstanding_only: bool,
) -> StoreResult<(Vec<GrantSnapshot>, bool)> {
    if limit == 0 || limit > 256 {
        return Err(rejected(
            "limit_exceeded",
            "inventory page size must be 1..256",
        ));
    }
    let exact = exact.map(key_string).transpose()?;
    let after = after.map(key_string).transpose()?;
    let mut statement = connection.prepare("SELECT g.snapshot_json FROM machine_grants g JOIN machine_candidates c USING(allocation_key) WHERE c.domain_id=?1 AND (?2 IS NULL OR allocation_key=?2) AND (?3 IS NULL OR allocation_key>?3) AND (?4=0 OR g.state IN ('offered','armed','uncertain')) ORDER BY allocation_key LIMIT ?5")?;
    let mut rows = statement.query(params![
        domain.0.to_string(),
        exact,
        after,
        outstanding_only,
        limit + 1
    ])?;
    let mut grants = Vec::new();
    let mut bytes = 0;
    let mut truncated = false;
    while let Some(row) = rows.next()? {
        let json: String = row.get(0)?;
        if grants.len() == limit as usize
            || bytes + json.len() + 1 > crate::machine::MAX_FRAME_BYTES - 4096
        {
            if grants.is_empty() {
                return Err(rejected(
                    "limit_exceeded",
                    "single allocation exceeds the inventory page byte budget",
                ));
            }
            truncated = true;
            break;
        }
        bytes += json.len() + 1;
        grants.push(serde_json::from_str::<GrantSnapshot>(&json)?);
    }
    Ok((grants, truncated))
}

#[cfg(test)]
mod inventory_tests {
    use super::*;

    #[test]
    fn expired_candidate_history_does_not_exhaust_ready_budget_but_armed_still_counts() {
        let c = Connection::open_in_memory().unwrap();
        c.execute_batch("CREATE TABLE machine_candidates(allocation_key TEXT PRIMARY KEY,domain_id TEXT,state TEXT,expires_ms INTEGER); CREATE TABLE machine_grants(allocation_key TEXT PRIMARY KEY,state TEXT);").unwrap();
        let domain = crate::ExecutionDomainId(Uuid::now_v7());
        for i in 0..crate::machine::MAX_DOMAIN_CANDIDATES {
            c.execute(
                "INSERT INTO machine_candidates VALUES (?1,?2,'ready',1)",
                params![i.to_string(), domain.0.to_string()],
            )
            .unwrap();
        }
        assert_eq!(
            active_candidate_counts(&c, domain, "new", 100).unwrap(),
            (0, 0)
        );
        c.execute("INSERT INTO machine_grants VALUES ('0','armed')", [])
            .unwrap();
        c.execute(
            "UPDATE machine_candidates SET state='withdrawn' WHERE allocation_key='0'",
            [],
        )
        .unwrap();
        assert_eq!(
            active_candidate_counts(&c, domain, "new", 100).unwrap(),
            (1, 1)
        );
        c.execute(
            "UPDATE machine_candidates SET expires_ms=200 WHERE allocation_key='1'",
            [],
        )
        .unwrap();
        assert_eq!(
            active_candidate_counts(&c, domain, "new", 100).unwrap(),
            (2, 2)
        );
        assert_eq!(
            active_candidate_counts(&c, domain, "1", 100).unwrap(),
            (1, 1)
        );
    }

    #[test]
    fn inventory_pages_cover_large_unicode_records_without_exceeding_wire_bytes() {
        use crate::machine::*;
        let c = Connection::open_in_memory().unwrap();
        c.execute_batch("CREATE TABLE machine_candidates(allocation_key TEXT PRIMARY KEY,domain_id TEXT); CREATE TABLE machine_grants(allocation_key TEXT PRIMARY KEY,snapshot_json TEXT,state TEXT);").unwrap();
        let store = Uuid::now_v7();
        let domain = crate::ExecutionDomainId(Uuid::now_v7());
        let session = SessionIdentity {
            machine_id: Uuid::now_v7(),
            authority_epoch: Uuid::now_v7(),
            domain_id: domain,
            manager_store_uuid: store,
            executor_incarnation: Uuid::now_v7(),
            connection_epoch: 1,
        };
        let mut expected = std::collections::BTreeSet::new();
        for _ in 0..300 {
            let key = AllocationKey {
                machine_id: session.machine_id,
                authority_epoch: session.authority_epoch,
                domain_id: domain,
                manager_store_uuid: store,
                lease_id: Uuid::now_v7(),
            };
            let grant = GrantSnapshot {
                risk_clearance: None,
                uncertainty_reason: None,
                queue_accepted_unix_millis: 1,
                queue_sequence: 1,
                grant_id: format!("{store}~{}", Uuid::now_v7()).parse().unwrap(),
                candidate: Candidate {
                    key: key.clone(),
                    owner: AllocationOwner::Work {
                        job_id: format!("{store}~{}", Uuid::now_v7()).parse().unwrap(),
                        attempt_id: format!("{store}~{}", Uuid::now_v7()).parse().unwrap(),
                    },
                    revision: 1,
                    priority: 0,
                    claims: Claims {
                        scalars: [("я".repeat(6000), 1)].into(),
                        ..Claims::default()
                    },
                    configuration_sha256: "a".repeat(64),
                    observed: None,
                    quiet: None,
                },
                offer_nonce: Uuid::now_v7(),
                state: GrantState::Armed,
                offered_unix_millis: 1,
                offer_deadline_unix_millis: 2,
                armed_unix_millis: Some(1),
                released_unix_millis: None,
                tickets: vec![],
                sealed_release: None,
            };
            let encoded = key_string(&key).unwrap();
            expected.insert(encoded.clone());
            c.execute(
                "INSERT INTO machine_candidates VALUES (?1,?2)",
                params![encoded, domain.0.to_string()],
            )
            .unwrap();
            c.execute(
                "INSERT INTO machine_grants VALUES (?1,?2,'armed')",
                params![encoded, serde_json::to_string(&grant).unwrap()],
            )
            .unwrap();
        }
        let mut after = None;
        let mut actual = std::collections::BTreeSet::new();
        let mut page_count = 0;
        loop {
            let (grants, more) =
                inventory_page(&c, domain, None, after.as_ref(), 256, true).unwrap();
            assert!(!grants.is_empty());
            assert!(
                grants.len() < 256,
                "byte limit must bind before count limit"
            );
            for grant in &grants {
                assert!(actual.insert(key_string(&grant.candidate.key).unwrap()));
            }
            after = more.then(|| grants.last().unwrap().candidate.key.clone());
            let reply = Reply {
                session: session.clone(),
                request_sequence: 1,
                operation_id: Uuid::now_v7(),
                coordinator_revision: u64::MAX,
                outcome: Outcome::InventoryPage {
                    grants,
                    next: after.clone(),
                    configuration_sha256: "a".repeat(64),
                },
            };
            write_frame(&mut std::io::sink(), &reply).unwrap();
            page_count += 1;
            if !more {
                break;
            }
        }
        assert_eq!(actual, expected);
        assert!(page_count > 2);
        assert!(inventory_page(&c, domain, None, None, 0, true).is_err());
        assert!(inventory_page(&c, domain, None, None, 257, true).is_err());
    }
}
