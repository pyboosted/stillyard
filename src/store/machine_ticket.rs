use super::machine::rejected;
use super::machine_allocation::{is_digest, key_string};
use super::*;
use crate::machine::{
    AllocationOwner, GrantSnapshot, InvocationIntent, InvocationTicket, Outcome, Request,
    TicketCleanup,
};

pub(super) struct Readiness<'a> {
    pub(super) config: &'a HostConfig,
    pub(super) sample: Option<&'a crate::host_observation::HostSample>,
}

#[derive(Default, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct QuietProgress {
    consumed: u64,
    generation: Option<String>,
    evaluated: Option<u64>,
    first: Option<u64>,
    last: Option<u64>,
}

pub(super) fn cleanup_matches(intent: &InvocationIntent, proof: &TicketCleanup) -> bool {
    proof.invocation_id == intent.invocation_id
        && proof.release_sequence == intent.release_sequence
        && proof.boundary_sha256 == intent.boundary_sha256
        && is_digest(&proof.proof_sha256)
}

fn validate_next(grant: &GrantSnapshot, intent: &InvocationIntent) -> StoreResult<()> {
    let store = grant.candidate.key.manager_store_uuid;
    if intent.invocation_id.store_uuid() != store
        || intent.containment_id.store_uuid() != store
        || intent.invocation_id.entity_uuid().is_nil()
        || intent.containment_id.entity_uuid().is_nil()
        || intent.readiness_challenge.is_nil()
        || !is_digest(&intent.executable_sha256)
        || !is_digest(&intent.boundary_sha256)
        || intent.release_sequence == 0
        || intent.release_sequence > i64::MAX as u64
    {
        return Err(rejected(
            "invalid_spec",
            "Invocation identity, challenge or boundary is invalid",
        ));
    }
    if grant.tickets.len() >= 256 {
        return Err(rejected(
            "limit_exceeded",
            "allocation ticket history exhausted",
        ));
    }
    if grant.tickets.iter().any(|t| {
        t.invocation_id == intent.invocation_id
            || t.containment_id == intent.containment_id
            || t.readiness_challenge == intent.readiness_challenge
    }) {
        return Err(rejected(
            "conflict",
            "ticket identity is single-use; replay the original operation",
        ));
    }
    let previous = grant.tickets.last();
    match (previous, &intent.previous_cleanup) {
        (None, None) if intent.release_sequence == 1 => {}
        (Some(last), Some(proof))
            if cleanup_matches(last, proof)
                && last.release_sequence.checked_add(1) == Some(intent.release_sequence) => {}
        _ => {
            return Err(rejected(
                "cleanup_incomplete",
                "next Invocation requires the exact predecessor cleanup and release sequence",
            ));
        }
    }
    let valid_role = match &grant.candidate.owner {
        AllocationOwner::Probe { invocation_id, .. } => {
            previous.is_none()
                && intent.role == InvocationRole::Probe
                && intent.role_index == 0
                && intent.invocation_id == *invocation_id
        }
        AllocationOwner::Work { .. } => match previous {
            None => intent.role == InvocationRole::Primary && intent.role_index == 0,
            Some(last) => {
                let released = intent
                    .previous_cleanup
                    .as_ref()
                    .is_some_and(|p| p.user_code_released);
                match (last.role, intent.role, released) {
                    (InvocationRole::Primary, InvocationRole::Primary, false) => {
                        intent.role_index == 0
                    }
                    (InvocationRole::Primary, InvocationRole::Postcondition, true) => {
                        intent.role_index == 0
                    }
                    (InvocationRole::Postcondition, InvocationRole::Postcondition, false) => {
                        intent.role_index == last.role_index
                    }
                    (InvocationRole::Postcondition, InvocationRole::Postcondition, true) => {
                        last.role_index.checked_add(1) == Some(intent.role_index)
                    }
                    _ => false,
                }
            }
        },
    };
    if !valid_role {
        return Err(rejected(
            "conflict",
            "Invocation role/order is incompatible with its allocation owner",
        ));
    }
    Ok(())
}

pub(super) fn authorize(
    tx: &Transaction<'_>,
    request: &Request,
    mut grant: GrantSnapshot,
    intent: &InvocationIntent,
    readiness: &Readiness<'_>,
) -> StoreResult<Outcome> {
    validate_next(&grant, intent)?;
    let key = key_string(&grant.candidate.key)?;
    let state: String = tx.query_row(
        "SELECT state FROM machine_candidates WHERE allocation_key=?1",
        [&key],
        |r| r.get(0),
    )?;
    if state != "ready" {
        return Err(rejected(
            "stale",
            "withdrawn or canceled allocation cannot issue new start rights",
        ));
    }
    let sample = readiness.sample.ok_or_else(|| {
        rejected(
            "unavailable",
            "Invocation needs a fresh host observation barrier",
        )
    })?;
    let (unix, monotonic) = crate::host_observation::observation_clock()?;
    if monotonic
        .checked_sub(sample.captured_monotonic_millis)
        .is_none_or(|age| age > 250)
    {
        return Err(rejected(
            "unavailable",
            "host sample expired before ticket commit",
        ));
    }
    let physical: String = tx.query_row(
        "SELECT physical_claims_json FROM machine_grants WHERE allocation_key=?1",
        [&key],
        |r| r.get(0),
    )?;
    let physical: ResolvedClaims = serde_json::from_str(&physical)?;
    let mut active = super::machine_queue::native_debits(tx)?;
    let mut statement = tx.prepare("SELECT physical_claims_json FROM machine_grants WHERE allocation_key!=?1 AND state IN ('offered','armed','uncertain')")?;
    for row in statement.query_map([&key], |r| r.get::<_, String>(0))? {
        active.push(serde_json::from_str(&row?)?);
    }
    let resources = crate::ResourceClaims {
        cpu_units: u32::try_from(physical.cpu_units).ok().filter(|v| *v > 0),
        ram_mb: (physical.ram_mb > 0).then_some(physical.ram_mb),
        cargo_slots: u32::try_from(physical.cargo_slots).ok().filter(|v| *v > 0),
        gpu_slots: u32::try_from(physical.gpu_slots).ok().filter(|v| *v > 0),
        custom: physical.custom.clone(),
        shared_fences: vec![],
        exclusive_fences: vec![],
        impacts: physical.impacts.iter().cloned().collect(),
    };
    if physical.cpu_units > u64::from(u32::MAX)
        || physical.cargo_slots > u64::from(u32::MAX)
        || physical.gpu_slots > u64::from(u32::MAX)
    {
        return Err(rejected(
            "invalid_spec",
            "built-in claims exceed supported quantities",
        ));
    }
    let context = crate::host_observation::evaluate_request(
        &crate::host_observation::ObservationRequest {
            resources: &resources,
            observed: &grant.candidate.observed,
            quiet: &grant.candidate.quiet,
        },
        readiness.config,
        sample,
        &active,
        unix,
        monotonic,
    );
    let prior: Option<String> = tx
        .query_row(
            "SELECT progress_json FROM machine_readiness WHERE allocation_key=?1",
            [&key],
            |r| r.get(0),
        )
        .optional()?;
    let mut progress: QuietProgress = prior
        .map(|p| serde_json::from_str(&p))
        .transpose()?
        .unwrap_or_default();
    let resource_blockers = physical.blockers(
        &readiness.config.resources,
        &active,
        &readiness.config.impact_incompatibilities,
    );
    let mut waiting = resource_blockers
        .first()
        .or(context.non_quiet_blockers.first())
        .cloned();
    if waiting.is_some() {
        progress.generation = None;
        progress.evaluated = None;
        progress.first = None;
        progress.last = None;
    } else if let Some(quiet) = &grant.candidate.quiet {
        progress.consumed = crate::host_observation::quiet_budget(
            progress.consumed,
            progress.generation.as_deref(),
            progress.evaluated,
            &context,
        );
        (progress.first, progress.last) = crate::host_observation::quiet_stability(
            progress.generation.as_deref(),
            progress.first,
            progress.last,
            &context,
            readiness.config.observation.quiet_max_sample_gap_millis,
        );
        progress.generation = Some(context.observation_generation.to_string());
        progress.evaluated = Some(monotonic);
        if progress.consumed >= quiet.wait_budget_seconds.saturating_mul(1000) {
            waiting = Some(Blocker {
                code: "quiet_unattainable".into(),
                detail: "allocation exhausted its cumulative quiet wait budget".into(),
            });
        } else if progress
            .first
            .and_then(|first| monotonic.checked_sub(first))
            .is_none_or(|elapsed| elapsed < quiet.stable_seconds.saturating_mul(1000))
        {
            waiting = Some(context.quiet_blockers.first().cloned().unwrap_or(Blocker {
                code: "quiet_waiting".into(),
                detail: "host quiet interval is not yet stable".into(),
            }));
        }
    }
    tx.execute("INSERT INTO machine_readiness VALUES (?1,?2) ON CONFLICT(allocation_key) DO UPDATE SET progress_json=excluded.progress_json",params![key,serde_json::to_string(&progress)?])?;
    if let Some(blocker) = waiting {
        // Waiting is a durable result: keep cumulative progress rather than
        // rolling it back with the command rejection savepoint.
        return Ok(Outcome::Rejected {
            code: blocker.code,
            detail: blocker.detail,
        });
    }
    grant.tickets.push(intent.clone());
    super::machine_queue::save_grant(tx, &grant, None)?;
    Ok(Outcome::Ticket {
        ticket: Box::new(InvocationTicket {
            grant_id: grant.grant_id,
            key: grant.candidate.key,
            offer_nonce: grant.offer_nonce,
            intent: intent.clone(),
            configuration_sha256: grant.candidate.configuration_sha256,
            issued_unix_millis: unix,
            host_observation_generation: sample.observation_generation,
            host_sample_unix_millis: sample.captured_unix_millis,
            session: request.session.clone(),
        }),
    })
}
