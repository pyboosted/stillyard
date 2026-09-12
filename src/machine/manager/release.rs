//! Local cancellation/freshness barrier. Hold it from the last readiness check
//! through the durable start commit and actual platform release.
use super::*;
use std::sync::Mutex;

pub struct ReleaseBarrier {
    state: Mutex<State>,
}
struct State {
    session: SessionIdentity,
    generation: Uuid,
    configuration: String,
    canceled: bool,
    challenge: Option<Challenge>,
    attempted: bool,
}
struct Challenge {
    operation: Uuid,
    intent: InvocationIntent,
    key: AllocationKey,
    nonce: Uuid,
    clock: (i64, u64),
}
/// Produced only when this barrier committed consumption but did not call the
/// OS release function. Platform whole-boundary cleanup is still required.
#[derive(Debug)]
pub struct NeverReleased {
    ticket: InvocationTicket,
}
#[derive(Debug)]
pub enum Disposition {
    Released,
    AlreadyConsumed,
    NeverReleased(Box<NeverReleased>),
    Uncertain(std::io::Error),
}

impl ReleaseBarrier {
    pub fn new(session: SessionIdentity, generation: Uuid, configuration: String) -> Result<Self> {
        if generation.is_nil()
            || configuration.len() != 64
            || !configuration.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(invalid(
                "release barrier needs current provider and configuration identities",
            ));
        }
        Ok(Self {
            state: Mutex::new(State {
                session,
                generation,
                configuration,
                canceled: false,
                challenge: None,
                attempted: false,
            }),
        })
    }

    /// Invoke before the first transmission. Retry keeps the ORIGINAL clock;
    /// reconnect needs a new Invocation challenge, never a rejuvenated ticket.
    pub fn challenge(&self, request: &Request) -> Result<()> {
        self.challenge_at(request, crate::host_observation::observation_clock()?)
    }
    fn challenge_at(&self, request: &Request, clock: (i64, u64)) -> Result<()> {
        request.validate()?;
        let Command::AuthorizeInvocation {
            key,
            offer_nonce,
            intent,
        } = &request.command
        else {
            return Err(invalid("release challenge requires an Invocation request"));
        };
        let mut state = self
            .state
            .lock()
            .map_err(|_| invalid("release barrier poisoned"))?;
        if request.session != state.session || state.canceled || state.attempted {
            return Err(invalid("release barrier was fenced"));
        }
        if let Some(old) = &state.challenge {
            if old.operation != request.operation_id
                || old.intent != *intent
                || old.key != *key
                || old.nonce != *offer_nonce
            {
                return Err(invalid(
                    "one release barrier cannot authorize another Invocation",
                ));
            }
            return Ok(());
        }
        state.challenge = Some(Challenge {
            operation: request.operation_id,
            intent: intent.clone(),
            key: key.clone(),
            nonce: *offer_nonce,
            clock,
        });
        Ok(())
    }

    /// Serialize known disconnect, cancel and provider/session invalidation with
    /// actual release. A canceled barrier is never reopened by reconnect.
    pub fn cancel(&self) -> Result<()> {
        self.state
            .lock()
            .map_err(|_| invalid("release barrier poisoned"))?
            .canceled = true;
        Ok(())
    }

    /// `commit` atomically consumes the ticket with Invocation lifecycle and
    /// commits SQLite. `local_ready` runs twice under this barrier. No callback
    /// may re-enter the barrier. An OS release error retains uncertainty.
    pub fn release(
        &self,
        ticket: &InvocationTicket,
        generation: Uuid,
        local_ready: impl FnMut() -> Result<bool>,
        commit: impl FnOnce() -> Result<bool>,
        release: impl FnOnce() -> std::io::Result<()>,
    ) -> Result<Disposition> {
        self.release_with_clock(
            ticket,
            generation,
            local_ready,
            commit,
            release,
            crate::host_observation::observation_clock,
        )
    }
    fn release_with_clock(
        &self,
        ticket: &InvocationTicket,
        generation: Uuid,
        mut local_ready: impl FnMut() -> Result<bool>,
        commit: impl FnOnce() -> Result<bool>,
        release: impl FnOnce() -> std::io::Result<()>,
        mut clock: impl FnMut() -> std::io::Result<(i64, u64)>,
    ) -> Result<Disposition> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| invalid("release barrier poisoned"))?;
        let challenge = state
            .challenge
            .as_ref()
            .ok_or_else(|| invalid("ticket has no local send challenge"))?;
        if state.canceled
            || state.attempted
            || state.generation != generation
            || state.session != ticket.session
            || state.configuration != ticket.configuration_sha256
            || challenge.intent != ticket.intent
            || challenge.key != ticket.key
            || challenge.nonce != ticket.offer_nonce
            || !fresh(challenge.clock, clock()?)
            || !local_ready()?
        {
            return Err(invalid(
                "ticket is stale, canceled, used or locally unready",
            ));
        }
        let sent = challenge.clock;
        state.attempted = true;
        if !commit()? {
            return Ok(Disposition::AlreadyConsumed);
        }
        // Once committed, any pre-release failure proves only that THIS barrier
        // did not resume the child. Persist cleanup before retiring the ticket.
        if !local_ready().unwrap_or(false) || !clock().is_ok_and(|now| fresh(sent, now)) {
            return Ok(Disposition::NeverReleased(Box::new(NeverReleased {
                ticket: ticket.clone(),
            })));
        }
        Ok(match release() {
            Ok(()) => Disposition::Released,
            Err(error) => Disposition::Uncertain(error),
        })
    }
}

fn fresh(sent: (i64, u64), now: (i64, u64)) -> bool {
    let Some(boot) = now.1.checked_sub(sent.1) else {
        return false;
    };
    let Some(wall) = now
        .0
        .checked_sub(sent.0)
        .and_then(|n| u64::try_from(n).ok())
    else {
        return false;
    };
    boot <= 250 && wall <= 250 && boot.abs_diff(wall) <= 50
}

/// Pair the barrier's no-release result with a platform whole-boundary proof.
/// Losing this transient result is conservative: recover consumed as possibly
/// released. It never gives permission to consume that same ticket again.
pub fn record_unused_cleanup(
    tx: &Transaction<'_>,
    proof: &NeverReleased,
    cleanup: &TicketCleanup,
) -> Result<()> {
    let (json, consumed, prior): (String, bool, Option<String>) = tx.query_row(
        "SELECT ticket_json,consumed,cleanup_json FROM attached_tickets WHERE invocation_id=?1",
        [cleanup.invocation_id.to_string()],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )?;
    let ticket = &proof.ticket;
    let encoded = serde_json::to_string(cleanup)?;
    if json != serde_json::to_string(ticket)?
        || !consumed
        || cleanup.user_code_released
        || cleanup.invocation_id != ticket.intent.invocation_id
        || cleanup.release_sequence != ticket.intent.release_sequence
        || cleanup.boundary_sha256 != ticket.intent.boundary_sha256
        || cleanup.proof_sha256.len() != 64
        || !cleanup.proof_sha256.bytes().all(|b| b.is_ascii_hexdigit())
        || prior.is_some_and(|p| p != encoded)
    {
        return Err(invalid(
            "unused cleanup does not match the committed never-released ticket",
        ));
    }
    tx.execute(
        "UPDATE attached_tickets SET cleanup_json=?2 WHERE invocation_id=?1",
        params![cleanup.invocation_id.to_string(), encoded],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> (ReleaseBarrier, Request, InvocationTicket, Uuid) {
        let store = Uuid::now_v7();
        let session = SessionIdentity {
            machine_id: Uuid::now_v7(),
            authority_epoch: Uuid::now_v7(),
            domain_id: crate::ExecutionDomainId(Uuid::now_v7()),
            manager_store_uuid: store,
            executor_incarnation: Uuid::now_v7(),
            connection_epoch: 1,
        };
        let key = AllocationKey {
            machine_id: session.machine_id,
            authority_epoch: session.authority_epoch,
            domain_id: session.domain_id,
            manager_store_uuid: store,
            lease_id: Uuid::now_v7(),
        };
        let intent = InvocationIntent {
            invocation_id: format!("{store}~{}", Uuid::now_v7()).parse().unwrap(),
            containment_id: format!("{store}~{}", Uuid::now_v7()).parse().unwrap(),
            role: crate::InvocationRole::Primary,
            role_index: 0,
            release_sequence: 1,
            executable_sha256: "a".repeat(64),
            boundary_sha256: "b".repeat(64),
            readiness_challenge: Uuid::now_v7(),
            previous_cleanup: None,
        };
        let nonce = Uuid::now_v7();
        let request = Request::new(
            session.clone(),
            1,
            Uuid::now_v7(),
            Command::AuthorizeInvocation {
                key: key.clone(),
                offer_nonce: nonce,
                intent: intent.clone(),
            },
        )
        .unwrap();
        let ticket = InvocationTicket {
            grant_id: format!("{store}~{}", Uuid::now_v7()).parse().unwrap(),
            key,
            offer_nonce: nonce,
            intent,
            configuration_sha256: "c".repeat(64),
            issued_unix_millis: 9,
            host_observation_generation: Uuid::now_v7(),
            host_sample_unix_millis: 9,
            session: session.clone(),
        };
        let generation = Uuid::now_v7();
        (
            ReleaseBarrier::new(session, generation, "c".repeat(64)).unwrap(),
            request,
            ticket,
            generation,
        )
    }

    #[test]
    fn retry_cannot_rejuvenate_a_delayed_ticket_and_cancel_fences_release() {
        let (barrier, request, ticket, generation) = fixture();
        barrier.challenge_at(&request, (1000, 1000)).unwrap();
        barrier.challenge_at(&request, (1200, 1200)).unwrap();
        assert!(
            barrier
                .release_with_clock(
                    &ticket,
                    generation,
                    || Ok(true),
                    || panic!("stale ticket committed"),
                    || panic!("stale ticket released"),
                    || Ok((1251, 1251))
                )
                .is_err()
        );
        barrier.cancel().unwrap();
        assert!(
            barrier
                .release_with_clock(
                    &ticket,
                    generation,
                    || Ok(true),
                    || panic!("canceled ticket committed"),
                    || panic!("canceled ticket released"),
                    || Ok((1100, 1100))
                )
                .is_err()
        );
    }

    #[test]
    fn loss_of_readiness_after_commit_proves_no_release_without_replaying_consumption() {
        let (barrier, request, ticket, generation) = fixture();
        barrier.challenge_at(&request, (1000, 1000)).unwrap();
        let mut checks = 0;
        let result = barrier
            .release_with_clock(
                &ticket,
                generation,
                || {
                    checks += 1;
                    Ok(checks == 1)
                },
                || Ok(true),
                || panic!("unready child released"),
                || Ok((1100, 1100)),
            )
            .unwrap();
        assert!(matches!(result, Disposition::NeverReleased(_)));
        assert!(
            barrier
                .release_with_clock(
                    &ticket,
                    generation,
                    || Ok(true),
                    || panic!("consumption replayed"),
                    || panic!("release replayed"),
                    || Ok((1100, 1100))
                )
                .is_err()
        );
    }

    #[test]
    fn delay_in_final_local_check_is_rechecked_before_os_release() {
        let (barrier, request, ticket, generation) = fixture();
        barrier.challenge_at(&request, (1000, 1000)).unwrap();
        let mut clocks = 0;
        let result = barrier
            .release_with_clock(
                &ticket,
                generation,
                || Ok(true),
                || Ok(true),
                || panic!("expired final barrier released"),
                || {
                    clocks += 1;
                    Ok(if clocks == 1 {
                        (1100, 1100)
                    } else {
                        (1300, 1300)
                    })
                },
            )
            .unwrap();
        assert!(matches!(result, Disposition::NeverReleased(_)));
    }
    #[test]
    fn local_send_age_detects_delay_suspend_and_clock_discontinuity() {
        assert!(fresh((1000, 1000), (1250, 1250)));
        for now in [
            (1251, 1251),
            (999, 1000),
            (1000, 999),
            (1250, 1000),
            (5000, 1001),
        ] {
            assert!(!fresh((1000, 1000), now));
        }
    }

    #[test]
    fn unused_after_commit_keeps_consumption_and_requires_exact_cleanup_before_sealing() {
        let (barrier, request, ticket, generation) = fixture();
        let mut c = Connection::open_in_memory().unwrap();
        let tx = c.transaction().unwrap();
        initialize(&tx, ticket.key.manager_store_uuid).unwrap();
        tx.execute(
            "UPDATE attached_peer SET session_json=?1",
            [serde_json::to_string(&ticket.session).unwrap()],
        )
        .unwrap();
        let store = ticket.key.manager_store_uuid;
        let grant = GrantSnapshot {
            risk_clearance: None,
            queue_accepted_unix_millis: 1,
            queue_sequence: 1,
            uncertainty_reason: None,
            grant_id: ticket.grant_id,
            candidate: Candidate {
                key: ticket.key.clone(),
                owner: AllocationOwner::Work {
                    job_id: format!("{store}~{}", Uuid::now_v7()).parse().unwrap(),
                    attempt_id: format!("{store}~{}", Uuid::now_v7()).parse().unwrap(),
                },
                revision: 1,
                priority: 0,
                claims: Claims::default(),
                configuration_sha256: ticket.configuration_sha256.clone(),
                observed: None,
                quiet: None,
            },
            offer_nonce: ticket.offer_nonce,
            state: GrantState::Armed,
            offered_unix_millis: 1,
            offer_deadline_unix_millis: 2,
            armed_unix_millis: Some(1),
            released_unix_millis: None,
            tickets: vec![ticket.intent.clone()],
            sealed_release: None,
        };
        let key = serde_json::to_string(&ticket.key).unwrap();
        tx.execute(
            "INSERT INTO attached_grants VALUES (?1,?2,NULL)",
            params![key, serde_json::to_string(&grant).unwrap()],
        )
        .unwrap();
        tx.execute(
            "INSERT INTO attached_tickets VALUES (?1,?2,?3,0,NULL)",
            params![
                ticket.intent.invocation_id.to_string(),
                key,
                serde_json::to_string(&ticket).unwrap()
            ],
        )
        .unwrap();
        tx.commit().unwrap();
        barrier.challenge_at(&request, (1000, 1000)).unwrap();
        let mut checks = 0;
        let result = barrier
            .release_with_clock(
                &ticket,
                generation,
                || {
                    checks += 1;
                    Ok(checks == 1)
                },
                || {
                    let tx = c.transaction()?;
                    let consumed = consume_ticket(&tx, &ticket)?;
                    tx.commit()?;
                    Ok(consumed)
                },
                || panic!("unready child was resumed"),
                || Ok((1100, 1100)),
            )
            .unwrap();
        let Disposition::NeverReleased(proof) = result else {
            panic!("expected pre-release failure after commit");
        };
        let tx = c.transaction().unwrap();
        assert!(seal_release(&tx, &ticket.key, Uuid::now_v7()).is_err());
        tx.rollback().unwrap();
        let cleanup = TicketCleanup {
            invocation_id: ticket.intent.invocation_id,
            release_sequence: ticket.intent.release_sequence,
            boundary_sha256: ticket.intent.boundary_sha256.clone(),
            proof_sha256: "d".repeat(64),
            user_code_released: false,
        };
        let tx = c.transaction().unwrap();
        assert!(
            record_cleanup(&tx, &cleanup).is_err(),
            "untyped cleanup erased consumption"
        );
        let mut wrong = cleanup.clone();
        wrong.boundary_sha256 = "e".repeat(64);
        assert!(record_unused_cleanup(&tx, &proof, &wrong).is_err());
        record_unused_cleanup(&tx, &proof, &cleanup).unwrap();
        record_unused_cleanup(&tx, &proof, &cleanup).unwrap();
        assert!(consume_ticket(&tx, &ticket).is_err());
        seal_release(&tx, &ticket.key, Uuid::now_v7()).unwrap();
        tx.commit().unwrap();
        assert_eq!(
            c.query_row("SELECT consumed FROM attached_tickets", [], |r| r
                .get::<_, u64>(0))
                .unwrap(),
            1
        );
        let pending = pending(&c, &PairingSecret::generate().unwrap())
            .unwrap()
            .unwrap();
        assert!(
            matches!(pending.command,Command::Release { release } if release.tickets==vec![cleanup])
        );
    }
}
