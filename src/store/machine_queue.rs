use super::*;
use crate::admission::{
    DomainBudget, ResourceTopology, ScheduleKey, ScopedClaims, effective_priority_at,
    schedule_order,
};
use crate::machine::{Candidate, GrantSnapshot, GrantState};

pub(super) fn native_debits(connection: &Connection) -> StoreResult<Vec<ResolvedClaims>> {
    let mut statement =
        connection.prepare("SELECT claims_json FROM leases WHERE state='granted'")?;
    statement
        .query_map([], |row| row.get::<_, String>(0))?
        .map(|row| Ok(serde_json::from_str(&row?)?))
        .collect()
}

pub(super) fn remote_debits(connection: &Connection) -> StoreResult<Vec<ResolvedClaims>> {
    let mut statement = connection.prepare("SELECT physical_claims_json FROM machine_grants WHERE state IN ('offered','armed','uncertain')")?;
    statement
        .query_map([], |row| row.get::<_, String>(0))?
        .map(|row| Ok(serde_json::from_str(&row?)?))
        .collect()
}

pub(super) fn claims(claims: &crate::machine::Claims) -> ResolvedClaims {
    let mut scalars = claims.scalars.clone();
    ResolvedClaims {
        cpu_units: scalars.remove("cpu_units").unwrap_or(0),
        ram_mb: scalars.remove("ram_mb").unwrap_or(0),
        cargo_slots: scalars.remove("cargo_slots").unwrap_or(0),
        gpu_slots: scalars.remove("gpu_slots").unwrap_or(0),
        custom: scalars,
        shared_fences: claims.shared_fences.iter().cloned().collect(),
        exclusive_fences: claims.exclusive_fences.iter().cloned().collect(),
        impacts: claims.impacts.iter().cloned().collect(),
    }
}

pub(super) fn physical_claims(
    expanded: &ScopedClaims,
    root: crate::ExecutionDomainId,
) -> ResolvedClaims {
    // Ancestors constrain this same physical debit; never sum their entries.
    let scalars = expanded
        .scalars
        .iter()
        .filter(|(key, _)| key.scope == root)
        .map(|(key, value)| (key.resource_id.clone(), *value))
        .collect();
    let mut result = claims(&crate::machine::Claims {
        scalars,
        shared_fences: vec![],
        exclusive_fences: vec![],
        impacts: expanded.impacts.iter().cloned().collect(),
    });
    result.shared_fences = expanded
        .shared_fences
        .iter()
        .map(|id| format!("domain:{}:{}", id.scope.0, id.resource_id))
        .collect();
    result.exclusive_fences = expanded
        .exclusive_fences
        .iter()
        .map(|id| format!("domain:{}:{}", id.scope.0, id.resource_id))
        .collect();
    result
}

pub(super) fn save_grant(
    connection: &Connection,
    grant: &GrantSnapshot,
    physical: Option<&ResolvedClaims>,
) -> StoreResult<()> {
    let key = super::machine_allocation::key_string(&grant.candidate.key)?;
    let state = serde_json::to_value(&grant.state)?
        .as_str()
        .ok_or_else(|| StoreError::InvalidState("invalid Grant state".into()))?
        .to_owned();
    if let Some(physical) = physical {
        connection.execute("INSERT INTO machine_grants VALUES (?1,?2,?3,?4,?5) ON CONFLICT(allocation_key) DO UPDATE SET snapshot_json=excluded.snapshot_json,physical_claims_json=excluded.physical_claims_json,state=excluded.state,deadline_ms=excluded.deadline_ms",
            params![key,serde_json::to_string(grant)?,serde_json::to_string(physical)?,state,grant.offer_deadline_unix_millis])?;
    } else {
        connection.execute("UPDATE machine_grants SET snapshot_json=?2,state=?3,deadline_ms=?4 WHERE allocation_key=?1",params![key,serde_json::to_string(grant)?,state,grant.offer_deadline_unix_millis])?;
    }
    for intent in &grant.tickets {
        let invocation = intent.invocation_id.to_string();
        let containment = intent.containment_id.to_string();
        let json = serde_json::to_string(intent)?;
        connection.execute(
            "INSERT OR IGNORE INTO machine_ticket_identities VALUES (?1,?2,?3,?4)",
            params![invocation, containment, key, json],
        )?;
        let matches: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM machine_ticket_identities WHERE invocation_id=?1 AND containment_id=?2 AND allocation_key=?3 AND intent_json=?4)",params![invocation,containment,key,json],|r|r.get(0))?;
        if !matches {
            return Err(super::machine::rejected(
                "conflict",
                "Invocation or Containment already has another start right",
            ));
        }
    }
    Ok(())
}

impl Store {
    pub(super) fn native_allocations(
        &self,
        job_id: JobId,
    ) -> StoreResult<Vec<crate::machine::NativeAllocationSnapshot>> {
        #[cfg(target_os = "linux")]
        if self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM attached_local_mode)",
            [],
            |r| r.get::<_, bool>(0),
        )? {
            return self.attached_allocations(job_id);
        }
        let authority = self
            .authority
            .as_ref()
            .map(|_| self.authority_snapshot())
            .transpose()?;
        let identity = authority.and_then(|a| a.epoch.zip(a.domains));
        let mut statement = self.connection.prepare("SELECT l.id,l.attempt_id,l.invocation_id,l.state,l.claims_json FROM leases l JOIN attempts a ON a.id=l.attempt_id WHERE a.job_id=?1 ORDER BY l.rowid")?;
        statement
            .query_map([self.local_id(job_id)?], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, Option<String>>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                ))
            })?
            .map(|row| {
                let (lease, attempt, probe, state, json) = row?;
                let lease = Uuid::parse_str(&lease)?;
                let claims: ResolvedClaims = serde_json::from_str(&json)?;
                let owner = match probe {
                    Some(invocation) => crate::machine::AllocationOwner::Probe {
                        job_id,
                        invocation_id: InvocationId::from_parts(
                            self.store_uuid,
                            Uuid::parse_str(&invocation)?,
                        ),
                    },
                    None => crate::machine::AllocationOwner::Work {
                        job_id,
                        attempt_id: AttemptId::from_parts(
                            self.store_uuid,
                            Uuid::parse_str(&attempt)?,
                        ),
                    },
                };
                let state = match state.as_str() {
                    "granted" => GrantState::Armed,
                    "released" => GrantState::Released,
                    _ => {
                        return Err(StoreError::InvalidState(
                            "invalid native allocation state".into(),
                        ));
                    }
                };
                Ok(crate::machine::NativeAllocationSnapshot {
                    grant_id: crate::GrantId::from_parts(self.store_uuid, lease),
                    lease_id: lease,
                    key: identity
                        .as_ref()
                        .map(|(epoch, domains)| crate::machine::AllocationKey {
                            machine_id: domains.machine_id,
                            authority_epoch: *epoch,
                            domain_id: domains.native_domain,
                            manager_store_uuid: self.store_uuid,
                            lease_id: lease,
                        }),
                    owner,
                    state,
                    claims: crate::machine::Claims {
                        scalars: crate::admission::scalar_claim_entries(&claims),
                        shared_fences: claims.shared_fences.into_iter().collect(),
                        exclusive_fences: claims.exclusive_fences.into_iter().collect(),
                        impacts: claims.impacts.into_iter().collect(),
                    },
                })
            })
            .collect()
    }

    pub(super) fn machine_topology(
        &self,
    ) -> StoreResult<Option<(crate::AuthorityDomains, ResourceTopology)>> {
        if self.authority.is_none() {
            return Ok(None);
        }
        let authority = self.authority_snapshot()?;
        let Some(domains) = authority.domains else {
            return Ok(None);
        };
        let anchors = self.authority_lock()?.participants()?;
        if anchors.is_empty() {
            return Ok(None);
        }
        let mut nodes = vec![
            DomainBudget {
                id: domains.machine_scope,
                parent: None,
                capacities: crate::admission::machine_capacities(&self.capacities),
            },
            DomainBudget {
                id: domains.native_domain,
                parent: Some(domains.machine_scope),
                capacities: Default::default(),
            },
        ];
        let mut aliases = std::collections::BTreeMap::new();
        for anchor in anchors {
            let r = anchor.registration;
            nodes.push(DomainBudget {
                id: r.installation.domain_id,
                parent: Some(r.parent_domain),
                capacities: r.budgets,
            });
            aliases.extend(
                r.aliases
                    .into_iter()
                    .map(|(a, p)| ((r.installation.domain_id, a), p)),
            );
        }
        let topology = ResourceTopology::new(domains.machine_scope, nodes, aliases)
            .map_err(StoreError::InvalidState)?;
        Ok(Some((domains, topology)))
    }

    pub(super) fn machine_offer_before_native(
        &mut self,
        native: Option<JobId>,
    ) -> StoreResult<bool> {
        if self.authority_blocker()?.is_some() {
            return Ok(false);
        }
        let Some((domains, topology)) = self.machine_topology()? else {
            return Ok(false);
        };
        let config = self.config_sha256.clone();
        let rules = self.impact_incompatibilities.clone();
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let last: String = tx.query_row(
            "SELECT value FROM machine_meta WHERE key='schedule_ms'",
            [],
            |row| row.get(0),
        )?;
        let now = now_millis().max(
            last.parse()
                .map_err(|_| StoreError::InvalidState("invalid coordinator clock".into()))?,
        );
        let mut changed = super::machine_reservation::expire(&tx, now)?;
        let expired = {
            let mut s = tx.prepare("SELECT snapshot_json FROM machine_grants WHERE state='offered' AND deadline_ms<=?1")?;
            s.query_map([now], |row| row.get::<_, String>(0))?
                .collect::<std::result::Result<Vec<_>, _>>()?
        };
        for json in expired {
            let mut grant: GrantSnapshot = serde_json::from_str(&json)?;
            grant.state = GrantState::Expired;
            save_grant(&tx, &grant, None)?;
            tx.execute(
                "UPDATE machine_candidates SET not_before_ms=?2 WHERE allocation_key=?1",
                params![
                    super::machine_allocation::key_string(&grant.candidate.key)?,
                    now.saturating_add(5000)
                ],
            )?;
            changed = true;
        }
        let native_rank = native.map(|id| -> StoreResult<ScheduleKey> {
            let (accepted,sequence,spec): (i64,i64,String) = tx.query_row("SELECT q.accepted_ms,q.sequence,j.spec_json FROM machine_queue q JOIN jobs j ON q.owner='native:' || j.id WHERE j.id=?1",[id.entity_uuid().to_string()],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
            Ok(ScheduleKey { effective_priority: effective_priority_at(serde_json::from_str::<JobSpec>(&spec)?.priority,accepted,now),accepted_ms:accepted,rowid:sequence })
        }).transpose()?;
        let mut ready = {
            let mut s=tx.prepare("SELECT c.candidate_json,q.accepted_ms,q.sequence FROM machine_candidates c JOIN machine_queue q ON q.owner=c.queue_owner JOIN machine_domains d USING(domain_id) LEFT JOIN machine_grants g USING(allocation_key) WHERE c.state='ready' AND c.expires_ms>?1 AND COALESCE(c.not_before_ms,0)<=?1 AND d.reconciliation_required=0 AND (g.state IS NULL OR g.state='expired')")?;
            s.query_map([now], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, i64>(2)?,
                ))
            })?
            .map(|row| {
                let (json, accepted, sequence) = row?;
                let c: Candidate = serde_json::from_str(&json)?;
                let rank = ScheduleKey {
                    effective_priority: effective_priority_at(c.priority, accepted, now),
                    accepted_ms: accepted,
                    rowid: sequence,
                };
                Ok((c, rank))
            })
            .collect::<StoreResult<Vec<_>>>()?
        };
        ready.sort_by(|a, b| {
            schedule_order(a.1, b.1).then_with(|| a.0.key.lease_id.cmp(&b.0.key.lease_id))
        });
        let mut active = {
            let mut s = tx.prepare("SELECT claims_json FROM leases WHERE state='granted'")?;
            s.query_map([], |r| r.get::<_, String>(0))?
                .map(|row| {
                    topology
                        .expand(domains.native_domain, &serde_json::from_str(&row?)?)
                        .map_err(StoreError::InvalidState)
                })
                .collect::<StoreResult<Vec<_>>>()?
        };
        {
            let mut s=tx.prepare("SELECT snapshot_json FROM machine_grants WHERE state IN ('offered','armed','uncertain')")?;
            for row in s.query_map([], |r| r.get::<_, String>(0))? {
                let g: GrantSnapshot = serde_json::from_str(&row?)?;
                active.push(
                    topology
                        .expand(g.candidate.key.domain_id, &claims(&g.candidate.claims))
                        .map_err(StoreError::InvalidState)?,
                );
            }
        }
        // Keep native and attached reservations in the same accepted order.
        let mut reserved = {
            let mut s = tx
                .prepare("SELECT job_id,claims_json FROM reservations WHERE hold_deadline_ms>?1")?;
            s.query_map([now], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })?
            .map(|row| {
                let (job, claims) = row?;
                Ok((
                    job.clone(),
                    true,
                    super::reservation::schedule_key(&tx, &job, now)?,
                    topology
                        .expand(domains.native_domain, &serde_json::from_str(&claims)?)
                        .map_err(StoreError::InvalidState)?,
                ))
            })
            .collect::<StoreResult<Vec<_>>>()?
        };
        for reservation in super::machine_reservation::remote_reservations(&tx, now)? {
            let c = reservation.candidate;
            let key = super::machine_allocation::key_string(&c.key)?;
            if c.configuration_sha256 != config {
                changed |= super::machine_reservation::drop_reservation(&tx, &key)?;
                continue;
            }
            let expanded = topology
                .expand(c.key.domain_id, &claims(&c.claims))
                .map_err(StoreError::InvalidState)?;
            reserved.push((
                key,
                false,
                reservation.rank,
                ScopedClaims {
                    scalars: expanded.scalars,
                    ..ScopedClaims::default()
                },
            ));
        }
        reserved.sort_by(|a, b| schedule_order(a.2, b.2).then_with(|| a.0.cmp(&b.0)));
        let mut retained = Vec::new();
        let mut suffix = false;
        for (key, native, rank, claims) in reserved {
            suffix |= !claims
                .blockers(
                    &topology,
                    &retained
                        .iter()
                        .map(|r: &(String, bool, ScheduleKey, ScopedClaims)| r.3.clone())
                        .collect::<Vec<_>>(),
                    &rules,
                )
                .is_empty();
            if suffix {
                if native {
                    tx.execute(
                        "UPDATE jobs SET reservation_not_before_ms=?2 WHERE id=?1",
                        params![key, now.saturating_add(5000)],
                    )?;
                    super::reservation::release_reservation_tx(&tx, &key)?;
                } else {
                    super::machine_reservation::drop_reservation(&tx, &key)?;
                    tx.execute(
                        "UPDATE machine_candidates SET not_before_ms=?2 WHERE allocation_key=?1",
                        params![key, now.saturating_add(5000)],
                    )?;
                }
                changed = true;
            } else {
                retained.push((key, native, rank, claims));
            }
        }
        let mut reserved = retained;
        for (candidate, rank) in ready {
            if native_rank.is_some_and(|n| schedule_order(rank, n).is_gt()) {
                break;
            }
            if candidate.configuration_sha256 != config {
                continue;
            }
            let Ok(expanded) = topology.expand(candidate.key.domain_id, &claims(&candidate.claims))
            else {
                continue;
            };
            let key = super::machine_allocation::key_string(&candidate.key)?;
            let non_scalar = ScopedClaims {
                scalars: Default::default(),
                ..expanded.clone()
            };
            if !non_scalar.blockers(&topology, &active, &rules).is_empty() {
                changed |= super::machine_reservation::drop_reservation(&tx, &key)?;
                reserved.retain(|r| r.0 != key || r.1);
                continue;
            }
            let (own_deadline,not_before):(Option<i64>,Option<i64>) = tx.query_row("SELECT reservation_deadline_ms,not_before_ms FROM machine_candidates WHERE allocation_key=?1",[&key],|r|Ok((r.get(0)?,r.get(1)?)))?;
            let higher_overlaps = reserved.iter().any(|r| {
                (r.1 || r.0 != key)
                    && crate::admission::outranks(r.2, rank)
                    && expanded.scalars.iter().any(|(key, value)| {
                        *value > 0 && r.3.scalars.get(key).is_some_and(|v| *v > 0)
                    })
            });
            let decision =
                crate::admission::reservation_decision(crate::admission::ReservationInput {
                    claims: &expanded.scalars,
                    capacities: &topology.capacities(),
                    active: &active.iter().map(|c| c.scalars.clone()).collect::<Vec<_>>(),
                    reserved: &reserved
                        .iter()
                        .map(|r| r.3.scalars.clone())
                        .collect::<Vec<_>>(),
                    own_deadline,
                    higher_overlaps,
                    not_before,
                    now,
                });
            use crate::admission::ReservationDecision;
            match decision {
                ReservationDecision::Drop | ReservationDecision::Expire => {
                    changed |= super::machine_reservation::drop_reservation(&tx, &key)?;
                    reserved.retain(|r| r.1 || r.0 != key);
                    if decision == ReservationDecision::Expire {
                        tx.execute("UPDATE machine_candidates SET not_before_ms=?2 WHERE allocation_key=?1",params![key,now.saturating_add(5000)])?;
                        changed = true;
                    }
                    continue;
                }
                ReservationDecision::Hold => continue,
                ReservationDecision::Create => {
                    tx.execute("UPDATE machine_candidates SET reservation_deadline_ms=?2 WHERE allocation_key=?1",params![key,now.saturating_add(crate::SCALAR_RESERVATION_HOLD_MILLIS as i64)])?;
                    tx.execute("INSERT INTO machine_reservations VALUES (?1,?2) ON CONFLICT(allocation_key) DO UPDATE SET physical_claims_json=excluded.physical_claims_json",params![key,serde_json::to_string(&physical_claims(&expanded,domains.machine_scope).scalar_only())?])?;
                    reserved.push((
                        key,
                        false,
                        rank,
                        ScopedClaims {
                            scalars: expanded.scalars,
                            ..ScopedClaims::default()
                        },
                    ));
                    changed = true;
                    continue;
                }
                ReservationDecision::Grant => {
                    super::machine_reservation::drop_reservation(&tx, &key)?;
                    reserved.retain(|r| r.1 || r.0 != key);
                }
            }
            let old: Option<String> = tx
                .query_row(
                    "SELECT snapshot_json FROM machine_grants WHERE allocation_key=?1",
                    [&key],
                    |r| r.get(0),
                )
                .optional()?;
            let grant_id = match old {
                Some(json) => {
                    let prior = serde_json::from_str::<GrantSnapshot>(&json)?.grant_id;
                    if prior.store_uuid() != self.store_uuid {
                        return Err(StoreError::InvalidState(
                            "Grant belongs to another coordinator store".into(),
                        ));
                    }
                    crate::GrantId::from_parts(self.store_uuid, prior.entity_uuid())
                }
                None => crate::GrantId::new(self.store_uuid),
            };
            let grant = GrantSnapshot {
                risk_clearance: None,
                uncertainty_reason: None,
                queue_accepted_unix_millis: rank.accepted_ms,
                queue_sequence: u64::try_from(rank.rowid)
                    .map_err(|_| StoreError::InvalidState("negative queue identity".into()))?,
                grant_id,
                candidate,
                offer_nonce: Uuid::now_v7(),
                state: GrantState::Offered,
                offered_unix_millis: now,
                offer_deadline_unix_millis: now.saturating_add(crate::machine::OFFER_MILLIS),
                armed_unix_millis: None,
                released_unix_millis: None,
                tickets: vec![],
                sealed_release: None,
            };
            save_grant(
                &tx,
                &grant,
                Some(&physical_claims(&expanded, domains.machine_scope)),
            )?;
            active.push(expanded);
            changed = true;
        }
        if changed {
            tx.execute(
                "UPDATE machine_meta SET value=?1 WHERE key='schedule_ms'",
                [now.to_string()],
            )?;
            tx.execute(
                "UPDATE machine_meta SET value=CAST(value AS INTEGER)+1 WHERE key='revision'",
                [],
            )?;
        }
        tx.commit()?;
        Ok(changed)
    }
}
