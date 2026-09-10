//! Pure admission and ordering decisions. No filesystem, clock, IPC or database access.
mod scoped;
pub(crate) use scoped::native_accounting;
pub(crate) use scoped::scalar_entries as scalar_claim_entries;
pub(crate) use scoped::{DomainBudget, ResourceTopology, ScopedClaims, machine_capacities};
pub use scoped::{ExecutionDomainId, ResourceKind, ScopedResourceId, ScopedResourceSnapshot};

use crate::spec::canonical_custom_resource_name;
use crate::{Blocker, ResourceCapacities, ScalarResourceClaims};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct ResolvedClaims {
    pub(crate) cpu_units: u64,
    pub(crate) ram_mb: u64,
    pub(crate) cargo_slots: u64,
    pub(crate) gpu_slots: u64,
    pub(crate) custom: BTreeMap<String, u64>,
    pub(crate) shared_fences: BTreeSet<String>,
    pub(crate) exclusive_fences: BTreeSet<String>,
    pub(crate) impacts: BTreeSet<String>,
}

impl ResolvedClaims {
    pub(crate) fn blockers(
        &self,
        capacities: &ResourceCapacities,
        active: &[Self],
        impact_incompatibilities: &BTreeMap<String, Vec<String>>,
    ) -> Vec<Blocker> {
        scoped::legacy_full_vector_blockers(self, capacities, active, impact_incompatibilities)
    }

    pub(crate) fn scalar_blockers(
        &self,
        capacities: &ResourceCapacities,
        debits: &[Self],
    ) -> Vec<Blocker> {
        scoped::scalar_vector_blockers(
            &scoped::scalar_entries(self),
            &scoped::legacy_capacities(capacities),
            &debits
                .iter()
                .map(scoped::scalar_entries)
                .collect::<Vec<_>>(),
            Clone::clone,
            false,
        )
    }

    pub(crate) fn non_scalar_blockers(
        &self,
        active: &[Self],
        impact_incompatibilities: &BTreeMap<String, Vec<String>>,
    ) -> Vec<Blocker> {
        let mut blockers = Vec::new();
        for claim in active {
            for fence in self.exclusive_fences.intersection(&claim.exclusive_fences) {
                fence_blocker(&mut blockers, fence);
            }
            for fence in self.exclusive_fences.intersection(&claim.shared_fences) {
                fence_blocker(&mut blockers, fence);
            }
            for fence in self.shared_fences.intersection(&claim.exclusive_fences) {
                fence_blocker(&mut blockers, fence);
            }
            for impact in &self.impacts {
                for active_impact in &claim.impacts {
                    if impacts_conflict(impact, active_impact, impact_incompatibilities) {
                        blockers.push(Blocker {
                            code: "impact_busy".into(),
                            detail: format!("{impact} incompatible with active {active_impact}"),
                        });
                    }
                }
            }
        }
        sort_blockers(&mut blockers);
        blockers
    }

    pub(crate) fn scalar_only(&self) -> Self {
        Self {
            cpu_units: self.cpu_units,
            ram_mb: self.ram_mb,
            cargo_slots: self.cargo_slots,
            gpu_slots: self.gpu_slots,
            custom: self.custom.clone(),
            ..Self::default()
        }
    }

    pub(crate) fn public_scalars(&self) -> ScalarResourceClaims {
        ScalarResourceClaims {
            cpu_units: self.cpu_units,
            ram_mb: self.ram_mb,
            cargo_slots: self.cargo_slots,
            gpu_slots: self.gpu_slots,
            custom: self.custom.clone(),
        }
    }

    pub(crate) fn overlaps_scalars(&self, other: &Self) -> bool {
        (self.cpu_units > 0 && other.cpu_units > 0)
            || (self.ram_mb > 0 && other.ram_mb > 0)
            || (self.cargo_slots > 0 && other.cargo_slots > 0)
            || (self.gpu_slots > 0 && other.gpu_slots > 0)
            || self
                .custom
                .iter()
                .any(|(name, value)| *value > 0 && other.custom.get(name).is_some_and(|v| *v > 0))
    }

    /// Reports only conflicts that exist because authenticated ancestors retain Leases.
    /// Unrelated active Jobs are intentionally excluded: they can finish while the caller waits.
    pub(crate) fn ancestor_blockers(
        &self,
        capacities: &ResourceCapacities,
        ancestors: &[Self],
        impact_incompatibilities: &BTreeMap<String, Vec<String>>,
    ) -> Vec<Blocker> {
        let mut blockers = scoped::scalar_vector_blockers(
            &scoped::scalar_entries(self),
            &scoped::legacy_capacities(capacities),
            &ancestors
                .iter()
                .map(scoped::scalar_entries)
                .collect::<Vec<_>>(),
            Clone::clone,
            true,
        );
        for claim in ancestors {
            for fence in self.exclusive_fences.intersection(&claim.exclusive_fences) {
                ancestor_fence_blocker(&mut blockers, fence);
            }
            for fence in self.exclusive_fences.intersection(&claim.shared_fences) {
                ancestor_fence_blocker(&mut blockers, fence);
            }
            for fence in self.shared_fences.intersection(&claim.exclusive_fences) {
                ancestor_fence_blocker(&mut blockers, fence);
            }
            for impact in &self.impacts {
                for ancestor_impact in &claim.impacts {
                    if impacts_conflict(impact, ancestor_impact, impact_incompatibilities) {
                        blockers.push(Blocker {
                            code: "blocked_by_ancestor".into(),
                            detail: format!(
                                "impact {impact} incompatible with ancestor {ancestor_impact}"
                            ),
                        });
                    }
                }
            }
        }
        blockers.sort_by(|left, right| {
            left.code
                .cmp(&right.code)
                .then(left.detail.cmp(&right.detail))
        });
        blockers.dedup();
        blockers
    }
}

fn sort_blockers(blockers: &mut Vec<Blocker>) {
    blockers.sort_by(|left, right| {
        left.code
            .cmp(&right.code)
            .then(left.detail.cmp(&right.detail))
    });
    blockers.dedup();
}

fn custom_capacity(capacities: &ResourceCapacities, requested_name: &str) -> u64 {
    capacities
        .custom
        .iter()
        .find_map(|(name, capacity)| {
            canonical_custom_resource_name(name)
                .ok()
                .filter(|canonical| canonical == requested_name)
                .map(|_| *capacity)
        })
        .unwrap_or(0)
}

fn impacts_conflict(left: &str, right: &str, rules: &BTreeMap<String, Vec<String>>) -> bool {
    rules
        .get(left)
        .is_some_and(|values| values.iter().any(|value| value == right))
        || rules
            .get(right)
            .is_some_and(|values| values.iter().any(|value| value == left))
}

fn ancestor_scalar_blocker(
    blockers: &mut Vec<Blocker>,
    name: &str,
    requested: u64,
    capacity: u64,
    retained_by_ancestors: Option<u64>,
) {
    if requested == 0 {
        return;
    }
    if requested > capacity {
        blockers.push(Blocker {
            code: "resource_capacity".into(),
            detail: format!("{name}: requested {requested}, configured capacity {capacity}"),
        });
        return;
    }
    let Some(retained_by_ancestors) = retained_by_ancestors else {
        blockers.push(Blocker {
            code: "blocked_by_ancestor".into(),
            detail: format!("{name}: retained ancestor debit sum overflow"),
        });
        return;
    };
    if retained_by_ancestors == 0 {
        return;
    }
    let available_after_ancestors = capacity.saturating_sub(retained_by_ancestors);
    if requested > available_after_ancestors {
        blockers.push(Blocker {
            code: "blocked_by_ancestor".into(),
            detail: format!(
                "{name}: requested {requested}, available while ancestors retain Leases {available_after_ancestors}, configured {capacity}"
            ),
        });
    }
}

fn ancestor_fence_blocker(blockers: &mut Vec<Blocker>, fence: &str) {
    blockers.push(Blocker {
        code: "blocked_by_ancestor".into(),
        detail: format!("path fence retained by an ancestor: {fence}"),
    });
}

fn scalar_blocker(
    blockers: &mut Vec<Blocker>,
    name: &str,
    requested: u64,
    capacity: u64,
    granted: Option<u64>,
) {
    if requested == 0 {
        return;
    }
    let Some(granted) = granted else {
        blockers.push(Blocker {
            code: "resource_busy".into(),
            detail: format!("{name}: granted debit sum overflow"),
        });
        return;
    };
    let available = capacity.saturating_sub(granted);
    if requested > available {
        blockers.push(Blocker {
            code: if requested > capacity {
                "resource_capacity"
            } else {
                "resource_busy"
            }
            .into(),
            detail: format!(
                "{name}: requested {requested}, available {available}, configured {capacity}"
            ),
        });
    }
}

fn checked_total(mut values: impl Iterator<Item = u64>) -> Option<u64> {
    values.try_fold(0_u64, u64::checked_add)
}

pub(crate) fn observed_resource_blocker(
    name: &str,
    requested: u64,
    observed_headroom: u64,
    safety_margin: u64,
    granted_excluding_self: u64,
) -> Option<Blocker> {
    if requested == 0 {
        return None;
    }
    let available = observed_headroom
        .checked_sub(safety_margin)
        .and_then(|headroom| headroom.checked_sub(granted_excluding_self));
    match available {
        Some(available) if requested <= available => None,
        Some(available) => Some(Blocker {
            code: "observed_resource_busy".into(),
            detail: format!(
                "{name}: requested {requested}, observed {observed_headroom}, margin {safety_margin}, granted {granted_excluding_self}, available {available}"
            ),
        }),
        None => Some(Blocker {
            code: "observation_unusable".into(),
            detail: format!(
                "{name}: checked headroom arithmetic failed for observed {observed_headroom}, margin {safety_margin}, granted {granted_excluding_self}"
            ),
        }),
    }
}

fn fence_blocker(blockers: &mut Vec<Blocker>, fence: &str) {
    blockers.push(Blocker {
        code: "path_fence_busy".into(),
        detail: fence.to_owned(),
    });
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ScheduleKey {
    pub(crate) effective_priority: i64,
    pub(crate) accepted_ms: i64,
    pub(crate) rowid: i64,
}

pub(crate) fn effective_priority_at(priority: i8, accepted_ms: i64, now: i64) -> i64 {
    let waited_ms = u64::try_from(now.saturating_sub(accepted_ms)).unwrap_or(0);
    let quanta = waited_ms / crate::PRIORITY_AGING_QUANTUM_MILLIS;
    i64::from(priority)
        .saturating_add(i64::try_from(quanta).unwrap_or(i64::MAX))
        .min(crate::MAX_EFFECTIVE_PRIORITY)
}

pub(crate) fn outranks(left: ScheduleKey, right: ScheduleKey) -> bool {
    schedule_order(left, right).is_lt()
}

pub(crate) fn schedule_order(left: ScheduleKey, right: ScheduleKey) -> std::cmp::Ordering {
    right
        .effective_priority
        .cmp(&left.effective_priority)
        .then(left.accepted_ms.cmp(&right.accepted_ms))
        .then(left.rowid.cmp(&right.rowid))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReservationDecision {
    Grant,
    Hold,
    Create,
    Expire,
    Drop,
}

pub(crate) struct ReservationInput<'a, K> {
    pub(crate) claims: &'a BTreeMap<K, u64>,
    pub(crate) capacities: &'a BTreeMap<K, u64>,
    pub(crate) active: &'a [BTreeMap<K, u64>],
    pub(crate) reserved: &'a [BTreeMap<K, u64>],
    pub(crate) own_deadline: Option<i64>,
    pub(crate) higher_overlaps: bool,
    pub(crate) not_before: Option<i64>,
    pub(crate) now: i64,
}

/// Decides scalar reservation/conversion without mutating a queue or acquiring
/// resources. The caller commits the decision with its complete lifecycle change.
pub(crate) fn reservation_decision<K: Ord>(input: ReservationInput<'_, K>) -> ReservationDecision {
    use ReservationDecision::*;
    let fits = |debits: &[&BTreeMap<K, u64>]| {
        input.claims.iter().all(|(key, amount)| {
            *amount == 0
                || checked_total(
                    debits
                        .iter()
                        .map(|claims| claims.get(key).copied().unwrap_or(0)),
                )
                .is_some_and(|used| {
                    *amount
                        <= input
                            .capacities
                            .get(key)
                            .copied()
                            .unwrap_or(0)
                            .saturating_sub(used)
                })
        })
    };
    if !fits(&[]) {
        return Drop;
    }
    let active = input.active.iter().collect::<Vec<_>>();
    if let Some(deadline) = input.own_deadline {
        if deadline <= input.now {
            return Expire;
        }
        return if !input.higher_overlaps && fits(&active) {
            Grant
        } else {
            Hold
        };
    }
    if input
        .not_before
        .is_some_and(|not_before| not_before > input.now)
    {
        return Hold;
    }
    let accounted = input
        .active
        .iter()
        .chain(input.reserved)
        .collect::<Vec<_>>();
    if fits(&accounted) {
        return Grant;
    }
    let overflow = input.claims.iter().any(|(key, amount)| {
        *amount > 0
            && checked_total(
                input
                    .active
                    .iter()
                    .map(|claim| claim.get(key).copied().unwrap_or(0)),
            )
            .is_none()
    });
    if !input.claims.values().any(|amount| *amount > 0)
        || overflow
        || !fits(&input.reserved.iter().collect::<Vec<_>>())
    {
        Hold
    } else {
        Create
    }
}

pub(crate) fn local_reservation_decision(
    claims: &ResolvedClaims,
    capacities: &ResourceCapacities,
    active: &[ResolvedClaims],
    reserved: &[ResolvedClaims],
    state: (Option<i64>, bool, Option<i64>, i64),
) -> ReservationDecision {
    reservation_decision(ReservationInput {
        claims: &scoped::scalar_entries(claims),
        capacities: &scoped::legacy_capacities(capacities),
        active: &active
            .iter()
            .map(scoped::scalar_entries)
            .collect::<Vec<_>>(),
        reserved: &reserved
            .iter()
            .map(scoped::scalar_entries)
            .collect::<Vec<_>>(),
        own_deadline: state.0,
        higher_overlaps: state.1,
        not_before: state.2,
        now: state.3,
    })
}
