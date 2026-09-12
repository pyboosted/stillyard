//! Resource identities and hierarchy expansion. Each ancestor is a constraint,
//! never a second physical consumer. This module has no mutable allocation store.

use std::collections::{BTreeMap, BTreeSet};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{ResolvedClaims, ancestor_scalar_blocker, scalar_blocker, sort_blockers};
use crate::{Blocker, ResourceCapacities};

#[derive(
    Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize, JsonSchema,
)]
#[serde(transparent)]
pub struct ExecutionDomainId(pub Uuid);

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    Scalar,
    FilesystemFence,
    Impact,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScopedResourceId {
    pub scope: ExecutionDomainId,
    pub kind: ResourceKind,
    pub resource_id: String,
}

impl ScopedResourceId {
    fn display(&self) -> String {
        if self.scope.0.is_nil() {
            self.resource_id.clone()
        } else {
            format!("{} in domain {}", self.resource_id, self.scope.0)
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScopedResourceSnapshot {
    pub resource: ScopedResourceId,
    pub capacity: u64,
    pub granted: u64,
    pub offered: u64,
    pub reserved: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct DomainBudget {
    pub(crate) id: ExecutionDomainId,
    pub(crate) parent: Option<ExecutionDomainId>,
    /// Missing an ancestor budget adds no extra constraint. A present zero is zero.
    pub(crate) capacities: BTreeMap<String, u64>,
}

#[derive(Clone, Debug)]
pub(crate) struct ResourceTopology {
    root: ExecutionDomainId,
    domains: BTreeMap<ExecutionDomainId, DomainBudget>,
    /// Explicit aliases to physical scalar resources, scoped to their submitting domain.
    aliases: BTreeMap<(ExecutionDomainId, String), String>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ScopedClaims {
    pub(crate) scalars: BTreeMap<ScopedResourceId, u64>,
    pub(crate) shared_fences: BTreeSet<ScopedResourceId>,
    pub(crate) exclusive_fences: BTreeSet<ScopedResourceId>,
    pub(crate) impacts: BTreeSet<String>,
}

impl ResourceTopology {
    pub(crate) fn accounting(
        &self,
        granted: &[ScopedClaims],
        offered: &[ScopedClaims],
        reserved: &[ScopedClaims],
    ) -> Result<Vec<ScopedResourceSnapshot>, String> {
        let mut keys = self.capacities().into_keys().collect::<BTreeSet<_>>();
        for claim in granted.iter().chain(offered).chain(reserved) {
            keys.extend(claim.scalars.keys().cloned());
        }
        let capacities = self.capacities();
        keys.into_iter()
            .map(|key| {
                let total = |claims: &[ScopedClaims]| {
                    super::checked_total(
                        claims
                            .iter()
                            .map(|claim| claim.scalars.get(&key).copied().unwrap_or(0)),
                    )
                    .ok_or_else(|| format!("{}: accounting overflow", key.display()))
                };
                Ok(ScopedResourceSnapshot {
                    capacity: capacities.get(&key).copied().unwrap_or(0),
                    granted: total(granted)?,
                    offered: total(offered)?,
                    reserved: total(reserved)?,
                    resource: key,
                })
            })
            .collect()
    }

    pub(crate) fn new(
        root: ExecutionDomainId,
        domains: Vec<DomainBudget>,
        aliases: BTreeMap<(ExecutionDomainId, String), String>,
    ) -> Result<Self, String> {
        if domains.is_empty() || domains.len() > 256 {
            return Err("topology requires 1..256 domains".into());
        }
        let count = domains.len();
        let domains: BTreeMap<_, _> = domains
            .into_iter()
            .map(|domain| (domain.id, domain))
            .collect();
        if domains.len() != count
            || domains
                .get(&root)
                .is_none_or(|domain| domain.parent.is_some())
        {
            return Err("duplicate domain or invalid machine root".into());
        }
        let topology = Self {
            root,
            domains,
            aliases,
        };
        for id in topology.domains.keys() {
            topology.ancestors(*id)?;
        }
        for ((domain, _), physical) in &topology.aliases {
            if !topology.domains.contains_key(domain)
                || !topology.domains[&root].capacities.contains_key(physical)
            {
                return Err("alias has an unknown domain or physical resource".into());
            }
        }
        Ok(topology)
    }

    fn ancestors(&self, mut id: ExecutionDomainId) -> Result<Vec<&DomainBudget>, String> {
        let mut result = Vec::new();
        let mut seen = BTreeSet::new();
        loop {
            if result.len() >= 16 || !seen.insert(id) {
                return Err("domain tree is cyclic or deeper than 16".into());
            }
            let domain = self.domains.get(&id).ok_or("unknown domain")?;
            result.push(domain);
            match domain.parent {
                Some(parent) => id = parent,
                None if id == self.root => return Ok(result),
                None => return Err("domain is disconnected from machine root".into()),
            }
        }
    }

    pub(crate) fn expand(
        &self,
        domain: ExecutionDomainId,
        claims: &ResolvedClaims,
    ) -> Result<ScopedClaims, String> {
        let ancestors = self.ancestors(domain)?;
        let mut result = ScopedClaims {
            impacts: claims.impacts.clone(),
            ..ScopedClaims::default()
        };
        let mut physical = BTreeMap::<String, u64>::new();
        for (name, value) in scalar_entries(claims) {
            if value == 0 {
                continue;
            }
            let name = self
                .aliases
                .get(&(domain, name.clone()))
                .cloned()
                .unwrap_or(name);
            if !self.domains[&self.root].capacities.contains_key(&name) {
                return Err(format!("unmapped physical resource {name}"));
            }
            let old = physical.entry(name).or_default();
            *old = old.checked_add(value).ok_or("physical claim overflow")?;
        }
        for (name, value) in physical {
            for ancestor in &ancestors {
                if ancestor.capacities.contains_key(&name) {
                    result.scalars.insert(
                        ScopedResourceId {
                            scope: ancestor.id,
                            kind: ResourceKind::Scalar,
                            resource_id: name.clone(),
                        },
                        value,
                    );
                }
            }
        }
        let fence = |name: &String| ScopedResourceId {
            scope: domain,
            kind: ResourceKind::FilesystemFence,
            resource_id: name.clone(),
        };
        result.shared_fences = claims.shared_fences.iter().map(fence).collect();
        result.exclusive_fences = claims.exclusive_fences.iter().map(fence).collect();
        Ok(result)
    }

    pub(crate) fn capacities(&self) -> BTreeMap<ScopedResourceId, u64> {
        self.domains
            .values()
            .flat_map(|domain| {
                domain.capacities.iter().map(|(name, value)| {
                    (
                        ScopedResourceId {
                            scope: domain.id,
                            kind: ResourceKind::Scalar,
                            resource_id: name.clone(),
                        },
                        *value,
                    )
                })
            })
            .collect()
    }
}

pub(crate) fn scalar_entries(claims: &ResolvedClaims) -> BTreeMap<String, u64> {
    let mut values = claims.custom.clone();
    values.extend([
        ("cpu_units".into(), claims.cpu_units),
        ("ram_mb".into(), claims.ram_mb),
        ("cargo_slots".into(), claims.cargo_slots),
        ("gpu_slots".into(), claims.gpu_slots),
    ]);
    values
}

pub(crate) fn machine_capacities(capacities: &ResourceCapacities) -> BTreeMap<String, u64> {
    legacy_capacities(capacities)
}

/// Shared scalar evaluation used by native local admission and hierarchical grants.
/// Each decision observes the complete vector and performs no partial mutation.
pub(crate) fn scalar_vector_blockers<K: Ord>(
    claims: &BTreeMap<K, u64>,
    capacities: &BTreeMap<K, u64>,
    debits: &[BTreeMap<K, u64>],
    display: impl Fn(&K) -> String,
    ancestors_only: bool,
) -> Vec<Blocker> {
    let mut blockers = Vec::new();
    for (key, requested) in claims {
        let used = super::checked_total(
            debits
                .iter()
                .map(|claim| claim.get(key).copied().unwrap_or(0)),
        );
        let capacity = capacities.get(key).copied().unwrap_or(0);
        if ancestors_only {
            ancestor_scalar_blocker(&mut blockers, &display(key), *requested, capacity, used);
        } else {
            scalar_blocker(&mut blockers, &display(key), *requested, capacity, used);
        }
    }
    sort_blockers(&mut blockers);
    blockers
}

impl ScopedClaims {
    pub(crate) fn blockers(
        &self,
        topology: &ResourceTopology,
        active: &[Self],
        rules: &BTreeMap<String, Vec<String>>,
    ) -> Vec<Blocker> {
        let mut blockers = scalar_vector_blockers(
            &self.scalars,
            &topology.capacities(),
            &active
                .iter()
                .map(|claim| claim.scalars.clone())
                .collect::<Vec<_>>(),
            ScopedResourceId::display,
            false,
        );
        for claim in active {
            for fence in self
                .exclusive_fences
                .intersection(&claim.exclusive_fences)
                .chain(self.exclusive_fences.intersection(&claim.shared_fences))
                .chain(self.shared_fences.intersection(&claim.exclusive_fences))
            {
                super::fence_blocker(&mut blockers, &fence.display());
            }
            for impact in &self.impacts {
                for held in &claim.impacts {
                    if super::impacts_conflict(impact, held, rules) {
                        blockers.push(Blocker {
                            code: "impact_busy".into(),
                            detail: format!("{impact} incompatible with active {held}"),
                        });
                    }
                }
            }
        }
        sort_blockers(&mut blockers);
        blockers
    }

    #[cfg(test)]
    pub(crate) fn scalar_only(&self) -> Self {
        Self {
            scalars: self.scalars.clone(),
            ..Self::default()
        }
    }

    #[cfg(test)]
    pub(crate) fn overlaps_scalars(&self, other: &Self) -> bool {
        self.scalars.iter().any(|(key, value)| {
            *value > 0 && other.scalars.get(key).is_some_and(|value| *value > 0)
        })
    }
}

pub(super) fn legacy_capacities(capacities: &ResourceCapacities) -> BTreeMap<String, u64> {
    let mut values = capacities
        .custom
        .keys()
        .map(|name| {
            let canonical =
                crate::spec::canonical_custom_resource_name(name).unwrap_or_else(|_| name.clone());
            let capacity = super::custom_capacity(capacities, &canonical);
            (canonical, capacity)
        })
        .collect::<BTreeMap<_, _>>();
    values.extend([
        ("cpu_units".into(), u64::from(capacities.cpu_units)),
        ("ram_mb".into(), capacities.ram_mb),
        ("cargo_slots".into(), u64::from(capacities.cargo_slots)),
        ("gpu_slots".into(), u64::from(capacities.gpu_slots)),
    ]);
    values
}

pub(crate) fn native_accounting(
    domains: &crate::AuthorityDomains,
    capacities: &ResourceCapacities,
    granted: &[ResolvedClaims],
    reserved: &[ResolvedClaims],
) -> Result<Vec<ScopedResourceSnapshot>, String> {
    let mut capacities = legacy_capacities(capacities);
    for claim in granted.iter().chain(reserved) {
        for name in scalar_entries(claim).keys() {
            capacities.entry(name.clone()).or_insert(0);
        }
    }
    let topology = ResourceTopology::new(
        domains.machine_scope,
        vec![
            DomainBudget {
                id: domains.machine_scope,
                parent: None,
                capacities,
            },
            DomainBudget {
                id: domains.native_domain,
                parent: Some(domains.machine_scope),
                capacities: BTreeMap::new(),
            },
        ],
        BTreeMap::new(),
    )?;
    let expand = |claims: &[ResolvedClaims]| {
        claims
            .iter()
            .map(|claim| topology.expand(domains.native_domain, claim))
            .collect::<Result<Vec<_>, _>>()
    };
    topology.accounting(&expand(granted)?, &[], &expand(reserved)?)
}

/// The standalone fast path uses the same vector kernel in a single root scope.
/// Nil is a private calculation scope, never a persisted/advertised domain identity.
pub(super) fn legacy_full_vector_blockers(
    claims: &ResolvedClaims,
    capacities: &ResourceCapacities,
    active: &[ResolvedClaims],
    rules: &BTreeMap<String, Vec<String>>,
) -> Vec<Blocker> {
    let root = ExecutionDomainId(Uuid::nil());
    let mut capacities = legacy_capacities(capacities);
    for claim in std::iter::once(claims).chain(active) {
        for name in scalar_entries(claim).keys() {
            capacities.entry(name.clone()).or_insert(0);
        }
    }
    let topology = ResourceTopology::new(
        root,
        vec![DomainBudget {
            id: root,
            parent: None,
            capacities,
        }],
        BTreeMap::new(),
    )
    .expect("one native root is a valid topology");
    let expanded = topology
        .expand(root, claims)
        .expect("all native scalar names were included");
    let active = active
        .iter()
        .map(|claim| {
            topology
                .expand(root, claim)
                .expect("all native debit names were included")
        })
        .collect::<Vec<_>>();
    expanded.blockers(&topology, &active, rules)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scoped_reservations_convert_whole_vectors_and_accounting_rejects_overflow() {
        use super::super::{ReservationDecision, ReservationInput, reservation_decision};
        let (topology, host, guest) = topology();
        let work = topology
            .expand(
                guest,
                &ResolvedClaims {
                    cpu_units: 4,
                    ram_mb: 6144,
                    cargo_slots: 1,
                    ..Default::default()
                },
            )
            .unwrap();
        let capacities = topology.capacities();
        let active = [work.scalars.clone()];
        let decision = |active: &[BTreeMap<ScopedResourceId, u64>], own, higher, not_before| {
            reservation_decision(ReservationInput {
                claims: &work.scalars,
                capacities: &capacities,
                active,
                reserved: &[],
                own_deadline: own,
                higher_overlaps: higher,
                not_before,
                now: 100,
            })
        };
        assert_eq!(
            decision(&active, None, false, None),
            ReservationDecision::Create
        );
        assert_eq!(
            decision(&active, Some(200), false, None),
            ReservationDecision::Hold
        );
        assert_eq!(
            decision(&[], Some(200), true, None),
            ReservationDecision::Hold
        );
        assert_eq!(
            decision(&[], Some(200), false, None),
            ReservationDecision::Grant
        );
        assert_eq!(
            decision(&[], Some(100), false, None),
            ReservationDecision::Expire
        );
        assert_eq!(
            decision(&[], None, false, Some(101)),
            ReservationDecision::Hold
        );
        let usage = topology
            .accounting(
                std::slice::from_ref(&work),
                &[],
                std::slice::from_ref(&work),
            )
            .unwrap();
        let physical = usage
            .iter()
            .find(|row| row.resource.scope == host && row.resource.resource_id == "cpu_units")
            .unwrap();
        assert_eq!(
            (physical.capacity, physical.granted, physical.reserved),
            (16, 4, 4)
        );
        let extreme = topology
            .expand(
                host,
                &ResolvedClaims {
                    cpu_units: u64::MAX,
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(
            topology
                .accounting(&[extreme.clone(), extreme], &[], &[])
                .is_err()
        );
    }

    fn topology() -> (ResourceTopology, ExecutionDomainId, ExecutionDomainId) {
        let machine = ExecutionDomainId(Uuid::from_u128(1));
        let vm = ExecutionDomainId(Uuid::from_u128(2));
        let guest = ExecutionDomainId(Uuid::from_u128(3));
        let budget = |id, parent, cpu, ram| DomainBudget {
            id,
            parent,
            capacities: BTreeMap::from([
                ("cpu_units".into(), cpu),
                ("ram_mb".into(), ram),
                ("cargo_slots".into(), 2),
            ]),
        };
        (
            ResourceTopology::new(
                machine,
                vec![
                    budget(machine, None, 16, 32768),
                    budget(vm, Some(machine), 8, 12288),
                    budget(guest, Some(vm), 6, 10240),
                ],
                BTreeMap::new(),
            )
            .unwrap(),
            machine,
            guest,
        )
    }

    #[test]
    fn hierarchy_counts_each_constraint_once_and_rejects_full_vector() {
        let (topology, host, guest) = topology();
        let work = ResolvedClaims {
            cpu_units: 4,
            ram_mb: 6144,
            cargo_slots: 1,
            ..Default::default()
        };
        let guest_work = topology.expand(guest, &work).unwrap();
        let physical_cpu = ScopedResourceId {
            scope: host,
            kind: ResourceKind::Scalar,
            resource_id: "cpu_units".into(),
        };
        assert_eq!(guest_work.scalars[&physical_cpu], 4); // Not 12 across three ancestors.
        assert!(
            guest_work
                .blockers(&topology, &[], &BTreeMap::new())
                .is_empty()
        );
        assert!(
            !guest_work
                .blockers(
                    &topology,
                    std::slice::from_ref(&guest_work),
                    &BTreeMap::new()
                )
                .is_empty()
        );
        let host_work = topology
            .expand(
                host,
                &ResolvedClaims {
                    cpu_units: 8,
                    ram_mb: 8192,
                    cargo_slots: 1,
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(
            host_work
                .blockers(
                    &topology,
                    std::slice::from_ref(&guest_work),
                    &BTreeMap::new()
                )
                .is_empty()
        );
        assert!(host_work.overlaps_scalars(&guest_work.scalar_only()));
    }

    #[test]
    fn aliases_share_physical_capacity_but_fences_are_domain_local() {
        let (mut topology, host, guest) = topology();
        topology
            .domains
            .get_mut(&host)
            .unwrap()
            .capacities
            .insert("vram_mb:gpu0".into(), 16384);
        topology
            .aliases
            .insert((guest, "guest-card0".into()), "vram_mb:gpu0".into());
        let request = |name: &str| ResolvedClaims {
            custom: BTreeMap::from([(name.into(), 10240)]),
            exclusive_fences: BTreeSet::from(["same-spelling".into()]),
            ..Default::default()
        };
        let a = topology.expand(host, &request("vram_mb:gpu0")).unwrap();
        let b = topology.expand(guest, &request("guest-card0")).unwrap();
        let blockers = b.blockers(&topology, &[a], &BTreeMap::new());
        assert_eq!(blockers.len(), 1);
        assert_eq!(blockers[0].code, "resource_busy");
    }

    #[test]
    fn missing_budget_is_not_zero_and_cycles_never_expand() {
        let (mut topology, host, guest) = topology();
        topology
            .domains
            .get_mut(&guest)
            .unwrap()
            .capacities
            .remove("cargo_slots");
        let expanded = topology
            .expand(
                guest,
                &ResolvedClaims {
                    cargo_slots: 1,
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(expanded.scalars.len(), 2);
        topology.domains.get_mut(&host).unwrap().parent = Some(guest);
        assert!(topology.expand(guest, &ResolvedClaims::default()).is_err());
    }
}
