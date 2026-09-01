use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::Component;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::spec::canonical_custom_resource_name;
use crate::{
    Blocker, ChildSubmissionPolicy, ResourceCapacities, ResourceClaims, ScalarResourceClaims,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ResolvedPolicyFence {
    pub(crate) identity_key: String,
    pub(crate) remaining_components: Vec<String>,
    pub(crate) display_path: PathBuf,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct ResolvedChildSubmissionPolicy {
    pub(crate) policy: ChildSubmissionPolicy,
    pub(crate) shared_fences: Vec<ResolvedPolicyFence>,
    pub(crate) exclusive_fences: Vec<ResolvedPolicyFence>,
}

impl ResolvedChildSubmissionPolicy {
    pub(crate) fn resolve(policy: &ChildSubmissionPolicy) -> io::Result<Self> {
        let shared_fences = policy
            .fences
            .shared_roots
            .iter()
            .map(|path| resolve_policy_fence(path))
            .collect::<io::Result<Vec<_>>>()?;
        let exclusive_fences = policy
            .fences
            .exclusive_roots
            .iter()
            .map(|path| resolve_policy_fence(path))
            .collect::<io::Result<Vec<_>>>()?;
        ensure_unique_policy_fences(&shared_fences)?;
        ensure_unique_policy_fences(&exclusive_fences)?;
        let mut resolved = Self {
            policy: policy.clone(),
            shared_fences,
            exclusive_fences,
        };
        resolved.canonicalize_custom_claims()?;
        Ok(resolved)
    }

    /// Older retained alpha.9 rows may contain a display-valid but non-canonical VRAM UUID.
    /// Normalize only the claim names: fence identities must remain exactly as admitted.
    pub(crate) fn canonicalize_custom_claims(&mut self) -> io::Result<()> {
        let mut canonical = BTreeMap::new();
        for (name, value) in &self.policy.max_claims.custom {
            let name = canonical_custom_resource_name(name)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
            if canonical.insert(name, *value).is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "duplicate canonical child-policy custom resource",
                ));
            }
        }
        self.policy.max_claims.custom = canonical;
        Ok(())
    }

    pub(crate) fn allows_shared(&self, path: &Path) -> io::Result<bool> {
        policy_fences_allow(&self.shared_fences, path)
    }

    pub(crate) fn allows_exclusive(&self, path: &Path) -> io::Result<bool> {
        policy_fences_allow(&self.exclusive_fences, path)
    }
}

fn ensure_unique_policy_fences(fences: &[ResolvedPolicyFence]) -> io::Result<()> {
    let mut seen = BTreeSet::new();
    for fence in fences {
        if !seen.insert((
            fence.identity_key.as_str(),
            fence.remaining_components.as_slice(),
        )) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "child policy roots collide after resolution",
            ));
        }
    }
    Ok(())
}

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
    pub(crate) fn resolve(claims: &ResourceClaims) -> io::Result<Self> {
        let mut custom = BTreeMap::new();
        for (name, value) in &claims.custom {
            let canonical = canonical_custom_resource_name(name)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
            if custom.insert(canonical, *value).is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "duplicate canonical custom resource",
                ));
            }
        }
        let resolved = Self {
            cpu_units: u64::from(claims.cpu_units.unwrap_or(0)),
            ram_mb: claims.ram_mb.unwrap_or(0),
            cargo_slots: u64::from(claims.cargo_slots.unwrap_or(0)),
            gpu_slots: u64::from(claims.gpu_slots.unwrap_or(0)),
            custom,
            shared_fences: resolve_fences(&claims.shared_fences)?,
            exclusive_fences: resolve_fences(&claims.exclusive_fences)?,
            impacts: claims.impacts.iter().cloned().collect(),
        };
        if resolved
            .shared_fences
            .iter()
            .any(|fence| resolved.exclusive_fences.contains(fence))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "one resolved path fence cannot be both shared and exclusive",
            ));
        }
        Ok(resolved)
    }

    pub(crate) fn blockers(
        &self,
        capacities: &ResourceCapacities,
        active: &[Self],
        impact_incompatibilities: &BTreeMap<String, Vec<String>>,
    ) -> Vec<Blocker> {
        let mut blockers = self.scalar_blockers(capacities, active);
        blockers.extend(self.non_scalar_blockers(active, impact_incompatibilities));
        sort_blockers(&mut blockers);
        blockers
    }

    pub(crate) fn scalar_blockers(
        &self,
        capacities: &ResourceCapacities,
        debits: &[Self],
    ) -> Vec<Blocker> {
        let mut blockers = Vec::new();
        scalar_blocker(
            &mut blockers,
            "cpu_units",
            self.cpu_units,
            u64::from(capacities.cpu_units),
            checked_total(debits.iter().map(|claim| claim.cpu_units)),
        );
        scalar_blocker(
            &mut blockers,
            "ram_mb",
            self.ram_mb,
            capacities.ram_mb,
            checked_total(debits.iter().map(|claim| claim.ram_mb)),
        );
        scalar_blocker(
            &mut blockers,
            "cargo_slots",
            self.cargo_slots,
            u64::from(capacities.cargo_slots),
            checked_total(debits.iter().map(|claim| claim.cargo_slots)),
        );
        scalar_blocker(
            &mut blockers,
            "gpu_slots",
            self.gpu_slots,
            u64::from(capacities.gpu_slots),
            checked_total(debits.iter().map(|claim| claim.gpu_slots)),
        );
        for (name, requested) in &self.custom {
            scalar_blocker(
                &mut blockers,
                name,
                *requested,
                custom_capacity(capacities, name),
                checked_total(
                    debits
                        .iter()
                        .map(|claim| claim.custom.get(name).copied().unwrap_or(0)),
                ),
            );
        }
        sort_blockers(&mut blockers);
        blockers
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

    pub(crate) fn has_positive_scalars(&self) -> bool {
        self.cpu_units > 0
            || self.ram_mb > 0
            || self.cargo_slots > 0
            || self.gpu_slots > 0
            || self.custom.values().any(|value| *value > 0)
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
        let mut blockers = Vec::new();
        ancestor_scalar_blocker(
            &mut blockers,
            "cpu_units",
            self.cpu_units,
            u64::from(capacities.cpu_units),
            checked_total(ancestors.iter().map(|claim| claim.cpu_units)),
        );
        ancestor_scalar_blocker(
            &mut blockers,
            "ram_mb",
            self.ram_mb,
            capacities.ram_mb,
            checked_total(ancestors.iter().map(|claim| claim.ram_mb)),
        );
        ancestor_scalar_blocker(
            &mut blockers,
            "cargo_slots",
            self.cargo_slots,
            u64::from(capacities.cargo_slots),
            checked_total(ancestors.iter().map(|claim| claim.cargo_slots)),
        );
        ancestor_scalar_blocker(
            &mut blockers,
            "gpu_slots",
            self.gpu_slots,
            u64::from(capacities.gpu_slots),
            checked_total(ancestors.iter().map(|claim| claim.gpu_slots)),
        );
        for (name, requested) in &self.custom {
            ancestor_scalar_blocker(
                &mut blockers,
                name,
                *requested,
                custom_capacity(capacities, name),
                checked_total(
                    ancestors
                        .iter()
                        .map(|claim| claim.custom.get(name).copied().unwrap_or(0)),
                ),
            );
        }
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

fn resolve_fences(paths: &[String]) -> io::Result<BTreeSet<String>> {
    let mut keys = BTreeSet::new();
    for path in paths {
        keys.extend(resolve_fence(Path::new(path))?);
    }
    Ok(keys)
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

fn resolve_policy_fence(path: &Path) -> io::Result<ResolvedPolicyFence> {
    let (ancestor, remainder) = existing_ancestor(path)?;
    let identity_key = policy_identity_key(&ancestor, remainder.as_os_str().is_empty())?;
    let remaining_components = policy_components(&remainder);
    let display_path = if remainder.as_os_str().is_empty() {
        match (ancestor.parent(), ancestor.file_name()) {
            (Some(parent), Some(name)) => std::fs::canonicalize(parent)?.join(name),
            _ => std::fs::canonicalize(&ancestor)?,
        }
    } else {
        std::fs::canonicalize(&ancestor)?.join(&remainder)
    };
    Ok(ResolvedPolicyFence {
        identity_key,
        remaining_components,
        display_path,
    })
}

fn policy_fences_allow(fences: &[ResolvedPolicyFence], path: &Path) -> io::Result<bool> {
    let mut candidate = path.to_path_buf();
    let mut remainder = PathBuf::new();
    loop {
        match std::fs::symlink_metadata(&candidate) {
            Ok(_) => {
                let identity = policy_identity_key(&candidate, remainder.as_os_str().is_empty())?;
                let components = policy_components(&remainder);
                let clean = !has_intermediate_reparse(&candidate, &remainder)?;
                if clean
                    && fences.iter().any(|scope| {
                        scope.identity_key == identity
                            && components.starts_with(&scope.remaining_components)
                    })
                {
                    return Ok(true);
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        let Some(name) = candidate.file_name() else {
            return Ok(false);
        };
        remainder = PathBuf::from(name).join(remainder);
        let Some(parent) = candidate.parent() else {
            return Ok(false);
        };
        if parent == candidate {
            return Ok(false);
        }
        candidate = parent.to_path_buf();
    }
}

fn policy_components(path: &Path) -> Vec<String> {
    path.components()
        .filter_map(|component| match component {
            Component::Normal(value) => {
                let rendered = value.to_string_lossy().into_owned();
                #[cfg(windows)]
                let rendered = rendered.to_lowercase();
                Some(rendered)
            }
            _ => None,
        })
        .collect()
}

#[cfg(windows)]
fn policy_identity_key(path: &Path, leaf: bool) -> io::Result<String> {
    use std::mem::size_of;
    use std::os::windows::fs::OpenOptionsExt;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_ID_INFO, FILE_SHARE_DELETE,
        FILE_SHARE_READ, FILE_SHARE_WRITE, FileIdInfo, GetFileInformationByHandleEx,
    };

    let flags = FILE_FLAG_BACKUP_SEMANTICS
        | if leaf {
            FILE_FLAG_OPEN_REPARSE_POINT
        } else {
            0
        };
    let file = std::fs::OpenOptions::new()
        .access_mode(0)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
        .custom_flags(flags)
        .open(path)?;
    let mut info = FILE_ID_INFO::default();
    // SAFETY: the handle is valid and info is an exactly sized writable output buffer.
    if unsafe {
        GetFileInformationByHandleEx(
            file.as_raw_handle(),
            FileIdInfo,
            (&raw mut info).cast(),
            size_of::<FILE_ID_INFO>() as u32,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(format!(
        "{:016x}:{}",
        info.VolumeSerialNumber,
        info.FileId
            .Identifier
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    ))
}

#[cfg(not(windows))]
fn policy_identity_key(path: &Path, _leaf: bool) -> io::Result<String> {
    Ok(std::fs::canonicalize(path)?.to_string_lossy().into_owned())
}

#[cfg(windows)]
fn has_intermediate_reparse(ancestor: &Path, remainder: &Path) -> io::Result<bool> {
    use std::os::windows::fs::MetadataExt;
    use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;

    let components = remainder
        .components()
        .filter_map(|component| match component {
            Component::Normal(value) => Some(value.to_owned()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let mut current = ancestor.to_path_buf();
    for component in components.iter().take(components.len().saturating_sub(1)) {
        current.push(component);
        match std::fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 => {
                return Ok(true);
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error),
        }
    }
    Ok(false)
}

#[cfg(not(windows))]
fn has_intermediate_reparse(_ancestor: &Path, _remainder: &Path) -> io::Result<bool> {
    Ok(false)
}

#[cfg(windows)]
fn resolve_fence(path: &Path) -> io::Result<Vec<String>> {
    use std::mem::size_of;
    use std::os::windows::fs::OpenOptionsExt;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_ID_INFO, FILE_SHARE_DELETE,
        FILE_SHARE_READ, FILE_SHARE_WRITE, FileIdInfo, GetFileInformationByHandleEx,
    };

    let (ancestor, remainder) = existing_ancestor(path)?;
    let flags = if remainder.as_os_str().is_empty() {
        FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT
    } else {
        // The ancestor is an intermediate component. Follow it; only a replaceable leaf is
        // opened as the reparse object itself.
        FILE_FLAG_BACKUP_SEMANTICS
    };
    let file = std::fs::OpenOptions::new()
        .access_mode(0)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
        .custom_flags(flags)
        .open(&ancestor)?;
    let mut info = FILE_ID_INFO::default();
    // SAFETY: the handle is owned and valid; info is an exactly sized writable output buffer.
    if unsafe {
        GetFileInformationByHandleEx(
            file.as_raw_handle(),
            FileIdInfo,
            (&raw mut info).cast(),
            size_of::<FILE_ID_INFO>() as u32,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    let stable = format!(
        "identity:{:016x}:{}:{}",
        info.VolumeSerialNumber,
        info.FileId
            .Identifier
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>(),
        canonical_remainder(&remainder).to_lowercase()
    );
    Ok(vec![stable, canonical_path_key(&ancestor, &remainder)?])
}

#[cfg(not(windows))]
fn resolve_fence(path: &Path) -> io::Result<Vec<String>> {
    let (ancestor, remainder) = existing_ancestor(path)?;
    Ok(vec![canonical_path_key(&ancestor, &remainder)?])
}

fn canonical_path_key(ancestor: &Path, remainder: &Path) -> io::Result<String> {
    let canonical = if remainder.as_os_str().is_empty() {
        match (ancestor.parent(), ancestor.file_name()) {
            (Some(parent), Some(name)) => std::fs::canonicalize(parent)?.join(name),
            _ => std::fs::canonicalize(ancestor)?,
        }
    } else {
        std::fs::canonicalize(ancestor)?.join(remainder)
    };
    let rendered = canonical.to_string_lossy();
    #[cfg(windows)]
    let rendered = rendered.to_lowercase();
    Ok(format!("path:{rendered}"))
}

fn existing_ancestor(path: &Path) -> io::Result<(PathBuf, PathBuf)> {
    let mut ancestor = path.to_path_buf();
    let mut remainder = PathBuf::new();
    loop {
        match std::fs::symlink_metadata(&ancestor) {
            Ok(_) => return Ok((ancestor, remainder)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let name = ancestor.file_name().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        "path fence has no existing ancestor",
                    )
                })?;
                remainder = PathBuf::from(name).join(remainder);
                ancestor = ancestor
                    .parent()
                    .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "path has no parent"))?
                    .to_path_buf();
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(windows)]
fn canonical_remainder(path: &Path) -> String {
    path.components()
        .filter_map(|component| match component {
            Component::Normal(value) => Some(value.to_string_lossy().into_owned()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observed_headroom_is_checked_and_excludes_only_the_supplied_debit() {
        assert!(observed_resource_blocker("ram_mb", 8_000, 12_000, 500, 0).is_none());
        assert_eq!(
            observed_resource_blocker("ram_mb", 8_000, 12_000, 500, 4_000)
                .unwrap()
                .code,
            "observed_resource_busy"
        );
        assert_eq!(
            observed_resource_blocker("ram_mb", 1, 10, 11, 0)
                .unwrap()
                .code,
            "observation_unusable"
        );
        assert_eq!(
            observed_resource_blocker("ram_mb", 1, 10, 0, 11)
                .unwrap()
                .code,
            "observation_unusable"
        );
    }

    #[test]
    fn complete_scalar_lease_never_partially_fits() {
        let capacities = ResourceCapacities {
            cpu_units: 4,
            ram_mb: 100,
            cargo_slots: 1,
            gpu_slots: 1,
            custom: BTreeMap::new(),
        };
        let active = ResolvedClaims {
            cpu_units: 2,
            ram_mb: 20,
            ..ResolvedClaims::default()
        };
        let requested = ResolvedClaims {
            cpu_units: 2,
            ram_mb: 90,
            ..ResolvedClaims::default()
        };
        let blockers = requested.blockers(&capacities, &[active], &BTreeMap::new());
        assert_eq!(blockers.len(), 1);
        assert!(blockers[0].detail.starts_with("ram_mb:"));
    }

    #[test]
    fn granted_debit_overflow_blocks_instead_of_wrapping_capacity() {
        let capacities = ResourceCapacities {
            ram_mb: u64::MAX,
            ..Default::default()
        };
        let requested = ResolvedClaims {
            ram_mb: 1,
            ..ResolvedClaims::default()
        };
        let active = [
            ResolvedClaims {
                ram_mb: u64::MAX,
                ..ResolvedClaims::default()
            },
            ResolvedClaims {
                ram_mb: 1,
                ..ResolvedClaims::default()
            },
        ];
        let blockers = requested.blockers(&capacities, &active, &BTreeMap::new());
        assert_eq!(blockers.len(), 1);
        assert_eq!(blockers[0].code, "resource_busy");
        assert!(blockers[0].detail.contains("sum overflow"));
    }

    #[test]
    fn shared_fences_overlap_but_exclusive_conflicts() {
        let fence = "stable:fence".to_owned();
        let shared = ResolvedClaims {
            shared_fences: [fence.clone()].into(),
            ..ResolvedClaims::default()
        };
        assert!(
            shared
                .blockers(
                    &ResourceCapacities::default(),
                    std::slice::from_ref(&shared),
                    &BTreeMap::new(),
                )
                .is_empty()
        );
        let exclusive = ResolvedClaims {
            exclusive_fences: [fence].into(),
            ..ResolvedClaims::default()
        };
        assert_eq!(
            exclusive
                .blockers(&ResourceCapacities::default(), &[shared], &BTreeMap::new())
                .first()
                .map(|blocker| blocker.code.as_str()),
            Some("path_fence_busy")
        );
    }

    #[test]
    fn managed_wait_checks_only_components_retained_by_ancestors() {
        let capacities = ResourceCapacities {
            cpu_units: 4,
            ram_mb: 100,
            cargo_slots: 2,
            gpu_slots: 1,
            custom: BTreeMap::new(),
        };
        let ancestor = ResolvedClaims {
            cargo_slots: 1,
            shared_fences: ["shared".into()].into(),
            exclusive_fences: ["exclusive".into()].into(),
            ..ResolvedClaims::default()
        };
        let orthogonal = ResolvedClaims {
            cargo_slots: 1,
            gpu_slots: 1,
            shared_fences: ["shared".into()].into(),
            ..ResolvedClaims::default()
        };
        assert!(
            orthogonal
                .ancestor_blockers(
                    &capacities,
                    std::slice::from_ref(&ancestor),
                    &BTreeMap::new(),
                )
                .is_empty()
        );

        let blocked = ResolvedClaims {
            cargo_slots: 2,
            shared_fences: ["exclusive".into()].into(),
            ..ResolvedClaims::default()
        };
        let blockers = blocked.ancestor_blockers(&capacities, &[ancestor], &BTreeMap::new());
        assert_eq!(blockers.len(), 2);
        assert!(
            blockers
                .iter()
                .all(|blocker| blocker.code == "blocked_by_ancestor")
        );

        let impossible = ResolvedClaims {
            cargo_slots: 3,
            ..ResolvedClaims::default()
        };
        assert_eq!(
            impossible
                .ancestor_blockers(&capacities, &[], &BTreeMap::new())
                .first()
                .map(|blocker| blocker.code.as_str()),
            Some("resource_capacity")
        );
    }

    #[test]
    fn different_missing_children_under_one_ancestor_do_not_alias() {
        let temp = tempfile::tempdir().unwrap();
        let first = ResourceClaims {
            exclusive_fences: vec![temp.path().join("first").to_string_lossy().into_owned()],
            ..ResourceClaims::default()
        };
        let second = ResourceClaims {
            exclusive_fences: vec![temp.path().join("second").to_string_lossy().into_owned()],
            ..ResourceClaims::default()
        };
        let first = ResolvedClaims::resolve(&first).unwrap();
        let second = ResolvedClaims::resolve(&second).unwrap();
        assert!(
            first
                .blockers(&ResourceCapacities::default(), &[second], &BTreeMap::new(),)
                .is_empty()
        );
    }

    #[test]
    fn existing_policy_root_replacement_does_not_inherit_object_authority() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("authority");
        std::fs::create_dir_all(&root).unwrap();
        let policy = ChildSubmissionPolicy {
            fences: crate::ChildFencePolicy {
                shared_roots: vec![root.clone()],
                exclusive_roots: Vec::new(),
            },
            ..Default::default()
        };
        let resolved = ResolvedChildSubmissionPolicy::resolve(&policy).unwrap();
        let retained = temp.path().join("retained-object");
        std::fs::rename(&root, &retained).unwrap();
        std::fs::create_dir_all(&root).unwrap();

        assert!(!resolved.allows_shared(&root.join("child")).unwrap());
        assert!(resolved.allows_shared(&retained.join("child")).unwrap());
    }

    #[test]
    fn missing_policy_root_retains_component_authority_after_creation() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("future-authority");
        let policy = ChildSubmissionPolicy {
            fences: crate::ChildFencePolicy {
                shared_roots: vec![root.clone()],
                exclusive_roots: Vec::new(),
            },
            ..Default::default()
        };
        let resolved = ResolvedChildSubmissionPolicy::resolve(&policy).unwrap();
        std::fs::create_dir_all(&root).unwrap();

        assert!(resolved.allows_shared(&root.join("child")).unwrap());
        assert!(
            !resolved
                .allows_shared(&temp.path().join("sibling"))
                .unwrap()
        );
    }

    #[test]
    fn child_policy_vram_claim_names_are_canonicalized() {
        let policy = ChildSubmissionPolicy {
            max_claims: crate::ResourceClaimLimits {
                custom: [("vram_mb:GPU-AbC123".into(), 4096)].into(),
                ..Default::default()
            },
            ..Default::default()
        };

        let resolved = ResolvedChildSubmissionPolicy::resolve(&policy).unwrap();

        assert_eq!(
            resolved.policy.max_claims.custom,
            [("vram_mb:gpu-abc123".into(), 4096)].into()
        );
    }

    #[test]
    fn impact_rules_are_symmetric_self_compatibility_is_explicit_and_ancestors_block() {
        let rules: BTreeMap<String, Vec<String>> = [(
            "measurement".into(),
            vec!["cpu_heavy".into(), "gpu_heavy".into()],
        )]
        .into();
        let cpu = ResolvedClaims {
            impacts: ["cpu_heavy".into()].into(),
            ..ResolvedClaims::default()
        };
        assert!(
            cpu.blockers(
                &ResourceCapacities::default(),
                std::slice::from_ref(&cpu),
                &rules,
            )
            .is_empty(),
            "cpu_heavy is self-compatible unless configured otherwise"
        );
        let measurement = ResolvedClaims {
            impacts: ["measurement".into()].into(),
            ..ResolvedClaims::default()
        };
        assert_eq!(
            measurement
                .blockers(
                    &ResourceCapacities::default(),
                    std::slice::from_ref(&cpu),
                    &rules,
                )
                .first()
                .map(|blocker| blocker.code.as_str()),
            Some("impact_busy")
        );
        assert_eq!(
            measurement
                .ancestor_blockers(&ResourceCapacities::default(), &[cpu], &rules)
                .first()
                .map(|blocker| blocker.code.as_str()),
            Some("blocked_by_ancestor")
        );
    }
}
