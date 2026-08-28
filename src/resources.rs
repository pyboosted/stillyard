use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::{Blocker, ResourceCapacities, ResourceClaims};

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
}

impl ResolvedClaims {
    pub(crate) fn resolve(claims: &ResourceClaims) -> io::Result<Self> {
        let resolved = Self {
            cpu_units: u64::from(claims.cpu_units.unwrap_or(0)),
            ram_mb: claims.ram_mb.unwrap_or(0),
            cargo_slots: u64::from(claims.cargo_slots.unwrap_or(0)),
            gpu_slots: u64::from(claims.gpu_slots.unwrap_or(0)),
            custom: claims.custom.clone(),
            shared_fences: resolve_fences(&claims.shared_fences)?,
            exclusive_fences: resolve_fences(&claims.exclusive_fences)?,
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
    ) -> Vec<Blocker> {
        let mut blockers = Vec::new();
        scalar_blocker(
            &mut blockers,
            "cpu_units",
            self.cpu_units,
            u64::from(capacities.cpu_units),
            active.iter().map(|claim| claim.cpu_units).sum(),
        );
        scalar_blocker(
            &mut blockers,
            "ram_mb",
            self.ram_mb,
            capacities.ram_mb,
            active.iter().map(|claim| claim.ram_mb).sum(),
        );
        scalar_blocker(
            &mut blockers,
            "cargo_slots",
            self.cargo_slots,
            u64::from(capacities.cargo_slots),
            active.iter().map(|claim| claim.cargo_slots).sum(),
        );
        scalar_blocker(
            &mut blockers,
            "gpu_slots",
            self.gpu_slots,
            u64::from(capacities.gpu_slots),
            active.iter().map(|claim| claim.gpu_slots).sum(),
        );
        for (name, requested) in &self.custom {
            scalar_blocker(
                &mut blockers,
                name,
                *requested,
                capacities.custom.get(name).copied().unwrap_or(0),
                active
                    .iter()
                    .map(|claim| claim.custom.get(name).copied().unwrap_or(0))
                    .sum(),
            );
        }
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
        }
        blockers.sort_by(|left, right| {
            left.code
                .cmp(&right.code)
                .then(left.detail.cmp(&right.detail))
        });
        blockers.dedup();
        blockers
    }

    /// Reports only conflicts that exist because authenticated ancestors retain Leases.
    /// Unrelated active Jobs are intentionally excluded: they can finish while the caller waits.
    pub(crate) fn ancestor_blockers(
        &self,
        capacities: &ResourceCapacities,
        ancestors: &[Self],
    ) -> Vec<Blocker> {
        let mut blockers = Vec::new();
        ancestor_scalar_blocker(
            &mut blockers,
            "cpu_units",
            self.cpu_units,
            u64::from(capacities.cpu_units),
            ancestors.iter().map(|claim| claim.cpu_units).sum(),
        );
        ancestor_scalar_blocker(
            &mut blockers,
            "ram_mb",
            self.ram_mb,
            capacities.ram_mb,
            ancestors.iter().map(|claim| claim.ram_mb).sum(),
        );
        ancestor_scalar_blocker(
            &mut blockers,
            "cargo_slots",
            self.cargo_slots,
            u64::from(capacities.cargo_slots),
            ancestors.iter().map(|claim| claim.cargo_slots).sum(),
        );
        ancestor_scalar_blocker(
            &mut blockers,
            "gpu_slots",
            self.gpu_slots,
            u64::from(capacities.gpu_slots),
            ancestors.iter().map(|claim| claim.gpu_slots).sum(),
        );
        for (name, requested) in &self.custom {
            ancestor_scalar_blocker(
                &mut blockers,
                name,
                *requested,
                capacities.custom.get(name).copied().unwrap_or(0),
                ancestors
                    .iter()
                    .map(|claim| claim.custom.get(name).copied().unwrap_or(0))
                    .sum(),
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

fn ancestor_scalar_blocker(
    blockers: &mut Vec<Blocker>,
    name: &str,
    requested: u64,
    capacity: u64,
    retained_by_ancestors: u64,
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
    granted: u64,
) {
    if requested == 0 {
        return;
    }
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

fn fence_blocker(blockers: &mut Vec<Blocker>, fence: &str) {
    blockers.push(Blocker {
        code: "path_fence_busy".into(),
        detail: fence.to_owned(),
    });
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
        let blockers = requested.blockers(&capacities, &[active]);
        assert_eq!(blockers.len(), 1);
        assert!(blockers[0].detail.starts_with("ram_mb:"));
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
                    std::slice::from_ref(&shared)
                )
                .is_empty()
        );
        let exclusive = ResolvedClaims {
            exclusive_fences: [fence].into(),
            ..ResolvedClaims::default()
        };
        assert_eq!(
            exclusive
                .blockers(&ResourceCapacities::default(), &[shared])
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
                .ancestor_blockers(&capacities, std::slice::from_ref(&ancestor))
                .is_empty()
        );

        let blocked = ResolvedClaims {
            cargo_slots: 2,
            shared_fences: ["exclusive".into()].into(),
            ..ResolvedClaims::default()
        };
        let blockers = blocked.ancestor_blockers(&capacities, &[ancestor]);
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
                .ancestor_blockers(&capacities, &[])
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
                .blockers(&ResourceCapacities::default(), &[second])
                .is_empty()
        );
    }
}
