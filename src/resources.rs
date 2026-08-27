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
            shared_fences: claims
                .shared_fences
                .iter()
                .map(|path| resolve_fence(Path::new(path)))
                .collect::<io::Result<_>>()?,
            exclusive_fences: claims
                .exclusive_fences
                .iter()
                .map(|path| resolve_fence(Path::new(path)))
                .collect::<io::Result<_>>()?,
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
fn resolve_fence(path: &Path) -> io::Result<String> {
    use std::mem::size_of;
    use std::os::windows::fs::OpenOptionsExt;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_ID_INFO, FileIdInfo,
        GetFileInformationByHandleEx,
    };

    let (ancestor, remainder) = existing_ancestor(path)?;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .share_mode(0x1 | 0x2 | 0x4)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
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
    Ok(format!(
        "{:016x}:{}:{}",
        info.VolumeSerialNumber,
        info.FileId
            .Identifier
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>(),
        canonical_remainder(&remainder).to_lowercase()
    ))
}

#[cfg(not(windows))]
fn resolve_fence(path: &Path) -> io::Result<String> {
    let (ancestor, remainder) = existing_ancestor(path)?;
    Ok(format!(
        "{}:{}",
        std::fs::canonicalize(ancestor)?.display(),
        canonical_remainder(&remainder)
    ))
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
}
