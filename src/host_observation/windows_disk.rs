use std::collections::BTreeMap;
use std::ffi::c_void;
use std::mem::size_of;
use std::ptr::{null, null_mut};

use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, INVALID_HANDLE_VALUE};
use windows_sys::Win32::Storage::FileSystem::{
    CreateFileW, FILE_SHARE_READ, FILE_SHARE_WRITE, OPEN_EXISTING,
};
use windows_sys::Win32::System::IO::DeviceIoControl;
use windows_sys::Win32::System::Ioctl::{DISK_PERFORMANCE, IOCTL_DISK_PERFORMANCE};

use super::{ComponentEvidence, ComponentValue};

const MAX_PHYSICAL_DRIVES: u32 = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DiskCounter {
    query_time: u64,
    idle_time: u64,
}

#[derive(Default)]
pub(crate) struct DiskUtilizationSampler {
    previous: Option<BTreeMap<u32, DiskCounter>>,
}

impl DiskUtilizationSampler {
    pub(crate) fn sample(
        &mut self,
        captured_unix_millis: i64,
        captured_monotonic_millis: u64,
    ) -> ComponentEvidence<u8> {
        let current = match disk_counters() {
            Ok(current) => current,
            Err(error) => {
                self.previous = None;
                return ComponentEvidence {
                    captured_unix_millis,
                    captured_monotonic_millis,
                    value: ComponentValue::Error(error.to_string()),
                };
            }
        };
        let value = self.previous.replace(current.clone()).map_or_else(
            || ComponentValue::Unavailable("warming_up".into()),
            |previous| utilization(&previous, &current),
        );
        ComponentEvidence {
            captured_unix_millis,
            captured_monotonic_millis,
            value,
        }
    }

    pub(crate) fn reset(&mut self) {
        self.previous = None;
    }
}

fn utilization(
    previous: &BTreeMap<u32, DiskCounter>,
    current: &BTreeMap<u32, DiskCounter>,
) -> ComponentValue<u8> {
    if previous.keys().ne(current.keys()) {
        return ComponentValue::Unavailable("disk_topology_changed".into());
    }
    let mut query_delta = 0_u128;
    let mut busy_delta = 0_u128;
    for (number, current) in current {
        let Some(previous) = previous.get(number) else {
            return ComponentValue::Unavailable("disk_topology_changed".into());
        };
        let Some(query) = current.query_time.checked_sub(previous.query_time) else {
            return ComponentValue::Unavailable("disk_counter_reset".into());
        };
        let Some(idle) = current.idle_time.checked_sub(previous.idle_time) else {
            return ComponentValue::Unavailable("disk_counter_reset".into());
        };
        let Some(busy) = query.checked_sub(idle) else {
            return ComponentValue::Unavailable("disk_counter_inconsistent".into());
        };
        query_delta += u128::from(query);
        busy_delta += u128::from(busy);
    }
    if query_delta == 0 {
        return ComponentValue::Unavailable("disk_counter_did_not_advance".into());
    }
    let percent = (busy_delta * 100 / query_delta).min(100) as u8;
    ComponentValue::Available(percent)
}

fn disk_counters() -> std::io::Result<BTreeMap<u32, DiskCounter>> {
    let mut counters = BTreeMap::new();
    for index in 0..MAX_PHYSICAL_DRIVES {
        let path = format!(r"\\.\PhysicalDrive{index}")
            .encode_utf16()
            .chain(std::iter::once(0))
            .collect::<Vec<_>>();
        // SAFETY: path is NUL terminated; null security/template pointers request defaults.
        let handle = unsafe {
            CreateFileW(
                path.as_ptr(),
                0,
                FILE_SHARE_READ | FILE_SHARE_WRITE,
                null(),
                OPEN_EXISTING,
                0,
                null_mut(),
            )
        };
        if handle == INVALID_HANDLE_VALUE {
            let error = std::io::Error::last_os_error();
            if matches!(error.raw_os_error(), Some(2 | 3)) {
                continue;
            }
            return Err(error);
        }
        let handle = OwnedHandle(handle);
        let mut performance = DISK_PERFORMANCE::default();
        let mut returned = 0_u32;
        // SAFETY: handle is open and performance is an exactly sized writable output buffer.
        if unsafe {
            DeviceIoControl(
                handle.0,
                IOCTL_DISK_PERFORMANCE,
                null(),
                0,
                (&raw mut performance).cast::<c_void>(),
                size_of::<DISK_PERFORMANCE>() as u32,
                &raw mut returned,
                null_mut(),
            )
        } == 0
        {
            return Err(std::io::Error::last_os_error());
        }
        if returned < size_of::<DISK_PERFORMANCE>() as u32 {
            return Err(std::io::Error::other(
                "IOCTL_DISK_PERFORMANCE returned a short structure",
            ));
        }
        let query_time = u64::try_from(performance.QueryTime)
            .map_err(|_| std::io::Error::other("negative disk query counter"))?;
        let idle_time = u64::try_from(performance.IdleTime)
            .map_err(|_| std::io::Error::other("negative disk idle counter"))?;
        if counters
            .insert(
                performance.StorageDeviceNumber,
                DiskCounter {
                    query_time,
                    idle_time,
                },
            )
            .is_some()
        {
            return Err(std::io::Error::other(
                "duplicate physical disk device number",
            ));
        }
    }
    if counters.is_empty() {
        return Err(std::io::Error::other(
            "no physical disk performance counters are available",
        ));
    }
    Ok(counters)
}

struct OwnedHandle(HANDLE);

impl Drop for OwnedHandle {
    fn drop(&mut self) {
        // SAFETY: this guard uniquely owns a valid file handle.
        unsafe { CloseHandle(self.0) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_disk_delta_is_warming_up_or_honestly_unavailable() {
        let mut sampler = DiskUtilizationSampler::default();
        let sample = sampler.sample(10, 10);
        assert!(
            matches!(
                sample.value,
                ComponentValue::Unavailable(ref detail) if detail == "warming_up"
            ) || matches!(sample.value, ComponentValue::Error(_))
        );
    }
}
