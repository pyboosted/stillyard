use std::time::{SystemTime, UNIX_EPOCH};

use windows_sys::Win32::Foundation::FILETIME;
use windows_sys::Win32::System::Threading::GetSystemTimes;
use windows_sys::Win32::System::WindowsProgramming::QueryUnbiasedInterruptTimePrecise;

use super::{ComponentEvidence, ComponentValue};

#[derive(Clone, Copy)]
struct CpuTimes {
    idle: u64,
    total: u64,
}

#[derive(Default)]
pub(crate) struct CpuUtilizationSampler {
    previous: Option<CpuTimes>,
}

impl CpuUtilizationSampler {
    pub(crate) fn sample(
        &mut self,
        captured_unix_millis: i64,
        captured_monotonic_millis: u64,
    ) -> ComponentEvidence<u8> {
        let current = match system_times() {
            Ok(current) => current,
            Err(error) => {
                return ComponentEvidence {
                    captured_unix_millis,
                    captured_monotonic_millis,
                    value: ComponentValue::Error(error.to_string()),
                };
            }
        };
        let value = self.previous.replace(current).map_or_else(
            || ComponentValue::Unavailable("warming_up".into()),
            |previous| match (
                current.total.checked_sub(previous.total),
                current.idle.checked_sub(previous.idle),
            ) {
                (Some(total), Some(idle)) if total > 0 && idle <= total => {
                    let busy = total - idle;
                    let percent = ((u128::from(busy) * 100) / u128::from(total)) as u8;
                    ComponentValue::Available(percent)
                }
                _ => ComponentValue::Unavailable("counter_reset".into()),
            },
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

pub(crate) fn observation_clock() -> std::io::Result<(i64, u64)> {
    let wall = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX);
    let mut unbiased_100ns = 0_u64;
    // SAFETY: the output pointer is valid and writable.
    unsafe { QueryUnbiasedInterruptTimePrecise(&raw mut unbiased_100ns) };
    Ok((wall, unbiased_100ns / 10_000))
}

fn system_times() -> std::io::Result<CpuTimes> {
    let mut idle = FILETIME::default();
    let mut kernel = FILETIME::default();
    let mut user = FILETIME::default();
    // SAFETY: all three FILETIME outputs are valid and writable.
    if unsafe { GetSystemTimes(&raw mut idle, &raw mut kernel, &raw mut user) } == 0 {
        return Err(std::io::Error::last_os_error());
    }
    let idle = filetime_value(idle);
    let kernel = filetime_value(kernel);
    let user = filetime_value(user);
    Ok(CpuTimes {
        idle,
        total: kernel
            .checked_add(user)
            .ok_or_else(|| std::io::Error::other("CPU time overflow"))?,
    })
}

fn filetime_value(value: FILETIME) -> u64 {
    (u64::from(value.dwHighDateTime) << 32) | u64::from(value.dwLowDateTime)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_cpu_delta_is_warming_up() {
        let (wall, monotonic) = observation_clock().unwrap();
        let mut sampler = CpuUtilizationSampler::default();
        assert!(matches!(
            sampler.sample(wall, monotonic).value,
            ComponentValue::Unavailable(ref detail) if detail == "warming_up"
        ));
    }
}
