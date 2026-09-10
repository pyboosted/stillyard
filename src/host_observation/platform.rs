//! Provider selection is a platform boundary; common admission never assumes
//! that successfully compiling an unsupported provider means usable evidence.

#[cfg(windows)]
pub(crate) use super::{
    windows_disk::DiskUtilizationSampler,
    windows_memory::probe_memory,
    windows_nvml::NvmlProvider,
    windows_process::probe_processes,
    windows_utilization::{CpuUtilizationSampler, observation_clock},
};

#[cfg(target_os = "linux")]
pub(crate) use super::{
    linux_cpu::CpuUtilizationSampler, linux_disk::DiskUtilizationSampler,
    linux_memory::probe_memory, linux_process::probe_processes,
};
#[cfg(not(any(windows, target_os = "linux")))]
pub(crate) use unsupported::*;
#[cfg(target_os = "linux")]
pub(crate) use unsupported::{NvmlProvider, observation_clock};

pub(super) fn provider_name(detector: &str) -> &'static str {
    match detector {
        "detector.physical_memory" | "detector.commit_headroom" => {
            if cfg!(windows) {
                "windows_memory"
            } else if cfg!(target_os = "linux") {
                "linux_memavailable"
            } else {
                "unsupported_memory"
            }
        }
        "detector.cpu" => {
            if cfg!(windows) {
                "windows_system_times"
            } else if cfg!(target_os = "linux") {
                "linux_proc_stat"
            } else {
                "unsupported_cpu"
            }
        }
        "detector.disk" => {
            if cfg!(windows) {
                "windows_disk_performance"
            } else if cfg!(target_os = "linux") {
                "linux_diskstats_pressure"
            } else {
                "unsupported_disk"
            }
        }
        "detector.processes" | "detector.process_rules" => {
            if cfg!(windows) {
                "windows_toolhelp"
            } else if cfg!(target_os = "linux") {
                "linux_visible_pid_namespace"
            } else {
                "unsupported_processes"
            }
        }
        "detector.nvml" => "nvml",
        code if code.starts_with("detector.gpu_") => "nvml",
        "detector.sampler_freshness" => "host_sampler",
        _ => "host_observation",
    }
}

#[cfg(not(windows))]
mod unsupported {
    use super::super::{
        ComponentEvidence, ComponentValue, GpuEvidence, MemoryEvidence, ProcessEvidence,
    };
    use std::{collections::BTreeMap, io};

    pub(crate) fn observation_clock() -> io::Result<(i64, u64)> {
        #[cfg(target_os = "linux")]
        {
            use std::time::{SystemTime, UNIX_EPOCH};
            // /proc/uptime is the kernel boot clock, shared by all local observations.
            let uptime = std::fs::read_to_string("/proc/uptime")?;
            let value = uptime
                .split_whitespace()
                .next()
                .ok_or_else(|| io::Error::other("missing boot clock"))?;
            let (seconds, fraction) = value
                .split_once('.')
                .ok_or_else(|| io::Error::other("invalid boot clock"))?;
            let seconds: u64 = seconds.parse().map_err(io::Error::other)?;
            let fraction = format!("{fraction:0<3}");
            let millis: u64 = fraction
                .get(..3)
                .ok_or_else(|| io::Error::other("invalid boot clock fraction"))?
                .parse()
                .map_err(io::Error::other)?;
            let boot = seconds
                .checked_mul(1000)
                .and_then(|seconds| seconds.checked_add(millis))
                .ok_or_else(|| io::Error::other("boot clock overflow"))?;
            let wall = i64::try_from(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(io::Error::other)?
                    .as_millis(),
            )
            .map_err(io::Error::other)?;
            Ok((wall, boot))
        }
        #[cfg(not(target_os = "linux"))]
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "platform observation clock is unavailable",
        ))
    }

    pub(crate) fn probe_memory() -> io::Result<MemoryEvidence> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "platform memory provider is not implemented",
        ))
    }

    pub(crate) fn probe_processes() -> io::Result<Vec<ProcessEvidence>> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "platform process provider is not implemented",
        ))
    }

    #[derive(Default)]
    pub(crate) struct CpuUtilizationSampler {}
    #[derive(Default)]
    pub(crate) struct DiskUtilizationSampler {}

    macro_rules! unavailable_sampler {
        ($name:ident, $message:literal) => {
            impl $name {
                pub(crate) fn sample(
                    &mut self,
                    wall: i64,
                    monotonic: u64,
                ) -> ComponentEvidence<u8> {
                    ComponentEvidence {
                        captured_unix_millis: wall,
                        captured_monotonic_millis: monotonic,
                        value: ComponentValue::Unavailable($message.into()),
                    }
                }
                pub(crate) fn reset(&mut self) {}
            }
        };
    }
    unavailable_sampler!(CpuUtilizationSampler, "unsupported CPU provider");
    unavailable_sampler!(DiskUtilizationSampler, "unsupported disk provider");

    pub(crate) struct NvmlProvider {}
    impl NvmlProvider {
        pub(crate) fn load() -> Result<Self, String> {
            Err("unsupported NVML provider".into())
        }
        pub(crate) fn sample(
            &self,
            _: &[ProcessEvidence],
        ) -> Result<BTreeMap<String, GpuEvidence>, String> {
            Err("unsupported NVML provider".into())
        }
    }
}
