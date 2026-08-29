mod evidence;
mod policy;
mod service;
#[cfg(windows)]
mod windows_disk;
#[cfg(windows)]
mod windows_memory;
#[cfg(windows)]
mod windows_nvml;
#[cfg(windows)]
mod windows_process;
#[cfg(windows)]
mod windows_utilization;

pub(crate) use evidence::{
    ComponentEvidence, ComponentValue, GpuEvidence, HostSample, MemoryEvidence, ProcessEvidence,
};
pub(crate) use policy::{AdmissionContext, evaluate_admission};
pub(crate) use service::{HostObservationRequirements, HostObservationService};
#[cfg(windows)]
pub(crate) use windows_disk::DiskUtilizationSampler;
#[cfg(windows)]
pub(crate) use windows_memory::probe_memory;
#[cfg(windows)]
pub(crate) use windows_nvml::NvmlProvider;
#[cfg(windows)]
pub(crate) use windows_process::probe_processes;
#[cfg(windows)]
pub(crate) use windows_utilization::{CpuUtilizationSampler, observation_clock};

#[derive(Clone, Copy)]
pub(crate) struct ObservationMoment<'a> {
    pub(crate) sample: &'a HostSample,
    pub(crate) now_unix_millis: i64,
    pub(crate) now_monotonic_millis: u64,
}
