use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard};

use uuid::Uuid;

use super::{
    ComponentEvidence, ComponentValue, CpuUtilizationSampler, DiskUtilizationSampler, GpuEvidence,
    HostSample, NvmlProvider, ProcessEvidence, observation_clock, probe_memory, probe_processes,
};
use crate::{
    DoctorCheck, DoctorCheckStatus, DoctorCoverage, GpuProviderConfig, HostObservationConfig,
};

pub(crate) struct HostObservationService {
    config: HostObservationConfig,
    release_barrier: Mutex<()>,
    state: Mutex<ServiceState>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct HostObservationRequirements {
    pub(crate) memory: bool,
    pub(crate) cpu: bool,
    pub(crate) disk: bool,
    pub(crate) processes: bool,
    pub(crate) gpu: bool,
    pub(crate) gpu_uuid: Option<String>,
}

struct ServiceState {
    observation_generation: Uuid,
    last_clock: Option<(i64, u64)>,
    gpu_fingerprint: Option<Vec<(String, String)>>,
    nvml: Option<NvmlProvider>,
    nvml_initialization_attempted: bool,
    cpu: CpuUtilizationSampler,
    disk: DiskUtilizationSampler,
}

impl HostObservationService {
    pub(crate) fn new(config: HostObservationConfig) -> Self {
        Self {
            config,
            release_barrier: Mutex::new(()),
            state: Mutex::new(ServiceState {
                observation_generation: Uuid::now_v7(),
                last_clock: None,
                gpu_fingerprint: None,
                nvml: None,
                nvml_initialization_attempted: false,
                cpu: CpuUtilizationSampler::default(),
                disk: DiskUtilizationSampler::default(),
            }),
        }
    }

    /// Takes one synchronous sample without holding any Store lock.
    pub(crate) fn sample_now(&self) -> Result<HostSample, String> {
        let _barrier = self
            .release_barrier
            .lock()
            .map_err(|_| "observation release barrier poisoned".to_owned())?;
        self.sample_under_barrier(None)
    }

    pub(crate) fn sample_interval_millis(&self) -> u64 {
        self.config.sample_interval_millis
    }

    pub(crate) fn release_discontinuity_limit_millis(&self) -> u64 {
        self.config.generation_max_cadence_gap_millis
    }

    pub(crate) fn doctor_diagnostics(
        &self,
        required: HostObservationRequirements,
    ) -> (Vec<DoctorCheck>, Vec<DoctorCoverage>) {
        let sample = match self.sample_now() {
            Ok(sample) => sample,
            Err(error) => {
                let check = DoctorCheck {
                    code: "detector.observation_clock".into(),
                    status: DoctorCheckStatus::Fail,
                    summary: "host observation sample could not be captured".into(),
                    remediation: Some(error),
                };
                let coverage = DoctorCoverage {
                    provider: "host_clock".into(),
                    detector: "observation_clock".into(),
                    status: check.status.clone(),
                    observed_unix_millis: None,
                    observation_generation: None,
                    remediation: check.remediation.clone(),
                };
                return (vec![check], vec![coverage]);
            }
        };
        let mut checks = vec![
            component_check(
                "detector.physical_memory",
                "available physical memory",
                &sample.memory.value,
                required.memory,
            ),
            component_check(
                "detector.commit_headroom",
                "commit-limit headroom",
                &sample.memory.value,
                required.memory,
            ),
            component_check(
                "detector.cpu",
                "total CPU utilization",
                &sample.cpu_utilization.value,
                required.cpu,
            ),
            component_check(
                "detector.disk",
                "aggregate disk utilization",
                &sample.disk_utilization.value,
                required.disk,
            ),
            component_check(
                "detector.processes",
                "bounded process inventory",
                &sample.processes.value,
                required.processes,
            ),
            DoctorCheck {
                code: "detector.process_rules".into(),
                status: DoctorCheckStatus::Pass,
                summary: format!(
                    "process policy has {} block and {} ignore pattern(s)",
                    self.config.process_rules.block.len(),
                    self.config.process_rules.ignore.len()
                ),
                remediation: None,
            },
            DoctorCheck {
                code: "detector.sampler_freshness".into(),
                status: DoctorCheckStatus::Pass,
                summary: "synchronous observation sample is fresh".into(),
                remediation: None,
            },
        ];
        checks.push(match &sample.gpus.value {
            ComponentValue::Available(gpus) if !gpus.is_empty() => DoctorCheck {
                code: "detector.nvml".into(),
                status: DoctorCheckStatus::Pass,
                summary: format!(
                    "NVML covers {} GPU(s): {}",
                    gpus.len(),
                    gpus.values()
                        .map(|gpu| format!("{} driver {}", gpu.uuid, gpu.driver_version))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                remediation: None,
            },
            ComponentValue::Available(_) => DoctorCheck {
                code: "detector.nvml".into(),
                status: if required.gpu {
                    DoctorCheckStatus::Fail
                } else {
                    DoctorCheckStatus::Warning
                },
                summary: "NVML is available but reports no GPU devices".into(),
                remediation: required
                    .gpu
                    .then(|| "a retained GPU policy requires the configured GPU UUID".into()),
            },
            ComponentValue::Unavailable(detail) | ComponentValue::Error(detail) => DoctorCheck {
                code: "detector.nvml".into(),
                status: if required.gpu {
                    DoctorCheckStatus::Fail
                } else {
                    DoctorCheckStatus::Warning
                },
                summary: "NVML GPU evidence is unavailable".into(),
                remediation: Some(detail.clone()),
            },
        });
        if required.gpu {
            checks.push(gpu_placement_check(
                required.gpu_uuid.as_deref(),
                &sample.gpus.value,
            ));
        }
        if let ComponentValue::Available(gpus) = &sample.gpus.value {
            for gpu in gpus.values() {
                checks.extend([
                    DoctorCheck {
                        code: format!("detector.gpu_memory:{}", gpu.uuid),
                        status: DoctorCheckStatus::Pass,
                        summary: format!(
                            "GPU {} driver {} reports {} MiB free VRAM",
                            gpu.uuid, gpu.driver_version, gpu.free_memory_mb
                        ),
                        remediation: None,
                    },
                    DoctorCheck {
                        code: format!("detector.gpu_utilization:{}", gpu.uuid),
                        status: DoctorCheckStatus::Pass,
                        summary: format!(
                            "GPU {} utilization is {}%",
                            gpu.uuid, gpu.utilization_percent
                        ),
                        remediation: None,
                    },
                    DoctorCheck {
                        code: format!("detector.gpu_compute_processes:{}", gpu.uuid),
                        status: DoctorCheckStatus::Pass,
                        summary: format!(
                            "GPU {} reports {} compute process(es)",
                            gpu.uuid,
                            gpu.compute_processes.len()
                        ),
                        remediation: None,
                    },
                ]);
            }
        }
        let coverage = checks
            .iter()
            .map(|check| DoctorCoverage {
                provider: match check.code.as_str() {
                    "detector.physical_memory" | "detector.commit_headroom" => "windows_memory",
                    "detector.cpu" => "windows_system_times",
                    "detector.disk" => "windows_disk_performance",
                    "detector.processes" | "detector.process_rules" => "windows_toolhelp",
                    "detector.nvml" => "nvml",
                    code if code.starts_with("detector.gpu_") => "nvml",
                    "detector.sampler_freshness" => "host_sampler",
                    _ => "host_observation",
                }
                .into(),
                detector: check
                    .code
                    .strip_prefix("detector.")
                    .unwrap_or(&check.code)
                    .to_owned(),
                status: check.status.clone(),
                observed_unix_millis: Some(sample.captured_unix_millis),
                observation_generation: Some(sample.observation_generation),
                remediation: check.remediation.clone(),
            })
            .collect();
        (checks, coverage)
    }

    /// The callback runs while provider generation is frozen. Callers must acquire the Store
    /// mutex only inside this callback, preserving the release-barrier-before-Store lock order.
    pub(crate) fn with_release_sample<R>(
        &self,
        excluded_pid: u32,
        callback: impl FnOnce(&HostSample) -> R,
    ) -> Result<R, String> {
        let _barrier = self
            .release_barrier
            .lock()
            .map_err(|_| "observation release barrier poisoned".to_owned())?;
        let sample = self.sample_under_barrier(Some(excluded_pid))?;
        Ok(callback(&sample))
    }

    fn sample_under_barrier(&self, excluded_pid: Option<u32>) -> Result<HostSample, String> {
        let (captured_unix_millis, captured_monotonic_millis) =
            observation_clock().map_err(|error| format!("reading observation clock: {error}"))?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| "observation state mutex poisoned".to_owned())?;
        let mut generation_changed = cadence_changed(
            state.last_clock,
            (captured_unix_millis, captured_monotonic_millis),
            self.config.generation_max_cadence_gap_millis,
        );
        state.last_clock = Some((captured_unix_millis, captured_monotonic_millis));

        let processes_result = probe_processes()
            .map(|mut processes| {
                if let Some(excluded_pid) = excluded_pid {
                    processes.retain(|process| process.pid != excluded_pid);
                }
                processes
            })
            .map_err(|error| error.to_string());
        let process_values = processes_result.as_deref().unwrap_or(&[]);
        let gpu_result = sample_gpus(&self.config, &mut state, process_values);
        if gpu_result.provider_generation_changed {
            generation_changed = true;
        }
        if let Ok(gpus) = &gpu_result.value {
            let fingerprint = gpus
                .values()
                .map(|gpu| (gpu.uuid.clone(), gpu.driver_version.clone()))
                .collect::<Vec<_>>();
            if state
                .gpu_fingerprint
                .as_ref()
                .is_some_and(|previous| previous != &fingerprint)
            {
                generation_changed = true;
            }
            state.gpu_fingerprint = Some(fingerprint);
        }
        if generation_changed {
            state.observation_generation = Uuid::now_v7();
            state.cpu.reset();
            state.disk.reset();
        }

        let memory = evidence_from_result(
            captured_unix_millis,
            captured_monotonic_millis,
            probe_memory().map_err(|error| error.to_string()),
        );
        let processes = evidence_from_result(
            captured_unix_millis,
            captured_monotonic_millis,
            processes_result,
        );
        let gpus = evidence_from_result(
            captured_unix_millis,
            captured_monotonic_millis,
            gpu_result.value,
        );
        let cpu_utilization = state
            .cpu
            .sample(captured_unix_millis, captured_monotonic_millis);
        let disk_utilization = state
            .disk
            .sample(captured_unix_millis, captured_monotonic_millis);
        Ok(HostSample {
            observation_generation: state.observation_generation,
            captured_unix_millis,
            captured_monotonic_millis,
            memory,
            cpu_utilization,
            disk_utilization,
            processes,
            gpus,
        })
    }
}

fn gpu_placement_check(
    required_uuid: Option<&str>,
    evidence: &ComponentValue<BTreeMap<String, GpuEvidence>>,
) -> DoctorCheck {
    let (status, summary, remediation) = match (required_uuid, evidence) {
        (None, _) => (
            DoctorCheckStatus::Fail,
            "GPU work is retained but host gpu_slot_uuid is not configured".into(),
            Some("configure the exact canonical GPU UUID used for placement".into()),
        ),
        (Some(uuid), ComponentValue::Available(gpus)) if gpus.contains_key(uuid) => (
            DoctorCheckStatus::Pass,
            format!("configured GPU {uuid} is present in current NVML topology"),
            None,
        ),
        (Some(uuid), ComponentValue::Available(_)) => (
            DoctorCheckStatus::Fail,
            format!("configured GPU {uuid} is absent from current NVML topology"),
            Some("restore the configured GPU or update host policy before accepting work".into()),
        ),
        (Some(uuid), ComponentValue::Unavailable(detail) | ComponentValue::Error(detail)) => (
            DoctorCheckStatus::Fail,
            format!("configured GPU {uuid} cannot be verified"),
            Some(detail.clone()),
        ),
    };
    DoctorCheck {
        code: "detector.gpu_placement".into(),
        status,
        summary,
        remediation,
    }
}

fn component_check<T>(
    code: &str,
    description: &str,
    value: &ComponentValue<T>,
    required: bool,
) -> DoctorCheck {
    match value {
        ComponentValue::Available(_) => DoctorCheck {
            code: code.into(),
            status: DoctorCheckStatus::Pass,
            summary: format!("{description} evidence is available"),
            remediation: None,
        },
        ComponentValue::Unavailable(detail) => DoctorCheck {
            code: code.into(),
            status: if required {
                DoctorCheckStatus::Fail
            } else {
                DoctorCheckStatus::Warning
            },
            summary: format!("{description} is temporarily unavailable"),
            remediation: Some(detail.clone()),
        },
        ComponentValue::Error(detail) => DoctorCheck {
            code: code.into(),
            status: if required {
                DoctorCheckStatus::Fail
            } else {
                DoctorCheckStatus::Warning
            },
            summary: format!("{description} provider failed"),
            remediation: Some(detail.clone()),
        },
    }
}

struct GpuSampleResult {
    value: Result<BTreeMap<String, GpuEvidence>, String>,
    provider_generation_changed: bool,
}

fn sample_gpus(
    config: &HostObservationConfig,
    state: &mut MutexGuard<'_, ServiceState>,
    processes: &[ProcessEvidence],
) -> GpuSampleResult {
    if config.gpu_provider == GpuProviderConfig::Disabled {
        return GpuSampleResult {
            value: Err("NVML provider is disabled by host policy".into()),
            provider_generation_changed: false,
        };
    }
    let mut generation_changed = false;
    if state.nvml.is_none() {
        match NvmlProvider::load() {
            Ok(provider) => {
                generation_changed = state.nvml_initialization_attempted;
                state.nvml = Some(provider);
            }
            Err(error) => {
                state.nvml_initialization_attempted = true;
                return GpuSampleResult {
                    value: Err(error),
                    provider_generation_changed: false,
                };
            }
        }
        state.nvml_initialization_attempted = true;
    }
    let result = state
        .nvml
        .as_ref()
        .expect("NVML provider was initialized above")
        .sample(processes);
    if result.is_err() {
        state.nvml = None;
        generation_changed = true;
    }
    GpuSampleResult {
        value: result,
        provider_generation_changed: generation_changed,
    }
}

fn evidence_from_result<T>(
    captured_unix_millis: i64,
    captured_monotonic_millis: u64,
    value: Result<T, String>,
) -> ComponentEvidence<T> {
    match value {
        Ok(value) => {
            ComponentEvidence::available(captured_unix_millis, captured_monotonic_millis, value)
        }
        Err(error) => ComponentEvidence {
            captured_unix_millis,
            captured_monotonic_millis,
            value: ComponentValue::Error(bounded_diagnostic(error)),
        },
    }
}

fn bounded_diagnostic(mut value: String) -> String {
    const MAX_BYTES: usize = 512;
    if value.len() <= MAX_BYTES {
        return value;
    }
    let mut end = MAX_BYTES;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
    value
}

fn cadence_changed(
    previous: Option<(i64, u64)>,
    current: (i64, u64),
    maximum_gap_millis: u64,
) -> bool {
    let Some((previous_wall, previous_monotonic)) = previous else {
        return false;
    };
    let Some(monotonic_delta) = current.1.checked_sub(previous_monotonic) else {
        return true;
    };
    let Some(wall_delta) = current.0.checked_sub(previous_wall) else {
        return true;
    };
    let Ok(wall_delta) = u64::try_from(wall_delta) else {
        return true;
    };
    monotonic_delta > maximum_gap_millis
        || wall_delta.abs_diff(monotonic_delta) > maximum_gap_millis
}

#[cfg(test)]
mod tests {
    use super::{
        ComponentValue, HostObservationRequirements, HostObservationService, cadence_changed,
        component_check, gpu_placement_check,
    };
    use crate::{DoctorCheckStatus, HostObservationConfig};

    #[test]
    fn cadence_and_suspend_discontinuities_change_generation() {
        assert!(!cadence_changed(None, (100, 100), 2_500));
        assert!(!cadence_changed(Some((100, 100)), (1_100, 1_100), 2_500));
        assert!(cadence_changed(Some((100, 100)), (4_000, 4_000), 2_500));
        assert!(cadence_changed(Some((100, 100)), (5_000, 1_100), 2_500));
        assert!(cadence_changed(Some((100, 100)), (50, 200), 2_500));
    }

    #[test]
    fn doctor_fails_closed_only_when_unavailable_coverage_is_required() {
        let unavailable = ComponentValue::<u8>::Unavailable("warming_up".into());
        assert_eq!(
            component_check("detector.cpu", "CPU", &unavailable, false).status,
            DoctorCheckStatus::Warning
        );
        assert_eq!(
            component_check("detector.cpu", "CPU", &unavailable, true).status,
            DoctorCheckStatus::Fail
        );
    }

    #[test]
    fn doctor_coverage_names_every_strict_provider_with_fresh_provenance() {
        let service = HostObservationService::new(HostObservationConfig::default());
        let (checks, coverage) = service.doctor_diagnostics(HostObservationRequirements::default());
        assert_eq!(checks.len(), coverage.len());
        for provider in [
            "windows_memory",
            "windows_system_times",
            "windows_disk_performance",
            "windows_toolhelp",
            "nvml",
            "host_sampler",
        ] {
            let item = coverage
                .iter()
                .find(|item| item.provider == provider)
                .unwrap_or_else(|| panic!("missing coverage for {provider}"));
            assert!(item.observed_unix_millis.is_some());
            assert!(item.observation_generation.is_some());
        }
    }

    #[test]
    fn doctor_gpu_coverage_requires_the_exact_configured_device() {
        let configured = "gpu-a1144c26-a15c-cba1-3b7a-870c755ef08a";
        let other = "gpu-b2255d37-b26d-dcb2-4c8b-981d866ff19b";
        let evidence = ComponentValue::Available(
            [(
                other.into(),
                super::super::GpuEvidence {
                    uuid: other.into(),
                    driver_version: "999.42".into(),
                    free_memory_mb: 16_384,
                    utilization_percent: 0,
                    compute_processes: Vec::new(),
                },
            )]
            .into(),
        );
        assert_eq!(
            gpu_placement_check(Some(configured), &evidence).status,
            DoctorCheckStatus::Fail
        );
        assert_eq!(
            gpu_placement_check(None, &evidence).status,
            DoctorCheckStatus::Fail
        );
        let exact = ComponentValue::Available(
            [(
                configured.into(),
                super::super::GpuEvidence {
                    uuid: configured.into(),
                    driver_version: "999.42".into(),
                    free_memory_mb: 16_384,
                    utilization_percent: 0,
                    compute_processes: Vec::new(),
                },
            )]
            .into(),
        );
        assert_eq!(
            gpu_placement_check(Some(configured), &exact).status,
            DoctorCheckStatus::Pass
        );
    }
}
