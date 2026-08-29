use uuid::Uuid;

use super::{ComponentEvidence, GpuEvidence, HostSample, ProcessEvidence};
use crate::resources::{ResolvedClaims, observed_resource_blocker};
use crate::spec::canonical_gpu_uuid;
use crate::{
    Blocker, DetectorEvidenceSnapshot, HostConfig, JobSpec, ObservedOperandSnapshot, QuietDetector,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AdmissionContext {
    pub(crate) evaluated_unix_millis: i64,
    pub(crate) evaluated_monotonic_millis: u64,
    pub(crate) observation_generation: Uuid,
    pub(crate) blockers: Vec<Blocker>,
    pub(crate) non_quiet_blockers: Vec<Blocker>,
    pub(crate) quiet_blockers: Vec<Blocker>,
    pub(crate) quiet_sample_satisfied: bool,
    pub(crate) gpu_uuid: Option<String>,
    pub(crate) gpu_driver_version: Option<String>,
    pub(crate) operands: Vec<ObservedOperandSnapshot>,
    pub(crate) detectors: Vec<DetectorEvidenceSnapshot>,
}

pub(crate) fn evaluate_admission(
    job: &JobSpec,
    config: &HostConfig,
    sample: &HostSample,
    granted: &[ResolvedClaims],
    now_unix_millis: i64,
    now_monotonic_millis: u64,
) -> AdmissionContext {
    let mut non_quiet_blockers = Vec::new();
    let mut quiet_blockers = Vec::new();
    let mut operands = Vec::new();
    let mut detectors = Vec::new();
    let memory_age = config.observation.memory_max_sample_age_millis;
    if let Some(requested) = job.resources.ram_mb {
        let mut operand = ObservedOperandSnapshot {
            name: "ram_mb".into(),
            requested,
            configured_capacity: Some(config.resources.ram_mb),
            observed: None,
            safety_margin: config.observation.ram_safety_margin_mb,
            granted_debit: None,
            satisfied: false,
        };
        match sample
            .memory
            .value_if_fresh(now_monotonic_millis, memory_age)
        {
            Ok(memory) => {
                operand.observed = Some(memory.headroom_mb());
                match checked_granted("ram_mb", granted.iter().map(|claims| claims.ram_mb)) {
                    Ok(granted) => {
                        operand.granted_debit = Some(granted);
                        let blocker = observed_resource_blocker(
                            "ram_mb",
                            requested,
                            memory.headroom_mb(),
                            config.observation.ram_safety_margin_mb,
                            granted,
                        );
                        operand.satisfied = blocker.is_none();
                        if let Some(blocker) = blocker {
                            non_quiet_blockers.push(blocker);
                        }
                    }
                    Err(blocker) => non_quiet_blockers.push(blocker),
                }
            }
            Err(failure) => {
                non_quiet_blockers.push(resource_evidence_blocker("ram_mb", failure));
            }
        }
        operands.push(operand);
    }

    let placement = config
        .observation
        .gpu_slot_uuid
        .as_deref()
        .and_then(|uuid| canonical_gpu_uuid(uuid).ok());
    let non_quiet_gpu_required = job.resources.gpu_slots.unwrap_or(0) > 0
        || job
            .resources
            .custom
            .keys()
            .any(|name| name.starts_with("vram_mb:"))
        || job
            .observed
            .as_ref()
            .is_some_and(|policy| !policy.gpu_utilization_percent_at_most.is_empty());
    let quiet_gpu_required = job.quiet.as_ref().is_some_and(|quiet| {
        quiet.detectors.iter().any(|detector| {
            matches!(
                detector,
                QuietDetector::GpuUtilization { .. } | QuietDetector::ForeignGpuCompute { .. }
            )
        })
    });
    let gpu_required = non_quiet_gpu_required || quiet_gpu_required;
    let gpu_max_age = required_gpu_max_age(job, config);
    let mut gpu = None;
    if gpu_required && placement.is_none() {
        let blocker = Blocker {
            code: "gpu_placement_unconfigured".into(),
            detail: "GPU-dependent Job requires host gpu_slot_uuid".into(),
        };
        if non_quiet_gpu_required {
            non_quiet_blockers.push(blocker.clone());
        }
        if quiet_gpu_required {
            quiet_blockers.push(blocker);
        }
    } else if let (Some(placement), Some(max_age)) = (placement.as_deref(), gpu_max_age) {
        match sample.gpus.value_if_fresh(now_monotonic_millis, max_age) {
            Ok(gpus) => match gpus.get(placement) {
                Some(evidence) => gpu = Some(evidence),
                None => {
                    if non_quiet_gpu_required {
                        non_quiet_blockers.push(Blocker {
                            code: "observation_missing".into(),
                            detail: format!(
                                "configured GPU {placement} is absent from current NVML topology"
                            ),
                        });
                    }
                    if quiet_gpu_required {
                        quiet_blockers.push(Blocker {
                            code: "detector_unavailable".into(),
                            detail: format!(
                                "configured GPU {placement} is absent from current NVML topology"
                            ),
                        });
                    }
                }
            },
            Err(failure) => {
                if non_quiet_gpu_required {
                    non_quiet_blockers.push(resource_evidence_blocker("nvml", failure.clone()));
                }
                if quiet_gpu_required {
                    quiet_blockers.push(evidence_blocker("nvml", failure));
                }
            }
        }
    }

    if let Some(gpu) = gpu {
        evaluate_vram(
            job,
            config,
            gpu,
            granted,
            &mut non_quiet_blockers,
            &mut operands,
        );
        evaluate_observed(
            job,
            sample,
            gpu,
            now_monotonic_millis,
            &mut non_quiet_blockers,
            &mut detectors,
        );
    } else {
        evaluate_observed_cpu(
            job,
            sample,
            now_monotonic_millis,
            &mut non_quiet_blockers,
            &mut detectors,
        );
    }

    let quiet_sample_satisfied = evaluate_quiet(
        job,
        config,
        sample,
        gpu,
        now_monotonic_millis,
        &mut quiet_blockers,
        &mut detectors,
    );
    sort_blockers(&mut non_quiet_blockers);
    sort_blockers(&mut quiet_blockers);
    let mut blockers = non_quiet_blockers.clone();
    blockers.extend(quiet_blockers.clone());
    sort_blockers(&mut blockers);
    AdmissionContext {
        evaluated_unix_millis: now_unix_millis,
        evaluated_monotonic_millis: now_monotonic_millis,
        observation_generation: sample.observation_generation,
        blockers,
        non_quiet_blockers,
        quiet_blockers,
        quiet_sample_satisfied,
        gpu_uuid: gpu.map(|gpu| gpu.uuid.clone()),
        gpu_driver_version: gpu.map(|gpu| gpu.driver_version.clone()),
        operands,
        detectors,
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

fn evaluate_vram(
    job: &JobSpec,
    config: &HostConfig,
    gpu: &GpuEvidence,
    granted: &[ResolvedClaims],
    blockers: &mut Vec<Blocker>,
    operands: &mut Vec<ObservedOperandSnapshot>,
) {
    for (name, requested) in &job.resources.custom {
        if !name.starts_with("vram_mb:") {
            continue;
        }
        let canonical_name = format!("vram_mb:{}", gpu.uuid);
        let mut operand = ObservedOperandSnapshot {
            name: canonical_name.clone(),
            requested: *requested,
            configured_capacity: config.resources.custom.get(name).copied().or_else(|| {
                config
                    .resources
                    .custom
                    .iter()
                    .find(|(configured, _)| configured.eq_ignore_ascii_case(&canonical_name))
                    .map(|(_, capacity)| *capacity)
            }),
            observed: Some(gpu.free_memory_mb),
            safety_margin: config.observation.vram_safety_margin_mb,
            granted_debit: None,
            satisfied: false,
        };
        let granted = match checked_granted(
            name,
            granted
                .iter()
                .map(|claims| claims.custom.get(&canonical_name).copied().unwrap_or(0)),
        ) {
            Ok(granted) => granted,
            Err(blocker) => {
                blockers.push(blocker);
                operands.push(operand);
                continue;
            }
        };
        operand.granted_debit = Some(granted);
        let blocker = observed_resource_blocker(
            name,
            *requested,
            gpu.free_memory_mb,
            config.observation.vram_safety_margin_mb,
            granted,
        );
        operand.satisfied = blocker.is_none();
        if let Some(blocker) = blocker {
            blockers.push(blocker);
        }
        operands.push(operand);
    }
}

fn required_gpu_max_age(job: &JobSpec, config: &HostConfig) -> Option<u64> {
    let mut ages = Vec::new();
    if job.resources.gpu_slots.unwrap_or(0) > 0
        || job
            .resources
            .custom
            .keys()
            .any(|name| name.starts_with("vram_mb:"))
    {
        ages.push(config.observation.memory_max_sample_age_millis);
    }
    if job
        .observed
        .as_ref()
        .is_some_and(|policy| !policy.gpu_utilization_percent_at_most.is_empty())
    {
        ages.push(
            job.observed
                .as_ref()
                .expect("observed policy checked above")
                .max_sample_age_seconds
                .saturating_mul(1_000),
        );
    }
    if job.quiet.as_ref().is_some_and(|quiet| {
        quiet.detectors.iter().any(|detector| {
            matches!(
                detector,
                QuietDetector::GpuUtilization { .. } | QuietDetector::ForeignGpuCompute { .. }
            )
        })
    }) {
        ages.push(
            job.quiet
                .as_ref()
                .expect("quiet policy checked above")
                .max_sample_age_seconds
                .saturating_mul(1_000),
        );
    }
    ages.into_iter().min()
}

fn checked_granted(name: &str, mut values: impl Iterator<Item = u64>) -> Result<u64, Blocker> {
    values.try_fold(0_u64, |total, value| {
        total.checked_add(value).ok_or_else(|| Blocker {
            code: "observation_unusable".into(),
            detail: format!("{name}: granted debit sum overflow"),
        })
    })
}

fn evaluate_observed(
    job: &JobSpec,
    sample: &HostSample,
    gpu: &GpuEvidence,
    now_monotonic_millis: u64,
    blockers: &mut Vec<Blocker>,
    detectors: &mut Vec<DetectorEvidenceSnapshot>,
) {
    evaluate_observed_cpu(job, sample, now_monotonic_millis, blockers, detectors);
    let Some(policy) = &job.observed else {
        return;
    };
    let max_age = policy.max_sample_age_seconds.saturating_mul(1_000);
    for (uuid, threshold) in &policy.gpu_utilization_percent_at_most {
        if canonical_gpu_uuid(uuid).ok().as_deref() != Some(gpu.uuid.as_str()) {
            continue;
        }
        let blocker = (gpu.utilization_percent > *threshold).then(|| Blocker {
            code: "observed_resource_busy".into(),
            detail: format!(
                "gpu_utilization:{} observed {}%, threshold {}%, max_age {}ms",
                gpu.uuid, gpu.utilization_percent, threshold, max_age
            ),
        });
        detectors.push(DetectorEvidenceSnapshot {
            detector: format!("observed.gpu_utilization:{}", gpu.uuid),
            observed: Some(u64::from(gpu.utilization_percent)),
            threshold: Some(u64::from(*threshold)),
            satisfied: blocker.is_none(),
            detail: blocker.as_ref().map(|item| item.detail.clone()),
        });
        if let Some(blocker) = blocker {
            blockers.push(blocker);
        }
    }
}

fn evaluate_observed_cpu(
    job: &JobSpec,
    sample: &HostSample,
    now_monotonic_millis: u64,
    blockers: &mut Vec<Blocker>,
    detectors: &mut Vec<DetectorEvidenceSnapshot>,
) {
    let Some(policy) = &job.observed else {
        return;
    };
    let Some(threshold) = policy.cpu_utilization_percent_at_most else {
        return;
    };
    let max_age = policy.max_sample_age_seconds.saturating_mul(1_000);
    let result = match sample
        .cpu_utilization
        .value_if_fresh(now_monotonic_millis, max_age)
    {
        Ok(value) if *value <= threshold => (Some(u64::from(*value)), None),
        Ok(value) => (
            Some(u64::from(*value)),
            Some(Blocker {
                code: "observed_resource_busy".into(),
                detail: format!("cpu_utilization observed {value}%, threshold {threshold}%"),
            }),
        ),
        Err(failure) => (None, Some(evidence_blocker("cpu_utilization", failure))),
    };
    detectors.push(DetectorEvidenceSnapshot {
        detector: "observed.cpu_utilization".into(),
        observed: result.0,
        threshold: Some(u64::from(threshold)),
        satisfied: result.1.is_none(),
        detail: result.1.as_ref().map(|item| item.detail.clone()),
    });
    if let Some(blocker) = result.1 {
        blockers.push(blocker);
    }
}

fn evaluate_quiet(
    job: &JobSpec,
    config: &HostConfig,
    sample: &HostSample,
    gpu: Option<&GpuEvidence>,
    now_monotonic_millis: u64,
    blockers: &mut Vec<Blocker>,
    detectors: &mut Vec<DetectorEvidenceSnapshot>,
) -> bool {
    let Some(quiet) = &job.quiet else {
        return true;
    };
    let max_age = quiet.max_sample_age_seconds.saturating_mul(1_000);
    let mut satisfied = true;
    for detector in &quiet.detectors {
        let (name, observed, threshold, result) = match detector {
            QuietDetector::CpuUtilization { max_percent } => {
                let result = compare_component(
                    "cpu_utilization",
                    &sample.cpu_utilization,
                    *max_percent,
                    now_monotonic_millis,
                    max_age,
                );
                (
                    "quiet.cpu_utilization".to_owned(),
                    component_value(&sample.cpu_utilization, now_monotonic_millis, max_age),
                    Some(u64::from(*max_percent)),
                    result,
                )
            }
            QuietDetector::DiskUtilization { max_percent } => {
                let result = compare_component(
                    "disk_utilization",
                    &sample.disk_utilization,
                    *max_percent,
                    now_monotonic_millis,
                    max_age,
                );
                (
                    "quiet.disk_utilization".to_owned(),
                    component_value(&sample.disk_utilization, now_monotonic_millis, max_age),
                    Some(u64::from(*max_percent)),
                    result,
                )
            }
            QuietDetector::GpuUtilization { max_percent, .. } => {
                let result = gpu.map_or_else(
                    || {
                        Err(Blocker {
                            code: "detector_unavailable".into(),
                            detail: "GPU utilization has no fresh evidence".into(),
                        })
                    },
                    |gpu| {
                        if gpu.utilization_percent <= *max_percent {
                            Ok(())
                        } else {
                            Err(Blocker {
                                code: "quiet_contaminated".into(),
                                detail: format!(
                                    "gpu_utilization:{} observed {}%, threshold {}%",
                                    gpu.uuid, gpu.utilization_percent, max_percent
                                ),
                            })
                        }
                    },
                );
                (
                    "quiet.gpu_utilization".to_owned(),
                    gpu.map(|gpu| u64::from(gpu.utilization_percent)),
                    Some(u64::from(*max_percent)),
                    result,
                )
            }
            QuietDetector::ForeignGpuCompute { .. } => {
                let result = gpu.map_or_else(
                    || {
                        Err(Blocker {
                            code: "detector_unavailable".into(),
                            detail: "GPU compute process evidence unavailable".into(),
                        })
                    },
                    |gpu| {
                        let foreign = gpu.compute_processes.iter().find(|process| {
                            !matches_any(
                                &process.basename,
                                &config.observation.process_rules.ignore,
                            )
                        });
                        foreign.map_or(Ok(()), |process| {
                            Err(Blocker {
                                code: "quiet_contaminated".into(),
                                detail: format!(
                                    "foreign_gpu_compute:{} pid={} basename={}",
                                    gpu.uuid, process.pid, process.basename
                                ),
                            })
                        })
                    },
                );
                (
                    "quiet.foreign_gpu_compute".to_owned(),
                    gpu.map(|gpu| u64::try_from(gpu.compute_processes.len()).unwrap_or(u64::MAX)),
                    Some(0),
                    result,
                )
            }
            QuietDetector::BlockedProcesses => {
                let observed = sample
                    .processes
                    .value_if_fresh(now_monotonic_millis, max_age)
                    .ok()
                    .map(|processes| {
                        u64::try_from(
                            processes
                                .iter()
                                .filter(|process| {
                                    !matches_any(
                                        &process.basename,
                                        &config.observation.process_rules.ignore,
                                    ) && matches_any(
                                        &process.basename,
                                        &config.observation.process_rules.block,
                                    )
                                })
                                .count(),
                        )
                        .unwrap_or(u64::MAX)
                    });
                let result = sample
                    .processes
                    .value_if_fresh(now_monotonic_millis, max_age)
                    .map_err(|failure| evidence_blocker("blocked_processes", failure))
                    .and_then(|processes| blocked_process(config, processes));
                (
                    "quiet.blocked_processes".to_owned(),
                    observed,
                    Some(0),
                    result,
                )
            }
        };
        detectors.push(DetectorEvidenceSnapshot {
            detector: name,
            observed,
            threshold,
            satisfied: result.is_ok(),
            detail: result.as_ref().err().map(|item| item.detail.clone()),
        });
        if let Err(blocker) = result {
            satisfied = false;
            blockers.push(blocker);
        }
    }
    satisfied
}

fn component_value(
    evidence: &ComponentEvidence<u8>,
    now_monotonic_millis: u64,
    max_age_millis: u64,
) -> Option<u64> {
    evidence
        .value_if_fresh(now_monotonic_millis, max_age_millis)
        .ok()
        .map(|value| u64::from(*value))
}

fn compare_component(
    name: &str,
    evidence: &ComponentEvidence<u8>,
    threshold: u8,
    now_monotonic_millis: u64,
    max_age_millis: u64,
) -> Result<(), Blocker> {
    match evidence.value_if_fresh(now_monotonic_millis, max_age_millis) {
        Ok(value) if *value <= threshold => Ok(()),
        Ok(value) => Err(Blocker {
            code: "quiet_contaminated".into(),
            detail: format!("{name} observed {value}%, threshold {threshold}%"),
        }),
        Err(failure) => Err(evidence_blocker(name, failure)),
    }
}

fn blocked_process(config: &HostConfig, processes: &[ProcessEvidence]) -> Result<(), Blocker> {
    for process in processes {
        if matches_any(&process.basename, &config.observation.process_rules.ignore) {
            continue;
        }
        if matches_any(&process.basename, &config.observation.process_rules.block) {
            return Err(Blocker {
                code: "quiet_contaminated".into(),
                detail: format!(
                    "blocked_process pid={} basename={}",
                    process.pid, process.basename
                ),
            });
        }
    }
    Ok(())
}

fn matches_any(value: &str, patterns: &[String]) -> bool {
    patterns
        .iter()
        .any(|pattern| wildcard_match(value, pattern))
}

fn wildcard_match(value: &str, pattern: &str) -> bool {
    let value = value.to_ascii_lowercase();
    let pattern = pattern.to_ascii_lowercase();
    let parts = pattern.split('*').collect::<Vec<_>>();
    if parts.len() == 1 {
        return value == pattern;
    }
    let mut offset = 0;
    for (index, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }
        let Some(found) = value[offset..].find(part) else {
            return false;
        };
        if index == 0 && !pattern.starts_with('*') && found != 0 {
            return false;
        }
        offset += found + part.len();
    }
    pattern.ends_with('*') || parts.last().is_none_or(|part| value.ends_with(part))
}

fn evidence_blocker(name: &str, failure: super::evidence::EvidenceFailure<'_>) -> Blocker {
    use super::evidence::EvidenceFailure;
    match failure {
        EvidenceFailure::Stale { age_millis } => Blocker {
            code: "observation_stale".into(),
            detail: format!("{name} evidence age {age_millis}ms exceeds policy"),
        },
        EvidenceFailure::Unavailable(detail) | EvidenceFailure::Error(detail) => Blocker {
            code: "detector_unavailable".into(),
            detail: format!("{name}: {detail}"),
        },
        EvidenceFailure::ClockDiscontinuity => Blocker {
            code: "observation_stale".into(),
            detail: format!("{name}: monotonic clock moved behind capture"),
        },
    }
}

fn resource_evidence_blocker(name: &str, failure: super::evidence::EvidenceFailure<'_>) -> Blocker {
    use super::evidence::EvidenceFailure;
    match failure {
        EvidenceFailure::Stale { age_millis } => Blocker {
            code: "observation_stale".into(),
            detail: format!("{name} evidence age {age_millis}ms exceeds host policy"),
        },
        EvidenceFailure::Unavailable(detail) | EvidenceFailure::Error(detail) => Blocker {
            code: "observation_missing".into(),
            detail: format!("{name}: {detail}"),
        },
        EvidenceFailure::ClockDiscontinuity => Blocker {
            code: "observation_stale".into(),
            detail: format!("{name}: monotonic clock moved behind capture"),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::{
        GpuProviderConfig, HostObservationConfig, ObservedResourcePolicy, ProcessRules,
        QuietPolicy, ResourceCapacities, ResourceClaims, RetryPolicy, SPEC_VERSION, StdinSpec,
    };

    fn config(uuid: &str) -> HostConfig {
        HostConfig {
            resources: ResourceCapacities {
                ram_mb: 32_768,
                gpu_slots: 1,
                custom: [(format!("vram_mb:{uuid}"), 32_000)].into(),
                ..Default::default()
            },
            impact_incompatibilities: BTreeMap::new(),
            observation: HostObservationConfig {
                ram_safety_margin_mb: 1_024,
                vram_safety_margin_mb: 512,
                gpu_slot_uuid: Some(uuid.into()),
                process_rules: ProcessRules {
                    block: vec!["cargo.exe".into(), "obs*".into()],
                    ignore: vec!["dwm.exe".into(), "LogonUI.exe".into()],
                },
                gpu_provider: GpuProviderConfig::Nvml,
                ..Default::default()
            },
        }
    }

    fn job(uuid: &str) -> JobSpec {
        JobSpec {
            spec_version: SPEC_VERSION,
            executable: "C:\\tool.exe".into(),
            args: Vec::new(),
            working_directory: "C:\\work".into(),
            stdin: StdinSpec::Eof,
            environment: Default::default(),
            resources: ResourceClaims {
                ram_mb: Some(8_000),
                gpu_slots: Some(1),
                custom: [(format!("vram_mb:{uuid}"), 8_000)].into(),
                ..Default::default()
            },
            observed: Some(ObservedResourcePolicy {
                max_sample_age_seconds: 2,
                cpu_utilization_percent_at_most: Some(20),
                gpu_utilization_percent_at_most: [(uuid.into(), 0)].into(),
            }),
            conditions: Vec::new(),
            retry: RetryPolicy::default(),
            postconditions: Vec::new(),
            labels: Vec::new(),
            expected_duration_seconds: None,
            timeout_seconds: None,
            quiet: Some(QuietPolicy {
                stable_seconds: 30,
                max_sample_age_seconds: 2,
                wait_budget_seconds: 600,
                detectors: vec![
                    QuietDetector::GpuUtilization {
                        gpu_uuid: uuid.into(),
                        max_percent: 0,
                    },
                    QuietDetector::ForeignGpuCompute {
                        gpu_uuid: uuid.into(),
                    },
                    QuietDetector::BlockedProcesses,
                ],
            }),
            artifacts: Vec::new(),
            allow_child_submissions: false,
        }
    }

    fn sample(uuid: &str) -> HostSample {
        let captured = 10_000;
        HostSample {
            observation_generation: Uuid::now_v7(),
            captured_unix_millis: 10_000,
            captured_monotonic_millis: captured,
            memory: ComponentEvidence::available(
                10_000,
                captured,
                super::super::MemoryEvidence {
                    available_physical_mb: 64_000,
                    commit_headroom_mb: 12_000,
                },
            ),
            cpu_utilization: ComponentEvidence::available(10_000, captured, 0),
            disk_utilization: ComponentEvidence::available(10_000, captured, 0),
            processes: ComponentEvidence::available(
                10_000,
                captured,
                vec![ProcessEvidence {
                    pid: 10,
                    basename: "dwm.exe".into(),
                }],
            ),
            gpus: ComponentEvidence::available(
                10_000,
                captured,
                [(
                    uuid.to_ascii_lowercase(),
                    GpuEvidence {
                        uuid: uuid.to_ascii_lowercase(),
                        driver_version: "610.88".into(),
                        free_memory_mb: 12_000,
                        utilization_percent: 0,
                        compute_processes: vec![ProcessEvidence {
                            pid: 10,
                            basename: "dwm.exe".into(),
                        }],
                    },
                )]
                .into(),
            ),
        }
    }

    #[test]
    fn strict_admission_uses_commit_vram_margins_and_ignores_shell_processes() {
        let uuid = "GPU-a1144c26-a15c-cba1-3b7a-870c755ef08a";
        let context = evaluate_admission(
            &job(uuid),
            &config(uuid),
            &sample(uuid),
            &[],
            10_100,
            10_100,
        );
        assert!(context.blockers.is_empty(), "{:?}", context.blockers);
        assert!(context.quiet_sample_satisfied);
        assert_eq!(context.gpu_driver_version.as_deref(), Some("610.88"));
    }

    #[test]
    fn stale_memory_and_blocked_process_fail_closed() {
        let uuid = "GPU-a1144c26-a15c-cba1-3b7a-870c755ef08a";
        let mut sample = sample(uuid);
        sample.processes = ComponentEvidence::available(
            10_000,
            10_000,
            vec![ProcessEvidence {
                pid: 42,
                basename: "cargo.exe".into(),
            }],
        );
        let context = evaluate_admission(&job(uuid), &config(uuid), &sample, &[], 20_000, 20_000);
        assert!(
            context
                .blockers
                .iter()
                .any(|blocker| blocker.code == "observation_stale")
        );
        assert!(!context.quiet_sample_satisfied);
    }

    #[test]
    fn unavailable_quiet_detector_is_not_treated_as_a_clean_sample() {
        let uuid = "GPU-a1144c26-a15c-cba1-3b7a-870c755ef08a";
        let mut job = job(uuid);
        job.resources = ResourceClaims::default();
        job.observed = None;
        job.quiet.as_mut().unwrap().detectors =
            vec![QuietDetector::CpuUtilization { max_percent: 0 }];
        let mut sample = sample(uuid);
        sample.cpu_utilization.value =
            super::super::ComponentValue::Error("counter provider failed".into());
        let context = evaluate_admission(&job, &config(uuid), &sample, &[], 10_100, 10_100);
        assert!(!context.quiet_sample_satisfied);
        assert!(
            context
                .quiet_blockers
                .iter()
                .any(|blocker| blocker.code == "detector_unavailable")
        );
    }

    #[test]
    fn standalone_observed_thresholds_block_without_creating_quiet_policy() {
        let uuid = "GPU-a1144c26-a15c-cba1-3b7a-870c755ef08a";
        let mut job = job(uuid);
        job.resources = ResourceClaims::default();
        job.quiet = None;
        let mut sample = sample(uuid);
        sample.cpu_utilization = ComponentEvidence::available(10_000, 10_000, 21);
        sample.gpus.value_if_fresh(10_000, 1).unwrap();
        let ComponentEvidence {
            value: super::super::ComponentValue::Available(gpus),
            ..
        } = &mut sample.gpus
        else {
            unreachable!()
        };
        gpus.get_mut(&uuid.to_ascii_lowercase())
            .unwrap()
            .utilization_percent = 1;

        let blocked = evaluate_admission(&job, &config(uuid), &sample, &[], 10_100, 10_100);
        assert!(blocked.quiet_blockers.is_empty());
        assert_eq!(
            blocked
                .non_quiet_blockers
                .iter()
                .filter(|blocker| blocker.code == "observed_resource_busy")
                .count(),
            2
        );
        assert_eq!(blocked.detectors.len(), 2);

        sample.cpu_utilization = ComponentEvidence::available(10_200, 10_200, 20);
        let ComponentEvidence {
            value: super::super::ComponentValue::Available(gpus),
            ..
        } = &mut sample.gpus
        else {
            unreachable!()
        };
        gpus.get_mut(&uuid.to_ascii_lowercase())
            .unwrap()
            .utilization_percent = 0;
        let admitted = evaluate_admission(&job, &config(uuid), &sample, &[], 10_200, 10_200);
        assert!(admitted.blockers.is_empty(), "{:?}", admitted.blockers);
    }

    #[test]
    fn debrix_process_patterns_are_case_insensitive_and_wildcard_aware() {
        let uuid = "GPU-a1144c26-a15c-cba1-3b7a-870c755ef08a";
        let mut config = config(uuid);
        config.observation.process_rules.block = vec![
            "cargo.exe".into(),
            "rustc.exe".into(),
            "rust-analyzer.exe".into(),
            "obs*".into(),
            "nsight*".into(),
            "ngfx*".into(),
            "renderdoc*".into(),
        ];
        let mut job = job(uuid);
        job.resources = ResourceClaims::default();
        job.observed = None;
        job.quiet.as_mut().unwrap().detectors = vec![QuietDetector::BlockedProcesses];

        for basename in [
            "CARGO.EXE",
            "rustc.exe",
            "rust-analyzer.exe",
            "obs64.exe",
            "Nsight.Graphics.exe",
            "ngfx-ui.exe",
            "renderdoccmd.exe",
        ] {
            let mut sample = sample(uuid);
            sample.processes = ComponentEvidence::available(
                10_000,
                10_000,
                vec![ProcessEvidence {
                    pid: 42,
                    basename: basename.into(),
                }],
            );
            let context = evaluate_admission(&job, &config, &sample, &[], 10_100, 10_100);
            assert!(
                context
                    .quiet_blockers
                    .iter()
                    .any(|blocker| blocker.code == "quiet_contaminated"),
                "{basename} did not reset quiet"
            );
        }

        for basename in ["dwm.exe", "LOGONUI.EXE"] {
            let mut sample = sample(uuid);
            sample.processes = ComponentEvidence::available(
                10_000,
                10_000,
                vec![ProcessEvidence {
                    pid: 43,
                    basename: basename.into(),
                }],
            );
            let context = evaluate_admission(&job, &config, &sample, &[], 10_100, 10_100);
            assert!(
                context.quiet_blockers.is_empty(),
                "ignored shell process {basename} contaminated quiet"
            );
        }
    }

    #[test]
    fn unresolved_nvml_compute_process_fails_closed_but_shell_processes_do_not() {
        let uuid = "GPU-a1144c26-a15c-cba1-3b7a-870c755ef08a";
        let mut job = job(uuid);
        job.resources = ResourceClaims::default();
        job.observed = None;
        job.quiet.as_mut().unwrap().detectors = vec![QuietDetector::ForeignGpuCompute {
            gpu_uuid: uuid.into(),
        }];
        let mut sample = sample(uuid);
        if let super::super::ComponentValue::Available(gpus) = &mut sample.gpus.value {
            gpus.get_mut(&uuid.to_ascii_lowercase())
                .unwrap()
                .compute_processes = vec![ProcessEvidence {
                pid: 999,
                basename: "<unresolved>".into(),
            }];
        } else {
            unreachable!()
        }
        let blocked = evaluate_admission(&job, &config(uuid), &sample, &[], 10_100, 10_100);
        assert!(
            blocked
                .quiet_blockers
                .iter()
                .any(|blocker| blocker.detail.contains("<unresolved>"))
        );

        if let super::super::ComponentValue::Available(gpus) = &mut sample.gpus.value {
            gpus.get_mut(&uuid.to_ascii_lowercase())
                .unwrap()
                .compute_processes = vec![
                ProcessEvidence {
                    pid: 10,
                    basename: "dwm.exe".into(),
                },
                ProcessEvidence {
                    pid: 11,
                    basename: "LogonUI.exe".into(),
                },
            ];
        } else {
            unreachable!()
        }
        let admitted = evaluate_admission(&job, &config(uuid), &sample, &[], 10_100, 10_100);
        assert!(admitted.quiet_blockers.is_empty());
    }
}
