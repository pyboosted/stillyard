use std::collections::BTreeMap;

use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ComponentValue<T> {
    Available(T),
    Unavailable(String),
    Error(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ComponentEvidence<T> {
    pub(crate) captured_unix_millis: i64,
    pub(crate) captured_monotonic_millis: u64,
    pub(crate) value: ComponentValue<T>,
}

impl<T> ComponentEvidence<T> {
    pub(crate) fn available(
        captured_unix_millis: i64,
        captured_monotonic_millis: u64,
        value: T,
    ) -> Self {
        Self {
            captured_unix_millis,
            captured_monotonic_millis,
            value: ComponentValue::Available(value),
        }
    }

    pub(crate) fn value_if_fresh(
        &self,
        now_monotonic_millis: u64,
        max_age_millis: u64,
    ) -> Result<&T, EvidenceFailure<'_>> {
        let age = now_monotonic_millis
            .checked_sub(self.captured_monotonic_millis)
            .ok_or(EvidenceFailure::ClockDiscontinuity)?;
        if age > max_age_millis {
            return Err(EvidenceFailure::Stale { age_millis: age });
        }
        match &self.value {
            ComponentValue::Available(value) => Ok(value),
            ComponentValue::Unavailable(detail) => Err(EvidenceFailure::Unavailable(detail)),
            ComponentValue::Error(detail) => Err(EvidenceFailure::Error(detail)),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MemoryEvidence {
    pub(crate) available_physical_mb: u64,
    pub(crate) commit_headroom_mb: u64,
}

impl MemoryEvidence {
    pub(crate) fn headroom_mb(self) -> u64 {
        self.available_physical_mb.min(self.commit_headroom_mb)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProcessEvidence {
    pub(crate) pid: u32,
    pub(crate) basename: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GpuEvidence {
    pub(crate) uuid: String,
    pub(crate) driver_version: String,
    pub(crate) free_memory_mb: u64,
    pub(crate) utilization_percent: u8,
    pub(crate) compute_processes: Vec<ProcessEvidence>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct HostSample {
    pub(crate) observation_generation: Uuid,
    pub(crate) captured_unix_millis: i64,
    pub(crate) captured_monotonic_millis: u64,
    pub(crate) memory: ComponentEvidence<MemoryEvidence>,
    pub(crate) cpu_utilization: ComponentEvidence<u8>,
    pub(crate) disk_utilization: ComponentEvidence<u8>,
    pub(crate) processes: ComponentEvidence<Vec<ProcessEvidence>>,
    pub(crate) gpus: ComponentEvidence<BTreeMap<String, GpuEvidence>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum EvidenceFailure<'a> {
    Stale { age_millis: u64 },
    Unavailable(&'a str),
    Error(&'a str),
    ClockDiscontinuity,
}
