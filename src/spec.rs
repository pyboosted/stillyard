use std::collections::BTreeMap;
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;

use schemars::{JsonSchema, schema_for};
use serde::{Deserialize, Serialize};

use crate::{Error, Result};

pub const SPEC_VERSION: u32 = 4;

/// Least urgent explicit Job priority accepted by Stillyard.
pub const MIN_JOB_PRIORITY: i8 = -3;
/// Neutral Job priority used when the field is omitted.
pub const NEUTRAL_JOB_PRIORITY: i8 = 0;
/// Most urgent explicit Job priority accepted by Stillyard.
pub const MAX_JOB_PRIORITY: i8 = 3;
/// Waiting-time quantum added to effective priority.
pub const PRIORITY_AGING_QUANTUM_MILLIS: u64 = 60_000;
/// Saturating upper bound for effective priority arithmetic.
pub const MAX_EFFECTIVE_PRIORITY: i64 = 1_000_000;
/// Fixed lifetime of one durable scalar reservation.
pub const SCALAR_RESERVATION_HOLD_MILLIS: u64 = 60_000;
/// Durable cooldown after an expired reservation before the Job may be admitted or reserve again.
pub const SCALAR_RESERVATION_BACKOFF_MILLIS: u64 = 5_000;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct JobSpec {
    pub spec_version: u32,
    /// Immutable urgency in `MIN_JOB_PRIORITY..=MAX_JOB_PRIORITY`; larger is more urgent.
    #[serde(default)]
    #[schemars(range(min = -3, max = 3))]
    pub priority: i8,
    pub executable: PathBuf,
    #[serde(default)]
    pub args: Vec<String>,
    pub working_directory: PathBuf,
    #[serde(default)]
    pub stdin: StdinSpec,
    #[serde(default)]
    pub environment: EnvironmentSpec,
    #[serde(default)]
    pub resources: ResourceClaims,
    pub observed: Option<ObservedResourcePolicy>,
    #[serde(default)]
    pub conditions: Vec<ConditionSpec>,
    #[serde(default)]
    pub retry: RetryPolicy,
    #[serde(default)]
    pub postconditions: Vec<PostconditionSpec>,
    #[serde(default)]
    pub labels: Vec<Label>,
    pub expected_duration_seconds: Option<u64>,
    pub timeout_seconds: Option<u64>,
    pub quiet: Option<QuietPolicy>,
    #[serde(default)]
    pub artifacts: Vec<PathBuf>,
    /// Maximum capabilities that authenticated managed descendants may request.
    #[serde(default)]
    pub child_submission_policy: Option<ChildSubmissionPolicy>,
}

impl JobSpec {
    pub fn validate(&self) -> Result<()> {
        if self.spec_version != SPEC_VERSION {
            return Err(Error::InvalidSpec(format!(
                "unsupported spec_version {}, expected {SPEC_VERSION}",
                self.spec_version
            )));
        }
        if !(MIN_JOB_PRIORITY..=MAX_JOB_PRIORITY).contains(&self.priority) {
            return Err(Error::InvalidSpec(format!(
                "priority {} is outside the supported inclusive range {MIN_JOB_PRIORITY}..={MAX_JOB_PRIORITY}",
                self.priority
            )));
        }
        if self.executable.as_os_str().is_empty() {
            return Err(Error::InvalidSpec("executable is empty".into()));
        }
        if !self.executable.is_absolute() || !self.working_directory.is_absolute() {
            return Err(Error::InvalidSpec(
                "executable and working_directory must be absolute".into(),
            ));
        }
        if self.executable.as_os_str().to_string_lossy().contains('\0')
            || self
                .working_directory
                .as_os_str()
                .to_string_lossy()
                .contains('\0')
            || self.args.iter().any(|argument| argument.contains('\0'))
        {
            return Err(Error::InvalidSpec(
                "process path or argument contains NUL".into(),
            ));
        }
        if self.labels.len() > 32 {
            return Err(Error::InvalidSpec("more than 32 labels".into()));
        }
        let mut label_keys = BTreeSet::new();
        for label in &self.labels {
            label.validate()?;
            if !label_keys.insert(&label.key) {
                return Err(Error::InvalidSpec(format!(
                    "duplicate label key {:?}",
                    label.key
                )));
            }
        }
        self.environment.validate()?;
        if let StdinSpec::File { path } = &self.stdin {
            if !path.is_absolute() || path.as_os_str().to_string_lossy().contains('\0') {
                return Err(Error::InvalidSpec(
                    "stdin file source must be an absolute path without NUL".into(),
                ));
            }
        }
        if self.retry.max_attempts == 0 {
            return Err(Error::InvalidSpec(
                "retry.max_attempts must be positive".into(),
            ));
        }
        // The first executable slice fails closed for baseline features whose admission providers
        // are not shipped yet. Declaring a claim must never silently run as if it were satisfied.
        self.resources.validate()?;
        if let Some(policy) = &self.child_submission_policy {
            policy.validate()?;
        }
        if let Some(observed) = &self.observed {
            observed.validate()?;
        }
        if let Some(quiet) = &self.quiet {
            quiet.validate()?;
        }
        self.retry.validate()?;
        if self.postconditions.len() > 32 {
            return Err(Error::InvalidSpec("more than 32 postconditions".into()));
        }
        if self.conditions.len() > 32 {
            return Err(Error::InvalidSpec("more than 32 Conditions".into()));
        }
        for condition in &self.conditions {
            condition.validate()?;
        }
        let probes_per_attempt = self
            .conditions
            .iter()
            .filter(|condition| matches!(&condition.predicate, ConditionPredicate::Probe { .. }))
            .count();
        let lifecycle_invocations = u64::from(self.retry.max_attempts).saturating_mul(
            u64::try_from(self.postconditions.len() + probes_per_attempt + 1).unwrap_or(u64::MAX),
        );
        if lifecycle_invocations > 256 {
            return Err(Error::InvalidSpec(
                "retry attempts times Invocations per Attempt exceeds 256".into(),
            ));
        }
        for postcondition in &self.postconditions {
            postcondition.validate(self)?;
        }
        if !self.artifacts.is_empty() {
            return Err(Error::InvalidSpec(
                "staged/EOF stdin, explicit environment, resource admission, retries, postconditions, observed thresholds, quiet admission, and Conditions do not support artifacts".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn gpu_uuids(&self) -> Result<BTreeSet<String>> {
        let mut uuids = BTreeSet::new();
        for name in self.resources.custom.keys() {
            if name.starts_with("vram_mb:") {
                let (_, uuid) = split_vram_key(name)?;
                uuids.insert(canonical_gpu_uuid(uuid)?);
            }
        }
        for probe in self.conditions.iter().filter_map(|condition| {
            let ConditionPredicate::Probe { probe } = &condition.predicate else {
                return None;
            };
            Some(probe)
        }) {
            for name in probe.resources.custom.keys() {
                if name.starts_with("vram_mb:") {
                    let (_, uuid) = split_vram_key(name)?;
                    uuids.insert(canonical_gpu_uuid(uuid)?);
                }
            }
        }
        if let Some(observed) = &self.observed {
            for uuid in observed.gpu_utilization_percent_at_most.keys() {
                uuids.insert(canonical_gpu_uuid(uuid)?);
            }
        }
        if let Some(quiet) = &self.quiet {
            for uuid in quiet.detectors.iter().filter_map(QuietDetector::gpu_uuid) {
                uuids.insert(canonical_gpu_uuid(uuid)?);
            }
        }
        if uuids.len() > 1 {
            return Err(Error::InvalidSpec(
                "one Job cannot name more than one GPU UUID".into(),
            ));
        }
        Ok(uuids)
    }

    pub(crate) fn requires_host_observation(&self) -> bool {
        resource_claims_require_host_observation(&self.resources)
            || self.conditions.iter().any(|condition| {
                matches!(
                    &condition.predicate,
                    ConditionPredicate::Probe { probe }
                        if resource_claims_require_host_observation(&probe.resources)
                )
            })
            || self.observed.is_some()
            || self.quiet.is_some()
    }

    pub(crate) fn minimum_observation_age_millis(&self, host: &HostConfig) -> Option<u64> {
        let mut ages = Vec::new();
        if resource_claims_require_host_observation(&self.resources)
            || self.conditions.iter().any(|condition| {
                matches!(
                    &condition.predicate,
                    ConditionPredicate::Probe { probe }
                        if resource_claims_require_host_observation(&probe.resources)
                )
            })
        {
            ages.push(host.observation.memory_max_sample_age_millis);
        }
        if let Some(observed) = &self.observed {
            ages.push(observed.max_sample_age_seconds.saturating_mul(1_000));
        }
        if let Some(quiet) = &self.quiet {
            ages.push(quiet.max_sample_age_seconds.saturating_mul(1_000));
        }
        ages.into_iter().min()
    }
}

fn resource_claims_require_host_observation(resources: &ResourceClaims) -> bool {
    resources.ram_mb.is_some()
        || resources.gpu_slots.is_some()
        || resources
            .custom
            .keys()
            .any(|name| name.starts_with("vram_mb:"))
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ChildSubmissionPolicy {
    /// Inclusive minimum explicit priority that a child may request.
    #[schemars(range(min = -3, max = 3))]
    pub min_priority: i8,
    /// Inclusive maximum explicit priority that a child may request.
    #[schemars(range(min = -3, max = 3))]
    pub max_priority: i8,
    pub max_claims: ResourceClaimLimits,
    pub allowed_impacts: Vec<String>,
    pub required_labels: Vec<Label>,
    pub fences: ChildFencePolicy,
    pub allow_observed: bool,
    pub allow_quiet: bool,
    pub allow_delegation: bool,
}

impl ChildSubmissionPolicy {
    fn validate(&self) -> Result<()> {
        if self.min_priority < MIN_JOB_PRIORITY
            || self.max_priority > MAX_JOB_PRIORITY
            || self.min_priority > self.max_priority
        {
            return Err(Error::InvalidSpec(format!(
                "child priority range must be inclusive, ordered, and within {MIN_JOB_PRIORITY}..={MAX_JOB_PRIORITY}"
            )));
        }
        self.max_claims.validate()?;
        if self.allowed_impacts.len() > 16 {
            return Err(Error::InvalidSpec(
                "child policy has more than 16 allowed impacts".into(),
            ));
        }
        let mut impacts = BTreeSet::new();
        for impact in &self.allowed_impacts {
            validate_policy_name("impact", impact)?;
            if !impacts.insert(impact) {
                return Err(Error::InvalidSpec(
                    "child policy contains a duplicate allowed impact".into(),
                ));
            }
        }
        if self.required_labels.len() > 32 {
            return Err(Error::InvalidSpec(
                "child policy has more than 32 required labels".into(),
            ));
        }
        let mut label_keys = BTreeSet::new();
        for label in &self.required_labels {
            label.validate()?;
            if !label_keys.insert(&label.key) {
                return Err(Error::InvalidSpec(format!(
                    "child policy repeats required label key {:?}",
                    label.key
                )));
            }
        }
        self.fences.validate()
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ResourceClaimLimits {
    pub cpu_units: Option<u32>,
    pub ram_mb: Option<u64>,
    pub cargo_slots: Option<u32>,
    pub gpu_slots: Option<u32>,
    pub custom: BTreeMap<String, u64>,
}

impl ResourceClaimLimits {
    fn validate(&self) -> Result<()> {
        if self.custom.len() > 16 {
            return Err(Error::InvalidSpec(
                "child policy has more than 16 custom claim limits".into(),
            ));
        }
        if self.cpu_units == Some(0)
            || self.ram_mb == Some(0)
            || self.cargo_slots == Some(0)
            || self.gpu_slots == Some(0)
            || self.custom.iter().any(|(name, value)| {
                name.is_empty()
                    || name.len() > 64
                    || name.contains('\0')
                    || is_builtin_resource(name)
                    || *value == 0
            })
        {
            return Err(Error::InvalidSpec(
                "child claim limits and custom names must be valid and non-zero".into(),
            ));
        }
        validate_vram_keys(self.custom.keys())
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ChildFencePolicy {
    pub shared_roots: Vec<PathBuf>,
    pub exclusive_roots: Vec<PathBuf>,
}

impl ChildFencePolicy {
    fn validate(&self) -> Result<()> {
        if self.shared_roots.len() > 8 || self.exclusive_roots.len() > 8 {
            return Err(Error::InvalidSpec(
                "child policy has more than 8 roots in one fence mode".into(),
            ));
        }
        validate_policy_fence_roots("shared", &self.shared_roots)?;
        validate_policy_fence_roots("exclusive", &self.exclusive_roots)
    }
}

fn validate_policy_fence_roots(mode: &str, roots: &[PathBuf]) -> Result<()> {
    let mut spellings = BTreeSet::new();
    for root in roots {
        let spelling = root.as_os_str().to_string_lossy();
        let encoded_len = serde_json::to_vec(root)?.len();
        if spelling.is_empty()
            || spelling.contains('\0')
            || !root.is_absolute()
            || encoded_len > 512
        {
            return Err(Error::InvalidSpec(format!(
                "child policy {mode} roots must be nonempty absolute paths without NUL and at most 512 JSON bytes"
            )));
        }
        if !spellings.insert(spelling.into_owned()) {
            return Err(Error::InvalidSpec(format!(
                "child policy contains a duplicate {mode} root spelling"
            )));
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BatchSpec {
    pub spec_version: u32,
    pub jobs: Vec<BatchMember>,
}

/// Schema root covering both accepted submission document shapes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum SubmissionSpec {
    Job(Box<JobSpec>),
    Batch(Box<BatchSpec>),
}

impl BatchSpec {
    pub fn validate(&self) -> Result<()> {
        if self.spec_version != SPEC_VERSION {
            return Err(Error::InvalidSpec(format!(
                "unsupported spec_version {}, expected {SPEC_VERSION}",
                self.spec_version
            )));
        }
        if self.jobs.is_empty() || self.jobs.len() > 1024 {
            return Err(Error::InvalidSpec(
                "a batch must contain 1..=1024 jobs".into(),
            ));
        }
        let mut indices = HashMap::new();
        for (index, member) in self.jobs.iter().enumerate() {
            if member.name.is_empty() || member.name.len() > 128 || member.name.contains('\0') {
                return Err(Error::InvalidSpec("invalid batch member name".into()));
            }
            if indices.insert(member.name.as_str(), index).is_some() {
                return Err(Error::InvalidSpec(format!(
                    "duplicate batch member {:?}",
                    member.name
                )));
            }
            member.spec.validate()?;
        }
        let mut incoming = vec![0_usize; self.jobs.len()];
        let mut successors = vec![Vec::new(); self.jobs.len()];
        for (index, member) in self.jobs.iter().enumerate() {
            let mut seen = BTreeSet::new();
            for dependency in &member.dependencies {
                let Some(&predecessor) = indices.get(dependency.job.as_str()) else {
                    return Err(Error::InvalidSpec(format!(
                        "batch member {:?} depends on unknown member {:?}",
                        member.name, dependency.job
                    )));
                };
                if predecessor == index || !seen.insert(predecessor) {
                    return Err(Error::InvalidSpec(format!(
                        "invalid or duplicate dependency for {:?}",
                        member.name
                    )));
                }
                incoming[index] += 1;
                successors[predecessor].push(index);
            }
        }
        let mut ready: Vec<_> = incoming
            .iter()
            .enumerate()
            .filter_map(|(index, count)| (*count == 0).then_some(index))
            .collect();
        let mut visited = 0;
        while let Some(index) = ready.pop() {
            visited += 1;
            for &successor in &successors[index] {
                incoming[successor] -= 1;
                if incoming[successor] == 0 {
                    ready.push(successor);
                }
            }
        }
        if visited != self.jobs.len() {
            return Err(Error::InvalidSpec(
                "batch dependency graph contains a cycle".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BatchMember {
    pub name: String,
    pub spec: JobSpec,
    #[serde(default)]
    pub dependencies: Vec<DependencySpec>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DependencySpec {
    pub job: String,
    pub on: DependencyKind,
}

#[derive(
    Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize, JsonSchema,
)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum DependencyKind {
    Success,
    Failure,
    Terminal,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum StdinSpec {
    #[default]
    Eof,
    File {
        path: PathBuf,
    },
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentSpec {
    #[serde(default)]
    pub set: BTreeMap<String, String>,
    #[serde(default)]
    pub unset: Vec<String>,
}

impl EnvironmentSpec {
    fn validate(&self) -> Result<()> {
        let mut names = BTreeSet::new();
        for (name, value) in &self.set {
            validate_environment_entry(name, Some(value))?;
            if !names.insert(canonical_environment_name(name)) {
                return Err(Error::InvalidSpec(format!("environment repeats {name:?}")));
            }
        }
        for name in &self.unset {
            validate_environment_entry(name, None)?;
            if !names.insert(canonical_environment_name(name)) {
                return Err(Error::InvalidSpec(format!(
                    "environment both sets and unsets {name:?}"
                )));
            }
        }
        Ok(())
    }
}

fn validate_environment_entry(name: &str, value: Option<&String>) -> Result<()> {
    if name.is_empty()
        || name.contains(['\0', '='])
        || value.is_some_and(|value| value.contains('\0'))
        || canonical_environment_name(name).starts_with("STILLYARD_")
    {
        return Err(Error::InvalidSpec(format!(
            "invalid or reserved environment entry {name:?}"
        )));
    }
    Ok(())
}

fn canonical_environment_name(name: &str) -> String {
    if cfg!(windows) {
        name.to_uppercase()
    } else {
        name.to_owned()
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ResourceClaims {
    pub cpu_units: Option<u32>,
    pub ram_mb: Option<u64>,
    pub cargo_slots: Option<u32>,
    pub gpu_slots: Option<u32>,
    #[serde(default)]
    pub custom: BTreeMap<String, u64>,
    #[serde(default)]
    pub shared_fences: Vec<String>,
    #[serde(default)]
    pub exclusive_fences: Vec<String>,
    #[serde(default)]
    pub impacts: Vec<String>,
}

impl ResourceClaims {
    fn validate(&self) -> Result<()> {
        if self.custom.len() > 16
            || self.impacts.len() > 16
            || self.shared_fences.len() + self.exclusive_fences.len() > 8
        {
            return Err(Error::InvalidSpec(
                "resource claims exceed the version 2 count bounds".into(),
            ));
        }
        if self.cpu_units == Some(0)
            || self.ram_mb == Some(0)
            || self.cargo_slots == Some(0)
            || self.gpu_slots == Some(0)
            || self.custom.iter().any(|(name, value)| {
                name.is_empty()
                    || name.len() > 64
                    || name.contains('\0')
                    || is_builtin_resource(name)
                    || *value == 0
            })
        {
            return Err(Error::InvalidSpec(
                "resource quantities and custom names must be non-zero".into(),
            ));
        }
        validate_vram_keys(self.custom.keys())?;
        let mut impacts = BTreeSet::new();
        for impact in &self.impacts {
            validate_policy_name("impact", impact)?;
            if !impacts.insert(impact) {
                return Err(Error::InvalidSpec("duplicate impact tag".into()));
            }
        }
        let shared: BTreeSet<_> = self.shared_fences.iter().collect();
        let exclusive: BTreeSet<_> = self.exclusive_fences.iter().collect();
        if shared.len() != self.shared_fences.len()
            || exclusive.len() != self.exclusive_fences.len()
        {
            return Err(Error::InvalidSpec("duplicate path fence".into()));
        }
        if self
            .exclusive_fences
            .iter()
            .any(|fence| shared.contains(fence))
        {
            return Err(Error::InvalidSpec(
                "one path fence cannot be both shared and exclusive".into(),
            ));
        }
        for fence in self
            .shared_fences
            .iter()
            .chain(self.exclusive_fences.iter())
        {
            let path = PathBuf::from(fence);
            if fence.is_empty()
                || fence.contains('\0')
                || !path.is_absolute()
                || serde_json::to_vec(fence)?.len() > 512
            {
                return Err(Error::InvalidSpec(
                    "path fences must be nonempty absolute paths without NUL and at most 512 JSON bytes"
                        .into(),
                ));
            }
        }
        Ok(())
    }
}

/// Scalar capacities configured by the owner for one host-local daemon.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ResourceCapacities {
    pub cpu_units: u32,
    pub ram_mb: u64,
    pub cargo_slots: u32,
    pub gpu_slots: u32,
    #[serde(default)]
    pub custom: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct HostConfig {
    pub resources: ResourceCapacities,
    /// Directed declarations interpreted symmetrically by admission.
    #[serde(default)]
    pub impact_incompatibilities: BTreeMap<String, Vec<String>>,
    pub observation: HostObservationConfig,
}

impl HostConfig {
    pub fn validate(&self) -> Result<()> {
        self.resources.validate()?;
        self.observation.validate()?;
        for (impact, incompatible) in &self.impact_incompatibilities {
            validate_policy_name("impact", impact)?;
            let mut seen = BTreeSet::new();
            for other in incompatible {
                validate_policy_name("impact", other)?;
                if !seen.insert(other) {
                    return Err(Error::InvalidSpec(format!(
                        "duplicate incompatibility for impact {impact}"
                    )));
                }
            }
        }
        let configured_vram = self
            .resources
            .custom
            .iter()
            .filter(|(name, value)| name.starts_with("vram_mb:") && **value > 0)
            .collect::<Vec<_>>();
        if self.resources.ram_mb > 0 && self.observation.ram_safety_margin_mb == 0 {
            return Err(Error::InvalidSpec(
                "nonzero ram_mb capacity requires a positive RAM safety margin".into(),
            ));
        }
        if !configured_vram.is_empty() && self.observation.vram_safety_margin_mb == 0 {
            return Err(Error::InvalidSpec(
                "nonzero VRAM capacity requires a positive VRAM safety margin".into(),
            ));
        }
        if self.resources.gpu_slots > 0 || !configured_vram.is_empty() {
            let placement = self.observation.gpu_slot_uuid.as_deref().ok_or_else(|| {
                Error::InvalidSpec("configured GPU capacity requires gpu_slot_uuid".into())
            })?;
            let placement = canonical_gpu_uuid(placement)?;
            for (name, _) in configured_vram {
                let (_, uuid) = split_vram_key(name)?;
                if canonical_gpu_uuid(uuid)? != placement {
                    return Err(Error::InvalidSpec(
                        "configured VRAM UUID differs from gpu_slot_uuid".into(),
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn validate_job(&self, job: &JobSpec) -> Result<()> {
        let configured = self
            .observation
            .gpu_slot_uuid
            .as_deref()
            .map(canonical_gpu_uuid)
            .transpose()?;
        for uuid in job.gpu_uuids()? {
            if configured.as_deref().is_some_and(|value| value != uuid) {
                return Err(Error::InvalidSpec(
                    "Job GPU UUID differs from host gpu_slot_uuid".into(),
                ));
            }
        }
        if let Some(quiet) = &job.quiet {
            if quiet.stable_seconds > self.observation.admission_wall_clock_limit_seconds {
                return Err(Error::InvalidSpec(
                    "quiet stable_seconds exceeds host admission wall-clock limit".into(),
                ));
            }
            let invocations = u64::from(job.retry.max_attempts).saturating_mul(
                u64::try_from(job.postconditions.len() + 1)
                    .unwrap_or(u64::MAX)
                    .saturating_add(u64::from(self.observation.pre_release_max_deferrals)),
            );
            if invocations > 256 {
                return Err(Error::InvalidSpec(
                    "quiet retries, postconditions, and host deferrals exceed 256 Invocations"
                        .into(),
                ));
            }
        }
        Ok(())
    }
}

impl ResourceCapacities {
    pub fn validate(&self) -> Result<()> {
        if self.custom.keys().any(|name| {
            name.is_empty() || name.len() > 128 || name.contains('\0') || is_builtin_resource(name)
        }) {
            return Err(Error::InvalidSpec(
                "invalid, empty, or reserved custom capacity name".into(),
            ));
        }
        validate_vram_keys(self.custom.keys())?;
        Ok(())
    }
}

fn is_builtin_resource(name: &str) -> bool {
    matches!(name, "cpu_units" | "ram_mb" | "cargo_slots" | "gpu_slots")
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ConditionSpec {
    pub predicate: ConditionPredicate,
    /// Acceptance-anchored deadline; `none` is explicit and never synthesized by the daemon.
    pub deadline: ConditionDeadline,
    #[serde(default)]
    pub on_deadline: ConditionDeadlineOutcome,
}

impl ConditionSpec {
    fn validate(&self) -> Result<()> {
        match &self.deadline {
            ConditionDeadline::None => {}
            ConditionDeadline::Relative { seconds } if (1..=31_536_000).contains(seconds) => {}
            ConditionDeadline::Absolute { unix_millis } if *unix_millis > 0 => {}
            _ => return Err(Error::InvalidSpec("invalid Condition deadline".into())),
        }
        match &self.predicate {
            ConditionPredicate::PathTransition { from, to, .. } if from == to => {
                return Err(Error::InvalidSpec(
                    "path transition must change between absent and present".into(),
                ));
            }
            ConditionPredicate::PathExists { path }
            | ConditionPredicate::PathAbsent { path }
            | ConditionPredicate::PathTransition { path, .. } => validate_condition_path(path)?,
            ConditionPredicate::NotBefore { unix_millis } if *unix_millis <= 0 => {
                return Err(Error::InvalidSpec(
                    "not_before unix_millis must be positive".into(),
                ));
            }
            ConditionPredicate::NotBefore { .. } => {}
            ConditionPredicate::Probe { probe } => probe.validate()?,
        }
        Ok(())
    }
}

fn validate_condition_path(path: &PathBuf) -> Result<()> {
    if !path.is_absolute()
        || path.as_os_str().is_empty()
        || path.as_os_str().to_string_lossy().contains('\0')
        || serde_json::to_vec(path)?.len() > 4096
    {
        return Err(Error::InvalidSpec(
            "Condition path must be a bounded absolute path without NUL".into(),
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ConditionPredicate {
    PathExists {
        path: PathBuf,
    },
    PathAbsent {
        path: PathBuf,
    },
    PathTransition {
        path: PathBuf,
        from: PathConditionState,
        to: PathConditionState,
    },
    NotBefore {
        unix_millis: i64,
    },
    Probe {
        probe: Box<ProbeCondition>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PathConditionState {
    Absent,
    Present,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ConditionDeadline {
    None,
    Relative { seconds: u64 },
    Absolute { unix_millis: i64 },
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ConditionDeadlineOutcome {
    #[default]
    Failed,
    Canceled,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProbeCondition {
    pub executable: PathBuf,
    #[serde(default)]
    pub args: Vec<String>,
    pub working_directory: PathBuf,
    #[serde(default)]
    pub environment: EnvironmentSpec,
    #[serde(default)]
    pub resources: ResourceClaims,
    pub timeout_seconds: u64,
    pub interval_seconds: u64,
    pub accepted_exit_codes: Vec<i32>,
}

impl ProbeCondition {
    fn validate(&self) -> Result<()> {
        if !self.executable.is_absolute()
            || !self.working_directory.is_absolute()
            || self.executable.as_os_str().to_string_lossy().contains('\0')
            || self
                .working_directory
                .as_os_str()
                .to_string_lossy()
                .contains('\0')
            || self.args.iter().any(|argument| argument.contains('\0'))
        {
            return Err(Error::InvalidSpec(
                "probe executable and working_directory must be absolute and contain no NUL".into(),
            ));
        }
        if !(1..=3_600).contains(&self.timeout_seconds)
            || !(1..=86_400).contains(&self.interval_seconds)
            || self.accepted_exit_codes.is_empty()
            || self.accepted_exit_codes.len() > 256
        {
            return Err(Error::InvalidSpec("invalid probe bounds".into()));
        }
        let mut exits = BTreeSet::new();
        if self
            .accepted_exit_codes
            .iter()
            .any(|code| !exits.insert(*code))
        {
            return Err(Error::InvalidSpec(
                "duplicate accepted probe exit code".into(),
            ));
        }
        self.environment.validate()?;
        self.resources.validate()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct QuietPolicy {
    pub stable_seconds: u64,
    pub max_sample_age_seconds: u64,
    pub wait_budget_seconds: u64,
    #[serde(default)]
    pub detectors: Vec<QuietDetector>,
}

impl QuietPolicy {
    fn validate(&self) -> Result<()> {
        if !(1..=3600).contains(&self.stable_seconds)
            || !(1..=30).contains(&self.max_sample_age_seconds)
            || self.wait_budget_seconds < self.stable_seconds
            || self.wait_budget_seconds > 86_400
            || self.detectors.is_empty()
            || self.detectors.len() > 16
        {
            return Err(Error::InvalidSpec("invalid quiet policy bounds".into()));
        }
        let mut unique = BTreeSet::new();
        for detector in &self.detectors {
            detector.validate()?;
            if !unique.insert(detector) {
                return Err(Error::InvalidSpec("duplicate quiet detector".into()));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum QuietDetector {
    CpuUtilization { max_percent: u8 },
    GpuUtilization { gpu_uuid: String, max_percent: u8 },
    DiskUtilization { max_percent: u8 },
    ForeignGpuCompute { gpu_uuid: String },
    BlockedProcesses,
}

impl QuietDetector {
    fn validate(&self) -> Result<()> {
        match self {
            Self::CpuUtilization { max_percent } | Self::DiskUtilization { max_percent } => {
                validate_percent(*max_percent)
            }
            Self::GpuUtilization {
                gpu_uuid,
                max_percent,
            } => {
                canonical_gpu_uuid(gpu_uuid)?;
                validate_percent(*max_percent)
            }
            Self::ForeignGpuCompute { gpu_uuid } => canonical_gpu_uuid(gpu_uuid).map(|_| ()),
            Self::BlockedProcesses => Ok(()),
        }
    }

    fn gpu_uuid(&self) -> Option<&str> {
        match self {
            Self::GpuUtilization { gpu_uuid, .. } | Self::ForeignGpuCompute { gpu_uuid } => {
                Some(gpu_uuid)
            }
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ObservedResourcePolicy {
    pub max_sample_age_seconds: u64,
    pub cpu_utilization_percent_at_most: Option<u8>,
    #[serde(default)]
    pub gpu_utilization_percent_at_most: BTreeMap<String, u8>,
}

impl ObservedResourcePolicy {
    fn validate(&self) -> Result<()> {
        let nonempty = self.cpu_utilization_percent_at_most.is_some()
            || !self.gpu_utilization_percent_at_most.is_empty();
        if !nonempty || !(1..=30).contains(&self.max_sample_age_seconds) {
            return Err(Error::InvalidSpec(
                "invalid observed resource policy bounds".into(),
            ));
        }
        if let Some(percent) = self.cpu_utilization_percent_at_most {
            validate_percent(percent)?;
        }
        let mut canonical = BTreeSet::new();
        for (uuid, percent) in &self.gpu_utilization_percent_at_most {
            validate_percent(*percent)?;
            if !canonical.insert(canonical_gpu_uuid(uuid)?) {
                return Err(Error::InvalidSpec("duplicate observed GPU UUID".into()));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct HostObservationConfig {
    pub sample_interval_millis: u64,
    pub quiet_max_sample_gap_millis: u64,
    pub generation_max_cadence_gap_millis: u64,
    pub memory_max_sample_age_millis: u64,
    pub ram_safety_margin_mb: u64,
    pub vram_safety_margin_mb: u64,
    pub gpu_slot_uuid: Option<String>,
    pub process_rules: ProcessRules,
    pub pre_release_max_deferrals: u32,
    pub pre_release_backoff_millis: u64,
    pub admission_wall_clock_limit_seconds: u64,
    /// Maximum age of filesystem/clock Condition evidence before an authoritative rescan.
    pub condition_rescan_interval_millis: u64,
    pub gpu_provider: GpuProviderConfig,
}

impl Default for HostObservationConfig {
    fn default() -> Self {
        Self {
            sample_interval_millis: 1_000,
            quiet_max_sample_gap_millis: 2_000,
            generation_max_cadence_gap_millis: 2_500,
            memory_max_sample_age_millis: 2_500,
            ram_safety_margin_mb: 0,
            vram_safety_margin_mb: 0,
            gpu_slot_uuid: None,
            process_rules: ProcessRules::default(),
            pre_release_max_deferrals: 16,
            pre_release_backoff_millis: 1_000,
            admission_wall_clock_limit_seconds: 3_600,
            condition_rescan_interval_millis: 1_000,
            gpu_provider: GpuProviderConfig::Nvml,
        }
    }
}

impl HostObservationConfig {
    fn validate(&self) -> Result<()> {
        if !(100..=5_000).contains(&self.sample_interval_millis)
            || self.quiet_max_sample_gap_millis < self.sample_interval_millis
            || self.generation_max_cadence_gap_millis < self.quiet_max_sample_gap_millis
            || self.memory_max_sample_age_millis < self.sample_interval_millis
            || !(1..=64).contains(&self.pre_release_max_deferrals)
            || !(100..=60_000).contains(&self.pre_release_backoff_millis)
            || !(1..=86_400).contains(&self.admission_wall_clock_limit_seconds)
            || !(100..=60_000).contains(&self.condition_rescan_interval_millis)
        {
            return Err(Error::InvalidSpec("invalid host observation bounds".into()));
        }
        if let Some(uuid) = &self.gpu_slot_uuid {
            canonical_gpu_uuid(uuid)?;
        }
        self.process_rules.validate()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProcessRules {
    #[serde(default)]
    pub block: Vec<String>,
    #[serde(default = "default_process_ignores")]
    pub ignore: Vec<String>,
}

impl Default for ProcessRules {
    fn default() -> Self {
        Self {
            block: Vec::new(),
            ignore: default_process_ignores(),
        }
    }
}

impl ProcessRules {
    fn validate(&self) -> Result<()> {
        let mut block = BTreeSet::new();
        let mut ignore = BTreeSet::new();
        for pattern in &self.block {
            validate_process_pattern(pattern)?;
            if !block.insert(pattern.to_ascii_lowercase()) {
                return Err(Error::InvalidSpec(
                    "duplicate blocked process pattern".into(),
                ));
            }
        }
        for pattern in &self.ignore {
            validate_process_pattern(pattern)?;
            if !ignore.insert(pattern.to_ascii_lowercase()) {
                return Err(Error::InvalidSpec(
                    "duplicate ignored process pattern".into(),
                ));
            }
        }
        if block.iter().any(|pattern| ignore.contains(pattern)) {
            return Err(Error::InvalidSpec(
                "process pattern appears in both block and ignore".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum GpuProviderConfig {
    #[default]
    Nvml,
    Disabled,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub backoff_seconds: u64,
    #[serde(default)]
    pub retryable: Vec<String>,
}

impl RetryPolicy {
    fn validate(&self) -> Result<()> {
        const VERDICTS: &[&str] = &[
            "succeeded",
            "process_failed",
            "start_failed",
            "timed_out",
            "interrupted",
            "safety_failed",
            "postcondition_retryable",
            "postcondition_failed",
            "canceled",
        ];
        if self.max_attempts == 0 || self.max_attempts > 100 {
            return Err(Error::InvalidSpec(
                "retry.max_attempts must be in 1..=100".into(),
            ));
        }
        let mut seen = BTreeSet::new();
        for verdict in &self.retryable {
            if !VERDICTS.contains(&verdict.as_str()) {
                return Err(Error::InvalidSpec(format!(
                    "unknown retryable Attempt verdict {verdict}"
                )));
            }
            if matches!(
                verdict.as_str(),
                "succeeded" | "canceled" | "timed_out" | "interrupted"
            ) {
                return Err(Error::InvalidSpec(format!(
                    "Attempt verdict {verdict} cannot be retried"
                )));
            }
            if !seen.insert(verdict) {
                return Err(Error::InvalidSpec(
                    "retry.retryable contains a duplicate verdict".into(),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PostconditionSpec {
    pub executable: PathBuf,
    #[serde(default)]
    pub args: Vec<String>,
    /// Defaults to the owning Job's working directory.
    pub working_directory: Option<PathBuf>,
    #[serde(default = "default_accepted_exit_codes")]
    pub accepted_exit_codes: Vec<i32>,
    #[serde(default)]
    pub retryable_exit_codes: Vec<i32>,
}

impl PostconditionSpec {
    fn validate(&self, job: &JobSpec) -> Result<()> {
        if !self.executable.is_absolute()
            || self.executable.as_os_str().to_string_lossy().contains('\0')
            || self.args.iter().any(|argument| argument.contains('\0'))
        {
            return Err(Error::InvalidSpec(
                "postcondition executable must be absolute and process fields must not contain NUL"
                    .into(),
            ));
        }
        let working_directory = self
            .working_directory
            .as_deref()
            .unwrap_or(&job.working_directory);
        if !working_directory.is_absolute()
            || working_directory
                .as_os_str()
                .to_string_lossy()
                .contains('\0')
        {
            return Err(Error::InvalidSpec(
                "postcondition working_directory must be absolute without NUL".into(),
            ));
        }
        let accepted: BTreeSet<_> = self.accepted_exit_codes.iter().collect();
        if self.accepted_exit_codes.len() > 256
            || self.retryable_exit_codes.len() > 256
            || accepted.len() != self.accepted_exit_codes.len()
            || self
                .retryable_exit_codes
                .iter()
                .any(|code| accepted.contains(code))
            || self
                .retryable_exit_codes
                .iter()
                .collect::<BTreeSet<_>>()
                .len()
                != self.retryable_exit_codes.len()
        {
            return Err(Error::InvalidSpec(
                "postcondition accepted/retryable exit-code sets must be unique and disjoint"
                    .into(),
            ));
        }
        Ok(())
    }
}

fn default_accepted_exit_codes() -> Vec<i32> {
    vec![0]
}

fn validate_policy_name(kind: &str, name: &str) -> Result<()> {
    if name.is_empty() || name.len() > 64 || name.contains('\0') {
        return Err(Error::InvalidSpec(format!("invalid {kind} name")));
    }
    Ok(())
}

fn validate_percent(value: u8) -> Result<()> {
    if value <= 100 {
        Ok(())
    } else {
        Err(Error::InvalidSpec(
            "utilization percentage exceeds 100".into(),
        ))
    }
}

pub(crate) fn canonical_gpu_uuid(value: &str) -> Result<String> {
    if value.is_empty()
        || value.len() > 128
        || value.contains('\0')
        || !value.is_ascii()
        || !value.to_ascii_lowercase().starts_with("gpu-")
    {
        return Err(Error::InvalidSpec("invalid GPU UUID".into()));
    }
    Ok(value.to_ascii_lowercase())
}

fn split_vram_key(value: &str) -> Result<(&str, &str)> {
    let Some(uuid) = value.strip_prefix("vram_mb:") else {
        return Err(Error::InvalidSpec("invalid VRAM resource key".into()));
    };
    canonical_gpu_uuid(uuid)?;
    Ok(("vram_mb", uuid))
}

pub(crate) fn canonical_custom_resource_name(value: &str) -> Result<String> {
    if let Some(uuid) = value.strip_prefix("vram_mb:") {
        return Ok(format!("vram_mb:{}", canonical_gpu_uuid(uuid)?));
    }
    Ok(value.to_owned())
}

fn validate_vram_keys<'a>(keys: impl Iterator<Item = &'a String>) -> Result<()> {
    let mut canonical = BTreeSet::new();
    for name in keys {
        if name.to_ascii_lowercase().starts_with("vram_mb:") {
            if !name.starts_with("vram_mb:") {
                return Err(Error::InvalidSpec(
                    "VRAM resource keys require the lowercase vram_mb: prefix".into(),
                ));
            }
            let (_, uuid) = split_vram_key(name)?;
            if !canonical.insert(canonical_gpu_uuid(uuid)?) {
                return Err(Error::InvalidSpec("duplicate VRAM GPU UUID".into()));
            }
        }
    }
    Ok(())
}

fn validate_process_pattern(pattern: &str) -> Result<()> {
    if pattern.is_empty()
        || pattern.len() > 128
        || pattern.contains(['\0', '/', '\\', '?', '[', ']'])
        || !pattern.is_ascii()
    {
        return Err(Error::InvalidSpec(
            "invalid process basename pattern".into(),
        ));
    }
    Ok(())
}

fn default_process_ignores() -> Vec<String> {
    vec!["dwm.exe".into(), "LogonUI.exe".into()]
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 1,
            backoff_seconds: 0,
            retryable: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Label {
    pub key: String,
    pub value: String,
}

impl Label {
    fn validate(&self) -> Result<()> {
        if self.key.is_empty()
            || self.key.len() > 64
            || self.value.is_empty()
            || self.value.len() > 128
        {
            return Err(Error::InvalidSpec("label key/value is empty".into()));
        }
        if self.key.contains(['\0', '=']) || self.value.contains('\0') {
            return Err(Error::InvalidSpec(
                "label contains NUL or key contains '='".into(),
            ));
        }
        Ok(())
    }
}

pub fn schema_json() -> Result<String> {
    let schema = schema_for!(SubmissionSpec);
    let mut json = serde_json::to_string_pretty(&schema)?;
    json.push('\n');
    Ok(json)
}

pub fn config_schema_json() -> Result<String> {
    let schema = schema_for!(HostConfig);
    let mut json = serde_json::to_string_pretty(&schema)?;
    json.push('\n');
    Ok(json)
}

pub fn managed_execution_schema_json() -> Result<String> {
    let schema = schema_for!(crate::ManagedExecutionRecord);
    let mut json = serde_json::to_string_pretty(&schema)?;
    json.push('\n');
    Ok(json)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_is_stable_within_one_build() {
        assert_eq!(
            schema_json().unwrap(),
            include_str!("../schema/stillyard-spec-v4.json")
        );
        assert_eq!(
            config_schema_json().unwrap(),
            include_str!("../schema/stillyard-config-v2.json")
        );
        assert_eq!(
            managed_execution_schema_json().unwrap(),
            include_str!("../schema/stillyard-managed-execution-v3.json")
        );
    }

    #[test]
    fn unknown_job_field_rejects() {
        let json = r#"{
            "spec_version": 3,
            "executable": "tool.exe",
            "working_directory": ".",
            "surprise": true
        }"#;
        assert!(serde_json::from_str::<JobSpec>(json).is_err());
    }

    #[test]
    fn daemon_environment_presets_are_not_accepted() {
        let job = r#"{
            "spec_version": 3,
            "executable": "tool.exe",
            "working_directory": ".",
            "environment": { "profile": "reviewer" }
        }"#;
        assert!(serde_json::from_str::<JobSpec>(job).is_err());

        let config = r#"{
            "resources": {},
            "profiles": { "reviewer": { "set": { "PATH": "tools" } } }
        }"#;
        assert!(serde_json::from_str::<HostConfig>(config).is_err());
    }

    #[test]
    fn supported_claim_and_impact_validate() {
        let mut job: JobSpec = serde_json::from_str(
            r#"{
                "spec_version": 4,
                "executable": "tool.exe",
                "working_directory": ".",
                "resources": { "gpu_slots": 1 }
            }"#,
        )
        .unwrap();
        let root = std::env::current_dir().unwrap();
        job.executable = root.join("tool.exe");
        job.working_directory = root;
        assert!(job.validate().is_ok());
        job.resources.impacts.push("measurement".into());
        assert!(job.validate().is_ok());
    }

    #[test]
    fn retry_verdicts_must_be_unique() {
        let retry = RetryPolicy {
            max_attempts: 2,
            backoff_seconds: 0,
            retryable: vec!["process_failed".into(), "process_failed".into()],
        };

        assert!(retry.validate().is_err());
    }

    #[test]
    fn lifecycle_history_has_a_finite_snapshot_bound() {
        let root = std::env::current_dir().unwrap();
        let mut job: JobSpec = serde_json::from_str(&format!(
            r#"{{
                "spec_version": 4,
                "executable": {},
                "working_directory": {},
                "retry": {{ "max_attempts": 100, "backoff_seconds": 0 }}
            }}"#,
            serde_json::to_string(&root.join("tool.exe")).unwrap(),
            serde_json::to_string(&root).unwrap(),
        ))
        .unwrap();
        job.postconditions = (0..2)
            .map(|_| PostconditionSpec {
                executable: root.join("validator.exe"),
                args: Vec::new(),
                working_directory: None,
                accepted_exit_codes: vec![0],
                retryable_exit_codes: Vec::new(),
            })
            .collect();

        assert!(job.validate().is_err());
    }

    #[test]
    fn host_deferral_bound_applies_only_to_jobs_that_request_quiet() {
        let root = std::env::current_dir().unwrap();
        let mut job: JobSpec = serde_json::from_str(&format!(
            r#"{{
                "spec_version": 4,
                "executable": {},
                "working_directory": {},
                "retry": {{ "max_attempts": 100, "backoff_seconds": 0 }}
            }}"#,
            serde_json::to_string(&root.join("tool.exe")).unwrap(),
            serde_json::to_string(&root).unwrap(),
        ))
        .unwrap();
        job.postconditions.push(PostconditionSpec {
            executable: root.join("validator.exe"),
            args: Vec::new(),
            working_directory: None,
            accepted_exit_codes: vec![0],
            retryable_exit_codes: Vec::new(),
        });
        assert!(job.validate().is_ok());
        let mut host = HostConfig::default();
        host.observation.pre_release_max_deferrals = 64;
        assert!(host.validate_job(&job).is_ok());

        job.quiet = Some(QuietPolicy {
            stable_seconds: 1,
            max_sample_age_seconds: 1,
            wait_budget_seconds: 1,
            detectors: vec![QuietDetector::CpuUtilization { max_percent: 0 }],
        });
        assert!(job.validate().is_ok());
        assert!(host.validate_job(&job).is_err());
    }

    #[test]
    fn version_four_rejects_duplicate_label_keys_and_invalid_policy_shapes() {
        let root = std::env::current_dir().unwrap();
        let mut job: JobSpec = serde_json::from_str(&format!(
            r#"{{
                "spec_version": 4,
                "executable": {},
                "working_directory": {}
            }}"#,
            serde_json::to_string(&root.join("tool.exe")).unwrap(),
            serde_json::to_string(&root).unwrap(),
        ))
        .unwrap();
        job.labels = vec![
            Label {
                key: "project".into(),
                value: "one".into(),
            },
            Label {
                key: "project".into(),
                value: "two".into(),
            },
        ];
        assert!(
            matches!(job.validate(), Err(Error::InvalidSpec(detail)) if detail.contains("duplicate label key"))
        );

        job.labels.clear();
        job.child_submission_policy = Some(ChildSubmissionPolicy {
            allowed_impacts: vec!["cpu_heavy".into(), "cpu_heavy".into()],
            ..Default::default()
        });
        assert!(
            matches!(job.validate(), Err(Error::InvalidSpec(detail)) if detail.contains("duplicate allowed impact"))
        );

        job.child_submission_policy = Some(ChildSubmissionPolicy {
            max_claims: ResourceClaimLimits {
                cargo_slots: Some(0),
                ..Default::default()
            },
            ..Default::default()
        });
        assert!(job.validate().is_err());
    }

    #[test]
    fn child_policy_unknown_fields_are_rejected_during_decode() {
        let json = r#"{
            "spec_version": 3,
            "executable": "C:\\tool.exe",
            "working_directory": "C:\\",
            "child_submission_policy": { "future_authority": true }
        }"#;
        assert!(serde_json::from_str::<JobSpec>(json).is_err());
    }
}
