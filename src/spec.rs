use std::collections::BTreeMap;
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;

use schemars::{JsonSchema, schema_for};
use serde::{Deserialize, Serialize};

use crate::{Error, Result};

pub const SPEC_VERSION: u32 = 1;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct JobSpec {
    pub spec_version: u32,
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
    /// Allows processes in this primary Invocation's OS containment to submit child work.
    #[serde(default)]
    pub allow_child_submissions: bool,
}

impl JobSpec {
    pub fn validate(&self) -> Result<()> {
        if self.spec_version != SPEC_VERSION {
            return Err(Error::InvalidSpec(format!(
                "unsupported spec_version {}, expected {SPEC_VERSION}",
                self.spec_version
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
        for label in &self.labels {
            label.validate()?;
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
        self.retry.validate()?;
        if self.postconditions.len() > 32 {
            return Err(Error::InvalidSpec("more than 32 postconditions".into()));
        }
        let lifecycle_invocations = u64::from(self.retry.max_attempts)
            .saturating_mul(u64::try_from(self.postconditions.len() + 1).unwrap_or(u64::MAX));
        if lifecycle_invocations > 256 {
            return Err(Error::InvalidSpec(
                "retry attempts times Invocations per Attempt exceeds 256".into(),
            ));
        }
        for postcondition in &self.postconditions {
            postcondition.validate(self)?;
        }
        if !self.conditions.is_empty() || self.quiet.is_some() || !self.artifacts.is_empty() {
            return Err(Error::InvalidSpec(
                "this alpha implements staged/EOF stdin, environment profiles, resource admission, retries, and postconditions without Conditions/quiet/artifacts only".into(),
            ));
        }
        Ok(())
    }
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
    pub profile: Option<String>,
    #[serde(default)]
    pub set: BTreeMap<String, String>,
    #[serde(default)]
    pub unset: Vec<String>,
}

impl EnvironmentSpec {
    fn validate(&self) -> Result<()> {
        if self
            .profile
            .as_ref()
            .is_some_and(|name| name.is_empty() || name.len() > 128 || name.contains('\0'))
        {
            return Err(Error::InvalidSpec(
                "invalid environment profile name".into(),
            ));
        }
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

pub(crate) fn expand_environment(
    requested: &EnvironmentSpec,
    profiles: &BTreeMap<String, EnvironmentProfile>,
) -> Result<EnvironmentSpec> {
    requested.validate()?;
    let mut values = BTreeMap::<String, String>::new();
    let mut removed = BTreeSet::<String>::new();
    let mut locked = BTreeSet::<String>::new();
    let mut profile_unset = BTreeSet::<String>::new();

    if let Some(profile_name) = &requested.profile {
        let profile = profiles.get(profile_name).ok_or_else(|| {
            Error::InvalidSpec(format!("unknown environment profile {profile_name:?}"))
        })?;
        profile.validate()?;
        for (name, value) in &profile.set {
            let name = canonical_environment_name(name);
            values.insert(name.clone(), value.clone());
            removed.remove(&name);
        }
        for name in &profile.unset {
            let name = canonical_environment_name(name);
            values.remove(&name);
            removed.insert(name.clone());
            profile_unset.insert(name);
        }
        for (name, value) in &profile.locked_set {
            let name = canonical_environment_name(name);
            values.insert(name.clone(), value.clone());
            removed.remove(&name);
            locked.insert(name);
        }
        for name in &profile.locked_unset {
            let name = canonical_environment_name(name);
            values.remove(&name);
            removed.insert(name.clone());
            locked.insert(name);
        }
    }

    for (name, value) in &requested.set {
        let name = canonical_environment_name(name);
        if locked.contains(&name) {
            return Err(Error::InvalidSpec(format!(
                "job overrides locked environment name {name:?}"
            )));
        }
        if profile_unset.contains(&name) {
            continue;
        }
        values.insert(name.clone(), value.clone());
        removed.remove(&name);
    }
    for name in &requested.unset {
        let name = canonical_environment_name(name);
        if locked.contains(&name) {
            return Err(Error::InvalidSpec(format!(
                "job overrides locked environment name {name:?}"
            )));
        }
        values.remove(&name);
        removed.insert(name);
    }

    Ok(EnvironmentSpec {
        profile: requested.profile.clone(),
        set: values,
        unset: removed.into_iter().collect(),
    })
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
#[serde(default, deny_unknown_fields)]
pub struct EnvironmentProfile {
    pub set: BTreeMap<String, String>,
    pub unset: Vec<String>,
    pub locked_set: BTreeMap<String, String>,
    pub locked_unset: Vec<String>,
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
        if self.cpu_units == Some(0)
            || self.ram_mb == Some(0)
            || self.cargo_slots == Some(0)
            || self.gpu_slots == Some(0)
            || self.custom.iter().any(|(name, value)| {
                name.is_empty()
                    || name.len() > 128
                    || name.contains('\0')
                    || is_builtin_resource(name)
                    || *value == 0
            })
        {
            return Err(Error::InvalidSpec(
                "resource quantities and custom names must be non-zero".into(),
            ));
        }
        let mut impacts = BTreeSet::new();
        for impact in &self.impacts {
            validate_policy_name("impact", impact)?;
            if !impacts.insert(impact) {
                return Err(Error::InvalidSpec("duplicate impact tag".into()));
            }
        }
        let shared: BTreeSet<_> = self.shared_fences.iter().collect();
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
            if fence.is_empty() || fence.contains('\0') || !path.is_absolute() {
                return Err(Error::InvalidSpec(
                    "path fences must be nonempty absolute paths without NUL".into(),
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
    pub profiles: BTreeMap<String, EnvironmentProfile>,
    /// Directed declarations interpreted symmetrically by admission.
    #[serde(default)]
    pub impact_incompatibilities: BTreeMap<String, Vec<String>>,
}

impl HostConfig {
    pub fn validate(&self) -> Result<()> {
        self.resources.validate()?;
        for (name, profile) in &self.profiles {
            if name.is_empty() || name.len() > 128 || name.contains('\0') {
                return Err(Error::InvalidSpec(
                    "invalid environment profile name".into(),
                ));
            }
            profile.validate()?;
        }
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
        Ok(())
    }
}

impl EnvironmentProfile {
    fn validate(&self) -> Result<()> {
        let mut names = BTreeSet::new();
        for (name, value) in self.set.iter().chain(&self.locked_set) {
            validate_environment_entry(name, Some(value))?;
            if !names.insert(canonical_environment_name(name)) {
                return Err(Error::InvalidSpec(format!(
                    "environment profile repeats {name:?}"
                )));
            }
        }
        for name in self.unset.iter().chain(&self.locked_unset) {
            validate_environment_entry(name, None)?;
            if !names.insert(canonical_environment_name(name)) {
                return Err(Error::InvalidSpec(format!(
                    "environment profile repeats {name:?}"
                )));
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
        Ok(())
    }
}

fn is_builtin_resource(name: &str) -> bool {
    matches!(name, "cpu_units" | "ram_mb" | "cargo_slots" | "gpu_slots")
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ConditionSpec {
    PathExists { path: PathBuf },
    PathAbsent { path: PathBuf },
    NotBefore { unix_millis: i64 },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct QuietPolicy {
    pub stable_seconds: u64,
    pub max_sample_age_seconds: u64,
    pub wait_budget_seconds: u64,
    #[serde(default)]
    pub detectors: Vec<String>,
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
    if name.is_empty() || name.len() > 128 || name.contains('\0') {
        return Err(Error::InvalidSpec(format!("invalid {kind} name")));
    }
    Ok(())
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
        if self.key.is_empty() || self.value.is_empty() {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_is_stable_within_one_build() {
        if std::env::var_os("STILLYARD_UPDATE_SCHEMA").is_some() {
            std::fs::write("schema/stillyard-spec-v1.json", schema_json().unwrap()).unwrap();
            std::fs::write(
                "schema/stillyard-config-v1.json",
                config_schema_json().unwrap(),
            )
            .unwrap();
        }
        assert_eq!(
            schema_json().unwrap(),
            include_str!("../schema/stillyard-spec-v1.json")
        );
        assert_eq!(
            config_schema_json().unwrap(),
            include_str!("../schema/stillyard-config-v1.json")
        );
    }

    #[test]
    fn unknown_job_field_rejects() {
        let json = r#"{
            "spec_version": 1,
            "executable": "tool.exe",
            "working_directory": ".",
            "surprise": true
        }"#;
        assert!(serde_json::from_str::<JobSpec>(json).is_err());
    }

    #[test]
    fn supported_claim_and_impact_validate() {
        let mut job: JobSpec = serde_json::from_str(
            r#"{
                "spec_version": 1,
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
                "spec_version": 1,
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
}
