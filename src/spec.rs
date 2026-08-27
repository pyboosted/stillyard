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
    pub labels: Vec<Label>,
    pub expected_duration_seconds: Option<u64>,
    pub timeout_seconds: Option<u64>,
    pub quiet: Option<QuietPolicy>,
    #[serde(default)]
    pub artifacts: Vec<PathBuf>,
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
        for (name, value) in &self.environment.set {
            if name.is_empty()
                || name.contains(['\0', '='])
                || value.contains('\0')
                || name.to_ascii_uppercase().starts_with("STILLYARD_")
            {
                return Err(Error::InvalidSpec(format!(
                    "invalid or reserved environment entry {name:?}"
                )));
            }
        }
        if self
            .environment
            .unset
            .iter()
            .any(|name| name.is_empty() || name.contains(['\0', '=']))
        {
            return Err(Error::InvalidSpec(
                "invalid environment name in unset list".into(),
            ));
        }
        if self.retry.max_attempts == 0 {
            return Err(Error::InvalidSpec(
                "retry.max_attempts must be positive".into(),
            ));
        }
        // The first executable slice fails closed for baseline features whose admission providers
        // are not shipped yet. Declaring a claim must never silently run as if it were satisfied.
        self.resources.validate()?;
        if self.stdin != StdinSpec::Eof
            || self.environment.profile.is_some()
            || !self.conditions.is_empty()
            || self.retry != RetryPolicy::default()
            || self.quiet.is_some()
            || !self.artifacts.is_empty()
        {
            return Err(Error::InvalidSpec(
                "this alpha implements EOF stdin, resource admission, and single-attempt jobs without Conditions/quiet/artifacts only".into(),
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
        if !self.impacts.is_empty() {
            return Err(Error::InvalidSpec(
                "impact incompatibility policies are not implemented in increment 2a".into(),
            ));
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
    let schema = schema_for!(ResourceCapacities);
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
    fn supported_claim_validates_but_unimplemented_impact_rejects() {
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
        assert!(job.validate().is_err());
    }
}
