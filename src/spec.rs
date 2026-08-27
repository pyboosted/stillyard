use std::collections::BTreeMap;
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
        if self.stdin != StdinSpec::Eof
            || self.environment.profile.is_some()
            || self.resources != ResourceClaims::default()
            || !self.conditions.is_empty()
            || self.retry != RetryPolicy::default()
            || self.quiet.is_some()
            || !self.artifacts.is_empty()
        {
            return Err(Error::InvalidSpec(
                "this alpha implements EOF stdin and unconstrained single-attempt jobs only".into(),
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
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
    let schema = schema_for!(BatchSpec);
    let mut json = serde_json::to_string_pretty(&schema)?;
    json.push('\n');
    Ok(json)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_is_stable_within_one_build() {
        assert_eq!(schema_json().unwrap(), schema_json().unwrap());
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
    fn unsupported_claim_never_runs_unenforced() {
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
        assert!(job.validate().is_err());
        job.resources = ResourceClaims::default();
        assert!(job.validate().is_ok());
    }
}
