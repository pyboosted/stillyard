use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    AttemptId, BatchId, ContainmentId, InvocationId, JobId, JobOutcome, JobSpec, JobState,
    SubmissionId, SubmissionState,
};

/// Server-authenticated identity of the primary Invocation that submitted child work.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ManagedParent {
    pub job_id: JobId,
    pub attempt_id: AttemptId,
    pub invocation_id: InvocationId,
}

/// Store and optional managed-parent identity observed for this client connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SubmissionContext {
    pub store_uuid: Uuid,
    pub parent: Option<ManagedParent>,
}

#[derive(Clone, Debug, Default)]
pub struct CancellationToken {
    canceled: Arc<AtomicBool>,
}

impl CancellationToken {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.canceled.store(true, Ordering::Release);
    }

    #[must_use]
    pub fn is_canceled(&self) -> bool {
        self.canceled.load(Ordering::Acquire)
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct SubmitOptions {
    pub idempotency_key: Uuid,
    pub result_file: Option<PathBuf>,
    /// Declares that the caller will synchronously wait for the accepted Job or Batch.
    ///
    /// Managed callers are admitted only when that wait cannot depend on resources retained by
    /// the caller or its authenticated ancestors. Detached submissions leave this false.
    pub wait_for_completion: bool,
}

impl Default for SubmitOptions {
    fn default() -> Self {
        Self {
            idempotency_key: Uuid::now_v7(),
            result_file: None,
            wait_for_completion: false,
        }
    }
}

impl SubmitOptions {
    #[must_use]
    pub fn new(idempotency_key: Uuid) -> Self {
        Self {
            idempotency_key,
            result_file: None,
            wait_for_completion: false,
        }
    }

    #[must_use]
    pub fn with_result_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.result_file = Some(path.into());
        self
    }

    /// Declares a combined submit-and-wait operation for managed deadlock admission.
    #[must_use]
    pub fn with_wait_for_completion(mut self) -> Self {
        self.wait_for_completion = true;
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum EstimateConfidence {
    Estimated,
    LowerBoundOnly,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(deny_unknown_fields)]
pub struct Estimate {
    pub confidence: EstimateConfidence,
    pub start_in_millis: Option<u64>,
    #[serde(default)]
    pub assumptions: Vec<String>,
}

impl Estimate {
    #[must_use]
    pub fn unknown(reason: impl Into<String>) -> Self {
        Self {
            confidence: EstimateConfidence::Unknown,
            start_in_millis: None,
            assumptions: vec![reason.into()],
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(deny_unknown_fields)]
pub struct Blocker {
    pub code: String,
    pub detail: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(deny_unknown_fields)]
pub struct JobReceipt {
    pub submission_id: SubmissionId,
    pub job_id: JobId,
    pub submission_state: SubmissionState,
    pub job_state: JobState,
    #[serde(default)]
    pub blockers: Vec<Blocker>,
    pub queue_rank: Option<u64>,
    pub estimate: Estimate,
    pub parent: Option<ManagedParent>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(deny_unknown_fields)]
pub struct BatchJobReceipt {
    pub name: String,
    pub receipt: JobReceipt,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(deny_unknown_fields)]
pub struct BatchReceipt {
    pub submission_id: SubmissionId,
    pub batch_id: BatchId,
    pub submission_state: SubmissionState,
    pub jobs: Vec<BatchJobReceipt>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(deny_unknown_fields)]
pub struct JobSnapshot {
    pub job_id: JobId,
    pub submission_id: SubmissionId,
    pub batch_id: Option<BatchId>,
    pub batch_member: Option<String>,
    pub state: JobState,
    pub outcome: Option<JobOutcome>,
    pub attempt_id: Option<AttemptId>,
    pub invocation_id: Option<InvocationId>,
    pub containment_id: Option<ContainmentId>,
    pub root_exit_code: Option<i32>,
    pub accepted_unix_millis: i64,
    pub started_unix_millis: Option<i64>,
    pub finished_unix_millis: Option<i64>,
    pub spec: JobSpec,
    #[serde(default)]
    pub parent: Option<ManagedParent>,
    #[serde(default)]
    pub blockers: Vec<Blocker>,
}

impl JobSnapshot {
    #[must_use]
    pub fn is_final(&self) -> bool {
        self.state == JobState::Final
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum RecoveryResult {
    Received { submission_id: SubmissionId },
    Accepted(JobReceipt),
    AcceptedBatch(BatchReceipt),
    Rejected { code: String, detail: String },
    Conflict,
    NotReceived,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum LogStream {
    Stdout,
    Stderr,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(deny_unknown_fields)]
pub struct LogChunk {
    pub job_id: JobId,
    pub stream: LogStream,
    pub offset: u64,
    pub bytes: Vec<u8>,
    pub next_offset: u64,
    pub eof: bool,
    pub gap: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(deny_unknown_fields)]
pub struct DaemonSnapshot {
    pub store_uuid: Uuid,
    pub version: String,
    pub pid: u32,
    pub store_path: PathBuf,
    pub config_path: PathBuf,
    pub capacities: crate::ResourceCapacities,
    pub queued_jobs: u64,
    pub running_jobs: u64,
}
