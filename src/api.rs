use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    AttemptId, AttemptVerdict, BatchId, ContainmentId, InvocationId, JobId, JobOutcome, JobSpec,
    JobState, Label, ResourceClaims, SubmissionId, SubmissionState,
};

pub const MAX_OBSERVATION_PAGE: u32 = 1_024;
pub const MAX_WAIT_STREAM_JOBS: usize = 1_024;

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("invalid observation cursor")]
pub struct ObservationCursorParseError;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EventCursor {
    pub store_uuid: Uuid,
    pub sequence: u64,
}

impl fmt::Display for EventCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}", self.store_uuid, self.sequence)
    }
}

impl FromStr for EventCursor {
    type Err = ObservationCursorParseError;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        let (store, sequence) = value.split_once(':').ok_or(ObservationCursorParseError)?;
        Ok(Self {
            store_uuid: Uuid::parse_str(store).map_err(|_| ObservationCursorParseError)?,
            sequence: sequence.parse().map_err(|_| ObservationCursorParseError)?,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct JobListCursor {
    pub store_uuid: Uuid,
    pub accepted_unix_millis: i64,
    pub job_id: JobId,
}

impl fmt::Display for JobListCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}:{}:{}",
            self.store_uuid,
            self.accepted_unix_millis,
            self.job_id.entity_uuid()
        )
    }
}

impl FromStr for JobListCursor {
    type Err = ObservationCursorParseError;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        let mut parts = value.split(':');
        let store_uuid = Uuid::parse_str(parts.next().ok_or(ObservationCursorParseError)?)
            .map_err(|_| ObservationCursorParseError)?;
        let accepted_unix_millis = parts
            .next()
            .ok_or(ObservationCursorParseError)?
            .parse()
            .map_err(|_| ObservationCursorParseError)?;
        let entity = Uuid::parse_str(parts.next().ok_or(ObservationCursorParseError)?)
            .map_err(|_| ObservationCursorParseError)?;
        if parts.next().is_some() {
            return Err(ObservationCursorParseError);
        }
        Ok(Self {
            store_uuid,
            accepted_unix_millis,
            job_id: JobId::from_parts(store_uuid, entity),
        })
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum JobSelector {
    #[default]
    All,
    Jobs {
        job_ids: Vec<JobId>,
    },
    Batch {
        batch_id: BatchId,
    },
    Labels {
        labels: Vec<Label>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum SchedulerEventKind {
    JobChanged,
    LogCommitted,
    AttemptChanged,
    InvocationChanged,
    ContainmentChanged,
    CancellationRequested,
    /// A newer daemon committed a change kind this client does not yet name.
    #[serde(other)]
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(deny_unknown_fields)]
pub struct SchedulerEvent {
    pub cursor: EventCursor,
    pub kind: SchedulerEventKind,
    pub job_id: JobId,
    pub batch_id: Option<BatchId>,
    pub committed_unix_millis: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(deny_unknown_fields)]
pub struct JobSummary {
    pub job_id: JobId,
    pub batch_id: Option<BatchId>,
    pub batch_member: Option<String>,
    pub parent: Option<ManagedParent>,
    pub state: JobState,
    pub outcome: Option<JobOutcome>,
    pub accepted_unix_millis: i64,
    pub started_unix_millis: Option<i64>,
    pub finished_unix_millis: Option<i64>,
    pub queue_rank: Option<u64>,
    pub estimate: Estimate,
    pub claims: ResourceClaims,
    pub blocker: Option<Blocker>,
    pub attempt_id: Option<AttemptId>,
    pub invocation_id: Option<InvocationId>,
    pub stdout_committed: u64,
    pub stderr_committed: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(deny_unknown_fields)]
pub struct JobListPage {
    pub jobs: Vec<JobSummary>,
    pub next_cursor: Option<JobListCursor>,
    pub event_cursor: EventCursor,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(deny_unknown_fields)]
pub struct EventGap {
    pub requested: EventCursor,
    pub oldest_available: EventCursor,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(tag = "frame", rename_all = "snake_case", deny_unknown_fields)]
pub enum ObservationFrame {
    Events {
        events: Vec<SchedulerEvent>,
        cursor: EventCursor,
    },
    Gap {
        gap: EventGap,
        snapshot: JobListPage,
        cursor: EventCursor,
    },
}

impl ObservationFrame {
    #[must_use]
    pub fn cursor(&self) -> EventCursor {
        match self {
            Self::Events { cursor, .. } | Self::Gap { cursor, .. } => *cursor,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(tag = "item", rename_all = "snake_case", deny_unknown_fields)]
pub enum WaitStreamItem {
    Settlement { snapshot: Box<JobSnapshot> },
    Aggregate { outcome: Option<JobOutcome> },
}

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
    pub daemon_generation: Uuid,
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
    pub daemon_generation: Uuid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum InvocationRole {
    Primary,
    Postcondition,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum InvocationState {
    Prepared,
    Started,
    Exited,
    Resolved,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum ContainmentState {
    Creating,
    Live,
    Empty,
    Uncertain,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(deny_unknown_fields)]
pub struct ContainmentSnapshot {
    pub containment_id: ContainmentId,
    pub state: ContainmentState,
    /// `windows_job_object` in v0.1; kept explicit for Linux v0.2 provenance.
    pub strength: String,
    pub incident_id: Option<ContainmentId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum ExitClassification {
    Accepted,
    Retryable,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(deny_unknown_fields)]
pub struct InvocationSnapshot {
    pub invocation_id: InvocationId,
    pub role: InvocationRole,
    pub role_index: u32,
    pub state: InvocationState,
    pub root_pid: Option<u32>,
    pub root_exit_code: Option<i32>,
    pub exit_classification: Option<ExitClassification>,
    pub executable_hash: Option<String>,
    pub daemon_generation: Option<Uuid>,
    pub started_unix_millis: Option<i64>,
    pub finished_unix_millis: Option<i64>,
    pub containment: ContainmentSnapshot,
    pub stdout_tail: String,
    pub stderr_tail: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(deny_unknown_fields)]
pub struct AttemptSnapshot {
    pub attempt_id: AttemptId,
    pub attempt_index: u32,
    pub verdict: Option<AttemptVerdict>,
    pub started_unix_millis: i64,
    pub deadline_unix_millis: Option<i64>,
    pub finished_unix_millis: Option<i64>,
    pub invocations: Vec<InvocationSnapshot>,
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
    pub cancel_requested: bool,
    pub accepted_unix_millis: i64,
    pub started_unix_millis: Option<i64>,
    pub finished_unix_millis: Option<i64>,
    pub spec: JobSpec,
    #[serde(default)]
    pub parent: Option<ManagedParent>,
    #[serde(default)]
    pub blockers: Vec<Blocker>,
    #[serde(default)]
    pub attempts: Vec<AttemptSnapshot>,
    pub daemon_generation: Uuid,
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
    pub daemon_generation: Uuid,
    pub version: String,
    pub pid: u32,
    pub store_path: PathBuf,
    pub config_path: PathBuf,
    pub capacities: crate::ResourceCapacities,
    pub profile_names: Vec<String>,
    pub config_sha256: String,
    pub queued_jobs: u64,
    pub running_jobs: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alpha7_cursors_have_stable_cli_round_trips() {
        let store_uuid = Uuid::now_v7();
        let event = EventCursor {
            store_uuid,
            sequence: 42,
        };
        assert_eq!(event.to_string().parse::<EventCursor>().unwrap(), event);

        let list = JobListCursor {
            store_uuid,
            accepted_unix_millis: -7,
            job_id: JobId::from_parts(store_uuid, Uuid::now_v7()),
        };
        assert_eq!(list.to_string().parse::<JobListCursor>().unwrap(), list);
        assert!("not-a-cursor".parse::<EventCursor>().is_err());
        assert_eq!(
            serde_json::from_str::<SchedulerEventKind>("\"future_kind\"").unwrap(),
            SchedulerEventKind::Unknown
        );
    }
}
