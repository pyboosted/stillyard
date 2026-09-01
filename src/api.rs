use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    AttemptId, AttemptVerdict, BatchId, ChildFencePolicy, ContainmentId, InvocationId, JobId,
    JobOutcome, JobSpec, JobState, Label, ReservationId, ResourceClaimLimits, ResourceClaims,
    SubmissionId, SubmissionState,
};

pub const MAX_OBSERVATION_PAGE: u32 = 1_024;
pub const MAX_TREE_PAGE_NODES: u32 = 256;
pub const MAX_TREE_SELECTOR_JOBS: usize = 64;
pub const MAX_WAIT_STREAM_JOBS: usize = 1_024;

/// Scalar-only claim vector protected by a Reservation.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
pub struct ScalarResourceClaims {
    pub cpu_units: u64,
    pub ram_mb: u64,
    pub cargo_slots: u64,
    pub gpu_slots: u64,
    #[serde(default)]
    pub custom: std::collections::BTreeMap<String, u64>,
}

/// Durable, finite promise of scalar capacity to one Pending Job.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
pub struct ScalarReservation {
    pub reservation_id: ReservationId,
    pub claims: ScalarResourceClaims,
    pub created_unix_millis: i64,
    pub hold_deadline_unix_millis: i64,
}

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

/// Provider-reported Invocation transition carried by an `InvocationChanged` event.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum InvocationTransition {
    Started,
    Exited,
    /// A newer daemon committed a transition this client does not yet name.
    #[serde(other)]
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
pub struct SchedulerEvent {
    pub cursor: EventCursor,
    pub kind: SchedulerEventKind,
    pub job_id: JobId,
    pub batch_id: Option<BatchId>,
    /// Present for every `InvocationChanged` event and absent for other event kinds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt_id: Option<AttemptId>,
    /// Present for every `InvocationChanged` event and absent for other event kinds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invocation_id: Option<InvocationId>,
    /// Present for every `InvocationChanged` event and absent for other event kinds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transition: Option<InvocationTransition>,
    pub committed_unix_millis: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(deny_unknown_fields)]
pub struct JobSummary {
    pub job_id: JobId,
    /// Bounded, single-line executable and argument preview for operator-facing lists.
    #[serde(default)]
    pub command_preview: String,
    pub batch_id: Option<BatchId>,
    pub batch_member: Option<String>,
    pub parent: Option<ManagedParent>,
    pub state: JobState,
    pub outcome: Option<JobOutcome>,
    pub accepted_unix_millis: i64,
    pub priority: i8,
    pub effective_priority: Option<i64>,
    pub started_unix_millis: Option<i64>,
    pub finished_unix_millis: Option<i64>,
    pub queue_rank: Option<u64>,
    pub reservation: Option<ScalarReservation>,
    pub estimate: Estimate,
    pub claims: ResourceClaims,
    /// Operator-declared labels copied from the accepted spec, e.g. `project=…` and `gate=…`,
    /// so list views can group and colour rows without fetching every snapshot.
    #[serde(default)]
    pub labels: Vec<Label>,
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

impl JobListPage {
    #[must_use]
    pub fn from_jobs(jobs: Vec<JobSummary>, event_cursor: EventCursor) -> Self {
        Self {
            jobs,
            next_cursor: None,
            event_cursor,
        }
    }
}

#[derive(
    Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize, JsonSchema,
)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum TreeAttentionBucket {
    Running,
    Queued,
    Finished,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct JobTreeRootCursor {
    pub store_uuid: Uuid,
    pub order_revision: u64,
    pub selector_hash: String,
    pub bucket: TreeAttentionBucket,
    pub accepted_unix_millis: i64,
    pub root_job_id: JobId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct JobChildrenCursor {
    pub store_uuid: Uuid,
    pub selector_hash: String,
    pub parent_job_id: JobId,
    pub accepted_unix_millis: i64,
    pub child_job_id: JobId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(deny_unknown_fields)]
pub struct JobTreeNode {
    pub summary: JobSummary,
    pub depth: u32,
    /// Aggregate attention bucket on a family root; absent on descendants.
    pub family_attention: Option<TreeAttentionBucket>,
    pub context_only: bool,
    pub parent_retained: Option<bool>,
    pub has_children: bool,
    pub descendants_truncated: bool,
    pub next_children_cursor: Option<JobChildrenCursor>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(deny_unknown_fields)]
pub struct JobTreePage {
    pub nodes: Vec<JobTreeNode>,
    pub next_root_cursor: Option<JobTreeRootCursor>,
    pub selected_job_id: Option<JobId>,
    pub event_cursor: EventCursor,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(deny_unknown_fields)]
pub struct JobChildrenPage {
    pub parent_job_id: JobId,
    pub nodes: Vec<JobTreeNode>,
    pub next_children_cursor: Option<JobChildrenCursor>,
    pub event_cursor: EventCursor,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct JobTreeSelector {
    pub root_job_ids: Vec<JobId>,
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(tag = "frame", rename_all = "snake_case", deny_unknown_fields)]
// Keep the frozen public Gap shape (`snapshot: JobTreePage`) rather than exposing Box ownership
// solely to reduce this short-lived response enum's stack size.
#[allow(clippy::large_enum_variant)]
pub enum TreeObservationFrame {
    Events {
        events: Vec<SchedulerEvent>,
        cursor: EventCursor,
    },
    Gap {
        gap: EventGap,
        snapshot: JobTreePage,
        cursor: EventCursor,
    },
}

impl TreeObservationFrame {
    #[must_use]
    pub fn cursor(&self) -> EventCursor {
        match self {
            Self::Events { cursor, .. } | Self::Gap { cursor, .. } => *cursor,
        }
    }
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

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct EffectiveChildSubmissionPolicy {
    pub min_priority: i8,
    pub max_priority: i8,
    pub max_claims: ResourceClaimLimits,
    pub allowed_impacts: Vec<String>,
    pub required_labels: Vec<Label>,
    pub fences: ChildFencePolicy,
    pub allow_observed: bool,
    pub allow_quiet: bool,
    pub allow_delegation: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ManagedPolicyAdmissionSnapshot {
    pub parent: ManagedParent,
    pub evaluated_unix_millis: i64,
    pub effective_policy: EffectiveChildSubmissionPolicy,
    pub policy_ancestors: Vec<JobId>,
}

/// Store and optional managed-parent identity observed for this client connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SubmissionContext {
    pub store_uuid: Uuid,
    pub parent: Option<ManagedParent>,
}

/// Durable identity of a managed submission, with any accepted execution identities known so far.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(deny_unknown_fields)]
pub struct SubmissionRef {
    pub submission_id: SubmissionId,
    #[serde(default)]
    pub job_ids: Vec<JobId>,
    pub batch_id: Option<BatchId>,
}

impl SubmissionRef {
    /// Creates a Submission reference before any accepted Job or Batch identity is known.
    #[must_use]
    pub fn new(submission_id: SubmissionId) -> Self {
        Self {
            submission_id,
            job_ids: Vec::new(),
            batch_id: None,
        }
    }

    /// Adds the accepted Job identities represented by this Submission.
    #[must_use]
    pub fn with_job_ids(mut self, job_ids: impl IntoIterator<Item = JobId>) -> Self {
        self.job_ids = job_ids.into_iter().collect();
        self
    }

    /// Adds the accepted Batch identity represented by this Submission.
    #[must_use]
    pub fn with_batch_id(mut self, batch_id: BatchId) -> Self {
        self.batch_id = Some(batch_id);
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(deny_unknown_fields)]
pub struct RejectReason {
    pub code: String,
    pub detail: String,
}

impl RejectReason {
    #[must_use]
    pub fn new(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            detail: detail.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(deny_unknown_fields)]
pub struct EnsuredJob {
    pub receipt: JobReceipt,
    pub snapshot: Option<Box<JobSnapshot>>,
}

impl EnsuredJob {
    /// Creates an ensured Job value without a terminal snapshot.
    #[must_use]
    pub fn new(receipt: JobReceipt) -> Self {
        Self {
            receipt,
            snapshot: None,
        }
    }

    /// Adds the terminal snapshot when the ensure operation observed one.
    #[must_use]
    pub fn with_snapshot(mut self, snapshot: JobSnapshot) -> Self {
        self.snapshot = Some(Box::new(snapshot));
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(deny_unknown_fields)]
pub struct EnsuredBatch {
    pub receipt: BatchReceipt,
    #[serde(default)]
    pub snapshots: Vec<JobSnapshot>,
}

impl EnsuredBatch {
    /// Creates an ensured Batch value without terminal member snapshots.
    #[must_use]
    pub fn new(receipt: BatchReceipt) -> Self {
        Self {
            receipt,
            snapshots: Vec::new(),
        }
    }

    /// Adds terminal member snapshots in Batch receipt order.
    #[must_use]
    pub fn with_snapshots(mut self, snapshots: impl IntoIterator<Item = JobSnapshot>) -> Self {
        self.snapshots = snapshots.into_iter().collect();
        self
    }
}

/// One fail-closed decision from [`crate::Client::ensure_job`] or
/// [`crate::Client::ensure_batch`].
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(tag = "outcome", content = "value", rename_all = "snake_case")]
pub enum EnsureOutcome<T> {
    Accepted(T),
    Pending(SubmissionRef),
    Final(T),
    Rejected(RejectReason),
    Conflict {
        existing_payload_hash: String,
        requested_payload_hash: String,
    },
    Unknown,
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

/// Options for one atomic ensure-or-recover operation.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct EnsureOptions {
    pub idempotency_key: Uuid,
    pub result_file: Option<PathBuf>,
    pub wait_for_completion: bool,
}

impl EnsureOptions {
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

    #[must_use]
    pub fn with_wait_for_completion(mut self) -> Self {
        self.wait_for_completion = true;
        self
    }
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
pub struct GpuProvenance {
    pub uuid: String,
    pub driver_version: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum AdmissionDecisionState {
    Waiting,
    Reserved,
    Released,
    Replanned,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(deny_unknown_fields)]
pub struct ObservedOperandSnapshot {
    pub name: String,
    pub requested: u64,
    pub configured_capacity: Option<u64>,
    pub observed: Option<u64>,
    pub safety_margin: u64,
    pub granted_debit: Option<u64>,
    pub satisfied: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(deny_unknown_fields)]
pub struct DetectorEvidenceSnapshot {
    pub detector: String,
    pub observed: Option<u64>,
    pub threshold: Option<u64>,
    pub satisfied: bool,
    pub detail: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(deny_unknown_fields)]
pub struct AdmissionDecisionSnapshot {
    pub state: AdmissionDecisionState,
    pub evaluated_unix_millis: Option<i64>,
    pub observation_generation: Option<Uuid>,
    #[serde(default)]
    pub blockers: Vec<Blocker>,
    #[serde(default)]
    pub operands: Vec<ObservedOperandSnapshot>,
    #[serde(default)]
    pub detectors: Vec<DetectorEvidenceSnapshot>,
    pub gpu_provenance: Option<GpuProvenance>,
    pub final_sample: bool,
    pub deferral_count: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(deny_unknown_fields)]
pub struct JobReceipt {
    pub submission_id: SubmissionId,
    pub job_id: JobId,
    pub submission_state: SubmissionState,
    pub job_state: JobState,
    pub accepted_unix_millis: i64,
    pub priority: i8,
    pub effective_priority: Option<i64>,
    #[serde(default)]
    pub blockers: Vec<Blocker>,
    pub queue_rank: Option<u64>,
    pub reservation: Option<ScalarReservation>,
    pub estimate: Estimate,
    pub parent: Option<ManagedParent>,
    #[serde(default)]
    pub managed_policy_admission: Option<ManagedPolicyAdmissionSnapshot>,
    #[serde(default)]
    pub gpu_provenance: Option<GpuProvenance>,
    #[serde(default)]
    pub admission: Option<AdmissionDecisionSnapshot>,
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

#[derive(Clone, Debug, Eq, PartialEq, JsonSchema)]
#[schemars(with = "String")]
#[non_exhaustive]
pub enum ContainmentState {
    Creating,
    Live,
    Empty,
    Uncertain,
    Cleared,
    Unknown(String),
}

macro_rules! string_enum {
    ($name:ident { $($variant:ident => $value:literal),+ $(,)? }) => {
        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serializer.serialize_str(match self {
                    $(Self::$variant => $value,)+
                    Self::Unknown(value) => value,
                })
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Ok(match value.as_str() {
                    $($value => Self::$variant,)+
                    _ => Self::Unknown(value),
                })
            }
        }
    };
}

string_enum!(ContainmentState {
    Creating => "creating",
    Live => "live",
    Empty => "empty",
    Uncertain => "uncertain",
    Cleared => "cleared",
});

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct BootId(pub String);

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct HostId(pub String);

#[derive(Clone, Debug, Eq, PartialEq, JsonSchema)]
#[schemars(with = "std::collections::BTreeMap<String, serde_json::Value>")]
#[non_exhaustive]
pub enum ProcessIdentity {
    Windows {
        host_id: HostId,
        boot_id: BootId,
        pid: u32,
        creation_filetime_100ns: u64,
    },
    Unknown {
        unknown_platform: String,
        evidence: serde_json::Value,
    },
}

impl Serialize for ProcessIdentity {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;

        match self {
            Self::Windows {
                host_id,
                boot_id,
                pid,
                creation_filetime_100ns,
            } => {
                let mut map = serializer.serialize_map(Some(5))?;
                map.serialize_entry("platform", "windows")?;
                map.serialize_entry("host_id", host_id)?;
                map.serialize_entry("boot_id", boot_id)?;
                map.serialize_entry("pid", pid)?;
                map.serialize_entry("creation_filetime_100ns", creation_filetime_100ns)?;
                map.end()
            }
            Self::Unknown {
                unknown_platform,
                evidence,
            } => {
                let object = evidence.as_object().ok_or_else(|| {
                    serde::ser::Error::custom("unknown process identity evidence must be an object")
                })?;
                let mut map = serializer.serialize_map(Some(object.len() + 1))?;
                map.serialize_entry("platform", unknown_platform)?;
                for (key, value) in object {
                    if key != "platform" {
                        map.serialize_entry(key, value)?;
                    }
                }
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for ProcessIdentity {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct WindowsEvidence {
            host_id: HostId,
            boot_id: BootId,
            pid: u32,
            creation_filetime_100ns: u64,
        }

        struct IdentityVisitor;

        impl<'de> serde::de::Visitor<'de> for IdentityVisitor {
            type Value = (String, serde_json::Map<String, serde_json::Value>);

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a process identity object with exactly one platform tag")
            }

            fn visit_map<A>(self, mut access: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut platform = None;
                let mut evidence = serde_json::Map::new();
                while let Some(key) = access.next_key::<String>()? {
                    if key == "platform" {
                        if platform.is_some() {
                            return Err(serde::de::Error::duplicate_field("platform"));
                        }
                        platform = Some(access.next_value::<String>()?);
                    } else {
                        if evidence.contains_key(&key) {
                            return Err(serde::de::Error::custom(format!(
                                "duplicate process identity field {key}"
                            )));
                        }
                        evidence.insert(key, access.next_value()?);
                    }
                }
                Ok((
                    platform.ok_or_else(|| serde::de::Error::missing_field("platform"))?,
                    evidence,
                ))
            }
        }

        let (platform, object) = deserializer.deserialize_map(IdentityVisitor)?;
        if platform == "windows" {
            let evidence: WindowsEvidence =
                serde_json::from_value(serde_json::Value::Object(object))
                    .map_err(serde::de::Error::custom)?;
            Ok(Self::Windows {
                host_id: evidence.host_id,
                boot_id: evidence.boot_id,
                pid: evidence.pid,
                creation_filetime_100ns: evidence.creation_filetime_100ns,
            })
        } else {
            Ok(Self::Unknown {
                unknown_platform: platform,
                evidence: serde_json::Value::Object(object),
            })
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, JsonSchema)]
#[schemars(with = "String")]
#[non_exhaustive]
pub enum DoctorCheckStatus {
    Pass,
    Warning,
    Fail,
    Unknown(String),
}
string_enum!(DoctorCheckStatus {
    Pass => "pass",
    Warning => "warning",
    Fail => "fail",
});

#[derive(Clone, Debug, Eq, PartialEq, JsonSchema)]
#[schemars(with = "String")]
#[non_exhaustive]
pub enum DoctorOverallStatus {
    Healthy,
    AttentionRequired,
    Unsafe,
    Unknown(String),
}
string_enum!(DoctorOverallStatus {
    Healthy => "healthy",
    AttentionRequired => "attention_required",
    Unsafe => "unsafe",
});

#[derive(Clone, Debug, Eq, PartialEq, JsonSchema)]
#[schemars(with = "String")]
#[non_exhaustive]
pub enum ContainmentResolution {
    ProvenEmpty,
    Reboot,
    ForcedRiskAcceptance,
    Unknown(String),
}
string_enum!(ContainmentResolution {
    ProvenEmpty => "proven_empty",
    Reboot => "reboot",
    ForcedRiskAcceptance => "forced_risk_acceptance",
});

#[derive(Clone, Debug, Eq, PartialEq, JsonSchema)]
#[schemars(with = "String")]
#[non_exhaustive]
pub enum ReconciliationResult {
    StillResolves,
    BoundaryNotEmpty,
    BoundaryUninspectable,
    IdentityUnavailable,
    IdentityAbsent,
    PidReused,
    ProvenEmpty,
    PriorBoot,
    Unknown(String),
}
string_enum!(ReconciliationResult {
    StillResolves => "still_resolves",
    BoundaryNotEmpty => "boundary_not_empty",
    BoundaryUninspectable => "boundary_uninspectable",
    IdentityUnavailable => "identity_unavailable",
    IdentityAbsent => "identity_absent",
    PidReused => "pid_reused",
    ProvenEmpty => "proven_empty",
    PriorBoot => "prior_boot",
});

#[derive(Clone, Debug, Eq, PartialEq, JsonSchema)]
#[schemars(with = "String")]
#[non_exhaustive]
pub enum ClearanceOrigin {
    Automatic,
    Forced,
    Unknown(String),
}
string_enum!(ClearanceOrigin {
    Automatic => "automatic",
    Forced => "forced",
});

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
pub struct ForcedClearanceAudit {
    pub requested_unix_millis: i64,
    pub requester: ProcessIdentity,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
pub struct ContainmentResolutionAudit {
    pub resolved_unix_millis: i64,
    pub daemon_generation: Uuid,
    pub resolution: ContainmentResolution,
    pub last_reconciliation: ReconciliationResult,
    pub origin: ClearanceOrigin,
    pub forced: Option<ForcedClearanceAudit>,
    pub lease_released: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
pub struct ContainmentIncidentSnapshot {
    pub incident_id: ContainmentId,
    pub incident_sequence: u64,
    pub containment_id: ContainmentId,
    pub job_id: JobId,
    pub attempt_id: AttemptId,
    pub invocation_id: InvocationId,
    pub state: ContainmentState,
    pub reason_code: String,
    pub detail: String,
    pub opened_unix_millis: i64,
    pub last_reconciled_unix_millis: Option<i64>,
    pub last_reconciliation: Option<ReconciliationResult>,
    pub root_identity: Option<ProcessIdentity>,
    pub retained_claims: ResourceClaims,
    pub resolution: Option<ContainmentResolution>,
    pub resolved_unix_millis: Option<i64>,
}

/// Maximum number of incidents retained in one snapshot-consistent doctor inventory.
pub const MAX_COMPLETE_DOCTOR_INCIDENTS: u64 = 16_384;

/// Maximum serialized incident payload retained for one complete doctor inventory.
pub const MAX_COMPLETE_DOCTOR_BYTES: u64 = 64 * 1024 * 1024;

/// Maximum number of incidents returned by one doctor page.
pub const MAX_DOCTOR_PAGE: u32 = 256;

/// Lifetime of a generation-local doctor continuation snapshot.
pub const DOCTOR_SNAPSHOT_TTL_SECONDS: u64 = 5 * 60;

/// Opaque, store- and snapshot-scoped continuation for [`crate::Client::doctor`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(try_from = "String", into = "String")]
#[schemars(with = "String")]
pub struct ContainmentIncidentCursor {
    pub(crate) store_uuid: Uuid,
    pub(crate) daemon_generation: Uuid,
    pub(crate) snapshot_uuid: Uuid,
    pub(crate) token_uuid: Uuid,
    pub(crate) offset: u64,
}

impl From<ContainmentIncidentCursor> for String {
    fn from(cursor: ContainmentIncidentCursor) -> Self {
        cursor.to_string()
    }
}

impl TryFrom<String> for ContainmentIncidentCursor {
    type Error = ObservationCursorParseError;

    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        value.parse()
    }
}

impl fmt::Display for ContainmentIncidentCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "v2:{}:{}:{}:{}:{}",
            self.store_uuid,
            self.daemon_generation,
            self.snapshot_uuid,
            self.token_uuid,
            self.offset
        )
    }
}

impl FromStr for ContainmentIncidentCursor {
    type Err = ObservationCursorParseError;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        let mut parts = value.split(':');
        if parts.next() != Some("v2") {
            return Err(ObservationCursorParseError);
        }
        let store_uuid = Uuid::parse_str(parts.next().ok_or(ObservationCursorParseError)?)
            .map_err(|_| ObservationCursorParseError)?;
        let daemon_generation = Uuid::parse_str(parts.next().ok_or(ObservationCursorParseError)?)
            .map_err(|_| ObservationCursorParseError)?;
        let snapshot_uuid = Uuid::parse_str(parts.next().ok_or(ObservationCursorParseError)?)
            .map_err(|_| ObservationCursorParseError)?;
        let token_uuid = Uuid::parse_str(parts.next().ok_or(ObservationCursorParseError)?)
            .map_err(|_| ObservationCursorParseError)?;
        let offset = parts
            .next()
            .ok_or(ObservationCursorParseError)?
            .parse()
            .map_err(|_| ObservationCursorParseError)?;
        if parts.next().is_some() {
            return Err(ObservationCursorParseError);
        }
        Ok(Self {
            store_uuid,
            daemon_generation,
            snapshot_uuid,
            token_uuid,
            offset,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
pub struct DoctorIncidentPage {
    pub total_unresolved: u64,
    pub incidents: Vec<ContainmentIncidentSnapshot>,
    pub truncated: bool,
    pub next_cursor: Option<ContainmentIncidentCursor>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
pub struct DoctorCheck {
    pub code: String,
    pub status: DoctorCheckStatus,
    pub summary: String,
    pub remediation: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
pub struct DoctorCoverage {
    pub provider: String,
    pub detector: String,
    pub status: DoctorCheckStatus,
    pub observed_unix_millis: Option<i64>,
    pub observation_generation: Option<Uuid>,
    pub remediation: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
pub struct DoctorBoundary {
    pub code: String,
    pub statement: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
pub struct DoctorHostSnapshot {
    pub platform: String,
    pub host_name: Option<String>,
    pub host_id: Option<HostId>,
    pub boot_id: Option<BootId>,
    pub containment_strength: String,
    pub session_survival: DoctorCheckStatus,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
pub struct DoctorStoreSnapshot {
    pub store_uuid: Uuid,
    pub schema_epoch: String,
    pub bound_host_id: Option<HostId>,
    pub filesystem: String,
    pub sqlite_journal_mode: String,
    pub sqlite_synchronous: String,
    pub foreign_keys_enabled: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
pub struct DoctorSnapshot {
    pub schema_version: u32,
    pub observed_unix_millis: i64,
    pub overall: DoctorOverallStatus,
    pub daemon: DaemonSnapshot,
    pub host: DoctorHostSnapshot,
    pub store: DoctorStoreSnapshot,
    pub checks: Vec<DoctorCheck>,
    #[serde(default)]
    pub coverage: Vec<DoctorCoverage>,
    pub incidents: DoctorIncidentPage,
    pub boundaries: Vec<DoctorBoundary>,
}

/// A complete, bounded doctor inventory collected from one logical incident snapshot.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
pub struct CompleteDoctorSnapshot {
    pub schema_version: u32,
    pub observed_unix_millis: i64,
    pub overall: DoctorOverallStatus,
    pub daemon: DaemonSnapshot,
    pub host: DoctorHostSnapshot,
    pub store: DoctorStoreSnapshot,
    pub checks: Vec<DoctorCheck>,
    #[serde(default)]
    pub coverage: Vec<DoctorCoverage>,
    pub total_unresolved: u64,
    pub incidents: Vec<ContainmentIncidentSnapshot>,
    pub boundaries: Vec<DoctorBoundary>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
pub struct ClearContainmentResult {
    pub schema_version: u32,
    pub containment_id: ContainmentId,
    pub prior_state: ContainmentState,
    pub state: ContainmentState,
    pub audit: ContainmentResolutionAudit,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
pub struct ContainmentSnapshot {
    pub containment_id: ContainmentId,
    pub state: ContainmentState,
    /// `windows_job_object` in v0.1; kept explicit for Linux v0.2 provenance.
    pub strength: String,
    pub incident_id: Option<ContainmentId>,
    #[serde(default)]
    pub incident: Option<ContainmentIncidentSnapshot>,
    #[serde(default)]
    pub resolution: Option<ContainmentResolution>,
    #[serde(default)]
    pub resolution_audit: Option<ContainmentResolutionAudit>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum ExitClassification {
    Accepted,
    Retryable,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum InvocationVerdict {
    Succeeded,
    ProcessFailed,
    StartFailed,
    TimedOut,
    Interrupted,
    SafetyFailed,
    Canceled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum TerminationReason {
    Exited,
    StartFailed,
    Timeout,
    Interrupt,
    Cancel,
    SafetyFailure,
}

/// Immutable primary outcome recorded only after its Containment is proved empty.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(deny_unknown_fields)]
pub struct PrimaryInvocationResult {
    pub schema_version: u32,
    pub job_id: JobId,
    pub attempt_id: AttemptId,
    pub invocation_id: InvocationId,
    pub verdict: InvocationVerdict,
    pub root_exit_code: Option<i32>,
    pub termination: TerminationReason,
    pub containment: ContainmentState,
    pub started_unix_millis: Option<i64>,
    pub exited_unix_millis: Option<i64>,
    pub resolved_unix_millis: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum PendingReason {
    ClientDeadline,
    ClientCanceled,
    SubmissionReceived,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum WaitOutcome {
    Pending {
        reason: PendingReason,
    },
    Final {
        snapshot: Box<JobSnapshot>,
        root_exit_code: Option<i32>,
    },
    Unavailable {
        detail: String,
    },
    GapOrUnknown {
        detail: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum ExitSource {
    Process,
    Scheduler,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(deny_unknown_fields)]
pub struct WaitReport {
    pub outcome: WaitOutcome,
    pub exit_source: ExitSource,
    pub exit_code: i32,
}

impl WaitReport {
    #[must_use]
    pub fn new(outcome: WaitOutcome, exit_source: ExitSource, exit_code: i32) -> Self {
        Self {
            outcome,
            exit_source,
            exit_code,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(deny_unknown_fields)]
pub struct EnsureReport<T> {
    pub outcome: EnsureOutcome<T>,
    pub exit_source: ExitSource,
    pub exit_code: i32,
}

impl<T> EnsureReport<T> {
    #[must_use]
    pub fn new(outcome: EnsureOutcome<T>, exit_source: ExitSource, exit_code: i32) -> Self {
        Self {
            outcome,
            exit_source,
            exit_code,
        }
    }
}

/// Root type for the recorded managed-execution JSON Schema.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
#[serde(untagged)]
pub enum ManagedExecutionRecord {
    EnsureJob(Box<EnsureReport<EnsuredJob>>),
    EnsureBatch(EnsureReport<EnsuredBatch>),
    Wait(WaitReport),
    PrimaryInvocationResult(PrimaryInvocationResult),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
pub struct InvocationSnapshot {
    pub invocation_id: InvocationId,
    pub role: InvocationRole,
    pub role_index: u32,
    pub state: InvocationState,
    pub root_pid: Option<u32>,
    #[serde(default)]
    pub root_identity: Option<ProcessIdentity>,
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
pub struct AttemptSnapshot {
    pub attempt_id: AttemptId,
    pub attempt_index: u32,
    pub verdict: Option<AttemptVerdict>,
    #[serde(default, alias = "safety_reason")]
    pub reason_code: Option<String>,
    pub created_unix_millis: i64,
    pub started_unix_millis: Option<i64>,
    pub deadline_unix_millis: Option<i64>,
    pub finished_unix_millis: Option<i64>,
    #[serde(default)]
    pub primary_result: Option<PrimaryInvocationResult>,
    #[serde(default)]
    pub admission: Option<AdmissionDecisionSnapshot>,
    pub invocations: Vec<InvocationSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
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
    pub priority: i8,
    pub effective_priority: Option<i64>,
    pub queue_rank: Option<u64>,
    pub reservation: Option<ScalarReservation>,
    pub started_unix_millis: Option<i64>,
    pub finished_unix_millis: Option<i64>,
    pub spec: JobSpec,
    #[serde(default)]
    pub parent: Option<ManagedParent>,
    #[serde(default)]
    pub managed_policy_admission: Option<ManagedPolicyAdmissionSnapshot>,
    #[serde(default)]
    pub blockers: Vec<Blocker>,
    #[serde(default)]
    pub attempts: Vec<AttemptSnapshot>,
    #[serde(default)]
    pub gpu_provenance: Option<GpuProvenance>,
    #[serde(default)]
    pub admission: Option<AdmissionDecisionSnapshot>,
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
// This public result mirrors the wire response. Boxing only the receipt would make the Rust API
// less direct for a modest stack saving while leaving its serialized representation unchanged.
#[allow(clippy::large_enum_variant)]
pub enum RecoveryResult {
    Received {
        submission_id: SubmissionId,
    },
    Accepted(JobReceipt),
    AcceptedBatch(BatchReceipt),
    Rejected {
        code: String,
        detail: String,
    },
    Conflict {
        existing_payload_hash: String,
        requested_payload_hash: String,
    },
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

/// Authoritative accounting for one configured scalar resource.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
pub struct ScalarResourceSnapshot {
    pub capacity: u64,
    pub granted: u64,
    pub reserved: u64,
}

/// Authoritative scalar-resource accounting captured by the daemon.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
pub struct ResourceSnapshot {
    pub cpu_units: ScalarResourceSnapshot,
    pub ram_mb: ScalarResourceSnapshot,
    pub cargo_slots: ScalarResourceSnapshot,
    pub gpu_slots: ScalarResourceSnapshot,
    #[serde(default)]
    pub custom: std::collections::BTreeMap<String, ScalarResourceSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
pub struct DaemonSnapshot {
    pub store_uuid: Uuid,
    pub daemon_generation: Uuid,
    pub version: String,
    pub pid: u32,
    #[serde(default)]
    pub process_identity: Option<ProcessIdentity>,
    pub endpoint: String,
    pub store_path: PathBuf,
    pub config_path: PathBuf,
    pub capacities: crate::ResourceCapacities,
    /// `None` only when a protocol-compatible older daemon omitted resource accounting.
    #[serde(default)]
    pub resources: Option<ResourceSnapshot>,
    pub config_sha256: String,
    pub queued_jobs: u64,
    pub running_jobs: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn managed_execution_value_constructors_use_safe_defaults() {
        let store_uuid = Uuid::now_v7();
        let submission_id = SubmissionId::from_parts(store_uuid, Uuid::now_v7());
        let job_id = JobId::from_parts(store_uuid, Uuid::now_v7());
        let batch_id = BatchId::from_parts(store_uuid, Uuid::now_v7());
        let submission = SubmissionRef::new(submission_id);
        assert!(submission.job_ids.is_empty());
        assert_eq!(submission.batch_id, None);

        let reason = RejectReason::new("fixture", "test double");
        assert_eq!(reason.code, "fixture");
        assert_eq!(reason.detail, "test double");

        let job_receipt = JobReceipt {
            submission_id,
            job_id,
            submission_state: SubmissionState::Accepted,
            job_state: JobState::Pending,
            accepted_unix_millis: 0,
            priority: crate::NEUTRAL_JOB_PRIORITY,
            effective_priority: Some(0),
            blockers: Vec::new(),
            queue_rank: None,
            reservation: None,
            estimate: Estimate::unknown("fixture"),
            parent: None,
            managed_policy_admission: None,
            gpu_provenance: None,
            admission: None,
            daemon_generation: Uuid::now_v7(),
        };
        assert!(EnsuredJob::new(job_receipt).snapshot.is_none());

        let batch_receipt = BatchReceipt {
            submission_id,
            batch_id,
            submission_state: SubmissionState::Accepted,
            jobs: Vec::new(),
            daemon_generation: Uuid::now_v7(),
        };
        assert!(EnsuredBatch::new(batch_receipt).snapshots.is_empty());
    }

    #[test]
    fn cursors_have_stable_cli_round_trips() {
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

    #[test]
    fn scheduler_events_accept_additive_wire_evolution() {
        let store_uuid = Uuid::now_v7();
        let job_id = JobId::from_parts(store_uuid, Uuid::now_v7());
        let old_payload = serde_json::json!({
            "cursor": { "store_uuid": store_uuid, "sequence": 7 },
            "kind": "job_changed",
            "job_id": job_id,
            "batch_id": null,
            "committed_unix_millis": 42
        });
        let old_event: SchedulerEvent = serde_json::from_value(old_payload).unwrap();
        assert_eq!(old_event.attempt_id, None);
        assert_eq!(old_event.invocation_id, None);
        assert_eq!(old_event.transition, None);

        let mut future_payload = serde_json::to_value(&old_event).unwrap();
        future_payload
            .as_object_mut()
            .unwrap()
            .insert("future_field".into(), serde_json::json!({ "version": 18 }));
        assert_eq!(
            serde_json::from_value::<SchedulerEvent>(future_payload).unwrap(),
            old_event
        );
        assert_eq!(
            serde_json::from_str::<InvocationTransition>("\"future_transition\"").unwrap(),
            InvocationTransition::Unknown
        );
    }

    #[test]
    fn unknown_safety_values_round_trip_without_authority() {
        let state: ContainmentState = serde_json::from_str("\"future_state\"").unwrap();
        assert_eq!(state, ContainmentState::Unknown("future_state".into()));
        assert_eq!(serde_json::to_string(&state).unwrap(), "\"future_state\"");

        let identity: ProcessIdentity = serde_json::from_value(serde_json::json!({
            "platform": "linux_pidfd",
            "pid": 17,
            "start_ticks": 99
        }))
        .unwrap();
        assert_eq!(
            identity,
            ProcessIdentity::Unknown {
                unknown_platform: "linux_pidfd".into(),
                evidence: serde_json::json!({"pid": 17, "start_ticks": 99}),
            }
        );
        let encoded = serde_json::to_value(identity).unwrap();
        assert_eq!(encoded["platform"], "linux_pidfd");
        assert_eq!(
            encoded
                .as_object()
                .unwrap()
                .keys()
                .filter(|key| *key == "platform")
                .count(),
            1
        );
        assert!(
            serde_json::from_str::<ProcessIdentity>(
                r#"{"platform":"future","platform":"windows","pid":1}"#
            )
            .is_err()
        );
    }

    #[test]
    fn incident_cursor_is_store_scoped_and_stable() {
        let store_uuid = Uuid::now_v7();
        let cursor = ContainmentIncidentCursor {
            store_uuid,
            daemon_generation: Uuid::now_v7(),
            snapshot_uuid: Uuid::now_v7(),
            token_uuid: Uuid::now_v7(),
            offset: 37,
        };
        assert_eq!(
            cursor
                .to_string()
                .parse::<ContainmentIncidentCursor>()
                .unwrap(),
            cursor
        );
        assert_eq!(
            serde_json::to_value(cursor).unwrap(),
            serde_json::Value::String(cursor.to_string())
        );
        assert_eq!(
            serde_json::from_value::<ContainmentIncidentCursor>(serde_json::Value::String(
                cursor.to_string()
            ))
            .unwrap(),
            cursor
        );
        assert_eq!(
            serde_json::to_value(schemars::schema_for!(ContainmentIncidentCursor)).unwrap()["type"],
            "string"
        );
        assert!("bad:cursor".parse::<ContainmentIncidentCursor>().is_err());
    }
}
