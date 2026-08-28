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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(try_from = "String", into = "String")]
#[schemars(with = "String")]
pub struct ContainmentIncidentCursor {
    pub(crate) store_uuid: Uuid,
    pub(crate) incident_sequence: u64,
    pub(crate) containment_id: ContainmentId,
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
            "{}:{}:{}",
            self.store_uuid,
            self.incident_sequence,
            self.containment_id.entity_uuid()
        )
    }
}

impl FromStr for ContainmentIncidentCursor {
    type Err = ObservationCursorParseError;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        let mut parts = value.split(':');
        let store_uuid = Uuid::parse_str(parts.next().ok_or(ObservationCursorParseError)?)
            .map_err(|_| ObservationCursorParseError)?;
        let incident_sequence = parts
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
            incident_sequence,
            containment_id: ContainmentId::from_parts(store_uuid, entity),
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
    pub incidents: DoctorIncidentPage,
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
    pub started_unix_millis: i64,
    pub deadline_unix_millis: Option<i64>,
    pub finished_unix_millis: Option<i64>,
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

    #[test]
    fn alpha8_unknown_safety_values_round_trip_without_authority() {
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
    fn alpha8_incident_cursor_is_store_scoped_and_stable() {
        let store_uuid = Uuid::now_v7();
        let cursor = ContainmentIncidentCursor {
            store_uuid,
            incident_sequence: 37,
            containment_id: ContainmentId::from_parts(store_uuid, Uuid::now_v7()),
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
