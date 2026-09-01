//! Public, runtime-neutral client contract for Stillyard.

mod api;
mod client;
#[cfg_attr(not(windows), allow(dead_code))]
pub(crate) mod daemon;
mod error;
mod filesystem;
#[cfg_attr(not(windows), allow(dead_code))]
pub(crate) mod host_observation;
mod identity;
mod instance;
mod model;
mod payload;
#[cfg_attr(not(windows), allow(dead_code))]
pub(crate) mod protocol;
mod resources;
#[cfg_attr(not(windows), allow(dead_code))]
pub(crate) mod runner;
mod spec;
#[cfg_attr(not(windows), allow(dead_code))]
pub(crate) mod store;

pub use api::{
    AdmissionDecisionSnapshot, AdmissionDecisionState, AttemptSnapshot, BatchJobReceipt,
    BatchReceipt, Blocker, BootId, CancellationToken, ClearContainmentResult, ClearanceOrigin,
    CompleteDoctorSnapshot, ContainmentIncidentCursor, ContainmentIncidentSnapshot,
    ContainmentResolution, ContainmentResolutionAudit, ContainmentSnapshot, ContainmentState,
    DOCTOR_SNAPSHOT_TTL_SECONDS, DaemonSnapshot, DetectorEvidenceSnapshot, DoctorBoundary,
    DoctorCheck, DoctorCheckStatus, DoctorCoverage, DoctorHostSnapshot, DoctorIncidentPage,
    DoctorOverallStatus, DoctorSnapshot, DoctorStoreSnapshot, EffectiveChildSubmissionPolicy,
    EnsureOptions, EnsureOutcome, EnsureReport, EnsuredBatch, EnsuredJob, Estimate,
    EstimateConfidence, EventCursor, EventGap, ExitClassification, ExitSource,
    ForcedClearanceAudit, GpuProvenance, HostId, InvocationRole, InvocationSnapshot,
    InvocationState, InvocationVerdict, JobChildrenCursor, JobChildrenPage, JobListCursor,
    JobListPage, JobReceipt, JobSelector, JobSnapshot, JobSummary, JobTreeNode, JobTreePage,
    JobTreeRootCursor, JobTreeSelector, LogChunk, LogStream, MAX_COMPLETE_DOCTOR_BYTES,
    MAX_COMPLETE_DOCTOR_INCIDENTS, MAX_DOCTOR_PAGE, MAX_OBSERVATION_PAGE, MAX_TREE_PAGE_NODES,
    MAX_TREE_SELECTOR_JOBS, MAX_WAIT_STREAM_JOBS, ManagedExecutionRecord, ManagedParent,
    ManagedPolicyAdmissionSnapshot, ObservationCursorParseError, ObservationFrame,
    ObservedOperandSnapshot, PendingReason, PrimaryInvocationResult, ProcessIdentity,
    ReconciliationResult, RecoveryResult, RejectReason, ResourceSnapshot, ScalarResourceSnapshot,
    SchedulerEvent, SchedulerEventKind, SubmissionContext, SubmissionRef, SubmitOptions,
    TerminationReason, TreeAttentionBucket, TreeObservationFrame, WaitOutcome, WaitReport,
    WaitStreamItem,
};
pub use client::{Client, ClientBuilder, LogFollower, ObservationStream, WaitStream};
pub use error::{Error, Result};
pub use instance::{DefaultInstance, default_instance};
pub use model::{
    AttemptId, AttemptVerdict, BatchId, ContainmentId, DurableIdParseError, InvocationId, JobId,
    JobOutcome, JobState, SubmissionId, SubmissionState,
};
pub use spec::{
    BatchMember, BatchSpec, ChildFencePolicy, ChildSubmissionPolicy, ConditionSpec, DependencyKind,
    DependencySpec, EnvironmentSpec, GpuProviderConfig, HostConfig, HostObservationConfig, JobSpec,
    Label, ObservedResourcePolicy, PostconditionSpec, ProcessRules, QuietDetector, QuietPolicy,
    ResourceCapacities, ResourceClaimLimits, ResourceClaims, RetryPolicy, SPEC_VERSION, StdinSpec,
    SubmissionSpec, config_schema_json, managed_execution_schema_json, schema_json,
};

/// Runs the per-user daemon in the foreground.
///
/// This entry point exists for the bundled CLI. Embedders normally use [`Client`].
#[doc(hidden)]
pub fn run_daemon() -> Result<()> {
    daemon::run(None, None)
}

/// Runs a selected daemon instance in the foreground.
///
/// This entry point exists for the bundled CLI. Consumer harnesses should spawn the pinned
/// `stillyard daemon` executable and connect through [`ClientBuilder`].
#[doc(hidden)]
pub fn run_daemon_instance(
    store_root: Option<std::path::PathBuf>,
    endpoint: Option<String>,
) -> Result<()> {
    daemon::run(store_root, endpoint)
}
