//! Public, runtime-neutral client contract for Stillyard.

mod api;
mod client;
#[cfg_attr(not(windows), allow(dead_code))]
pub(crate) mod daemon;
mod error;
mod filesystem;
mod model;
#[cfg_attr(not(windows), allow(dead_code))]
pub(crate) mod protocol;
mod resources;
#[cfg_attr(not(windows), allow(dead_code))]
pub(crate) mod runner;
mod spec;
#[cfg_attr(not(windows), allow(dead_code))]
pub(crate) mod store;

pub use api::{
    AttemptSnapshot, BatchJobReceipt, BatchReceipt, Blocker, CancellationToken,
    ContainmentSnapshot, ContainmentState, DaemonSnapshot, Estimate, EstimateConfidence,
    EventCursor, EventGap, ExitClassification, InvocationRole, InvocationSnapshot, InvocationState,
    JobListCursor, JobListPage, JobReceipt, JobSelector, JobSnapshot, JobSummary, LogChunk,
    LogStream, MAX_OBSERVATION_PAGE, MAX_WAIT_STREAM_JOBS, ManagedParent,
    ObservationCursorParseError, ObservationFrame, RecoveryResult, SchedulerEvent,
    SchedulerEventKind, SubmissionContext, SubmitOptions, WaitStreamItem,
};
pub use client::{Client, ClientBuilder, LogFollower, ObservationStream, WaitStream};
pub use error::{Error, Result};
pub use model::{
    AttemptId, AttemptVerdict, BatchId, ContainmentId, DurableIdParseError, InvocationId, JobId,
    JobOutcome, JobState, SubmissionId, SubmissionState,
};
pub use spec::{
    BatchMember, BatchSpec, ConditionSpec, DependencyKind, DependencySpec, EnvironmentProfile,
    EnvironmentSpec, HostConfig, JobSpec, Label, PostconditionSpec, QuietPolicy,
    ResourceCapacities, ResourceClaims, RetryPolicy, SPEC_VERSION, StdinSpec, SubmissionSpec,
    config_schema_json, schema_json,
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
