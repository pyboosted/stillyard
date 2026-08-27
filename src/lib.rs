//! Public, runtime-neutral client contract for Stillyard.

mod api;
mod client;
#[cfg_attr(not(windows), allow(dead_code))]
pub(crate) mod daemon;
mod error;
mod model;
#[cfg_attr(not(windows), allow(dead_code))]
pub(crate) mod protocol;
#[cfg_attr(not(windows), allow(dead_code))]
pub(crate) mod runner;
mod spec;
#[cfg_attr(not(windows), allow(dead_code))]
pub(crate) mod store;

pub use api::{
    Blocker, CancellationToken, DaemonSnapshot, Estimate, EstimateConfidence, JobReceipt,
    JobSnapshot, LogChunk, LogStream, RecoveryResult, SubmitOptions,
};
pub use client::{Client, ClientBuilder};
pub use error::{Error, Result};
pub use model::{
    AttemptId, AttemptVerdict, BatchId, ContainmentId, InvocationId, JobId, JobOutcome, JobState,
    SubmissionId, SubmissionState,
};
pub use spec::{
    BatchMember, BatchSpec, ConditionSpec, DependencyKind, DependencySpec, EnvironmentSpec,
    JobSpec, Label, QuietPolicy, ResourceClaims, RetryPolicy, SPEC_VERSION, StdinSpec, schema_json,
};

/// Runs the per-user daemon in the foreground.
///
/// This entry point exists for the bundled CLI. Embedders normally use [`Client`].
#[doc(hidden)]
pub fn run_daemon() -> Result<()> {
    daemon::run()
}
