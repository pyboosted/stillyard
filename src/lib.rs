//! Public, runtime-neutral client contract for Stillyard.

mod error;
mod model;
mod spec;

pub use error::{Error, Result};
pub use model::{
    AttemptId, AttemptVerdict, BatchId, ContainmentId, InvocationId, JobId, JobOutcome, JobState,
    SubmissionId, SubmissionState,
};
pub use spec::{
    BatchMember, BatchSpec, ConditionSpec, DependencyKind, DependencySpec, EnvironmentSpec,
    JobSpec, Label, QuietPolicy, ResourceClaims, RetryPolicy, SPEC_VERSION, StdinSpec, schema_json,
};
