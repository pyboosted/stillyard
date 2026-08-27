use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

macro_rules! durable_id {
    ($name:ident) => {
        #[derive(
            Clone,
            Copy,
            Debug,
            Eq,
            Hash,
            JsonSchema,
            Ord,
            PartialEq,
            PartialOrd,
            Serialize,
            Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(pub Uuid);

        impl $name {
            #[must_use]
            pub fn new() -> Self {
                Self(Uuid::now_v7())
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(formatter)
            }
        }

        impl std::str::FromStr for $name {
            type Err = uuid::Error;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                value.parse().map(Self)
            }
        }
    };
}

durable_id!(SubmissionId);
durable_id!(BatchId);
durable_id!(JobId);
durable_id!(AttemptId);
durable_id!(InvocationId);
durable_id!(ContainmentId);

#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubmissionState {
    Received,
    Accepted,
    Rejected,
}

#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Pending,
    Active,
    Finalizing,
    Final,
}

#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptVerdict {
    Succeeded,
    ProcessFailed,
    StartFailed,
    TimedOut,
    Interrupted,
    SafetyFailed,
    PostconditionRetryable,
    PostconditionFailed,
    Canceled,
}

#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobOutcome {
    Succeeded,
    Failed,
    TimedOut,
    Interrupted,
    Canceled,
    Skipped,
}
