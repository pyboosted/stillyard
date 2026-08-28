use std::fmt;
use std::str::FromStr;

use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

/// Error returned when a durable Stillyard identifier is malformed.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("durable ID must be '<store-uuid>~<entity-uuid>'")]
pub struct DurableIdParseError;

macro_rules! durable_id {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd)]
        #[schemars(with = "String")]
        pub struct $name {
            store_uuid: Uuid,
            entity_uuid: Uuid,
        }

        impl $name {
            pub(crate) fn new(store_uuid: Uuid) -> Self {
                Self {
                    store_uuid,
                    entity_uuid: Uuid::now_v7(),
                }
            }

            pub(crate) fn from_parts(store_uuid: Uuid, entity_uuid: Uuid) -> Self {
                Self {
                    store_uuid,
                    entity_uuid,
                }
            }

            /// UUID of the durable store that owns this entity.
            #[must_use]
            pub fn store_uuid(self) -> Uuid {
                self.store_uuid
            }

            /// UUID of the entity within its owning store.
            #[must_use]
            pub fn entity_uuid(self) -> Uuid {
                self.entity_uuid
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "{}~{}", self.store_uuid, self.entity_uuid)
            }
        }

        impl FromStr for $name {
            type Err = DurableIdParseError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                let (store, entity) = value.split_once('~').ok_or(DurableIdParseError)?;
                if entity.contains('~') {
                    return Err(DurableIdParseError);
                }
                Ok(Self {
                    store_uuid: Uuid::parse_str(store).map_err(|_| DurableIdParseError)?,
                    entity_uuid: Uuid::parse_str(entity).map_err(|_| DurableIdParseError)?,
                })
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.collect_str(self)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                value.parse().map_err(serde::de::Error::custom)
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
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum SubmissionState {
    Received,
    Accepted,
    Rejected,
}

#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Pending,
    Active,
    Finalizing,
    Final,
}

#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
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

impl AttemptVerdict {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::ProcessFailed => "process_failed",
            Self::StartFailed => "start_failed",
            Self::TimedOut => "timed_out",
            Self::Interrupted => "interrupted",
            Self::SafetyFailed => "safety_failed",
            Self::PostconditionRetryable => "postcondition_retryable",
            Self::PostconditionFailed => "postcondition_failed",
            Self::Canceled => "canceled",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum JobOutcome {
    Succeeded,
    Failed,
    TimedOut,
    Interrupted,
    Canceled,
    Skipped,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durable_id_round_trip_carries_store_identity() {
        let id = JobId::new(Uuid::now_v7());
        assert_eq!(id.to_string().parse::<JobId>().unwrap(), id);
        assert_eq!(
            serde_json::from_str::<JobId>(&serde_json::to_string(&id).unwrap()).unwrap(),
            id
        );
    }

    #[test]
    fn unscoped_uuid_is_not_a_public_durable_id() {
        assert!(Uuid::now_v7().to_string().parse::<JobId>().is_err());
    }
}
