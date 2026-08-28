use std::collections::BTreeMap;
use std::io::{Read, Write};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    BatchReceipt, BatchSpec, ClearContainmentResult, ContainmentId, ContainmentIncidentCursor,
    DaemonSnapshot, DoctorSnapshot, EventCursor, JobId, JobListCursor, JobListPage, JobReceipt,
    JobSelector, JobSnapshot, JobSpec, LogChunk, LogStream, ManagedParent, ObservationFrame,
    SubmissionContext,
};

pub(crate) const PROTOCOL_VERSION: u32 = 13;
const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

pub(crate) mod error_code {
    pub(crate) const BLOCKED_BY_ANCESTOR: &str = "blocked_by_ancestor";
    pub(crate) const IDEMPOTENCY_CONFLICT: &str = "idempotency_conflict";
    pub(crate) const INVALID_SPEC: &str = "invalid_spec";
    pub(crate) const NOT_FOUND: &str = "not_found";
    pub(crate) const REJECTED: &str = "rejected";
    pub(crate) const RESOURCE_CAPACITY: &str = "resource_capacity";
    pub(crate) const STORE_ERROR: &str = "store_error";
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StagedInputRef {
    pub(crate) sha256: String,
    pub(crate) length: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Request {
    Ping {},
    StageBegin {
        upload_id: Uuid,
        expected_sha256: String,
        expected_length: u64,
    },
    StageChunk {
        upload_id: Uuid,
        offset: u64,
        bytes: Vec<u8>,
    },
    StageCommit {
        upload_id: Uuid,
    },
    SubmissionContext {
        claimed_parent: Option<ManagedParent>,
    },
    Submit {
        idempotency_key: Uuid,
        payload_hash: String,
        spec: Box<JobSpec>,
        stdin: Option<StagedInputRef>,
        expected_store_uuid: Option<Uuid>,
        expected_parent: Option<ManagedParent>,
        wait_for_completion: bool,
    },
    SubmitBatch {
        idempotency_key: Uuid,
        payload_hash: String,
        spec: Box<BatchSpec>,
        stdins: BTreeMap<String, StagedInputRef>,
        expected_store_uuid: Option<Uuid>,
        expected_parent: Option<ManagedParent>,
        wait_for_completion: bool,
    },
    Recover {
        idempotency_key: Uuid,
        payload_hash: String,
        expected_parent: Option<ManagedParent>,
    },
    Status {
        job_id: JobId,
    },
    List {
        selector: JobSelector,
        cursor: Option<JobListCursor>,
        limit: u32,
    },
    Observe {
        selector: JobSelector,
        cursor: Option<EventCursor>,
        limit: u32,
        max_wait_millis: u32,
        managed_wait: bool,
    },
    Cancel {
        job_ids: Vec<JobId>,
    },
    Wait {
        job_id: JobId,
        max_wait_millis: u32,
        claimed_parent: Option<ManagedParent>,
    },
    Logs {
        job_id: JobId,
        stream: LogStream,
        offset: u64,
        limit: u32,
    },
    DaemonStatus {},
    Doctor {
        cursor: Option<ContainmentIncidentCursor>,
        limit: Option<u32>,
    },
    ForceClearContainment {
        containment_id: ContainmentId,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub(crate) enum Response {
    Pong {
        protocol_version: u32,
    },
    StageReady {
        next_offset: u64,
    },
    StageCommitted {
        input: StagedInputRef,
    },
    SubmissionContext(SubmissionContext),
    Submitted(JobReceipt),
    BatchSubmitted(BatchReceipt),
    Recovered {
        store_uuid: Uuid,
        recovery: crate::RecoveryResult,
    },
    Snapshot(Box<JobSnapshot>),
    Listed(JobListPage),
    Observed(ObservationFrame),
    Canceled {
        snapshots: Vec<JobSnapshot>,
    },
    Logs(LogChunk),
    DaemonStatus(DaemonSnapshot),
    Doctor(Box<DoctorSnapshot>),
    ContainmentCleared(ClearContainmentResult),
    Error {
        code: String,
        message: String,
    },
}

pub(crate) fn write_frame(mut writer: impl Write, value: &impl Serialize) -> std::io::Result<()> {
    let bytes = serde_json::to_vec(value).map_err(std::io::Error::other)?;
    if bytes.len() > MAX_FRAME_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "local protocol frame exceeds 16 MiB",
        ));
    }
    writer.write_all(&(bytes.len() as u32).to_le_bytes())?;
    writer.write_all(&bytes)?;
    writer.flush()
}

pub(crate) fn read_frame<T: for<'de> Deserialize<'de>>(
    mut reader: impl Read,
) -> std::io::Result<T> {
    let mut length = [0_u8; 4];
    reader.read_exact(&mut length)?;
    let length = u32::from_le_bytes(length) as usize;
    if length > MAX_FRAME_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "local protocol frame exceeds 16 MiB",
        ));
    }
    let mut bytes = vec![0; length];
    reader.read_exact(&mut bytes)?;
    serde_json::from_slice(&bytes).map_err(std::io::Error::other)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_round_trip() {
        let request = Request::Ping {};
        let mut bytes = Vec::new();
        write_frame(&mut bytes, &request).unwrap();
        let decoded: Request = read_frame(bytes.as_slice()).unwrap();
        assert!(matches!(decoded, Request::Ping {}));
    }

    #[test]
    fn responses_are_additive_but_requests_remain_strict() {
        let response: Response = serde_json::from_value(serde_json::json!({
            "result": "pong",
            "protocol_version": PROTOCOL_VERSION,
            "future_evidence": true
        }))
        .unwrap();
        assert!(matches!(response, Response::Pong { .. }));
        assert!(
            serde_json::from_value::<Request>(serde_json::json!({
                "operation": "ping",
                "future_authority": true
            }))
            .is_err()
        );
    }
}
