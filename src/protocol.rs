use std::collections::BTreeMap;
use std::io::{Read, Write};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    BatchReceipt, BatchSpec, DaemonSnapshot, JobId, JobReceipt, JobSnapshot, JobSpec, LogChunk,
    LogStream,
};

pub(crate) const PROTOCOL_VERSION: u32 = 4;
const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StagedInputRef {
    pub(crate) sha256: String,
    pub(crate) length: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Request {
    Ping,
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
    Submit {
        idempotency_key: Uuid,
        payload_hash: String,
        spec: Box<JobSpec>,
        stdin: Option<StagedInputRef>,
        expected_store_uuid: Option<Uuid>,
    },
    SubmitBatch {
        idempotency_key: Uuid,
        payload_hash: String,
        spec: Box<BatchSpec>,
        stdins: BTreeMap<String, StagedInputRef>,
        expected_store_uuid: Option<Uuid>,
    },
    Recover {
        idempotency_key: Uuid,
        payload_hash: String,
    },
    Status {
        job_id: JobId,
    },
    Wait {
        job_id: JobId,
        max_wait_millis: u32,
    },
    Logs {
        job_id: JobId,
        stream: LogStream,
        offset: u64,
        limit: u32,
    },
    DaemonStatus,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
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
    Submitted(JobReceipt),
    BatchSubmitted(BatchReceipt),
    Recovered {
        store_uuid: Uuid,
        recovery: crate::RecoveryResult,
    },
    Snapshot(Box<JobSnapshot>),
    Logs(LogChunk),
    DaemonStatus(DaemonSnapshot),
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
        let request = Request::Ping;
        let mut bytes = Vec::new();
        write_frame(&mut bytes, &request).unwrap();
        let decoded: Request = read_frame(bytes.as_slice()).unwrap();
        assert!(matches!(decoded, Request::Ping));
    }
}
