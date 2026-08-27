use std::io::{Read, Write};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{DaemonSnapshot, JobId, JobReceipt, JobSnapshot, JobSpec, LogChunk, LogStream};

pub(crate) const PROTOCOL_VERSION: u32 = 1;
const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Request {
    Ping,
    Submit {
        idempotency_key: Uuid,
        payload_hash: String,
        spec: Box<JobSpec>,
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
    Pong { protocol_version: u32 },
    Submitted(JobReceipt),
    Recovered(crate::RecoveryResult),
    Snapshot(Box<JobSnapshot>),
    Logs(LogChunk),
    DaemonStatus(DaemonSnapshot),
    Error { code: String, message: String },
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
