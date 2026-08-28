use std::collections::BTreeMap;

use sha2::{Digest, Sha256};

use crate::protocol::StagedInputRef;
use crate::{BatchSpec, JobSpec};

pub(crate) const MAX_CANCEL_JOBS: usize = 16;
pub(crate) const MAX_STDIN_BYTES: u64 = 64 * 1024 * 1024;

pub(crate) fn job_hash(
    spec: &JobSpec,
    stdin: Option<&StagedInputRef>,
) -> serde_json::Result<String> {
    let normalized = serde_json::to_vec(&(spec, stdin))?;
    Ok(format!("{:x}", Sha256::digest(normalized)))
}

pub(crate) fn batch_hash(
    spec: &BatchSpec,
    stdins: &BTreeMap<String, StagedInputRef>,
) -> serde_json::Result<String> {
    let normalized = serde_json::to_vec(&(spec, stdins))?;
    Ok(format!("{:x}", Sha256::digest(normalized)))
}
