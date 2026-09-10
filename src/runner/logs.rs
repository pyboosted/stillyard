//! Publish only flushed canonical log prefixes on every executor platform.
use super::lifecycle::RunResult;
use crate::LogStream;
use crate::store::{Store, StoreError};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

pub(super) fn spawn_drain(
    mut input: File,
    path: PathBuf,
    job_id: crate::JobId,
    stream: LogStream,
    store: Arc<Mutex<Store>>,
    publish_job_offset: bool,
) -> std::io::Result<std::thread::JoinHandle<RunResult<()>>> {
    std::thread::Builder::new()
        .name(format!("stillyard-log-{}-{stream:?}", job_id.entity_uuid()))
        .spawn(move || {
            let mut output = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(path)?;
            let mut offset = 0_u64;
            let mut buffer = [0_u8; 64 * 1024];
            loop {
                let read = input.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                output.write_all(&buffer[..read])?;
                output.sync_data()?;
                offset += read as u64;
                if publish_job_offset {
                    store
                        .lock()
                        .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?
                        .commit_log_offset(job_id, stream, offset)?;
                }
            }
            output.sync_all()?;
            Ok(())
        })
}
