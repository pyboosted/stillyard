use super::*;

impl Store {
    pub(crate) fn stage_begin(
        &self,
        upload_id: Uuid,
        expected_sha256: &str,
        expected_length: u64,
    ) -> StoreResult<u64> {
        validate_input_ref(&StagedInputRef {
            sha256: expected_sha256.to_owned(),
            length: expected_length,
        })?;
        let metadata_path = self.upload_metadata_path(upload_id);
        let partial_path = self.upload_partial_path(upload_id);
        let expected = UploadMetadata {
            sha256: expected_sha256.to_owned(),
            length: expected_length,
        };
        let metadata_exists = metadata_path.try_exists()?;
        let partial_exists = partial_path.try_exists()?;
        if metadata_exists && partial_exists {
            let actual: UploadMetadata = serde_json::from_reader(File::open(&metadata_path)?)?;
            if actual.sha256 != expected.sha256 || actual.length != expected.length {
                return Err(StoreError::InvalidSpec(
                    "upload ID was reused with different stdin metadata".into(),
                ));
            }
            let offset = std::fs::metadata(&partial_path)?.len();
            if offset > expected_length {
                return Err(StoreError::InvalidState(
                    "partial stdin upload exceeds its declared length".into(),
                ));
            }
            return Ok(offset);
        }
        if metadata_exists || partial_exists {
            remove_file_allow_readonly(&metadata_path)?;
            remove_file_allow_readonly(&partial_path)?;
        }
        let mut metadata = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&metadata_path)?;
        let initialized = (|| -> StoreResult<()> {
            serde_json::to_writer(&mut metadata, &expected)?;
            metadata.write_all(b"\n")?;
            metadata.sync_all()?;
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&partial_path)?
                .sync_all()?;
            Ok(())
        })();
        if let Err(error) = initialized {
            drop(metadata);
            let _ = std::fs::remove_file(&partial_path);
            let _ = std::fs::remove_file(&metadata_path);
            return Err(error);
        }
        Ok(0)
    }

    pub(crate) fn stage_chunk(
        &self,
        upload_id: Uuid,
        offset: u64,
        bytes: &[u8],
    ) -> StoreResult<u64> {
        if bytes.is_empty() || bytes.len() > MAX_UPLOAD_CHUNK_BYTES {
            return Err(StoreError::InvalidSpec(format!(
                "stdin upload chunks must contain 1..={MAX_UPLOAD_CHUNK_BYTES} bytes"
            )));
        }
        let metadata: UploadMetadata =
            serde_json::from_reader(File::open(self.upload_metadata_path(upload_id))?)?;
        let partial_path = self.upload_partial_path(upload_id);
        let current = std::fs::metadata(&partial_path)?.len();
        if current != offset {
            return Err(StoreError::InvalidState(format!(
                "stdin upload offset mismatch: expected {current}, received {offset}"
            )));
        }
        let next = current.saturating_add(bytes.len() as u64);
        if next > metadata.length {
            return Err(StoreError::InvalidSpec(
                "stdin upload exceeds its declared length".into(),
            ));
        }
        let mut partial = OpenOptions::new().append(true).open(partial_path)?;
        partial.write_all(bytes)?;
        Ok(next)
    }

    pub(crate) fn stage_commit(&self, upload_id: Uuid) -> StoreResult<StagedInputRef> {
        let metadata_path = self.upload_metadata_path(upload_id);
        let metadata: UploadMetadata = serde_json::from_reader(File::open(&metadata_path)?)?;
        let input = StagedInputRef {
            sha256: metadata.sha256,
            length: metadata.length,
        };
        validate_input_ref(&input)?;
        let partial_path = self.upload_partial_path(upload_id);
        let blob_path = self.paths.blob_path(&input.sha256);
        if !partial_path.try_exists()? && blob_path.try_exists()? {
            verify_file(&blob_path, &input)?;
            set_file_readonly(&blob_path)?;
            std::fs::remove_file(metadata_path)?;
            return Ok(input);
        }
        verify_file(&partial_path, &input)?;
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(&partial_path)?
            .sync_all()?;
        if blob_path.try_exists()? {
            verify_file(&blob_path, &input)?;
            set_file_readonly(&blob_path)?;
            remove_file_allow_readonly(&partial_path)?;
        } else {
            std::fs::rename(&partial_path, &blob_path).map_err(|error| {
                StoreError::InvalidState(format!(
                    "cannot publish staged stdin {}: {error}",
                    blob_path.display()
                ))
            })?;
            if let Err(error) = set_file_readonly(&blob_path) {
                let _ = std::fs::rename(&blob_path, &partial_path);
                return Err(error);
            }
        }
        std::fs::remove_file(&metadata_path).map_err(|error| {
            StoreError::InvalidState(format!(
                "cannot finalize staged stdin metadata {}: {error}",
                metadata_path.display()
            ))
        })?;
        Ok(input)
    }

    pub(super) fn upload_metadata_path(&self, upload_id: Uuid) -> PathBuf {
        self.paths.uploads.join(format!("{upload_id}.json"))
    }

    pub(super) fn upload_partial_path(&self, upload_id: Uuid) -> PathBuf {
        self.paths.uploads.join(format!("{upload_id}.partial"))
    }

    pub(super) fn collect_abandoned_staging(&self) -> StoreResult<()> {
        for entry in std::fs::read_dir(&self.paths.uploads)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                remove_file_allow_readonly(&entry.path())?;
            }
        }
        let mut referenced = std::collections::HashSet::new();
        let mut statement = self
            .connection
            .prepare("SELECT DISTINCT stdin_hash FROM jobs WHERE stdin_hash IS NOT NULL")?;
        for hash in statement.query_map([], |row| row.get::<_, String>(0))? {
            referenced.insert(hash?);
        }
        for entry in std::fs::read_dir(&self.paths.blobs)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            let hash = name.strip_suffix(".stdin").unwrap_or_default();
            if !referenced.contains(hash) {
                remove_file_allow_readonly(&entry.path())?;
            }
        }
        Ok(())
    }

    pub(super) fn verify_staged_input(
        &self,
        spec: &JobSpec,
        stdin: Option<&StagedInputRef>,
    ) -> StoreResult<()> {
        validate_input_shape(spec, stdin)?;
        if let Some(stdin) = stdin {
            verify_file(&self.paths.blob_path(&stdin.sha256), stdin)?;
        }
        Ok(())
    }

    pub(super) fn verify_staged_batch_inputs(
        &self,
        spec: &BatchSpec,
        stdins: &std::collections::BTreeMap<String, StagedInputRef>,
    ) -> StoreResult<()> {
        validate_batch_input_shape(spec, stdins)?;
        for stdin in stdins.values() {
            verify_file(&self.paths.blob_path(&stdin.sha256), stdin)?;
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) fn normalized_payload_hash(spec: &JobSpec) -> StoreResult<String> {
    normalized_payload_hash_with_input(spec, None)
}

pub(crate) fn normalized_payload_hash_with_input(
    spec: &JobSpec,
    stdin: Option<&StagedInputRef>,
) -> StoreResult<String> {
    Ok(crate::payload::job_hash(spec, stdin)?)
}

#[cfg(test)]
pub(crate) fn normalized_batch_payload_hash(spec: &BatchSpec) -> StoreResult<String> {
    normalized_batch_payload_hash_with_inputs(spec, &Default::default())
}

pub(crate) fn normalized_batch_payload_hash_with_inputs(
    spec: &BatchSpec,
    stdins: &std::collections::BTreeMap<String, StagedInputRef>,
) -> StoreResult<String> {
    Ok(crate::payload::batch_hash(spec, stdins)?)
}

pub(super) fn validate_input_ref(input: &StagedInputRef) -> StoreResult<()> {
    if input.length > MAX_STDIN_BYTES
        || input.sha256.len() != 64
        || !input
            .sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(StoreError::InvalidSpec(format!(
            "staged stdin must be at most {MAX_STDIN_BYTES} bytes with a lowercase SHA-256"
        )));
    }
    Ok(())
}

pub(super) fn validate_input_shape(
    spec: &JobSpec,
    stdin: Option<&StagedInputRef>,
) -> StoreResult<()> {
    match (&spec.stdin, stdin) {
        (StdinSpec::Eof, None) => Ok(()),
        (StdinSpec::File { .. }, Some(stdin)) => validate_input_ref(stdin),
        (StdinSpec::Eof, Some(_)) => Err(StoreError::InvalidSpec(
            "EOF stdin must not carry a staged input".into(),
        )),
        (StdinSpec::File { .. }, None) => Err(StoreError::InvalidSpec(
            "file stdin requires one committed staged input".into(),
        )),
    }
}

pub(super) fn validate_batch_input_shape(
    spec: &BatchSpec,
    stdins: &std::collections::BTreeMap<String, StagedInputRef>,
) -> StoreResult<()> {
    let expected: std::collections::BTreeSet<_> = spec
        .jobs
        .iter()
        .filter(|member| matches!(member.spec.stdin, StdinSpec::File { .. }))
        .map(|member| member.name.as_str())
        .collect();
    if expected.len() != stdins.len() || !stdins.keys().all(|name| expected.contains(name.as_str()))
    {
        return Err(StoreError::InvalidSpec(
            "Batch staged stdin mapping must exactly match file-stdin members".into(),
        ));
    }
    for stdin in stdins.values() {
        validate_input_ref(stdin)?;
    }
    Ok(())
}

pub(super) fn verify_file(path: &Path, input: &StagedInputRef) -> StoreResult<()> {
    validate_input_ref(input)?;
    let mut file = File::open(path)?;
    if file.metadata()?.len() != input.length {
        return Err(StoreError::InvalidSpec(
            "staged stdin length does not match its reference".into(),
        ));
    }
    let mut hash = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hash.update(&buffer[..read]);
    }
    if format!("{:x}", hash.finalize()) != input.sha256 {
        return Err(StoreError::InvalidSpec(
            "staged stdin hash does not match its reference".into(),
        ));
    }
    Ok(())
}

pub(super) fn remove_file_allow_readonly(path: &Path) -> StoreResult<()> {
    if !path.try_exists()? {
        return Ok(());
    }
    let mut permissions = std::fs::metadata(path)?.permissions();
    if permissions.readonly() {
        make_file_writable(path, &mut permissions)?;
    }
    std::fs::remove_file(path)?;
    Ok(())
}

#[cfg(windows)]
pub(super) fn make_file_writable(
    path: &Path,
    permissions: &mut std::fs::Permissions,
) -> StoreResult<()> {
    permissions.set_readonly(false);
    std::fs::set_permissions(path, permissions.clone())?;
    Ok(())
}

#[cfg(unix)]
pub(super) fn make_file_writable(
    path: &Path,
    permissions: &mut std::fs::Permissions,
) -> StoreResult<()> {
    use std::os::unix::fs::PermissionsExt;

    permissions.set_mode(permissions.mode() | 0o200);
    std::fs::set_permissions(path, permissions.clone())?;
    Ok(())
}

pub(super) fn set_file_readonly(path: &Path) -> StoreResult<()> {
    let mut permissions = std::fs::metadata(path)?.permissions();
    if !permissions.readonly() {
        permissions.set_readonly(true);
        std::fs::set_permissions(path, permissions).map_err(|error| {
            StoreError::InvalidState(format!(
                "cannot make staged stdin immutable at {}: {error}",
                path.display()
            ))
        })?;
    }
    Ok(())
}
