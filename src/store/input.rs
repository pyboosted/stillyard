use super::*;

#[cfg(test)]
pub(crate) fn normalized_payload_hash(spec: &JobSpec) -> StoreResult<String> {
    normalized_payload_hash_with_input(spec, None)
}

pub(crate) fn normalized_payload_hash_with_input(
    spec: &JobSpec,
    stdin: Option<&StagedInputRef>,
) -> StoreResult<String> {
    let normalized = serde_json::to_vec(&(spec, stdin))?;
    Ok(format!("{:x}", Sha256::digest(normalized)))
}

#[cfg(test)]
pub(crate) fn normalized_batch_payload_hash(spec: &BatchSpec) -> StoreResult<String> {
    normalized_batch_payload_hash_with_inputs(spec, &Default::default())
}

pub(crate) fn normalized_batch_payload_hash_with_inputs(
    spec: &BatchSpec,
    stdins: &std::collections::BTreeMap<String, StagedInputRef>,
) -> StoreResult<String> {
    let normalized = serde_json::to_vec(&(spec, stdins))?;
    Ok(format!("{:x}", Sha256::digest(normalized)))
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
