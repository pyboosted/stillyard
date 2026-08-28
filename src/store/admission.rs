use super::*;

pub(super) fn rejection_decision(error: &StoreError) -> (String, String) {
    match error {
        StoreError::BlockedByAncestor(detail) => ("blocked_by_ancestor".into(), detail.clone()),
        StoreError::ManagedWaitRejected { code, detail } => (code.clone(), detail.clone()),
        _ => ("rejected".into(), error.to_string()),
    }
}

pub(super) fn retained_rejection(code: Option<String>, detail: Option<String>) -> StoreError {
    let code = code.unwrap_or_else(|| "rejected".into());
    let detail = detail.unwrap_or_else(|| "the retained submission decision is rejected".into());
    match code.as_str() {
        "blocked_by_ancestor" => StoreError::BlockedByAncestor(detail),
        "resource_capacity" => StoreError::ManagedWaitRejected { code, detail },
        _ => StoreError::Rejected(detail),
    }
}

pub(super) fn validate_current_parent(
    connection: &Connection,
    store_uuid: Uuid,
    daemon_generation: Uuid,
    scope: SubmissionScope,
) -> StoreResult<()> {
    let SubmissionScope::Managed(parent) = scope else {
        return Ok(());
    };
    if parent.job_id.store_uuid() != store_uuid
        || parent.attempt_id.store_uuid() != store_uuid
        || parent.invocation_id.store_uuid() != store_uuid
    {
        return Err(StoreError::Rejected(
            "managed parent belongs to a foreign store".into(),
        ));
    }
    let spec_json = connection
        .query_row(
            "SELECT jobs.spec_json
             FROM jobs
             JOIN attempts ON attempts.id = jobs.attempt_id
             JOIN invocations ON invocations.id = jobs.invocation_id
             JOIN containments ON containments.invocation_id = invocations.id
             WHERE jobs.id = ?1
               AND attempts.id = ?2
               AND invocations.id = ?3
               AND jobs.state = 'active'
               AND attempts.state = 'running'
               AND invocations.state = 'started'
               AND invocations.role = 'primary'
               AND invocations.root_pid IS NOT NULL
               AND invocations.root_exit_code IS NULL
               AND invocations.daemon_generation = ?4
               AND containments.state = 'live'",
            params![
                parent.job_id.entity_uuid().to_string(),
                parent.attempt_id.entity_uuid().to_string(),
                parent.invocation_id.entity_uuid().to_string(),
                daemon_generation.to_string(),
            ],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    let Some(spec_json) = spec_json else {
        return Err(StoreError::Rejected(
            "managed parent is no longer the live current primary Invocation".into(),
        ));
    };
    let spec: JobSpec = serde_json::from_str(&spec_json)?;
    if !spec.allow_child_submissions {
        return Err(StoreError::Rejected(
            "managed parent does not allow child submissions".into(),
        ));
    }
    Ok(())
}

pub(super) fn validate_managed_wait_targets(
    connection: &Connection,
    store_uuid: Uuid,
    daemon_generation: Uuid,
    capacities: &ResourceCapacities,
    impact_incompatibilities: &std::collections::BTreeMap<String, Vec<String>>,
    scope: SubmissionScope,
    targets: &[JobId],
) -> StoreResult<()> {
    let SubmissionScope::Managed(parent) = scope else {
        return Ok(());
    };
    if targets.is_empty() {
        return Err(StoreError::Rejected(
            "managed wait requires at least one target".into(),
        ));
    }
    validate_current_parent(connection, store_uuid, daemon_generation, scope)?;
    let ancestor_claims = managed_ancestor_claims(connection, store_uuid, parent)?;
    let mut pending = std::collections::VecDeque::from_iter(targets.iter().copied());
    let mut visited = std::collections::HashSet::new();
    let mut waited_claims = Vec::new();
    while let Some(job_id) = pending.pop_front() {
        if job_id.store_uuid() != store_uuid {
            return Err(StoreError::Rejected(format!(
                "managed wait target {job_id} belongs to a foreign store"
            )));
        }
        let job_key = job_id.entity_uuid().to_string();
        if !visited.insert(job_key.clone()) {
            continue;
        }
        if job_id == parent.job_id {
            return Err(StoreError::BlockedByAncestor(
                "the dependency closure reaches the waiting Job itself".into(),
            ));
        }
        if !job_descends_from(connection, store_uuid, job_id, parent.job_id)? {
            return Err(StoreError::Rejected(format!(
                "managed wait target {job_id} is not an authenticated descendant of {}",
                parent.job_id
            )));
        }
        let (state, claims_json, display_name) = connection
            .query_row(
                "SELECT state, claims_json, COALESCE(batch_member, id) FROM jobs WHERE id = ?1",
                [&job_key],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(job_id.to_string()))?;
        if state == "final" {
            continue;
        }
        let claims: ResolvedClaims = serde_json::from_str(&claims_json)?;
        waited_claims.push((job_id, display_name, claims));
        let mut statement = connection.prepare(
            "SELECT dependencies.predecessor_id
             FROM dependencies
             JOIN jobs ON jobs.id = dependencies.predecessor_id
             WHERE dependencies.successor_id = ?1 AND jobs.state != 'final'
             ORDER BY jobs.accepted_ms, jobs.rowid",
        )?;
        let predecessors = statement
            .query_map([&job_key], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        for predecessor in predecessors {
            pending.push_back(JobId::from_parts(
                store_uuid,
                Uuid::parse_str(&predecessor)?,
            ));
        }
    }
    for (job_id, display_name, claims) in waited_claims {
        let blockers =
            claims.ancestor_blockers(capacities, &ancestor_claims, impact_incompatibilities);
        if !blockers.is_empty() {
            let detail = format!(
                "target {display_name} ({job_id}): {}",
                blockers
                    .iter()
                    .map(|blocker| blocker.detail.as_str())
                    .collect::<Vec<_>>()
                    .join("; ")
            );
            if blockers
                .iter()
                .any(|blocker| blocker.code == "resource_capacity")
            {
                return Err(StoreError::ManagedWaitRejected {
                    code: "resource_capacity".into(),
                    detail,
                });
            }
            return Err(StoreError::BlockedByAncestor(detail));
        }
    }
    Ok(())
}

pub(super) fn job_descends_from(
    connection: &Connection,
    store_uuid: Uuid,
    job_id: JobId,
    ancestor_id: JobId,
) -> StoreResult<bool> {
    let mut current = job_id;
    let mut visited = std::collections::HashSet::new();
    loop {
        if !visited.insert(current.entity_uuid()) {
            return Err(StoreError::InvalidState(
                "managed parent graph contains a cycle".into(),
            ));
        }
        let columns = connection
            .query_row(
                "SELECT parent_job_id, parent_attempt_id, parent_invocation_id
                 FROM jobs WHERE id = ?1",
                [current.entity_uuid().to_string()],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(current.to_string()))?;
        let Some(parent) = managed_parent_from_columns(store_uuid, columns)? else {
            return Ok(false);
        };
        if parent.job_id == ancestor_id {
            return Ok(true);
        }
        current = parent.job_id;
    }
}

pub(super) fn managed_ancestor_claims(
    connection: &Connection,
    store_uuid: Uuid,
    parent: ManagedParent,
) -> StoreResult<Vec<ResolvedClaims>> {
    let mut current = Some(parent);
    let mut visited = std::collections::HashSet::new();
    let mut claims = Vec::new();
    while let Some(ancestor) = current {
        if !visited.insert((
            ancestor.job_id.entity_uuid(),
            ancestor.attempt_id.entity_uuid(),
        )) {
            return Err(StoreError::InvalidState(
                "managed ancestor graph contains a cycle".into(),
            ));
        }
        let lease = connection
            .query_row(
                "SELECT leases.state, leases.claims_json
                 FROM leases
                 JOIN attempts ON attempts.id = leases.attempt_id
                 WHERE attempts.id = ?1 AND attempts.job_id = ?2",
                params![
                    ancestor.attempt_id.entity_uuid().to_string(),
                    ancestor.job_id.entity_uuid().to_string(),
                ],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?
            .ok_or_else(|| {
                StoreError::InvalidState(format!(
                    "managed ancestor {} has no Lease for Attempt {}",
                    ancestor.job_id, ancestor.attempt_id
                ))
            })?;
        match lease.0.as_str() {
            "granted" => claims.push(serde_json::from_str(&lease.1)?),
            "released" => {}
            other => {
                return Err(StoreError::InvalidState(format!(
                    "managed ancestor Lease has unknown state {other}"
                )));
            }
        }
        let columns = connection.query_row(
            "SELECT parent_job_id, parent_attempt_id, parent_invocation_id
             FROM jobs WHERE id = ?1",
            [ancestor.job_id.entity_uuid().to_string()],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            },
        )?;
        current = managed_parent_from_columns(store_uuid, columns)?;
    }
    Ok(claims)
}

pub(super) fn managed_parent_from_columns(
    store_uuid: Uuid,
    columns: (Option<String>, Option<String>, Option<String>),
) -> StoreResult<Option<ManagedParent>> {
    match columns {
        (None, None, None) => Ok(None),
        (Some(job), Some(attempt), Some(invocation)) => Ok(Some(ManagedParent {
            job_id: JobId::from_parts(store_uuid, Uuid::parse_str(&job)?),
            attempt_id: AttemptId::from_parts(store_uuid, Uuid::parse_str(&attempt)?),
            invocation_id: InvocationId::from_parts(store_uuid, Uuid::parse_str(&invocation)?),
        })),
        _ => Err(StoreError::InvalidState(
            "managed parent columns are only partially populated".into(),
        )),
    }
}

pub(super) fn dependency_blockers_tx(
    transaction: &rusqlite::Transaction<'_>,
    job_id: JobId,
) -> StoreResult<(Vec<Blocker>, bool)> {
    let mut statement = transaction.prepare(
        "SELECT dependencies.kind, jobs.state, jobs.outcome, jobs.batch_member
         FROM dependencies JOIN jobs ON jobs.id = dependencies.predecessor_id
         WHERE dependencies.successor_id = ?1 ORDER BY jobs.batch_index",
    )?;
    let rows = statement.query_map([job_id.entity_uuid().to_string()], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<String>>(3)?,
        ))
    })?;
    let mut blockers = Vec::new();
    let mut impossible = false;
    for row in rows {
        let (kind, state, outcome, name) = row?;
        if state != "final" {
            blockers.push(Blocker {
                code: "dependency_pending".into(),
                detail: name.unwrap_or_else(|| "predecessor".into()),
            });
            continue;
        }
        let satisfied = match kind.as_str() {
            "success" => outcome.as_deref() == Some("succeeded"),
            "failure" => outcome.as_deref() == Some("failed"),
            "terminal" => true,
            other => {
                return Err(StoreError::InvalidState(format!(
                    "unknown dependency kind {other}"
                )));
            }
        };
        impossible |= !satisfied;
    }
    Ok((blockers, impossible))
}

pub(super) fn active_claims_tx(
    transaction: &rusqlite::Transaction<'_>,
) -> StoreResult<Vec<ResolvedClaims>> {
    let mut statement =
        transaction.prepare("SELECT claims_json FROM leases WHERE state = 'granted'")?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
}

pub(super) fn dependency_kind(kind: crate::DependencyKind) -> &'static str {
    match kind {
        crate::DependencyKind::Success => "success",
        crate::DependencyKind::Failure => "failure",
        crate::DependencyKind::Terminal => "terminal",
    }
}
