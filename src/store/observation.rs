use super::*;

fn command_preview(spec: &JobSpec, max_chars: usize) -> String {
    command_preview_from_parts(&spec.executable, &spec.args, max_chars)
}

fn command_preview_from_parts(executable: &Path, args: &[String], max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }
    let executable = executable
        .file_name()
        .unwrap_or(executable.as_os_str())
        .to_string_lossy();
    let mut preview = String::new();
    for raw in std::iter::once(executable.as_ref()).chain(args.iter().map(String::as_str)) {
        let token = if raw.is_empty()
            || raw.chars().any(|character| {
                character.is_whitespace() || character.is_control() || character == '"'
            }) {
            serde_json::to_string(raw).unwrap_or_else(|_| "\"?\"".into())
        } else {
            raw.to_owned()
        };
        let separator = if preview.is_empty() { "" } else { " " };
        let available = max_chars.saturating_sub(preview.chars().count());
        let required = separator.chars().count() + token.chars().count();
        if required <= available {
            preview.push_str(separator);
            preview.push_str(&token);
            continue;
        }
        if available > 0 {
            let content = separator
                .chars()
                .chain(token.chars())
                .take(available.saturating_sub(1));
            preview.extend(content);
            preview.push('…');
        }
        break;
    }
    preview
}

impl Store {
    pub(crate) fn status(&self, job_id: JobId) -> StoreResult<JobSnapshot> {
        self.status_with_reconciliation(job_id, &ReconciliationObservations::default())
    }

    pub(crate) fn status_with_reconciliation(
        &self,
        job_id: JobId,
        observations: &ReconciliationObservations,
    ) -> StoreResult<JobSnapshot> {
        self.connection
            .query_row(
                "SELECT submission_id, state, outcome, attempt_id, invocation_id,
                    containment_id, root_exit_code, accepted_ms, started_ms, finished_ms,
                    spec_json, batch_id, batch_member,
                    parent_job_id, parent_attempt_id, parent_invocation_id,
                    cancel_requested != 0, managed_policy_admission_json
                 FROM jobs WHERE id = ?1",
                [self.local_id(job_id)?],
                |row| {
                    let submission_id: String = row.get(0)?;
                    let state: String = row.get(1)?;
                    let outcome: Option<String> = row.get(2)?;
                    let attempt_id: Option<String> = row.get(3)?;
                    let invocation_id: Option<String> = row.get(4)?;
                    let containment_id: Option<String> = row.get(5)?;
                    let spec_json: String = row.get(10)?;
                    Ok((
                        submission_id,
                        state,
                        outcome,
                        attempt_id,
                        invocation_id,
                        containment_id,
                        row.get::<_, Option<i32>>(6)?,
                        row.get::<_, i64>(7)?,
                        row.get::<_, Option<i64>>(8)?,
                        row.get::<_, Option<i64>>(9)?,
                        spec_json,
                        row.get::<_, Option<String>>(11)?,
                        row.get::<_, Option<String>>(12)?,
                        row.get::<_, Option<String>>(13)?,
                        row.get::<_, Option<String>>(14)?,
                        row.get::<_, Option<String>>(15)?,
                        row.get::<_, bool>(16)?,
                        row.get::<_, Option<String>>(17)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(job_id.to_string()))
            .and_then(
                |(
                    submission_id,
                    state,
                    outcome,
                    attempt_id,
                    invocation_id,
                    containment_id,
                    root_exit_code,
                    accepted_ms,
                    started_ms,
                    finished_ms,
                    spec_json,
                    batch_id,
                    batch_member,
                    parent_job,
                    parent_attempt,
                    parent_invocation,
                    cancel_requested,
                    managed_policy_admission_json,
                )| {
                    let parsed_state = parse_job_state(&state)?;
                    Ok(JobSnapshot {
                        job_id,
                        submission_id: SubmissionId::from_parts(
                            self.store_uuid,
                            Uuid::parse_str(&submission_id)?,
                        ),
                        batch_id: batch_id
                            .map(|value| {
                                Uuid::parse_str(&value)
                                    .map(|uuid| BatchId::from_parts(self.store_uuid, uuid))
                            })
                            .transpose()?,
                        batch_member,
                        state: parsed_state,
                        outcome: outcome.map(|value| parse_outcome(&value)).transpose()?,
                        attempt_id: attempt_id
                            .map(|value| {
                                Uuid::parse_str(&value)
                                    .map(|uuid| AttemptId::from_parts(self.store_uuid, uuid))
                            })
                            .transpose()?,
                        invocation_id: invocation_id
                            .map(|value| {
                                Uuid::parse_str(&value)
                                    .map(|uuid| InvocationId::from_parts(self.store_uuid, uuid))
                            })
                            .transpose()?,
                        containment_id: containment_id
                            .map(|value| {
                                Uuid::parse_str(&value)
                                    .map(|uuid| ContainmentId::from_parts(self.store_uuid, uuid))
                            })
                            .transpose()?,
                        root_exit_code,
                        cancel_requested,
                        accepted_unix_millis: accepted_ms,
                        started_unix_millis: started_ms,
                        finished_unix_millis: finished_ms,
                        spec: serde_json::from_str(&spec_json)?,
                        parent: managed_parent_from_columns(
                            self.store_uuid,
                            (parent_job, parent_attempt, parent_invocation),
                        )?,
                        managed_policy_admission: managed_policy_admission_json
                            .map(|json| serde_json::from_str(&json))
                            .transpose()?,
                        blockers: if parsed_state == JobState::Pending {
                            self.blockers_for_job(job_id)?
                        } else {
                            Vec::new()
                        },
                        attempts: self.attempt_snapshots(job_id, observations)?,
                        gpu_provenance: self.gpu_provenance_for_job(job_id)?,
                        admission: self.admission_for_job(job_id)?,
                        daemon_generation: self.daemon_generation,
                    })
                },
            )
    }

    pub(crate) fn list_jobs(
        &self,
        selector: &JobSelector,
        cursor: Option<JobListCursor>,
        limit: u32,
    ) -> StoreResult<JobListPage> {
        self.validate_selector(selector)?;
        let limit = usize::try_from(limit.clamp(1, MAX_OBSERVATION_PAGE)).unwrap_or(1);
        if let Some(cursor) = cursor {
            if cursor.store_uuid != self.store_uuid || cursor.job_id.store_uuid() != self.store_uuid
            {
                return Err(StoreError::Rejected(
                    "list cursor belongs to a different store".into(),
                ));
            }
            let valid = self.connection.query_row(
                "SELECT EXISTS(SELECT 1 FROM jobs WHERE id = ?1 AND accepted_ms = ?2)",
                params![
                    cursor.job_id.entity_uuid().to_string(),
                    cursor.accepted_unix_millis
                ],
                |row| row.get::<_, bool>(0),
            )?;
            if !valid {
                return Err(StoreError::Rejected("invalid list cursor".into()));
            }
        }

        let mut scan = cursor;
        let mut selected = Vec::with_capacity(limit + 1);
        let mut exhausted = false;
        while selected.len() <= limit && !exhausted {
            let rows = self.scan_job_rows(scan, MAX_OBSERVATION_PAGE)?;
            exhausted = rows.len() < usize::try_from(MAX_OBSERVATION_PAGE).unwrap();
            if let Some(last) = rows.last() {
                scan = Some(JobListCursor {
                    store_uuid: self.store_uuid,
                    accepted_unix_millis: last.1,
                    job_id: last.0,
                });
            }
            for row in rows {
                if self.row_matches_selector(row.0, row.3.as_deref(), Some(&row.4), selector)? {
                    selected.push(row);
                    if selected.len() > limit {
                        break;
                    }
                }
            }
        }
        let has_more = selected.len() > limit;
        if has_more {
            selected.pop();
        }
        let next_cursor = if has_more {
            let last = selected.last().expect("a positive page limit has one row");
            Some(JobListCursor {
                store_uuid: self.store_uuid,
                accepted_unix_millis: last.1,
                job_id: last.0,
            })
        } else {
            None
        };
        let jobs = selected
            .into_iter()
            .map(|(job_id, _, _, _, _)| self.job_summary(job_id))
            .collect::<StoreResult<Vec<_>>>()?;
        Ok(JobListPage {
            jobs,
            next_cursor,
            event_cursor: self.event_head()?,
        })
    }

    pub(crate) fn observe(
        &self,
        selector: &JobSelector,
        cursor: Option<EventCursor>,
        limit: u32,
    ) -> StoreResult<ObservationFrame> {
        let head = self.event_head()?;
        let requested = cursor.unwrap_or(EventCursor {
            store_uuid: self.store_uuid,
            sequence: 0,
        });
        if requested.store_uuid != self.store_uuid {
            let snapshot = match selector {
                JobSelector::All | JobSelector::Labels { .. } => {
                    self.list_jobs(selector, None, MAX_OBSERVATION_PAGE)?
                }
                JobSelector::Jobs { .. } | JobSelector::Batch { .. } => JobListPage {
                    jobs: Vec::new(),
                    next_cursor: None,
                    event_cursor: head,
                },
            };
            return Ok(ObservationFrame::Gap {
                gap: EventGap {
                    requested,
                    oldest_available: self.oldest_event_cursor(head.sequence)?,
                },
                snapshot,
                cursor: head,
            });
        }
        self.validate_selector(selector)?;
        if requested.sequence > head.sequence {
            return Err(StoreError::Rejected(
                "event cursor is ahead of durable history".into(),
            ));
        }
        let oldest = self.oldest_event_cursor(head.sequence)?;
        if requested.sequence.saturating_add(1) < oldest.sequence {
            let snapshot = self.list_jobs(selector, None, MAX_OBSERVATION_PAGE)?;
            return Ok(ObservationFrame::Gap {
                gap: EventGap {
                    requested,
                    oldest_available: oldest,
                },
                snapshot,
                cursor: head,
            });
        }

        let wanted = usize::try_from(limit.clamp(1, MAX_OBSERVATION_PAGE)).unwrap_or(1);
        let mut events = Vec::with_capacity(wanted);
        let mut scanned = requested.sequence;
        if scanned < head.sequence {
            let mut statement = self.connection.prepare(
                "SELECT sequence, kind, job_id, batch_id, attempt_id, invocation_id,
                        transition, committed_ms FROM events
                 WHERE sequence > ?1 ORDER BY sequence LIMIT ?2",
            )?;
            let rows = statement
                .query_map(params![scanned, MAX_OBSERVATION_PAGE], |row| {
                    Ok((
                        row.get::<_, u64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, i64>(7)?,
                    ))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            if rows.is_empty() {
                scanned = head.sequence;
            } else {
                for (sequence, kind, job, batch, attempt, invocation, transition, committed) in rows
                {
                    scanned = sequence;
                    let job_id = JobId::from_parts(self.store_uuid, Uuid::parse_str(&job)?);
                    let spec_json = if matches!(selector, JobSelector::Labels { .. }) {
                        Some(self.connection.query_row(
                            "SELECT spec_json FROM jobs WHERE id = ?1",
                            [&job],
                            |row| row.get::<_, String>(0),
                        )?)
                    } else {
                        None
                    };
                    if !self.row_matches_selector(
                        job_id,
                        batch.as_deref(),
                        spec_json.as_deref(),
                        selector,
                    )? {
                        continue;
                    }
                    let kind = parse_scheduler_event_kind(&kind)?;
                    let (attempt_id, invocation_id, transition) = match kind {
                        SchedulerEventKind::InvocationChanged => {
                            let (Some(attempt), Some(invocation), Some(transition)) =
                                (attempt, invocation, transition)
                            else {
                                return Err(StoreError::InvalidState(
                                    "InvocationChanged event has incomplete transition identity"
                                        .into(),
                                ));
                            };
                            let identity_matches_job = self.connection.query_row(
                                "SELECT EXISTS(
                                     SELECT 1 FROM invocations
                                     JOIN attempts ON attempts.id = invocations.attempt_id
                                     WHERE invocations.id = ?1
                                       AND invocations.attempt_id = ?2
                                       AND attempts.job_id = ?3
                                 )",
                                params![&invocation, &attempt, &job],
                                |row| row.get::<_, bool>(0),
                            )?;
                            if !identity_matches_job {
                                return Err(StoreError::InvalidState(
                                    "InvocationChanged event identity does not belong to its Job"
                                        .into(),
                                ));
                            }
                            (
                                Some(AttemptId::from_parts(
                                    self.store_uuid,
                                    Uuid::parse_str(&attempt)?,
                                )),
                                Some(InvocationId::from_parts(
                                    self.store_uuid,
                                    Uuid::parse_str(&invocation)?,
                                )),
                                Some(parse_invocation_transition(&transition)?),
                            )
                        }
                        _ if attempt.is_none() && invocation.is_none() && transition.is_none() => {
                            (None, None, None)
                        }
                        _ => {
                            return Err(StoreError::InvalidState(
                                "non-Invocation event carries Invocation transition identity"
                                    .into(),
                            ));
                        }
                    };
                    events.push(SchedulerEvent {
                        cursor: EventCursor {
                            store_uuid: self.store_uuid,
                            sequence,
                        },
                        kind,
                        job_id,
                        batch_id: batch
                            .map(|value| {
                                Uuid::parse_str(&value)
                                    .map(|uuid| BatchId::from_parts(self.store_uuid, uuid))
                            })
                            .transpose()?,
                        attempt_id,
                        invocation_id,
                        transition,
                        committed_unix_millis: committed,
                    });
                    if events.len() == wanted {
                        break;
                    }
                }
            }
        }
        Ok(ObservationFrame::Events {
            events,
            cursor: EventCursor {
                store_uuid: self.store_uuid,
                sequence: scanned,
            },
        })
    }

    pub(super) fn event_head(&self) -> StoreResult<EventCursor> {
        let sequence = self.connection.query_row(
            "SELECT COALESCE(MAX(sequence), 0) FROM events",
            [],
            |row| row.get(0),
        )?;
        Ok(EventCursor {
            store_uuid: self.store_uuid,
            sequence,
        })
    }

    fn oldest_event_cursor(&self, head: u64) -> StoreResult<EventCursor> {
        let sequence = self.connection.query_row(
            "SELECT COALESCE(MIN(sequence), ?1 + 1) FROM events",
            [head],
            |row| row.get(0),
        )?;
        Ok(EventCursor {
            store_uuid: self.store_uuid,
            sequence,
        })
    }

    pub(super) fn validate_selector(&self, selector: &JobSelector) -> StoreResult<()> {
        match selector {
            JobSelector::All => Ok(()),
            JobSelector::Jobs { job_ids } => {
                if job_ids.is_empty() || job_ids.len() > crate::MAX_WAIT_STREAM_JOBS {
                    return Err(StoreError::Rejected(
                        "explicit Job selector must contain 1..=1024 IDs".into(),
                    ));
                }
                let mut seen = std::collections::BTreeSet::new();
                for job_id in job_ids {
                    if job_id.store_uuid() != self.store_uuid || !seen.insert(*job_id) {
                        return Err(StoreError::Rejected(
                            "explicit Job selector contains a foreign or duplicate ID".into(),
                        ));
                    }
                    let exists = self.connection.query_row(
                        "SELECT EXISTS(SELECT 1 FROM jobs WHERE id = ?1)",
                        [job_id.entity_uuid().to_string()],
                        |row| row.get::<_, bool>(0),
                    )?;
                    if !exists {
                        return Err(StoreError::NotFound(job_id.to_string()));
                    }
                }
                Ok(())
            }
            JobSelector::Batch { batch_id } => {
                if batch_id.store_uuid() != self.store_uuid {
                    return Err(StoreError::Rejected(
                        "Batch selector belongs to a different store".into(),
                    ));
                }
                let exists = self.connection.query_row(
                    "SELECT EXISTS(SELECT 1 FROM batches WHERE id = ?1)",
                    [batch_id.entity_uuid().to_string()],
                    |row| row.get::<_, bool>(0),
                )?;
                if exists {
                    Ok(())
                } else {
                    Err(StoreError::NotFound(batch_id.to_string()))
                }
            }
            JobSelector::Labels { labels } => {
                if labels.is_empty() || labels.len() > 32 {
                    return Err(StoreError::Rejected(
                        "label selector must contain 1..=32 labels".into(),
                    ));
                }
                let mut seen = std::collections::BTreeSet::new();
                for label in labels {
                    if label.key.is_empty()
                        || label.value.is_empty()
                        || label.key.contains(['\0', '='])
                        || label.value.contains('\0')
                        || !seen.insert((label.key.as_str(), label.value.as_str()))
                    {
                        return Err(StoreError::Rejected(
                            "label selector contains an invalid or duplicate label".into(),
                        ));
                    }
                }
                Ok(())
            }
        }
    }

    fn row_matches_selector(
        &self,
        job_id: JobId,
        batch_id: Option<&str>,
        spec_json: Option<&str>,
        selector: &JobSelector,
    ) -> StoreResult<bool> {
        Ok(match selector {
            JobSelector::All => true,
            JobSelector::Jobs { job_ids } => job_ids.contains(&job_id),
            JobSelector::Batch { batch_id: selected } => batch_id
                .map(Uuid::parse_str)
                .transpose()?
                .is_some_and(|batch| batch == selected.entity_uuid()),
            JobSelector::Labels { labels } => {
                let spec: JobSpec = serde_json::from_str(spec_json.ok_or_else(|| {
                    StoreError::InvalidState("label match is missing retained Job spec".into())
                })?)?;
                labels.iter().all(|label| spec.labels.contains(label))
            }
        })
    }

    #[allow(clippy::type_complexity)]
    fn scan_job_rows(
        &self,
        cursor: Option<JobListCursor>,
        limit: u32,
    ) -> StoreResult<Vec<(JobId, i64, String, Option<String>, String)>> {
        let sql = if cursor.is_some() {
            "SELECT id, accepted_ms, state, batch_id, spec_json FROM jobs
             WHERE accepted_ms < ?1 OR (accepted_ms = ?1 AND id < ?2)
             ORDER BY accepted_ms DESC, id DESC LIMIT ?3"
        } else {
            "SELECT id, accepted_ms, state, batch_id, spec_json FROM jobs
             ORDER BY accepted_ms DESC, id DESC LIMIT ?1"
        };
        let mut statement = self.connection.prepare(sql)?;
        let map = |row: &rusqlite::Row<'_>| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, String>(4)?,
            ))
        };
        let rows = if let Some(cursor) = cursor {
            statement
                .query_map(
                    params![
                        cursor.accepted_unix_millis,
                        cursor.job_id.entity_uuid().to_string(),
                        limit
                    ],
                    map,
                )?
                .collect::<std::result::Result<Vec<_>, _>>()?
        } else {
            statement
                .query_map([limit], map)?
                .collect::<std::result::Result<Vec<_>, _>>()?
        };
        rows.into_iter()
            .map(|(job, accepted, state, batch, spec)| {
                Ok((
                    JobId::from_parts(self.store_uuid, Uuid::parse_str(&job)?),
                    accepted,
                    state,
                    batch,
                    spec,
                ))
            })
            .collect()
    }

    pub(super) fn job_summary(&self, job_id: JobId) -> StoreResult<JobSummary> {
        let (
            state,
            outcome,
            accepted,
            started,
            finished,
            spec_json,
            batch,
            batch_member,
            attempt,
            invocation,
            stdout,
            stderr,
        ) = self.connection.query_row(
            "SELECT state, outcome, accepted_ms, started_ms, finished_ms, spec_json,
                    batch_id, batch_member, attempt_id, invocation_id, stdout_len, stderr_len
             FROM jobs WHERE id = ?1",
            [self.local_id(job_id)?],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, Option<String>>(8)?,
                    row.get::<_, Option<String>>(9)?,
                    row.get::<_, u64>(10)?,
                    row.get::<_, u64>(11)?,
                ))
            },
        )?;
        let state = parse_job_state(&state)?;
        let blockers = if state == JobState::Pending {
            self.blockers_for_job(job_id)?
        } else {
            Vec::new()
        };
        let queue_rank = if state == JobState::Pending {
            Some(self.connection.query_row(
                "SELECT COUNT(*) FROM jobs WHERE state = 'pending' AND rowid <=
                    (SELECT rowid FROM jobs WHERE id = ?1)",
                [job_id.entity_uuid().to_string()],
                |row| row.get(0),
            )?)
        } else {
            None
        };
        let estimate = if state == JobState::Pending {
            self.estimate_for_job(job_id, &blockers)?
        } else {
            Estimate::unknown("Job is no longer pending")
        };
        let spec: JobSpec = serde_json::from_str(&spec_json)?;
        Ok(JobSummary {
            job_id,
            command_preview: command_preview(&spec, 160),
            batch_id: batch
                .map(|value| {
                    Uuid::parse_str(&value).map(|uuid| BatchId::from_parts(self.store_uuid, uuid))
                })
                .transpose()?,
            batch_member,
            parent: self.parent_for_job(job_id)?,
            state,
            outcome: outcome.map(|value| parse_outcome(&value)).transpose()?,
            accepted_unix_millis: accepted,
            started_unix_millis: started,
            finished_unix_millis: finished,
            queue_rank,
            estimate,
            claims: spec.resources,
            labels: spec.labels,
            blocker: blockers.into_iter().next(),
            attempt_id: attempt
                .map(|value| {
                    Uuid::parse_str(&value).map(|uuid| AttemptId::from_parts(self.store_uuid, uuid))
                })
                .transpose()?,
            invocation_id: invocation
                .map(|value| {
                    Uuid::parse_str(&value)
                        .map(|uuid| InvocationId::from_parts(self.store_uuid, uuid))
                })
                .transpose()?,
            stdout_committed: stdout,
            stderr_committed: stderr,
        })
    }

    fn attempt_snapshots(
        &self,
        job_id: JobId,
        observations: &ReconciliationObservations,
    ) -> StoreResult<Vec<AttemptSnapshot>> {
        let mut statement = self.connection.prepare(
            "SELECT attempts.id, attempts.attempt_index, attempts.verdict,
                    attempts.safety_reason, attempts.created_ms, attempts.started_ms,
                    attempts.deadline_ms, attempts.finished_ms,
                    invocations.id, invocations.role, invocations.role_index, invocations.state,
                    invocations.root_pid, invocations.root_host_id, invocations.root_boot_id,
                    invocations.root_creation_filetime_100ns, invocations.root_exit_code,
                    invocations.exit_classification, invocations.executable_hash,
                    invocations.daemon_generation, invocations.started_ms,
                    invocations.finished_ms, invocations.stdout_tail, invocations.stderr_tail,
                    containments.id, containments.state, containments.strength,
                    containments.incident_sequence, containments.reason_code, containments.detail,
                    containments.opened_ms, containments.retained_claims_json,
                    containments.resolution, containments.resolved_ms,
                    containments.last_reconciliation, containments.resolution_audit_json
             FROM attempts
             LEFT JOIN invocations ON invocations.attempt_id = attempts.id
             LEFT JOIN containments ON containments.invocation_id = invocations.id
             WHERE attempts.job_id = ?1
             ORDER BY attempts.attempt_index, invocations.role_index, invocations.rowid",
        )?;
        let rows = statement.query_map([self.local_id(job_id)?], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, u32>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, Option<i64>>(5)?,
                row.get::<_, Option<i64>>(6)?,
                row.get::<_, Option<i64>>(7)?,
                row.get::<_, Option<String>>(8)?,
                row.get::<_, Option<String>>(9)?,
                row.get::<_, Option<u32>>(10)?,
                row.get::<_, Option<String>>(11)?,
                row.get::<_, Option<u32>>(12)?,
                row.get::<_, Option<String>>(13)?,
                row.get::<_, Option<String>>(14)?,
                row.get::<_, Option<i64>>(15)?,
                row.get::<_, Option<i32>>(16)?,
                row.get::<_, Option<String>>(17)?,
                row.get::<_, Option<String>>(18)?,
                row.get::<_, Option<String>>(19)?,
                row.get::<_, Option<i64>>(20)?,
                row.get::<_, Option<i64>>(21)?,
                row.get::<_, Option<String>>(22)?,
                row.get::<_, Option<String>>(23)?,
                row.get::<_, Option<String>>(24)?,
                row.get::<_, Option<String>>(25)?,
                row.get::<_, Option<String>>(26)?,
                row.get::<_, Option<u64>>(27)?,
                row.get::<_, Option<String>>(28)?,
                row.get::<_, Option<String>>(29)?,
                row.get::<_, Option<i64>>(30)?,
                row.get::<_, Option<String>>(31)?,
                row.get::<_, Option<String>>(32)?,
                row.get::<_, Option<i64>>(33)?,
                row.get::<_, Option<String>>(34)?,
                row.get::<_, Option<String>>(35)?,
            ))
        })?;
        let mut attempts = Vec::<AttemptSnapshot>::new();
        for row in rows {
            let (
                attempt,
                attempt_index,
                verdict,
                safety_reason,
                attempt_created,
                attempt_started,
                attempt_deadline,
                attempt_finished,
                invocation,
                role,
                role_index,
                invocation_state,
                root_pid,
                root_host_id,
                root_boot_id,
                root_creation,
                root_exit_code,
                exit_classification,
                executable_hash,
                daemon_generation,
                started,
                finished,
                stdout_tail,
                stderr_tail,
                containment,
                containment_state,
                containment_strength,
                incident_sequence,
                incident_reason,
                incident_detail,
                incident_opened,
                retained_claims,
                resolution,
                resolved,
                last_reconciliation,
                resolution_audit,
            ) = row?;
            if attempts
                .last()
                .is_none_or(|current| current.attempt_id.entity_uuid().to_string() != attempt)
            {
                attempts.push(AttemptSnapshot {
                    attempt_id: AttemptId::from_parts(self.store_uuid, Uuid::parse_str(&attempt)?),
                    attempt_index,
                    verdict: verdict.as_deref().map(parse_attempt_verdict).transpose()?,
                    reason_code: safety_reason,
                    created_unix_millis: attempt_created,
                    started_unix_millis: attempt_started,
                    deadline_unix_millis: attempt_deadline,
                    finished_unix_millis: attempt_finished,
                    primary_result: self
                        .connection
                        .query_row(
                            "SELECT primary_result_json FROM attempts WHERE id = ?1",
                            [&attempt],
                            |row| row.get::<_, Option<String>>(0),
                        )?
                        .as_deref()
                        .map(serde_json::from_str)
                        .transpose()?,
                    admission: self.admission_for_attempt(AttemptId::from_parts(
                        self.store_uuid,
                        Uuid::parse_str(&attempt)?,
                    ))?,
                    invocations: Vec::new(),
                });
            }
            let (
                Some(invocation),
                Some(role),
                Some(role_index),
                Some(invocation_state),
                Some(containment),
                Some(containment_state),
            ) = (
                invocation,
                role,
                role_index,
                invocation_state,
                containment,
                containment_state,
            )
            else {
                continue;
            };
            let containment_id =
                ContainmentId::from_parts(self.store_uuid, Uuid::parse_str(&containment)?);
            let containment_state = parse_containment_state(&containment_state)?;
            let observed_reconciliation = observations.get(&containment_id).cloned();
            attempts
                .last_mut()
                .expect("attempt inserted above")
                .invocations
                .push(InvocationSnapshot {
                    invocation_id: InvocationId::from_parts(
                        self.store_uuid,
                        Uuid::parse_str(&invocation)?,
                    ),
                    role: parse_invocation_role(&role)?,
                    role_index,
                    state: parse_invocation_state(&invocation_state)?,
                    root_pid,
                    root_identity: process_identity_from_columns(
                        root_pid,
                        root_host_id.clone(),
                        root_boot_id.clone(),
                        root_creation,
                    )?,
                    root_exit_code,
                    exit_classification: exit_classification
                        .as_deref()
                        .map(parse_exit_classification)
                        .transpose()?,
                    executable_hash,
                    daemon_generation: daemon_generation
                        .map(|value| Uuid::parse_str(&value))
                        .transpose()?,
                    started_unix_millis: started,
                    finished_unix_millis: finished,
                    containment: ContainmentSnapshot {
                        containment_id,
                        state: containment_state.clone(),
                        strength: containment_strength.unwrap_or_else(|| "unknown".into()),
                        incident_id: incident_sequence.map(|_| containment_id),
                        incident: incident_sequence
                            .map(|incident_sequence| {
                                Ok::<_, StoreError>(ContainmentIncidentSnapshot {
                                    incident_id: containment_id,
                                    incident_sequence,
                                    containment_id,
                                    job_id,
                                    attempt_id: AttemptId::from_parts(
                                        self.store_uuid,
                                        Uuid::parse_str(&attempt)?,
                                    ),
                                    invocation_id: InvocationId::from_parts(
                                        self.store_uuid,
                                        Uuid::parse_str(&invocation)?,
                                    ),
                                    state: containment_state.clone(),
                                    reason_code: incident_reason
                                        .clone()
                                        .unwrap_or_else(|| "unknown".into()),
                                    detail: incident_detail.clone().unwrap_or_default(),
                                    opened_unix_millis: incident_opened.unwrap_or(0),
                                    last_reconciled_unix_millis: observed_reconciliation
                                        .as_ref()
                                        .map(|(observed, _)| *observed),
                                    last_reconciliation: match &observed_reconciliation {
                                        Some((_, result)) => Some(result.clone()),
                                        None => last_reconciliation
                                            .as_deref()
                                            .map(parse_reconciliation_result)
                                            .transpose()?,
                                    },
                                    root_identity: process_identity_from_columns(
                                        root_pid,
                                        root_host_id.clone(),
                                        root_boot_id.clone(),
                                        root_creation,
                                    )?,
                                    retained_claims: retained_claims
                                        .as_deref()
                                        .map(serde_json::from_str)
                                        .transpose()?
                                        .unwrap_or_default(),
                                    resolution: resolution
                                        .as_deref()
                                        .map(parse_containment_resolution)
                                        .transpose()?,
                                    resolved_unix_millis: resolved,
                                })
                            })
                            .transpose()?,
                        resolution: resolution
                            .as_deref()
                            .map(parse_containment_resolution)
                            .transpose()?,
                        resolution_audit: resolution_audit
                            .as_deref()
                            .map(serde_json::from_str)
                            .transpose()?,
                    },
                    stdout_tail: stdout_tail.unwrap_or_default(),
                    stderr_tail: stderr_tail.unwrap_or_default(),
                });
        }
        bound_snapshot_diagnostics(&mut attempts);
        Ok(attempts)
    }

    pub(crate) fn validate_managed_wait(
        &self,
        scope: SubmissionScope,
        targets: &[JobId],
    ) -> StoreResult<()> {
        validate_managed_wait_targets(
            &self.connection,
            self.store_uuid,
            self.daemon_generation,
            &self.capacities,
            &self.impact_incompatibilities,
            scope,
            targets,
        )
    }

    pub(crate) fn logs(
        &self,
        job_id: JobId,
        stream: LogStream,
        offset: u64,
        limit: u32,
    ) -> StoreResult<LogChunk> {
        let (committed, state, containment): (u64, String, String) = match stream {
            LogStream::Stdout => self.connection.query_row(
                "SELECT stdout_len, state, COALESCE((
                    SELECT state FROM containments WHERE id = jobs.containment_id
                 ), 'empty') FROM jobs WHERE id = ?1",
                [self.local_id(job_id)?],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?,
            LogStream::Stderr => self.connection.query_row(
                "SELECT stderr_len, state, COALESCE((
                    SELECT state FROM containments WHERE id = jobs.containment_id
                 ), 'empty') FROM jobs WHERE id = ?1",
                [self.local_id(job_id)?],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?,
        };
        let path = match stream {
            LogStream::Stdout => self.paths.stdout_path(job_id),
            LogStream::Stderr => self.paths.stderr_path(job_id),
        };
        if offset > committed {
            return Ok(LogChunk {
                job_id,
                stream,
                offset,
                bytes: Vec::new(),
                next_offset: committed,
                eof: state == "final" && containment != "uncertain",
                gap: Some(format!(
                    "requested offset {offset} exceeds committed offset {committed}"
                )),
            });
        }
        let available = committed.saturating_sub(offset);
        let length = available.min(u64::from(limit.min(1024 * 1024))) as usize;
        let mut bytes = vec![0_u8; length];
        if length > 0 {
            let read = File::open(&path).and_then(|mut file| {
                file.seek(SeekFrom::Start(offset))?;
                file.read_exact(&mut bytes)
            });
            if let Err(error) = read {
                return Ok(LogChunk {
                    job_id,
                    stream,
                    offset,
                    bytes: Vec::new(),
                    next_offset: offset,
                    eof: false,
                    gap: Some(format!(
                        "committed range {offset}..{} is unavailable: {error}",
                        offset + length as u64
                    )),
                });
            }
        }
        let next_offset = offset + bytes.len() as u64;
        Ok(LogChunk {
            job_id,
            stream,
            offset,
            bytes,
            next_offset,
            eof: state == "final" && containment != "uncertain" && next_offset == committed,
            gap: None,
        })
    }

    pub(crate) fn daemon_status(&self, endpoint: &str) -> StoreResult<DaemonSnapshot> {
        let queued_jobs = self.connection.query_row(
            "SELECT COUNT(*) FROM jobs WHERE state = 'pending'",
            [],
            |row| row.get(0),
        )?;
        let running_jobs = self.connection.query_row(
            "SELECT COUNT(*) FROM jobs WHERE state = 'active'",
            [],
            |row| row.get(0),
        )?;
        Ok(DaemonSnapshot {
            store_uuid: self.store_uuid,
            daemon_generation: self.daemon_generation,
            version: env!("CARGO_PKG_VERSION").to_owned(),
            pid: std::process::id(),
            process_identity: self.startup_identity.daemon_process.clone(),
            endpoint: endpoint.to_owned(),
            store_path: self.paths.root.clone(),
            config_path: self.paths.config.clone(),
            capacities: self.capacities.clone(),
            resources: Some(self.resource_snapshot()?),
            config_sha256: self.config_sha256.clone(),
            queued_jobs,
            running_jobs,
        })
    }

    fn resource_snapshot(&self) -> StoreResult<crate::ResourceSnapshot> {
        let (granted, reserved) = self.granted_and_reserved_claims(None)?;
        let scalar = |name: &str,
                      capacity: u64,
                      granted_values: &dyn Fn(&ResolvedClaims) -> u64,
                      reserved_values: &dyn Fn(&ResolvedClaims) -> u64|
         -> StoreResult<crate::ScalarResourceSnapshot> {
            Ok(crate::ScalarResourceSnapshot {
                capacity,
                granted: checked_resource_total(
                    name,
                    granted.iter().map(granted_values),
                    "granted",
                )?,
                reserved: checked_resource_total(
                    name,
                    reserved.iter().map(reserved_values),
                    "reserved",
                )?,
            })
        };
        let mut custom = std::collections::BTreeMap::new();
        for (configured_name, capacity) in &self.capacities.custom {
            let name = crate::spec::canonical_custom_resource_name(configured_name)
                .map_err(|error| StoreError::InvalidState(error.to_string()))?;
            let granted_name = name.clone();
            let reserved_name = name.clone();
            custom.insert(
                name.clone(),
                scalar(
                    &name,
                    *capacity,
                    &|claims| claims.custom.get(&granted_name).copied().unwrap_or(0),
                    &|claims| claims.custom.get(&reserved_name).copied().unwrap_or(0),
                )?,
            );
        }
        Ok(crate::ResourceSnapshot {
            cpu_units: scalar(
                "cpu_units",
                u64::from(self.capacities.cpu_units),
                &|claims| claims.cpu_units,
                &|claims| claims.cpu_units,
            )?,
            ram_mb: scalar(
                "ram_mb",
                self.capacities.ram_mb,
                &|claims| claims.ram_mb,
                &|claims| claims.ram_mb,
            )?,
            cargo_slots: scalar(
                "cargo_slots",
                u64::from(self.capacities.cargo_slots),
                &|claims| claims.cargo_slots,
                &|claims| claims.cargo_slots,
            )?,
            gpu_slots: scalar(
                "gpu_slots",
                u64::from(self.capacities.gpu_slots),
                &|claims| claims.gpu_slots,
                &|claims| claims.gpu_slots,
            )?,
            custom,
        })
    }

    #[cfg(test)]
    pub(crate) fn doctor(
        &self,
        endpoint: &str,
        cursor: Option<ContainmentIncidentCursor>,
        limit: Option<u32>,
        cache: &mut DoctorSnapshotCache,
    ) -> StoreResult<DoctorSnapshot> {
        let page_limit = limit
            .unwrap_or(crate::MAX_DOCTOR_PAGE)
            .clamp(1, crate::MAX_DOCTOR_PAGE) as usize;
        let incident_page = match cursor {
            Some(cursor) => cache.next(cursor, page_limit)?,
            None => cache.begin(
                self.capture_doctor_incidents(&ReconciliationObservations::default())?,
                page_limit,
            )?,
        };
        self.doctor_with_incident_page(endpoint, incident_page)
    }

    pub(crate) fn capture_doctor_incidents(
        &self,
        observations: &ReconciliationObservations,
    ) -> StoreResult<CapturedDoctorInventory> {
        let transaction = self.connection.unchecked_transaction()?;
        let total_unresolved: u64 = transaction.query_row(
            "SELECT COUNT(*) FROM containments WHERE state = 'uncertain'",
            [],
            |row| row.get(0),
        )?;
        if total_unresolved > MAX_COMPLETE_DOCTOR_INCIDENTS {
            return Err(StoreError::DoctorIncidentLimit);
        }
        let mut statement = transaction.prepare(
            "SELECT containments.id, containments.incident_sequence, jobs.id, attempts.id,
                    invocations.id, containments.state, containments.reason_code,
                    containments.detail, containments.opened_ms,
                    containments.last_reconciliation, invocations.root_pid,
                    invocations.root_host_id, invocations.root_boot_id,
                    invocations.root_creation_filetime_100ns,
                    containments.retained_claims_json, containments.resolution,
                    containments.resolved_ms
             FROM containments
             JOIN invocations ON invocations.id = containments.invocation_id
             JOIN attempts ON attempts.id = invocations.attempt_id
             JOIN jobs ON jobs.id = attempts.job_id
             WHERE containments.state = 'uncertain'
             ORDER BY containments.incident_sequence, containments.id",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, u64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, Option<String>>(7)?,
                row.get::<_, Option<i64>>(8)?,
                row.get::<_, Option<String>>(9)?,
                row.get::<_, Option<u32>>(10)?,
                row.get::<_, Option<String>>(11)?,
                row.get::<_, Option<String>>(12)?,
                row.get::<_, Option<i64>>(13)?,
                row.get::<_, Option<String>>(14)?,
                row.get::<_, Option<String>>(15)?,
                row.get::<_, Option<i64>>(16)?,
            ))
        })?;
        let mut inventory = Vec::with_capacity(total_unresolved as usize);
        let mut serialized_bytes = 0_u64;
        for row in rows {
            let (
                containment,
                sequence,
                job,
                attempt,
                invocation,
                state,
                reason,
                detail,
                opened,
                last_reconciliation,
                root_pid,
                root_host,
                root_boot,
                root_creation,
                retained_claims,
                resolution,
                resolved,
            ) = row?;
            let containment_id =
                ContainmentId::from_parts(self.store_uuid, Uuid::parse_str(&containment)?);
            let observed_reconciliation = observations.get(&containment_id).cloned();
            let incident = ContainmentIncidentSnapshot {
                incident_id: containment_id,
                incident_sequence: sequence,
                containment_id,
                job_id: JobId::from_parts(self.store_uuid, Uuid::parse_str(&job)?),
                attempt_id: AttemptId::from_parts(self.store_uuid, Uuid::parse_str(&attempt)?),
                invocation_id: InvocationId::from_parts(
                    self.store_uuid,
                    Uuid::parse_str(&invocation)?,
                ),
                state: parse_containment_state(&state)?,
                reason_code: bounded_doctor_code(reason.unwrap_or_else(|| "unknown".into())),
                detail: bounded_doctor_text(detail.unwrap_or_default(), DOCTOR_DETAIL_MAX_BYTES),
                opened_unix_millis: opened.unwrap_or(0),
                last_reconciled_unix_millis: observed_reconciliation
                    .as_ref()
                    .map(|(observed, _)| *observed),
                last_reconciliation: match observed_reconciliation {
                    Some((_, result)) => Some(result),
                    None => last_reconciliation
                        .as_deref()
                        .map(parse_reconciliation_result)
                        .transpose()?,
                },
                root_identity: process_identity_from_columns(
                    root_pid,
                    root_host,
                    root_boot,
                    root_creation,
                )?,
                retained_claims: retained_claims
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()?
                    .unwrap_or_default(),
                resolution: resolution
                    .as_deref()
                    .map(parse_containment_resolution)
                    .transpose()?,
                resolved_unix_millis: resolved,
            };
            let json = serde_json::to_vec(&incident)?;
            serialized_bytes = serialized_bytes
                .checked_add(json.len() as u64)
                .ok_or(StoreError::DoctorMemoryLimit)?;
            if serialized_bytes > MAX_COMPLETE_DOCTOR_BYTES {
                return Err(StoreError::DoctorMemoryLimit);
            }
            inventory.push(incident);
        }
        if inventory.len() as u64 != total_unresolved {
            return Err(StoreError::InvalidState(
                "doctor inventory changed while creating its snapshot".into(),
            ));
        }
        drop(statement);
        transaction.commit()?;

        Ok(CapturedDoctorInventory {
            incidents: inventory,
            serialized_bytes,
        })
    }

    pub(crate) fn doctor_with_incident_page(
        &self,
        endpoint: &str,
        incident_page: DoctorIncidentPage,
    ) -> StoreResult<DoctorSnapshot> {
        let total_unresolved = incident_page.total_unresolved;
        let journal_mode: String = self
            .connection
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
        let synchronous: i64 = self
            .connection
            .query_row("PRAGMA synchronous", [], |row| row.get(0))?;
        let foreign_keys_enabled: bool =
            self.connection
                .query_row("PRAGMA foreign_keys", [], |row| row.get(0))?;
        let sqlite_ok =
            journal_mode.eq_ignore_ascii_case("wal") && synchronous == 2 && foreign_keys_enabled;
        let host_matches = self.startup_identity.host_id.is_some()
            && self.startup_identity.host_id == self.bound_host_id;
        let mut checks = vec![
            DoctorCheck {
                code: "configuration.loaded".into(),
                status: DoctorCheckStatus::Pass,
                summary: "loaded configuration evidence is available".into(),
                remediation: None,
            },
            DoctorCheck {
                code: "containment.unresolved".into(),
                status: if total_unresolved == 0 {
                    DoctorCheckStatus::Pass
                } else {
                    DoctorCheckStatus::Warning
                },
                summary: format!("{total_unresolved} unresolved containment incident(s)"),
                remediation: (total_unresolved > 0).then(|| {
                    "allow reconciliation to finish or explicitly review force-clear risk".into()
                }),
            },
            DoctorCheck {
                code: "containment.windows_job_object".into(),
                status: if self.startup_identity.capable() {
                    DoctorCheckStatus::Pass
                } else {
                    DoctorCheckStatus::Fail
                },
                summary: "born-contained Windows Job Object capability".into(),
                remediation: (!self.startup_identity.capable())
                    .then(|| self.startup_identity.failures.join("; ")),
            },
            DoctorCheck {
                code: "host.boot_identity".into(),
                status: if self.startup_identity.boot_id.is_some() {
                    DoctorCheckStatus::Pass
                } else {
                    DoctorCheckStatus::Fail
                },
                summary: "startup-latched boot identity".into(),
                remediation: None,
            },
            DoctorCheck {
                code: "host.machine_identity".into(),
                status: if host_matches {
                    DoctorCheckStatus::Pass
                } else {
                    DoctorCheckStatus::Fail
                },
                summary: "current host matches the durable store binding".into(),
                remediation: None,
            },
            DoctorCheck {
                code: "host.session_survival".into(),
                status: DoctorCheckStatus::Pass,
                summary: "detached per-user daemon session is active".into(),
                remediation: None,
            },
            DoctorCheck {
                code: "ipc.owner_only".into(),
                status: DoctorCheckStatus::Pass,
                summary: "named pipe is owner-only and rejects remote clients".into(),
                remediation: None,
            },
            DoctorCheck {
                code: "store.filesystem".into(),
                status: DoctorCheckStatus::Pass,
                summary: "store path was validated as local fixed NTFS".into(),
                remediation: None,
            },
            DoctorCheck {
                code: "store.schema".into(),
                status: DoctorCheckStatus::Pass,
                summary: format!("validated schema epoch {STORE_SCHEMA_EPOCH}"),
                remediation: None,
            },
            DoctorCheck {
                code: "store.sqlite_durability".into(),
                status: if sqlite_ok {
                    DoctorCheckStatus::Pass
                } else {
                    DoctorCheckStatus::Fail
                },
                summary: format!(
                    "journal={journal_mode}, synchronous={synchronous}, foreign_keys={foreign_keys_enabled}"
                ),
                remediation: None,
            },
        ];
        for check in &mut checks {
            check.code = bounded_doctor_code(std::mem::take(&mut check.code));
            check.summary =
                bounded_doctor_text(std::mem::take(&mut check.summary), DOCTOR_SUMMARY_MAX_BYTES);
            if let Some(remediation) = check.remediation.take() {
                check.remediation = Some(bounded_doctor_text(remediation, DOCTOR_DETAIL_MAX_BYTES));
            }
        }
        checks.sort_by(|left, right| left.code.cmp(&right.code));
        let overall = if checks
            .iter()
            .any(|check| check.status == DoctorCheckStatus::Fail)
        {
            DoctorOverallStatus::Unsafe
        } else if checks.iter().any(|check| {
            matches!(
                check.status,
                DoctorCheckStatus::Warning | DoctorCheckStatus::Unknown(_)
            )
        }) {
            DoctorOverallStatus::AttentionRequired
        } else {
            DoctorOverallStatus::Healthy
        };
        Ok(DoctorSnapshot {
            schema_version: 1,
            observed_unix_millis: now_millis(),
            overall,
            daemon: self.daemon_status(endpoint)?,
            host: DoctorHostSnapshot {
                platform: std::env::consts::OS.into(),
                host_name: std::env::var("COMPUTERNAME").ok(),
                host_id: self.startup_identity.host_id.clone(),
                boot_id: self.startup_identity.boot_id.clone(),
                containment_strength: "windows_job_object".into(),
                session_survival: DoctorCheckStatus::Pass,
            },
            store: DoctorStoreSnapshot {
                store_uuid: self.store_uuid,
                schema_epoch: STORE_SCHEMA_EPOCH.into(),
                bound_host_id: self.bound_host_id.clone(),
                filesystem: "local_fixed_ntfs".into(),
                sqlite_journal_mode: journal_mode,
                sqlite_synchronous: if synchronous == 2 {
                    "full".into()
                } else {
                    synchronous.to_string()
                },
                foreign_keys_enabled,
            },
            checks,
            coverage: Vec::new(),
            incidents: incident_page,
            boundaries: vec![
                DoctorBoundary {
                    code: "cloned_host_identity".into(),
                    statement: "simultaneous machine clones with the same MachineGuid cannot be distinguished".into(),
                },
                DoctorBoundary {
                    code: "no_hard_resource_partition".into(),
                    statement: "resource admission is not CPU, GPU, or RAM hard enforcement".into(),
                },
                DoctorBoundary {
                    code: "physical_power_loss_after_ack".into(),
                    statement: "physical power loss may exceed the acknowledged storage boundary".into(),
                },
                DoctorBoundary {
                    code: "same_owner_out_of_boundary_process".into(),
                    statement: "deliberate same-owner out-of-boundary process creation is outside the cooperative guarantee".into(),
                },
            ],
        })
    }
}

fn checked_resource_total(
    name: &str,
    mut values: impl Iterator<Item = u64>,
    accounting: &str,
) -> StoreResult<u64> {
    values.try_fold(0_u64, |total, value| {
        total.checked_add(value).ok_or_else(|| {
            StoreError::InvalidState(format!("{name} {accounting} resource accounting overflow"))
        })
    })
}

#[cfg(test)]
mod command_preview_tests {
    use super::command_preview_from_parts;

    #[test]
    fn preview_is_bounded_single_line_and_identifies_the_command() {
        let preview = command_preview_from_parts(
            std::path::Path::new(r"C:\tools\review runner.exe"),
            &["audit".into(), "two words".into(), "line\nbreak".into()],
            48,
        );
        assert_eq!(
            preview,
            r#""review runner.exe" audit "two words" "line\nbr…"#
        );
        assert_eq!(preview.chars().count(), 48);
        assert!(!preview.contains('\n'));
    }
}
