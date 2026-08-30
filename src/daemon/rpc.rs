use super::*;
use crate::protocol::error_code;

fn open_read_view(store: &SharedStore) -> std::result::Result<Store, StoreError> {
    store
        .lock()
        .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?
        .open_read_view()
}

pub(super) fn handle_request(
    store: &SharedStore,
    scheduler: &DaemonReactor,
    peer: Option<&PeerProcess>,
    request: Request,
) -> Response {
    let result = match request {
        Request::Ping {} => {
            return Response::Pong {
                protocol_version: PROTOCOL_VERSION,
            };
        }
        Request::StageBegin {
            upload_id,
            expected_sha256,
            expected_length,
        } => store
            .lock()
            .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))
            .and_then(|store| store.stage_begin(upload_id, &expected_sha256, expected_length))
            .map(|next_offset| Response::StageReady { next_offset }),
        Request::StageChunk {
            upload_id,
            offset,
            bytes,
        } => store
            .lock()
            .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))
            .and_then(|store| store.stage_chunk(upload_id, offset, &bytes))
            .map(|next_offset| Response::StageReady { next_offset }),
        Request::StageCommit { upload_id } => store
            .lock()
            .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))
            .and_then(|store| store.stage_commit(upload_id))
            .map(|input| Response::StageCommitted { input }),
        Request::SubmissionContext { claimed_parent } => {
            submission_context(store, &scheduler.live_containments, peer).and_then(|context| {
                if claimed_parent.is_some() && claimed_parent != context.parent {
                    return Err(StoreError::Rejected(
                        "claimed managed parent does not match daemon-held OS containment".into(),
                    ));
                }
                Ok(Response::SubmissionContext(context))
            })
        }
        Request::Submit {
            idempotency_key,
            payload_hash,
            spec,
            stdin,
            expected_store_uuid,
            expected_parent,
            wait_for_completion,
        } => submission_context(store, &scheduler.live_containments, peer).and_then(|context| {
            if context.parent != expected_parent {
                return Err(StoreError::Rejected(
                    "submission parent changed after client preflight".into(),
                ));
            }
            store
                .lock()
                .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))
                .and_then(|mut store| {
                    if expected_store_uuid.is_some_and(|expected| expected != store.store_uuid()) {
                        return Err(StoreError::InvalidState(
                            "store identity changed during submission".into(),
                        ));
                    }
                    let scope = context
                        .parent
                        .map_or(SubmissionScope::Unmanaged, SubmissionScope::Managed);
                    match store.submit_with_stdin_scoped_for_wait(
                        scope,
                        idempotency_key,
                        &payload_hash,
                        &spec,
                        stdin.as_ref(),
                        wait_for_completion,
                    ) {
                        Ok(submitted) => {
                            if submitted.should_schedule {
                                scheduler.wake();
                            }
                            Ok(Response::Submitted(submitted.receipt))
                        }
                        Err(error) => retained_submission_rejection(
                            &store,
                            scope,
                            idempotency_key,
                            &payload_hash,
                            error,
                        ),
                    }
                })
        }),
        Request::SubmitBatch {
            idempotency_key,
            payload_hash,
            spec,
            stdins,
            expected_store_uuid,
            expected_parent,
            wait_for_completion,
        } => submission_context(store, &scheduler.live_containments, peer).and_then(|context| {
            if context.parent != expected_parent {
                return Err(StoreError::Rejected(
                    "submission parent changed after client preflight".into(),
                ));
            }
            store
                .lock()
                .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))
                .and_then(|mut store| {
                    if expected_store_uuid.is_some_and(|expected| expected != store.store_uuid()) {
                        return Err(StoreError::InvalidState(
                            "store identity changed during submission".into(),
                        ));
                    }
                    let scope = context
                        .parent
                        .map_or(SubmissionScope::Unmanaged, SubmissionScope::Managed);
                    match store.submit_batch_with_stdins_scoped_for_wait(
                        scope,
                        idempotency_key,
                        &payload_hash,
                        &spec,
                        &stdins,
                        wait_for_completion,
                    ) {
                        Ok(submitted) => {
                            if submitted.should_schedule {
                                scheduler.wake();
                            }
                            Ok(Response::BatchSubmitted(submitted.receipt))
                        }
                        Err(error) => retained_submission_rejection(
                            &store,
                            scope,
                            idempotency_key,
                            &payload_hash,
                            error,
                        ),
                    }
                })
        }),
        Request::Recover {
            idempotency_key,
            payload_hash,
            expected_parent,
        } => submission_context(store, &scheduler.live_containments, peer).and_then(|context| {
            if context.parent != expected_parent {
                return Err(StoreError::Rejected(
                    "recovery parent changed after client preflight".into(),
                ));
            }
            store
                .lock()
                .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))
                .and_then(|store| {
                    store
                        .recover_submission_scoped(
                            context
                                .parent
                                .map_or(SubmissionScope::Unmanaged, SubmissionScope::Managed),
                            idempotency_key,
                            &payload_hash,
                        )
                        .map(|recovery| Response::Recovered {
                            store_uuid: store.store_uuid(),
                            recovery,
                        })
                })
        }),
        Request::Status { job_id } => scheduler
            .reconciliation_observations
            .lock()
            .map_err(|_| StoreError::InvalidState("reconciliation mutex poisoned".into()))
            .and_then(|observations| {
                store
                    .lock()
                    .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))
                    .and_then(|store| store.status_with_reconciliation(job_id, &observations))
            })
            .map(|snapshot| Response::Snapshot(Box::new(snapshot))),
        Request::List {
            selector,
            cursor,
            limit,
        } => store
            .lock()
            .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))
            .and_then(|store| store.list_jobs(&selector, cursor, limit))
            .map(Response::Listed),
        Request::Tree {
            selector,
            root_cursor,
            root_limit,
            node_limit,
            max_depth,
        } => open_read_view(store)
            .and_then(|store| store.tree(&selector, root_cursor, root_limit, node_limit, max_depth))
            .map(Response::Tree),
        Request::TreeForJob {
            job_id,
            node_limit,
            max_depth,
        } => open_read_view(store)
            .and_then(|store| store.tree_for_job(job_id, node_limit, max_depth))
            .map(Response::Tree),
        Request::TreeChildren {
            cursor,
            node_limit,
            additional_depth,
        } => open_read_view(store)
            .and_then(|store| store.tree_children(&cursor, node_limit, additional_depth))
            .map(Response::TreeChildren),
        Request::Observe {
            selector,
            cursor,
            limit,
            max_wait_millis,
            managed_wait,
        } => (|| {
            if managed_wait {
                let context = submission_context(store, &scheduler.live_containments, peer)?;
                let crate::JobSelector::Jobs { job_ids } = &selector else {
                    return Err(StoreError::InvalidSpec(
                        "managed wait observation requires explicit Job IDs".into(),
                    ));
                };
                store
                    .lock()
                    .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?
                    .validate_managed_wait(
                        context
                            .parent
                            .map_or(SubmissionScope::Unmanaged, SubmissionScope::Managed),
                        job_ids,
                    )?;
            }
            scheduler
                .wait_observation(
                    store,
                    &selector,
                    cursor,
                    limit,
                    Duration::from_millis(u64::from(max_wait_millis.min(60_000))),
                )
                .map(Response::Observed)
        })(),
        Request::ObserveTrees {
            selector,
            cursor,
            event_limit,
            root_limit,
            node_limit,
            max_depth,
            max_wait_millis,
        } => scheduler
            .wait_tree_observation(
                store,
                &selector,
                cursor,
                event_limit,
                root_limit,
                node_limit,
                max_depth,
                Duration::from_millis(u64::from(max_wait_millis.min(60_000))),
            )
            .map(Response::TreesObserved),
        Request::Cancel { job_ids } => store
            .lock()
            .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))
            .and_then(|mut store| store.cancel_jobs(&job_ids))
            .map(|snapshots| {
                scheduler.wake();
                Response::Canceled { snapshots }
            }),
        Request::Wait {
            job_id,
            max_wait_millis,
            claimed_parent,
        } => submission_context(store, &scheduler.live_containments, peer).and_then(|context| {
            if claimed_parent.is_some() && claimed_parent != context.parent {
                return Err(StoreError::Rejected(
                    "claimed managed parent does not match daemon-held OS containment".into(),
                ));
            }
            store
                .lock()
                .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?
                .validate_managed_wait(
                    context
                        .parent
                        .map_or(SubmissionScope::Unmanaged, SubmissionScope::Managed),
                    &[job_id],
                )?;
            scheduler
                .wait_snapshot(
                    store,
                    job_id,
                    Duration::from_millis(u64::from(max_wait_millis.min(1_000))),
                )
                .map(|snapshot| Response::Snapshot(Box::new(snapshot)))
        }),
        Request::Logs {
            job_id,
            stream,
            offset,
            limit,
        } => store
            .lock()
            .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))
            .and_then(|store| store.logs(job_id, stream, offset, limit))
            .map(Response::Logs),
        Request::DaemonStatus {} => store
            .lock()
            .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))
            .and_then(|store| store.daemon_status(&scheduler.endpoint))
            .map(Response::DaemonStatus),
        Request::Doctor { cursor, limit } => (|| {
            let doctor_store = open_read_view(store)?;
            let requirements = doctor_store.host_observation_requirements()?;
            let page_limit = limit
                .unwrap_or(crate::MAX_DOCTOR_PAGE)
                .clamp(1, crate::MAX_DOCTOR_PAGE) as usize;
            let incident_page = match cursor {
                Some(cursor) => scheduler
                    .doctor_snapshots
                    .lock()
                    .map_err(|_| StoreError::InvalidState("doctor snapshot mutex poisoned".into()))?
                    .next(cursor, page_limit)?,
                None => {
                    let observations = scheduler
                        .reconciliation_observations
                        .lock()
                        .map_err(|_| {
                            StoreError::InvalidState("reconciliation mutex poisoned".into())
                        })?
                        .clone();
                    let captured = doctor_store.capture_doctor_incidents(&observations)?;
                    scheduler
                        .doctor_snapshots
                        .lock()
                        .map_err(|_| {
                            StoreError::InvalidState("doctor snapshot mutex poisoned".into())
                        })?
                        .begin(captured, page_limit)?
                }
            };
            let mut snapshot =
                doctor_store.doctor_with_incident_page(&scheduler.endpoint, incident_page)?;
            {
                let (detector_checks, detector_coverage) =
                    scheduler.host_observation.doctor_diagnostics(requirements);
                snapshot.checks.extend(detector_checks);
                snapshot.coverage.extend(detector_coverage);
            }
            snapshot
                .checks
                .sort_by(|left, right| left.code.cmp(&right.code));
            snapshot.overall = if snapshot
                .checks
                .iter()
                .any(|check| check.status == crate::DoctorCheckStatus::Fail)
            {
                crate::DoctorOverallStatus::Unsafe
            } else if snapshot.checks.iter().any(|check| {
                matches!(
                    check.status,
                    crate::DoctorCheckStatus::Warning | crate::DoctorCheckStatus::Unknown(_)
                )
            }) {
                crate::DoctorOverallStatus::AttentionRequired
            } else {
                crate::DoctorOverallStatus::Healthy
            };
            Ok(Response::Doctor(Box::new(snapshot)))
        })(),
        Request::ForceClearContainment { containment_id } => {
            force_clear_containment(store, scheduler, peer, containment_id)
                .map(Response::ContainmentCleared)
        }
    };
    result.unwrap_or_else(|error| match error {
        StoreError::NotFound(_) => Response::Error {
            code: error_code::NOT_FOUND.into(),
            message: error.to_string(),
        },
        StoreError::IdempotencyConflict {
            existing_payload_hash,
            requested_payload_hash,
        } => Response::Conflict {
            existing_payload_hash,
            requested_payload_hash,
        },
        StoreError::Rejected(_) => Response::Error {
            code: error_code::REJECTED.into(),
            message: error.to_string(),
        },
        StoreError::OperationRejected { code, detail } => Response::Error {
            code,
            message: detail,
        },
        StoreError::BlockedByAncestor(detail) => Response::Error {
            code: error_code::BLOCKED_BY_ANCESTOR.into(),
            message: detail,
        },
        StoreError::ManagedWaitRejected { code, detail } => Response::Error {
            code,
            message: detail,
        },
        StoreError::InvalidSpec(_) => Response::Error {
            code: error_code::INVALID_SPEC.into(),
            message: error.to_string(),
        },
        StoreError::ViewStale(detail) => Response::Error {
            code: error_code::TREE_CURSOR_STALE.into(),
            message: detail,
        },
        StoreError::DoctorCursorStale(detail) => Response::Error {
            code: error_code::DOCTOR_CURSOR_STALE.into(),
            message: detail,
        },
        StoreError::ViewUnavailable(detail) => Response::Error {
            code: error_code::TREE_SCAN_LIMIT.into(),
            message: detail,
        },
        StoreError::DoctorIncidentLimit => Response::Error {
            code: error_code::DOCTOR_INCIDENT_LIMIT.into(),
            message: error.to_string(),
        },
        StoreError::DoctorMemoryLimit => Response::Error {
            code: error_code::DOCTOR_MEMORY_LIMIT.into(),
            message: error.to_string(),
        },
        StoreError::DoctorSnapshotCapacity => Response::Error {
            code: error_code::DOCTOR_SNAPSHOT_CAPACITY.into(),
            message: error.to_string(),
        },
        _ => Response::Error {
            code: error_code::STORE_ERROR.into(),
            message: error.to_string(),
        },
    })
}

fn retained_submission_rejection(
    store: &Store,
    scope: SubmissionScope,
    idempotency_key: uuid::Uuid,
    payload_hash: &str,
    original_error: StoreError,
) -> std::result::Result<Response, StoreError> {
    match store.recover_submission_scoped(scope, idempotency_key, payload_hash) {
        Ok(crate::RecoveryResult::Rejected { code, detail }) => Ok(Response::SubmissionRejected {
            code,
            message: detail,
        }),
        _ => Err(original_error),
    }
}
