use super::*;

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
        } => submission_context(store, &scheduler.live_containments, peer)
            .and_then(|context| {
                if context.parent != expected_parent {
                    return Err(StoreError::Rejected(
                        "submission parent changed after client preflight".into(),
                    ));
                }
                store
                    .lock()
                    .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))
                    .and_then(|mut store| {
                        if expected_store_uuid
                            .is_some_and(|expected| expected != store.store_uuid())
                        {
                            return Err(StoreError::InvalidState(
                                "store identity changed during submission".into(),
                            ));
                        }
                        store.submit_with_stdin_scoped_for_wait(
                            context
                                .parent
                                .map_or(SubmissionScope::Unmanaged, SubmissionScope::Managed),
                            idempotency_key,
                            &payload_hash,
                            &spec,
                            stdin.as_ref(),
                            wait_for_completion,
                        )
                    })
            })
            .map(|submitted| {
                if submitted.should_schedule {
                    scheduler.wake();
                }
                Response::Submitted(submitted.receipt)
            }),
        Request::SubmitBatch {
            idempotency_key,
            payload_hash,
            spec,
            stdins,
            expected_store_uuid,
            expected_parent,
            wait_for_completion,
        } => submission_context(store, &scheduler.live_containments, peer)
            .and_then(|context| {
                if context.parent != expected_parent {
                    return Err(StoreError::Rejected(
                        "submission parent changed after client preflight".into(),
                    ));
                }
                store
                    .lock()
                    .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))
                    .and_then(|mut store| {
                        if expected_store_uuid
                            .is_some_and(|expected| expected != store.store_uuid())
                        {
                            return Err(StoreError::InvalidState(
                                "store identity changed during submission".into(),
                            ));
                        }
                        store.submit_batch_with_stdins_scoped_for_wait(
                            context
                                .parent
                                .map_or(SubmissionScope::Unmanaged, SubmissionScope::Managed),
                            idempotency_key,
                            &payload_hash,
                            &spec,
                            &stdins,
                            wait_for_completion,
                        )
                    })
            })
            .map(|submitted| {
                if submitted.should_schedule {
                    scheduler.wake();
                }
                Response::BatchSubmitted(submitted.receipt)
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
        Request::Doctor { cursor, limit } => scheduler
            .reconciliation_observations
            .lock()
            .map_err(|_| StoreError::InvalidState("reconciliation mutex poisoned".into()))
            .and_then(|observations| {
                store
                    .lock()
                    .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))
                    .and_then(|store| {
                        store.doctor_with_reconciliation(
                            &scheduler.endpoint,
                            cursor,
                            limit,
                            &observations,
                        )
                    })
            })
            .map(|snapshot| Response::Doctor(Box::new(snapshot))),
        Request::ForceClearContainment { containment_id } => {
            force_clear_containment(store, scheduler, peer, containment_id)
                .map(Response::ContainmentCleared)
        }
    };
    result.unwrap_or_else(|error| match error {
        StoreError::NotFound(_) => Response::Error {
            code: "not_found".into(),
            message: error.to_string(),
        },
        StoreError::IdempotencyConflict => Response::Error {
            code: "idempotency_conflict".into(),
            message: error.to_string(),
        },
        StoreError::Rejected(_) => Response::Error {
            code: "rejected".into(),
            message: error.to_string(),
        },
        StoreError::OperationRejected { code, detail } => Response::Error {
            code,
            message: detail,
        },
        StoreError::BlockedByAncestor(detail) => Response::Error {
            code: "blocked_by_ancestor".into(),
            message: detail,
        },
        StoreError::ManagedWaitRejected { code, detail } => Response::Error {
            code,
            message: detail,
        },
        StoreError::InvalidSpec(_) => Response::Error {
            code: "invalid_spec".into(),
            message: error.to_string(),
        },
        _ => Response::Error {
            code: "store_error".into(),
            message: error.to_string(),
        },
    })
}
