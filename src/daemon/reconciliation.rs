use super::*;

pub(super) fn probe_reconciliation_candidate(
    live_containments: &crate::runner::LiveContainments,
    candidate: &crate::store::ReconciliationCandidate,
    context: &(crate::HostId, crate::BootId, uuid::Uuid),
) -> (
    Option<crate::ContainmentResolution>,
    crate::ReconciliationResult,
) {
    let (current_host, current_boot, current_generation) = context;
    if candidate.host_id.as_ref() != Some(current_host) {
        return (None, crate::ReconciliationResult::IdentityUnavailable);
    }
    let Some(recorded_boot) = candidate.boot_id.as_ref() else {
        return (None, crate::ReconciliationResult::IdentityUnavailable);
    };
    if recorded_boot != current_boot {
        return if candidate.daemon_generation != Some(*current_generation) {
            (
                Some(crate::ContainmentResolution::Reboot),
                crate::ReconciliationResult::PriorBoot,
            )
        } else {
            (None, crate::ReconciliationResult::IdentityUnavailable)
        };
    }
    if candidate.daemon_generation == Some(*current_generation) {
        return match live_containments.inspect(candidate.invocation_id) {
            Ok(Some(crate::ReconciliationResult::ProvenEmpty)) => (
                Some(crate::ContainmentResolution::ProvenEmpty),
                crate::ReconciliationResult::ProvenEmpty,
            ),
            Ok(Some(evidence)) => (None, evidence),
            Ok(None) => (None, crate::ReconciliationResult::BoundaryUninspectable),
            Err(_) => (None, crate::ReconciliationResult::BoundaryUninspectable),
        };
    }
    let Some(prior_daemon) = candidate.prior_daemon_identity.as_ref() else {
        return (None, crate::ReconciliationResult::IdentityUnavailable);
    };
    let prior_daemon =
        crate::identity::probe_recorded_process(prior_daemon, current_host, current_boot);
    if !matches!(
        prior_daemon,
        crate::ReconciliationResult::IdentityAbsent | crate::ReconciliationResult::PidReused
    ) {
        return (None, prior_daemon);
    }
    let Some(root_identity) = candidate.root_identity.as_ref() else {
        if candidate.root_pid_recorded {
            return (None, crate::ReconciliationResult::IdentityUnavailable);
        }
        return (
            Some(crate::ContainmentResolution::ProvenEmpty),
            crate::ReconciliationResult::IdentityAbsent,
        );
    };
    let root = crate::identity::probe_recorded_process(root_identity, current_host, current_boot);
    if matches!(
        root,
        crate::ReconciliationResult::IdentityAbsent | crate::ReconciliationResult::PidReused
    ) {
        (Some(crate::ContainmentResolution::ProvenEmpty), root)
    } else {
        (None, root)
    }
}

pub(super) fn authorize_force_peer(
    live_containments: &crate::runner::LiveContainments,
    peer: &PeerProcess,
    requester: &crate::ProcessIdentity,
    authorization_invocations: &[crate::InvocationId],
    unresolved_roots: &[crate::ProcessIdentity],
) -> std::result::Result<(), StoreError> {
    for &invocation_id in authorization_invocations {
        match live_containments.contains_process(invocation_id, peer.handle) {
            Ok(Some(true)) => {
                return Err(StoreError::OperationRejected {
                    code: "containment_caller_managed".into(),
                    detail: "a managed process cannot accept containment risk".into(),
                });
            }
            Ok(Some(false)) => {}
            Ok(None) | Err(_) => {
                return Err(StoreError::OperationRejected {
                    code: "containment_authorization_unavailable".into(),
                    detail: "a current-generation containment boundary cannot be inspected".into(),
                });
            }
        }
    }
    if unresolved_roots
        .iter()
        .any(|identity| identity == requester)
    {
        return Err(StoreError::OperationRejected {
            code: "containment_caller_managed".into(),
            detail: "the requester is an unresolved recorded containment root".into(),
        });
    }
    Ok(())
}

pub(super) fn force_clear_containment(
    store: &SharedStore,
    scheduler: &DaemonReactor,
    peer: Option<&PeerProcess>,
    containment_id: crate::ContainmentId,
) -> std::result::Result<crate::ClearContainmentResult, StoreError> {
    let peer = peer.ok_or_else(|| StoreError::OperationRejected {
        code: "containment_requester_unidentifiable".into(),
        detail: "force-clear requires a connected peer process".into(),
    })?;
    let requester = peer
        .identity
        .clone()
        .ok_or_else(|| StoreError::OperationRejected {
            code: "containment_requester_unidentifiable".into(),
            detail: "the connection-time requester identity is unavailable".into(),
        })?;
    let (context, mut authorization_invocations, mut unresolved_roots) = {
        let guard = store
            .lock()
            .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?;
        let context =
            guard
                .reconciliation_context()
                .ok_or_else(|| StoreError::OperationRejected {
                    code: "containment_requester_unidentifiable".into(),
                    detail: "the daemon host/boot identity is unavailable".into(),
                })?;
        let (authorization_invocations, unresolved_roots) =
            guard.clearance_authorization_evidence()?;
        (context, authorization_invocations, unresolved_roots)
    };

    authorize_force_peer(
        &scheduler.live_containments,
        peer,
        &requester,
        &authorization_invocations,
        &unresolved_roots,
    )?;
    let mut candidate = {
        let guard = store
            .lock()
            .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?;
        if let Some(result) = guard.persisted_clearance(containment_id)? {
            return Ok(result);
        }
        guard.reconciliation_candidate(containment_id)?
    };

    let (automatic_resolution, automatic_evidence) =
        probe_reconciliation_candidate(&scheduler.live_containments, &candidate, &context);
    if let Ok(mut observations) = scheduler.reconciliation_observations.lock() {
        observations.record(candidate.containment_id, automatic_evidence.clone());
    }
    if let Some(resolution) = automatic_resolution {
        if let Some(result) = store
            .lock()
            .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?
            .commit_containment_resolution(
                &candidate,
                resolution,
                automatic_evidence.clone(),
                crate::ClearanceOrigin::Automatic,
                None,
                None,
            )?
        {
            scheduler.live_containments.clear(candidate.invocation_id);
            if result.audit.lease_released {
                scheduler.wake();
            }
            return Ok(result);
        }
    }
    match automatic_evidence {
        crate::ReconciliationResult::BoundaryNotEmpty => {
            return Err(StoreError::OperationRejected {
                code: "containment_boundary_not_empty".into(),
                detail: "the daemon-owned containment boundary is known nonempty".into(),
            });
        }
        crate::ReconciliationResult::BoundaryUninspectable
            if candidate.daemon_generation == Some(context.2) =>
        {
            return Err(StoreError::OperationRejected {
                code: "containment_owned_boundary_uninspectable".into(),
                detail: "restart the daemon to close the owned boundary before risk acceptance"
                    .into(),
            });
        }
        _ => {}
    }
    if candidate.host_id.as_ref() != Some(&context.0) {
        return Err(StoreError::OperationRejected {
            code: "containment_host_mismatch".into(),
            detail: "containment host identity does not match the daemon".into(),
        });
    }
    let mut target_evidence = match candidate.root_identity.as_ref() {
        Some(identity) => crate::identity::probe_recorded_process(identity, &context.0, &context.1),
        None if !candidate.root_pid_recorded => crate::ReconciliationResult::IdentityAbsent,
        None => crate::ReconciliationResult::IdentityUnavailable,
    };
    match target_evidence {
        crate::ReconciliationResult::StillResolves => {
            return Err(StoreError::OperationRejected {
                code: "containment_identity_still_resolves".into(),
                detail: "the exact recorded root process is still running".into(),
            });
        }
        crate::ReconciliationResult::IdentityUnavailable => {
            return Err(StoreError::OperationRejected {
                code: "containment_identity_unavailable".into(),
                detail: "the exact recorded root identity cannot be inspected".into(),
            });
        }
        crate::ReconciliationResult::IdentityAbsent | crate::ReconciliationResult::PidReused => {}
        _ => {
            return Err(StoreError::OperationRejected {
                code: "containment_identity_unavailable".into(),
                detail: "the target identity has no affirmative absence evidence".into(),
            });
        }
    }
    let requested_unix_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX);
    let forced = crate::ForcedClearanceAudit {
        requested_unix_millis,
        requester,
    };
    for attempt in 0..2 {
        if let Some(result) = store
            .lock()
            .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?
            .commit_containment_resolution(
                &candidate,
                crate::ContainmentResolution::ForcedRiskAcceptance,
                target_evidence.clone(),
                crate::ClearanceOrigin::Forced,
                Some(forced.clone()),
                Some(&authorization_invocations),
            )?
        {
            if result.audit.lease_released {
                scheduler.wake();
            }
            return Ok(result);
        }
        if let Some(result) = store
            .lock()
            .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?
            .persisted_clearance(containment_id)?
        {
            return Ok(result);
        }
        if attempt == 0 {
            let guard = store
                .lock()
                .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?;
            if let Some(result) = guard.persisted_clearance(containment_id)? {
                return Ok(result);
            }
            candidate = guard.reconciliation_candidate(containment_id)?;
            drop(guard);
            if candidate.host_id.as_ref() != Some(&context.0) {
                return Err(StoreError::OperationRejected {
                    code: "containment_host_mismatch".into(),
                    detail: "containment host identity changed during force-clear".into(),
                });
            }
            target_evidence = match candidate.root_identity.as_ref() {
                Some(identity) => {
                    crate::identity::probe_recorded_process(identity, &context.0, &context.1)
                }
                None if !candidate.root_pid_recorded => crate::ReconciliationResult::IdentityAbsent,
                None => crate::ReconciliationResult::IdentityUnavailable,
            };
            if !matches!(
                target_evidence,
                crate::ReconciliationResult::IdentityAbsent
                    | crate::ReconciliationResult::PidReused
            ) {
                return Err(StoreError::OperationRejected {
                    code: "containment_identity_unavailable".into(),
                    detail: "containment evidence changed during force-clear".into(),
                });
            }
            (authorization_invocations, unresolved_roots) = store
                .lock()
                .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?
                .clearance_authorization_evidence()?;
            authorize_force_peer(
                &scheduler.live_containments,
                peer,
                &forced.requester,
                &authorization_invocations,
                &unresolved_roots,
            )?;
            continue;
        }
    }
    Err(StoreError::OperationRejected {
        code: "containment_authorization_unavailable".into(),
        detail: "containment evidence changed during force-clear".into(),
    })
}
