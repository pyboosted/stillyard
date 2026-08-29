use super::*;

pub(super) struct DaemonReactor {
    pub(super) signal: Arc<(Mutex<bool>, Condvar)>,
    pub(super) events: Arc<(Mutex<u64>, Condvar)>,
    pub(super) endpoint: Arc<str>,
    pub(super) live_containments: crate::runner::LiveContainments,
    pub(super) reconciliation_observations: Mutex<crate::store::ReconciliationObservations>,
    pub(super) host_observation: Arc<crate::host_observation::HostObservationService>,
}

pub(super) fn submission_context(
    store: &SharedStore,
    live_containments: &crate::runner::LiveContainments,
    peer: Option<&PeerProcess>,
) -> std::result::Result<crate::SubmissionContext, StoreError> {
    let (store_uuid, candidates) = {
        let store = store
            .lock()
            .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?;
        (store.store_uuid(), store.managed_containment_candidates()?)
    };
    let parent = match peer {
        Some(peer) => authenticate_managed_peer(live_containments, peer, &candidates)?,
        None => None,
    };
    Ok(crate::SubmissionContext { store_uuid, parent })
}

pub(super) fn resolve_managed_membership(
    candidates: &[ManagedCandidate],
    mut is_member: impl FnMut(crate::InvocationId) -> std::io::Result<Option<bool>>,
) -> std::result::Result<Option<crate::ManagedParent>, StoreError> {
    let mut matched = Vec::new();
    for candidate in candidates {
        match is_member(candidate.parent.invocation_id).map_err(StoreError::Io)? {
            Some(true) => {}
            Some(false) => continue,
            None => {
                return Err(StoreError::InvalidState(
                    "a possibly live managed Containment has no daemon-held handle".into(),
                ));
            }
        }
        matched.push(candidate);
    }
    if matched.is_empty() {
        return Ok(None);
    }

    // A process in a nested Windows Job hierarchy is a member of the immediate Job and every
    // ancestor Job. Select the unique leaf containment, then prove that every other match is on
    // its direct durable parent chain. Multiple leaves are an ambiguous authority match.
    let leaves = matched
        .iter()
        .copied()
        .filter(|candidate| {
            !matched
                .iter()
                .any(|other| other.parent_job_id == Some(candidate.parent.job_id))
        })
        .collect::<Vec<_>>();
    let [immediate] = leaves.as_slice() else {
        return Err(StoreError::Rejected(
            "named-pipe peer belongs to ambiguous Stillyard containments".into(),
        ));
    };
    let mut lineage = std::collections::HashSet::new();
    let mut current = Some(*immediate);
    while let Some(candidate) = current {
        if !lineage.insert(candidate.parent.job_id) {
            return Err(StoreError::InvalidState(
                "managed containment parent graph contains a cycle".into(),
            ));
        }
        current = candidate.parent_job_id.and_then(|parent_job_id| {
            matched
                .iter()
                .copied()
                .find(|parent| parent.parent.job_id == parent_job_id)
        });
    }
    if lineage.len() != matched.len() {
        return Err(StoreError::Rejected(
            "named-pipe peer belongs to unrelated Stillyard containments".into(),
        ));
    }
    if !immediate.current {
        return Err(StoreError::Rejected(
            "the containing primary is no longer current and live".into(),
        ));
    }
    if !immediate.submissions_enabled {
        return Err(StoreError::Rejected(
            "the containing primary does not allow child submissions".into(),
        ));
    }
    Ok(Some(immediate.parent))
}

#[cfg(windows)]
pub(super) fn authenticate_managed_peer(
    live_containments: &crate::runner::LiveContainments,
    peer: &PeerProcess,
    candidates: &[ManagedCandidate],
) -> std::result::Result<Option<crate::ManagedParent>, StoreError> {
    resolve_managed_membership(candidates, |invocation_id| {
        live_containments.contains_process(invocation_id, peer.handle)
    })
    .map_err(|error| match error {
        StoreError::Io(source) => StoreError::InvalidState(format!(
            "cannot inspect named-pipe peer {}: {source}",
            peer.pid
        )),
        other => other,
    })
}

#[cfg(not(windows))]
pub(super) fn authenticate_managed_peer(
    _live_containments: &crate::runner::LiveContainments,
    _peer: &PeerProcess,
    _candidates: &[ManagedCandidate],
) -> std::result::Result<Option<crate::ManagedParent>, StoreError> {
    Ok(None)
}

impl DaemonReactor {
    pub(super) fn start(
        store: SharedStore,
        endpoint: String,
        observation_config: crate::HostObservationConfig,
    ) -> Arc<Self> {
        let scheduler = Arc::new(Self {
            signal: Arc::new((Mutex::new(false), Condvar::new())),
            events: Arc::new((Mutex::new(0), Condvar::new())),
            endpoint: Arc::from(endpoint),
            live_containments: crate::runner::LiveContainments::default(),
            reconciliation_observations: Mutex::new(Default::default()),
            host_observation: Arc::new(crate::host_observation::HostObservationService::new(
                observation_config,
            )),
        });
        let worker = Arc::clone(&scheduler);
        std::thread::Builder::new()
            .name("stillyard-scheduler".into())
            .spawn(move || worker.run(store))
            .expect("scheduler thread must start");
        scheduler
    }

    pub(super) fn wake(&self) {
        let (lock, condition) = &*self.signal;
        if let Ok(mut pending) = lock.lock() {
            *pending = true;
            condition.notify_one();
        }
        self.notify_change();
    }

    pub(super) fn notify_change(&self) {
        let (lock, condition) = &*self.events;
        if let Ok(mut generation) = lock.lock() {
            *generation = generation.wrapping_add(1);
            condition.notify_all();
        }
    }

    pub(super) fn wait_snapshot(
        &self,
        store: &SharedStore,
        job_id: crate::JobId,
        max_wait: Duration,
    ) -> std::result::Result<crate::JobSnapshot, StoreError> {
        let deadline = Instant::now() + max_wait;
        loop {
            let observed = self
                .events
                .0
                .lock()
                .map_err(|_| StoreError::InvalidState("event mutex poisoned".into()))
                .map(|generation| *generation)?;
            let snapshot = {
                let observations = self.reconciliation_observations.lock().map_err(|_| {
                    StoreError::InvalidState("reconciliation mutex poisoned".into())
                })?;
                store
                    .lock()
                    .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?
                    .status_with_reconciliation(job_id, &observations)?
            };
            if snapshot.is_final() || Instant::now() >= deadline {
                return Ok(snapshot);
            }
            let (lock, condition) = &*self.events;
            let mut generation = lock
                .lock()
                .map_err(|_| StoreError::InvalidState("event mutex poisoned".into()))?;
            while *generation == observed {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                let waited = condition
                    .wait_timeout(generation, remaining)
                    .map_err(|_| StoreError::InvalidState("event mutex poisoned".into()))?;
                generation = waited.0;
                if waited.1.timed_out() {
                    break;
                }
            }
        }
    }

    pub(super) fn wait_observation(
        &self,
        store: &SharedStore,
        selector: &crate::JobSelector,
        cursor: Option<crate::EventCursor>,
        limit: u32,
        max_wait: Duration,
    ) -> std::result::Result<crate::ObservationFrame, StoreError> {
        let deadline = Instant::now() + max_wait;
        let mut cursor = cursor;
        loop {
            let observed = self
                .events
                .0
                .lock()
                .map_err(|_| StoreError::InvalidState("event mutex poisoned".into()))
                .map(|generation| *generation)?;
            let requested = cursor;
            let frame = store
                .lock()
                .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))?
                .observe(selector, cursor, limit)?;
            let ready = match &frame {
                crate::ObservationFrame::Events { events, .. } => !events.is_empty(),
                crate::ObservationFrame::Gap { .. } => true,
            } || requested != Some(frame.cursor());
            if ready || Instant::now() >= deadline {
                return Ok(frame);
            }
            cursor = Some(frame.cursor());
            let (lock, condition) = &*self.events;
            let mut generation = lock
                .lock()
                .map_err(|_| StoreError::InvalidState("event mutex poisoned".into()))?;
            while *generation == observed {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                let waited = condition
                    .wait_timeout(generation, remaining)
                    .map_err(|_| StoreError::InvalidState("event mutex poisoned".into()))?;
                generation = waited.0;
                if waited.1.timed_out() {
                    break;
                }
            }
        }
    }

    pub(super) fn run(self: Arc<Self>, store: SharedStore) {
        const RECONCILIATION_BACKOFF_SECONDS: [u64; 9] = [1, 2, 4, 8, 16, 30, 60, 120, 300];
        let mut reconciliation_cursor = 0_u64;
        let mut reconciliation_known_latest = 0_u64;
        let mut reconciliation_backoff = 0_usize;
        let mut reconciliation_deadline: Option<Instant> = None;
        loop {
            let newest_incident = store
                .lock()
                .ok()
                .and_then(|guard| guard.latest_unresolved_incident_sequence().ok())
                .flatten();
            let new_incident =
                newest_incident.is_some_and(|sequence| sequence > reconciliation_known_latest);
            if new_incident {
                reconciliation_known_latest =
                    newest_incident.unwrap_or(reconciliation_known_latest);
                reconciliation_backoff = 0;
            }
            let reconciliation_due = new_incident
                || reconciliation_deadline.is_none()
                || reconciliation_deadline.is_some_and(|deadline| Instant::now() >= deadline);
            if reconciliation_due {
                let snapshot = store.lock().ok().and_then(|guard| {
                    let context = guard.reconciliation_context()?;
                    let candidates = guard
                        .reconciliation_candidates(reconciliation_cursor, 32)
                        .ok()?;
                    Some((context, candidates))
                });
                if let Some((context, candidates)) = snapshot {
                    if candidates.is_empty() {
                        reconciliation_deadline = None;
                        reconciliation_backoff = 0;
                    } else {
                        for candidate in &candidates {
                            let (resolution, evidence) = probe_reconciliation_candidate(
                                &self.live_containments,
                                candidate,
                                &context,
                            );
                            if let Ok(mut observations) = self.reconciliation_observations.lock() {
                                observations.record(candidate.containment_id, evidence.clone());
                            }
                            if let Some(resolution) = resolution {
                                let committed = store.lock().ok().and_then(|mut guard| {
                                    guard
                                        .commit_containment_resolution(
                                            candidate,
                                            resolution,
                                            evidence,
                                            crate::ClearanceOrigin::Automatic,
                                            None,
                                            None,
                                        )
                                        .ok()
                                        .flatten()
                                });
                                if let Some(committed) = committed {
                                    self.live_containments.clear(candidate.invocation_id);
                                    if committed.audit.lease_released {
                                        if let Ok(mut pending) = self.signal.0.lock() {
                                            *pending = true;
                                        }
                                    }
                                }
                            }
                        }
                        reconciliation_cursor = candidates
                            .last()
                            .map_or(reconciliation_cursor, |candidate| {
                                candidate.incident_sequence
                            });
                        let unresolved = store
                            .lock()
                            .ok()
                            .and_then(|guard| guard.latest_unresolved_incident_sequence().ok())
                            .flatten()
                            .is_some();
                        if unresolved {
                            let delay = RECONCILIATION_BACKOFF_SECONDS[reconciliation_backoff
                                .min(RECONCILIATION_BACKOFF_SECONDS.len() - 1)];
                            reconciliation_backoff = reconciliation_backoff.saturating_add(1);
                            reconciliation_deadline =
                                Instant::now().checked_add(Duration::from_secs(delay));
                        } else {
                            reconciliation_deadline = None;
                            reconciliation_backoff = 0;
                        }
                    }
                }
            }
            let retry_scan_started = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .try_into()
                .unwrap_or(i64::MAX);
            let mut retry = false;
            let observation_needed = store
                .lock()
                .ok()
                .and_then(|guard| guard.host_observation_demand().ok())
                .unwrap_or(false);
            let sample = observation_needed
                .then(|| self.host_observation.sample_now().ok())
                .flatten();
            let next = {
                let mut guard = match store.lock() {
                    Ok(guard) => guard,
                    Err(_) => return,
                };
                match guard.prepare_next_job_with_sample(sample.as_ref()) {
                    Ok(job) => job,
                    Err(_) => {
                        retry = true;
                        crate::store::PrepareNext {
                            job: None,
                            // prepare_next_job may have committed skip closure before a later
                            // SQLite error. A spurious notification is safer than hiding it.
                            state_changed: true,
                        }
                    }
                }
            };
            if next.state_changed {
                self.notify_change();
            }
            if let Some(job) = next.job {
                self.notify_change();
                let worker_store = Arc::clone(&store);
                let worker_scheduler = Arc::clone(&self);
                let worker_endpoint = Arc::clone(&self.endpoint);
                let worker_containments = self.live_containments.clone();
                let worker_observation = Arc::clone(&self.host_observation);
                let thread_job = job.clone();
                let spawned = std::thread::Builder::new()
                    .name(format!("stillyard-job-{}", job.job_id.entity_uuid()))
                    .spawn(move || {
                        let wake_scheduler = Arc::downgrade(&worker_scheduler);
                        crate::runner::run(
                            thread_job,
                            worker_store,
                            worker_endpoint,
                            worker_containments,
                            worker_observation,
                            Arc::new(move || {
                                if let Some(scheduler) = wake_scheduler.upgrade() {
                                    scheduler.wake();
                                }
                            }),
                        );
                        worker_scheduler.wake();
                    });
                if let Err(error) = spawned {
                    if let Ok(mut guard) = store.lock() {
                        let _ = guard.mark_finished(
                            &job,
                            None,
                            crate::JobOutcome::Failed,
                            "start_failed",
                        );
                    }
                    eprintln!(
                        "stillyard could not start worker thread for {}: {error}",
                        job.job_id
                    );
                    self.wake();
                    std::thread::sleep(Duration::from_millis(100));
                }
                continue;
            }
            if retry {
                std::thread::sleep(Duration::from_millis(100));
                continue;
            }
            let retry_delay = store
                .lock()
                .ok()
                .and_then(|guard| guard.next_retry_delay(retry_scan_started).ok())
                .flatten();
            let retry_deadline = retry_delay.and_then(|delay| Instant::now().checked_add(delay));
            let observation_deadline = observation_needed.then(|| {
                Instant::now()
                    + Duration::from_millis(self.host_observation.sample_interval_millis())
            });
            let wake_deadline = [
                retry_deadline,
                reconciliation_deadline,
                observation_deadline,
            ]
            .into_iter()
            .flatten()
            .min();
            let (lock, condition) = &*self.signal;
            let mut pending = match lock.lock() {
                Ok(pending) => pending,
                Err(_) => return,
            };
            while !*pending {
                if let Some(deadline) = wake_deadline {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        break;
                    }
                    let waited = match condition.wait_timeout(pending, remaining) {
                        Ok(waited) => waited,
                        Err(_) => return,
                    };
                    pending = waited.0;
                    if waited.1.timed_out() {
                        break;
                    }
                } else {
                    pending = match condition.wait(pending) {
                        Ok(pending) => pending,
                        Err(_) => return,
                    };
                }
            }
            *pending = false;
        }
    }
}
