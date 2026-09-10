//! One Invocation on the tracer thread; shared lifecycle owns all settlement.
use super::launch::{LaunchSpec, PreparedLaunch};
use crate::machine::manager::release::{Disposition, NeverReleased};
use crate::runner::lifecycle::{self, RunProgress, RunResult};
use crate::runner::{LiveContainments, ReconciliationWake};
use crate::store::{PreparedJob, ReleaseAuthorization, Store, StoreError};
use crate::{InvocationRole, LogStream, ProcessIdentity};
use std::cell::{Cell, RefCell};
use std::fs::File;
use std::io;
use std::os::fd::OwnedFd;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn lock(store: &Arc<Mutex<Store>>) -> RunResult<std::sync::MutexGuard<'_, Store>> {
    store
        .lock()
        .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()).into())
}
fn protocol_error(error: impl std::fmt::Display) -> crate::machine::manager::JournalError {
    crate::machine::manager::JournalError::History(error.to_string())
}

pub(in crate::runner) fn execute(
    job: &PreparedJob,
    store: &Arc<Mutex<Store>>,
    endpoint: &str,
    live: &LiveContainments,
    observation: &crate::host_observation::HostObservationService,
    progress: &mut RunProgress,
    wake: &ReconciliationWake,
) -> RunResult<(u32, bool)> {
    let (lease, parent, generation) = lock(store)?.attached_launch_identity(job)?;
    #[cfg(test)]
    let parent = std::env::var_os("STILLYARD_TEST_CGROUP_ROOT")
        .map(std::path::PathBuf::from)
        .unwrap_or(parent);
    let helper = std::env::current_exe()?;
    #[cfg(test)]
    let helper = std::env::var_os("STILLYARD_TEST_EXECUTABLE")
        .map(std::path::PathBuf::from)
        .unwrap_or(helper);
    let context = live.linux_server_context(crate::ManagedParent {
        job_id: job.job_id,
        attempt_id: job.attempt_id,
        invocation_id: job.invocation_id,
    })?;
    // Caller variables reach only the final executable, never the trusted stub.
    let mut environment = job.spec.environment.set.clone();
    for name in &job.spec.environment.unset {
        environment.remove(name);
    }
    for (name, value) in [
        ("STILLYARD_JOB_ID", job.job_id.to_string()),
        ("STILLYARD_ATTEMPT", job.attempt_id.to_string()),
        ("STILLYARD_INVOCATION_ID", job.invocation_id.to_string()),
        ("STILLYARD_ENDPOINT", endpoint.into()),
        ("STILLYARD_DAEMON_ID", job.job_id.store_uuid().to_string()),
        (
            "STILLYARD_ROLE",
            match job.role {
                InvocationRole::Primary => "primary",
                InvocationRole::Probe => "probe",
                InvocationRole::Postcondition => "postcondition",
            }
            .into(),
        ),
        (
            crate::identity::attestation::ENVIRONMENT,
            serde_json::to_string(&context)?,
        ),
    ] {
        environment.insert(name.into(), value);
    }
    if let Some(result) = &job.primary_result {
        environment.insert(
            "STILLYARD_PRIMARY_RESULT".into(),
            serde_json::to_string(result)?,
        );
    }
    let spec = LaunchSpec {
        executable: job.spec.executable.clone(),
        args: job.spec.args.clone(),
        working_directory: job.spec.working_directory.clone(),
        environment,
    };
    let stdin = super::input::open(job)?;
    let creator =
        crate::identity::linux::Process::open(std::process::id(), unsafe { libc::geteuid() })?;
    let mut launch = None;
    let mut drains = Vec::new();
    let mut never: Option<Box<NeverReleased>> = None;
    // A failed journal/CG creation is uncertain until its recorded obligation
    // can be sealed. A failed OS call is never interpreted as no children.
    progress.cleanup_proven = false;
    let execution = (|| -> RunResult<(u32, bool)> {
        let boundary = Arc::new(live.linux.with_journal(|journal| {
            journal.create(
                &parent,
                job.invocation_id,
                job.containment_id,
                lease,
                generation,
                creator.identity.clone(),
            )
        })?);
        live.linux
            .register(job.invocation_id, Arc::clone(&boundary))?;
        launch = Some(PreparedLaunch::prepare(
            &boundary,
            &helper,
            &spec,
            stdin,
            Instant::now() + Duration::from_secs(30),
        )?);
        let child = launch.as_mut().unwrap();
        live.linux
            .with_journal(|journal| journal.ready(job.invocation_id, child))?;
        #[cfg(test)]
        crate::test_support::linux_runtime_checkpoint("ready");
        let boundary_hash = live
            .linux
            .with_journal(|journal| journal.records()?[&job.invocation_id].boundary_sha256())?;
        let root = child.root.identity.clone();
        let ProcessIdentity::Linux { pid, .. } = root else {
            return Err(io::Error::other("non-Linux prepared root").into());
        };
        for (input, path, stream) in [
            (
                File::from(OwnedFd::from(
                    child
                        .wrapper
                        .stdout
                        .take()
                        .ok_or_else(|| io::Error::other("stdout pipe missing"))?,
                )),
                job.stdout_path.clone(),
                LogStream::Stdout,
            ),
            (
                File::from(OwnedFd::from(
                    child
                        .wrapper
                        .stderr
                        .take()
                        .ok_or_else(|| io::Error::other("stderr pipe missing"))?,
                )),
                job.stderr_path.clone(),
                LogStream::Stderr,
            ),
        ] {
            drains.push(crate::runner::logs::spawn_drain(
                input,
                path,
                job.job_id,
                stream,
                Arc::clone(store),
                job.role == InvocationRole::Primary,
            )?);
        }
        lock(store)?.request_attached_ticket(
            job,
            &root,
            &child.requested_sha256,
            &boundary_hash,
        )?;
        wake();
        let until = Instant::now()
            + Duration::from_secs(
                job.spec
                    .quiet
                    .as_ref()
                    .map_or(5, |q| q.wait_budget_seconds.saturating_add(5)),
            );
        let mut retry_at = Instant::now() + Duration::from_secs(1);
        let ticket = loop {
            if let Some(stop) = lifecycle::pending_stop_verdict(job, store, is_guarded(job))? {
                progress.canceled = stop.verdict == crate::AttemptVerdict::Canceled;
                progress.timed_out = stop.verdict == crate::AttemptVerdict::TimedOut;
                if is_guarded(job) {
                    progress.never_run_reason = Some(stop.reason.clone());
                }
                return Err(io::Error::other(stop.reason).into());
            }
            if let Some(ticket) = lock(store)?.attached_ticket(job.invocation_id)? {
                break ticket;
            }
            if Instant::now() >= retry_at {
                let retried = lock(store)?.retry_waiting_attached_ticket(job.invocation_id)?;
                if retried {
                    wake();
                }
                retry_at = Instant::now() + Duration::from_secs(1);
            }
            if Instant::now() >= until {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "Invocation Ticket response deadline",
                )
                .into());
            }
            std::thread::sleep(Duration::from_millis(5));
        };
        let barrier = live.linux_releases.barrier(job.invocation_id)?;
        let executable_hash = child.requested_sha256.clone();
        live.linux
            .with_journal(|journal| journal.release_intent(&ticket))?;
        #[cfg(test)]
        crate::test_support::linux_runtime_checkpoint("release-intent");
        let runtime_deadline = Cell::new(job.attempt_deadline_unix_millis);
        let evidence_expiry = Cell::new(u64::MAX);
        let consumed = Cell::new(false);
        let deferred = RefCell::new(None::<String>);
        let mut release =
            |sample: Option<&crate::host_observation::HostSample>| -> RunResult<Disposition> {
                let mut guard = lock(store)?;
                let locked = RefCell::new(&mut *guard);
                let disposition = barrier.release(
                    &ticket,
                    generation,
                    || {
                        let mut store = locked.borrow_mut();
                        store.check_authority_release().map_err(protocol_error)?;
                        if !store
                            .attached_observation_ready(job, lease, sample)
                            .map_err(protocol_error)?
                        {
                            return Ok(false);
                        }
                        if store
                            .invocation_stop_requested(job.job_id)
                            .map_err(protocol_error)?
                        {
                            return Ok(false);
                        }
                        let (wall, monotonic) = crate::host_observation::observation_clock()?;
                        if runtime_deadline
                            .get()
                            .is_some_and(|deadline| wall >= deadline)
                            || monotonic >= evidence_expiry.get()
                        {
                            return Ok(false);
                        }
                        if is_guarded(job)
                            && store
                                .pre_resume_defer_reason(job.job_id)
                                .map_err(protocol_error)?
                                .is_some()
                        {
                            return Ok(false);
                        }
                        if let Some(sample) = sample {
                            let Some(wall_age) = wall
                                .checked_sub(sample.captured_unix_millis)
                                .and_then(|n| u64::try_from(n).ok())
                            else {
                                return Ok(false);
                            };
                            let Some(age) = monotonic.checked_sub(sample.captured_monotonic_millis)
                            else {
                                return Ok(false);
                            };
                            if wall_age.abs_diff(age)
                                > observation.release_discontinuity_limit_millis()
                            {
                                return Ok(false);
                            }
                        }
                        Ok(true)
                    },
                    || {
                        let mut store = locked.borrow_mut();
                        let (wall, monotonic) = crate::host_observation::observation_clock()?;
                        let moment =
                            sample.map(|sample| crate::host_observation::ObservationMoment {
                                sample,
                                now_unix_millis: wall,
                                now_monotonic_millis: monotonic,
                                live_clock: true,
                            });
                        let authorization = if is_guarded(job) {
                            if job.spec.quiet.is_some() {
                                store.authorize_release_with_ticket(
                                    job,
                                    moment.ok_or_else(|| {
                                        protocol_error("quiet sample unavailable")
                                    })?,
                                    Some(&ticket),
                                )
                            } else {
                                store.authorize_condition_release_with_ticket(
                                    job,
                                    moment,
                                    Some(&ticket),
                                )
                            }
                        } else {
                            store
                                .mark_started_with_ticket(
                                    job,
                                    pid,
                                    &executable_hash,
                                    Some(&root),
                                    Some(&ticket),
                                )
                                .map(|()| ReleaseAuthorization::Authorized {
                                    runtime_deadline_unix_millis: job.attempt_deadline_unix_millis,
                                    evidence_expires_monotonic_millis: u64::MAX,
                                })
                        }
                        .map_err(protocol_error)?;
                        match authorization {
                            ReleaseAuthorization::Authorized {
                                runtime_deadline_unix_millis,
                                evidence_expires_monotonic_millis,
                            } => {
                                runtime_deadline.set(runtime_deadline_unix_millis);
                                evidence_expiry.set(evidence_expires_monotonic_millis);
                                consumed.set(true);
                                Ok(true)
                            }
                            ReleaseAuthorization::Deferred { reason } => {
                                *deferred.borrow_mut() = Some(reason.clone());
                                Err(protocol_error(reason))
                            }
                        }
                    },
                    || {
                        #[cfg(test)]
                        crate::test_support::linux_runtime_checkpoint("consumed");
                        child.release()?;
                        #[cfg(test)]
                        crate::test_support::linux_runtime_checkpoint("released");
                        Ok(())
                    },
                )?;
                Ok(disposition)
            };
        let released = if job.spec.requires_host_observation() {
            observation
                .with_release_sample(pid, |sample| release(Some(sample)))
                .map_err(io::Error::other)
                .and_then(|result| result.map_err(io::Error::other))
        } else {
            release(None).map_err(io::Error::other)
        };
        progress.durable_release_authorized = consumed.get();
        progress.never_run_reason = deferred.into_inner();
        match released? {
            Disposition::Released => {
                progress.user_code_released = true;
                progress.release_authorized = true;
            }
            Disposition::NeverReleased(proof) => {
                never = Some(proof);
                if is_guarded(job) {
                    progress.never_run_reason =
                        Some("readiness expired after Ticket commit".into());
                }
                return Err(io::Error::other("kernel release prevented after commit").into());
            }
            Disposition::AlreadyConsumed => {
                return Err(io::Error::other("Ticket already consumed").into());
            }
            Disposition::Uncertain(error) => {
                progress.user_code_released = true;
                return Err(error.into());
            }
        }
        loop {
            if let Some(exit) = child.poll_root()? {
                progress.exit_code = Some(exit);
                lock(store)?.mark_root_exited(job, exit)?;
                return Ok((exit as u32, false));
            }
            if lock(store)?.invocation_stop_requested(job.job_id)? {
                progress.canceled = true;
                return Ok((22, false));
            }
            if runtime_deadline.get().is_some_and(|deadline| {
                crate::host_observation::observation_clock()
                    .map_or(true, |(wall, _)| wall >= deadline)
            }) {
                progress.timed_out = true;
                return Ok((21, true));
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    })();
    let cleanup = (|| -> RunResult<()> {
        let seal = live
            .linux
            .cleanup(job.invocation_id, Instant::now() + Duration::from_secs(30))?;
        #[cfg(test)]
        crate::test_support::linux_runtime_checkpoint("sealed");
        lock(store)?.record_attached_cleanup(
            job.invocation_id,
            &seal.boundary_sha256,
            &seal.sha256()?,
            never.as_deref(),
        )?;
        live.linux_releases.retire(job.invocation_id)?;
        progress.cleanup_proven = true;
        if let Some(child) = launch.as_mut() {
            if let Some(exit) = child.reap_after_cleanup(Instant::now() + Duration::from_secs(5))? {
                progress.exit_code = Some(exit);
                if progress.durable_release_authorized {
                    lock(store)?.mark_root_exited(job, exit)?;
                }
            }
        }
        for drain in drains.drain(..) {
            drain
                .join()
                .map_err(|_| io::Error::other("log drain panicked"))??;
        }
        Ok(())
    })();
    wake();
    if !progress.cleanup_proven {
        if let Ok(mut locked) = store.lock() {
            lifecycle::persist_uncertain_cleanup(&mut locked, job, progress)?;
            progress.uncertainty_persisted = true;
        }
    }
    cleanup?;
    execution.map(|(code, timed_out)| {
        (
            progress.exit_code.map_or(code, |exit| exit as u32),
            timed_out,
        )
    })
}

fn is_guarded(job: &PreparedJob) -> bool {
    job.role == InvocationRole::Primary
        && (job.spec.quiet.is_some() || !job.spec.conditions.is_empty())
}
