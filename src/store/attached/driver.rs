//! One persistent transport, durable outbox, and generation-local release barriers.
//! Network I/O never retains the Store mutex used by cancellation and settlement.
use super::*;
use crate::machine::{
    bridge::{BridgeCommand, BridgeOutcome},
    manager,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

type SharedStore = Arc<Mutex<Store>>;
type Barrier = Arc<manager::release::ReleaseBarrier>;

#[derive(Default)]
pub(crate) struct ReleaseState {
    barriers: Mutex<BTreeMap<InvocationId, Barrier>>,
    signal: (Mutex<bool>, std::sync::Condvar),
}
impl ReleaseState {
    pub(crate) fn wake(&self) {
        if let Ok(mut pending) = self.signal.0.lock() {
            *pending = true;
            self.signal.1.notify_one();
        }
    }
    pub(crate) fn wait(&self, duration: Duration) {
        if let Ok(pending) = self.signal.0.lock() {
            if let Ok((mut pending, result)) =
                self.signal
                    .1
                    .wait_timeout_while(pending, duration, |pending| !*pending)
            {
                crate::runtime_metrics::waited(
                    crate::runtime_metrics::Timer::Attached,
                    result.timed_out(),
                );
                *pending = false;
            }
        }
    }
    pub(crate) fn challenge(
        &self,
        request: &crate::machine::Request,
        generation: Uuid,
        configuration: String,
    ) -> StoreResult<()> {
        let Command::AuthorizeInvocation { intent, .. } = &request.command else {
            return Err(StoreError::InvalidState(
                "release barrier requires an Invocation intent".into(),
            ));
        };
        let mut barriers = self
            .barriers
            .lock()
            .map_err(|_| StoreError::InvalidState("release registry poisoned".into()))?;
        if barriers.len() >= 1024 && !barriers.contains_key(&intent.invocation_id) {
            return Err(StoreError::InvalidState(
                "release registry capacity reached".into(),
            ));
        }
        let barrier = match barriers.entry(intent.invocation_id) {
            std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::btree_map::Entry::Vacant(entry) => entry.insert(Arc::new(
                manager::release::ReleaseBarrier::new(
                    request.session.clone(),
                    generation,
                    configuration,
                )
                .map_err(protocol_error)?,
            )),
        };
        barrier.challenge(request).map_err(protocol_error)
    }
    pub(crate) fn retire(&self, invocation: InvocationId) -> StoreResult<()> {
        let barrier = self
            .barriers
            .lock()
            .map_err(|_| StoreError::InvalidState("release registry poisoned".into()))?
            .remove(&invocation);
        if let Some(barrier) = barrier {
            barrier.cancel().map_err(protocol_error)?;
        }
        Ok(())
    }
    /// Call only after the matching authenticated outcome is durably accepted.
    pub(crate) fn accepted_rejection(
        &self,
        request: &crate::machine::Request,
        outcome: &crate::machine::Outcome,
    ) -> StoreResult<()> {
        if let (
            Command::AuthorizeInvocation { intent, .. },
            crate::machine::Outcome::Rejected { .. },
        ) = (&request.command, outcome)
        {
            self.retire(intent.invocation_id)?;
        }
        Ok(())
    }
    pub(crate) fn barrier(&self, invocation: InvocationId) -> StoreResult<Barrier> {
        self.barriers
            .lock()
            .map_err(|_| StoreError::InvalidState("release registry poisoned".into()))?
            .get(&invocation)
            .cloned()
            .ok_or_else(|| {
                StoreError::InvalidState("Invocation has no live transmission barrier".into())
            })
    }
    /// Never hold Store while canceling these barriers: release takes Store
    /// first, then its barrier, to order a concurrent cancellation commit.
    pub(crate) fn disconnect(&self) -> StoreResult<()> {
        let old = std::mem::take(
            &mut *self
                .barriers
                .lock()
                .map_err(|_| StoreError::InvalidState("release registry poisoned".into()))?,
        );
        for barrier in old.into_values() {
            barrier.cancel().map_err(protocol_error)?;
        }
        Ok(())
    }
}

pub(crate) struct Driver {
    configuration: installation::Configuration,
    bridge: crate::machine::bridge::linux::Bridge,
    releases: Arc<ReleaseState>,
    generation: Uuid,
    last_heartbeat: Instant,
    last_observation: Instant,
}
fn lock(store: &SharedStore) -> StoreResult<std::sync::MutexGuard<'_, Store>> {
    store
        .lock()
        .map_err(|_| StoreError::InvalidState("store mutex poisoned".into()))
}
fn bridge_error(e: crate::Error) -> StoreError {
    StoreError::InvalidState(e.to_string())
}

impl Driver {
    pub(crate) fn connect(store: &SharedStore, releases: Arc<ReleaseState>) -> StoreResult<Self> {
        releases.disconnect()?;
        let (configuration, generation) = {
            let store = lock(store)?;
            installation::validate_store(&store.paths.root, &store.connection, store.store_uuid)?;
            store
                .connection
                .execute("UPDATE attached_local_mode SET connected=0", [])?;
            let configuration = installation::load(&store.paths.root)?
                .ok_or_else(|| StoreError::InvalidState("installed attachment missing".into()))?;
            (configuration, store.daemon_generation)
        };
        let mut bridge = crate::machine::bridge::linux::Bridge::spawn(
            &configuration.bridge_executable,
            &configuration.bridge_sha256,
            &configuration.coordinator_endpoint,
            &configuration.interop_socket,
        )?;
        let mut nonce = [0; 32];
        getrandom::fill(&mut nonce).map_err(|e| StoreError::Io(std::io::Error::other(e)))?;
        let hello = crate::machine::ConnectHello {
            installation_nonce: configuration.pairing.installation.installation_nonce,
            manager_store_uuid: configuration.pairing.manager_store_uuid,
            executor_incarnation: generation,
            executor_protocol: crate::protocol::PROTOCOL_VERSION,
            executor_nonce: nonce,
        };
        let BridgeOutcome::Challenge { challenge } = bridge
            .exchange(
                BridgeCommand::ConnectBegin { hello },
                Instant::now() + Duration::from_secs(5),
            )
            .map_err(bridge_error)?
        else {
            return Err(StoreError::InvalidState(
                "bridge returned no handshake challenge".into(),
            ));
        };
        if challenge.executor_nonce != nonce
            || challenge.installation != configuration.pairing.installation
            || challenge.coordinator_installation != configuration.coordinator_installation
            || challenge.session.machine_id != configuration.machine_id
            || challenge.session.manager_store_uuid != configuration.pairing.manager_store_uuid
            || challenge.session.domain_id != configuration.pairing.installation.domain_id
            || challenge.session.executor_incarnation != generation
            || challenge.executor_protocol != crate::protocol::PROTOCOL_VERSION
            || challenge.coordinator_protocol != crate::protocol::PROTOCOL_VERSION
        {
            return Err(StoreError::InvalidState(
                "coordinator handshake differs from installed attachment".into(),
            ));
        }
        let secret = crate::machine::PairingSecret::from_anchor(configuration.pairing.secret);
        let tag = secret.sign_challenge(&challenge)?;
        let session = challenge.session.clone();
        let BridgeOutcome::Participant { participant } = bridge
            .exchange(
                BridgeCommand::ConnectFinish { challenge, tag },
                Instant::now() + Duration::from_secs(5),
            )
            .map_err(bridge_error)?
        else {
            return Err(StoreError::InvalidState(
                "bridge returned no authenticated participant".into(),
            ));
        };
        if participant.installation != configuration.pairing.installation {
            return Err(StoreError::InvalidState(
                "authenticated participant changed installation".into(),
            ));
        }
        {
            let mut store = lock(store)?;
            installation::validate_store(&store.paths.root, &store.connection, store.store_uuid)?;
            let tx = store.connection.transaction()?;
            // Bind validates continuous local watermarks. Inventory recovery
            // imports obligations, never resurrects a consumed launch ticket.
            let prior: Option<String> = tx.query_row(
                "SELECT session_json FROM attached_peer WHERE singleton=1",
                [],
                |r| r.get(0),
            )?;
            if prior.is_none() {
                manager::bind(&tx, &session, &participant).map_err(protocol_error)?;
            }
            manager::recovery::begin(&tx, &session, &participant).map_err(protocol_error)?;
            tx.execute(
                "UPDATE attached_local_mode SET session_json=?1,connected=0",
                [serde_json::to_string(&session)?],
            )?;
            tx.commit()?;
        }
        let driver = Self {
            configuration,
            bridge,
            releases,
            generation,
            last_heartbeat: Instant::now(),
            last_observation: Instant::now(),
        };
        let mut driver = driver;
        driver.refresh_observation(store)?;
        Ok(driver)
    }

    fn refresh_observation(&mut self, store: &SharedStore) -> StoreResult<()> {
        let BridgeOutcome::Scheduling {
            snapshot: Some(snapshot),
        } = self
            .bridge
            .exchange(
                BridgeCommand::SchedulingStatus,
                Instant::now() + Duration::from_secs(2),
            )
            .map_err(bridge_error)?
        else {
            return Err(StoreError::InvalidState(
                "coordinator scheduling evidence is unavailable".into(),
            ));
        };
        let store = lock(store)?;
        store.record_attached_machine_observation(&snapshot)?;
        self.last_observation = Instant::now();
        let changed: bool = store.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM attached_local_mode WHERE connected=1 AND configuration_sha256!=?1)",
            [&snapshot.configuration_sha256], |r| r.get(0),
        )?;
        if changed {
            // Diagnostic evidence can fence a stale session, never authorize a
            // new configuration. Full authenticated inventory binds that hash.
            return Err(StoreError::InvalidState(
                "coordinator configuration changed; full inventory is required".into(),
            ));
        }
        Ok(())
    }

    /// Return whether an operation was exchanged. The caller uses the reactor's
    /// wake signal and bounded backoff for idle/reconnect; no busy-spin thread.
    pub(crate) fn step(&mut self, store: &SharedStore) -> StoreResult<bool> {
        // Busy protocol traffic must not indefinitely postpone observations.
        if self.last_observation.elapsed() >= Duration::from_secs(20) {
            self.refresh_observation(store)?;
        }
        let secret = crate::machine::PairingSecret::from_anchor(self.configuration.pairing.secret);
        let pending = {
            let mut store = lock(store)?;
            installation::validate_store(&store.paths.root, &store.connection, store.store_uuid)?;
            let tx = store.connection.transaction()?;
            if manager::pending(&tx, &secret)
                .map_err(protocol_error)?
                .is_none()
            {
                let recovering: bool = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM attached_meta WHERE key='recovery')",
                    [],
                    |r| r.get(0),
                )?;
                if recovering {
                    if let Some(command) =
                        manager::recovery::next_page(&tx).map_err(protocol_error)?
                    {
                        manager::enqueue(&tx, Uuid::now_v7(), &command).map_err(protocol_error)?;
                    } else {
                        for command in
                            manager::recovery::reconciliation(&tx).map_err(protocol_error)?
                        {
                            manager::enqueue(&tx, Uuid::now_v7(), &command)
                                .map_err(protocol_error)?;
                        }
                    }
                } else {
                    super::queue::maintain(&tx)?;
                }
            }
            let pending = manager::pending(&tx, &secret).map_err(protocol_error)?;
            tx.commit()?;
            pending
        };
        let Some(request) = pending else {
            if self.last_heartbeat.elapsed() >= Duration::from_secs(5) {
                let outcome = self
                    .bridge
                    .exchange(
                        BridgeCommand::Participant {
                            domain: self.configuration.pairing.installation.domain_id,
                        },
                        Instant::now() + Duration::from_secs(2),
                    )
                    .map_err(bridge_error)?;
                let BridgeOutcome::Participant { participant } = outcome else {
                    return Err(StoreError::InvalidState(
                        "invalid bridge heartbeat response".into(),
                    ));
                };
                if participant.executor_incarnation != Some(self.generation)
                    || participant.reconciliation_required
                {
                    return Err(StoreError::InvalidState(
                        "coordinator fenced the executor session".into(),
                    ));
                }
                self.refresh_observation(store)?;
                self.last_heartbeat = Instant::now();
            }
            return Ok(false);
        };
        if matches!(&request.command, Command::AuthorizeInvocation { .. }) {
            let configuration: String = lock(store)?.connection.query_row(
                "SELECT configuration_sha256 FROM attached_local_mode",
                [],
                |r| r.get(0),
            )?;
            self.releases
                .challenge(&request, self.generation, configuration)?;
        }
        let BridgeOutcome::Exchange { reply } = self
            .bridge
            .exchange(
                BridgeCommand::Exchange {
                    request: Box::new(request.clone()),
                },
                Instant::now() + Duration::from_secs(2),
            )
            .map_err(bridge_error)?
        else {
            return Err(StoreError::InvalidState(
                "bridge returned no protocol reply".into(),
            ));
        };
        let mut store = lock(store)?;
        installation::validate_store(&store.paths.root, &store.connection, store.store_uuid)?;
        let tx = store.connection.transaction()?;
        accept(&tx, &request, &reply)?;
        if let crate::machine::Outcome::InventoryPage {
            configuration_sha256,
            ..
        } = &reply.outcome
        {
            tx.execute(
                "UPDATE attached_local_mode SET configuration_sha256=?1",
                [configuration_sha256],
            )?;
        }
        if let crate::machine::Outcome::Reconciled { .. } = &reply.outcome {
            tx.execute("UPDATE attached_local_mode SET connected=1", [])?;
        }
        tx.commit()?;
        // A durably accepted rejection proves THIS operation issued no Ticket.
        // An unanswered operation or issued Ticket never gets a new clock.
        self.releases.accepted_rejection(&request, &reply.outcome)?;
        self.last_heartbeat = Instant::now();
        Ok(true)
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        // Stopping a pipe never manufactures a cleanup seal or releases tokens.
        let _ = self.releases.disconnect();
    }
}
