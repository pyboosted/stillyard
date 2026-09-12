use super::*;

impl Store {
    pub(crate) fn arm_bootstrap(
        &self,
        binding: crate::BootstrapBinding,
        requester: ProcessIdentity,
    ) -> StoreResult<crate::AuthoritySnapshot> {
        crate::bootstrap::validate_binding(&binding)?;
        let snapshot = self.status(binding.parent.job_id)?;
        let invocation = snapshot
            .attempts
            .iter()
            .flat_map(|attempt| &attempt.invocations)
            .find(|invocation| invocation.invocation_id == binding.parent.invocation_id)
            .ok_or_else(|| StoreError::Rejected("bootstrap primary is not present".into()))?;
        if invocation.root_identity.as_ref() != Some(&requester)
            || invocation.role != InvocationRole::Primary
            || snapshot.cancel_requested
            || snapshot.attempt_id != Some(binding.parent.attempt_id)
            || snapshot.invocation_id != Some(binding.parent.invocation_id)
        {
            return Err(StoreError::Rejected(
                "bootstrap requires the exact current native primary root".into(),
            ));
        }
        let (own, others): (u64, u64) = self.connection.query_row(
            "SELECT COALESCE(SUM(attempt_id = ?1 AND invocation_id IS NULL), 0),
                    COALESCE(SUM(attempt_id != ?1 OR invocation_id IS NOT NULL), 0)
             FROM leases WHERE state = 'granted'",
            [binding.parent.attempt_id.entity_uuid().to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if own != 1 || others != 0 {
            return Err(StoreError::OperationRejected { code: "authority_busy".into(), detail: "bootstrap requires its sole native work Lease; other live or uncertain work still owns resources".into() });
        }
        Ok(self.authority_lock()?.arm_bootstrap(binding, requester)?)
    }

    pub(crate) fn seal_bootstrap(
        &self,
        proof: crate::BootstrapProof,
        requester: ProcessIdentity,
    ) -> StoreResult<crate::AuthoritySnapshot> {
        Ok(self.authority_lock()?.seal_bootstrap(proof, requester)?)
    }

    /// Runtime attachment happens before the reactor can release any process.
    /// Store-only unit tests remain free to exercise the existing admission core.
    pub(crate) fn attach_authority(&mut self) -> StoreResult<()> {
        if self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM attached_local_mode)",
            [],
            |r| r.get::<_, bool>(0),
        )? {
            return Err(StoreError::InvalidState("attached executor requires its paired coordinator; standalone authority fallback is forbidden".into()));
        }
        let host = self
            .startup_identity
            .host_id
            .clone()
            .unwrap_or_else(|| HostId("identity-unavailable".into()));
        let mut authority =
            crate::authority::Authority::open(self.paths.root.join("authority"), host);
        if let Some(identity) = self.startup_identity.daemon_process.clone() {
            authority.bind_coordinator(self.store_uuid, self.daemon_generation, identity)?;
        }
        self.authority = Some(std::sync::Arc::new(std::sync::Mutex::new(authority)));
        self.reconcile_pending_domain_retirement()?;
        self.reconcile_pending_machine_commit()?;
        self.validate_machine_history()?;
        self.reconcile_native_start_permissions()?;
        self.establish_native_coverage()?;
        Ok(())
    }

    fn establish_native_coverage(&self) -> StoreResult<()> {
        let empty: bool = self.connection.query_row("SELECT NOT EXISTS(SELECT 1 FROM leases WHERE state='granted') AND NOT EXISTS(SELECT 1 FROM containments WHERE state NOT IN ('empty','cleared'))",[],|r|r.get(0))?;
        self.authority_lock()?
            .establish_native_coverage(self.store_uuid, empty)?;
        Ok(())
    }

    /// Build the same reset-independent permission for every native launch path.
    /// The caller publishes it after lifecycle validation, before recording a
    /// root in SQLite and before the suspended process can execute user code.
    pub(super) fn native_start_permission(
        &self,
        job: &PreparedJob,
        executable_hash: &str,
        root_identity: Option<&ProcessIdentity>,
    ) -> StoreResult<Option<crate::machine::NativeStartPermission>> {
        let permission = if self.authority.is_some() {
            let root_identity = root_identity.ok_or_else(|| {
                StoreError::InvalidState("native root identity unavailable".into())
            })?;
            let allocation = self
                .native_allocations(job.job_id)?
                .into_iter()
                .find(|a| {
                    a.state == crate::machine::GrantState::Armed
                        && match a.owner {
                            crate::machine::AllocationOwner::Work { attempt_id, .. } => {
                                job.role != InvocationRole::Probe && attempt_id == job.attempt_id
                            }
                            crate::machine::AllocationOwner::Probe { invocation_id, .. } => {
                                job.role == InvocationRole::Probe
                                    && invocation_id == job.invocation_id
                            }
                        }
                })
                .ok_or_else(|| StoreError::InvalidState("native root has no allocation".into()))?;
            Some(crate::machine::NativeStartPermission {
                allocation,
                invocation_id: job.invocation_id,
                containment_id: job.containment_id,
                root_identity: root_identity.clone(),
                creator_identity: self.startup_identity.daemon_process.clone().ok_or_else(
                    || StoreError::InvalidState("native creator identity unavailable".into()),
                )?,
                daemon_generation: self.daemon_generation,
                executable_sha256: executable_hash.into(),
                boundary_kind: "windows_job_object".into(),
            })
        } else {
            None
        };
        Ok(permission)
    }

    pub(super) fn reconcile_native_start_permissions(&self) -> StoreResult<()> {
        if self.authority.is_none() {
            return Ok(());
        }
        let permissions = self.authority_snapshot()?.native_obligations;
        let mut proven = Vec::new();
        for permission in permissions {
            if permission.invocation_id.store_uuid() != self.store_uuid {
                continue;
            }
            let empty: bool = self.connection.query_row("SELECT EXISTS(SELECT 1 FROM containments c JOIN invocations i ON c.invocation_id=i.id WHERE c.id=?1 AND i.id=?2 AND c.state IN ('empty','cleared') AND i.state='resolved')",
                params![permission.containment_id.entity_uuid().to_string(),permission.invocation_id.entity_uuid().to_string()],|r|r.get(0))?;
            if empty {
                proven.push(permission.invocation_id);
            }
        }
        if !proven.is_empty() {
            self.authority_lock()?.retire_native_starts(&proven)?;
        }
        Ok(())
    }

    pub(super) fn authority_lock(
        &self,
    ) -> StoreResult<std::sync::MutexGuard<'_, crate::authority::Authority>> {
        self.authority
            .as_ref()
            .ok_or_else(|| StoreError::InvalidState("runtime authority not attached".into()))?
            .lock()
            .map_err(|_| StoreError::InvalidState("authority mutex poisoned".into()))
    }

    pub(crate) fn authority_snapshot(&self) -> StoreResult<crate::AuthoritySnapshot> {
        Ok(self.authority_lock()?.snapshot())
    }

    pub(super) fn authority_blocker(&self) -> StoreResult<Option<Blocker>> {
        #[cfg(target_os = "linux")]
        super::attached::installation::validate_store(
            &self.paths.root,
            &self.connection,
            self.store_uuid,
        )?;
        let attached: Option<bool> = self
            .connection
            .query_row(
                "SELECT connected FROM attached_local_mode WHERE singleton=1",
                [],
                |r| r.get(0),
            )
            .optional()?;
        if attached == Some(false) {
            return Ok(Some(Blocker { code:"attached_disconnected".into(),
                detail:"Paired coordinator connection and inventory reconciliation are required before admission".into() }));
        }
        if self.authority.is_none() {
            return Ok(None);
        }
        Ok(self
            .authority_lock()?
            .admission_blocker()
            .map(|(code, detail)| Blocker { code, detail }))
    }

    pub(crate) fn check_authority_release(&self) -> StoreResult<()> {
        if let Some(blocker) = self.authority_blocker()? {
            return Err(StoreError::OperationRejected {
                code: blocker.code,
                detail: blocker.detail,
            });
        }
        Ok(())
    }

    fn require_no_granted_leases(&self) -> StoreResult<()> {
        let active: bool = self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM leases WHERE state = 'granted')",
            [],
            |row| row.get(0),
        )?;
        if active {
            return Err(StoreError::OperationRejected {
                code: "authority_busy".into(),
                detail: "Existing work or uncertain containment still owns a Lease; wait for its safe completion".into(),
            });
        }
        Ok(())
    }

    pub(crate) fn initialize_authority(&self) -> StoreResult<crate::AuthoritySnapshot> {
        if !self.startup_identity.capable() {
            return Err(StoreError::InvalidState(
                "Cannot initialize without verified host/process identity".into(),
            ));
        }
        if self.authority_snapshot()?.epoch.is_none() {
            self.require_no_granted_leases()?;
        }
        let mut authority = self.authority_lock()?;
        authority.initialize()?;
        if let Some(identity) = self.startup_identity.daemon_process.clone() {
            authority.bind_coordinator(self.store_uuid, self.daemon_generation, identity)?;
        }
        drop(authority);
        self.establish_native_coverage()?;
        self.authority_snapshot()
    }

    pub(crate) fn hold_authority(
        &self,
        id: Uuid,
        reason: String,
        requester: ProcessIdentity,
    ) -> StoreResult<crate::AuthoritySnapshot> {
        // This operation and admission/release share the Store mutex. An empty
        // snapshot followed by a separate file write would have a launch race.
        self.require_no_granted_leases()?;
        Ok(self.authority_lock()?.hold(id, reason, requester)?)
    }

    pub(crate) fn force_release_authority(
        &self,
        id: Uuid,
        reason: String,
        requester: ProcessIdentity,
    ) -> StoreResult<crate::AuthoritySnapshot> {
        Ok(self
            .authority_lock()?
            .force_release(id, reason, requester)?)
    }
}
