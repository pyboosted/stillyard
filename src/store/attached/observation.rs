//! Cached coordinator diagnostics never authorize a launch or release a debit.
use super::*;

pub(super) fn initialize(c: &Connection) -> StoreResult<()> {
    c.execute_batch(
        "CREATE TABLE IF NOT EXISTS attached_machine_observation(
        singleton INTEGER PRIMARY KEY CHECK(singleton=1), snapshot_json TEXT NOT NULL)",
    )?;
    Ok(())
}

fn validate(
    snapshot: &crate::MachineSchedulingSnapshot,
    session: &SessionIdentity,
) -> StoreResult<()> {
    if snapshot.domains.machine_id != session.machine_id
        || snapshot.authority_epoch != session.authority_epoch
        || snapshot.mode != crate::MachineSchedulingMode::Coordinator
        || snapshot.configuration_sha256.len() != 64
        || snapshot.resources.len() > 4096
    {
        return Err(StoreError::InvalidState(
            "coordinator observation differs from authenticated session".into(),
        ));
    }
    Ok(())
}

impl Store {
    pub(crate) fn attached_allocations(
        &self,
        job: JobId,
    ) -> StoreResult<Vec<crate::machine::NativeAllocationSnapshot>> {
        let mut statement = self.connection.prepare(
            "SELECT g.grant_json FROM attached_grants g
            JOIN attached_local_plans p ON p.allocation_key=g.allocation_key
            WHERE p.job_id=?1 ORDER BY p.rowid",
        )?;
        let rows =
            statement.query_map([job.entity_uuid().to_string()], |r| r.get::<_, String>(0))?;
        let mut allocations = Vec::new();
        for row in rows {
            let grant: crate::machine::GrantSnapshot = serde_json::from_str(&row?)?;
            allocations.push(crate::machine::NativeAllocationSnapshot {
                grant_id: grant.grant_id,
                lease_id: grant.candidate.key.lease_id,
                key: Some(grant.candidate.key),
                owner: grant.candidate.owner,
                state: grant.state,
                claims: grant.candidate.claims,
            });
        }
        Ok(allocations)
    }

    pub(crate) fn record_attached_machine_observation(
        &self,
        snapshot: &crate::MachineSchedulingSnapshot,
    ) -> StoreResult<()> {
        let session: String =
            self.connection
                .query_row("SELECT session_json FROM attached_local_mode", [], |r| {
                    r.get(0)
                })?;
        let session: SessionIdentity = serde_json::from_str(&session)?;
        validate(snapshot, &session)?;
        let json = serde_json::to_string(snapshot)?;
        if json.len() > 1024 * 1024 {
            return Err(StoreError::InvalidState(
                "coordinator observation exceeds byte bound".into(),
            ));
        }
        self.connection.execute(
            "INSERT INTO attached_machine_observation VALUES (1,?1)
            ON CONFLICT(singleton) DO UPDATE SET snapshot_json=excluded.snapshot_json",
            [json],
        )?;
        Ok(())
    }

    pub(crate) fn attached_machine_snapshot(
        &self,
    ) -> StoreResult<Option<crate::MachineSchedulingSnapshot>> {
        let mode: Option<(Option<String>, bool)> = self
            .connection
            .query_row(
                "SELECT session_json,connected FROM attached_local_mode",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        let Some((Some(session), connected)) = mode else {
            return Ok(None);
        };
        let session: SessionIdentity = serde_json::from_str(&session)?;
        let json: Option<String> = self
            .connection
            .query_row(
                "SELECT snapshot_json FROM attached_machine_observation",
                [],
                |r| r.get(0),
            )
            .optional()?;
        let Some(json) = json else {
            return Ok(None);
        };
        let mut snapshot: crate::MachineSchedulingSnapshot = serde_json::from_str(&json)?;
        // An older epoch's data is unavailable, not a representation of a new
        // authority's usage. Same-epoch stale counts keep their original time.
        if validate(&snapshot, &session).is_err() {
            return Ok(None);
        }
        snapshot.mode = crate::MachineSchedulingMode::Attached;
        let age = now_millis().saturating_sub(snapshot.observed_unix_millis);
        if !connected {
            snapshot.blocker = Some(Blocker { code: "attached_disconnected".into(),
                detail: "Retained coordinator observation; connection and reconciliation are unavailable".into() });
        } else if !(0..=30_000).contains(&age) {
            snapshot.blocker = Some(Blocker {
                code: "machine_observation_stale".into(),
                detail:
                    "Coordinator observation is older than 30 seconds or clock continuity changed"
                        .into(),
            });
        }
        Ok(Some(snapshot))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn attached_observation_preserves_counts_and_fences_stale_sessions() {
        let temp = crate::test_support::durable_tempdir().unwrap();
        let store = Store::open(StorePaths::new(temp.path().to_path_buf())).unwrap();
        let session = SessionIdentity {
            machine_id: Uuid::now_v7(),
            authority_epoch: Uuid::now_v7(),
            domain_id: crate::ExecutionDomainId(Uuid::now_v7()),
            manager_store_uuid: store.store_uuid(),
            executor_incarnation: store.daemon_generation(),
            connection_epoch: 1,
        };
        store.connection.execute("INSERT INTO attached_local_mode(singleton,store_uuid,session_json,connected) VALUES(1,?1,?2,1)",
            params![store.store_uuid().to_string(),serde_json::to_string(&session).unwrap()]).unwrap();
        let mut snapshot = crate::MachineSchedulingSnapshot {
            authority_epoch: session.authority_epoch,
            domains: crate::AuthorityDomains {
                machine_id: session.machine_id,
                machine_scope: crate::ExecutionDomainId(Uuid::now_v7()),
                native_domain: crate::ExecutionDomainId(Uuid::now_v7()),
            },
            mode: crate::MachineSchedulingMode::Coordinator,
            observed_unix_millis: now_millis(),
            configuration_sha256: "a".repeat(64),
            resources: vec![crate::ScopedResourceSnapshot {
                resource: crate::ScopedResourceId {
                    scope: session.domain_id,
                    kind: crate::ResourceKind::Scalar,
                    resource_id: "cargo_slots".into(),
                },
                capacity: 2,
                granted: 1,
                offered: 1,
                reserved: 0,
            }],
            blocker: None,
        };
        store
            .record_attached_machine_observation(&snapshot)
            .unwrap();
        let observed = store.attached_machine_snapshot().unwrap().unwrap();
        assert_eq!(observed.mode, crate::MachineSchedulingMode::Attached);
        assert_eq!(observed.resources, snapshot.resources);
        assert_eq!(observed.observed_unix_millis, snapshot.observed_unix_millis);
        assert!(observed.blocker.is_none());
        snapshot.observed_unix_millis -= 30_001;
        store
            .record_attached_machine_observation(&snapshot)
            .unwrap();
        assert_eq!(
            store
                .attached_machine_snapshot()
                .unwrap()
                .unwrap()
                .blocker
                .unwrap()
                .code,
            "machine_observation_stale"
        );
        store
            .connection
            .execute("UPDATE attached_local_mode SET connected=0", [])
            .unwrap();
        assert_eq!(
            store
                .attached_machine_snapshot()
                .unwrap()
                .unwrap()
                .blocker
                .unwrap()
                .code,
            "attached_disconnected"
        );
        snapshot.authority_epoch = Uuid::now_v7();
        assert!(
            store
                .record_attached_machine_observation(&snapshot)
                .is_err()
        );
        store
            .connection
            .execute(
                "UPDATE attached_local_mode SET session_json=?1",
                [serde_json::to_string(&SessionIdentity {
                    authority_epoch: Uuid::now_v7(),
                    ..session
                })
                .unwrap()],
            )
            .unwrap();
        assert!(store.attached_machine_snapshot().unwrap().is_none());
    }
}
