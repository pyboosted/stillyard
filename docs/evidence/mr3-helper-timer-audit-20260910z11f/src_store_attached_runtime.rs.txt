//! Installed attached mode is selected from its reset-independent anchor.
use super::*;
use std::sync::Arc;

impl Store {
    pub(crate) fn attached_runtime(
        &mut self,
        endpoint: &str,
    ) -> StoreResult<Option<(crate::runner::LiveContainments, Arc<driver::ReleaseState>)>> {
        installation::validate_store(&self.paths.root, &self.connection, self.store_uuid)?;
        let Some(config) = installation::load(&self.paths.root)? else {
            return Ok(None);
        };
        let registry = crate::runner::linux::installed_registry(
            &self.paths.root.join("attachment/executor"),
            config.journal,
            self.store_uuid,
            config.pairing.installation.domain_id,
            self.daemon_generation,
            endpoint.into(),
        )?;
        for (invocation, boundary, proof) in registry.durable_seals()? {
            self.record_attached_cleanup(invocation, &boundary, &proof, None)?;
        }
        let releases = Arc::new(driver::ReleaseState::default());
        Ok(Some((
            crate::runner::LiveContainments::attached_linux(registry, Arc::clone(&releases)),
            releases,
        )))
    }
    pub(crate) fn attached_offline(&self) -> StoreResult<()> {
        self.connection
            .execute("UPDATE attached_local_mode SET connected=0", [])?;
        Ok(())
    }
    pub(crate) fn attached_poll_interval(&self) -> StoreResult<std::time::Duration> {
        let pending: bool = self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM attached_local_plans WHERE committed=0 AND released=0)",
            [],
            |r| r.get(0),
        )?;
        Ok(std::time::Duration::from_millis(if pending {
            100
        } else {
            20_000
        }))
    }
}
