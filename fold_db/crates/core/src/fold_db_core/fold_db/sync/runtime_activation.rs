//! Live activation retains the serving LastStore facade and exact capture fence.

use super::*;

impl FoldDB {
    /// The node verifies its durable adoption acknowledgement before calling
    /// this transition. Attach every serving slot while mutation admission is
    /// fenced; workers start only after routing and portable keys are ready.
    pub async fn activate_adopted_cloud_purge_engine(
        &self,
        engine: Arc<crate::sync::SyncEngine>,
    ) -> crate::error::FoldDbResult<()> {
        self.require_cloud_sync_supported()
            .map_err(crate::error::FoldDbError::Config)?;
        self.set_sync_engine(Arc::clone(&engine))
            .await
            .map_err(crate::error::FoldDbError::Config)?;
        self.apply_org_sync_targets_from_store().await?;
        engine.finish_primary_resume().await;
        engine.start_laststore_backup_uploader();
        self.start_sync(engine.config.sync_interval_ms)
            .map_err(crate::error::FoldDbError::Config)?;
        Ok(())
    }
}
