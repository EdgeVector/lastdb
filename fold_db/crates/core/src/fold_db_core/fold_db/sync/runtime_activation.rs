//! Live activation retains the serving LastStore facade and exact capture fence.

use super::*;

impl FoldDB {
    pub async fn start_sync_engine_runtime(
        &self,
        api_url: &str,
        api_key: &str,
        data_dir: &str,
        e2e_keys: &crate::crypto::E2eKeys,
        auth_refresh: Option<crate::sync::AuthRefreshCallback>,
    ) -> crate::error::FoldDbResult<()> {
        if let Some(engine) = self.sync_engine() {
            return self.activate_adopted_cloud_purge_engine(engine).await;
        }
        let source = self
            .cloud_purge_source_engine(api_url, api_key, data_dir, e2e_keys)
            .await?;
        let mut engine = Arc::try_unwrap(source).map_err(|_| {
            crate::error::FoldDbError::Config("runtime source engine has another owner".into())
        })?;
        if let Some(callback) = auth_refresh {
            engine.set_auth_refresh(callback);
        }
        self.activate_adopted_cloud_purge_engine(Arc::new(engine))
            .await
    }

    /// The node verifies its durable adoption acknowledgement before calling
    /// this transition. Attach every serving slot while mutation admission is
    /// fenced; workers start only after routing and portable keys are ready.
    pub async fn activate_adopted_cloud_purge_engine(
        &self,
        engine: Arc<crate::sync::SyncEngine>,
    ) -> crate::error::FoldDbResult<()> {
        self.set_sync_engine(Arc::clone(&engine)).await;
        self.apply_org_sync_targets_from_store().await?;
        engine.finish_primary_resume().await;
        engine.start_laststore_backup_uploader();
        self.start_sync(engine.config.sync_interval_ms);
        Ok(())
    }
}
