//! Passive compatibility and local maintenance; no cloud effect is admitted.
use super::super::FoldDB;
impl FoldDB {
    pub async fn sync_state(&self) -> Option<crate::sync::SyncState> {
        std::future::ready(None).await
    }
    pub async fn sync_status(&self) -> Option<crate::sync::SyncStatus> {
        std::future::ready(None).await
    }
    pub fn is_sync_enabled(&self) -> bool {
        false
    }
    pub async fn automatic_compaction_status(
        &self,
    ) -> Option<std::collections::BTreeMap<String, crate::sync::engine::AutomaticCompactionStatus>>
    {
        self.sync_coordinator.automatic_compaction_status().await
    }
    pub async fn backup_publish_target_is_held(&self) -> bool {
        std::future::ready(false).await
    }
    pub async fn run_unless_backup_publish_target_held<T, F, Fut>(&self, op: F) -> Option<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        Some(op().await)
    }
}
