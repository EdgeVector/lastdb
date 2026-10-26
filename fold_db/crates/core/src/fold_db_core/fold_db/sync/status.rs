//! Local maintenance status.
use super::super::FoldDB;
impl FoldDB {
    pub async fn automatic_compaction_status(
        &self,
    ) -> Option<std::collections::BTreeMap<String, crate::sync::engine::AutomaticCompactionStatus>>
    {
        self.sync_coordinator.automatic_compaction_status().await
    }
}
