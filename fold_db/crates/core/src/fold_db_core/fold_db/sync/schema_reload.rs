//! Keep best-effort sync reload separate from the required tail dependency.

use super::*;

impl FoldDB {
    pub(super) async fn register_sync_schema_reloaders(&self, engine: &crate::sync::SyncEngine) {
        let schema_mgr = Arc::clone(&self.schema_manager);
        engine
            .set_schema_reloader(Arc::new(move || {
                let mgr = Arc::clone(&schema_mgr);
                Box::pin(async move {
                    mgr.reload_from_store()
                        .await
                        .map_err(|e| format!("SchemaCore reload failed: {e}"))
                })
            }))
            .await;
        let schema_mgr = Arc::clone(&self.schema_manager);
        engine
            .set_schema_replay_reloader(Arc::new(move || {
                let mgr = Arc::clone(&schema_mgr);
                Box::pin(async move {
                    mgr.reload_for_replay()
                        .await
                        .map_err(|e| format!("SchemaCore replay reload failed: {e}"))
                })
            }))
            .await;
    }
}
