use std::sync::Arc;

use crate::fold_db_core::sync_coordinator::SyncCoordinator;

use super::FoldDB;
mod cloud_purge_source;
mod cloud_resume;
mod engine_attach;
mod org_republish;
mod org_targets;
mod status;

mod inbound_targets;
mod replay_applier;
mod retired_applier;
mod runtime_activation;
mod schema_reload;

impl FoldDB {
    pub const CLOUD_SYNC_DISABLED: &'static str =
        "native LastDB cloud sync is disabled until its backup and cloud-sync gate is complete";

    /// Fail before any local or remote cloud activation effect.
    pub fn require_cloud_sync_supported(&self) -> Result<(), String> {
        Err(Self::CLOUD_SYNC_DISABLED.into())
    }

    /// Returns a reference to the sync coordinator.
    pub fn sync_coordinator(&self) -> &SyncCoordinator {
        &self.sync_coordinator
    }

    /// Set the sync engine (called by the factory when sync is configured).
    /// Also registers the schema reloader callback so SchemaCore's in-memory
    /// cache is refreshed after sync replays schema entries.
    ///
    /// Registration lives here because it needs access to FoldDB-owned
    /// components (SchemaCore); engine storage is then delegated to the
    /// SyncCoordinator. In-process native index reloading was retired;
    /// Search app owns semantic recall.
    pub async fn set_sync_engine(
        &self,
        engine: Arc<crate::sync::SyncEngine>,
    ) -> Result<(), String> {
        self.set_sync_engine_with_capture(engine, true).await
    }
}
