//! Passive legacy status and local maintenance. No cloud engine can attach.
use super::FoldDB;
use crate::fold_db_core::sync_coordinator::SyncCoordinator;
mod status;
impl FoldDB {
    pub const CLOUD_SYNC_DISABLED: &'static str =
        "legacy cloud sync is retired; use byte backup V2";
    pub fn require_cloud_sync_supported(&self) -> Result<(), String> {
        Err(Self::CLOUD_SYNC_DISABLED.into())
    }
    pub fn sync_coordinator(&self) -> &SyncCoordinator {
        &self.sync_coordinator
    }
}
