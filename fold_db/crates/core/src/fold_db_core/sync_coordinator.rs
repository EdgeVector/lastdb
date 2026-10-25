//! Local automatic compaction ownership. The legacy cloud engine is retired.
use crate::db_operations::DbOperations;
use std::sync::{Arc, Mutex, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, info};

mod cadence;
mod shutdown;

pub struct SyncCoordinator {
    plane_compaction_task: Mutex<Option<JoinHandle<()>>>,
    local_plane_compactor: RwLock<Option<Arc<crate::sync::capture::PlaneCompactor>>>,
    backup_publish_target: crate::sync::capture::plane_compactor::BackupPublishTargetSlot,
    cloud_sync_disabled_at: Arc<tokio::sync::Mutex<Option<u64>>>,
}
impl SyncCoordinator {
    pub fn new() -> Self {
        Self {
            plane_compaction_task: Mutex::new(None),
            local_plane_compactor: RwLock::new(None),
            backup_publish_target: Arc::new(tokio::sync::Mutex::new(None)),
            cloud_sync_disabled_at: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }
    pub(crate) fn abort_task(&self) {
        if let Some(handle) = self.plane_compaction_task.lock().unwrap().take() {
            debug!("local automatic compaction cadence stops");
            handle.abort();
        }
    }
}
impl Default for SyncCoordinator {
    fn default() -> Self {
        Self::new()
    }
}
