//! Shared cloud activity and progress state, independent of the node host.

use cloud_backup_protocol::Id;
use serde::Serialize;
use std::sync::Mutex;

#[derive(Default)]
pub struct SharedSyncRuntime {
    pub(crate) gate: tokio::sync::Mutex<()>,
    state: Mutex<SharedSyncProgress>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct SharedSyncProgress {
    pub running: bool,
    pub last_sync_at: Option<u64>,
    pub last_upload_at: Option<u64>,
    pub next_sync_at: u64,
    pub retry_at: u64,
    pub uploaded_through: u64,
    pub applied_ranges: u64,
    pub last_peer_applied: Option<Id>,
    pub last_peer_through: Option<u64>,
    pub reclaimed_delete_notes: u64,
    pub needs_restore: Option<Id>,
    pub blocked_reason: Option<&'static str>,
}

impl SharedSyncRuntime {
    pub(crate) fn update(&self, change: impl FnOnce(&mut SharedSyncProgress)) {
        change(&mut self.state.lock().expect("shared sync progress"));
    }
    pub(crate) fn snapshot(&self) -> SharedSyncProgress {
        self.state.lock().expect("shared sync progress").clone()
    }
}
