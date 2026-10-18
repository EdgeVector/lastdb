//! SyncCoordinator - manages the optional cloud sync engine lifecycle.
//!
//! Sync is opt-in. In local mode, the coordinator holds no engine and all
//! operations are no-ops (or return None). This type encapsulates the
//! interior mutability (RwLock + Mutex) needed so FoldDB can expose sync
//! operations via `&self`.

use crate::clock::unix_secs;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};

use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use std::sync::Arc;

use crate::db_operations::{
    AtomGcReapOptions, AtomGcReapReport, AtomStore, AutomaticGcAtomsDeleteOptions,
    AutomaticGcAtomsProbeOptions, AutomaticGcAtomsProbeResult, DbOperations,
};
use crate::sync::{SyncEngine, SyncError, SyncState, SyncStatus};

mod background;
mod backoff;
mod cadence;
mod gc_atoms;
mod shutdown;
use backoff::*;
pub use gc_atoms::AutomaticGcAtomsStatus;
use gc_atoms::*;

/// Coordinates the optional cloud sync engine lifecycle.
pub struct SyncCoordinator {
    engine: Arc<RwLock<Option<Arc<SyncEngine>>>>,
    task: Mutex<Option<JoinHandle<()>>>,
    /// Bounded marker-plane drain. It runs outside the serial cloud target
    /// cycle so a large residual queue does not wait for 40 scoped uploads.
    capture_reexport_task: Mutex<Option<JoinHandle<()>>>,
    /// Local gc-atoms cadence. Independent of `engine.sync()` success.
    gc_atoms_task: Mutex<Option<JoinHandle<()>>>,
    /// What the cadence has done, for `lastdb status`.
    gc_atoms_status: AutomaticGcAtomsStatusCell,
    /// Local plane self-compaction cadence. Independent of cloud sync: a node
    /// booted without `cloud_sync.json` runs the identical sweeps.
    plane_compaction_task: Mutex<Option<JoinHandle<()>>>,
    /// The compactor the local cadence steps while no engine is configured.
    /// Kept so `lastdb status` can read its last-fire reports.
    local_plane_compactor: RwLock<Option<Arc<crate::sync::capture::PlaneCompactor>>>,
    /// One packing lock for the process. Local sweeps and a later
    /// `SyncEngine` cut must share this Arc; a private dummy mutex at
    /// cadence start is the split-lock bug.
    backup_publish_target: crate::sync::capture::plane_compactor::BackupPublishTargetSlot,
    /// Cloud-pause slot paired with [`Self::backup_publish_target`].
    cloud_sync_disabled_at: Arc<tokio::sync::Mutex<Option<u64>>>,
    /// Last time a forced sync was actually dispatched while a replay blocker
    /// was armed — used to rate-limit `force_sync` under a blocker.
    last_forced_under_blocker: Mutex<Option<std::time::Instant>>,
}

impl SyncCoordinator {
    pub fn new() -> Self {
        Self::new_with_packing(
            Arc::new(tokio::sync::Mutex::new(None)),
            Arc::new(tokio::sync::Mutex::new(None)),
        )
    }

    /// Build a coordinator that already shares packing slots with a
    /// `SyncEngine` created first (factory cloud-on-at-boot).
    pub(crate) fn new_with_packing(
        backup_publish_target: crate::sync::capture::plane_compactor::BackupPublishTargetSlot,
        cloud_sync_disabled_at: Arc<tokio::sync::Mutex<Option<u64>>>,
    ) -> Self {
        Self {
            engine: Arc::new(RwLock::new(None)),
            task: Mutex::new(None),
            capture_reexport_task: Mutex::new(None),
            gc_atoms_task: Mutex::new(None),
            gc_atoms_status: AutomaticGcAtomsStatusCell::default(),
            plane_compaction_task: Mutex::new(None),
            local_plane_compactor: RwLock::new(None),
            backup_publish_target,
            cloud_sync_disabled_at,
            last_forced_under_blocker: Mutex::new(None),
        }
    }

    /// Packing lock and cloud-pause slot for [`SyncEngine::new_with_shared_packing`].
    pub(crate) fn packing_slots(
        &self,
    ) -> (
        crate::sync::capture::plane_compactor::BackupPublishTargetSlot,
        Arc<tokio::sync::Mutex<Option<u64>>>,
    ) {
        (
            Arc::clone(&self.backup_publish_target),
            Arc::clone(&self.cloud_sync_disabled_at),
        )
    }

    /// Store the sync engine. Caller is responsible for registering any
    /// reloader callbacks on the engine before calling this.
    pub(crate) fn set_engine(&self, engine: Arc<SyncEngine>) {
        *self.engine.write().unwrap() = Some(engine);
    }

    /// Returns a clone of the sync engine Arc, if configured.
    pub fn engine(&self) -> Option<Arc<SyncEngine>> {
        self.engine.read().unwrap().clone()
    }

    /// Returns true if a sync engine is configured.
    pub fn is_enabled(&self) -> bool {
        self.engine.read().unwrap().is_some()
    }

    /// Force an immediate sync. No-op if no engine is configured.
    ///
    /// While a replay blocker is armed (a corrupt cloud object or a failed
    /// pre-upload decrypt proof), forced syncs are rate-limited to
    /// [`MIN_FORCED_SYNC_UNDER_BLOCKER`]. Without this, a UI/API "sync now" (or
    /// a rapid succession of them) bypasses the inter-cycle failure backoff and
    /// hot-retriggers replay of the same poison object. Normal (unblocked)
    /// forced syncs are never throttled.
    pub async fn force_sync(&self) -> Result<(), SyncError> {
        self.force_sync_inner(false).await
    }

    async fn force_sync_inner(
        &self,
        bypass_replay_blocker_rate_limit: bool,
    ) -> Result<(), SyncError> {
        if let Some(engine) = self.engine() {
            let blocker_armed = engine.status().await.replay_blocker.is_some();
            if blocker_armed {
                let now = std::time::Instant::now();
                let suppress = {
                    // Scope the std Mutex guard so it is dropped before any await.
                    let mut last = self.last_forced_under_blocker.lock().unwrap();
                    replay_blocker_forced_sync_suppressed(
                        &mut last,
                        now,
                        bypass_replay_blocker_rate_limit,
                    )
                };
                if suppress {
                    debug!(
                        "force_sync suppressed: replay blocker armed and forced-sync rate limit not yet elapsed"
                    );
                    return Ok(());
                }
            }
            engine.sync().await?;
        }
        Ok(())
    }

    /// Get the sync engine state, if configured.
    pub async fn state(&self) -> Option<SyncState> {
        Some(self.engine()?.state().await)
    }

    /// Get a full sync status snapshot, if configured.
    pub async fn status(&self) -> Option<SyncStatus> {
        Some(self.engine()?.status().await)
    }

    /// Get the number of pending (unsynced) log entries, if configured.
    pub async fn pending_count(&self) -> Option<usize> {
        Some(self.engine()?.pending_count().await)
    }

    /// Abort the background sync, gc-atoms, and plane-compaction tasks without
    /// a final sync. Called from Drop to avoid tokio panics.
    pub(crate) fn abort_task(&self) {
        if let Some(handle) = self.task.lock().unwrap().take() {
            debug!("SyncCoordinator: aborting background sync task on drop");
            handle.abort();
        }
        if let Some(handle) = self.capture_reexport_task.lock().unwrap().take() {
            debug!("SyncCoordinator: aborting capture re-export drain on drop");
            handle.abort();
        }
        if let Some(handle) = self.gc_atoms_task.lock().unwrap().take() {
            debug!("SyncCoordinator: aborting automatic gc-atoms cadence on drop");
            handle.abort();
        }
        if let Some(handle) = self.plane_compaction_task.lock().unwrap().take() {
            debug!("SyncCoordinator: aborting automatic plane-compaction cadence on drop");
            handle.abort();
        }
    }
}

impl Default for SyncCoordinator {
    fn default() -> Self {
        Self::new()
    }
}
