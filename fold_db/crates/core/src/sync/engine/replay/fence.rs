//! Admit every replay operation once, before its capture suppression scope.

use super::super::SyncEngine;
use crate::sync::capture::{with_capture_suppressed, with_mutation_admission};
use crate::sync::error::{SyncError, SyncResult};
use crate::sync::log::LogEntry;
use crate::sync::org_sync::SyncTarget;
use std::sync::atomic::Ordering;

impl SyncEngine {
    /// Replay a single log entry with convergent per-key molecule handling.
    ///
    /// Non-molecule keys (atoms, history) are written unconditionally.
    /// Per-key molecule records merge via LWW. Legacy whole-molecule `ref:`
    /// keys are dropped (product path retired).
    ///
    /// **Org-scoped keys are skipped** (consented drop — incident 2026-07-13):
    /// after the org-crypto map was removed, org-E2E ciphertext cannot be
    /// opened by the personal provider. Keys matching
    /// [`crate::sync::org_sync::storage_prefix_for_key`] are dropped with a loud count rather than
    /// failing bootstrap/sync replay with AES-GCM errors.
    pub async fn replay_entry(
        &self,
        entry: &LogEntry,
        target: Option<&SyncTarget>,
    ) -> SyncResult<()> {
        if self.backup_only_mode.load(Ordering::SeqCst) {
            return Err(SyncError::Storage(
                "paused-home backup blocks peer replay".into(),
            ));
        }
        let router = self.photograph_mutation_router.lock().await.clone();
        with_mutation_admission(
            router,
            with_capture_suppressed(self.replay_entry_inner(entry, target)),
        )
        .await
    }
}
