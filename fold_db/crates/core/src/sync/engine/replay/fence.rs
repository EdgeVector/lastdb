//! Admit every replay operation once, before its capture suppression scope.

use super::super::SyncEngine;
use crate::sync::capture::{with_capture_suppressed, with_mutation_admission};
use crate::sync::engine::pin_log::ReplayEntryOutcome;
use crate::sync::engine::RetiredReplaySource;
use crate::sync::error::{SyncError, SyncResult};
use crate::sync::log::LogEntry;
use crate::sync::org_sync::SyncTarget;
use std::sync::atomic::Ordering;

impl SyncEngine {
    /// Replay complete mutations through the normal counter writer. Raw
    /// lifecycle entries fail before their first effect; opaque metadata keeps
    /// its own replay policy. Retired key formats remain unsupported.
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
        self.replay_entry_with_schema_refresh(entry, target, false, None)
            .await
            .map(|_| ())
    }

    pub(crate) async fn replay_entry_with_schema_refresh(
        &self,
        entry: &LogEntry,
        target: Option<&SyncTarget>,
        refresh_schemas: bool,
        source: Option<RetiredReplaySource>,
    ) -> SyncResult<ReplayEntryOutcome> {
        if self.backup_only_mode.load(Ordering::SeqCst) {
            return Err(SyncError::Storage(
                "paused-home backup blocks peer replay".into(),
            ));
        }
        let router = self.photograph_mutation_router.lock().await.clone();
        with_mutation_admission(
            router,
            with_capture_suppressed(async {
                if refresh_schemas {
                    self.reload_replayed_schemas().await?;
                }
                if target.is_none() {
                    let preserver = self.retired_intent_preserver.lock().await.clone();
                    if let (Some(preserver), Some(source)) = (preserver, source) {
                        if let Some(mutations) = preserver(source).await.map_err(|error| {
                            SyncError::ReplayApplyFailed {
                                target: "mutation_intent".into(),
                                seq: entry.seq,
                                reason: error.reason,
                                diagnosis: error.diagnosis,
                            }
                        })? {
                            return Ok(ReplayEntryOutcome::RetiredAuthorityPreserved { mutations });
                        }
                    }
                }
                self.replay_entry_inner(entry, target).await?;
                Ok(ReplayEntryOutcome::Applied)
            }),
        )
        .await
    }

    pub(crate) async fn refresh_replayed_schemas(&self) -> SyncResult<()> {
        if self.backup_only_mode.load(Ordering::SeqCst) {
            return Err(SyncError::Storage(
                "paused-home backup blocks peer replay".into(),
            ));
        }
        let router = self.photograph_mutation_router.lock().await.clone();
        with_mutation_admission(
            router,
            with_capture_suppressed(self.reload_replayed_schemas()),
        )
        .await
    }

    async fn reload_replayed_schemas(&self) -> SyncResult<()> {
        let reloader = self
            .schema_replay_reloader
            .lock()
            .await
            .clone()
            .ok_or_else(|| {
                SyncError::Storage("schema replay requires an authoritative cache reloader".into())
            })?;
        reloader().await.map(|_| ()).map_err(|error| {
            SyncError::Storage(format!("schema replay cache refresh failed: {error}"))
        })
    }
}
