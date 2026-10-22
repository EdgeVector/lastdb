//! Cloud sync enable/disable and backup-only mode transitions.

use super::*;

impl SyncEngine {
    /// Mark Cloud Sync intentionally off, stamping `disabled_at` if not already
    /// stamped. Idempotent: a second call while still off keeps the original
    /// timestamp so the grace window is not reset by status polls.
    ///
    /// Does not block local R/W. When intentional off is already past
    /// [`SyncConfig::sync_off_grace_secs`] (including grace `0`), clears any
    /// durable cloud staging so the node does not hold a dead mutation backlog.
    pub async fn set_cloud_sync_disabled(&self, disabled: bool) {
        {
            let mut guard = self.cloud_sync_disabled_at.lock().await;
            if disabled {
                if guard.is_none() {
                    let now = crate::clock::unix_secs();
                    *guard = Some(now);
                }
            } else {
                *guard = None;
            }
        }
        if disabled {
            // Hard interlock: pause means no cloud PUTs, not "config renamed,
            // uploader may keep dripping." Park backup progress + abandon the
            // sticky cut (and its freeze dir) so status is honest and disk is
            // not held for a publish that will not run while Off.
            if let Ok(mut tracker) = self.backup_progress.lock() {
                tracker.set_enabled(false);
            }
            // Off permits an explicit owner repair outside this process. Even
            // without a held target, its cloud tip can change while our old
            // post-CAS GC still has a keep-set. Wait for an in-flight DELETE
            // turn, then revoke that proof on every Off transition. GC refuses
            // until this process CASes and commits a new tip after On.
            let publish_turn = self.backup_publish_turn.lock().await;
            *self.backup_published_tip_identity.lock().await = None;
            self.backup_gc_identity_revoked
                .store(true, std::sync::atomic::Ordering::SeqCst);
            self.retire_backup_publish_target().await;
            drop(publish_turn);
            if let Err(e) = self.maybe_clear_staging_for_sync_off_past_grace().await {
                tracing::warn!(
                    target: "fold_db::sync",
                    error = %e,
                    "failed to clear cloud staging after intentional sync-off"
                );
            }
            tracing::info!(
                target: "fold_db::sync",
                "cloud plane Off: upload interlock armed (backup parked, sticky cut retired)"
            );
        } else if self.laststore_backup_source.is_some() {
            if let Ok(mut tracker) = self.backup_progress.lock() {
                tracker.set_enabled(true);
            }
        }
    }

    /// Unix timestamp (seconds) when Cloud Sync was intentionally disabled, if
    /// currently off.
    pub async fn cloud_sync_disabled_at(&self) -> Option<u64> {
        *self.cloud_sync_disabled_at.lock().await
    }

    /// Whether the cloud plane may issue any network upload (presign/PUT/CAS).
    ///
    /// **Hard interlock:** intentional Off (`cloud_sync_disabled_at` set)
    /// forbids *all* cloud writes immediately — not after grace. Grace only
    /// controls local staging accumulation ([`Self::should_stage_cloud_mutations`]).
    pub async fn cloud_plane_allows_upload(&self) -> bool {
        if self
            .backup_only_mode
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return PAUSED_HOME_BACKUP_UPLOAD.try_with(|_| ()).is_ok();
        }
        self.cloud_sync_disabled_at().await.is_none()
    }

    pub fn is_backup_only_mode(&self) -> bool {
        self.backup_only_mode
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Whether local mutations should still be recorded into the cloud staging
    /// plane (on, or intentionally off within grace).
    pub async fn should_stage_cloud_mutations(&self) -> bool {
        let now = crate::clock::unix_secs();
        let disabled_at = *self.cloud_sync_disabled_at.lock().await;
        self.config.recording_local_changes(disabled_at, now)
    }

    /// Re-enable Cloud Sync after an intentional pause.
    ///
    /// - **Within grace:** clear disabled stamp, keep any short staging buffer,
    ///   wake the sync coordinator for incremental drain (`incremental`).
    /// - **Past grace:** download and apply cloud logs. This path does not
    ///   publish a snapshot. The paused-home cut stays on
    ///   `prepare_primary_authoritative_cloud_resume` plus publish. Residual
    ///   staging stays. Never blocks local R/W. Pull failures are reported
    ///   on the outcome and recording still resumes.
    pub async fn reenable_cloud_sync(&self) -> CloudSyncReenableOutcome {
        // An explicit operator re-enable is a request to re-evaluate, not to
        // inherit a previous run's verdict. Drop any latched bootstrap block so
        // this call re-runs the proof for real; if the prefix is still
        // unprovable it re-latches on the first cycle.
        *self.backup_blocker.lock().await = None;

        let now = crate::clock::unix_secs();
        let disabled_at = *self.cloud_sync_disabled_at.lock().await;
        let Some(strategy) = self.config.reenable_strategy(disabled_at, now) else {
            return CloudSyncReenableOutcome {
                strategy: "already_on".to_string(),
                pull_attempted: false,
                pull_ok: true,
                snapshot_attempted: false,
                snapshot_ok: true,
                snapshot_seq: None,
                staging_cleared: 0,
                now_recording: true,
                errors: Vec::new(),
            };
        };

        let mut outcome = CloudSyncReenableOutcome {
            strategy: strategy.to_string(),
            pull_attempted: false,
            pull_ok: true,
            snapshot_attempted: false,
            snapshot_ok: true,
            snapshot_seq: None,
            staging_cleared: 0,
            now_recording: false,
            errors: Vec::new(),
        };

        if strategy == "snapshot_reconcile" {
            self.reenable_snapshot_reconcile(&mut outcome).await;
        }

        // Resume recording for new writes.
        self.set_cloud_sync_disabled(false).await;
        outcome.now_recording = self.should_stage_cloud_mutations().await;
        self.wake.notify_one();
        outcome
    }

    /// `snapshot_reconcile` branch of [`Self::reenable_cloud_sync`].
    ///
    /// The name is historical. Re-enable pulls peer logs and does not cut a
    /// snapshot. `snapshot_attempted` stays false so a missing cut is not a
    /// failed snapshot, and residual staging is left in place.
    async fn reenable_snapshot_reconcile(&self, outcome: &mut CloudSyncReenableOutcome) {
        outcome.pull_attempted = true;
        outcome.snapshot_attempted = false;
        outcome.snapshot_ok = true;
        outcome.snapshot_seq = None;
        let personal = self.targets.lock().await.first().cloned();
        if let Some(target) = personal {
            match self.download_with_auth_retry(&target).await {
                Ok(_) => outcome.pull_ok = true,
                Err(e) => {
                    outcome.pull_ok = false;
                    outcome.errors.push(format!("pull failed: {e}"));
                }
            }
        } else {
            outcome.pull_ok = false;
            outcome.errors.push("no personal sync target".to_string());
        }
    }

    /// If intentional Cloud Sync off has exceeded the grace window, drop all
    /// durable outbox + in-memory upload queue entries. Returns how many
    /// durable entries were removed. Idempotent and never fails local writers.
    pub async fn maybe_clear_staging_for_sync_off_past_grace(&self) -> Result<usize, String> {
        let now = crate::clock::unix_secs();
        let disabled_at = *self.cloud_sync_disabled_at.lock().await;
        if !self.config.sync_off_grace_expired(disabled_at, now) {
            return Ok(0);
        }
        let count = self.outbox_count().await.unwrap_or(0);
        let pending_len = self.pending.lock().await.len();
        if count == 0 && pending_len == 0 {
            return Ok(0);
        }
        let removed = if count > 0 {
            self.drop_oldest_outbox_entries(count).await?
        } else {
            0
        };
        {
            let mut pending = self.pending.lock().await;
            pending.clear();
        }
        if removed > 0 || pending_len > 0 {
            tracing::info!(
                target: "fold_db::sync",
                removed,
                pending_cleared = pending_len,
                "cleared cloud staging: intentional sync-off past grace"
            );
        }
        Ok(removed)
    }
}
