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

    /// Arm the upload interlock before the paused home accepts new writes.
    pub(crate) async fn set_backup_only_mode(&self) {
        self.backup_only_mode
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let mut disabled_at = self.cloud_sync_disabled_at.lock().await;
        *disabled_at = Some(crate::clock::unix_secs());
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

    /// Publish a full local backup while Cloud Sync remains Off. No peer data
    /// enters this node, and no normal cloud worker starts.
    pub async fn reconcile_paused_boot_snapshot(&self) -> SyncResult<LastStoreCloudSnapshotReport> {
        if !self
            .backup_only_mode
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(SyncError::Storage(
                "paused-home backup requires backup-only mode".into(),
            ));
        }
        let (_, report) = PAUSED_HOME_BACKUP_UPLOAD
            .scope((), self.laststore_cloud_snapshot(None))
            .await?;
        Ok(report)
    }

    /// Re-enable Cloud Sync after an intentional pause.
    ///
    /// - **Within grace:** clear disabled stamp, keep any short staging buffer,
    ///   wake the sync coordinator for incremental drain (`incremental`).
    /// - **Past grace:** attempt download/apply of cloud logs first, then force
    ///   a local snapshot publish (`snapshot_reconcile`). Residual outbox /
    ///   pending are cleared **only when that snapshot succeeds** — same rule
    ///   as overflow capture (`NEVER clear outbox if snapshot failed`). On
    ///   snapshot failure the residual staging is left in place so grace-window
    ///   mutations are not abandoned while capture default Off would fail to
    ///   re-export them. Never blocks local R/W; pull/snapshot failures are
    ///   reported on the outcome and recording still resumes.
    // lint:fn-size-ok verbatim move from wiring.rs; splitting this function is separate work
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
            // Pull first so multi-device / prior cloud state is applied before
            // we pin a new local snapshot base.
            outcome.pull_attempted = true;
            // Default to not-ok until a snapshot actually succeeds. Non-legacy
            // path never attempts snapshot and must not clear residual staging.
            outcome.snapshot_ok = false;
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

            if self.config.legacy_personal_cloud_sync {
                outcome.snapshot_attempted = true;
                match self.backup_snapshot().await {
                    Ok(seq) => {
                        outcome.snapshot_ok = true;
                        outcome.snapshot_seq = Some(seq);
                    }
                    Err(e) => {
                        outcome.snapshot_ok = false;
                        outcome.errors.push(format!("snapshot failed: {e}"));
                    }
                }
            }

            // Clear residual staging only after a successful snapshot publish.
            // Unconditionally dropping on re-enable abandoned grace-window
            // mutations when snapshot (and capture re-export) failed.
            if outcome.snapshot_ok {
                match self.maybe_clear_staging_for_sync_off_past_grace().await {
                    Ok(n) => outcome.staging_cleared = n,
                    Err(e) => outcome.errors.push(format!("staging clear failed: {e}")),
                }
                // Force-clear residual staging after re-enable decision even if
                // grace stamp still marks past-grace (we clear disabled next).
                let residual = self.outbox_count().await.unwrap_or(0);
                if residual > 0 {
                    if let Ok(n) = self.drop_oldest_outbox_entries(residual).await {
                        outcome.staging_cleared = outcome.staging_cleared.saturating_add(n);
                    }
                    self.pending.lock().await.clear();
                }
            } else if outcome.snapshot_attempted {
                // Count first. An `.await` inside `tracing::warn!` keeps
                // `Arguments` and a tracing value live across the await, and
                // that future is then not `Send`.
                let residual_outbox = self.outbox_count().await.unwrap_or(0);
                tracing::warn!(
                    target: "fold_db::sync",
                    residual_outbox,
                    "snapshot_reconcile: snapshot failed; keeping residual staging"
                );
            }
        }

        // Resume recording for new writes.
        self.set_cloud_sync_disabled(false).await;
        outcome.now_recording = self.should_stage_cloud_mutations().await;
        self.wake.notify_one();
        outcome
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
