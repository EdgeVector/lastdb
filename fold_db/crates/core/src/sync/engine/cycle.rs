//! Sync cycle orchestration: `sync` / `do_sync`, outcome recording, backlog alerts.

use super::super::error::{redact_sync_error_text, SyncError, SyncResult};
use super::super::log::LogEntry;
use super::*;
use crate::clock::unix_millis;

mod do_sync;
mod support;
pub(crate) use support::*;

impl SyncEngine {
    // =========================================================================
    // Recording operations
    // =========================================================================

    /// Select a memory-bounded prefix of the upload queue for this cycle.
    ///
    /// Returns a clone of the chosen entries (oldest first). Caps are
    /// independent of `max_pending` (queue depth) so a large durable outbox
    /// cannot seal multi-GB of BatchPuts in one tick.
    ///
    /// **Never force-admits an oversize head.** A single multi-MB BatchPut
    /// sealed + re-serialized for metrics was enough to thrash swap on re-enable
    /// (2026-07-14). Oversize heads must be dropped by
    /// [`Self::schedule_outbox_entries`] / `forget_recorded_op` so smaller
    /// backlog can catch up — not sealed "to make progress".
    pub(crate) fn select_upload_batch(
        pending: &[LogEntry],
        max_entries: usize,
        max_bytes: usize,
    ) -> Vec<LogEntry> {
        if pending.is_empty() {
            return Vec::new();
        }
        let entry_limit = if max_entries == 0 {
            pending.len()
        } else {
            max_entries.min(pending.len())
        };
        if max_bytes == 0 {
            return pending[..entry_limit].to_vec();
        }
        let mut selected = Vec::new();
        let mut used = 0usize;
        for entry in pending.iter().take(entry_limit) {
            let size = entry.serialized_len();
            // Refuse oversize entries entirely (including the head). Previously
            // we force-admitted the first entry even when `size > max_bytes`,
            // which re-enabled the thrash path for poison BatchPuts.
            if size > max_bytes {
                break;
            }
            if !selected.is_empty() && used.saturating_add(size) > max_bytes {
                break;
            }
            used = used.saturating_add(size);
            selected.push(entry.clone());
        }
        selected
    }

    /// Retire the liveness degradation signal — the failure streak, its start
    /// timestamp, and the `last_error` text/timestamp.
    ///
    /// The question this signal answers is "is the cloud path reachable and
    /// working", and a cycle that ran end to end without error answers it yes
    /// regardless of how many bytes it carried. It deliberately does NOT touch
    /// `last_sync_at` (which claims a *data* round-trip) or `replay_blocker`
    /// (which is only disproved by a replay that actually ran).
    async fn retire_liveness_failure_signal(&self) {
        *self.last_error.lock().await = None;
        *self.last_error_at.lock().await = None;
        self.consecutive_sync_failures
            .store(0, std::sync::atomic::Ordering::Relaxed);
        *self.failing_since.lock().await = None;
    }

    /// Bookkeeping for a cycle that completed without error.
    ///
    /// `synced` means "this cycle moved bytes". It is the right gate for
    /// `last_sync_at`, which claims a data round-trip, and the wrong gate for
    /// the liveness signal: a caught-up node reaches here with `synced ==
    /// false` on every cycle forever, so gating the failure-streak reset on it
    /// leaves the streak un-clearable on exactly the nodes that are healthiest.
    ///
    /// Before this split, the sole reset was [`Self::record_sync_success`], so
    /// on an idle node a single transient error latched
    /// `consecutive_sync_failures`, `failing_since` and `last_error`
    /// **forever**, and once the streak reached
    /// `sync_failure_degraded_threshold` the `sync_degraded` verdict latched
    /// with it. Measured on the primary 2026-08-04: one auth-Lambda 500 during
    /// an ExememStorageService deploy, then ~96 consecutive clean cycles over
    /// 48 min, still reporting `consecutive_failures=1 failing_for=48m`.
    ///
    /// A liveness signal that cannot go back down is as useless as one that
    /// never goes up: it makes the *next* real outage invisible.
    pub(crate) async fn record_cycle_outcome(&self, synced: bool) {
        if synced {
            self.record_sync_success().await;
        } else {
            self.retire_liveness_failure_signal().await;
        }
    }

    /// Record a successful sync: update last_sync_at timestamp and clear last_error.
    ///
    /// Also retires the liveness degradation signal — the failure streak, its
    /// start timestamp, and the `last_error` timestamp — so one good cycle is
    /// enough to clear a degraded verdict.
    pub(crate) async fn record_sync_success(&self) {
        let now = crate::clock::unix_secs();
        *self.last_sync_at.lock().await = Some(now);
        self.retire_liveness_failure_signal().await;
        *self.replay_blocker.lock().await = None;
        *self.backlog_alerts.lock().await = CloudSyncBacklogAlertState::default();
    }

    /// Record a sync failure: store the error message and transition state.
    ///
    /// The structured `replay_blocker` is **sticky**: once a poison failure
    /// (corrupt cloud object, or a failed pre-upload decrypt proof) arms it, a
    /// later *non-poison* failure (e.g. a transient network error on the next
    /// cycle) must NOT clear it. Only a successful sync
    /// (`record_sync_success`) — or an explicit quarantine — retires the
    /// blocker. Otherwise the blocker would flap to `None` on the first
    /// network blip after the poison, hiding the real problem and letting the
    /// wakeup/backoff logic treat the node as merely offline.
    pub(crate) async fn record_sync_failure(&self, err: &SyncError) {
        let msg = redact_sync_error_text(&err.to_string());
        let now = crate::clock::unix_secs();
        *self.last_error.lock().await = Some(msg.clone());
        *self.last_error_at.lock().await = Some(now);
        // Streak first, start-timestamp only on the transition into failing, so
        // `failing_since` answers "how long has this been broken" rather than
        // "when did it last fail".
        self.consecutive_sync_failures
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.failing_since.lock().await.get_or_insert(now);
        if let Some(blocker) = replay_blocker_from_error(err) {
            *self.replay_blocker.lock().await = Some(blocker);
        }
        let new_state = match err {
            SyncError::Network(_) => SyncState::Offline,
            _ => SyncState::Dirty,
        };
        self.set_state(new_state, Some(&msg)).await;
    }

    /// Remember a durable local mutation-log append so the next cycle can
    /// hold the upload. A zero pair of coalesce windows leaves the stamp at
    /// 0, which keeps the immediate upload path.
    pub(crate) fn note_mutation_log_local_append(&self) {
        if self.config.mutation_log_coalesce_quiet_ms == 0
            && self.config.mutation_log_coalesce_max_ms == 0
        {
            return;
        }
        self.mutation_log_last_append_ms
            .store(unix_millis(), std::sync::atomic::Ordering::Release);
    }

    /// Queue another bounded mutation-log upload pass when the pass that just
    /// completed made progress but left backlog at or above the wake floor.
    ///
    /// The background coordinator consumes this coalesced `Notify` permit on
    /// its next loop iteration, so catch-up does not wait for the normal sync
    /// interval. Requiring positive progress prevents a stalled scan or idle
    /// writer from turning persistent backlog into a busy-loop. Each pass still
    /// honors the normal segment and byte caps.
    ///
    /// This reads `mutation_log_backlog_wake_threshold_ns`, not the seconds
    /// degraded threshold. `upload_backlog_after` is a frontier delta in
    /// nanoseconds; the two knobs were one field until 2026-08-23, which is how
    /// a seconds-scale number came to be compared against a nanosecond value.
    pub(super) fn wake_mutation_log_publisher_for_backlog(
        &self,
        report: &MutationLogUploadReport,
    ) -> bool {
        let wake_threshold_ns = self.config.mutation_log_backlog_wake_threshold_ns;
        let should_wake = report.segments_uploaded > 0
            && wake_threshold_ns > 0
            && report.upload_backlog_after >= wake_threshold_ns;
        if should_wake {
            self.wake.notify_one();
        }
        should_wake
    }

    /// Loop bounded mutation-log upload cycles on one target until the
    /// backlog is gone or `budget` elapses.
    ///
    /// Each inner cycle still seals bodies, PUTs them, then advances
    /// `published_f` only after every PUT of that batch. `budget == 0`
    /// means one cycle.
    pub(crate) async fn run_mutation_log_upload_pass_on_target(
        &self,
        target_prefix: &str,
        plane: &mut super::pin_log::MutationLogLocalCloud,
        max_segments: usize,
        publish: super::pin_log::MutationLogPublish,
        budget: std::time::Duration,
    ) -> Result<MutationLogUploadPassReport, MutationLogUploadPassFailure> {
        let started = std::time::Instant::now();
        let mut pass = MutationLogUploadPassReport::default();
        loop {
            let report = match self
                .pin_log
                .run_mutation_log_segment_upload_cycle(
                    self,
                    target_prefix,
                    plane,
                    max_segments,
                    publish,
                )
                .await
            {
                Ok(report) => report,
                Err(error) => {
                    // Keep batches that already published. `?` used to drop
                    // `pass`, so do_sync never credited uploaded /
                    // entries_since_snapshot for those PUTs.
                    return Err(MutationLogUploadPassFailure { pass, error });
                }
            };
            let segments_uploaded = report.segments_uploaded;
            let upload_backlog_after = report.upload_backlog_after;
            let published_after = report.published_frontier_after;
            let published_before = report.published_frontier_before;
            if segments_uploaded > 0 {
                pass.batches = pass.batches.saturating_add(1);
                pass.segments_uploaded = pass.segments_uploaded.saturating_add(segments_uploaded);
                pass.records_considered = pass
                    .records_considered
                    .saturating_add(report.records_considered);
                pass.bytes_uploaded = pass.bytes_uploaded.saturating_add(report.bytes_uploaded);
                pass.records_quarantined = pass
                    .records_quarantined
                    .saturating_add(report.records_quarantined);
                pass.put_concurrency = pass.put_concurrency.max(report.put_concurrency);
            }
            pass.published_frontier_after = published_after;
            pass.upload_backlog_after = upload_backlog_after;
            pass.last = report;
            if published_after <= published_before
                || !should_continue_mutation_log_upload_batches(
                    segments_uploaded,
                    upload_backlog_after,
                    self.config.mutation_log_backlog_wake_threshold_ns,
                    started.elapsed(),
                    budget,
                )
            {
                break;
            }
        }
        Ok(pass)
    }

    /// Credit confirmed mutation-log publish totals from one target pass.
    ///
    /// Call this for Ok passes and for a later-batch Err that still carries
    /// earlier published batches. Photograph compaction measures personal log
    /// growth; the MutationLog publisher bypasses the legacy outbox, so those
    /// confirmed records land here.
    async fn credit_mutation_log_upload_pass(
        &self,
        target_prefix: &str,
        pass: &MutationLogUploadPassReport,
        uploaded: &mut usize,
        mutation_log_backlog_ns: &mut u64,
    ) {
        *uploaded = uploaded.saturating_add(pass.segments_uploaded);
        *mutation_log_backlog_ns = (*mutation_log_backlog_ns).max(pass.upload_backlog_after);
        if target_prefix.is_empty() {
            *self.entries_since_snapshot.lock().await += pass.records_considered as u64;
            *self.bytes_since_snapshot.lock().await += pass.bytes_uploaded;
        }
    }

    /// Clear the exact cloud log entry currently blocking replay.
    ///
    /// Callers must provide the target and seq from `SyncStatus.replay_blocker`
    /// (no wildcards). Two codes are supported, with different safety:
    ///
    /// - `cloud_replay_corrupt_entry` — **delete** the cloud object, write a
    ///   local tombstone, leave the cursor for the next cycle to skip the 404.
    ///   The object was unreadable; destroying it is correct for every device.
    /// - `cloud_replay_apply_failed` — **skip-with-tombstone only**: write a
    ///   local skip tombstone, advance this device's download cursor past the
    ///   seq, do **not** delete the cloud object. Peers / later builds can still
    ///   replay it. Deleting here would be irreversible shared-data loss for a
    ///   mere local apply refusal.
    pub async fn quarantine_current_replay_blocker(
        &self,
        target: &str,
        seq: u64,
    ) -> SyncResult<ReplayBlockerQuarantineOutcome> {
        if *self.state.lock().await == SyncState::Syncing {
            return Err(SyncError::Storage(
                "cannot quarantine replay blocker while sync is running".to_string(),
            ));
        }

        let blocker = self
            .replay_blocker
            .lock()
            .await
            .clone()
            .ok_or_else(|| SyncError::Storage("no replay blocker is active".to_string()))?;
        let mode = match blocker.code.as_str() {
            "cloud_replay_corrupt_entry" => "delete",
            "cloud_replay_apply_failed" => "skip_local",
            other => {
                return Err(SyncError::Storage(format!(
                    "unsupported replay blocker code '{other}'"
                )));
            }
        };
        if blocker.target != target || blocker.seq != seq {
            return Err(SyncError::Storage(format!(
                "replay blocker mismatch: active target='{}' seq={}, requested target='{target}' seq={seq}",
                blocker.target, blocker.seq
            )));
        }

        let sync_target = self
            .targets
            .lock()
            .await
            .iter()
            .find(|candidate| candidate.label == target)
            .cloned()
            .ok_or_else(|| {
                SyncError::Storage(format!(
                    "active replay blocker target '{target}' is not configured"
                ))
            })?;

        let deleted_log_objects = if mode == "delete" {
            let urls = self
                .auth
                .presign_log_delete_target(&sync_target, &[seq])
                .await?;
            if urls.len() != 1 {
                return Err(SyncError::Auth(format!(
                    "presign_log_delete '{}': expected 1 url, got {}",
                    sync_target.label,
                    urls.len()
                )));
            }
            self.s3.delete(&urls[0]).await?;
            if let Err(e) = self
                .auth
                .confirm_log_delete_object_keys(&sync_target, &[seq], &[])
                .await
            {
                tracing::warn!(
                    target = %sync_target.label,
                    seq,
                    error = %e,
                    "quarantine: log delete metering confirm failed (non-fatal)"
                );
            }
            1
        } else {
            0
        };

        // Local tombstone: corrupt path needs it so a listed-but-404 object
        // advances; apply-failed path needs it so a still-present object is
        // skipped without re-applying on the next cycle (defense in depth —
        // we also advance the cursor below for skip_local).
        self.save_replay_quarantine_tombstone(&sync_target.prefix, seq)
            .await?;

        if mode == "skip_local" {
            // Advance past the pinned seq so the next cycle can upload again
            // without re-hitting the un-applicable entry.
            self.checkpoint_download_cursor_after_replay(&sync_target, seq)
                .await;
        }

        let msg = if mode == "delete" {
            format!(
                "Quarantined (deleted) cloud replay blocker target='{}' seq={seq}; retry sync to advance replay",
                sync_target.label
            )
        } else {
            format!(
                "Skipped local cloud replay blocker target='{}' seq={seq} without deleting the cloud object; next cycle may upload",
                sync_target.label
            )
        };
        *self.replay_blocker.lock().await = None;
        *self.last_error.lock().await = Some(msg.clone());
        *self.last_error_at.lock().await = Some(crate::clock::unix_secs());
        self.set_state(SyncState::Dirty, Some(&msg)).await;

        Ok(ReplayBlockerQuarantineOutcome {
            target: sync_target.label,
            seq,
            deleted_log_objects,
            mode: mode.to_string(),
        })
    }

    pub(crate) async fn record_cloud_sync_transfer_failure(
        &self,
        operation: &str,
        sync_target: &str,
        err: &SyncError,
    ) {
        let failure_class = cloud_sync_failure_class(operation, err);
        let redacted_error = redact_sync_error_text(&err.to_string());
        let key = CloudSyncFailureKey {
            sync_target: sync_target.to_string(),
            failure_class,
        };

        let stats = {
            let mut alerts = self.backlog_alerts.lock().await;
            let stats = alerts
                .failures
                .entry(key.clone())
                .and_modify(|stats| {
                    stats.count += 1;
                    stats.last_error = redacted_error.clone();
                })
                .or_insert_with(|| CloudSyncFailureStats {
                    count: 1,
                    last_error: redacted_error,
                });
            stats.clone()
        };

        if stats.count < CLOUD_SYNC_BACKLOG_MIN_FAILURES {
            return;
        }

        let Some(snapshot) = self.cloud_sync_backlog_snapshot().await else {
            return;
        };

        let emission_key = CloudSyncAlertEmissionKey {
            sync_target: key.sync_target.clone(),
            failure_class: key.failure_class.clone(),
            threshold_percent: snapshot.threshold_percent,
        };
        let should_emit = {
            let mut alerts = self.backlog_alerts.lock().await;
            alerts.emitted.insert(emission_key)
        };
        if should_emit {
            emit_cloud_sync_backlog_incident(&key, &stats, snapshot, self.config.sync_concurrency);
        }
    }

    pub(crate) async fn cloud_sync_backlog_snapshot(&self) -> Option<CloudSyncBacklogSnapshot> {
        let max_pending = self.config.max_pending;
        if max_pending == 0 {
            return None;
        }

        // Read the depth from the counter and the oldest-entry age from the head
        // (smallest seq == first key == oldest write) rather than materializing
        // the whole outbox, so backlog alerting stays O(1) in outbox depth.
        let pending_count = match self.outbox_count().await {
            Ok(count) => count,
            Err(_) => self.pending.lock().await.len(),
        };

        let threshold_percent = crossed_cloud_sync_backlog_threshold(pending_count, max_pending)?;
        let oldest_timestamp_ms = match self.outbox_head_entry().await {
            Ok(Some(entry)) => Some(entry.timestamp_ms),
            _ => self
                .pending
                .lock()
                .await
                .iter()
                .map(|entry| entry.timestamp_ms)
                .min(),
        };
        let now_secs = crate::clock::unix_secs();
        let oldest_pending_age_secs =
            oldest_timestamp_ms.map(|timestamp_ms| now_secs.saturating_sub(timestamp_ms / 1000));
        let last_success_age_secs = self
            .last_sync_at
            .lock()
            .await
            .map(|last_sync_at| now_secs.saturating_sub(last_sync_at));

        Some(CloudSyncBacklogSnapshot {
            pending_count,
            max_pending,
            threshold_percent,
            oldest_pending_age_secs,
            last_success_age_secs,
        })
    }

    /// Run one sync cycle: upload pending log entries, compact if needed.
    ///
    /// Returns Ok(true) if all pending entries were uploaded,
    /// Ok(false) if there was nothing to sync.
    ///
    /// A cycle always runs when the engine is configured, even if the local
    /// state is `Idle`. Multi-device users have peers uploading to the shared
    /// `{user_hash}/log/` prefix, and a passive reader needs to poll
    /// downloads to pick those up. Previously the engine returned early when
    /// `Idle && !has_orgs` — that blocked personal multi-device sync
    /// completely. The only early exits now are "already syncing" (to avoid
    /// overlapping cycles) and an empty targets list (no sync configured).
    pub async fn sync(&self) -> SyncResult<bool> {
        // Hard interlock: intentional Cloud Sync Off skips the entire cycle
        // (no lock acquire, no presign/PUT/download thrash). Local R/W is
        // unaffected — this only parks the cloud plane.
        if !self.cloud_plane_allows_upload().await {
            tracing::debug!(
                target: "fold_db::sync",
                "sync cycle skipped: cloud plane Off (upload interlock)"
            );
            return Ok(false);
        }

        // Atomic check-and-set: hold the lock for both the state check and transition
        {
            let mut state = self.state.lock().await;
            if *state == SyncState::Syncing {
                tracing::info!("sync skipped: already syncing");
                return Ok(false);
            }
            *state = SyncState::Syncing;
        }

        // Acquire the write lock before uploading. This coordinates with other
        // devices so only one uploads at a time (prevents wasted presign URLs
        // and duplicate sequence assignment). Lock failures are non-fatal —
        // data integrity is guaranteed by the append-only log with nanosecond
        // keys, not by the lock.
        let lock_held = match self.acquire_lock().await {
            Ok(()) => true,
            Err(SyncError::Auth(_)) => false, // auth error — will be caught by do_sync
            Err(e) => {
                tracing::warn!("failed to acquire sync lock (proceeding anyway): {e}");
                false
            }
        };

        // Try do_sync; on auth error, attempt token refresh and retry once.
        let result = match self.do_sync().await {
            Ok(synced) => Ok(synced),
            Err(ref e) if matches!(e, SyncError::Auth(_)) => self.try_refresh_and_retry().await,
            Err(e) => Err(e),
        };

        // Always release the lock, even on error
        if lock_held {
            if let Err(e) = self.release_lock().await {
                tracing::warn!("failed to release sync lock: {e}");
            }
        }

        match result {
            Ok(synced) => {
                self.record_cycle_outcome(synced).await;
                self.set_state(SyncState::Idle, None).await;
                Ok(synced)
            }
            Err(e) => {
                self.record_sync_failure(&e).await;
                Err(e)
            }
        }
    }

    /// Attempt to refresh auth credentials and retry do_sync once.
    /// Falls through to the original auth error if refresh isn't available or fails.
    pub(crate) async fn try_refresh_and_retry(&self) -> SyncResult<bool> {
        self.refresh_auth_once("sync").await?;
        self.do_sync().await
    }

    /// Invoke the auth-refresh callback (if any) and update the shared
    /// `AuthClient` with the new credential. Errors if no callback is wired
    /// or the callback itself fails.
    ///
    /// `context` is a short label used only in log messages so on-demand
    /// paths (snapshot backup, restore) can be distinguished from the
    /// periodic sync cycle in logs.
    pub(crate) async fn refresh_auth_once(&self, context: &str) -> SyncResult<()> {
        let refresh_cb = match self.auth_refresh {
            Some(ref cb) => cb.clone(),
            None => return Err(SyncError::Auth("authentication failed".to_string())),
        };

        tracing::info!("{context} auth failed, attempting token refresh");
        let new_auth = refresh_cb().await.map_err(|e| {
            tracing::warn!("{context} token refresh failed: {e}");
            SyncError::Auth("authentication failed after token refresh failure".to_string())
        })?;

        self.auth.update_auth(new_auth).await;
        tracing::info!("{context} token refreshed");
        Ok(())
    }
}
