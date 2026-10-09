//! Engine identity, pending capacity, status reporting, and state transitions.
// lint:file-size-ok moved verbatim from wiring.rs; one method family per file

use super::*;

impl SyncEngine {
    /// Get the device identifier.
    pub fn device_id(&self) -> &str {
        &self.device_id
    }

    /// Returns the node signing keypair used to sign merge results during replay.
    /// Shared with `MutationManager` so local writes and merged writes trace to
    /// the same node identity.
    pub fn node_signer(&self) -> &Arc<Ed25519KeyPair> {
        &self.node_signer
    }

    /// Get the current sync state.
    pub async fn state(&self) -> SyncState {
        *self.state.lock().await
    }

    /// Get the number of durable pending (unsynced) log entries.
    pub async fn pending_count(&self) -> usize {
        match self.outbox_count().await {
            Ok(count) => count,
            Err(e) => {
                tracing::warn!("failed to count durable sync outbox: {e}");
                self.pending.lock().await.len()
            }
        }
    }

    /// Return whether the durable outbox is under its depth target.
    ///
    /// Local writes never depend on this — cloud lag must never block the
    /// local database. This flag is retained for status / observability ("is
    /// the outbox currently under its cap?"); the actual overflow response
    /// (force snapshot → reset staging) is driven by
    /// [`Self::outbox_overflow_reason`] from the sync cycle, off the write
    /// path.
    pub async fn has_pending_capacity(&self) -> bool {
        let max = self.config.max_outbox_entries;
        if max == 0 {
            return true;
        }
        match self.outbox_count().await {
            Ok(n) => n < max,
            Err(_) => true,
        }
    }

    /// Evaluate whether the durable outbox has crossed its depth or age
    /// overflow threshold. Read-only — never mutates state, never drops
    /// entries. Returns the machine-readable trigger reason so the sync
    /// cycle can decide whether to force a snapshot.
    pub(crate) async fn outbox_overflow_reason(&self) -> Result<Option<&'static str>, String> {
        let max = self.config.max_outbox_entries;
        if max > 0 {
            let count = self.outbox_count().await?;
            if count > max {
                return Ok(Some("outbox_overflow_depth"));
            }
        }

        let max_age = self.config.outbox_overflow_max_age_secs;
        if max_age > 0 {
            if let Some(head) = self.outbox_head_entry().await? {
                let now_ms = crate::clock::unix_millis();
                let age_secs = now_ms.saturating_sub(head.timestamp_ms) / 1000;
                if age_secs > max_age {
                    return Ok(Some("outbox_overflow_age"));
                }
            }
        }

        Ok(None)
    }

    /// Get a full status snapshot of the sync engine.
    ///
    /// `O(1)` in outbox depth: the durable count comes from the in-memory
    /// counter and the upload-queue count from the live pending queue — neither
    /// scans nor deserializes the outbox. Per-collection overhang is a TTL
    /// cache snapshot (never a recursive `read_dir` of `tips` / `atoms` /
    /// locators). Refilling the bounded upload queue is the job of the sync
    /// cycle (`schedule_outbox_entries` in `do_sync`), not of a status poll.
    // lint:fn-size-ok verbatim move from wiring.rs; splitting this function is separate work
    pub async fn status(&self) -> SyncStatus {
        let upload_queue_count = self.pending.lock().await.len();
        let durable_outbox_count = match self.outbox_count().await {
            Ok(count) => count,
            Err(e) => {
                tracing::warn!("failed to read durable sync outbox counter for status: {e}");
                upload_queue_count
            }
        };
        let snapshot_completion = self.last_snapshot_completion.lock().await.clone();
        let last_download = self.last_download_stats.lock().await.clone();
        let last_upload = self.last_upload_stats.lock().await.clone();
        let upload_policy = self.cycle_upload_caps.lock().await.clone();
        let backup_upload_concurrency = self.backup_upload_concurrency_status().await;
        let outbox_over_target = self.config.max_outbox_entries > 0
            && durable_outbox_count >= self.config.max_outbox_entries;
        let last_outbox_overflow_reason = self.last_outbox_overflow_reason.lock().await.clone();
        let oversize_outbox_drop_count = self
            .oversize_outbox_drop_count
            .load(std::sync::atomic::Ordering::Relaxed);
        let last_oversize_outbox_drop = self.last_oversize_outbox_drop.lock().await.clone();
        let pin_logs = self.pin_log.pin_log_statuses().await;
        let plane_vector_f = self.mutation_log_frontier.vector_frontier();
        let mutation_log = self.mutation_log_plane_status_from_pin_logs(&pin_logs, &plane_vector_f);
        // One implementation, shared with the local cadence: the fills and
        // their bars live on the compactor, and this passes the engine's cache
        // so a cloud-synced node does not keep a second memo of the same
        // directories.
        let automatic_compactions = self
            .compaction
            .status_snapshot_with_cache(&self.status_disk_usage_cache, STATUS_DISK_USAGE_TTL)
            .await;
        let capture = super::super::types::CapturePlaneStatus {
            pin_log_live_keys: pin_logs.iter().map(|pin| pin.entry_count).sum(),
            pin_log_disk_bytes: self
                .status_disk_usage_cache
                .get(
                    Arc::clone(&self.store),
                    super::super::pin_log::PIN_LOG_NAMESPACE,
                    STATUS_DISK_USAGE_TTL,
                )
                .map(|usage| usage.apparent_bytes),
            pin_log_last_compact_trigger: self.pin_log.last_compact_trigger().await,
            capture_envelope_version: matches!(
                self.config.capture_mode,
                super::super::types::CaptureMode::MutationLog
            )
            .then(|| "mutation_intent".to_string()),
            capture_physical_fallback_records: self
                .capture_physical_fallback_records
                .load(std::sync::atomic::Ordering::Relaxed),
            capture_physical_catalog_records: self
                .capture_physical_catalog_records
                .load(std::sync::atomic::Ordering::Relaxed),
            queue_jobs: self
                .capture_queue_jobs
                .load(std::sync::atomic::Ordering::Relaxed),
            queue_rejections: self
                .capture_queue_rejections
                .load(std::sync::atomic::Ordering::Relaxed),
            queue_admission_us: self
                .capture_queue_admission_us
                .load(std::sync::atomic::Ordering::Relaxed),
            queue_delay_us: self
                .capture_queue_delay_us
                .load(std::sync::atomic::Ordering::Relaxed),
            stage_us: self
                .capture_stage_us
                .load(std::sync::atomic::Ordering::Relaxed),
            record_us: self
                .capture_record_us
                .load(std::sync::atomic::Ordering::Relaxed),
            cleanup_us: self
                .capture_cleanup_us
                .load(std::sync::atomic::Ordering::Relaxed),
            queue_worker_panics: self
                .capture_queue_worker_panics
                .load(std::sync::atomic::Ordering::Relaxed),
            reexport_disk_bytes: self
                .status_disk_usage_cache
                .get(
                    Arc::clone(&self.store),
                    crate::sync::capture::CAPTURE_REEXPORT_NAMESPACE,
                    STATUS_DISK_USAGE_TTL,
                )
                .map(|usage| usage.apparent_bytes),
            reexport_poison_dropped: self
                .capture_reexport_poison_dropped_count
                .load(std::sync::atomic::Ordering::Relaxed),
            automatic_compactions,
        };

        // Liveness. Every pre-existing degradation trigger is a function of
        // outbox *depth*, so a cloud path that fails before anything is staged
        // leaves the outbox empty and reports healthy — which is how backup
        // failed on every cycle for 36 h under `degraded=false`. A failure
        // streak is the signal that survives an empty outbox.
        let consecutive_sync_failures = self
            .consecutive_sync_failures
            .load(std::sync::atomic::Ordering::Relaxed);
        let failure_threshold = self.config.sync_failure_degraded_threshold;
        let sync_failing = failure_threshold > 0 && consecutive_sync_failures >= failure_threshold;

        // The sealed-chunk backup uploader keeps its own failure streak
        // (`BackupProgressTracker`), but until now nothing folded it into the
        // one boolean callers actually key off. That is the precise shape of
        // the 36 h stall: the tracker knew backup was failing every cycle while
        // `sync_degraded` still answered `false`.
        // Under MutationLog continuous plane, sealed-chunk backup is demoted
        // (bootstrap/rare compact only) — lag on the log plane is the primary
        // continuous health signal instead.
        let backup = self.backup_progress_snapshot();
        let sealed_home_demoted = self.pin_log.continuous_sealed_home_backup_demoted();
        let backup_failing = failure_threshold > 0
            && backup.enabled
            && !sealed_home_demoted
            && u64::from(backup.consecutive_failures) >= failure_threshold;

        // Latched *before* the streak-based reasons: a deterministic block is a
        // strictly stronger statement than "the last N cycles failed", and it
        // is the one an operator can act on. It must not wait for the failure
        // threshold, because the whole point of latching is that the doomed
        // cycles stop running.
        let backup_blocker = self.backup_blocker.lock().await.clone();

        let mut degraded_reasons = Vec::new();
        if backup_blocker.is_some() {
            degraded_reasons.push("backup_bootstrap_blocked".to_string());
        }
        if sync_failing {
            degraded_reasons.push("sync_failing".to_string());
        }
        if backup_failing {
            degraded_reasons.push("backup_failing".to_string());
        }
        if outbox_over_target {
            degraded_reasons.push("outbox_over_target".to_string());
        }
        if oversize_outbox_drop_count > 0 {
            degraded_reasons.push("oversize_outbox_drops".to_string());
        }
        if mutation_log
            .as_ref()
            .is_some_and(|m| m.active && m.lag_degraded)
        {
            degraded_reasons.push("mutation_log_lag".to_string());
        }
        let capture_reexport_pending_known_nonempty =
            self.capture_reexport_pending_known_nonempty();
        let capture_reexport_pending_count = self
            .capture_reexport_pending_count
            .load(std::sync::atomic::Ordering::Relaxed)
            .max(u64::from(
                capture_reexport_pending_known_nonempty == Some(true),
            ));
        let capture_reexport_failure_count = self
            .capture_reexport_failure_count
            .load(std::sync::atomic::Ordering::Relaxed);
        let last_capture_reexport_error = self.last_capture_reexport_error.lock().await.clone();
        // Product rule (2026-08-15, card lastdb-capture-reexport-pending-keeps-
        // sync-degraded): residual dirty-key reexport after cutover is an
        // *observe-only drain phase* while the continuous plane is healthy.
        // Operators still see `capture_reexport_pending_count` on status.
        // Latch health RED only when pending markers remain *and* reexport is
        // stuck with an unrecovered error (not "pending alone for hours under
        // interactive_busy while log_lag=0 / transport OK").
        if capture_reexport_pending_count > 0 && last_capture_reexport_error.is_some() {
            degraded_reasons.push("capture_reexport_pending".to_string());
        }
        // A dropped poison marker is permanent capture loss, even after the
        // marker plane empties. This count is process-local, so a restart
        // cannot prove that no poison was dropped in an earlier process.
        if capture.reexport_poison_dropped > 0 {
            degraded_reasons.push("capture_reexport_poison_dropped".to_string());
        }

        let now_secs = crate::clock::unix_secs();
        let cloud_sync_disabled_at = *self.cloud_sync_disabled_at.lock().await;
        let recording_local_changes = self
            .config
            .recording_local_changes(cloud_sync_disabled_at, now_secs);
        let sync_off_grace_expired = self
            .config
            .sync_off_grace_expired(cloud_sync_disabled_at, now_secs);
        let reenable_strategy = self
            .config
            .reenable_strategy(cloud_sync_disabled_at, now_secs)
            .map(str::to_string);

        SyncStatus {
            state: *self.state.lock().await,
            local_writable: true,
            sync_degraded: !degraded_reasons.is_empty(),
            pending_count: durable_outbox_count,
            durable_outbox_count,
            upload_queue_count,
            // Surface the *active* adaptive queue cap, not the static config
            // default (which is only a fixed-mode / floor hint).
            upload_queue_max: upload_policy.max_pending,
            durable_outbox_max: self.config.max_outbox_entries,
            last_sync_at: *self.last_sync_at.lock().await,
            last_error: self.last_error.lock().await.clone(),
            last_error_at: *self.last_error_at.lock().await,
            consecutive_sync_failures,
            failing_since: *self.failing_since.lock().await,
            degraded_reasons,
            replay_blocker: self.replay_blocker.lock().await.clone(),
            backup_blocker,
            undecryptable_unsynced_count: snapshot_completion.undecryptable_unsynced_count,
            last_snapshot_complete: snapshot_completion.last_snapshot_complete,
            last_snapshot_undecryptable_namespaces: snapshot_completion.undecryptable_namespaces,
            last_download,
            last_upload,
            upload_policy: Some(upload_policy),
            backup_upload_concurrency,
            last_outbox_overflow_reason,
            oversize_outbox_drop_count,
            last_oversize_outbox_drop,
            recording_local_changes,
            sync_off_grace_expired,
            reenable_strategy,
            cloud_sync_disabled_at,
            sync_off_grace_secs: self.config.sync_off_grace_secs,
            pin_logs,
            mutation_log,
            capture_reexport_pending_count,
            capture_reexport_pending_known_nonempty,
            capture_reexport_failure_count,
            last_capture_reexport_error,
            capture,
        }
    }

    /// Build continuous mutation-log plane status from pin-log runtimes.
    ///
    /// Prefers the personal target when present; otherwise the highest-lag
    /// active continuous target. `None` when CaptureMode is not MutationLog.
    ///
    /// Vector F merges (max through per writer):
    /// 1. pin-log runtime `published_f_by_writer` for the chosen target
    /// 2. process-local mutation-log plane HWM map
    /// 3. single-writer scaffold: one-entry map `{ local_writer_id → scalar F }`
    ///    when the maps are empty but scalar F or durable progress is known
    pub(in crate::sync::engine) fn mutation_log_plane_status_from_pin_logs(
        &self,
        pin_logs: &[super::super::pin_log::PinLogTargetStatus],
        plane_vector_f: &std::collections::BTreeMap<String, u64>,
    ) -> Option<super::super::types::MutationLogPlaneStatus> {
        if !matches!(
            self.config.capture_mode,
            super::super::types::CaptureMode::MutationLog
        ) {
            return None;
        }
        let lag_threshold_secs = self.config.mutation_log_lag_degraded_threshold_secs;
        let personal = pin_logs.iter().find(|p| p.target_id == "personal");
        let chosen = personal.or_else(|| pin_logs.iter().max_by_key(|p| p.upload_backlog));
        // `published_frontier` is the cloud-confirmed watermark and MUST stay
        // distinct from the `frontier_f` scalar computed below: that one is the
        // max across writers after merging locally-sealed plane vector F, which
        // is not a cloud confirmation. Binding both to one variable made
        // `published_through` a copy of `frontier_f`, so the two watermarks were
        // equal in every emitted sample and publish lag could not be seen from
        // the fields that name it.
        let (
            published_frontier,
            last_durable,
            log_lag,
            segments,
            recovery_point_age_secs,
            mut by_writer,
            records_quarantined,
            last_quarantine_reason,
        ) = match chosen {
            Some(p) => (
                p.published_frontier,
                p.last_durable_frontier,
                p.upload_backlog,
                p.segments_uploaded,
                p.recovery_point_age_secs,
                p.published_f_by_writer.clone(),
                p.records_quarantined,
                p.last_quarantine_reason.clone(),
            ),
            None => (0, 0, 0, 0, None, std::collections::BTreeMap::new(), 0, None),
        };
        // Merge process plane vector F (other writers sealed onto this node).
        for (wid, through) in plane_vector_f {
            let e = by_writer.entry(wid.clone()).or_insert(0);
            *e = (*e).max(*through);
        }
        // Single-writer scaffold: still a one-entry map, never scalar-only JSON.
        if by_writer.is_empty() && (published_frontier > 0 || last_durable > 0) {
            by_writer.insert(self.device_id.clone(), published_frontier);
        } else if !self.device_id.is_empty() {
            // Ensure local writer appears when scalar advanced but map lagging.
            if published_frontier > 0 {
                let e = by_writer.entry(self.device_id.clone()).or_insert(0);
                *e = (*e).max(published_frontier);
            }
        }
        let frontier_f = by_writer
            .values()
            .copied()
            .max()
            .unwrap_or(0)
            .max(published_frontier);
        let lag_degraded =
            mutation_log_lag_degraded(recovery_point_age_secs, log_lag, lag_threshold_secs);
        Some(super::super::types::MutationLogPlaneStatus {
            active: true,
            writer_id: self.device_id.clone(),
            frontier_f,
            published_through: published_frontier,
            frontier_f_by_writer: by_writer,
            last_durable_frontier: last_durable,
            log_lag,
            recovery_point_age_secs,
            segments_uploaded: segments,
            // Engine-wide, not per-target: the peer apply cycle downloads one
            // writer-scoped plane, so attributing it to `chosen` would be a
            // guess. `chosen` can also be None on a node that has applied peer
            // work but never sealed any of its own.
            peer_segments_applied: self
                .mutation_log_peer_segments_applied
                .load(std::sync::atomic::Ordering::Relaxed),
            peer_records_applied: self
                .mutation_log_peer_records_applied
                .load(std::sync::atomic::Ordering::Relaxed),
            lag_degraded,
            // `chosen` is None exactly when no pin-log runtime is registered.
            // Consumers need independent local-write evidence before treating
            // that normal fresh-node state as degraded.
            capture_registered: chosen.is_some(),
            records_quarantined,
            last_quarantine_reason,
        })
    }

    /// Recompute adaptive upload caps from live RSS / EWMA / env and store them
    /// for this cycle. Called at the start of `do_sync` and before outbox
    /// schedule so schedule + select + PUT concurrency share one snapshot.
    pub(crate) async fn refresh_upload_policy(
        &self,
    ) -> super::super::upload_policy::UploadPolicySnapshot {
        let rss = super::super::upload_policy::sample_rss_bytes();
        let ewma = self.upload_policy.ewma_bps();
        // Process CPU once per policy refresh (cycle rate), not per chunk.
        // ForegroundPressure.cpu_percent from the node status sampler, when
        // present, still wins inside policy_inputs_from_env_and_config.
        let cpu_percent = super::super::upload_policy::sample_process_cpu_percent();
        // Env override remains as the forced test/manual throttle. Production
        // interactive pressure also comes from the embedding node via
        // `set_foreground_pressure_sample` (p95 / QoS sheds).
        let env_interactive_busy = env_flag::var_truthy("LASTDB_SYNC_INTERACTIVE_BUSY");
        let foreground_pressure = self
            .foreground_pressure
            .lock()
            .ok()
            .and_then(|guard| guard.clone());
        let live = super::super::upload_policy::UploadPolicyLiveSamples {
            rss_bytes: rss,
            ewma_upload_bps: ewma,
            cpu_percent,
            interactive_busy: env_interactive_busy,
            foreground_pressure,
        };
        let inputs = super::super::upload_policy::policy_inputs_from_env_and_config(
            self.config.max_pending,
            self.config.max_upload_entries_per_cycle,
            self.config.max_upload_bytes_per_cycle,
            self.config.sync_concurrency,
            &live,
        );
        let snap = super::super::upload_policy::compute_upload_policy(&inputs);
        tracing::info!(
            target: "fold_db::sync::memory",
            mode = ?snap.mode,
            budget_bytes = snap.budget_bytes,
            max_entries = snap.max_upload_entries,
            concurrency = snap.concurrency,
            headroom_rss = ?snap.headroom_rss_bytes,
            cpu_percent = ?snap.cpu_percent,
            foreground_pressure = ?snap.foreground_pressure,
            ewma_bps = snap.ewma_upload_bps,
            throttle = ?snap.throttle_reason,
            "upload policy refreshed for cycle"
        );
        self.upload_policy.store_snapshot(snap.clone());
        *self.cycle_upload_caps.lock().await = snap.clone();
        snap
    }

    pub(crate) async fn active_upload_caps(
        &self,
    ) -> super::super::upload_policy::UploadPolicySnapshot {
        self.cycle_upload_caps.lock().await.clone()
    }

    pub(crate) async fn set_state(&self, new_state: SyncState, _message: Option<&str>) {
        // `_message` retained for call-site stability; was only consumed by the
        // removed never-armed StatusCallback path.
        let mut state = self.state.lock().await;
        *state = new_state;
    }
}
