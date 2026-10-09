use super::*;

impl SyncEngine {
    /// Refill the upload queue (or skip it when a backup block is latched) and
    /// publish the continuous mutation-log plane.
    pub(super) async fn stage_and_upload_pending(
        &self,
        backup_blocked: bool,
        state: &mut CycleState,
        phases: &mut SyncPhaseTimer,
    ) -> SyncResult<()> {
        // Store-diff cold capture retired (run_capture_tick is a no-op); keep call site for cycle shape
        // before refilling the upload queue. LastStore backup homes still run
        // legacy download/bootstrap, but do not write old log/snapshot objects.
        if backup_blocked {
            // Nothing staged here could be state.uploaded this cycle. The durable
            // outbox is untouched — only the refill of the in-memory queue is
            // skipped — so no local change is lost, and the work resumes the
            // moment the block clears.
            tracing::debug!(
                target: "fold_db::sync::memory",
                "backup bootstrap blocked: skipped capture tick + outbox scheduling"
            );
        } else if self.config.legacy_personal_cloud_sync {
            if let Err(e) = self.run_capture_marker_tick().await {
                tracing::warn!(error = %e, "store-level capture tick failed (non-fatal)");
                self.set_state(SyncState::Dirty, Some(&format!("capture tick failed: {e}")))
                    .await;
            }

            self.schedule_outbox_entries()
                .await
                .map_err(SyncError::Storage)?;
        } else {
            tracing::info!(
                target: "fold_db::sync",
                "legacy personal upload staging disabled for LastStore backup home"
            );
            // The write path durably stages one MutationIntent marker and
            // wakes this coordinator. Drain those markers before publishing
            // continuous mutation-log segments so the writer does not await
            // the pin-log append or marker deletion durability barriers.
            if let Err(e) = self.run_capture_marker_tick().await {
                tracing::warn!(error = %e, "mutation-intent capture tick failed (non-fatal)");
                self.set_state(SyncState::Dirty, Some(&format!("capture tick failed: {e}")))
                    .await;
            }
            phases.mark("capture_tick");
            // Continuous mutation-log plane: seal/upload durable segments and
            // advance published F for every configured continuous target
            // (personal + org/share). Local R/W never awaits this path;
            // failures are logged and left for the next cycle (never-block).
            if matches!(self.config.capture_mode, CaptureMode::MutationLog) {
                self.publish_mutation_log_segments(state).await;
            }
        }
        Ok(())
    }

    /// Seal and upload durable mutation-log segments for every configured
    /// continuous target, unless the coalesce window holds the upload.
    async fn publish_mutation_log_segments(&self, state: &mut CycleState) {
        let now_ms = unix_millis();
        let last_append_ms = self
            .mutation_log_last_append_ms
            .load(std::sync::atomic::Ordering::Acquire);
        let hold_since_ms = self
            .mutation_log_hold_since_ms
            .load(std::sync::atomic::Ordering::Acquire);
        let coalesce = decide_mutation_log_coalesce(
            now_ms,
            last_append_ms,
            hold_since_ms,
            self.config.mutation_log_coalesce_quiet_ms,
            self.config.mutation_log_coalesce_max_ms,
        );
        if let MutationLogCoalesceDecision::Defer {
            retry_after_ms,
            hold_since_ms,
        } = coalesce
        {
            // Do not take the plane lock. A per-write wake must not
            // seal the one record that just landed, and must not
            // block the append behind an S3 PUT.
            state.mutation_log_upload_held = true;
            self.mutation_log_hold_since_ms
                .store(hold_since_ms, std::sync::atomic::Ordering::Release);
            self.mutation_log_coalesce_retry_ms
                .store(retry_after_ms, std::sync::atomic::Ordering::Release);
            let quiet_left_ms = if self.config.mutation_log_coalesce_quiet_ms > 0 {
                self.config
                    .mutation_log_coalesce_quiet_ms
                    .saturating_sub(now_ms.saturating_sub(last_append_ms))
            } else {
                0
            };
            tracing::info!(
                target: "fold_db::sync::mutation_log",
                quiet_left_ms,
                hold_age_ms = now_ms.saturating_sub(hold_since_ms),
                retry_after_ms,
                "mutation-log upload held to group changes"
            );
        } else {
            self.mutation_log_hold_since_ms
                .store(0, std::sync::atomic::Ordering::Release);
            self.mutation_log_coalesce_retry_ms
                .store(0, std::sync::atomic::Ordering::Release);
            let target_prefixes = self.target_prefixes().await;
            let mut plane = self.mutation_log_plane.lock().await;
            let catchup_budget =
                std::time::Duration::from_millis(self.config.mutation_log_upload_catchup_budget_ms);
            for target_prefix in target_prefixes {
                self.upload_mutation_log_pass(&target_prefix, &mut plane, catchup_budget, state)
                    .await;
            }
        }
    }

    /// Run one upload pass for one target prefix and fold its outcome into
    /// the cycle state.
    async fn upload_mutation_log_pass(
        &self,
        target_prefix: &str,
        plane: &mut super::super::super::pin_log::MutationLogLocalCloud,
        catchup_budget: std::time::Duration,
        state: &mut CycleState,
    ) {
        match self
            .run_mutation_log_upload_pass_on_target(
                target_prefix,
                plane,
                // NOT max_upload_entries_per_cycle: that is a RAM guard
                // sized for fat outbox BatchPuts (default 8) and
                // throttled this plane to ~331 B/s on a 4.6 MB/s link.
                self.config.max_log_segments_per_cycle,
                super::super::super::pin_log::MutationLogPublish::Cloud,
                catchup_budget,
            )
            .await
        {
            Ok(pass) if pass.segments_uploaded > 0 => {
                self.credit_mutation_log_upload_pass(
                    target_prefix,
                    &pass,
                    &mut state.uploaded,
                    &mut state.mutation_log_backlog_ns,
                )
                .await;
                let followup_wake = self.wake_mutation_log_publisher_for_backlog(&pass.last);
                tracing::info!(
                    target: "fold_db::sync::mutation_log",
                    target_prefix = %target_prefix,
                    batches = pass.batches,
                    segments = pass.segments_uploaded,
                    records = pass.records_considered,
                    quarantined = pass.records_quarantined,
                    published_f = pass.published_frontier_after,
                    backlog = pass.upload_backlog_after,
                    put_concurrency = pass.put_concurrency,
                    followup_wake,
                    "do_sync mutation-log segment upload cycle"
                );
            }
            Ok(pass) if pass.upload_backlog_after > 0 => {
                // S0 missing / empty Ok with pending backlog: surface as
                // status context without treating as transfer failure.
                // Operators still see lag via mutation_log status; a later
                // cycle either publishes or fails hard.
                state.mutation_log_backlog_ns =
                    state.mutation_log_backlog_ns.max(pass.upload_backlog_after);
                tracing::info!(
                    target: "fold_db::sync::mutation_log",
                    target_prefix = %target_prefix,
                    backlog = pass.upload_backlog_after,
                    published_f = pass.published_frontier_after,
                    "mutation-log upload deferred or idle with backlog (local R/W unaffected)"
                );
            }
            Ok(pass) => {
                state.mutation_log_backlog_ns =
                    state.mutation_log_backlog_ns.max(pass.upload_backlog_after);
            }
            Err(failure) => {
                self.record_mutation_log_upload_failure(target_prefix, failure, state)
                    .await;
            }
        }
    }

    /// A pass that failed after some batches may still have published; credit
    /// those, then promote the error so the degraded machinery fires.
    async fn record_mutation_log_upload_failure(
        &self,
        target_prefix: &str,
        failure: MutationLogUploadPassFailure,
        state: &mut CycleState,
    ) {
        // Batches that already published must still credit
        // uploaded / entries_since_snapshot; the error is
        // the later batch, not a no-op pass.
        if failure.pass.segments_uploaded > 0 {
            self.credit_mutation_log_upload_pass(
                target_prefix,
                &failure.pass,
                &mut state.uploaded,
                &mut state.mutation_log_backlog_ns,
            )
            .await;
            let _ = self.wake_mutation_log_publisher_for_backlog(&failure.pass.last);
        } else if failure.pass.upload_backlog_after > 0 {
            state.mutation_log_backlog_ns = state
                .mutation_log_backlog_ns
                .max(failure.pass.upload_backlog_after);
        }
        // Continuous plane is the primary durability path.
        // Swallowing Err left consecutive_sync_failures /
        // last_error clear while backlog grew (cycle looked
        // healthy). Promote into state.first_transfer_error so
        // record_sync_failure / degraded status fire.
        let err = SyncError::Network(format!(
            "mutation-log segment upload failed (target_prefix='{target_prefix}'): {failure}"
        ));
        self.record_cloud_sync_transfer_failure(
            "mutation_log_upload",
            if target_prefix.is_empty() {
                "personal"
            } else {
                target_prefix
            },
            &err,
        )
        .await;
        tracing::warn!(
            target: "fold_db::sync::mutation_log",
            target_prefix = %target_prefix,
            error = %redact_sync_error_text(&err.to_string()),
            "mutation-log segment upload cycle failed (local R/W unaffected; cycle fails closed)"
        );
        state.first_transfer_error = Some(select_more_severe_transfer_error(
            state.first_transfer_error.take(),
            err,
        ));
    }

    /// Peer writer-scoped mutation-log apply for this cycle.
    pub(super) async fn apply_peer_mutation_logs(&self, state: &mut CycleState) {
        let now_ms = unix_millis();
        let last_ms = self
            .mutation_log_last_peer_apply_ms
            .load(std::sync::atomic::Ordering::Relaxed);
        // A coalesce hold has unpublished records but did not scan them,
        // so the measured backlog is still 0. Treat the hold as drain so
        // a per-write wake does not list the cloud on every change.
        // The first attempt still runs (`last_ms == 0`).
        let backlog_for_peer_skip = if state.mutation_log_upload_held {
            state
                .mutation_log_backlog_ns
                .max(self.config.mutation_log_backlog_wake_threshold_ns.max(1))
        } else {
            state.mutation_log_backlog_ns
        };
        if should_skip_peer_apply_for_drain(
            backlog_for_peer_skip,
            self.config.mutation_log_backlog_wake_threshold_ns,
            last_ms,
            now_ms,
            self.config.mutation_log_peer_apply_min_interval_ms,
        ) {
            tracing::info!(
                target: "fold_db::sync::mutation_log",
                backlog = state.mutation_log_backlog_ns,
                last_peer_apply_ago_ms = now_ms.saturating_sub(last_ms),
                min_interval_ms = self.config.mutation_log_peer_apply_min_interval_ms,
                "do_sync skipped mutation-log peer apply (drain in progress)"
            );
        } else {
            let stamp_peer_apply = match self.run_mutation_log_peer_apply_cycle().await {
                Ok(report) if report.segments_applied > 0 || report.segments_considered > 0 => {
                    state.downloaded = state
                        .downloaded
                        .saturating_add(report.records_applied as u64);
                    tracing::info!(
                        target: "fold_db::sync::mutation_log",
                        segments_considered = report.segments_considered,
                        segments_applied = report.segments_applied,
                        records_applied = report.records_applied,
                        "do_sync mutation-log peer apply cycle"
                    );
                    true
                }
                Ok(_) => true,
                Err(e) => {
                    let stamp = should_stamp_last_peer_apply_ms(Some(&e));
                    self.record_cloud_sync_transfer_failure(
                        "mutation_log_peer_apply",
                        "personal",
                        &e,
                    )
                    .await;
                    tracing::warn!(
                        target: "fold_db::sync::mutation_log",
                        error = %redact_sync_error_text(&e.to_string()),
                        "mutation-log peer apply failed (local R/W unaffected; cycle fails closed)"
                    );
                    state.first_transfer_error = Some(select_more_severe_transfer_error(
                        state.first_transfer_error.take(),
                        e,
                    ));
                    stamp
                }
            };
            if stamp_peer_apply {
                self.mutation_log_last_peer_apply_ms
                    .store(unix_millis(), std::sync::atomic::Ordering::Relaxed);
            }
        }
    }
}
