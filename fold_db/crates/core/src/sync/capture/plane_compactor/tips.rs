//! Tips and residual plane compaction.

use super::*;

impl PlaneCompactor {
    /// Reclaim `tips` when filesystem allocation is materially above the
    /// apparent record length.
    ///
    /// `tips` has millions of live keys, so neither a flat plane-size cap nor a
    /// dry-run key walk is an acceptable unattended trigger. The filesystem
    /// already exposes the exact residue signal cheaply: allocated bytes versus
    /// apparent length. Require both a proportional gap and an absolute floor,
    /// then serialize the physical rewrite against a held backup cut.
    pub(crate) async fn maybe_compact_tips_plane(&self) {
        // lint:fn-size-ok verbatim move from plane_compactor.rs; splitting this function is separate work
        let min_overhang = self.tips_compact_min_overhang_bytes.load(Ordering::Relaxed);
        if min_overhang == 0 {
            return;
        }

        // Keep this guard through the rewrite. A boolean pre-check would race a
        // new BackupPublishTarget cut, retiring chunks under an uncommitted
        // manifest and recreating the gen-502 missing==unbackable livelock.
        let now_s = crate::clock::unix_secs();

        let backup_target = self.lock_backup_publish_target().await;
        if backup_target.is_some() {
            // Skipping is correct — retiring chunks under an uncommitted
            // manifest is what created the gen-502 missing==unbackable
            // livelock. But a cut that cannot complete holds this for hours,
            // and at `debug` the operator sees a plane growing with no reason
            // given. Escalate once the skip outlives a normal publish.
            self.warn_if_tips_reclaim_starved(now_s);
            return;
        }
        // Past the guard: any starvation run has ended.
        self.tips_backup_starved_since_unix_s
            .store(0, Ordering::Relaxed);
        self.tips_starved_last_warn_unix_s
            .store(0, Ordering::Relaxed);

        let last = self.tips_last_probe_unix_s.load(Ordering::Relaxed);
        let interval = self.tips_probe_interval_s.load(Ordering::Relaxed);
        if last != 0 && now_s.saturating_sub(last) < interval {
            return;
        }
        if self
            .tips_last_probe_unix_s
            .compare_exchange(last, now_s.max(1), Ordering::SeqCst, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        let Some(usage) = self.store.collection_disk_usage(TIPS_NAMESPACE) else {
            return;
        };
        let overhang = usage.allocated_bytes.saturating_sub(usage.apparent_bytes);
        let min_bps = self.tips_compact_min_overhang_bps.load(Ordering::Relaxed);
        if !overhang_trigger_met(&usage, min_bps, min_overhang) {
            return;
        }

        if self.automatic_compact_headroom_denied(TIPS_NAMESPACE, usage.apparent_bytes) {
            return;
        }

        // `tips` is not capture-free. Its named safety contract is physical
        // compact under the production wrapper's `with_capture_suppressed`.
        // Re-check that contract here, immediately before the rewrite.
        if !crate::sync::policy::compaction_is_capture_neutral_by_suppression(TIPS_NAMESPACE) {
            tracing::warn!(
                target: "fold_db::sync::mutation_log",
                "tips lost its capture-neutral compaction contract; refusing unattended rewrite"
            );
            return;
        }

        let options = crate::storage::laststore::CollectionCompactOptions {
            collection: TIPS_NAMESPACE.to_string(),
            dry_run: false,
            seed_committed_history: false,
        };
        match self.store.compact_collection(options).await {
            Ok(report) => {
                if let Some(reason) = report.skipped_reason.as_deref() {
                    tracing::warn!(
                        target: "fold_db::sync::mutation_log",
                        reason,
                        allocated_bytes = usage.allocated_bytes,
                        apparent_bytes = usage.apparent_bytes,
                        overhang_bytes = overhang,
                        "tips compaction refused"
                    );
                    return;
                }
                let alarm_max = self.tips_compact_max_bytes.load(Ordering::Relaxed);
                self.record_automatic_overhang_compact(
                    TIPS_NAMESPACE,
                    usage,
                    &report,
                    min_bps,
                    min_overhang,
                    alarm_max,
                )
                .await;
                tracing::info!(
                    target: "fold_db::sync::mutation_log",
                    trigger = "overhang",
                    live_keys = report.live_keys,
                    allocated_bytes_before = usage.allocated_bytes,
                    apparent_bytes_before = usage.apparent_bytes,
                    overhang_bytes_before = overhang,
                    bytes_after = report.bytes_after,
                    "tips filesystem overhang compacted"
                );
            }
            Err(error) => tracing::warn!(
                target: "fold_db::sync::mutation_log",
                %error,
                allocated_bytes = usage.allocated_bytes,
                apparent_bytes = usage.apparent_bytes,
                overhang_bytes = overhang,
                "tips compaction failed (retried on a later cycle)"
            ),
        }
    }

    pub(super) async fn maybe_compact_residual_plane(&self, plane: &'static str) {
        if !crate::sync::policy::compaction_is_capture_free(plane) {
            tracing::warn!(
                target: "fold_db::sync::mutation_log",
                collection = plane,
                "plane is no longer capture-free; skipping self-compaction so \
                 the rewrite cannot enter the mutation log"
            );
            return;
        }
        // Packing lock: hold through the rewrite. Same shape as
        // `maybe_compact_tips_plane` — a boolean pre-check would race a new
        // BackupPublishTarget cut. Residual planes include backup-eligible
        // collections (`indexes`, `change_feed`); rewriting their sealed
        // files under a held photograph is the same livelock as tips.
        let backup_target = self.lock_backup_publish_target().await;
        if backup_target.is_some() {
            tracing::debug!(
                target: "fold_db::sync::mutation_log",
                collection = plane,
                "skipping capture-free plane compaction while a backup publish \
                 target is held"
            );
            return;
        }
        let Some(trigger) = self.residual_plane_triggers.get(plane) else {
            return;
        };
        let Some(usage) = self.bloated_overhang_usage(plane, trigger) else {
            return;
        };
        if self.automatic_compact_headroom_denied(plane, usage.apparent_bytes) {
            return;
        }
        let overhang = usage.allocated_bytes.saturating_sub(usage.apparent_bytes);
        let options = crate::storage::laststore::CollectionCompactOptions {
            collection: plane.to_string(),
            dry_run: false,
            seed_committed_history: false,
        };
        match self.store.compact_collection(options).await {
            Ok(report) => {
                if let Some(reason) = report.skipped_reason.as_deref() {
                    tracing::warn!(
                        target: "fold_db::sync::mutation_log",
                        collection = plane,
                        reason,
                        allocated_bytes = usage.allocated_bytes,
                        apparent_bytes = usage.apparent_bytes,
                        overhang_bytes = overhang,
                        "capture-free plane compaction refused; the plane keeps \
                         growing one append per superseded row until this is fixed"
                    );
                    return;
                }
                tracing::info!(
                    target: "fold_db::sync::mutation_log",
                    collection = plane,
                    trigger = "overhang",
                    live_keys = report.live_keys,
                    allocated_bytes_before = usage.allocated_bytes,
                    apparent_bytes_before = usage.apparent_bytes,
                    overhang_bytes_before = overhang,
                    bytes_after = report.bytes_after,
                    reclaimed_bytes = report
                        .bytes_after
                        .map(|after| report.bytes_before.saturating_sub(after)),
                    "capture-free plane compacted"
                );
            }
            Err(e) => {
                tracing::warn!(
                    target: "fold_db::sync::mutation_log",
                    collection = plane,
                    error = %e,
                    allocated_bytes = usage.allocated_bytes,
                    apparent_bytes = usage.apparent_bytes,
                    overhang_bytes = overhang,
                    "capture-free plane compaction failed (retried on a later cycle)"
                );
            }
        }
    }

    /// Shared overhang probe for residual and large-captured planes.
    ///
    /// Rate-limits the filesystem walk, then fires only when both the ratio
    /// and the absolute floor hold. Zero bps or zero floor disables without
    /// walking. Does not raise a doubling floor: the trigger is stateless.
    pub(super) fn bloated_overhang_usage(
        &self,
        namespace: &str,
        trigger: &ResidualPlaneTrigger,
    ) -> Option<crate::storage::traits::CollectionDiskUsage> {
        let min_bps = trigger.min_overhang_bps.load(Ordering::Relaxed);
        let min_bytes = trigger.min_overhang_bytes.load(Ordering::Relaxed);
        if min_bps == 0 || min_bytes == 0 {
            return None;
        }
        let now_s = crate::clock::unix_secs();
        let last = trigger.last_probe_unix_s.load(Ordering::Relaxed);
        let interval = trigger.probe_interval_s.load(Ordering::Relaxed);
        if last != 0 && now_s.saturating_sub(last) < interval {
            return None;
        }
        if trigger
            .last_probe_unix_s
            .compare_exchange(last, now_s.max(1), Ordering::SeqCst, Ordering::Relaxed)
            .is_err()
        {
            return None;
        }
        let usage = self.store.collection_disk_usage(namespace)?;
        overhang_trigger_met(&usage, min_bps, min_bytes).then_some(usage)
    }

    /// Shared body of every churn-plane size probe. See
    /// [`SyncEngine::bloated_capture_reexport_bytes`] for the contract; the
    /// only per-plane state is the four atomics in [`ChurnPlaneTrigger`].
    ///
    /// Used only by the marker-queue plane (`sync_capture_reexport`), which
    /// still uses a cap + doubling floor because its live set is the in-flight
    /// capture queue, not a proportional residue of a large user-state plane.
    pub(super) fn bloated_plane_bytes(&self, t: ChurnPlaneTrigger<'_>) -> Option<u64> {
        let cap = t.max_bytes.load(Ordering::Relaxed);
        if cap == 0 {
            return None;
        }
        let now_s = crate::clock::unix_secs();
        let last = t.last_probe_unix_s.load(Ordering::Relaxed);
        let interval = t.probe_interval_s.load(Ordering::Relaxed);
        if last != 0 && now_s.saturating_sub(last) < interval {
            return None;
        }
        if t.last_probe_unix_s
            .compare_exchange(last, now_s.max(1), Ordering::SeqCst, Ordering::Relaxed)
            .is_err()
        {
            return None;
        }
        let bytes = self.store.collection_disk_bytes(t.namespace)?;
        let floor = t.floor_bytes.load(Ordering::Relaxed).max(cap);
        (bytes > floor).then_some(bytes)
    }
}
