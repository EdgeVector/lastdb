//! Photograph-aligned compaction of dirty planes.

use super::*;

impl PlaneCompactor {
    /// Compact-if-dirty pass aligned to the photograph cadence (D3).
    ///
    /// Uses the D2 overhang-ratio+floor predicate, ignores hourly probe
    /// timers, and stops when the 5-minute budget elapses. A held photograph
    /// packing lock skips the pass so an in-flight cut is not delayed.
    /// Continuous 120s snapshot+log publish never calls this method.
    pub(crate) async fn maybe_photograph_aligned_compact_if_dirty(&self) {
        let interval = self.photograph_compact_interval_s.load(Ordering::Relaxed);
        let now_s = crate::clock::unix_secs();
        let last = self.photograph_compact_last_unix_s.load(Ordering::Relaxed);
        if last != 0 && now_s.saturating_sub(last) < interval {
            return;
        }

        // Take the packing lock *before* CAS. A held cut used to burn the
        // cadence stamp and then return, so the next D3 waited a full interval
        // with no compact. Skip without stamping; retry as soon as the cut
        // releases the lock.
        let backup_target = self.lock_backup_publish_target().await;
        if backup_target.is_some() {
            tracing::debug!(
                target: "fold_db::sync::mutation_log",
                "skipping photograph-aligned compact-if-dirty; a backup cut is held"
            );
            return;
        }
        if self
            .photograph_compact_last_unix_s
            .compare_exchange(last, now_s.max(1), Ordering::SeqCst, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        let mut dirty = self.photograph_dirty_planes();
        dirty.sort_by(|left, right| {
            right
                .overhang()
                .cmp(&left.overhang())
                .then_with(|| left.name.cmp(right.name))
        });
        if dirty.is_empty() {
            tracing::debug!(
                target: "fold_db::sync::mutation_log",
                "photograph-aligned compact-if-dirty: no plane above the D2 overhang bar"
            );
            return;
        }

        let budget_secs = self.photograph_compact_budget_secs.load(Ordering::Relaxed);
        if budget_secs == 0 {
            tracing::info!(
                target: "fold_db::sync::mutation_log",
                dirty = dirty.len(),
                "photograph-aligned compact-if-dirty: budget is zero; photograph proceeds"
            );
            return;
        }

        let started = Instant::now();
        let budget = Duration::from_secs(budget_secs);
        let mut compacted = 0usize;
        let mut skipped = 0usize;
        let mut pause_guard: Option<CloudOffPauseGuard> = None;
        let dirty_len = dirty.len();

        for (index, plane) in dirty.into_iter().enumerate() {
            if photograph_compact_budget_exhausted(started, budget, Instant::now()) {
                skipped = dirty_len.saturating_sub(index);
                tracing::info!(
                    target: "fold_db::sync::mutation_log",
                    collection = plane.name,
                    budget_secs,
                    compacted,
                    skipped,
                    "photograph-aligned compact-if-dirty: budget elapsed; remaining planes wait for the next cycle"
                );
                break;
            }
            let needs_pause = matches!(plane.kind, PhotographPlaneKind::LargeCaptured)
                && automatic_large_plane_pauses_cloud(plane.name);
            if needs_pause && pause_guard.is_none() {
                pause_guard = Some(
                    CloudOffPauseGuard::arm(Arc::clone(&self.cloud_sync_disabled_at), now_s).await,
                );
            }
            self.compact_photograph_dirty_plane(&plane).await;
            compacted += 1;
        }

        if let Some(mut guard) = pause_guard.take() {
            guard.restore().await;
        }
        tracing::info!(
            target: "fold_db::sync::mutation_log",
            compacted,
            skipped,
            budget_secs,
            "photograph-aligned compact-if-dirty pass complete"
        );
    }

    pub(super) fn photograph_dirty_planes(&self) -> Vec<PhotographDirtyPlane> {
        let mut dirty = Vec::new();
        let tips_bps = self.tips_compact_min_overhang_bps.load(Ordering::Relaxed);
        let tips_bytes = self.tips_compact_min_overhang_bytes.load(Ordering::Relaxed);
        if let Some(usage) = self.dirty_overhang_usage(TIPS_NAMESPACE, tips_bps, tips_bytes) {
            dirty.push(PhotographDirtyPlane {
                name: TIPS_NAMESPACE,
                usage,
                kind: PhotographPlaneKind::Tips,
            });
        }
        for plane in LARGE_CAPTURED_SELF_COMPACT_PLANES {
            let Some(trigger) = self.large_captured_plane_triggers.get(plane) else {
                continue;
            };
            let min_bps = trigger.min_overhang_bps.load(Ordering::Relaxed);
            let min_bytes = trigger.min_overhang_bytes.load(Ordering::Relaxed);
            if let Some(usage) = self.dirty_overhang_usage(plane, min_bps, min_bytes) {
                dirty.push(PhotographDirtyPlane {
                    name: plane,
                    usage,
                    kind: PhotographPlaneKind::LargeCaptured,
                });
            }
        }
        for plane in RESIDUAL_SELF_COMPACT_PLANES {
            let Some(trigger) = self.residual_plane_triggers.get(plane) else {
                continue;
            };
            let min_bps = trigger.min_overhang_bps.load(Ordering::Relaxed);
            let min_bytes = trigger.min_overhang_bytes.load(Ordering::Relaxed);
            if let Some(usage) = self.dirty_overhang_usage(plane, min_bps, min_bytes) {
                dirty.push(PhotographDirtyPlane {
                    name: plane,
                    usage,
                    kind: PhotographPlaneKind::Residual,
                });
            }
        }
        let locator_bps = self
            .locator_compact_min_overhang_bps
            .load(Ordering::Relaxed);
        if locator_bps != 0 {
            if let Some(usage) = self.store.collection_disk_usage(LOCATOR_NAMESPACE) {
                if overhang_ratio_met(&usage, locator_bps) {
                    dirty.push(PhotographDirtyPlane {
                        name: LOCATOR_NAMESPACE,
                        usage,
                        kind: PhotographPlaneKind::Locator,
                    });
                }
            }
        }
        dirty
    }

    pub(super) fn dirty_overhang_usage(
        &self,
        namespace: &str,
        min_bps: u64,
        min_bytes: u64,
    ) -> Option<crate::storage::traits::CollectionDiskUsage> {
        if min_bps == 0 || min_bytes == 0 {
            return None;
        }
        let usage = self.store.collection_disk_usage(namespace)?;
        overhang_trigger_met(&usage, min_bps, min_bytes).then_some(usage)
    }

    pub(super) async fn compact_photograph_dirty_plane(&self, plane: &PhotographDirtyPlane) {
        match plane.kind {
            PhotographPlaneKind::LargeCaptured => {
                self.compact_large_captured_plane(plane.name, plane.usage)
                    .await;
                if let Some(trigger) = self.large_captured_plane_triggers.get(plane.name) {
                    let now_s = crate::clock::unix_secs().max(1);
                    trigger.last_probe_unix_s.store(now_s, Ordering::Relaxed);
                }
            }
            PhotographPlaneKind::Tips => {
                self.compact_named_capture_neutral_plane(TIPS_NAMESPACE, plane.usage)
                    .await;
                let now_s = crate::clock::unix_secs().max(1);
                self.tips_last_probe_unix_s.store(now_s, Ordering::Relaxed);
            }
            PhotographPlaneKind::Residual => {
                self.compact_named_capture_free_plane(plane.name, plane.usage)
                    .await;
                if let Some(trigger) = self.residual_plane_triggers.get(plane.name) {
                    let now_s = crate::clock::unix_secs().max(1);
                    trigger.last_probe_unix_s.store(now_s, Ordering::Relaxed);
                }
            }
            PhotographPlaneKind::Locator => {
                self.compact_named_capture_free_plane(LOCATOR_NAMESPACE, plane.usage)
                    .await;
                let now_s = crate::clock::unix_secs().max(1);
                self.locator_last_probe_unix_s
                    .store(now_s, Ordering::Relaxed);
            }
        }
    }

    pub(super) async fn compact_named_capture_neutral_plane(
        &self,
        plane: &'static str,
        usage: crate::storage::traits::CollectionDiskUsage,
    ) {
        if !crate::sync::policy::compaction_is_capture_neutral_by_suppression(plane) {
            tracing::warn!(
                target: "fold_db::sync::mutation_log",
                collection = plane,
                "photograph-aligned compact-if-dirty refused; capture-neutral contract missing"
            );
            return;
        }
        self.execute_photograph_plane_compact(plane, usage).await;
    }

    pub(super) async fn compact_named_capture_free_plane(
        &self,
        plane: &'static str,
        usage: crate::storage::traits::CollectionDiskUsage,
    ) {
        if !crate::sync::policy::compaction_is_capture_free(plane) {
            tracing::warn!(
                target: "fold_db::sync::mutation_log",
                collection = plane,
                "photograph-aligned compact-if-dirty refused; plane is no longer capture-free"
            );
            return;
        }
        self.execute_photograph_plane_compact(plane, usage).await;
    }

    pub(super) async fn execute_photograph_plane_compact(
        &self,
        plane: &'static str,
        usage: crate::storage::traits::CollectionDiskUsage,
    ) {
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
                        "photograph-aligned compact-if-dirty refused"
                    );
                    return;
                }
                tracing::info!(
                    target: "fold_db::sync::mutation_log",
                    collection = plane,
                    trigger = "photograph-aligned",
                    live_keys = report.live_keys,
                    allocated_bytes_before = usage.allocated_bytes,
                    apparent_bytes_before = usage.apparent_bytes,
                    overhang_bytes_before = overhang,
                    bytes_after = report.bytes_after,
                    "photograph-aligned compact-if-dirty compacted a dirty plane"
                );
            }
            Err(error) => tracing::warn!(
                target: "fold_db::sync::mutation_log",
                collection = plane,
                %error,
                allocated_bytes = usage.allocated_bytes,
                apparent_bytes = usage.apparent_bytes,
                overhang_bytes = overhang,
                "photograph-aligned compact-if-dirty failed (retried on a later cycle)"
            ),
        }
    }
}
