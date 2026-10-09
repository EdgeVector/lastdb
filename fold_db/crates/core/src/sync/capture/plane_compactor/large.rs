//! Large captured plane compaction and its overhang bookkeeping.

use super::*;

impl PlaneCompactor {
    /// Compact large captured planes after a cheap overhang-ratio probe.
    ///
    /// The backup target mutex is the photograph packing lock. Hold it across
    /// the full rewrite so no cut can begin between a pre-check and compaction.
    /// Atoms rewrites under capture-suppress with Cloud Sync left on (D5).
    /// Planes that still require isolation (order-log) stamp a live
    /// cloud-disabled pause around their rewrite and restore the prior value
    /// before the lock is released.
    pub(crate) async fn maybe_compact_large_captured_planes(&self) {
        let backup_target = self.lock_backup_publish_target().await;
        if backup_target.is_some() {
            tracing::debug!(
                target: "fold_db::sync::mutation_log",
                "skipping large captured-plane compaction while a backup publish target is held"
            );
            return;
        }

        let now_s = crate::clock::unix_secs();
        let due: Vec<(&'static str, crate::storage::traits::CollectionDiskUsage)> =
            LARGE_CAPTURED_SELF_COMPACT_PLANES
                .iter()
                .filter_map(|plane| {
                    let trigger = self.large_captured_plane_triggers.get(plane)?;
                    self.bloated_overhang_usage(plane, trigger)
                        .map(|usage| (*plane, usage))
                })
                .collect();
        if due.is_empty() {
            // Silence here read as "the sweep did not run" during the
            // local-cadence bring-up. Name the measurement instead: a plane
            // below its bar and a plane the probe could not measure are
            // different facts and only one of them is a defect.
            for plane in LARGE_CAPTURED_SELF_COMPACT_PLANES {
                let Some(trigger) = self.large_captured_plane_triggers.get(plane) else {
                    continue;
                };
                let Some(usage) = self.store.collection_disk_usage(plane) else {
                    tracing::debug!(
                        target: "fold_db::sync::mutation_log",
                        collection = plane,
                        "large captured plane disk usage is not measurable"
                    );
                    continue;
                };
                {
                    tracing::debug!(
                        target: "fold_db::sync::mutation_log",
                        collection = plane,
                        allocated_bytes = usage.allocated_bytes,
                        apparent_bytes = usage.apparent_bytes,
                        reclaimable_estimate_bytes = usage.reclaimable_estimate_bytes(),
                        min_overhang_bps = trigger.min_overhang_bps.load(Ordering::Relaxed),
                        min_overhang_bytes = trigger.min_overhang_bytes.load(Ordering::Relaxed),
                        "large captured plane is below its overhang bar"
                    );
                }
            }
            return;
        }

        let (pause_due, quiet_due): (Vec<_>, Vec<_>) = due
            .into_iter()
            .partition(|(plane, _)| automatic_large_plane_pauses_cloud(plane));

        for (plane, usage) in quiet_due {
            self.compact_large_captured_plane(plane, usage).await;
        }

        if !pause_due.is_empty() {
            let mut pause =
                CloudOffPauseGuard::arm(Arc::clone(&self.cloud_sync_disabled_at), now_s).await;

            for (plane, usage) in pause_due {
                self.compact_large_captured_plane(plane, usage).await;
            }

            pause.restore().await;
        }
        drop(backup_target);
    }

    pub(super) async fn compact_large_captured_plane(
        &self,
        plane: &'static str,
        usage: crate::storage::traits::CollectionDiskUsage,
    ) {
        let Some(trigger) = self.large_captured_plane_triggers.get(plane) else {
            return;
        };
        if !crate::sync::policy::compaction_is_capture_neutral_by_suppression(plane) {
            tracing::warn!(
                target: "fold_db::sync::mutation_log",
                collection = plane,
                "large captured plane lost its capture-neutral contract; refusing automatic rewrite"
            );
            return;
        }
        if self.automatic_compact_headroom_denied(plane, usage.apparent_bytes) {
            return;
        }

        let overhang = usage.allocated_bytes.saturating_sub(usage.apparent_bytes);
        let options = crate::storage::laststore::CollectionCompactOptions {
            collection: plane.to_string(),
            dry_run: false,
            seed_committed_history: plane == "atoms",
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
                        "large captured-plane compaction refused"
                    );
                    return;
                }
                let min_bps = trigger.min_overhang_bps.load(Ordering::Relaxed);
                let min_bytes = trigger.min_overhang_bytes.load(Ordering::Relaxed);
                let alarm_max = trigger.alarm_max_bytes.load(Ordering::Relaxed);
                self.record_automatic_overhang_compact(
                    plane, usage, &report, min_bps, min_bytes, alarm_max,
                )
                .await;
                tracing::info!(
                    target: "fold_db::sync::mutation_log",
                    collection = plane,
                    trigger = "overhang",
                    live_keys = report.live_keys,
                    allocated_bytes_before = usage.allocated_bytes,
                    apparent_bytes_before = usage.apparent_bytes,
                    overhang_bytes_before = overhang,
                    bytes_after = report.bytes_after,
                    "large captured plane compacted automatically"
                );
            }
            Err(error) => tracing::warn!(
                target: "fold_db::sync::mutation_log",
                collection = plane,
                %error,
                allocated_bytes = usage.allocated_bytes,
                apparent_bytes = usage.apparent_bytes,
                overhang_bytes = overhang,
                "large captured-plane compaction failed (retried on a later cycle)"
            ),
        }
    }

    /// Stamp `automatic_compactions` after a successful overhang-triggered rewrite.
    ///
    /// Tips uses a dedicated trigger (backup-starvation warn + no-pause) and
    /// atoms/order-log share `large_captured_plane_triggers`. Both must surface
    /// the same last-fire fields so `lastdb status` reads one shape.
    pub(super) async fn record_automatic_overhang_compact(
        &self,
        plane: &str,
        usage: crate::storage::traits::CollectionDiskUsage,
        report: &crate::storage::laststore::CollectionCompactReport,
        min_bps: u64,
        min_bytes: u64,
        alarm_max: u64,
    ) {
        let completed_at_unix_s = crate::clock::unix_secs();
        let overhang = usage.allocated_bytes.saturating_sub(usage.apparent_bytes);
        self.automatic_compaction_status.lock().await.insert(
            plane.to_string(),
            crate::sync::engine::AutomaticCompactionStatus {
                last_compacted_at_unix_s: Some(completed_at_unix_s),
                last_trigger: Some("overhang".to_string()),
                configured_max_bytes: alarm_max,
                last_bytes_before: Some(report.bytes_before),
                last_bytes_after: report.bytes_after,
                allocated_bytes: Some(usage.allocated_bytes),
                apparent_bytes: Some(usage.apparent_bytes),
                overhang_bytes: Some(overhang),
                overhang_bps: Some(overhang_bps(&usage)),
                trigger_overhang_bps: min_bps,
                trigger_overhang_bytes: min_bytes,
                above_trigger: true,
                cap_alarm: alarm_max > 0 && usage.allocated_bytes > alarm_max,
                live_bytes: usage.live_bytes,
                dead_bytes: usage.dead_bytes,
                dead_bps: usage.dead_bytes.map(|_| usage.dead_bps()),
                residue_unknown_bytes: Some(usage.residue_unknown_bytes),
                reclaimable_estimate_bytes: Some(usage.reclaimable_estimate_bytes()),
                reclaimable_bps: Some(usage.reclaimable_bps()),
            },
        );
    }

    /// Escalate a `tips` compaction skip that has outlived a normal publish.
    ///
    /// The first skip only records when it started. Once the run exceeds the
    /// probe interval — i.e. `tips` has now missed a reclaim it was due — this
    /// warns at most once per interval, and measures the plane so the line
    /// carries the bytes actually at stake rather than just a complaint.
    ///
    /// Cheap by construction: the duration check is two atomics, and the
    /// filesystem probe runs only on the throttled warn path.
    pub(crate) fn warn_if_tips_reclaim_starved(&self, now_s: u64) {
        // `Err` carries the value already stored, i.e. when this run began.
        // `Ok` means this is the first skip of a new run — nothing is overdue.
        let Err(since) = self.tips_backup_starved_since_unix_s.compare_exchange(
            0,
            now_s.max(1),
            Ordering::SeqCst,
            Ordering::Relaxed,
        ) else {
            return;
        };

        let interval = self.tips_probe_interval_s.load(Ordering::Relaxed);
        let starved_for = now_s.saturating_sub(since);
        if starved_for < interval {
            return;
        }

        let last_warn = self.tips_starved_last_warn_unix_s.load(Ordering::Relaxed);
        if last_warn != 0 && now_s.saturating_sub(last_warn) < interval {
            return;
        }
        if self
            .tips_starved_last_warn_unix_s
            .compare_exchange(last_warn, now_s.max(1), Ordering::SeqCst, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        let (allocated, apparent) = self
            .store
            .collection_disk_usage(TIPS_NAMESPACE)
            .map_or((0, 0), |usage| {
                (usage.allocated_bytes, usage.apparent_bytes)
            });
        tracing::warn!(
            target: "fold_db::sync::mutation_log",
            starved_for_s = starved_for,
            allocated_bytes = allocated,
            apparent_bytes = apparent,
            overhang_bytes = allocated.saturating_sub(apparent),
            "tips reclaim starved by a held backup publish target; the largest \
             plane is not returning bytes while the cut cannot complete"
        );
    }
}
