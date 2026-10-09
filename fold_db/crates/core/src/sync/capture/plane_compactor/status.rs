//! Compactor status snapshots.

use super::*;

impl PlaneCompactor {
    /// Snapshot the automatic-compaction reports for `lastdb status`.
    ///
    /// Every plane with a trigger appears, whether or not it has ever fired:
    /// a plane that is *below* its bar and a plane nothing is watching read
    /// identically when only fired planes are listed, and the second is the
    /// defect. `fill_overhang_status` adds the current measurement and the
    /// configured bars beside any last-fire numbers already stamped.
    ///
    /// `cache` keeps this scan-free. Every read is a cached gauge or "not
    /// measured yet" plus one background walk — status must never be more
    /// expensive than the thing it reports on.
    pub(crate) async fn status_snapshot_with_cache(
        &self,
        cache: &Arc<crate::sync::engine::StatusDiskUsageCache>,
        ttl: std::time::Duration,
    ) -> BTreeMap<String, AutomaticCompactionStatus> {
        let mut planes = self.automatic_compaction_status.lock().await.clone();
        let usage = |plane: &str| cache.get(Arc::clone(&self.store), plane, ttl);
        for (plane, trigger) in &self.large_captured_plane_triggers {
            super::super::fill_overhang_status(
                &mut planes,
                plane,
                usage(plane),
                trigger.min_overhang_bps.load(Ordering::Relaxed),
                trigger.min_overhang_bytes.load(Ordering::Relaxed),
                trigger.alarm_max_bytes.load(Ordering::Relaxed),
            );
        }
        for (plane, trigger) in &self.residual_plane_triggers {
            super::super::fill_overhang_status(
                &mut planes,
                plane,
                usage(plane),
                trigger.min_overhang_bps.load(Ordering::Relaxed),
                trigger.min_overhang_bytes.load(Ordering::Relaxed),
                trigger.alarm_max_bytes.load(Ordering::Relaxed),
            );
        }
        super::super::fill_overhang_status(
            &mut planes,
            TIPS_NAMESPACE,
            usage(TIPS_NAMESPACE),
            self.tips_compact_min_overhang_bps.load(Ordering::Relaxed),
            self.tips_compact_min_overhang_bytes.load(Ordering::Relaxed),
            self.tips_compact_max_bytes.load(Ordering::Relaxed),
        );
        super::super::fill_overhang_status(
            &mut planes,
            LOCATOR_NAMESPACE,
            usage(LOCATOR_NAMESPACE),
            self.locator_compact_min_overhang_bps
                .load(Ordering::Relaxed),
            0,
            0,
        );
        planes
    }

    /// [`Self::status_snapshot_with_cache`] over this compactor's own cache.
    /// Used by the local cadence, which has no engine to borrow one from.
    pub(crate) async fn status_snapshot(&self) -> BTreeMap<String, AutomaticCompactionStatus> {
        self.status_snapshot_with_cache(
            &self.status_disk_usage_cache,
            crate::sync::engine::STATUS_DISK_USAGE_TTL,
        )
        .await
    }
}
