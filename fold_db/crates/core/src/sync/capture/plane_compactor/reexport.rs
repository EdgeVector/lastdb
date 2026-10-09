//! Capture re-export plane compaction and its floor.

use super::*;

impl PlaneCompactor {
    /// Return the bytes the marker lifecycle only *logically* frees.
    ///
    /// Every captured local write puts one intent marker here, flushes it,
    /// performs the write, then deletes the marker and flushes again. `LastStore`
    /// is append-structured, so **both** halves of that lifecycle are appends:
    /// the delete writes a delete line and leaves the marker record in the
    /// segment. A plane whose live set returns to zero after every write
    /// therefore grows by roughly two records per captured write, forever, and
    /// no amount of correct draining returns a byte.
    ///
    /// Measured on Tom's primary 2026-08-17T10:20Z, build
    /// `0.23.3-686-g32b0707e6`: `sync_capture_reexport` held **3.14 GiB**, the
    /// third-largest plane in a 21.27 GiB store, behind a live set of zero. It
    /// was on neither `COMPACT_ALLOWLIST` nor `SYNC_INTERNAL_NAMESPACES`, so
    /// `lastdb db compact --collection sync_capture_reexport` answered
    /// `not on the compact allowlist` and there was no other reclaim path at all.
    ///
    /// This is the same defect as `sync_pin_log`
    /// (`pin_log::PinLog::maybe_compact_pin_log_plane`) with one difference that
    /// makes it simpler: the pin log's live set is the unconfirmed publish
    /// backlog, which is large during a cloud outage, while this plane's live set
    /// is the number of writes *in flight* — a handful, and independent of cloud
    /// health. So the cap can sit far lower, and a plane over it is dead bytes
    /// with near-certainty.
    ///
    /// # Why the trigger is size and not a counter
    ///
    /// The pin log first shipped a row-count trigger and it never armed on the
    /// primary: the count was a process-local `AtomicU64` and the daemon restarts
    /// (safe upgrades, memory guard) more often than the counter refilled, which
    /// left 20.4 GiB behind one live record with compaction permitted and never
    /// attempted. On-disk size is state rather than session history, so a plane
    /// inherited bloated from a previous process is noticed on the first sync
    /// cycle after start. That correction is not re-learned here.
    ///
    /// Safety and cost:
    /// - Best-effort, on the sync cycle. A failure is logged and retried next
    ///   cycle; it never fails a write, a publish, or the drain below.
    /// - `compact_collection` rewrites only keys still live in the shard index —
    ///   precisely the markers whose write has not finished — and locks one shard
    ///   of one collection at a time, so at worst a concurrent marker write for
    ///   that shard waits. `main`, `atoms`, and `tips` are untouched.
    /// - The plane is capture-skipped (`SYNC_INTERNAL_NAMESPACES`), so the
    ///   rewrite emits no mutation-log records. Compacting a *captured* plane is
    ///   what turned the 2026-08-08 `tips` compaction into 11.58 GiB of new pin
    ///   log; this cannot repeat that.
    ///
    /// # Where this is called from
    ///
    /// Called from [`SyncEngine::do_sync`] on every cycle, not only from
    /// [`SyncEngine::run_capture_tick`]. That tick is gated behind
    /// `config.legacy_personal_cloud_sync`, which is **false** on a LastStore
    /// backup home — the shape every current node runs. Measured on the primary
    /// 2026-08-17 after this reclaim shipped and installed: 180 log lines of
    /// "legacy personal upload staging disabled for LastStore backup home",
    /// zero capture ticks, and the plane still growing ~1.3 GiB/day behind 127
    /// live keys. A reclaim reachable only from a retired code path is not a
    /// retention policy.
    ///
    /// Both call sites are kept. The probe claims its slot with a compare-exchange
    /// on a rate-limited timestamp before it measures anything, so calling it
    /// twice in one cycle costs one `load` and cannot compact twice.
    pub(crate) async fn maybe_compact_capture_reexport_plane(&self) {
        // Packing lock: hold through the rewrite. A boolean pre-check would
        // race a new BackupPublishTarget cut.
        let backup_target = self.lock_backup_publish_target().await;
        if backup_target.is_some() {
            return;
        }
        let Some(bytes) = self.bloated_capture_reexport_bytes() else {
            return;
        };
        if self.automatic_compact_headroom_denied(CAPTURE_REEXPORT_NAMESPACE, bytes) {
            return;
        }
        let options = crate::storage::laststore::CollectionCompactOptions {
            collection: CAPTURE_REEXPORT_NAMESPACE.to_string(),
            dry_run: false,
            seed_committed_history: false,
        };
        match self.store.compact_collection(options).await {
            Ok(report) => {
                if let Some(reason) = report.skipped_reason.as_deref() {
                    tracing::warn!(
                        target: "fold_db::sync::mutation_log",
                        reason,
                        plane_bytes = bytes,
                        "capture re-export plane compaction refused; the plane \
                         keeps growing two appends per captured write until this \
                         is fixed"
                    );
                    return;
                }
                self.raise_capture_reexport_floor(report.bytes_after);
                tracing::info!(
                    target: "fold_db::sync::mutation_log",
                    live_keys = report.live_keys,
                    bytes_before = report.bytes_before,
                    bytes_after = report.bytes_after,
                    reclaimed_bytes = report
                        .bytes_after
                        .map(|after| report.bytes_before.saturating_sub(after)),
                    "capture re-export plane compacted"
                );
            }
            Err(e) => {
                tracing::warn!(
                    target: "fold_db::sync::mutation_log",
                    error = %e,
                    plane_bytes = bytes,
                    "capture re-export plane compaction failed (retried on a \
                     later cycle)"
                );
            }
        }
    }

    /// Plane bytes, if the plane is over its floor and a probe is due.
    ///
    /// Rate-limited because this runs on the sync cycle. The probe is a
    /// directory stat walk of one collection — no shard loads and no key-index
    /// walk, unlike the `compact --collection` dry run, which would load all
    /// 1024 shard handles and is the exact cost an unattended probe must not pay.
    /// See [`crate::storage::traits::NamespacedStore::collection_disk_bytes`].
    ///
    /// Returns `None` when the cap is disabled, a probe is not yet due, the
    /// backend cannot measure bytes, or the plane is within its floor. The probe
    /// slot is claimed *before* measuring so a concurrent cycle sees "not due"
    /// rather than racing into a second compaction.
    pub(super) fn bloated_capture_reexport_bytes(&self) -> Option<u64> {
        self.bloated_plane_bytes(ChurnPlaneTrigger {
            namespace: CAPTURE_REEXPORT_NAMESPACE,
            last_probe_unix_s: &self.capture_reexport_last_probe_unix_s,
            floor_bytes: &self.capture_reexport_compact_floor_bytes,
            max_bytes: &self.capture_reexport_compact_max_bytes,
            probe_interval_s: &self.capture_reexport_probe_interval_s,
        })
    }

    /// Raise the size trigger's floor so a large live set cannot turn the cap
    /// into a rewrite treadmill.
    ///
    /// The cap assumes the live set is near zero, which holds whenever markers
    /// are being cleared. If capture is failing, they are not: the queue fills
    /// with undrained intents that must be kept, and the plane can legitimately
    /// exceed the cap in live records. Without a floor the trigger would then
    /// rewrite the whole plane every probe interval and reclaim nothing — while
    /// capture is already in trouble. Doubling the post-compaction size makes the
    /// next size-triggered rewrite wait for the plane to double again, so growth
    /// stays bounded and the rewrite rate falls as the live set grows. The floor
    /// never drops below the cap, so a compaction that emptied the plane leaves
    /// the trigger at its default sensitivity.
    pub(super) fn raise_capture_reexport_floor(&self, bytes_after: Option<u64>) {
        raise_plane_floor(
            &self.capture_reexport_compact_floor_bytes,
            &self.capture_reexport_compact_max_bytes,
            bytes_after,
        );
    }
}
