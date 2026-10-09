use super::*;

use std::collections::HashSet;

impl SyncEngine {
    /// Reclaim bytes from the capture-free and captured planes. Runs every
    /// cycle on every home shape, independent of upload readiness.
    pub(super) async fn compact_local_planes(&self) {
        // Reclaim the capture re-export marker plane here, above the staging
        // branch, so it runs on every home shape and every cycle.
        //
        // The marker lifecycle is two appends per captured write — stage then
        // delete — and `LastStore::delete` appends a delete line rather than
        // removing the record, so the plane only grows while its live set
        // returns to zero. The reclaim used to hang off `run_capture_tick`
        // alone, which is gated behind `legacy_personal_cloud_sync` below and
        // therefore never runs on a LastStore backup home. Neither the blocked
        // branch nor the modern branch reached it.
        //
        // Disk reclaim does not depend on upload readiness: a plane of dead
        // markers is exactly as reclaimable when the backup is blocked as when
        // it is draining, so this sits above `backup_blocked` too.
        self.maybe_compact_capture_reexport_plane().await;

        // Same placement, same reason, for the `atom_locators` plane: an atom
        // that is re-addressed overwrites its locator row, the overwrite is an
        // append, and nothing prunes the superseded record. It reached 1.45 GiB
        // on the primary with no byte-return path at all — the collection was
        // not even on `COMPACT_ALLOWLIST`. Its rows are capture-skipped by the
        // `aloc:` prefix, so the rewrite emits no mutation-log records.
        self.maybe_compact_locator_plane().await;

        // `tips` is captured user state, but its physical LastStore rewrite is
        // capture-neutral under the outer wrapper. Trigger only on proportional
        // filesystem overhang and never while a backup cut is held.
        self.maybe_compact_tips_plane().await;

        // `atoms` and `field_update_order_log` have explicit crash-safe,
        // capture-neutral physical rewrite contracts. Probe overhang
        // (allocated − live) against per-plane ratio + floor and hold the
        // photograph packing lock across any rewrite. Atoms leaves Cloud Sync
        // on (D5 capture-suppress). Order-log still pauses uploads.
        self.maybe_compact_large_captured_planes().await;

        // Same placement, same reason, for every *other* capture-free plane on
        // `COMPACT_ALLOWLIST` — `schema_index`, `idempotency`, `change_feed`.
        // Each had a byte-return verb and no byte-return policy: 447 MiB
        // between them on the primary 2026-08-17 that only an operator who
        // happened to name that collection would ever have reclaimed. The
        // sweep is a list plus a loop rather than a fourth bespoke compactor,
        // so the next plane admitted to the allowlist is armed by adding a
        // string — and the policy bar fails the build if it is not.
        self.maybe_compact_residual_capture_free_planes().await;
    }

    /// Compact scoped (org/share) target logs, leak-guarded like scoped downloads.
    pub(super) async fn compact_scoped_targets(
        &self,
        targets: &[SyncTarget],
        scoped_idxs: &[usize],
        scoped_total: usize,
        proven_prefixes: &HashSet<String>,
    ) {
        // Scoped-target compaction is independent of whether THIS device has
        // local entries to upload this cycle. The 2026-07-20 fix made one
        // scoped upload failure non-starving for another target *within* a
        // non-empty upload cycle; before that pass lived inside
        // `if !entries.is_empty()`, a read-mostly multi-device reader (empty
        // outbox every cycle) never evaluated org/share log growth and left
        // remote scoped logs unbounded. Run after download phases so the
        // download cursor high-water guard in `compact_target` is as current
        // as this cycle can make it. `maybe_compact_target_log` no-ops when
        // the remote log is under threshold.
        //
        // Leak-guarded like scoped downloads (2026-08-31). This loop used to
        // visit EVERY scoped target each cycle: on a primary carrying ~319
        // leaked dogfood `org_hash` rows that meant ~319 authed
        // `list_log_objects` round-trips per cycle (each able to stall 30s on
        // an auth-Lambda timeout), plus `prove_prefix_decryptable` + a
        // photograph build for any target over threshold — all during the same
        // cycles whose RSS spikes crossed the 16 GiB footprint guard and got
        // the primary SIGKILLed mid-soak. Above the threshold,
        // `scoped_downloads_per_cycle` still seeds a download cursor for one
        // target per cycle (round-robin), so most targets on any given cycle
        // still lack a fresh cursor and would refuse at the hollow-stamp gate
        // after paying for the listing and the proof anyway — visiting all of
        // them every cycle to find the rare one worth compacting is not worth
        // the fan-out cost (fold #1845 tracks compaction fan-out separately).
        // Healthy configs (≤4 scoped targets) still evaluate every scoped
        // target every cycle — an under-threshold evaluation is one list
        // call, so only the leak shape needs the guard.
        if scoped_total > 4 {
            tracing::warn!(
                target: "fold_db::sync::memory",
                scoped_total,
                "skipping scoped-target compaction: too many org/share targets registered \
                 (likely leaked test orgs); scoped downloads are skipped in this state, so \
                 no download cursor can seed and compaction would refuse the stamp anyway"
            );
        } else {
            for &idx in scoped_idxs {
                let target = &targets[idx];
                if let Err(e) = self.maybe_compact_target_log(target, proven_prefixes).await {
                    tracing::warn!(
                        target = %target.label,
                        error = %e,
                        "scoped-target compaction failed (non-fatal)"
                    );
                }
            }
        }
    }

    /// Re-snapshot personal data rarely, with backoff after a failed attempt.
    pub(super) async fn compact_personal_log_with_backoff(
        &self,
        proven_prefixes: &HashSet<String>,
    ) {
        // Compaction: re-snapshot personal data RARELY, driven by log SIZE
        // (and bounded by a min/max time window) rather than a low entry count.
        // A snapshot is ≈ a full DB copy, so we only want one once the log it
        // would replace has itself grown ≈ DB-sized — that crossover compacts
        // dead history, halves future bootstrap work, and bounds cloud storage.
        //
        // A failed attempt backs off (see `CompactionFailureBackoff`): each
        // attempt builds a full-store photograph before it can fail, so an
        // unconditional per-cycle retry is a memory storm on an idle node.
        let backoff_now = crate::clock::unix_secs();
        let backoff = *self.personal_compaction_backoff.lock().await;
        if backoff.allows(backoff_now) {
            // `succeeded` is set only when an attempt ran to Ok. A cycle that
            // attempts nothing (busy `pending`, size trigger not met) must not
            // clear the backoff, or the doubling resets on a busy node.
            let mut compacted_personal = false;
            let mut personal_failed = false;
            let mut succeeded = false;
            if self.should_compact_now().await {
                let compact_high_water = self.personal_compaction_high_water().await;
                if compact_high_water > 0 {
                    if let Err(e) = self.compact(compact_high_water, proven_prefixes).await {
                        let retry_in_secs = self
                            .personal_compaction_backoff
                            .lock()
                            .await
                            .record_failure(backoff_now);
                        personal_failed = true;
                        tracing::warn!(retry_in_secs, "compaction failed (non-fatal): {e}");
                    } else {
                        compacted_personal = true;
                        succeeded = true;
                    }
                }
            }
            if !compacted_personal && !personal_failed && self.pending.lock().await.is_empty() {
                // Ok(()) is a completed compaction or a trigger that no
                // longer holds; both end the failure streak.
                match self.maybe_compact_personal_log(proven_prefixes).await {
                    Ok(()) => succeeded = true,
                    Err(e) => {
                        let retry_in_secs = self
                            .personal_compaction_backoff
                            .lock()
                            .await
                            .record_failure(backoff_now);
                        tracing::warn!(
                            retry_in_secs,
                            "remote-log-count compaction failed (non-fatal): {e}"
                        );
                    }
                }
            }
            if succeeded {
                self.personal_compaction_backoff
                    .lock()
                    .await
                    .record_success();
            }
        } else {
            tracing::debug!(
                consecutive_failures = backoff.consecutive_failures,
                retry_at_secs = backoff.retry_at_secs,
                "personal compaction deferred: backing off after failure"
            );
        }
    }
}
