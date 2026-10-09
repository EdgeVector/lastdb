//! Locator plane and residual capture-free plane compaction.

use super::*;

impl PlaneCompactor {
    /// Reclaim the `atom_locators` plane when it outgrows its cap.
    ///
    /// # Why it needs a trigger at all
    ///
    /// A locator row is rewritten whenever its atom is re-addressed, and a
    /// `LastStore` overwrite is an append: the superseded row stays in the
    /// segment. Nothing prunes it. Measured on Tom's primary 2026-08-17 the
    /// plane was 1.45 GiB — the fourth-largest collection in an 18.79 GiB
    /// store — behind one small live row per live atom, and
    /// `lastdb db compact --collection atom_locators` answered "not on the
    /// compact allowlist". The plane had no byte-return path of any kind.
    ///
    /// # Why it is safe with cloud on
    ///
    /// `atom_locators` holds exactly the `aloc:` rows, and `aloc:` is
    /// capture-skipped by `sync::policy::CAPTURE_SKIP_MAIN_KEY_PREFIXES`, so
    /// the rewrite emits no mutation-log records. That is the same property
    /// that makes the marker plane safe, reached by key prefix instead of by
    /// namespace;
    /// `sync::policy::locator_plane_stays_compactable_and_is_a_single_capture_skipped_prefix`
    /// pins both halves.
    ///
    /// # Where this is called from
    ///
    /// Both [`SyncEngine::do_sync`] and [`SyncEngine::run_capture_tick`], for
    /// the reason recorded on
    /// [`SyncEngine::maybe_compact_capture_reexport_plane`]: the tick alone is
    /// gated behind `config.legacy_personal_cloud_sync`, false on the
    /// LastStore backup home every current node runs, and a reclaim reachable
    /// only from a retired code path is not a retention policy.
    pub(crate) async fn maybe_compact_locator_plane(&self) {
        // Checked here, not only in the policy test: an unattended rewrite of a
        // captured plane is the 2026-08-08 `tips` mistake, and it cost 11.58
        // GiB of new pin log. Refuse rather than amplify.
        if !crate::sync::policy::compaction_is_capture_free(LOCATOR_NAMESPACE) {
            tracing::warn!(
                target: "fold_db::sync::mutation_log",
                collection = LOCATOR_NAMESPACE,
                "locator plane is no longer capture-free; skipping \
                 self-compaction so the rewrite cannot enter the mutation log"
            );
            return;
        }
        // Packing lock: hold through the rewrite. A boolean pre-check would
        // race a new BackupPublishTarget cut, retiring chunks under an
        // uncommitted manifest.
        let backup_target = self.lock_backup_publish_target().await;
        if backup_target.is_some() {
            return;
        }
        let min_bps = self
            .locator_compact_min_overhang_bps
            .load(Ordering::Relaxed);
        if min_bps == 0 {
            return;
        }
        let now_s = crate::clock::unix_secs();
        let last = self.locator_last_probe_unix_s.load(Ordering::Relaxed);
        let interval = self.locator_probe_interval_s.load(Ordering::Relaxed);
        if last != 0 && now_s.saturating_sub(last) < interval {
            return;
        }
        if self
            .locator_last_probe_unix_s
            .compare_exchange(last, now_s.max(1), Ordering::SeqCst, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        let Some(usage) = self.store.collection_disk_usage(LOCATOR_NAMESPACE) else {
            return;
        };
        let overhang = usage.allocated_bytes.saturating_sub(usage.apparent_bytes);
        if !overhang_ratio_met(&usage, min_bps) {
            return;
        }
        if self.automatic_compact_headroom_denied(LOCATOR_NAMESPACE, usage.apparent_bytes) {
            return;
        }
        let options = crate::storage::laststore::CollectionCompactOptions {
            collection: LOCATOR_NAMESPACE.to_string(),
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
                        "locator plane compaction refused; the plane keeps \
                         growing one append per re-addressed atom until this \
                         is fixed"
                    );
                    return;
                }
                tracing::info!(
                    target: "fold_db::sync::mutation_log",
                    live_keys = report.live_keys,
                    allocated_bytes_before = usage.allocated_bytes,
                    apparent_bytes_before = usage.apparent_bytes,
                    overhang_bytes_before = overhang,
                    bytes_after = report.bytes_after,
                    "locator filesystem overhang compacted"
                );
            }
            Err(e) => {
                tracing::warn!(
                    target: "fold_db::sync::mutation_log",
                    error = %e,
                    allocated_bytes = usage.allocated_bytes,
                    apparent_bytes = usage.apparent_bytes,
                    overhang_bytes = overhang,
                    "locator plane compaction failed (retried on a later cycle)"
                );
            }
        }
    }

    /// Reclaim every capture-free allowlisted plane that has no compactor of
    /// its own.
    ///
    /// # Why a sweep rather than a fourth bespoke compactor
    ///
    /// `sync_pin_log`, `sync_capture_reexport` and `atom_locators` each got a
    /// hand-written trigger, in that order, each time the plane had already
    /// grown past a gigabyte before anyone noticed. `schema_index`,
    /// `idempotency` and `change_feed` are the same shape — capture-skipped
    /// bookkeeping whose superseded records are appends that nothing prunes —
    /// and on 2026-08-17 they held 447 MiB between them with a byte-return verb
    /// nobody was calling. A list plus one loop arms all of them, and arms the
    /// next one for free: `RESIDUAL_SELF_COMPACT_PLANES` is what the policy bar
    /// checks a new allowlist entry against.
    ///
    /// # Why it is safe with cloud on
    ///
    /// Every plane in the sweep is capture-skipped, so the rewrite emits no
    /// mutation-log records and cannot repeat the 2026-08-08 `tips` compaction
    /// that turned a reclaim into 11.58 GiB of new pin log. That is asserted in
    /// `sync::policy` *and* re-checked here, per plane, immediately before the
    /// rewrite — a list can be edited, and the check that matters is the one at
    /// the call site.
    ///
    /// # Where this is called from
    ///
    /// Both [`SyncEngine::do_sync`] and [`SyncEngine::run_capture_tick`], for
    /// the reason recorded on
    /// [`SyncEngine::maybe_compact_capture_reexport_plane`]: the tick alone is
    /// gated behind `config.legacy_personal_cloud_sync`, false on the LastStore
    /// backup home every current node runs, and a reclaim reachable only from a
    /// retired code path is not a retention policy.
    pub(crate) async fn maybe_compact_residual_capture_free_planes(&self) {
        for plane in RESIDUAL_SELF_COMPACT_PLANES {
            self.maybe_compact_residual_plane(plane).await;
        }
    }
}
