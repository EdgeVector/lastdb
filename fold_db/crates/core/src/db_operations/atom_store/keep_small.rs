// lint:file-size-ok verbatim move out of the 2.6k-line atom_store/mod.rs; one AtomStore theme per file, split further when next touched
//! Keep-small projection: hydrate, debounced persist, and snapshot repair.

use super::*;

/// Main-store key that records "a process booted past a failed keep-small
/// hydrate". Outside the keep_small plane on purpose: that plane's group may
/// be the one that cannot be loaded. See
/// [`AtomStore::note_keep_small_hydrate_failure`].
pub(super) const KEEP_SMALL_DISTRUST_KEY: &str = "keep_small:hydrate_distrust";

/// Minimum wall time between durable writes of the keep-small projection.
///
/// The live projection is one in-memory map. The durable form is a small
/// header plus one shard per dirty schema, so a persist does not rewrite
/// unchanged schemas. Write paths call [`AtomStore::persist_keep_small`] on
/// *every* atom and molecule put. A whole-map blob at this cadence filled
/// the `keep_small` plane at ~1.2 MB/s on the primary (36 MB every 30 s).
///
/// The projection is a gauge, not correctness state (see
/// [`AtomStore::hydrate_keep_small`]: a missing key is simply a fresh home),
/// so trading up to this much staleness after an unclean stop is safe.
/// Override with `LASTDB_KEEP_SMALL_PERSIST_SECS` (0 = write inline, the
/// pre-debounce behavior).
///
/// **Never call [`AtomStore::flush_keep_small`] from a write path.** It
/// bypasses this interval by design (it exists for shutdown and for the rare
/// hard-erase debit), so a flush after every `persist_keep_small` turns the
/// debounce back into an inline whole-map write. That is exactly what fold
/// PR #2127 did on 2026-09-19: the map by then held one counter per
/// molecule (~7 MB per snapshot on the primary), and every molecule, protein
/// and schema write appended a fresh copy. Measured on the primary on
/// 2026-09-21: 6,782 segments, 39 GB, in ONE `metadata` hash group in a day
/// (~16 GB/hour at fleet write rates). `metadata` has no automatic
/// compaction, a cold group loads whole, and the first write after boot
/// pulled that group in, blew the 16 GiB memory guard and restarted the
/// daemon every 7-17 minutes. `keep_small_persist_stays_debounced_under_a_
/// molecule_write_burst` in `storage_breakdown.rs` guards the count of
/// durable writes per burst.
pub(super) const KEEP_SMALL_PERSIST_INTERVAL: Duration = Duration::from_secs(30);

/// Env override for [`KEEP_SMALL_PERSIST_INTERVAL`], in whole seconds.
pub(super) const KEEP_SMALL_PERSIST_INTERVAL_ENV: &str = "LASTDB_KEEP_SMALL_PERSIST_SECS";

#[cfg(debug_assertions)]
pub(super) fn keep_small_startup_fault(point: &str) -> bool {
    std::env::var("LASTDB_TEST_KEEP_SMALL_STARTUP_FAULT").as_deref() == Ok(point)
}

#[cfg(not(debug_assertions))]
pub(super) fn keep_small_startup_fault(_point: &str) -> bool {
    false
}

/// Resolve the debounce interval from the environment, falling back to
/// [`KEEP_SMALL_PERSIST_INTERVAL`] when unset or unparseable.
pub(super) fn keep_small_persist_interval() -> Duration {
    env_flag::var_parsed::<u64>(KEEP_SMALL_PERSIST_INTERVAL_ENV)
        .map_or(KEEP_SMALL_PERSIST_INTERVAL, Duration::from_secs)
}

mod persist;

impl AtomStore {
    /// Incremental keep-small meters (live budget + per-schema churn).
    #[must_use]
    pub fn keep_small(&self) -> &KeepSmallMeters {
        &self.keep_small
    }

    /// Persist the keep-small projection in `store`.
    ///
    /// Since 2026-09-21 that is the dedicated `keep_small` collection (see
    /// [`crate::db_operations::KEEP_SMALL_SNAPSHOT_COLLECTION`]), not
    /// `metadata`. The durable form is a small header plus one shard per
    /// dirty schema, on a plane the daemon self-compacts. Its former home,
    /// `metadata/keep_small:meters`, is the 39 GB group from the primary
    /// restart loop; hydrate never reads it again.
    #[must_use]
    pub(crate) fn with_keep_small_persist(mut self, store: Arc<TypedKvStore<dyn KvStore>>) -> Self {
        self.keep_small_persist = Some(store);
        self
    }

    /// Load a previously persisted snapshot.
    ///
    /// A missing key is NOT proof of a fresh home — an existing home upgraded
    /// from a build that never wrote the snapshot misses in exactly the same
    /// way. The meters cannot tell those apart on their own, so a miss is
    /// recorded rather than assumed benign
    /// ([`crate::db_operations::KeepSmallMeters::mark_hydrate_missed`]) and the
    /// read surfaces decide what to do with it. `GET /api/storage/app` refuses
    /// to call such a report complete.
    ///
    /// Reads only the store handed to [`Self::with_keep_small_persist`]. There
    /// is deliberately no fallback to the legacy `metadata/keep_small:meters`
    /// key: on a home that lived through the 2026-09-21 flush storm that key
    /// sits in a 39 GB hash group, and a point get of it is the whole-group
    /// load that killed the primary. A home upgraded across the move hydrates
    /// as a miss and rebuilds through the bootstrap path;
    /// `lastdb db reclaim-keep-small-legacy` drops the dead group.
    /// One bounded row read: true when the main store holds no row at all.
    pub(super) async fn main_store_is_empty(&self) -> Result<bool, crate::schema::SchemaError> {
        let first = self
            .main_store
            .inner()
            .scan_prefix_paged(b"", 1)
            .await
            .map_err(|e| {
                crate::schema::SchemaError::InvalidData(format!(
                    "probe empty home for keep-small hydrate: {e}"
                ))
            })?;
        Ok(first.is_empty())
    }

    pub async fn hydrate_keep_small(&self) -> Result<(), crate::schema::SchemaError> {
        let Some(store) = self.keep_small_persist.as_ref() else {
            return Ok(());
        };
        // A failed read must never leave the constructor's exact default in
        // memory. The caller can still enter the explicit repair constructor.
        self.keep_small.mark_incomplete("snapshot_read_failed");
        if keep_small_startup_fault("snapshot_get") {
            return Err(crate::schema::SchemaError::InvalidData(
                "hydrate keep-small: injected snapshot get failure".to_string(),
            ));
        }
        let snap = self.read_keep_small_snapshot(store).await?;
        let durable_debits = store
            .get_item::<KeepSmallHardEraseTotals>(
                crate::db_operations::KEEP_SMALL_HARD_ERASE_TOTALS_KEY,
            )
            .await
            .map_err(|e| {
                self.keep_small
                    .mark_incomplete("hard_erase_totals_read_failed");
                crate::schema::SchemaError::InvalidData(format!(
                    "hydrate hard-erase meter totals: {e}"
                ))
            })?
            .unwrap_or_default();
        let journal_checkpoint = snap.as_ref().map_or(0, |(snapshot, _)| {
            snapshot.hard_erase_journal_checkpoint_seq
        });
        let journal = self
            .read_keep_small_hard_erase_journal(journal_checkpoint)
            .await?;
        let latest_seq =
            self.install_hard_erase_hydrate_state(&durable_debits, &journal, journal_checkpoint)?;
        if let Some((snap, was_legacy_whole_map)) = snap {
            self.hydrate_from_snapshot(
                store,
                snap,
                was_legacy_whole_map,
                &durable_debits,
                &journal,
                latest_seq,
            )
            .await?;
        } else if durable_debits.by_schema.is_empty()
            && journal.totals.by_schema.is_empty()
            && journal.pending_keys.is_empty()
            && self.main_store_is_empty().await?
        {
            self.hydrate_empty_home(store).await?;
        } else {
            self.hydrate_snapshot_missed(store, &durable_debits, &journal)
                .await?;
        }
        self.count_pending_protein_folds().await?;
        // Every branch above wrote and flushed a durable row whose trust is
        // at most what this process can vouch for, so an earlier failed boot
        // no longer needs its marker.
        self.clear_keep_small_distrust_marker().await;
        Ok(())
    }

    /// Read the header row and merge its per-schema shards. Returns the
    /// snapshot and whether it was a legacy whole-map row.
    async fn read_keep_small_snapshot(
        &self,
        store: &Arc<TypedKvStore<dyn KvStore>>,
    ) -> Result<Option<(crate::db_operations::KeepSmallSnapshot, bool)>, crate::schema::SchemaError>
    {
        let snap = store
            .get_item::<crate::db_operations::KeepSmallSnapshot>(
                crate::db_operations::KEEP_SMALL_SNAPSHOT_KEY,
            )
            .await
            .map_err(|e| {
                crate::schema::SchemaError::InvalidData(format!("hydrate keep-small: {e}"))
            })?;
        let Some(mut snapshot) = snap else {
            return Ok(None);
        };
        let was_legacy_whole_map =
            !snapshot.sharded && (!snapshot.schemas.is_empty() || !snapshot.molecules.is_empty());
        if !was_legacy_whole_map {
            self.merge_keep_small_shards(store.as_ref(), &mut snapshot)
                .await?;
            snapshot.sharded = true;
        }
        Ok(Some((snapshot, was_legacy_whole_map)))
    }

    /// Seed the in-memory hard-erase totals, sequence counters and orphaned
    /// intents from the durable totals and the journal. Returns the latest
    /// journal sequence.
    fn install_hard_erase_hydrate_state(
        &self,
        durable_debits: &KeepSmallHardEraseTotals,
        journal: &super::hard_erase_journal::HardEraseJournalReplay,
        journal_checkpoint: u64,
    ) -> Result<u64, crate::schema::SchemaError> {
        *self.keep_small_hard_erase_totals.lock().map_err(|_| {
            crate::schema::SchemaError::InvalidData(
                "keep-small hard-erase totals lock poisoned".to_string(),
            )
        })? = durable_debits.clone();
        let latest_seq = journal_checkpoint.max(journal.max_seq);
        self.keep_small_hard_erase_next_seq
            .store(latest_seq, Ordering::Relaxed);
        self.keep_small_hard_erase_applied_seq
            .store(journal_checkpoint, Ordering::Relaxed);
        *self.keep_small_hard_erase_orphaned.lock().map_err(|_| {
            crate::schema::SchemaError::InvalidData(
                "keep-small orphaned intent lock poisoned".to_string(),
            )
        })? = journal.pending_keys.iter().cloned().collect();
        Ok(latest_seq)
    }

    /// A snapshot exists: import it, replay the hard-erase debits, judge its
    /// trust, and re-arm the durable row as "unclean" for this process.
    async fn hydrate_from_snapshot(
        &self,
        store: &Arc<TypedKvStore<dyn KvStore>>,
        snap: crate::db_operations::KeepSmallSnapshot,
        was_legacy_whole_map: bool,
        durable_debits: &KeepSmallHardEraseTotals,
        journal: &super::hard_erase_journal::HardEraseJournalReplay,
        latest_seq: u64,
    ) -> Result<(), crate::schema::SchemaError> {
        let clean_stop = snap.clean_stop;
        let checkpoint = snap.hard_erase_totals.clone();
        self.keep_small.import(snap).map_err(|e| {
            self.keep_small
                .mark_incomplete(&format!("hydrate_import_failed: {e}"));
            crate::schema::SchemaError::InvalidData(format!("hydrate keep-small import: {e}"))
        })?;
        if was_legacy_whole_map {
            // Next persist splits the whole-map row into per-schema shards.
            self.keep_small.mark_all_schemas_dirty();
        }
        self.replay_hard_erase_after_import(
            clean_stop,
            &checkpoint,
            durable_debits,
            journal,
            latest_seq,
        );
        // `Some(true)`: written by the shutdown flush, and no boot re-armed
        // it since, so the meters cover every metered write. `None`: a
        // pre-2026-09-21 snapshot, no verdict, as before. `Some(false)`:
        // the last writer was a runtime persist, so the process stopped
        // without its shutdown flush and metered writes after the last
        // debounce tick are missing. Keep the numbers as a hint; the
        // reports say `stale_after_unclean_stop` until the liveness
        // bootstrap re-measures, as they do for a miss.
        if clean_stop == Some(false) {
            tracing::warn!(
                target: "fold_node::database",
                "keep-small snapshot was not written by a clean shutdown; storage \
                 reports are incomplete until the liveness bootstrap runs"
            );
            self.keep_small.mark_stale_after_unclean_stop();
        }
        // A previous process booted past a failed hydrate and could not
        // re-arm, so this row may still claim a clean stop that covers
        // none of that process's writes. Distrust it; the re-arm below
        // writes the incomplete state, and only then is the marker cleared.
        if self.keep_small_distrust_marker_present().await {
            tracing::warn!(
                target: "fold_node::database",
                "keep-small snapshot follows a boot that could not read it; meters marked \
                 incomplete until the liveness bootstrap re-measures"
            );
            self.keep_small
                .mark_incomplete("previous_boot_hydrate_failed");
        }
        // Re-arm: from here on the durable row must read "unclean" until
        // this process's own shutdown flush rewrites it. One header put
        // per boot (plus a one-time shard split of a legacy whole-map
        // row), before any product write can be metered. If it fails
        // the next boot may read a clean stop that this process did not
        // make, so say so.
        if keep_small_startup_fault("rearm_put") {
            self.keep_small.mark_incomplete("rearm_put_failed");
            return Err(crate::schema::SchemaError::InvalidData(
                "persist keep-small: injected re-arm put failure".to_string(),
            ));
        }
        self.write_keep_small_with_clean_stop(false)
            .await
            .inspect_err(|_| {
                self.keep_small.mark_incomplete("rearm_put_failed");
            })?;
        if keep_small_startup_fault("rearm_flush") {
            self.keep_small.mark_incomplete("rearm_flush_failed");
            return Err(crate::schema::SchemaError::InvalidData(
                "flush keep-small re-arm: injected failure".to_string(),
            ));
        }
        store.inner().flush().await.map_err(|e| {
            self.keep_small.mark_incomplete("rearm_flush_failed");
            crate::schema::SchemaError::InvalidData(format!("flush keep-small re-arm: {e}"))
        })?;
        Ok(())
    }

    /// Replay the durable hard-erase debits, and the journal when the snapshot
    /// is trusted, onto the freshly imported meters.
    fn replay_hard_erase_after_import(
        &self,
        clean_stop: Option<bool>,
        checkpoint: &KeepSmallHardEraseTotals,
        durable_debits: &KeepSmallHardEraseTotals,
        journal: &super::hard_erase_journal::HardEraseJournalReplay,
        latest_seq: u64,
    ) {
        let replay_safe =
            clean_stop == Some(true) && self.keep_small.all_counter_domains_complete();
        self.replay_keep_small_hard_erase_totals(checkpoint, durable_debits);
        if replay_safe {
            self.replay_keep_small_hard_erase_totals(
                &KeepSmallHardEraseTotals::default(),
                &journal.totals,
            );
            self.keep_small_hard_erase_applied_seq
                .store(latest_seq, Ordering::Relaxed);
        } else if !journal.totals.by_schema.is_empty() {
            // The dirty snapshot can omit later write credits. Its
            // journal debits cannot be applied against that old base.
            self.keep_small
                .mark_incomplete("hard_erase_journal_requires_repair");
            self.keep_small_hard_erase_replay_skipped
                .store(true, Ordering::Relaxed);
        }
        if !journal.pending_keys.is_empty() {
            self.keep_small
                .mark_incomplete("hard_erase_intent_unresolved");
            self.keep_small_hard_erase_replay_skipped
                .store(true, Ordering::Relaxed);
        }
    }

    /// No snapshot on a home with no main-store row: nothing could have been
    /// missed, so the meters start complete.
    async fn hydrate_empty_home(
        &self,
        store: &Arc<TypedKvStore<dyn KvStore>>,
    ) -> Result<(), crate::schema::SchemaError> {
        // A home with no main-store row at all has nothing the meters
        // could have missed: every row it will ever hold is metered by
        // this process from the first write. Treating it as a miss left a
        // brand-new home "incomplete" until an operator ran the copy-only
        // liveness bootstrap
        // (papercut-fresh-home-logical-attribution-needs-copy-bootstrap-20260920).
        // A home upgraded from a pre-snapshot build has tips, so it still
        // misses.
        self.keep_small.mark_empty_home_absent();
        tracing::info!(
            target: "fold_node::database",
            "keep-small: no snapshot on an empty home; meters start complete"
        );
        // Persist the explicit absent state. The empty-home exception is
        // exact because the authoritative main plane is empty, not because
        // a missing row proves zero.
        let mut snapshot = self.keep_small.export();
        snapshot.trust = MeterTrustPayload::absent();
        self.write_keep_small_snapshot(&snapshot).await?;
        store.inner().flush().await.map_err(|e| {
            crate::schema::SchemaError::InvalidData(format!("flush keep-small absent state: {e}"))
        })?;
        Ok(())
    }

    /// No snapshot on a home that holds data: the meters missed it.
    async fn hydrate_snapshot_missed(
        &self,
        store: &Arc<TypedKvStore<dyn KvStore>>,
        durable_debits: &KeepSmallHardEraseTotals,
        journal: &super::hard_erase_journal::HardEraseJournalReplay,
    ) -> Result<(), crate::schema::SchemaError> {
        self.keep_small.mark_hydrate_missed();
        if !durable_debits.by_schema.is_empty()
            || !journal.totals.by_schema.is_empty()
            || !journal.pending_keys.is_empty()
        {
            self.keep_small
                .mark_incomplete("hard_erase_snapshot_absent");
            self.keep_small_hard_erase_replay_skipped
                .store(true, Ordering::Relaxed);
        }
        self.write_keep_small_with_clean_stop(false).await?;
        store.inner().flush().await.map_err(|e| {
            crate::schema::SchemaError::InvalidData(format!("flush keep-small absent state: {e}"))
        })?;
        Ok(())
    }

    pub(super) fn replay_keep_small_hard_erase_totals(
        &self,
        checkpoint: &KeepSmallHardEraseTotals,
        durable: &KeepSmallHardEraseTotals,
    ) {
        for (schema, before) in &checkpoint.by_schema {
            if !durable.by_schema.contains_key(schema)
                && *before != KeepSmallHardEraseDebit::default()
            {
                self.keep_small
                    .mark_incomplete("hard_erase_totals_regressed");
            }
        }
        for (schema, after) in &durable.by_schema {
            let before = checkpoint
                .by_schema
                .get(schema)
                .copied()
                .unwrap_or_default();
            let Some(atom_bytes) = after.atom_bytes.checked_sub(before.atom_bytes) else {
                self.keep_small
                    .mark_incomplete("hard_erase_totals_regressed");
                continue;
            };
            let Some(atom_count) = after.atom_count.checked_sub(before.atom_count) else {
                self.keep_small
                    .mark_incomplete("hard_erase_totals_regressed");
                continue;
            };
            let Some(tip_bytes) = after.tip_bytes.checked_sub(before.tip_bytes) else {
                self.keep_small
                    .mark_incomplete("hard_erase_totals_regressed");
                continue;
            };
            let Some(tip_count) = after.tip_count.checked_sub(before.tip_count) else {
                self.keep_small
                    .mark_incomplete("hard_erase_totals_regressed");
                continue;
            };
            let delta = KeepSmallHardEraseDebit {
                atom_bytes,
                atom_count,
                tip_bytes,
                tip_count,
            };
            if delta != KeepSmallHardEraseDebit::default() {
                self.keep_small.replay_hard_erase_debit(schema, delta);
            }
        }
    }

    /// Count queued protein-fold jobs into the meters. Part of a normal
    /// hydrate, and also run on its own after a failed hydrate so a later
    /// bootstrap in this process does not commit trust with a zero count.
    pub(super) async fn count_pending_protein_folds(
        &self,
    ) -> Result<(), crate::schema::SchemaError> {
        let fold_jobs: Vec<(String, serde_json::Value)> = self
            .main_store
            .scan_items_with_prefix(crate::protein::PROTEIN_FOLD_JOB_PREFIX)
            .await
            .map_err(|e| {
                crate::schema::SchemaError::InvalidData(format!(
                    "hydrate pending protein folds: {e}"
                ))
            })?;
        self.keep_small
            .set_pending_protein_folds(fold_jobs.len() as u64);
        Ok(())
    }

    /// Boot continues after a failed keep-small hydrate (Tom, 2026-09-25:
    /// fail-closed meter trust, not fail-closed availability). Leave a durable
    /// record of that outside the keep_small group, because this process may
    /// not be able to re-arm the snapshot (the group can be over the hard
    /// cold-load cap, so every put there fails) and the old row may claim a
    /// clean stop. The next hydrate that can read the row sees the marker and
    /// distrusts it. Best effort: a failure here is logged, not returned.
    pub async fn note_keep_small_hydrate_failure(&self, cause: &str) {
        if self.keep_small.trust_incomplete_cause().is_none() {
            self.keep_small
                .mark_incomplete("hydrate_failed_boot_continued");
        }
        let marker = serde_json::json!({
            "at": chrono::Utc::now().to_rfc3339(),
            "cause": cause,
        });
        if let Err(e) = self
            .main_store
            .put_item(KEEP_SMALL_DISTRUST_KEY, &marker)
            .await
        {
            tracing::warn!(
                target: "fold_node::database",
                error = %e,
                "could not write the keep-small distrust marker"
            );
        }
        if let Err(e) = self.count_pending_protein_folds().await {
            tracing::warn!(
                target: "fold_node::database",
                error = %e,
                "could not count pending protein folds after a failed keep-small hydrate"
            );
        }
    }

    pub(crate) async fn keep_small_distrust_marker_present(&self) -> bool {
        match self
            .main_store
            .get_item::<serde_json::Value>(KEEP_SMALL_DISTRUST_KEY)
            .await
        {
            Ok(found) => found.is_some(),
            // Cannot tell: distrust, the safe direction.
            Err(_) => true,
        }
    }

    pub(super) async fn clear_keep_small_distrust_marker(&self) {
        if let Err(e) = self.main_store.delete_item(KEEP_SMALL_DISTRUST_KEY).await {
            tracing::warn!(
                target: "fold_node::database",
                error = %e,
                "could not clear the keep-small distrust marker; the next boot distrusts again"
            );
        }
    }

    /// Measure JSON sizes of tip rows that are about to be deleted.
    ///
    /// Prefers the in-process last-accounted size so the debit matches the
    /// credit. After hydrate that map is empty, so this falls back to a
    /// point-get of the still-live row (scan-free).
    pub(crate) async fn keep_small_measure_tip_keys(&self, keys: &[String]) -> Vec<(String, u64)> {
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            let accounted = self.keep_small.last_key_bytes(key);
            if accounted > 0 {
                out.push((key.clone(), accounted));
                continue;
            }
            if let Ok(Some(value)) = self.main_store.get_item::<serde_json::Value>(key).await {
                let bytes = serde_json::to_vec(&value).map_or(0, |v| v.len() as u64);
                if bytes > 0 {
                    out.push((key.clone(), bytes));
                }
            }
        }
        out
    }
}
