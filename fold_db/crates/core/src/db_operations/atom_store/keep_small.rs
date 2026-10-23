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
/// Ordinary writes keep this debounce. Hard erases stage final aggregate
/// state with their native snapshot batch; shutdown writes the clean stamp.
pub(super) const KEEP_SMALL_PERSIST_INTERVAL: Duration = Duration::from_secs(30);

/// Env override for [`KEEP_SMALL_PERSIST_INTERVAL`], in whole seconds.
pub(super) const KEEP_SMALL_PERSIST_INTERVAL_ENV: &str = "LASTDB_KEEP_SMALL_PERSIST_SECS";

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
        let snap = self.read_keep_small_snapshot(store).await?;
        if let Some((snap, was_legacy_whole_map)) = snap {
            self.hydrate_from_snapshot(store, snap, was_legacy_whole_map)
                .await?;
        } else if self.main_store_is_empty().await?
            && store.inner().scan_prefix_paged(b"", 1).await?.is_empty()
        {
            self.hydrate_empty_home(store).await?;
        } else {
            self.hydrate_snapshot_missed(store).await?;
        }
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

    /// A snapshot exists: import its aggregate values directly, judge its
    /// trust, and re-arm the durable row as "unclean" for this process.
    async fn hydrate_from_snapshot(
        &self,
        store: &Arc<TypedKvStore<dyn KvStore>>,
        snap: crate::db_operations::KeepSmallSnapshot,
        was_legacy_whole_map: bool,
    ) -> Result<(), crate::schema::SchemaError> {
        let clean_stop = snap.clean_stop;
        let supported = snap.version == crate::db_operations::KEEP_SMALL_SNAPSHOT_VERSION;
        self.keep_small.import(snap).map_err(|e| {
            self.keep_small
                .mark_incomplete(&format!("hydrate_import_failed: {e}"));
            crate::schema::SchemaError::InvalidData(format!("hydrate keep-small import: {e}"))
        })?;
        if was_legacy_whole_map {
            // Next persist splits the whole-map row into per-schema shards.
            self.keep_small.mark_all_schemas_dirty();
        }
        if !supported {
            self.keep_small
                .mark_incomplete("unsupported_meter_snapshot_version");
            self.keep_small.mark_all_schemas_dirty();
        }
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
        self.write_keep_small_with_clean_stop(false)
            .await
            .inspect_err(|_| {
                self.keep_small.mark_incomplete("rearm_put_failed");
            })?;
        store.inner().flush().await.map_err(|e| {
            self.keep_small.mark_incomplete("rearm_flush_failed");
            crate::schema::SchemaError::InvalidData(format!("flush keep-small re-arm: {e}"))
        })?;
        Ok(())
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
    ) -> Result<(), crate::schema::SchemaError> {
        self.keep_small.mark_hydrate_missed();
        self.write_keep_small_with_clean_stop(false).await?;
        store.inner().flush().await.map_err(|e| {
            crate::schema::SchemaError::InvalidData(format!("flush keep-small absent state: {e}"))
        })?;
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
}
