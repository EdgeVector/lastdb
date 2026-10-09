//! Index-plane inventory and retired-index residue reclaim.

use super::*;

impl LastStoreNamespacedStore {
    /// Per-prefix key count and stored bytes for the `indexes` plane.
    ///
    /// Read-only, paged, and prefix-scoped — never a whole-collection walk, so
    /// it can run beside a live node without becoming the load it is measuring.
    /// One page per call; the caller follows `after` until `done`.
    ///
    /// This exists because `compact --collection indexes` reports one live-key
    /// total for the whole plane, which cannot say whether that plane is mostly
    /// reclaimable retired residue or mostly rows something still reads. Sizing
    /// the reclaim before running it needs the split.
    pub async fn index_plane_prefix_inventory(
        &self,
        prefix: String,
        after: Option<String>,
        limit: usize,
    ) -> StorageResult<IndexPlanePrefixInventory> {
        if limit == 0 {
            return Err(StorageError::BackendError(
                "index-plane inventory limit must be greater than zero".to_string(),
            ));
        }
        if !INDEX_RESIDUE_KEY_PREFIXES.contains(&prefix.as_str()) {
            return Err(StorageError::BackendError(format!(
                "unknown index-plane prefix {prefix}; expected one of {INDEX_RESIDUE_KEY_PREFIXES:?}"
            )));
        }
        let store = Arc::clone(&self.store);
        LastStoreKvStore::run_blocking(move || {
            let rows = store
                .list_prefix_paged(INDEXES_COLLECTION, &prefix, after.as_deref(), limit)
                .map_err(LastStoreKvStore::map_error)?;
            let mut report = IndexPlanePrefixInventory {
                prefix: prefix.clone(),
                done: rows.len() < limit,
                after: rows.last().map(|(id, _)| id.clone()),
                ..Default::default()
            };
            for (id, value) in &rows {
                report.keys = report.keys.saturating_add(1);
                report.bytes = report
                    .bytes
                    .saturating_add(id.len() as u64 + value.len() as u64);
            }
            if report.done {
                report.after = None;
            }
            Ok(report)
        })
        .await
    }

    /// Delete one page of retired derived-index rows from the `indexes` plane.
    ///
    /// Unlike [`Self::drain_plane_residue_collection`], which **moves** index
    /// keys into `indexes` from legacy split collections, this removes them
    /// outright. Nothing rebuilds them: the prefixes it accepts are the ones
    /// whose engine flags are off, so no read prefers them and no write
    /// restamps them.
    ///
    /// The caller must pass prefixes derived from those flags
    /// (`retired_index_reclaim_prefixes`); this method independently refuses
    /// anything outside [`RECLAIMABLE_RETIRED_INDEX_PREFIXES`], so a caller
    /// that drifts cannot widen the blast radius on its own.
    ///
    /// One page per call, dry-run unless `execute`. Standing rule:
    /// `preference-lastdb-no-engine-derived-hashrange-secondaries`.
    pub async fn reclaim_retired_index_residue(
        &self,
        options: IndexResidueReclaimOptions,
    ) -> StorageResult<IndexResidueReclaimReport> {
        if options.limit == 0 {
            return Err(StorageError::BackendError(
                "index-residue reclaim limit must be greater than zero".to_string(),
            ));
        }
        // An empty prefix would scan — and with `execute`, delete — the whole
        // `indexes` plane, including the rows no retirement covers.
        if options.prefix.trim().is_empty() {
            return Err(StorageError::BackendError(
                "index-residue reclaim requires a key prefix — a whole-plane \
                 delete is not allowed"
                    .to_string(),
            ));
        }
        if !RECLAIMABLE_RETIRED_INDEX_PREFIXES.contains(&options.prefix.as_str()) {
            return Err(StorageError::BackendError(format!(
                "prefix {} is not reclaimable index residue; expected one of {:?}",
                options.prefix, RECLAIMABLE_RETIRED_INDEX_PREFIXES
            )));
        }

        let logical = Arc::clone(&self.logical);
        let store = Arc::clone(&self.store);
        let high_water = self.high_water.clone();
        LastStoreKvStore::run_blocking(move || {
            let rows = store
                .list_prefix_paged(
                    INDEXES_COLLECTION,
                    &options.prefix,
                    options.after.as_deref(),
                    options.limit,
                )
                .map_err(LastStoreKvStore::map_error)?;

            let mut report = IndexResidueReclaimReport {
                prefix: options.prefix.clone(),
                dry_run: !options.execute,
                done: rows.len() < options.limit,
                after: rows.last().map(|(id, _)| id.clone()),
                ..Default::default()
            };

            let mut ops = Vec::new();
            for (id, value) in &rows {
                report.keys_scanned = report.keys_scanned.saturating_add(1);
                let key = LastStoreKvStore::decode_key(id)?;
                // The page came back under `prefix`, but the stored id may
                // carry an org storage prefix, so re-check the bare key rather
                // than trusting the scan bound to have meant the same thing.
                let key = String::from_utf8_lossy(&key);
                if !strip_org_storage_prefix(&key).starts_with(options.prefix.as_str()) {
                    report.skipped = report.skipped.saturating_add(1);
                    continue;
                }
                report.keys_deleted = report.keys_deleted.saturating_add(1);
                report.bytes_freed_approx = report
                    .bytes_freed_approx
                    .saturating_add(id.len() as u64 + value.len() as u64);
                if options.execute {
                    ops.push(TxnOp::delete(INDEXES_COLLECTION, id));
                }
            }

            if options.execute && !ops.is_empty() {
                direct_write_invalidation::after_direct_write(&logical, || {
                    store.transaction(ops).map_err(LastStoreKvStore::map_error)
                })?;
                LastStoreKvStore::record_high_water(high_water.as_ref(), &store)?;
            }

            if report.done {
                report.after = None;
            }
            Ok(report)
        })
        .await
    }
}
