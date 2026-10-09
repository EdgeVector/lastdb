//! Mutation-history clear, schema-index and ref-blob purge, and the prefix row scanner.

use super::*;

impl AtomStore {
    /// Delete mutation-history rows, keeping the newest `keep_last_per_key`
    /// events per (molecule, field_key).
    ///
    /// - **`keep_last_per_key == 0`**: full purge — delete **all** history rows
    ///   for the selected schemas. Latest-only storage: current tips (`mk:`)
    ///   and atoms remain; time-travel / `as_of` over deleted events is gone.
    /// - **`keep_last_per_key >= 1`**: keep the newest N events per field key.
    ///
    /// Does **not** delete current tips or atoms (safe; may leave orphan atoms
    /// until a later GC).
    // lint:fn-size-ok verbatim move from admin_db.rs; splitting this function is separate work
    pub async fn clear_mutation_history(
        &self,
        schema_field_molecules: &[(String, Vec<String>)],
        keep_last_per_key: usize,
        dry_run: bool,
        storage_prefix: Option<&str>,
    ) -> Result<HistoryClearReport, SchemaError> {
        let keep = keep_last_per_key;
        let mut report = HistoryClearReport {
            dry_run,
            keep_last_per_key: keep,
            schemas_touched: 0,
            history_rows_deleted: 0,
            history_bytes_freed_approx: 0,
            per_schema: Vec::new(),
        };

        for (schema_name, mols) in schema_field_molecules {
            let mut schema_deleted = 0u64;
            let mut schema_bytes = 0u64;
            for mol in mols {
                let events_with_keys: Vec<(String, MutationEvent)> = {
                    let base = crate::atom::molecule_key_codec::history_molecule_prefix(mol);
                    self.scan_exact_storage_prefix(&base, storage_prefix, "clear_history load")
                        .await?
                };
                if events_with_keys.is_empty() {
                    continue;
                }

                let mut to_delete: Vec<String> = Vec::new();
                let mut key_bytes: HashMap<String, u64> = HashMap::new();

                if keep == 0 {
                    // Latest-only: purge every history row for this molecule.
                    for (storage_key, ev) in &events_with_keys {
                        let b = storage_key.len() as u64
                            + serde_json::to_vec(ev).map_or(0, |v| v.len() as u64);
                        key_bytes.insert(storage_key.clone(), b);
                        to_delete.push(storage_key.clone());
                    }
                } else {
                    // Group by field_key; keep newest `keep` events per key.
                    let mut by_key: HashMap<String, Vec<(String, i64)>> = HashMap::new();
                    for (storage_key, ev) in &events_with_keys {
                        let fk = format!(
                            "{}|{}",
                            ev.field_key.hash.as_deref().unwrap_or(""),
                            ev.field_key.range.as_deref().unwrap_or("")
                        );
                        let ts = ev.timestamp.timestamp_nanos_opt().unwrap_or(0);
                        let b = storage_key.len() as u64
                            + serde_json::to_vec(ev).map_or(0, |v| v.len() as u64);
                        by_key
                            .entry(fk)
                            .or_default()
                            .push((storage_key.clone(), ts));
                        key_bytes.insert(storage_key.clone(), b);
                    }

                    for (_fk, mut rows) in by_key {
                        rows.sort_by_key(|b| std::cmp::Reverse(b.1)); // newest first
                        if rows.len() > keep {
                            for (k, _) in rows.into_iter().skip(keep) {
                                to_delete.push(k);
                            }
                        }
                    }
                }

                for k in &to_delete {
                    schema_deleted += 1;
                    schema_bytes += key_bytes.get(k).copied().unwrap_or(0);
                }

                if !dry_run && !to_delete.is_empty() {
                    self.raw().batch_delete_keys(to_delete).await.map_err(|e| {
                        SchemaError::InvalidData(format!("clear_history delete {schema_name}: {e}"))
                    })?;
                }
            }
            if schema_deleted > 0 {
                report.schemas_touched += 1;
                report.history_rows_deleted += schema_deleted;
                report.history_bytes_freed_approx += schema_bytes;
                report.per_schema.push(SchemaHistoryClearStat {
                    schema_name: schema_name.clone(),
                    history_rows_deleted: schema_deleted,
                    history_bytes_freed_approx: schema_bytes,
                });
            }
        }

        if !dry_run && report.history_rows_deleted > 0 {
            let _ = self.flush().await;
        }
        report
            .per_schema
            .sort_by_key(|b| std::cmp::Reverse(b.history_rows_deleted));
        Ok(report)
    }

    /// Delete all `schemaidx:` keys (atom UUID markers + sentinel), forcing the
    /// next schema listing to rebuild the secondary index from canonical atoms.
    pub async fn purge_schemaidx(
        &self,
        storage_prefix: Option<&str>,
    ) -> Result<SchemaIdxPurgeReport, SchemaError> {
        let (keys_deleted, bytes_freed_approx) =
            self.purge_schema_secondary_index(storage_prefix).await?;
        Ok(SchemaIdxPurgeReport {
            keys_deleted,
            bytes_freed_approx,
        })
    }

    /// Measure (and optionally delete) legacy `ref:` whole-molecule blobs in
    /// `legacy_blob_refs`. Dry-run by default; pass `dry_run: false` to delete
    /// only keys already rehydrated as per-key tips/headers. Blocked keys
    /// (no `mh:`/`mk:`) are never force-deleted.
    pub async fn purge_ref_blobs(
        &self,
        dry_run: bool,
        storage_prefix: Option<&str>,
    ) -> Result<LegacyRefBlobPurgeReport, SchemaError> {
        self.purge_legacy_ref_blobs(dry_run, storage_prefix).await
    }

    /// Rows fetched per storage page when a GC pass streams a whole plane.
    ///
    /// Bounds one pass's transient scan allocation by the page, not by the
    /// plane. 2048 matches [`Self::TIP_SCAN_PAGE`]; there is nothing special
    /// about the number beyond "large enough that per-call overhead is noise,
    /// small enough that a page is not itself the allocation problem".
    pub(crate) const GC_PLANE_SCAN_PAGE: usize = 2048;

    /// Stream every row under `prefix` in physical handle order, one bounded
    /// page at a time, handing each `(key, value)` to `on_row`.
    ///
    /// **Why this exists.** `scan_prefix` materializes an entire plane —
    /// every key AND every value — into one `Vec` before the caller decides a
    /// single row. `gc-atoms` did that five times over in one request: `mk:`
    /// twice (once to plan the tip-version prune, once to collect referenced
    /// uuids), then `tv:`, `history:`, `conflict:` and finally `atom:`. On the
    /// primary this owner measures, `mk:` alone is 3.82 GiB and `atom:` is
    /// 3.68 GiB, so a single `gc-atoms --execute` asked for something on the
    /// order of 11 GiB of transient allocation against a 16 GiB memory guard —
    /// to answer a question whose entire output is a set of uuid strings.
    ///
    /// That is the shape behind
    /// `papercut-lastdb-gc-atoms-is-unbounded-and-has-never-committed-a-delete`:
    /// the verb has never committed a delete on a real home, and its dry run
    /// takes ~90 s while its execute does not finish inside the 600 s control
    /// socket deadline. Paging makes the footprint a function of the answer
    /// rather than of the store.
    ///
    /// The caller keeps whatever it accumulates; this only bounds the raw rows
    /// held at once. `on_row` is synchronous by design — a scan that needs to
    /// await per row (the tip-version chain walk, the atom delete drain) pages
    /// inline instead, so the borrow of `self` stays explicit.
    pub(super) async fn for_each_row_under_prefix<F>(
        &self,
        prefix: &str,
        what: &str,
        dry_run: bool,
        mut on_row: F,
    ) -> Result<(), SchemaError>
    where
        F: FnMut(&[u8], &[u8]),
    {
        let (start, end) = crate::kind_partition::colon_plane_bounds(prefix);
        let mut cursor: Option<PhysicalScanCursor> = None;
        let mut progress = GcAtomsProgress::start(what, dry_run);
        // GC needs a complete set, not global key order. A logical page must
        // merge every physical group again before it can return its next 2048
        // rows. The same physical cursor as the prune phase avoids that work.
        // Empty/short pages are not terminal: a handle can contain no keys
        // under this prefix while later handles still contain live roots.
        // Kind-as-partition writes (`tv\0`, `history\0`, `conflict\0`) sort
        // before the colon prefix, so the walk starts at the NUL form.
        loop {
            let page = self
                .raw()
                .inner()
                .scan_range_physical_paged(
                    start.as_bytes(),
                    end.as_bytes(),
                    cursor.as_ref(),
                    Self::GC_PLANE_SCAN_PAGE,
                    1,
                )
                .await
                .map_err(|e| SchemaError::InvalidData(format!("{what}: {e}")))?;
            if page.next_cursor.is_some() && page.next_cursor == cursor {
                return Err(SchemaError::InvalidData(format!(
                    "{what}: physical cursor did not advance"
                )));
            }
            for (k, v) in page.rows {
                progress.walked(1);
                on_row(&k, &v);
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                progress.finish();
                return Ok(());
            }
        }
    }
}
