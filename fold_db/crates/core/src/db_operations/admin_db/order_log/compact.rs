use super::*;

impl AtomStore {
    /// Plan or reclaim order-log residue for zero-live **and** partially-live
    /// (bloated) molecules on one bounded bloat-audit page.
    ///
    /// Dry-run writes nothing. Execute deletes the order log of a zero-live
    /// molecule, a bloated molecule, and a clean molecule. It does not write
    /// a new log. `retention_seconds` does not keep rows.
    pub async fn compact_order_log_zero_live(
        &self,
        dry_run: bool,
        max_keys: Option<usize>,
        after_key: Option<&str>,
        storage_prefix: Option<&str>,
    ) -> Result<OrderLogZeroLiveCompactionReport, SchemaError> {
        self.compact_order_log_zero_live_with_retention(
            dry_run,
            max_keys,
            after_key,
            storage_prefix,
            0,
        )
        .await
    }

    /// `retention_seconds` is echoed on the report and does not select rows.
    pub async fn compact_order_log_zero_live_with_retention(
        &self,
        dry_run: bool,
        max_keys: Option<usize>,
        after_key: Option<&str>,
        storage_prefix: Option<&str>,
        retention_seconds: u64,
    ) -> Result<OrderLogZeroLiveCompactionReport, SchemaError> {
        let audit = self
            .audit_order_log_bloat(max_keys, after_key, storage_prefix)
            .await?;
        let zero_live = audit.zero_live_molecules.clone();
        let bloated = audit.bloated_molecules.clone();
        let bloated_ids: BTreeSet<String> =
            bloated.iter().map(|row| row.molecule.clone()).collect();
        let mut clean_ids = Vec::new();
        let mut clean_entries = 0u64;
        for molecule in &audit.live_molecule_ids {
            if bloated_ids.contains(molecule) {
                continue;
            }
            let (entries, _bytes) = self
                .measure_order_log_storage(molecule, storage_prefix)
                .await?;
            clean_ids.push(molecule.clone());
            clean_entries += entries;
        }
        let mut report = OrderLogZeroLiveCompactionReport {
            dry_run,
            retention_seconds,
            molecules_planned: zero_live.len() as u64,
            entries_planned: zero_live.iter().map(|row| row.order_log_entries).sum(),
            bytes_planned: zero_live
                .iter()
                .map(|row| row.order_log_bytes + row.order_count_bytes)
                .sum(),
            molecules_filter_planned: bloated.len() as u64 + clean_ids.len() as u64,
            entries_retained_planned: 0,
            entries_stale_planned: bloated.iter().map(|row| row.order_log_entries).sum::<u64>()
                + clean_entries,
            fanout: if dry_run {
                1
            } else {
                order_log_compact_fanout() as u64
            },
            more_remaining: audit.more_remaining,
            next_after_key: audit.next_after_key.clone(),
            audit,
            ..Default::default()
        };
        report.tips_bytes_planned = report.bytes_planned;

        if dry_run {
            return Ok(report);
        }

        let prefix = storage_prefix.map(str::to_owned);
        self.fanout_compact_molecules(
            zero_live.iter().map(|row| row.molecule.clone()).collect(),
            prefix.as_deref(),
            CompactKind::ZeroLive,
            &mut report,
        )
        .await?;
        let mut filter_ids: Vec<String> = bloated.iter().map(|row| row.molecule.clone()).collect();
        filter_ids.extend(clean_ids);
        self.fanout_compact_molecules(
            filter_ids,
            prefix.as_deref(),
            CompactKind::LiveFilter,
            &mut report,
        )
        .await?;
        report.tips_bytes_deleted = report
            .bytes_deleted
            .saturating_add(report.bytes_filter_deleted);

        Ok(report)
    }

    /// Auto reclaim does not delete. Explicit `compact-order-log --execute` does.
    pub fn compact_order_logs_for_molecules(
        &self,
        _molecules: &[String],
        _storage_prefix: Option<&str>,
    ) -> Result<OrderLogZeroLiveCompactionReport, SchemaError> {
        let _ = self;
        Ok(OrderLogZeroLiveCompactionReport::default())
    }

    pub(in super::super) async fn fanout_compact_molecules(
        &self,
        molecules: Vec<String>,
        storage_prefix: Option<&str>,
        kind: CompactKind,
        report: &mut OrderLogZeroLiveCompactionReport,
    ) -> Result<(), SchemaError> {
        if molecules.is_empty() {
            return Ok(());
        }
        let fanout = order_log_compact_fanout().max(1);
        let prefix = storage_prefix.map(str::to_owned);
        let store = self.clone();
        let mut stream = stream::iter(molecules.into_iter().map(move |molecule| {
            let store = store.clone();
            let prefix = prefix.clone();
            async move {
                let mut local = OrderLogZeroLiveCompactionReport::default();
                match kind {
                    CompactKind::ZeroLive => {
                        store
                            .compact_order_log_zero_live_one(
                                &molecule,
                                prefix.as_deref(),
                                &mut local,
                            )
                            .await?;
                    }
                    CompactKind::LiveFilter => {
                        store
                            .compact_order_log_live_filter_one(
                                &molecule,
                                prefix.as_deref(),
                                &mut local,
                            )
                            .await?;
                    }
                }
                Ok::<_, SchemaError>(local)
            }
        }))
        .buffer_unordered(fanout);

        while let Some(local) = stream.next().await {
            let local = local?;
            merge_compact_delta(report, &local);
        }
        Ok(())
    }

    pub(in super::super) async fn list_order_log_rows(
        &self,
        molecule: &str,
        storage_prefix: Option<&str>,
    ) -> Result<OrderLogRowSet, SchemaError> {
        let dense_prefix = build_storage_key(
            storage_prefix,
            &molecule_key_codec::order_log_prefix(molecule),
        );
        // Legacy rows are `mord:{M}:{seq}`. The anchored prefix does not see them.
        let legacy_prefix = build_storage_key(
            storage_prefix,
            &kind_partition::flat("mord", &format!("{molecule}:")),
        );
        let sparse_prefix = build_storage_key(
            storage_prefix,
            &molecule_key_codec::sparse_order_log_prefix(molecule),
        );
        let mut dense = self
            .scan_prefix_key_bytes(&dense_prefix, &format!("dense order log {molecule}"))
            .await?;
        dense.extend(
            self.scan_prefix_key_bytes(&legacy_prefix, &format!("legacy order log {molecule}"))
                .await?,
        );
        Ok(OrderLogRowSet {
            dense,
            sparse: self
                .scan_prefix_key_bytes(&sparse_prefix, &format!("sparse order log {molecule}"))
                .await?,
        })
    }

    pub(in super::super) async fn scan_prefix_key_bytes(
        &self,
        prefix: &str,
        what: &str,
    ) -> Result<Vec<(String, u64)>, SchemaError> {
        let rows = self
            .raw()
            .inner()
            .scan_prefix(prefix.as_bytes())
            .await
            .map_err(|error| SchemaError::InvalidData(format!("{what}: {error}")))?;
        Ok(rows
            .into_iter()
            .map(|(key, value)| {
                (
                    String::from_utf8_lossy(&key).into_owned(),
                    key.len() as u64 + value.len() as u64,
                )
            })
            .collect())
    }

    pub(in super::super) async fn compact_order_log_zero_live_one(
        &self,
        molecule: &str,
        storage_prefix: Option<&str>,
        report: &mut OrderLogZeroLiveCompactionReport,
    ) -> Result<(), SchemaError> {
        let _commit_guard = self.lock_molecule_commit(molecule, storage_prefix).await;
        if self
            .molecule_has_any_live_key(molecule, storage_prefix)
            .await?
        {
            report.molecules_skipped_live += 1;
            return Ok(());
        }

        let rows = self.list_order_log_rows(molecule, storage_prefix).await?;
        let entries = rows.keys().len() as u64;
        let order_bytes = rows.bytes();
        let mut order_keys = rows.delete_keys();
        let order_count_key = build_storage_key(
            storage_prefix,
            &molecule_key_codec::order_count_key(molecule),
        );
        let order_count = self
            .raw()
            .get_item::<u64>(&order_count_key)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "read order count {molecule} for zero-live compaction: {error}"
                ))
            })?;
        if order_keys.is_empty() && order_count.is_none() {
            return Ok(());
        }
        let mut bytes_deleted = order_bytes;
        if let Some(order_count) = order_count {
            let count_bytes = order_count_key.len() as u64
                + serde_json::to_vec(&order_count)
                    .map_err(|error| {
                        SchemaError::InvalidData(format!(
                            "measure order count {molecule} for zero-live compaction: {error}"
                        ))
                    })?
                    .len() as u64;
            bytes_deleted += count_bytes;
            if let Some(twin) = kind_partition::form_twin(&order_count_key) {
                order_keys.push(twin);
            }
            order_keys.push(order_count_key);
        }
        if !order_keys.is_empty() {
            self.raw()
                .batch_delete_keys(order_keys)
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!(
                        "delete zero-live order log {molecule}: {error}"
                    ))
                })?;
        }
        report.molecules_compacted += 1;
        report.entries_deleted += entries;
        report.bytes_deleted += bytes_deleted;
        Ok(())
    }

    /// Delete one molecule's order log under the commit guard.
    ///
    /// Explicit execute removes every `mord:` row and the `moc:` key. It does
    /// not write a replacement log. A molecule with nothing left is unchanged.
    pub(in super::super) async fn compact_order_log_live_filter_one(
        &self,
        molecule: &str,
        storage_prefix: Option<&str>,
        report: &mut OrderLogZeroLiveCompactionReport,
    ) -> Result<(), SchemaError> {
        let _commit_guard = self.lock_molecule_commit(molecule, storage_prefix).await;

        let rows = self.list_order_log_rows(molecule, storage_prefix).await?;
        let entries = rows.keys().len() as u64;
        let order_bytes = rows.bytes();
        let mut order_keys = rows.delete_keys();
        let order_count_key = build_storage_key(
            storage_prefix,
            &molecule_key_codec::order_count_key(molecule),
        );
        let order_count = self
            .raw()
            .get_item::<u64>(&order_count_key)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "read order count {molecule} for order-log compaction: {error}"
                ))
            })?;
        if order_keys.is_empty() && order_count.is_none() {
            return Ok(());
        }
        let mut bytes_deleted = order_bytes;
        if let Some(order_count) = order_count {
            let count_bytes = order_count_key.len() as u64
                + serde_json::to_vec(&order_count)
                    .map_err(|error| {
                        SchemaError::InvalidData(format!(
                            "measure order count {molecule} for order-log compaction: {error}"
                        ))
                    })?
                    .len() as u64;
            bytes_deleted += count_bytes;
            if let Some(twin) = kind_partition::form_twin(&order_count_key) {
                order_keys.push(twin);
            }
            order_keys.push(order_count_key);
        }
        if !order_keys.is_empty() {
            self.raw()
                .batch_delete_keys(order_keys)
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!("delete order log {molecule}: {error}"))
                })?;
        }
        report.molecules_filtered += 1;
        report.entries_stale_removed += entries;
        report.bytes_filter_deleted += bytes_deleted;
        Ok(())
    }
}
