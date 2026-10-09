use super::*;

impl AtomStore {
    /// Exact `(entries, key+value bytes)` for one molecule's dense + sparse
    /// order-log rows. Point-scoped prefixes — not a full-store scan.
    pub(in super::super) async fn measure_order_log_storage(
        &self,
        molecule: &str,
        storage_prefix: Option<&str>,
    ) -> Result<(u64, u64), SchemaError> {
        let dense = build_storage_key(
            storage_prefix,
            &molecule_key_codec::order_log_prefix(molecule),
        );
        let legacy = build_storage_key(
            storage_prefix,
            &kind_partition::flat("mord", &format!("{molecule}:")),
        );
        let sparse = build_storage_key(
            storage_prefix,
            &molecule_key_codec::sparse_order_log_prefix(molecule),
        );
        let (dense_keys, dense_bytes) = self
            .inventory_accumulate_prefix(&dense, &format!("order-log bloat dense {molecule}"))
            .await?;
        let (legacy_keys, legacy_bytes) = self
            .inventory_accumulate_prefix(&legacy, &format!("order-log bloat legacy {molecule}"))
            .await?;
        let (sparse_keys, sparse_bytes) = self
            .inventory_accumulate_prefix(&sparse, &format!("order-log bloat sparse {molecule}"))
            .await?;
        Ok((
            dense_keys + legacy_keys + sparse_keys,
            dense_bytes + legacy_bytes + sparse_bytes,
        ))
    }

    pub(in super::super) async fn decide_order_log_bloat_molecule(
        &self,
        report: &mut OrderLogBloatAudit,
        order_counts: &HashMap<String, (u64, u64)>,
        molecule: &str,
        live_keys: u64,
        live_unique_keys: u64,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        report.molecules_decided += 1;
        let (order_count, moc_bytes) = order_counts.get(molecule).copied().unwrap_or_default();
        let unique = live_unique_keys.min(live_keys);
        let (order_log_entries, order_log_bytes) = self
            .measure_order_log_storage(molecule, storage_prefix)
            .await?;
        if order_count == 0 && order_log_entries == 0 {
            report.molecules_without_order_count += 1;
            return Ok(());
        }
        if unique > 0 {
            report.live_molecule_ids.push(molecule.to_string());
        }
        if order_log_entries <= unique {
            report.molecules_ok += 1;
            return Ok(());
        }
        let stale = order_log_entries - unique;
        let row = OrderLogBloatRow {
            molecule: molecule.to_string(),
            schema: None,
            order_count,
            live_keys,
            live_unique_keys: unique,
            stale_entries: stale,
            order_log_entries,
            order_log_bytes,
            order_count_bytes: moc_bytes,
            zero_live: false,
        };
        report.molecules_bloated += 1;
        report.stale_entries += stale;
        report.order_log_bytes += order_log_bytes;
        report.order_count_bytes += moc_bytes;
        report.bloated_molecules.push(row);
        Ok(())
    }

    /// Order findings worst-first: bloated by stale-entry margin, zero-live by
    /// reclaimable order-log bytes.
    pub(in super::super) fn sort_order_log_bloat(report: &mut OrderLogBloatAudit) {
        report
            .bloated_molecules
            .sort_by_key(|row| std::cmp::Reverse(row.stale_entries));
        report
            .zero_live_molecules
            .sort_by_key(|row| std::cmp::Reverse(row.order_log_bytes));
    }
}
