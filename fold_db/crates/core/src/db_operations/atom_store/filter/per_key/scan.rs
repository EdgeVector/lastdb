use super::*;

impl AtomStore {
    pub(crate) fn strip_per_key_storage_prefix(
        storage_prefix: Option<&str>,
        rows: Vec<(String, PerKeyRecord)>,
    ) -> Vec<(String, PerKeyRecord)> {
        let strip = build_storage_key(storage_prefix, "");
        rows.into_iter()
            .map(|(k, r)| (k.strip_prefix(&strip).unwrap_or(&k).to_string(), r))
            .collect()
    }

    /// Scan **at most `limit`** per-key records under the molecule's record
    /// prefix, in ascending key order — the bounded read behind a paginated list
    /// query (`HashRangeFilter::Page`). Returns `(base_key, record)` pairs with
    /// the optional `{storage_prefix}:` prefix stripped.
    ///
    /// Because the per-key records are stored in key order and the page is the
    /// front `offset+limit` of that order, fetching only the first `limit`
    /// records (caller passes `offset + limit`) loads the page without touching
    /// the rest of the field — the read cost is independent of field cardinality.
    /// This is only correct where the storage key order matches the field's page
    /// order (Hash and Range fields, whose record key is the raw key segment);
    /// the HashRange page order differs from its `esc(hash)\0range` storage order
    /// and is handled separately (see `load_filtered_hash_range`).
    pub(crate) async fn scan_per_key_prefix_paged(
        &self,
        base_prefix: &str,
        storage_prefix: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, PerKeyRecord)>, SchemaError> {
        let _ = crate::schema::types::field::FilterUtils::create_prefix_end(base_prefix)?;
        let scan_prefix = build_storage_key(storage_prefix, base_prefix);
        let scanned: Vec<(String, PerKeyRecord)> = self
            .main_store
            .scan_items_with_prefix_paged(&scan_prefix, limit)
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!("paged scan per-key prefix {scan_prefix}: {e}"))
            })?;
        Ok(Self::strip_per_key_storage_prefix(storage_prefix, scanned))
    }

    /// Scan at most `limit` per-key records in `base_start..base_end`, with the
    /// optional org prefix stripped from returned keys. This is the bounded
    /// range primitive behind keyset continuation pages.
    pub(crate) async fn scan_per_key_range_paged(
        &self,
        base_start: &str,
        base_end: &str,
        storage_prefix: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, PerKeyRecord)>, SchemaError> {
        let start = build_storage_key(storage_prefix, base_start);
        let end = build_storage_key(storage_prefix, base_end);
        let scanned: Vec<(String, PerKeyRecord)> = self
            .main_store
            .scan_items_in_range_paged(&start, &end, limit)
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!("paged scan per-key range {start}..{end}: {e}"))
            })?;
        Ok(Self::strip_per_key_storage_prefix(storage_prefix, scanned))
    }
}
