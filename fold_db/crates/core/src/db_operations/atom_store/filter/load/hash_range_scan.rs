use super::*;

impl AtomStore {
    /// Key-narrowed scan for the single-hash filter shapes. Returns `Ok(None)`
    /// for shapes that fall through to the full path.
    pub(super) async fn scan_hash_range_narrowed(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        filter: &crate::schema::types::field::HashRangeFilter,
    ) -> Result<Option<Vec<(String, PerKeyRecord)>>, SchemaError> {
        use crate::schema::types::field::HashRangeFilter as F;

        let scanned = match filter {
            F::HashRangeKey { hash, range } => {
                let key = crate::schema::types::key_value::KeyValue::new(
                    Some(hash.clone()),
                    Some(range.clone()),
                );
                let hit = self
                    .load_query_tip_records(storage_prefix, molecule_uuid, &[key])
                    .await?
                    .pop()
                    .flatten();
                hit.into_iter().collect()
            }
            F::HashKey(hash) => {
                self.scan_hash_range_all_ranges(molecule_uuid, storage_prefix, hash)
                    .await?
            }
            F::HashRangePrefix { hash, prefix } => {
                self.scan_hash_range_prefix(molecule_uuid, storage_prefix, hash, prefix)
                    .await?
            }
            F::HashRangeRange { hash, start, end } => {
                self.scan_hash_range_between(molecule_uuid, storage_prefix, hash, start, end)
                    .await?
            }
            _ => return Ok(None),
        };
        Ok(Some(scanned))
    }

    /// All ranges under one hash for the molecule's readable encryption keys.
    pub(super) async fn scan_hash_range_all_ranges(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        hash: &str,
    ) -> Result<Vec<(String, PerKeyRecord)>, SchemaError> {
        let prefixes = self
            .key_codec_for_molecule(molecule_uuid)
            .api_hash_range_scan_prefixes_for_read(molecule_uuid, hash)
            .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
        if let Some(rows) = self
            .preloaded_per_key_prefixes_for_api(&prefixes, storage_prefix, Some(hash))
            .await?
        {
            return Ok(rows);
        }
        self.scan_per_key_prefixes(&prefixes, storage_prefix).await
    }

    /// `HashRangePrefix`: storage-form hash × range-prefix scan.
    pub(super) async fn scan_hash_range_prefix(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        hash: &str,
        prefix: &str,
    ) -> Result<Vec<(String, PerKeyRecord)>, SchemaError> {
        use crate::schema::types::field::FilterUtils;

        // Per-byte OPE preserves byte-prefixes inside one spelling; do not mix
        // encodings in one bound pair.
        if prefix.is_empty() {
            return self
                .scan_hash_range_all_ranges(molecule_uuid, storage_prefix, hash)
                .await;
        }
        let codec = self.key_codec_for_molecule(molecule_uuid);
        let mut out = Vec::new();
        let hash_cands = codec
            .storage_hash_read_candidates(molecule_uuid, hash)
            .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
        let range_prefix_cands = codec
            .storage_range_read_candidates(molecule_uuid, prefix)
            .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
        for sh in &hash_cands {
            for sr_prefix in &range_prefix_cands {
                let start = molecule_key_codec::hash_range_record_key(molecule_uuid, sh, sr_prefix);
                let end = molecule_key_codec::hash_range_record_key(
                    molecule_uuid,
                    sh,
                    &FilterUtils::create_prefix_end(sr_prefix),
                );
                let rows = match self
                    .preloaded_per_key_range_for_api(&start, &end, storage_prefix, Some(hash))
                    .await?
                {
                    Some(rows) => rows,
                    None => {
                        self.scan_per_key_range(&start, &end, storage_prefix)
                            .await?
                    }
                };
                out.extend(rows);
            }
        }
        Ok(out)
    }

    /// `HashRangeRange`: storage-form hash × range bounds; the first
    /// non-empty candidate pair wins.
    pub(super) async fn scan_hash_range_between(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        hash: &str,
        start: &str,
        end: &str,
    ) -> Result<Vec<(String, PerKeyRecord)>, SchemaError> {
        if start >= end {
            return Ok(Vec::new());
        }
        let codec = self.key_codec_for_molecule(molecule_uuid);
        let hash_cands = codec
            .storage_hash_read_candidates(molecule_uuid, hash)
            .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
        let start_cands = codec
            .storage_range_read_candidates(molecule_uuid, start)
            .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
        let end_cands = codec
            .storage_range_read_candidates(molecule_uuid, end)
            .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
        let n_range = start_cands.len().min(end_cands.len());
        let mut out = Vec::new();
        for sh in &hash_cands {
            for i in 0..n_range {
                let s =
                    molecule_key_codec::hash_range_record_key(molecule_uuid, sh, &start_cands[i]);
                let e = molecule_key_codec::hash_range_record_key(molecule_uuid, sh, &end_cands[i]);
                out = match self
                    .preloaded_per_key_range_for_api(&s, &e, storage_prefix, Some(hash))
                    .await?
                {
                    Some(rows) => rows,
                    None => self.scan_per_key_range(&s, &e, storage_prefix).await?,
                };
                if !out.is_empty() {
                    return Ok(out);
                }
            }
        }
        Ok(out)
    }
}
