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
        let prefix = self
            .key_codec_for_molecule(molecule_uuid)?
            .api_hash_range_scan_prefix_for_hash(molecule_uuid, hash)
            .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
        let prefixes = [prefix];
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
        // Both supported encodings preserve the caller's byte prefix.
        if prefix.is_empty() {
            return self
                .scan_hash_range_all_ranges(molecule_uuid, storage_prefix, hash)
                .await;
        }
        let codec = self.key_codec_for_molecule(molecule_uuid)?;
        let storage_hash = codec
            .storage_hash(molecule_uuid, hash)
            .map_err(codec_error)?;
        let prefix = codec
            .storage_range(molecule_uuid, prefix)
            .map_err(codec_error)?;
        let prefix =
            molecule_key_codec::hash_range_record_key(molecule_uuid, &storage_hash, &prefix);
        // Use the same prefix selector in memory and on disk. No synthetic
        // upper bound needs to exist in the caller's text coordinates.
        let prefixes = [prefix];
        match self
            .preloaded_per_key_prefixes_for_api(&prefixes, storage_prefix, Some(hash))
            .await?
        {
            Some(rows) => Ok(rows),
            None => self.scan_per_key_prefixes(&prefixes, storage_prefix).await,
        }
    }

    /// Select one half-open range under one hash with its fixed molecule keys.
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
        let codec = self.key_codec_for_molecule(molecule_uuid)?;
        let storage_hash = codec
            .storage_hash(molecule_uuid, hash)
            .map_err(codec_error)?;
        let start = codec
            .storage_range(molecule_uuid, start)
            .map_err(codec_error)?;
        let end = codec
            .storage_range(molecule_uuid, end)
            .map_err(codec_error)?;
        let start = molecule_key_codec::hash_range_record_key(molecule_uuid, &storage_hash, &start);
        let end = molecule_key_codec::hash_range_record_key(molecule_uuid, &storage_hash, &end);
        match self
            .preloaded_per_key_range_for_api(&start, &end, storage_prefix, Some(hash))
            .await?
        {
            Some(rows) => Ok(rows),
            None => self.scan_per_key_range(&start, &end, storage_prefix).await,
        }
    }
}

fn codec_error(error: impl std::fmt::Display) -> SchemaError {
    SchemaError::InvalidData(error.to_string())
}
