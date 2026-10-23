use super::*;

impl AtomStore {
    /// Check one exact Delete barrier for each returned molecule row. A range
    /// read never walks the barriers; it checks only the keys it already found.
    pub(crate) async fn filter_delete_barriers_for_records(
        &self,
        rows: Vec<(String, PerKeyRecord)>,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<(String, PerKeyRecord)>, SchemaError> {
        if rows.is_empty() {
            return Ok(rows);
        }
        let molecules: std::collections::BTreeSet<_> = rows
            .iter()
            .filter_map(|(base, _)| molecule_key_codec::molecule_uuid_from_storage_key(base))
            .collect();
        let mut captured = std::collections::BTreeMap::new();
        for molecule in molecules {
            captured.extend(self.captured_tip_changes(storage_prefix, molecule)?);
        }
        let final_keys: Vec<String> = rows
            .iter()
            .filter(|(base, _)| !captured.contains_key(base))
            .map(|(base, _)| build_storage_key(storage_prefix, base))
            .collect();
        let barriers = self.winning_delete_barriers(&final_keys).await?;
        Ok(rows
            .into_iter()
            .filter_map(|(base, record)| {
                if let Some(final_record) = captured.get(&base) {
                    return final_record.clone().map(|record| (base, record));
                }
                let key = build_storage_key(storage_prefix, &base);
                barriers
                    .get(&key)
                    .is_none_or(|barrier| !barrier.blocks_tip(&record.entry))
                    .then_some((base, record))
            })
            .collect())
    }

    /// Fetch a single per-key record by its already-encoded base key (e.g.
    /// [`molecule_key_codec::hash_range_record_key`]) — the O(1) point lookup.
    /// `None` when the key is absent.
    pub(crate) async fn get_per_key(
        &self,
        base_key: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Option<(String, PerKeyRecord)>, SchemaError> {
        let records = self
            .load_exact_per_key_records(storage_prefix, &[base_key.to_owned()])
            .await?;
        let rows = records
            .into_iter()
            .flatten()
            .map(|record| (base_key.to_owned(), record))
            .collect();
        Ok(self
            .filter_delete_barriers_for_records(rows, storage_prefix)
            .await?
            .into_iter()
            .next())
    }

    /// Resolve exact keys in request order, retaining duplicate positions.
    /// Generation overlays and Delete barriers use the same captured read.
    pub(crate) async fn load_visible_per_key_records(
        &self,
        storage_prefix: Option<&str>,
        keys: &[String],
    ) -> Result<Vec<Option<PerKeyRecord>>, SchemaError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut seen = std::collections::HashSet::new();
        let unique: Vec<_> = keys
            .iter()
            .filter(|key| seen.insert(key.as_str()))
            .cloned()
            .collect();
        let loaded = self
            .load_exact_per_key_records(storage_prefix, &unique)
            .await?;
        let present = unique
            .into_iter()
            .zip(loaded)
            .filter_map(|(key, record)| record.map(|record| (key, record)))
            .collect();
        let visible = self
            .filter_delete_barriers_for_records(present, storage_prefix)
            .await?;
        let visible: std::collections::HashMap<_, _> = visible.into_iter().collect();
        Ok(keys.iter().map(|key| visible.get(key).cloned()).collect())
    }
}
