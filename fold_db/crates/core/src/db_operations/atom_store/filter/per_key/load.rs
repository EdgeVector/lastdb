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

    /// One grouped read for many candidate spellings of many caller keys.
    ///
    /// `groups[i]` lists the dual-read spellings of caller key `i`, in the
    /// order [`Self::get_per_key`] would try them. The first spelling that is
    /// present and not hidden by a Delete barrier wins. A later spelling of
    /// that same row is ignored.
    ///
    /// Present spellings are read by one [`Self::load_exact_per_key_records`]
    /// and checked by one [`Self::filter_delete_barriers_for_records`]. Keys
    /// that share a hash group share one unpublished open. The group does
    /// not stay. An empty candidate list reads nothing.
    pub(crate) async fn load_first_present_candidate(
        &self,
        storage_prefix: Option<&str>,
        groups: &[Vec<String>],
    ) -> Result<Vec<Option<PerKeyRecord>>, SchemaError> {
        Ok(self
            .load_first_present_candidate_with_key(storage_prefix, groups)
            .await?
            .into_iter()
            .map(|hit| hit.map(|(_, record)| record))
            .collect())
    }

    /// Keep the winning physical coordinate until a caller records its tip.
    pub(crate) async fn load_first_present_candidate_with_key(
        &self,
        storage_prefix: Option<&str>,
        groups: &[Vec<String>],
    ) -> Result<Vec<Option<(String, PerKeyRecord)>>, SchemaError> {
        let mut seen = std::collections::HashSet::<&str>::new();
        let mut unique = Vec::new();
        for group in groups {
            for key in group {
                if seen.insert(key.as_str()) {
                    unique.push(key.clone());
                }
            }
        }
        if unique.is_empty() {
            return Ok(vec![None; groups.len()]);
        }
        let loaded = self
            .load_exact_per_key_records(storage_prefix, &unique)
            .await?;
        let present: Vec<(String, PerKeyRecord)> = unique
            .into_iter()
            .zip(loaded)
            .filter_map(|(key, record)| record.map(|record| (key, record)))
            .collect();
        let visible = self
            .filter_delete_barriers_for_records(present, storage_prefix)
            .await?;
        let visible_by_key: std::collections::HashMap<String, PerKeyRecord> =
            visible.into_iter().collect();
        let mut out = Vec::with_capacity(groups.len());
        for group in groups {
            let mut hit = None;
            for key in group {
                if let Some(record) = visible_by_key.get(key) {
                    hit = Some((key.clone(), record.clone()));
                    break;
                }
            }
            out.push(hit);
        }
        Ok(out)
    }

    /// Fetch a derived unique-hash marker for `HashRangeFilter::HashKey`.
    ///
    /// Returns `None` when the hash-key lookup index is retired, or for missing
    /// / ambiguous markers, so callers fall back to the authoritative `mk:`
    /// prefix scan.
    pub(crate) async fn get_unique_hash_key_record(
        &self,
        molecule_uuid: &str,
        api_hash: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Option<(String, PerKeyRecord)>, SchemaError> {
        if !super::super::super::helpers::HASH_RANGE_HASH_KEY_LOOKUP_ENABLED {
            // `mhk:` co-write retired — never prefer historical markers.
            let _ = (molecule_uuid, api_hash, storage_prefix);
            return Ok(None);
        }
        // Single storage form (plain or blind — no dual-read).
        let candidates = self
            .key_codec_for_molecule(molecule_uuid)
            .storage_hash_read_candidates(molecule_uuid, api_hash)
            .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
        for storage_hash in candidates {
            let base_key = molecule_key_codec::hash_key_lookup_key(molecule_uuid, &storage_hash);
            let storage_key = build_storage_key(storage_prefix, &base_key);
            let marker: Option<HashKeyLookupRecord> =
                self.main_store.get_item(&storage_key).await.map_err(|e| {
                    SchemaError::InvalidData(format!("read hash-key marker {storage_key}: {e}"))
                })?;
            let Some(marker) = marker else {
                continue;
            };
            match (marker.range, marker.record) {
                (Some(range), Some(record)) => {
                    return Ok(Some((
                        molecule_key_codec::hash_range_record_key(
                            molecule_uuid,
                            &storage_hash,
                            &range,
                        ),
                        record,
                    )));
                }
                _ => return Ok(None),
            }
        }
        Ok(None)
    }
}
