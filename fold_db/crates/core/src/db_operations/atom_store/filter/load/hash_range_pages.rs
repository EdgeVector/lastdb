use super::*;

impl AtomStore {
    /// `Page` / `PageAfter` / `SampleN` when the `mhr:` page index is off.
    /// Returns `Ok(None)` when the filter is not one of those shapes.
    pub(super) async fn load_hash_range_page_without_index(
        &self,
        molecule_uuid: &str,
        header: &MoleculeHeader,
        storage_prefix: Option<&str>,
        filter: &crate::schema::types::field::HashRangeFilter,
        fill: PageFill,
    ) -> Result<Option<MoleculeData>, SchemaError> {
        use crate::schema::types::field::HashRangeFilter as F;

        // SampleN shares this arm with Page. The order vector below is
        // empty on purpose: sample() reads the atom map, and a window
        // molecule with an empty log must still return the window.
        let page_window = match filter {
            F::Page { offset, limit } => Some((*offset, *limit)),
            F::SampleN(n) => Some((
                0usize,
                (*n).min(crate::schema::types::field::SAMPLE_PEEK_CAP),
            )),
            _ => None,
        };
        if let Some((offset, limit)) = page_window {
            // Match the mhr path: load the prefix [0, offset+limit) so a
            // subsequent apply_hash_range_filter(Page{offset,limit}) sees
            // the same window as a full-molecule Page.
            //
            // The window is measured in whatever rows `fill` counts. Bounding
            // it on STORED keys and letting the tombstone gate run afterwards
            // is what made a page of 100 arrive as 33 on a set that is one
            // third live — the same defect the one-dimensional path was fixed
            // for (`scan_per_key_window_filled`), which this branch kept
            // because it discarded `fill`.
            let want = offset.saturating_add(limit);
            let page = self
                .fill_hash_range_page_expanding(molecule_uuid, storage_prefix, want, None, fill)
                .await?;
            return Ok(Some(MoleculeHashRange::from_per_key_records(
                molecule_uuid.to_string(),
                header.version,
                header.updated_at,
                page,
            )));
        }
        if let F::PageAfter { after, limit } = filter {
            let after_r = after.range.as_deref().unwrap_or("");
            let after_h = after.hash.as_deref().unwrap_or("");
            // LiveRows refill: same expand-if-short pattern as Page.
            let page = self
                .fill_hash_range_page_expanding(
                    molecule_uuid,
                    storage_prefix,
                    *limit,
                    Some((after_r, after_h)),
                    fill,
                )
                .await?;
            return Ok(Some(MoleculeHashRange::from_per_key_records(
                molecule_uuid.to_string(),
                header.version,
                header.updated_at,
                page,
            )));
        }
        Ok(None)
    }

    /// Fill a `want`-row page from the `mk:` key window, starting with a
    /// bounded key list and expanding to the full key set only if short.
    ///
    /// LiveRows may need more stored keys than `want` (tombstone refill).
    /// Start with fill.chunk_for(want) and expand to the full key set only if
    /// the first window under-fills — common all-live pages stay O(want) on
    /// decoded pairs.
    pub(super) async fn fill_hash_range_page_expanding(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        want: usize,
        after: Option<(&str, &str)>,
        fill: PageFill,
    ) -> Result<Vec<(String, String, AtomEntry, Option<KeyMetadata>)>, SchemaError> {
        let key_cap = fill.chunk_for(want).max(want);
        let sorted_keys = self
            .list_hash_range_keys_page_window(molecule_uuid, storage_prefix, Some(key_cap), after)
            .await?;
        let mut page = self
            .fill_hash_range_window(molecule_uuid, storage_prefix, sorted_keys, want, fill)
            .await?;
        if page.len() < want {
            // Sparse live fraction or deep offset: take every key in
            // page order and refill. Rare on product key-restricted
            // paths; required for correctness of unfiltered Page.
            let sorted_keys = self
                .list_hash_range_keys_page_window(molecule_uuid, storage_prefix, None, after)
                .await?;
            // Skip re-scan when the first pass already saw every key.
            if sorted_keys.len() > key_cap {
                page = self
                    .fill_hash_range_window(molecule_uuid, storage_prefix, sorted_keys, want, fill)
                    .await?;
            }
        }
        Ok(page)
    }

    /// `Page` through the range-major `mhr:` index, rebuilding or repairing it
    /// when it is missing or stale.
    pub(super) async fn load_hash_range_page_via_index(
        &self,
        molecule_uuid: &str,
        header: &MoleculeHeader,
        storage_prefix: Option<&str>,
        want: usize,
        fill: PageFill,
    ) -> Result<Option<MoleculeData>, SchemaError> {
        if want == 0 {
            return Ok(Some(MoleculeHashRange::from_per_key_records(
                molecule_uuid.to_string(),
                header.version,
                header.updated_at,
                Vec::new(),
            )));
        }

        if !self
            .hash_range_page_index_complete(molecule_uuid, storage_prefix)
            .await?
        {
            self.rebuild_hash_range_page_index(molecule_uuid, storage_prefix, header)
                .await?;
        }

        let (mut records, stale) = self
            .load_hash_range_page_window_filled(molecule_uuid, storage_prefix, None, want, fill)
            .await?;
        if stale {
            self.repair_hash_range_page_index(molecule_uuid, storage_prefix, header)
                .await?;
            records = self
                .load_hash_range_page_window_filled(molecule_uuid, storage_prefix, None, want, fill)
                .await?
                .0;
        }
        Ok(Some(MoleculeHashRange::from_per_key_records(
            molecule_uuid.to_string(),
            header.version,
            header.updated_at,
            records,
        )))
    }

    /// `PageAfter` through the range-major `mhr:` index, rebuilding or
    /// repairing it when it is missing or stale.
    pub(super) async fn load_hash_range_page_after_via_index(
        &self,
        molecule_uuid: &str,
        header: &MoleculeHeader,
        storage_prefix: Option<&str>,
        after: &crate::schema::types::key_value::KeyValue,
        limit: usize,
        fill: PageFill,
    ) -> Result<Option<MoleculeData>, SchemaError> {
        if limit == 0 {
            return Ok(Some(MoleculeHashRange::from_per_key_records(
                molecule_uuid.to_string(),
                header.version,
                header.updated_at,
                Vec::new(),
            )));
        }

        if !self
            .hash_range_page_index_complete(molecule_uuid, storage_prefix)
            .await?
        {
            self.rebuild_hash_range_page_index(molecule_uuid, storage_prefix, header)
                .await?;
        }

        // The refill loop resumes from storage-form index keys, so map the
        // caller's API cursor once here rather than re-blinding a blinded
        // segment on every continuation.
        let cursor = Some((
            self.storage_hash(molecule_uuid, after.hash.as_deref().unwrap_or(""))?,
            self.storage_range(molecule_uuid, after.range.as_deref().unwrap_or(""))?,
        ));

        let (mut records, stale) = self
            .load_hash_range_page_window_filled(
                molecule_uuid,
                storage_prefix,
                cursor.clone(),
                limit,
                fill,
            )
            .await?;
        if stale {
            self.repair_hash_range_page_index(molecule_uuid, storage_prefix, header)
                .await?;
            records = self
                .load_hash_range_page_window_filled(
                    molecule_uuid,
                    storage_prefix,
                    cursor,
                    limit,
                    fill,
                )
                .await?
                .0;
        }
        Ok(Some(MoleculeHashRange::from_per_key_records(
            molecule_uuid.to_string(),
            header.version,
            header.updated_at,
            records,
        )))
    }

    /// `HashRangeKeys` multi-get: one grouped read for all API pairs.
    pub(super) async fn load_hash_range_keys_batch(
        &self,
        molecule_uuid: &str,
        header: &MoleculeHeader,
        storage_prefix: Option<&str>,
        keys: &[(String, String)],
    ) -> Result<Option<MoleculeData>, SchemaError> {
        let api_keys: Vec<_> = keys
            .iter()
            .map(|(hash, range)| {
                crate::schema::types::key_value::KeyValue::new(
                    Some(hash.clone()),
                    Some(range.clone()),
                )
            })
            .collect();
        let hits = self
            .load_query_tip_records(storage_prefix, molecule_uuid, &api_keys)
            .await?;
        let mut records = Vec::with_capacity(keys.len());
        for ((hash, range), hit) in keys.iter().zip(hits) {
            if let Some((_, rec)) = hit {
                // Option I: echo the API pair the caller supplied, so
                // `apply_hash_range_filter` — pure string comparison,
                // no codec — can match it.
                records.push((hash.clone(), range.clone(), rec.entry, rec.meta));
            }
        }
        Ok(Some(MoleculeHashRange::from_per_key_records(
            molecule_uuid.to_string(),
            header.version,
            header.updated_at,
            records,
        )))
    }
}
