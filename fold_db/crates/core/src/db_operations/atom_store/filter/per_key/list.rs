use super::*;

impl AtomStore {
    /// Keys-only page of **live** record identities on one molecule.
    ///
    /// Walks `mk:{M}:` with [`PageFill::LiveRows`] so tombstoned tips do not
    /// occupy the window. Decodes hash/range from the storage key. Does **not**
    /// fetch `atom:` bodies.
    ///
    /// `after` is an exclusive keyset cursor (a previous page's last storage
    /// key). It must sit under this molecule's prefix.
    pub async fn list_live_record_keys(
        &self,
        molecule: &str,
        limit: usize,
        after: Option<&str>,
        storage_prefix: Option<&str>,
    ) -> Result<(Vec<(String, String)>, Option<String>, bool), SchemaError> {
        self.list_live_record_keys_filtered(molecule, limit, after, storage_prefix, None)
            .await
    }

    /// Keys-only page, optionally restricted to one API hash partition.
    ///
    /// `hash_filter` is the **API** hash (not the storage token). The codec
    /// blinds it when the home uses BlindV1, then scans
    /// `mk:{M}:{esc(storage_hash)}\0` — O(log M) under that hash, not a
    /// schema-wide walk. No atom bodies except BlindV1's per-key API-hash
    /// recovery, which `list_live_record_keys` already pays.
    pub async fn list_live_record_keys_filtered(
        &self,
        molecule: &str,
        limit: usize,
        after: Option<&str>,
        storage_prefix: Option<&str>,
        hash_filter: Option<&str>,
    ) -> Result<(Vec<(String, String)>, Option<String>, bool), SchemaError> {
        if limit == 0 {
            return Ok((Vec::new(), None, false));
        }
        let prefix = match hash_filter {
            Some(api_hash) => self
                .key_codec()
                .api_hash_range_scan_prefix_for_hash(molecule, api_hash)
                .map_err(|e| SchemaError::InvalidData(e.to_string()))?,
            None => molecule_key_codec::molecule_record_prefix(molecule),
        };
        if let Some(cursor) = after {
            let ok = cursor.starts_with(&prefix) || cursor.contains(&format!(":{prefix}"));
            if !ok {
                return Err(SchemaError::InvalidCursor(
                    "cursor is not in this schema's key molecule".into(),
                ));
            }
        }

        let want = limit.saturating_add(1);
        let rows = self
            .scan_per_key_window_filled(&prefix, after, storage_prefix, want, PageFill::LiveRows)
            .await?;
        let has_more = rows.len() > limit;
        let kept = if has_more { &rows[..limit] } else { &rows[..] };
        let next_cursor = has_more
            .then(|| kept.last().map(|(key, _)| key.clone()))
            .flatten();

        let codec = self.key_codec();
        let blind = codec.encoding() == crate::atom::HashKeyEncoding::BlindV1;
        let mut keys = Vec::with_capacity(kept.len());
        for (key, rec) in kept {
            let decoded = molecule_key_codec::decode_hash_range(key, molecule).or_else(|| {
                let bare = key
                    .rsplit_once(":mk:")
                    .map_or_else(|| key.clone(), |(_, rest)| format!("mk:{rest}"));
                molecule_key_codec::decode_hash_range(&bare, molecule)
            });
            let (storage_hash, storage_range) =
                decoded.unwrap_or_else(|| (key.clone(), String::new()));
            let range = crate::crypto::E2eKeys::ope_decode_range_plaintext(&storage_range)
                .unwrap_or_else(|| storage_range.clone());
            let hash = if blind {
                match self
                    .get_atom_by_uuid(rec.entry.atom_uuid.as_str(), storage_prefix)
                    .await?
                {
                    Some(atom) => {
                        let api = atom
                            .content()
                            .as_str()
                            .map(str::to_string)
                            .unwrap_or_default();
                        if !api.is_empty()
                            && codec
                                .storage_hash(molecule, &api)
                                .is_ok_and(|blinded| blinded == storage_hash)
                        {
                            api
                        } else {
                            storage_hash
                        }
                    }
                    None => storage_hash,
                }
            } else {
                storage_hash
            };
            keys.push((hash, range));
        }
        Ok((keys, next_cursor, has_more))
    }
}
