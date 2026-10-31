use super::*;

/// The generation merge for one stored key scan.
enum StoredKeyMerge<'a> {
    Prefix {
        base_prefix: &'a str,
    },
    Range {
        base_start: &'a str,
        base_end: &'a str,
    },
}

impl<S: FilterPorts> Filter<'_, S> {
    /// Read one exact prefix, then apply generation and Delete state.
    pub(crate) async fn scan_per_key_prefix(
        &self,
        base_prefix: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<(String, PerKeyRecord)>, SchemaError> {
        super::super::prefer_preloaded_rows(
            self.store
                .preloaded_per_key_prefix(base_prefix, storage_prefix)
                .await,
            || self.scan_stored_per_key_prefix(base_prefix, storage_prefix),
        )
        .await
    }

    async fn scan_stored_per_key_prefix(
        &self,
        base_prefix: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<(String, PerKeyRecord)>, SchemaError> {
        let scan_prefix = build_storage_key(storage_prefix, base_prefix);
        let scanned: Vec<(String, PerKeyRecord)> = self
            .store
            .rows()
            .scan_items_with_prefix(&scan_prefix)
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!("scan per-key prefix {scan_prefix}: {e}"))
            })?;
        self.finish_stored_key_scan(
            base_prefix,
            storage_prefix,
            scanned,
            StoredKeyMerge::Prefix { base_prefix },
        )
        .await
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
            .store
            .rows()
            .scan_items_with_prefix_paged(&scan_prefix, limit)
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!("paged scan per-key prefix {scan_prefix}: {e}"))
            })?;
        Ok(strip_per_key_storage_prefix(storage_prefix, scanned))
    }

    /// Scan the per-key records whose base key falls in `start..end` (the
    /// encoded base keys, e.g. [`molecule_key_codec::hash_range_record_key`])
    /// — a bounded `O(matches)` ordered range scan. Returns `(base_key, record)`
    /// pairs with the optional `{storage_prefix}:` prefix stripped.
    pub(crate) async fn scan_per_key_range(
        &self,
        base_start: &str,
        base_end: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<(String, PerKeyRecord)>, SchemaError> {
        super::super::prefer_preloaded_rows(
            self.store
                .preloaded_per_key_range(base_start, base_end, storage_prefix)
                .await,
            || self.scan_stored_per_key_range(base_start, base_end, storage_prefix),
        )
        .await
    }

    async fn scan_stored_per_key_range(
        &self,
        base_start: &str,
        base_end: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<(String, PerKeyRecord)>, SchemaError> {
        let start = build_storage_key(storage_prefix, base_start);
        let end = build_storage_key(storage_prefix, base_end);
        let scanned: Vec<(String, PerKeyRecord)> = self
            .store
            .rows()
            .scan_items_in_range(&start, &end)
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!("scan per-key range {start}..{end}: {e}"))
            })?;
        self.finish_stored_key_scan(
            base_start,
            storage_prefix,
            scanned,
            StoredKeyMerge::Range {
                base_start,
                base_end,
            },
        )
        .await
    }

    /// Strip the storage prefix, merge one molecule generation, then order the keys.
    /// A key with no molecule still drops Delete barriers and keeps scan order.
    async fn finish_stored_key_scan(
        &self,
        identity_key: &str,
        storage_prefix: Option<&str>,
        scanned: Vec<(String, PerKeyRecord)>,
        merge: StoredKeyMerge<'_>,
    ) -> Result<Vec<(String, PerKeyRecord)>, SchemaError> {
        let rows = strip_per_key_storage_prefix(storage_prefix, scanned);
        let Some(molecule_uuid) = molecule_key_codec::molecule_uuid_from_storage_key(identity_key)
        else {
            return self
                .filter_delete_barriers_for_records(rows, storage_prefix)
                .await;
        };
        let rows = match merge {
            StoredKeyMerge::Prefix { base_prefix } => {
                self.store
                    .merge_generation_prefix_rows(molecule_uuid, base_prefix, storage_prefix, rows)
                    .await?
            }
            StoredKeyMerge::Range {
                base_start,
                base_end,
            } => {
                self.store
                    .merge_generation_range_rows(
                        molecule_uuid,
                        base_start,
                        base_end,
                        storage_prefix,
                        rows,
                    )
                    .await?
            }
        };
        self.ordered_molecule_rows(rows, storage_prefix).await
    }

    /// Keep the last record for one key, then return the keys in order.
    /// A scan with no molecule does not use this order.
    async fn ordered_molecule_rows(
        &self,
        rows: Vec<(String, PerKeyRecord)>,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<(String, PerKeyRecord)>, SchemaError> {
        let rows = self
            .filter_delete_barriers_for_records(rows, storage_prefix)
            .await?;
        Ok(rows
            .into_iter()
            .collect::<std::collections::BTreeMap<_, _>>()
            .into_iter()
            .collect())
    }

    /// Scan forward under `base_prefix` until `want` rows that `fill` keeps
    /// have been collected, or the prefix is exhausted.
    ///
    /// `after` is an **exclusive** lower bound (a keyset cursor); `None` starts
    /// at the head of the prefix.
    ///
    /// This is the bounded read behind every paginated list. It exists because
    /// [`Self::scan_per_key_prefix_paged`] bounds on STORED rows: a caller that
    /// asks for `limit` rows and then drops the tombstoned ones is left with
    /// `limit × live_fraction` and no way to tell that from a genuinely short
    /// page. Refilling here — rather than at the caller — keeps the escalation
    /// where the key order lives, so each extra scan resumes from the last key
    /// seen instead of re-walking the prefix from the head.
    ///
    /// Cost tracks the stored rows the window spans, which is what any correct
    /// answer must read; a molecule with no tombstones does exactly one scan.
    pub(crate) async fn scan_per_key_window_filled(
        &self,
        base_prefix: &str,
        after: Option<&str>,
        storage_prefix: Option<&str>,
        want: usize,
        fill: PageFill,
    ) -> Result<Vec<(String, PerKeyRecord)>, SchemaError> {
        use crate::schema::types::field::FilterUtils;

        if want == 0 {
            return Ok(Vec::new());
        }
        let base_end = FilterUtils::create_prefix_end(base_prefix)?;
        let mut cursor: Option<String> = after.map(ToString::to_string);
        let mut kept: Vec<(String, PerKeyRecord)> = Vec::new();

        loop {
            let ask = fill.chunk_for(want - kept.len());
            let (batch, exhausted) = match cursor.as_deref() {
                None => {
                    let rows = self
                        .scan_per_key_prefix_paged(base_prefix, storage_prefix, ask)
                        .await?;
                    let exhausted = rows.len() < ask;
                    (rows, exhausted)
                }
                Some(after) => {
                    // The range primitive is inclusive on `start`; ask for one
                    // extra so dropping the cursor row still yields `ask` new
                    // rows, and read exhaustion from the raw count.
                    let probe = ask.saturating_add(1);
                    let rows = self
                        .scan_per_key_range_paged(after, &base_end, storage_prefix, probe)
                        .await?;
                    let exhausted = rows.len() < probe;
                    let advanced: Vec<(String, PerKeyRecord)> = rows
                        .into_iter()
                        .filter(|(key, _)| key.as_str() > after)
                        .collect();
                    (advanced, exhausted)
                }
            };

            // An ascending scan from an inclusive `after` can only repeat that
            // one key, so a non-exhausted batch always advances the cursor.
            // Breaking on an empty batch keeps that an invariant, not a hang.
            if batch.is_empty() {
                break;
            }
            // The storage scan can read ahead to amortize sparse tombstones.
            // Check Delete barriers only for rows this page consumes. A live
            // five-row page must not point-read barriers for every row in the
            // read-ahead batch. Advance the cursor past only examined rows.
            let mut rows = batch.into_iter();
            while kept.len() < want {
                let need = want - kept.len();
                let mut candidates = Vec::new();
                for (key, record) in rows.by_ref() {
                    cursor = Some(key.clone());
                    if fill.keeps(&record) {
                        candidates.push((key, record));
                        if candidates.len() == need {
                            break;
                        }
                    }
                }
                if candidates.is_empty() {
                    break;
                }
                kept.extend(
                    self.filter_delete_barriers_for_records(candidates, storage_prefix)
                        .await?,
                );
            }
            if kept.len() >= want || exhausted {
                break;
            }
        }

        kept.truncate(want);
        Ok(kept)
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
            .store
            .rows()
            .scan_items_in_range_paged(&start, &end, limit)
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!("paged scan per-key range {start}..{end}: {e}"))
            })?;
        Ok(strip_per_key_storage_prefix(storage_prefix, scanned))
    }
}
