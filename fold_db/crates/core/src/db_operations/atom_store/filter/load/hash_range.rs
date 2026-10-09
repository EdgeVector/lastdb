use super::*;
// lint:file-size-ok moved verbatim from the parent module; one method family per file

impl AtomStore {
    /// HashRange field. The narrowable shapes all stay within a single hash, so
    /// the fetch is bounded to that hash's record subtree:
    /// - `HashRangeKey { hash, range }` → one `get`.
    /// - `HashRangeKeys([(hash, range), …])` → one grouped read, `O(K)`. Each
    ///   hash group that holds a requested key opens once. The only narrowed
    ///   shape that is NOT confined to a single hash: each pair is its own
    ///   point key, so the fetch is bounded by K regardless of how many
    ///   partitions the batch spans.
    /// - `HashKey(hash)` → `scan_prefix(esc(hash)\0)` (all of one hash's ranges).
    /// - `HashRangePrefix { hash, prefix }` (non-empty) → bounded prefix scan
    ///   within the hash.
    /// - `HashRangeRange { hash, start, end }` → bounded ordered range scan
    ///   within the hash.
    ///
    /// The cross-hash SCAN shapes (`RangeKey` / `RangePrefix` / `RangeRange`,
    /// `HashRange`), empty-prefix, and pattern variants fall through
    /// to the full path (`Ok(None)`) — documented as O(field), not the hot path.
    /// `SampleN` does not: with the page index off it uses the same `mk:` key
    /// window as `Page { offset: 0, limit: n.min(cap) }`. Body fetches stay on
    /// that filled window. The 1-D Hash slot does not have this bound.
    ///
    /// Every variant of the filter enum belongs in one of those two lists.
    /// `HashRangeKeys` was in neither until 2026-09-06, and being absent from
    /// the planner is not inert: it fell to the `_` arm, took the O(field) full
    /// load, and — because only the singular `HashRangeKey` arm echoes the
    /// caller's API range back onto the response — the batch then compared
    /// API-form keys against a molecule holding storage-form (OPE) ranges and
    /// silently dropped every row written under the current encoding.
    /// List `mk:` keys of a HashRange molecule in Page order `(range, hash)`.
    ///
    /// **Keys only** — no value fetch. Backs the retired-`mhr:` `Page` /
    /// `PageAfter` fallback: resolving range-major order still walks the field's
    /// key set (storage is hash-major), but that pass never deserializes an
    /// `AtomEntry`/`KeyMetadata`.
    ///
    /// When `take` is `Some(n)`, only the first `n` keys in Page order are
    /// returned. Selection is a bounded max-heap over decoded `(range, hash)`
    /// pairs so peak decoded-pair memory is `O(n)` rather than `O(field)` —
    /// body fetches stay pinned at the page, and the intermediate pair buffer
    /// no longer scales with field cardinality (the residual that still failed
    /// `hashrange_list_query_index_load_does_not_scale_with_field_size` after
    /// #1157). The raw key listing is still one prefix scan; a later streaming
    /// key API can drop that too.
    ///
    /// `after`, when set, is an exclusive `(range, hash)` lower bound (API form)
    /// matching [`HashRangeFilter::PageAfter`].
    async fn list_hash_range_keys_page_window(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        take: Option<usize>,
        after: Option<(&str, &str)>,
    ) -> Result<Vec<(String, String)>, SchemaError> {
        if matches!(take, Some(0)) {
            return Ok(Vec::new());
        }
        let scan_prefix = crate::schema::types::field::build_storage_key(
            storage_prefix,
            &molecule_key_codec::molecule_record_prefix(molecule_uuid),
        );
        let keys = self
            .main_store
            .list_keys_with_prefix(&scan_prefix)
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!(
                    "list HashRange keys for page scan {molecule_uuid}: {e}"
                ))
            })?;

        // Max-heap of the `take` smallest `(range, hash)` keys seen so far.
        // BinaryHeap is max-ordered: when full, replace the largest kept key if
        // the candidate is smaller — peak decoded pairs = take, not field size.
        #[derive(Eq, PartialEq)]
        struct RangeMajorKey {
            range: String,
            hash: String,
        }
        impl Ord for RangeMajorKey {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                self.range
                    .cmp(&other.range)
                    .then_with(|| self.hash.cmp(&other.hash))
            }
        }
        impl PartialOrd for RangeMajorKey {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }

        let mut heap: std::collections::BinaryHeap<RangeMajorKey> =
            std::collections::BinaryHeap::new();
        let mut all: Option<Vec<RangeMajorKey>> = take.is_none().then(Vec::new);

        // A legacy/current forked pair is ONE record. Excluding the legacy half
        // here rather than at the read boundary is what keeps the window and
        // `total_count` measured in rows: the boundary's collapse only fires
        // when both halves land in the same page, so a straddling pair is
        // served twice and every offset past it is off by one.
        let forked = self.forked_legacy_page_keys(molecule_uuid, &keys, &scan_prefix);

        for k in &keys {
            let suffix = k.strip_prefix(&scan_prefix).unwrap_or(k.as_str());
            let Some((hash, range)) = molecule_key_codec::decode_hash_range_suffix(suffix) else {
                continue;
            };
            if !forked.is_empty() && forked.contains(&(hash.clone(), range.clone())) {
                continue;
            }
            if let Some((after_r, after_h)) = after {
                if (range.as_str(), hash.as_str()) <= (after_r, after_h) {
                    continue;
                }
            }
            let item = RangeMajorKey { range, hash };
            match take {
                Some(cap) => {
                    if heap.len() < cap {
                        heap.push(item);
                    } else if item < *heap.peek().expect("heap non-empty when at cap") {
                        heap.pop();
                        heap.push(item);
                    }
                }
                None => {
                    all.as_mut().expect("all vec when take is None").push(item);
                }
            }
        }
        // Drop the raw key listing before materializing the return buffer.
        drop(keys);

        let mut ordered: Vec<RangeMajorKey> = if take.is_some() {
            heap.into_sorted_vec()
        } else {
            let mut v = all.expect("all vec when take is None");
            v.sort();
            v
        };
        if let Some(cap) = take {
            ordered.truncate(cap);
        }
        Ok(ordered.into_iter().map(|k| (k.hash, k.range)).collect())
    }

    /// Take records for the first `want` keys of `sorted_keys` that `fill`
    /// counts, point-getting in chunks so a sparse live fraction costs a few
    /// batches rather than one fetch of the whole field.
    ///
    /// The composite counterpart to [`Self::scan_per_key_window_filled`]. The
    /// key list may read ahead, but each body read also checks a Delete
    /// barrier. Fetch only the records needed to fill the page, then refill
    /// when a tombstone or Delete barrier hides one.
    async fn fill_hash_range_window(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        sorted_keys: Vec<(String, String)>,
        want: usize,
        fill: PageFill,
    ) -> Result<Vec<(String, String, AtomEntry, Option<KeyMetadata>)>, SchemaError> {
        if want == 0 {
            return Ok(Vec::new());
        }
        let mut kept: Vec<(String, String, AtomEntry, Option<KeyMetadata>)> = Vec::new();
        let mut cursor = 0usize;

        while kept.len() < want && cursor < sorted_keys.len() {
            // The key list can read ahead, but each point get also checks one
            // Delete barrier. Fetch only the rows this page still needs.
            let ask = (want - kept.len()).min(sorted_keys.len() - cursor);
            // The loop condition excludes zero; keep the guard if that changes.
            if ask == 0 {
                break;
            }
            let chunk = sorted_keys[cursor..cursor + ask].to_vec();
            cursor += ask;
            let (records, _stale) = self
                .fetch_hash_range_page_records(molecule_uuid, storage_prefix, chunk)
                .await?;
            kept.extend(
                records
                    .into_iter()
                    .filter(|(_, _, _, meta)| fill.keeps_meta(meta.as_ref())),
            );
        }

        kept.truncate(want);
        Ok(kept)
    }

    // lint:fn-size-ok moved verbatim from the parent module; splitting is separate work.
    pub(crate) async fn load_filtered_hash_range(
        &self,
        molecule_uuid: &str,
        header: &MoleculeHeader,
        storage_prefix: Option<&str>,
        filter: &crate::schema::types::field::HashRangeFilter,
        fill: PageFill,
    ) -> Result<Option<MoleculeData>, SchemaError> {
        use crate::schema::types::field::FilterUtils;
        use crate::schema::types::field::HashRangeFilter as F;

        // `Page` / `PageAfter` over a whole HashRange field. With `mhr:` retired,
        // this is Dynamo-style keyed access rather than a full materialize: list
        // just the `mk:` **keys** (hash, range — no bodies) to resolve range-major
        // order, take only the requested window, then point-get bodies for that
        // window alone. Growing the field grows the key listing (cheap: a few
        // bytes per key, no `AtomEntry`/metadata decode), never the number of
        // bodies fetched, which stays pinned at `want`. Key-restricted filters
        // below never needed `mhr:`.
        if !super::super::super::helpers::HASH_RANGE_PAGE_INDEX_ENABLED {
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
                //
                // LiveRows may need more stored keys than `want` (tombstone
                // refill). Start with fill.chunk_for(want) and expand to the
                // full key set only if the first window under-fills — common
                // all-live pages stay O(want) on decoded pairs.
                let want = offset.saturating_add(limit);
                let key_cap = fill.chunk_for(want).max(want);
                let mut sorted_keys = self
                    .list_hash_range_keys_page_window(
                        molecule_uuid,
                        storage_prefix,
                        Some(key_cap),
                        None,
                    )
                    .await?;
                let mut page = self
                    .fill_hash_range_window(molecule_uuid, storage_prefix, sorted_keys, want, fill)
                    .await?;
                if page.len() < want {
                    // Sparse live fraction or deep offset: take every key in
                    // page order and refill. Rare on product key-restricted
                    // paths; required for correctness of unfiltered Page.
                    sorted_keys = self
                        .list_hash_range_keys_page_window(molecule_uuid, storage_prefix, None, None)
                        .await?;
                    // Skip re-scan when the first pass already saw every key.
                    if sorted_keys.len() > key_cap {
                        page = self
                            .fill_hash_range_window(
                                molecule_uuid,
                                storage_prefix,
                                sorted_keys,
                                want,
                                fill,
                            )
                            .await?;
                    }
                }
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
                let key_cap = fill.chunk_for(*limit).max(*limit);
                let mut page_keys = self
                    .list_hash_range_keys_page_window(
                        molecule_uuid,
                        storage_prefix,
                        Some(key_cap),
                        Some((after_r, after_h)),
                    )
                    .await?;
                let mut page = self
                    .fill_hash_range_window(molecule_uuid, storage_prefix, page_keys, *limit, fill)
                    .await?;
                if page.len() < *limit {
                    page_keys = self
                        .list_hash_range_keys_page_window(
                            molecule_uuid,
                            storage_prefix,
                            None,
                            Some((after_r, after_h)),
                        )
                        .await?;
                    if page_keys.len() > key_cap {
                        page = self
                            .fill_hash_range_window(
                                molecule_uuid,
                                storage_prefix,
                                page_keys,
                                *limit,
                                fill,
                            )
                            .await?;
                    }
                }
                return Ok(Some(MoleculeHashRange::from_per_key_records(
                    molecule_uuid.to_string(),
                    header.version,
                    header.updated_at,
                    page,
                )));
            }
        }

        // `Page` paginated list over a HashRange field. The primary `mk:` keys
        // are hash-major (`mk:{M}:{esc(hash)}\0{range}`), while Page order is
        // `(range, hash)`. Use the derived range-major `mhr:` marker index so
        // the hot path scans only `offset+limit` marker keys, then point-gets
        // the authoritative `mk:` records for that page. Existing molecules
        // lazily build the index once; a stale marker triggers one rebuild.
        if let F::Page { offset, limit } = filter {
            let want = offset.saturating_add(*limit);
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
                    .load_hash_range_page_window_filled(
                        molecule_uuid,
                        storage_prefix,
                        None,
                        want,
                        fill,
                    )
                    .await?
                    .0;
            }
            return Ok(Some(MoleculeHashRange::from_per_key_records(
                molecule_uuid.to_string(),
                header.version,
                header.updated_at,
                records,
            )));
        }

        if let F::PageAfter { after, limit } = filter {
            if *limit == 0 {
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
                    *limit,
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
                        *limit,
                        fill,
                    )
                    .await?
                    .0;
            }
            return Ok(Some(MoleculeHashRange::from_per_key_records(
                molecule_uuid.to_string(),
                header.version,
                header.updated_at,
                records,
            )));
        }

        // Multi-get: one grouped read for every (hash, range) pair.
        //
        // This arm returns its own records rather than joining the `scanned`
        // path below, because that path carries ONE `api_hash_for_response` /
        // `api_range_for_response` for the whole filter. A batch has K API
        // pairs, which a single Option cannot express — and falling through
        // with `None` is exactly the bug: the response then surfaces the
        // storage-form key, and the caller's API-form key no longer matches it.
        // Dual-read candidates are spellings of ONE row; first visible wins.
        if let F::HashRangeKeys(keys) = filter {
            let codec = self.key_codec_for_molecule(molecule_uuid);
            let mut groups = Vec::with_capacity(keys.len());
            for (hash, range) in keys {
                groups.push(
                    codec
                        .api_hash_range_record_keys_for_read(molecule_uuid, hash, range)
                        .map_err(|e| SchemaError::InvalidData(e.to_string()))?,
                );
            }
            let hits = self
                .load_first_present_candidate(storage_prefix, &groups)
                .await?;
            let mut records = Vec::with_capacity(keys.len());
            for ((hash, range), hit) in keys.iter().zip(hits) {
                if let Some(rec) = hit {
                    // Option I: echo the API pair the caller supplied, so
                    // `apply_hash_range_filter` — pure string comparison,
                    // no codec — can match it.
                    records.push((hash.clone(), range.clone(), rec.entry, rec.meta));
                }
            }
            return Ok(Some(MoleculeHashRange::from_per_key_records(
                molecule_uuid.to_string(),
                header.version,
                header.updated_at,
                records,
            )));
        }

        // Filters carry API HashKey/RangeKey; map to storage-form before keying.
        let api_hash_for_response: Option<&str> = match filter {
            F::HashKey(h)
            | F::HashRangeKey { hash: h, .. }
            | F::HashRangePrefix { hash: h, .. }
            | F::HashRangeRange { hash: h, .. } => Some(h.as_str()),
            _ => None,
        };
        // Point range (exact HashRangeKey) can echo API range; between/prefix cannot.
        let api_range_for_response: Option<&str> = match filter {
            F::HashRangeKey { range: r, .. } => Some(r.as_str()),
            _ => None,
        };

        let scanned: Vec<(String, PerKeyRecord)> = match filter {
            F::HashRangeKey { hash, range } => {
                let mut out = Vec::new();
                let read_keys = self
                    .key_codec_for_molecule(molecule_uuid)
                    .api_hash_range_record_keys_for_read(molecule_uuid, hash, range)
                    .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
                for key in read_keys {
                    if let Some(hit) = self.get_per_key(&key, storage_prefix).await? {
                        out.push(hit);
                        break;
                    }
                }
                out
            }
            F::HashKey(hash) => {
                if let Some(unique) = self
                    .get_unique_hash_key_record(molecule_uuid, hash, storage_prefix)
                    .await?
                {
                    vec![unique]
                } else {
                    let prefixes = self
                        .key_codec_for_molecule(molecule_uuid)
                        .api_hash_range_scan_prefixes_for_read(molecule_uuid, hash)
                        .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
                    self.scan_per_key_prefixes(&prefixes, storage_prefix)
                        .await?
                }
            }
            F::HashRangePrefix { hash, prefix } => {
                // Storage-form hash × range-prefix scan, dual-read across
                // molecule-UUID spellings. Per-byte OPE preserves byte-prefixes
                // inside one spelling; do not mix encodings in one bound pair.
                let codec = self.key_codec_for_molecule(molecule_uuid);
                if prefix.is_empty() {
                    let prefixes = codec
                        .api_hash_range_scan_prefixes_for_read(molecule_uuid, hash)
                        .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
                    self.scan_per_key_prefixes(&prefixes, storage_prefix)
                        .await?
                } else {
                    let mut out = Vec::new();
                    for uid in crate::atom::molecule_uuid_read_candidates(molecule_uuid) {
                        let hash_cands = codec
                            .storage_hash_read_candidates(&uid, hash)
                            .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
                        let range_prefix_cands = codec
                            .storage_range_read_candidates(&uid, prefix)
                            .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
                        for sh in &hash_cands {
                            for sr_prefix in &range_prefix_cands {
                                let start =
                                    molecule_key_codec::hash_range_record_key(&uid, sh, sr_prefix);
                                let end = molecule_key_codec::hash_range_record_key(
                                    &uid,
                                    sh,
                                    &FilterUtils::create_prefix_end(sr_prefix),
                                );
                                out.extend(
                                    self.scan_per_key_range(&start, &end, storage_prefix)
                                        .await?,
                                );
                            }
                        }
                    }
                    out
                }
            }
            F::HashRangeRange { hash, start, end } => {
                if start >= end {
                    Vec::new()
                } else {
                    // Storage-form hash × range bounds.
                    let hash_cands = self
                        .key_codec_for_molecule(molecule_uuid)
                        .storage_hash_read_candidates(molecule_uuid, hash)
                        .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
                    let start_cands = self
                        .key_codec_for_molecule(molecule_uuid)
                        .storage_range_read_candidates(molecule_uuid, start)
                        .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
                    let end_cands = self
                        .key_codec_for_molecule(molecule_uuid)
                        .storage_range_read_candidates(molecule_uuid, end)
                        .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
                    let n_range = start_cands.len().min(end_cands.len());
                    let mut out = Vec::new();
                    'hrr: for sh in &hash_cands {
                        for i in 0..n_range {
                            let s = molecule_key_codec::hash_range_record_key(
                                molecule_uuid,
                                sh,
                                &start_cands[i],
                            );
                            let e = molecule_key_codec::hash_range_record_key(
                                molecule_uuid,
                                sh,
                                &end_cands[i],
                            );
                            out = self.scan_per_key_range(&s, &e, storage_prefix).await?;
                            if !out.is_empty() {
                                break 'hrr;
                            }
                        }
                    }
                    out
                }
            }
            _ => return Ok(None),
        };

        let mut records = Vec::with_capacity(scanned.len());
        for (k, r) in scanned {
            let (storage_hash, storage_range) = molecule_key_codec::decode_hash_range_any(&k)
                .or_else(|| molecule_key_codec::decode_hash_range(&k, molecule_uuid))
                .ok_or_else(|| {
                    SchemaError::InvalidData(format!("malformed hash-range record key: {k}"))
                })?;
            // Option I: never surface storage tokens as API keys when the
            // client already supplied the plaintext partition/range.
            let hash = api_hash_for_response.map_or(storage_hash, str::to_string);
            let range = api_range_for_response.map_or(storage_range, str::to_string);
            records.push((hash, range, r.entry, r.meta));
        }
        // The transient molecule is only the narrowed `mk:` window.
        // `sample()` sorts those live keys. This path does not read an order log.
        Ok(Some(MoleculeHashRange::from_per_key_records(
            molecule_uuid.to_string(),
            header.version,
            header.updated_at,
            records,
        )))
    }
}
