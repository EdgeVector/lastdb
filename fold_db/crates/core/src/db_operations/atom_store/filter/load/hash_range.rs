use super::*;

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
    pub(super) async fn list_hash_range_keys_page_window(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        take: Option<usize>,
        after: Option<(&str, &str)>,
    ) -> Result<Vec<(String, String)>, SchemaError> {
        if let Some(keys) = self
            .preloaded_hash_range_keys_page_window(molecule_uuid, storage_prefix, take, after)
            .await?
        {
            return Ok(keys);
        }
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

        let keys = self.merge_captured_tip_keys(storage_prefix, molecule_uuid, keys)?;
        let candidates = keys.iter().filter_map(|k| {
            let suffix = k.strip_prefix(&scan_prefix).unwrap_or(k.as_str());
            let (hash, range) = molecule_key_codec::decode_hash_range_suffix(suffix)?;
            if let Some((after_r, after_h)) = after {
                if (range.as_str(), hash.as_str()) <= (after_r, after_h) {
                    return None;
                }
            }
            Some(RangeMajorKey { range, hash })
        });
        let ordered = select_smallest_range_major(candidates, take);
        // Drop the raw key listing before materializing the return buffer.
        drop(keys);

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
    pub(super) async fn fill_hash_range_window(
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
            let records = self
                .load_hash_range_page_records(molecule_uuid, storage_prefix, chunk)
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

    pub(crate) async fn load_filtered_hash_range(
        &self,
        molecule_uuid: &str,
        header: &MoleculeHeader,
        storage_prefix: Option<&str>,
        filter: &crate::schema::types::field::HashRangeFilter,
        fill: PageFill,
    ) -> Result<Option<MoleculeData>, SchemaError> {
        use crate::schema::types::field::HashRangeFilter as F;

        if let Some(found) = self
            .load_hash_range_page(molecule_uuid, header, storage_prefix, filter, fill)
            .await?
        {
            return Ok(Some(found));
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
            return self
                .load_hash_range_keys_batch(molecule_uuid, header, storage_prefix, keys)
                .await;
        }

        self.load_scanned_hash_range(molecule_uuid, header, storage_prefix, filter)
            .await
    }

    /// Scan the narrowed `mk:` window for a key-restricted filter and wrap it
    /// as a transient molecule.
    async fn load_scanned_hash_range(
        &self,
        molecule_uuid: &str,
        header: &MoleculeHeader,
        storage_prefix: Option<&str>,
        filter: &crate::schema::types::field::HashRangeFilter,
    ) -> Result<Option<MoleculeData>, SchemaError> {
        use crate::schema::types::field::HashRangeFilter as F;

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

        let Some(scanned) = self
            .scan_hash_range_narrowed(molecule_uuid, storage_prefix, filter)
            .await?
        else {
            return Ok(None);
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
            let api_range = match api_range_for_response {
                Some(api) => api.to_string(),
                None if self
                    .key_codec_for_molecule(molecule_uuid)?
                    .range_encoding()
                    .writes_ope() =>
                {
                    crate::crypto::E2eKeys::ope_decode_range_plaintext(&range)
                        .unwrap_or_else(|| range.clone())
                }
                None => range.clone(),
            };
            let api_key =
                crate::schema::types::key_value::KeyValue::new(Some(hash.clone()), Some(api_range));
            self.note_selected_query_tip(storage_prefix, molecule_uuid, &api_key, &r, &k)?;
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

/// A `(range, hash)` pair ordered range-major, the Page order of a HashRange field.
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

/// The `take` smallest keys in Page order, or every key when `take` is `None`.
///
/// Max-heap of the `take` smallest keys seen so far. BinaryHeap is max-ordered:
/// when full, replace the largest kept key if the candidate is smaller — peak
/// decoded pairs = take, not field size.
fn select_smallest_range_major(
    candidates: impl Iterator<Item = RangeMajorKey>,
    take: Option<usize>,
) -> Vec<RangeMajorKey> {
    let Some(cap) = take else {
        let mut all: Vec<RangeMajorKey> = candidates.collect();
        all.sort();
        return all;
    };
    let mut heap: std::collections::BinaryHeap<RangeMajorKey> = std::collections::BinaryHeap::new();
    for item in candidates {
        if heap.len() < cap {
            heap.push(item);
        } else if item < *heap.peek().expect("heap non-empty when at cap") {
            heap.pop();
            heap.push(item);
        }
    }
    heap.into_sorted_vec()
}
