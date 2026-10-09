use super::*;
// lint:file-size-ok moved verbatim from the parent module; one method family per file

/// The molecule a 1-D page is read from. These three always travel together —
/// bundling them keeps [`AtomStore::load_1d_page_window`] inside the argument
/// budget without splitting a routine that is one decision.
pub(super) struct OneDPageTarget<'a> {
    molecule_uuid: &'a str,
    header: &'a MoleculeHeader,
    storage_prefix: Option<&'a str>,
}

impl AtomStore {
    /// One-dimensional keyed field (Hash-only or Range-only slots).
    ///
    /// Unified layout: hash-only at `(hash, "")`, range-only at `("", range)`
    /// under [`hash_range_record_key`].
    // lint:fn-size-ok moved verbatim from the parent module; splitting is separate work.
    pub(crate) async fn load_filtered_1d(
        &self,
        molecule_uuid: &str,
        header: &MoleculeHeader,
        storage_prefix: Option<&str>,
        filter: &crate::schema::types::field::HashRangeFilter,
        slot: OneDSlot,
        fill: PageFill,
    ) -> Result<Option<MoleculeData>, SchemaError> {
        use crate::schema::types::field::FilterUtils;
        use crate::schema::types::field::HashRangeFilter as F;

        // Paged 1-D reads. `SampleN` is that page at offset 0.
        // Range stops at the filled window (one `mk:{M}:\0` partition).
        // Hash, while the page index is off, decodes every `mk:` body and
        // then truncates. Those are not the same body bound.
        let page_window = match filter {
            F::Page { offset, limit } => Some((*offset, *limit)),
            F::SampleN(n) => Some((
                0usize,
                (*n).min(crate::schema::types::field::SAMPLE_PEEK_CAP),
            )),
            _ => None,
        };
        if let Some((offset, limit)) = page_window {
            let want = offset.saturating_add(limit);
            let records = self
                .load_1d_page_window(
                    OneDPageTarget {
                        molecule_uuid,
                        header,
                        storage_prefix,
                    },
                    slot,
                    None,
                    want,
                    fill,
                )
                .await?;
            return Ok(Some(MoleculeHashRange::from_per_key_records(
                molecule_uuid.to_string(),
                header.version,
                header.updated_at,
                records,
            )));
        }

        if let F::PageAfter { after, limit } = filter {
            // Storage-form cursor. Only the hash segment is mapped: the range
            // segment is used verbatim, exactly as the pre-index code did.
            let cursor = match slot {
                OneDSlot::Hash => (
                    self.storage_hash(molecule_uuid, after.hash.as_deref().unwrap_or(""))?,
                    EMPTY_KEY_COMPONENT.to_string(),
                ),
                OneDSlot::Range => (
                    EMPTY_KEY_COMPONENT.to_string(),
                    after.range.as_deref().unwrap_or("").to_string(),
                ),
            };
            let records = self
                .load_1d_page_window(
                    OneDPageTarget {
                        molecule_uuid,
                        header,
                        storage_prefix,
                    },
                    slot,
                    Some(cursor),
                    *limit,
                    fill,
                )
                .await?;
            return Ok(Some(MoleculeHashRange::from_per_key_records(
                molecule_uuid.to_string(),
                header.version,
                header.updated_at,
                records,
            )));
        }

        // Multi-get on a 1-D molecule: one grouped read for every pair, using
        // only the slot's own dimension (hash for Hash-only, range for
        // Range-only; the other component is ignored, as `apply_*_filter` does).
        // Without this arm the batch fell to the O(field) full load and then
        // compared API-form keys to storage-form keys, so it cost a whole-field
        // read and could return no rows. Echo the caller's API key, as the
        // composite arm does, so the in-memory apply matches it. Candidate
        // order matches `get_per_key_1d`: each hash spelling, then each range
        // spelling. The first visible spelling wins.
        if let F::HashRangeKeys(keys) = filter {
            let mut groups = Vec::with_capacity(keys.len());
            let mut api_keys = Vec::with_capacity(keys.len());
            for (hash, range) in keys {
                let (h, r) = match slot {
                    OneDSlot::Hash => (hash.as_str(), EMPTY_KEY_COMPONENT),
                    OneDSlot::Range => (EMPTY_KEY_COMPONENT, range.as_str()),
                };
                let hash_cands = self
                    .key_codec_for_molecule(molecule_uuid)
                    .storage_hash_read_candidates(molecule_uuid, h)
                    .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
                let range_cands = self
                    .key_codec_for_molecule(molecule_uuid)
                    .storage_range_read_candidates(molecule_uuid, r)
                    .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
                let mut group =
                    Vec::with_capacity(hash_cands.len().saturating_mul(range_cands.len()));
                for sh in &hash_cands {
                    for sr in &range_cands {
                        group.push(molecule_key_codec::hash_range_record_key(
                            molecule_uuid,
                            sh,
                            sr,
                        ));
                    }
                }
                groups.push(group);
                let (api_hash, api_range) = match slot {
                    OneDSlot::Hash => (hash.clone(), EMPTY_KEY_COMPONENT.to_string()),
                    OneDSlot::Range => (EMPTY_KEY_COMPONENT.to_string(), range.clone()),
                };
                api_keys.push((api_hash, api_range));
            }
            let hits = self
                .load_first_present_candidate(storage_prefix, &groups)
                .await?;
            let mut records = Vec::with_capacity(keys.len());
            for ((api_hash, api_range), hit) in api_keys.into_iter().zip(hits) {
                if let Some(rec) = hit {
                    records.push((api_hash, api_range, rec.entry, rec.meta));
                }
            }
            return Ok(Some(MoleculeHashRange::from_per_key_records(
                molecule_uuid.to_string(),
                header.version,
                header.updated_at,
                records,
            )));
        }

        match slot {
            OneDSlot::Hash => {
                let hash: &str = match filter {
                    F::HashKey(h)
                    | F::HashRangeKey { hash: h, .. }
                    | F::HashRangePrefix { hash: h, .. }
                    | F::HashRangeRange { hash: h, .. } => h,
                    _ => return Ok(None),
                };
                let scanned = self
                    .get_per_key_1d(molecule_uuid, hash, EMPTY_KEY_COMPONENT, storage_prefix)
                    .await?
                    .into_iter()
                    .collect();
                let records = Self::decode_1d_records(molecule_uuid, slot, scanned);
                Ok(Some(MoleculeHashRange::from_per_key_records(
                    molecule_uuid.to_string(),
                    header.version,
                    header.updated_at,
                    records,
                )))
            }
            OneDSlot::Range => {
                let scanned: Vec<(String, PerKeyRecord)> = match filter {
                    F::RangeKey(k) | F::HashKey(k) | F::HashRangeKey { range: k, .. } => self
                        .get_per_key_1d(molecule_uuid, EMPTY_KEY_COMPONENT, k, storage_prefix)
                        .await?
                        .into_iter()
                        .collect(),
                    F::RangePrefix(p) | F::HashRangePrefix { prefix: p, .. } => {
                        if p.is_empty() {
                            return Ok(None);
                        }
                        self.scan_per_key_1d_range(
                            molecule_uuid,
                            p,
                            &FilterUtils::create_prefix_end(p),
                            OneDSlot::Range,
                            storage_prefix,
                        )
                        .await?
                    }
                    F::RangeRange { start, end } | F::HashRangeRange { start, end, .. } => {
                        if start >= end {
                            Vec::new()
                        } else {
                            self.scan_per_key_1d_range(
                                molecule_uuid,
                                start,
                                end,
                                OneDSlot::Range,
                                storage_prefix,
                            )
                            .await?
                        }
                    }
                    _ => return Ok(None),
                };

                let records = Self::decode_1d_records(molecule_uuid, slot, scanned);
                Ok(Some(MoleculeHashRange::from_per_key_records(
                    molecule_uuid.to_string(),
                    header.version,
                    header.updated_at,
                    records,
                )))
            }
        }
    }

    /// One page of a 1-D molecule, in slot order, without sweeping the
    /// collection. `after` is an **exclusive storage-form** `(hash, range)`
    /// cursor; `None` starts at the head.
    ///
    /// The two slots need different remedies because their key shapes differ in
    /// where the partition separator lands:
    ///
    /// - **Range-only** records are `mk:{M}:\0{range}` — the separator sits
    ///   directly after the empty hash segment, so a molecule's whole
    ///   range-only key space is a SINGLE partition, already in `range` order.
    ///   Scanning `mk:{M}:\0` instead of `mk:{M}:` returns identical rows from
    ///   one partition's groups rather than all of them. No index involved.
    ///
    /// - **Hash-only** records are `mk:{M}:{esc(hash)}\0` — one partition *per
    ///   hash*, so a molecule-wide listing genuinely spans every partition and
    ///   no choice of `mk:` prefix can prune it. These are answered from the
    ///   derived `mhr:` index, which is pinned to one partition (`mhr:{M}\0`)
    ///   and is already maintained per-record by both write paths. For a
    ///   hash-only molecule every record's range is empty, so the index's
    ///   `(range, hash)` order collapses to plain `hash` order — the Page order
    ///   this path wants. The authoritative `mk:` records are then point-read
    ///   for just that page, which is what the 2-D path already does.
    // lint:fn-size-ok moved verbatim from the parent module; splitting is separate work.
    async fn load_1d_page_window(
        &self,
        target: OneDPageTarget<'_>,
        slot: OneDSlot,
        after: Option<(String, String)>,
        want: usize,
        fill: PageFill,
    ) -> Result<Vec<(String, String, AtomEntry, Option<KeyMetadata>)>, SchemaError> {
        let OneDPageTarget {
            molecule_uuid,
            header,
            storage_prefix,
        } = target;
        if want == 0 {
            return Ok(Vec::new());
        }
        match slot {
            OneDSlot::Range => {
                let prefix = molecule_key_codec::hash_range_scan_prefix_for_hash(
                    molecule_uuid,
                    EMPTY_KEY_COMPONENT,
                );
                let start = after.as_ref().map(|(_, range)| {
                    molecule_key_codec::hash_range_record_key(
                        molecule_uuid,
                        EMPTY_KEY_COMPONENT,
                        range,
                    )
                });
                let scanned = self
                    .scan_per_key_window_filled(
                        &prefix,
                        start.as_deref(),
                        storage_prefix,
                        want,
                        fill,
                    )
                    .await?;
                Ok(Self::decode_1d_records(molecule_uuid, slot, scanned))
            }
            OneDSlot::Hash => {
                if !super::super::super::helpers::HASH_RANGE_PAGE_INDEX_ENABLED {
                    // Page index retired: scan authoritative `mk:{M}:*` (hash-major).
                    // Product paths should key-restrict; this serves residual
                    // full-field / hash-only page walks only.
                    let _ = header;
                    let prefixes = self
                        .key_codec_for_molecule(molecule_uuid)
                        .molecule_record_prefixes_for_read(molecule_uuid);
                    let scanned = self
                        .scan_per_key_prefixes(&prefixes, storage_prefix)
                        .await
                        .map_err(|e| {
                            SchemaError::InvalidData(format!(
                                "scan hash-only molecule {molecule_uuid}: {e}"
                            ))
                        })?;
                    let mut decoded = Self::decode_1d_records(molecule_uuid, slot, scanned);
                    // Before the window is taken, not after: a forked pair that
                    // straddles two pages is delivered twice, and the offset the
                    // caller advances by would be counting storage keys.
                    let forked = self.forked_legacy_keys(
                        molecule_uuid,
                        decoded.iter().map(|(h, r, _, _)| (h.as_str(), r.as_str())),
                    );
                    if !forked.is_empty() {
                        decoded.retain(|(h, r, _, _)| !forked.contains(&(h.clone(), r.clone())));
                    }
                    decoded.sort_by(|a, b| a.0.cmp(&b.0));
                    if let Some((after_hash, _)) = after.as_ref() {
                        decoded.retain(|(h, _, _, _)| h > after_hash);
                    }
                    // The window counts the rows `fill` counts. This branch
                    // already holds the whole scan, so honouring the fill is a
                    // retain before the truncate rather than a refill loop —
                    // but skipping it is the same defect the range slot was
                    // fixed for: a page of 100 over a two-thirds-tombstoned
                    // set came back as 33, and the shortfall reads on the wire
                    // exactly like the end of the set.
                    decoded.retain(|(_, _, _, meta)| fill.keeps_meta(meta.as_ref()));
                    decoded.truncate(want);
                    for record in &mut decoded {
                        record.1 = EMPTY_KEY_COMPONENT.to_string();
                    }
                    return Ok(decoded);
                }
                if !self
                    .hash_range_page_index_complete(molecule_uuid, storage_prefix)
                    .await?
                {
                    self.rebuild_hash_range_page_index(molecule_uuid, storage_prefix, header)
                        .await?;
                }
                let (mut records, stale) = self
                    .load_hash_range_page_window_filled(
                        molecule_uuid,
                        storage_prefix,
                        after.clone(),
                        want,
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
                            after,
                            want,
                            fill,
                        )
                        .await?
                        .0;
                }
                // Hold the pre-index contract exactly: the `mk:` scan this
                // replaced decoded through `decode_1d_records`, which keeps the
                // hash and DISCARDS the range for a Hash slot. The index knows
                // the real range, so echoing it here would start surfacing a
                // non-empty range for a mis-oriented record where the old path
                // returned `("", ...)`. A hash-only molecule's ranges are empty
                // anyway; this only matters for legacy mis-oriented rows, and
                // there the old shape is the compatible one.
                for record in &mut records {
                    record.1 = EMPTY_KEY_COMPONENT.to_string();
                }
                Ok(records)
            }
        }
    }

    /// Point-get a 1-D slot under the unified key layout.
    /// `hash` / `range` are **API-form**, mapped once to storage form (plain or
    /// blind/OPE) — no dual-read fallback.
    pub(crate) async fn get_per_key_1d(
        &self,
        molecule_uuid: &str,
        hash: &str,
        range: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Option<(String, PerKeyRecord)>, SchemaError> {
        let hash_cands = self
            .key_codec_for_molecule(molecule_uuid)
            .storage_hash_read_candidates(molecule_uuid, hash)
            .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
        let range_cands = self
            .key_codec_for_molecule(molecule_uuid)
            .storage_range_read_candidates(molecule_uuid, range)
            .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
        for sh in &hash_cands {
            for sr in &range_cands {
                let key = molecule_key_codec::hash_range_record_key(molecule_uuid, sh, sr);
                if let Some(hit) = self.get_per_key(&key, storage_prefix).await? {
                    return Ok(Some(hit));
                }
            }
        }
        Ok(None)
    }

    /// Range-scan a 1-D slot under the unified key layout.
    /// Segment bounds are API-form. Under migrating encodings, scans the primary
    /// (blind/OPE) bound space first, then plain fallback when empty.
    pub(crate) async fn scan_per_key_1d_range(
        &self,
        molecule_uuid: &str,
        start_seg: &str,
        end_seg: &str,
        slot: OneDSlot,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<(String, PerKeyRecord)>, SchemaError> {
        match slot {
            OneDSlot::Hash => {
                // Blinded HashKeys are not order-preserving for plaintext; dual-read
                // still tries primary then plain bound encodings for migrate.
                let start_cands = self
                    .key_codec_for_molecule(molecule_uuid)
                    .storage_hash_read_candidates(molecule_uuid, start_seg)
                    .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
                let end_cands = self
                    .key_codec_for_molecule(molecule_uuid)
                    .storage_hash_read_candidates(molecule_uuid, end_seg)
                    .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
                // Pair primary-with-primary, then plain-with-plain (same candidate index).
                let n = start_cands.len().min(end_cands.len());
                let mut out = Vec::new();
                for i in 0..n {
                    let start = molecule_key_codec::hash_range_record_key(
                        molecule_uuid,
                        &start_cands[i],
                        EMPTY_KEY_COMPONENT,
                    );
                    let end = molecule_key_codec::hash_range_record_key(
                        molecule_uuid,
                        &end_cands[i],
                        EMPTY_KEY_COMPONENT,
                    );
                    out = self
                        .scan_per_key_range(&start, &end, storage_prefix)
                        .await?;
                    if !out.is_empty() {
                        break;
                    }
                }
                Ok(out)
            }
            OneDSlot::Range => {
                let start_cands = self
                    .key_codec_for_molecule(molecule_uuid)
                    .storage_range_read_candidates(molecule_uuid, start_seg)
                    .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
                let end_cands = self
                    .key_codec_for_molecule(molecule_uuid)
                    .storage_range_read_candidates(molecule_uuid, end_seg)
                    .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
                let n = start_cands.len().min(end_cands.len());
                let mut out = Vec::new();
                for i in 0..n {
                    let start = molecule_key_codec::hash_range_record_key(
                        molecule_uuid,
                        EMPTY_KEY_COMPONENT,
                        &start_cands[i],
                    );
                    let end = molecule_key_codec::hash_range_record_key(
                        molecule_uuid,
                        EMPTY_KEY_COMPONENT,
                        &end_cands[i],
                    );
                    out = self
                        .scan_per_key_range(&start, &end, storage_prefix)
                        .await?;
                    if !out.is_empty() {
                        break;
                    }
                }
                Ok(out)
            }
        }
    }

    /// Decode scanned unified `mk:` keys into 1-D hash-only or range-only records.
    pub(crate) fn decode_1d_records(
        molecule_uuid: &str,
        slot: OneDSlot,
        scanned: Vec<(String, PerKeyRecord)>,
    ) -> Vec<(String, String, AtomEntry, Option<KeyMetadata>)> {
        scanned
            .into_iter()
            .filter_map(|(k, r)| {
                let (h, range) = molecule_key_codec::decode_hash_range_any(&k)
                    .or_else(|| molecule_key_codec::decode_hash_range(&k, molecule_uuid))?;
                let segment = match slot {
                    OneDSlot::Hash => h,
                    OneDSlot::Range => range,
                };
                Some(match slot {
                    OneDSlot::Hash => (segment, EMPTY_KEY_COMPONENT.to_string(), r.entry, r.meta),
                    OneDSlot::Range => (EMPTY_KEY_COMPONENT.to_string(), segment, r.entry, r.meta),
                })
            })
            .collect()
    }
}
