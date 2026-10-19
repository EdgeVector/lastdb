use super::*;
// lint:file-size-ok moved verbatim from the parent module; one method family per file

mod exact;

/// The molecule a 1-D page is read from. These three always travel together —
/// bundling them keeps [`AtomStore::load_1d_page_window`] inside the argument
/// budget without splitting a routine that is one decision.
#[derive(Clone, Copy)]
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
    pub(crate) async fn load_filtered_1d(
        &self,
        molecule_uuid: &str,
        header: &MoleculeHeader,
        storage_prefix: Option<&str>,
        filter: &crate::schema::types::field::HashRangeFilter,
        slot: OneDSlot,
        fill: PageFill,
    ) -> Result<Option<MoleculeData>, SchemaError> {
        use crate::schema::types::field::HashRangeFilter as F;

        let target = OneDPageTarget {
            molecule_uuid,
            header,
            storage_prefix,
        };

        // Paged 1-D reads. `SampleN` is that page at offset 0.
        // Range stops at the filled window (one `mk:{M}:\0` partition).
        // Hash decodes every `mk:` body and
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
                .load_1d_page_window(target, slot, None, want, fill)
                .await?;
            return Ok(Some(Self::wrap_1d_records(&target, records)));
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
                .load_1d_page_window(target, slot, Some(cursor), *limit, fill)
                .await?;
            return Ok(Some(Self::wrap_1d_records(&target, records)));
        }

        if let F::HashRangeKeys(keys) = filter {
            let records = self.load_1d_multi_get(&target, slot, keys).await?;
            return Ok(Some(Self::wrap_1d_records(&target, records)));
        }

        let records = match slot {
            OneDSlot::Hash => self.load_1d_hash_key(&target, filter).await?,
            OneDSlot::Range => self
                .scan_1d_range_slot(&target, filter)
                .await?
                .map(|scanned| Self::decode_1d_records(molecule_uuid, slot, scanned)),
        };
        Ok(records.map(|records| Self::wrap_1d_records(&target, records)))
    }

    fn wrap_1d_records(
        target: &OneDPageTarget<'_>,
        records: Vec<(String, String, AtomEntry, Option<KeyMetadata>)>,
    ) -> MoleculeData {
        MoleculeHashRange::from_per_key_records(
            target.molecule_uuid.to_string(),
            target.header.version,
            target.header.updated_at,
            records,
        )
    }

    /// Key, prefix, or range read of a Range-only molecule. `None` means the
    /// filter cannot be answered from the range slot and the caller must fall
    /// back.
    async fn scan_1d_range_slot(
        &self,
        target: &OneDPageTarget<'_>,
        filter: &crate::schema::types::field::HashRangeFilter,
    ) -> Result<Option<Vec<(String, PerKeyRecord)>>, SchemaError> {
        use crate::schema::types::field::FilterUtils;
        use crate::schema::types::field::HashRangeFilter as F;
        let (molecule_uuid, storage_prefix) = (target.molecule_uuid, target.storage_prefix);
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
                self.scan_per_key_range_slot(
                    molecule_uuid,
                    p,
                    &FilterUtils::create_prefix_end(p),
                    storage_prefix,
                )
                .await?
            }
            F::RangeRange { start, end } | F::HashRangeRange { start, end, .. } => {
                if start >= end {
                    Vec::new()
                } else {
                    self.scan_per_key_range_slot(molecule_uuid, start, end, storage_prefix)
                        .await?
                }
            }
            _ => return Ok(None),
        };
        Ok(Some(scanned))
    }

    /// One page of a 1-D molecule, in slot order, without sweeping the
    /// collection. `after` is an **exclusive storage-form** `(hash, range)`
    /// cursor; `None` starts at the head.
    ///
    /// Range-only records share the empty hash and permit one bounded ordered
    /// read. Hash-only inspection pages read the authoritative `mk:` prefixes
    /// in encoded hash order. They use no separate page index.
    async fn load_1d_page_window(
        &self,
        target: OneDPageTarget<'_>,
        slot: OneDSlot,
        after: Option<(String, String)>,
        want: usize,
        fill: PageFill,
    ) -> Result<Vec<(String, String, AtomEntry, Option<KeyMetadata>)>, SchemaError> {
        if want == 0 {
            return Ok(Vec::new());
        }
        match slot {
            OneDSlot::Range => self.load_1d_range_window(target, after, want, fill).await,
            OneDSlot::Hash => self.load_1d_hash_window(target, after, want, fill).await,
        }
    }

    /// Page window for the Range slot: a keyed range scan under the empty hash.
    async fn load_1d_range_window(
        &self,
        target: OneDPageTarget<'_>,
        after: Option<(String, String)>,
        want: usize,
        fill: PageFill,
    ) -> Result<Vec<(String, String, AtomEntry, Option<KeyMetadata>)>, SchemaError> {
        let OneDPageTarget {
            molecule_uuid,
            storage_prefix,
            ..
        } = target;
        let prefix =
            molecule_key_codec::hash_range_scan_prefix_for_hash(molecule_uuid, EMPTY_KEY_COMPONENT);
        let start = after.as_ref().map(|(_, range)| {
            molecule_key_codec::hash_range_record_key(molecule_uuid, EMPTY_KEY_COMPONENT, range)
        });
        let scanned = self
            .scan_per_key_window_filled(&prefix, start.as_deref(), storage_prefix, want, fill)
            .await?;
        Ok(Self::decode_1d_records(
            molecule_uuid,
            OneDSlot::Range,
            scanned,
        ))
    }

    /// Page index retired: scan authoritative `mk:{M}:*` (hash-major).
    /// Product paths should key-restrict; this serves residual full-field /
    /// hash-only page walks only.
    async fn load_1d_hash_window(
        &self,
        target: OneDPageTarget<'_>,
        after: Option<(String, String)>,
        want: usize,
        fill: PageFill,
    ) -> Result<Vec<(String, String, AtomEntry, Option<KeyMetadata>)>, SchemaError> {
        let OneDPageTarget {
            molecule_uuid,
            storage_prefix,
            ..
        } = target;
        let prefixes = self
            .key_codec_for_molecule(molecule_uuid)?
            .molecule_record_prefixes_for_read(molecule_uuid);
        let scanned = self
            .scan_per_key_prefixes(&prefixes, storage_prefix)
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!("scan hash-only molecule {molecule_uuid}: {e}"))
            })?;
        let mut decoded = Self::decode_1d_records(molecule_uuid, OneDSlot::Hash, scanned);
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
        Ok(decoded)
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
            .key_codec_for_molecule(molecule_uuid)?
            .storage_hash_read_candidates(molecule_uuid, hash)
            .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
        let range_cands = self
            .key_codec_for_molecule(molecule_uuid)?
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

    /// Range-scan the range-only slot under the unified key layout.
    /// Bounds use API form. Try each supported physical bound encoding in
    /// order and return the first nonempty range.
    async fn scan_per_key_range_slot(
        &self,
        molecule_uuid: &str,
        start_seg: &str,
        end_seg: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<(String, PerKeyRecord)>, SchemaError> {
        let start_cands = self
            .key_codec_for_molecule(molecule_uuid)?
            .storage_range_read_candidates(molecule_uuid, start_seg)
            .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
        let end_cands = self
            .key_codec_for_molecule(molecule_uuid)?
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
