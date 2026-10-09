//! Bounded ordered tip reads, including immutable generation overlays.

use std::collections::BTreeMap;

use crate::atom::molecule_key_codec;
use crate::schema::types::field::{build_storage_key, FilterUtils, KeyWindow};
use crate::schema::SchemaError;

use super::super::types::{MoleculeGenerationDelete, MoleculeGenerationSlot, PerKeyRecord};
use super::super::AtomStore;

impl AtomStore {
    /// Apply a HashKey page before cloning the entire partition. Each readable
    /// molecule spelling has one ordered stream. Current spellings win ties.
    pub(crate) async fn scan_hash_tip_window(
        &self,
        molecule: &str,
        hash: &str,
        storage_prefix: Option<&str>,
        window: &KeyWindow,
        include_tombstones: bool,
    ) -> Result<Option<Vec<(String, PerKeyRecord)>>, SchemaError> {
        let codec = self.key_codec_for_molecule(molecule);
        let invalid = |e| SchemaError::InvalidData(format!("partition page codec: {e}"));
        let prefixes = codec
            .api_hash_range_scan_prefixes_for_read(molecule, hash)
            .map_err(invalid)?;
        let (mut skip, limit, mut after) = match window {
            KeyWindow::Offset { offset, limit } => (*offset, *limit, None),
            KeyWindow::After { after, limit } if after.hash.as_deref() == Some(hash) => {
                (0, *limit, Some(after.range.clone().unwrap_or_default()))
            }
            // An arbitrary cross-hash cursor uses the general comparator path.
            KeyWindow::After { .. } => return Ok(None),
        };
        // A range call may decode legacy sidecars. Match larger requested
        // pages without repeatedly decoding them, while bounding offset walks
        // and large internal callers to at most 1,024 tips per stream.
        let batch_limit = limit.clamp(128, 1024);
        // The first OPE word has a fixed high 24 bits for each key and
        // molecule. Each six-hex-digit prefix is one independently ordered
        // stream, including the fallback key during key rotation.
        let mut lanes = Vec::new();
        for prefix in &prefixes {
            let uid = molecule_key_codec::molecule_uuid_from_storage_key(prefix)
                .expect("codec molecule prefix");
            if codec.range_encoding().writes_ope() {
                let candidates = codec
                    .storage_range_read_candidates(uid, "\0")
                    .map_err(invalid)?;
                let mut seen = std::collections::HashSet::new();
                for (index, candidate) in candidates.into_iter().enumerate() {
                    let family = &candidate[..6];
                    // A rare first-word PRF collision cannot certify a single
                    // ordered stream. Keep the compatibility read in that case.
                    if !seen.insert(family.to_owned()) {
                        return Ok(None);
                    }
                    lanes.push((prefix, format!("{prefix}{family}"), index));
                }
            } else {
                lanes.push((prefix, prefix.clone(), 0));
            }
        }
        let mut output = Vec::new();
        while output.len() < limit {
            let want = skip
                .saturating_add(limit - output.len())
                .clamp(1, batch_limit);
            let mut frontier: Option<String> = None;
            let mut union = BTreeMap::new();
            if after.is_none() && codec.range_encoding().writes_ope() {
                // Empty ranges are intentionally not OPE encoded.
                for prefix in &prefixes {
                    if let Some(row) = self.get_per_key(prefix, storage_prefix).await? {
                        union.entry(String::new()).or_insert(row);
                    }
                }
            }
            for (prefix, lane, index) in &lanes {
                let uid = molecule_key_codec::molecule_uuid_from_storage_key(prefix)
                    .expect("codec molecule prefix");
                let cursor = after
                    .as_deref()
                    .filter(|range| !range.is_empty() || !codec.range_encoding().writes_ope())
                    .map(|range| {
                        codec
                            .storage_range_read_candidates(uid, range)
                            .map(|ranges| format!("{prefix}{}", ranges[*index]))
                    })
                    .transpose()
                    .map_err(invalid)?;
                let rows = self
                    .scan_logical_tip_page(lane, cursor.as_deref(), storage_prefix, want)
                    .await?;
                let full = rows.len() == want;
                let mut last = None;
                for (key, record) in rows {
                    let (_, range) =
                        molecule_key_codec::decode_hash_range_any(&key).ok_or_else(|| {
                            SchemaError::InvalidData("invalid partition page key".into())
                        })?;
                    let range = if codec.range_encoding().writes_ope() {
                        crate::crypto::E2eKeys::ope_decode_range_plaintext(&range).unwrap_or(range)
                    } else {
                        range
                    };
                    last = Some(range.clone());
                    union.entry(range).or_insert((key, record));
                }
                if full {
                    if let Some(last) = last {
                        frontier = Some(frontier.map_or(last.clone(), |current| current.min(last)));
                    }
                }
            }
            let mut advanced = false;
            for (range, row) in union {
                if frontier.as_ref().is_some_and(|end| range > *end) {
                    break;
                }
                after = Some(range);
                advanced = true;
                if !include_tombstones && row.1.meta.as_ref().is_some_and(|meta| meta.tombstoned) {
                    continue;
                }
                if skip > 0 {
                    skip -= 1;
                } else {
                    output.push(row);
                }
                if output.len() == limit {
                    break;
                }
            }
            if !advanced || frontier.is_none() {
                break;
            }
        }
        Ok(Some(output))
    }

    /// Merge bounded live and generation streams. Deleted slots advance the
    /// cursor, but never consume a result slot. No atom bodies are read here.
    pub(crate) async fn scan_logical_tip_page(
        &self,
        prefix: &str,
        after: Option<&str>,
        storage_prefix: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, PerKeyRecord)>, SchemaError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let molecule = molecule_key_codec::molecule_uuid_from_storage_key(prefix)
            .ok_or_else(|| SchemaError::InvalidData("tip page needs a molecule prefix".into()))?;
        let pointer = self
            .active_molecule_generation(molecule, storage_prefix)
            .await?;
        let end = FilterUtils::create_prefix_end(prefix);
        let mut cursor = after.map(str::to_owned);
        let mut output = Vec::new();
        while output.len() < limit {
            let remaining = limit - output.len();
            let ask = remaining.saturating_add(usize::from(cursor.is_some()));
            let start = cursor.as_deref().unwrap_or(prefix);
            let live: Vec<(String, PerKeyRecord)> = self
                .main_store
                .scan_items_in_range_paged(
                    &build_storage_key(storage_prefix, start),
                    &build_storage_key(storage_prefix, &end),
                    ask,
                )
                .await
                .map_err(|e| SchemaError::InvalidData(format!("tip page: {e}")))?;
            let live_exhausted = live.len() < ask;
            let mut rows: BTreeMap<String, (Option<PerKeyRecord>, Option<MoleculeGenerationSlot>)> =
                BTreeMap::new();
            for (key, record) in Self::strip_per_key_storage_prefix(storage_prefix, live) {
                if cursor.as_ref().is_none_or(|cursor| key > *cursor) {
                    rows.entry(key).or_default().0 = Some(record);
                }
            }
            let mut generation_exhausted = true;
            if let Some(pointer) = &pointer {
                let bound = |key| {
                    molecule_key_codec::molecule_generation_bound_for_record_bound(
                        molecule,
                        &pointer.generation,
                        key,
                    )
                    .ok_or_else(|| SchemaError::InvalidData("invalid generation page bound".into()))
                };
                let generated: Vec<(String, MoleculeGenerationSlot)> = self
                    .main_store
                    .scan_items_in_range_paged(
                        &build_storage_key(storage_prefix, &bound(start)?),
                        &build_storage_key(storage_prefix, &bound(&end)?),
                        ask,
                    )
                    .await
                    .map_err(|e| SchemaError::InvalidData(format!("generation tip page: {e}")))?;
                generation_exhausted = generated.len() < ask;
                let strip = build_storage_key(storage_prefix, "");
                for (key, slot) in generated {
                    let key = molecule_key_codec::molecule_record_key_from_generation_key(
                        molecule,
                        &pointer.generation,
                        key.strip_prefix(&strip).unwrap_or(&key),
                    )
                    .ok_or_else(|| {
                        SchemaError::InvalidData("invalid generation page key".into())
                    })?;
                    if cursor.as_ref().is_none_or(|cursor| key > *cursor) {
                        rows.entry(key).or_default().1 = Some(slot);
                    }
                }
            }
            // Each stream supplies at least `remaining` new keys unless it is
            // exhausted. Only this common ordered frontier is safe to consume.
            let rows: Vec<_> = rows.into_iter().take(remaining).collect();
            let Some((last, _)) = rows.last() else { break };
            cursor = Some(last.clone());
            let deletions: Vec<Option<MoleculeGenerationDelete>> = if pointer.is_some() {
                let keys = rows
                    .iter()
                    .map(|(key, _)| {
                        molecule_key_codec::molecule_generation_delete_bound_for_record_bound(
                            molecule, key,
                        )
                        .map(|key| build_storage_key(storage_prefix, &key))
                        .ok_or_else(|| {
                            SchemaError::InvalidData("invalid generation delete key".into())
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                self.main_store.get_items(&keys).await.map_err(|e| {
                    SchemaError::InvalidData(format!("generation page deletes: {e}"))
                })?
            } else {
                vec![None; rows.len()]
            };
            let consumed = rows.len();
            for ((key, (live, base)), deletion) in rows.into_iter().zip(deletions) {
                if let Some(record) =
                    Self::apply_generation_delete(Self::merge_generation_slot(live, base), deletion)
                {
                    output.push((key, record));
                }
            }
            if live_exhausted && generation_exhausted && consumed < remaining {
                break;
            }
        }
        Ok(output)
    }
}
