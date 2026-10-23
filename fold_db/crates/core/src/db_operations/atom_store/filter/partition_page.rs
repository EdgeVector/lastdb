//! Bounded ordered tip reads, including immutable generation overlays.

use std::collections::BTreeMap;

use crate::atom::molecule_key_codec;
use crate::schema::types::field::{build_storage_key, FilterUtils, KeyWindow};
use crate::schema::SchemaError;

use super::super::types::{
    MoleculeGenerationDelete, MoleculeGenerationPointer, MoleculeGenerationSlot, PerKeyRecord,
};
use super::super::AtomStore;

mod live;

type TipRow = (String, PerKeyRecord);
type TipSlots = (Option<PerKeyRecord>, Option<MoleculeGenerationSlot>);
type TipRows = BTreeMap<String, TipSlots>;

/// Paging state shared by the rounds of `scan_hash_tip_window`.
struct TipPageCursor {
    skip: usize,
    after: Option<String>,
    output: Vec<TipRow>,
    limit: usize,
    include_tombstones: bool,
}

impl TipPageCursor {
    /// Consume ordered rows, advancing past tombstones before counting live rows.
    fn consume(&mut self, rows: Vec<(String, TipRow)>) {
        for (range, row) in rows {
            self.after = Some(range);
            if !self.include_tombstones && row.1.meta.as_ref().is_some_and(|meta| meta.tombstoned) {
                continue;
            }
            if self.skip > 0 {
                self.skip -= 1;
            } else {
                self.output.push(row);
            }
            if self.output.len() == self.limit {
                break;
            }
        }
    }
}

/// Scan bounds for one round of `scan_logical_tip_page`.
struct TipScan<'a> {
    storage_prefix: Option<&'a str>,
    start: &'a str,
    end: &'a str,
    ask: usize,
    cursor: Option<&'a str>,
}

fn codec_err(e: impl std::fmt::Display) -> SchemaError {
    SchemaError::InvalidData(format!("partition page codec: {e}"))
}

impl AtomStore {
    /// Select a HashKey page from its single ordered range stream.
    pub(crate) async fn scan_hash_tip_window(
        &self,
        molecule: &str,
        hash: &str,
        storage_prefix: Option<&str>,
        window: &KeyWindow,
        include_tombstones: bool,
    ) -> Result<Option<Vec<TipRow>>, SchemaError> {
        if let Some(rows) = self
            .preloaded_window(molecule, hash, storage_prefix, window, include_tombstones)
            .await?
        {
            return Ok(Some(rows));
        }
        let codec = self.key_codec_for_molecule(molecule)?;
        let prefix = codec
            .api_hash_range_scan_prefix_for_hash(molecule, hash)
            .map_err(codec_err)?;
        // All nonempty OPE ranges share the first word's high 24 bits.
        // The empty range is outside this subtree and is read separately.
        let nonempty_prefix = if codec.range_encoding().writes_ope() {
            let first = codec.storage_range(molecule, "\0").map_err(codec_err)?;
            format!("{prefix}{}", &first[..6])
        } else {
            prefix.clone()
        };
        let (skip, limit, after) = match window {
            KeyWindow::Offset { offset, limit } => (*offset, *limit, None),
            KeyWindow::After { after, limit } if after.hash.as_deref() == Some(hash) => {
                (0, *limit, Some(after.range.clone().unwrap_or_default()))
            }
            // An arbitrary cross-hash cursor uses the general comparator path.
            KeyWindow::After { .. } => return Ok(None),
        };
        // A range call may decode legacy sidecars. Match larger requested
        // pages without repeatedly decoding them, while bounding offset walks
        // and large internal callers to at most 1,024 tips per batch.
        let batch_limit = limit.clamp(128, 1024);
        let mut page = TipPageCursor {
            skip,
            after,
            output: Vec::new(),
            limit,
            include_tombstones,
        };
        while page.output.len() < limit {
            let want = page
                .skip
                .saturating_add(limit - page.output.len())
                .clamp(1, batch_limit);
            let (mut rows, exhausted) = self
                .read_partition_page(
                    &codec,
                    &prefix,
                    &nonempty_prefix,
                    page.after.as_deref(),
                    storage_prefix,
                    want,
                )
                .await?;
            if page.after.is_none() && codec.range_encoding().writes_ope() {
                if let Some(row) = self.get_per_key(&prefix, storage_prefix).await? {
                    rows.insert(0, (String::new(), row));
                }
            }
            page.consume(rows);
            if exhausted {
                break;
            }
        }
        Ok(Some(page.output))
    }

    /// Decode one bounded page. With a fixed encoding, storage and API ranges
    /// have the same order, so no cross-stream union or safe frontier is needed.
    async fn read_partition_page(
        &self,
        codec: &molecule_key_codec::MoleculeKeyCodec,
        prefix: &str,
        nonempty_prefix: &str,
        after: Option<&str>,
        storage_prefix: Option<&str>,
        want: usize,
    ) -> Result<(Vec<(String, TipRow)>, bool), SchemaError> {
        let writes_ope = codec.range_encoding().writes_ope();
        let uid = molecule_key_codec::molecule_uuid_from_storage_key(prefix)
            .expect("codec molecule prefix");
        let cursor = after
            .filter(|range| !range.is_empty() || !writes_ope)
            .map(|range| {
                codec
                    .storage_range(uid, range)
                    .map(|range| format!("{prefix}{range}"))
            })
            .transpose()
            .map_err(codec_err)?;
        let rows = self
            .scan_logical_tip_page(nonempty_prefix, cursor.as_deref(), storage_prefix, want)
            .await?;
        let exhausted = rows.len() < want;
        let rows = rows
            .into_iter()
            .map(|(key, record)| {
                let (_, range) = molecule_key_codec::decode_hash_range_any(&key)
                    .ok_or_else(|| SchemaError::InvalidData("invalid partition page key".into()))?;
                let range = if writes_ope {
                    crate::crypto::E2eKeys::ope_decode_range_plaintext(&range).unwrap_or(range)
                } else {
                    range
                };
                Ok((range, (key, record)))
            })
            .collect::<Result<_, SchemaError>>()?;
        Ok((rows, exhausted))
    }

    /// Merge bounded live and generation streams. Deleted slots advance the
    /// cursor, but never consume a result slot. No atom bodies are read here.
    pub(crate) async fn scan_logical_tip_page(
        &self,
        prefix: &str,
        after: Option<&str>,
        storage_prefix: Option<&str>,
        limit: usize,
    ) -> Result<Vec<TipRow>, SchemaError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let molecule = molecule_key_codec::molecule_uuid_from_storage_key(prefix)
            .ok_or_else(|| SchemaError::InvalidData("tip page needs a molecule prefix".into()))?;
        let changes = self.captured_tip_changes(storage_prefix, molecule)?;
        let pointer = self
            .active_molecule_generation(molecule, storage_prefix)
            .await?;
        let end = FilterUtils::create_prefix_end(prefix)?;
        let mut cursor = after.map(str::to_owned);
        let mut output = Vec::new();
        while output.len() < limit {
            let remaining = limit - output.len();
            let scan = TipScan {
                storage_prefix,
                start: cursor.as_deref().unwrap_or(prefix),
                end: &end,
                ask: remaining.saturating_add(usize::from(cursor.is_some())),
                cursor: cursor.as_deref(),
            };
            let mut rows = TipRows::new();
            let live_exhausted = self.merge_live_tip_rows(&scan, &mut rows).await?;
            let mut generation_exhausted = true;
            if let Some(pointer) = &pointer {
                generation_exhausted = self
                    .merge_generation_tip_rows(&scan, molecule, pointer, &mut rows)
                    .await?;
            }
            // Each stream supplies at least `remaining` new keys unless it is
            // exhausted. Only this common ordered frontier is safe to consume.
            let rows: Vec<_> = rows.into_iter().take(remaining).collect();
            let Some((last, _)) = rows.last() else { break };
            cursor = Some(last.clone());
            let deletions = self
                .generation_deletions(&rows, molecule, storage_prefix, pointer.is_some())
                .await?;
            let consumed = rows.len();
            for ((key, (live, base)), deletion) in rows.into_iter().zip(deletions) {
                let record = changes.get(&key).cloned().unwrap_or_else(|| {
                    Self::apply_generation_delete(Self::merge_generation_slot(live, base), deletion)
                });
                if let Some(record) = record {
                    output.push((key, record));
                }
            }
            if live_exhausted && generation_exhausted && consumed < remaining {
                break;
            }
        }
        Ok(output)
    }

    /// Add the generation stream to `rows`. Returns whether it is exhausted.
    async fn merge_generation_tip_rows(
        &self,
        scan: &TipScan<'_>,
        molecule: &str,
        pointer: &MoleculeGenerationPointer,
        rows: &mut TipRows,
    ) -> Result<bool, SchemaError> {
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
                &build_storage_key(scan.storage_prefix, &bound(scan.start)?),
                &build_storage_key(scan.storage_prefix, &bound(scan.end)?),
                scan.ask,
            )
            .await
            .map_err(|e| SchemaError::InvalidData(format!("generation tip page: {e}")))?;
        let exhausted = generated.len() < scan.ask;
        let strip = build_storage_key(scan.storage_prefix, "");
        for (key, slot) in generated {
            let key = molecule_key_codec::molecule_record_key_from_generation_key(
                molecule,
                &pointer.generation,
                key.strip_prefix(&strip).unwrap_or(&key),
            )
            .ok_or_else(|| SchemaError::InvalidData("invalid generation page key".into()))?;
            if scan.cursor.is_none_or(|cursor| key.as_str() > cursor) {
                rows.entry(key).or_default().1 = Some(slot);
            }
        }
        Ok(exhausted)
    }

    /// Read the generation delete marker of each row. Without a generation
    /// pointer no row can have one.
    async fn generation_deletions(
        &self,
        rows: &[(String, TipSlots)],
        molecule: &str,
        storage_prefix: Option<&str>,
        has_generation: bool,
    ) -> Result<Vec<Option<MoleculeGenerationDelete>>, SchemaError> {
        if !has_generation {
            return Ok(vec![None; rows.len()]);
        }
        let keys = rows
            .iter()
            .map(|(key, _)| {
                molecule_key_codec::molecule_generation_delete_bound_for_record_bound(molecule, key)
                    .map(|key| build_storage_key(storage_prefix, &key))
                    .ok_or_else(|| SchemaError::InvalidData("invalid generation delete key".into()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.main_store
            .get_items(&keys)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("generation page deletes: {e}")))
    }
}
