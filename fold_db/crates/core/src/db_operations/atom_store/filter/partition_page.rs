//! Bounded ordered tip reads, including immutable generation overlays.

use std::collections::BTreeMap;

use crate::atom::molecule_key_codec;
use crate::schema::types::field::{build_storage_key, FilterUtils, HashRangeFilter, KeyWindow};
use crate::schema::SchemaError;

use super::super::types::{
    MoleculeGenerationDelete, MoleculeGenerationPointer, MoleculeGenerationSlot, PerKeyRecord,
};
use super::super::AtomStore;

mod live;

use super::HashTipSelection;

type TipRow = (String, PerKeyRecord);
type TipSlots = (Option<PerKeyRecord>, Option<MoleculeGenerationSlot>);
type TipRows = BTreeMap<String, TipSlots>;

/// One independently ordered stream of tips under a hash.
struct TipLane {
    prefix: String,
    start: String,
    end: String,
}

/// Paging state shared by the rounds of `scan_hash_tip_window`.
struct TipPageCursor {
    skip: usize,
    after: Option<String>,
    output: Vec<TipRow>,
    limit: usize,
    include_tombstones: bool,
}

impl TipPageCursor {
    /// Consume merged rows up to the common frontier. Returns whether any
    /// row advanced the cursor.
    fn consume(
        &mut self,
        codec: &molecule_key_codec::MoleculeKeyCodec,
        union: BTreeMap<String, TipRow>,
        frontier: Option<&String>,
    ) -> Result<bool, SchemaError> {
        let mut advanced = false;
        for (range, row) in union {
            if frontier.is_some_and(|end| range > *end) {
                break;
            }
            self.after = Some(codec.api_range(&range).map_err(codec_err)?);
            advanced = true;
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
        Ok(advanced)
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

/// Each current molecule has one fixed OPE key. Its first encoded word
/// identifies the ordered nonempty range stream; empty ranges stay separate.
fn hash_tip_lanes(
    codec: &molecule_key_codec::MoleculeKeyCodec,
    prefixes: &[String],
    selection: &HashTipSelection<'_>,
) -> Result<Vec<TipLane>, SchemaError> {
    prefixes
        .iter()
        .map(|prefix| {
            let lane = if codec.range_encoding().writes_ope() {
                let uid = molecule_key_codec::molecule_uuid_from_storage_key(prefix)
                    .expect("codec molecule prefix");
                let first = codec.storage_range(uid, "\0").map_err(codec_err)?;
                format!("{prefix}{}", &first[..6])
            } else {
                prefix.clone()
            };
            let lane_end = FilterUtils::create_prefix_end(&lane)?;
            let start = lane.max(format!("{prefix}{}", selection.range_start));
            let end = match &selection.range_end {
                Some(end) => format!("{prefix}{end}"),
                None => FilterUtils::create_prefix_end(&format!(
                    "{prefix}{}",
                    selection.range_prefix.as_deref().unwrap_or("")
                ))?,
            };
            Ok(TipLane {
                prefix: prefix.clone(),
                start,
                end: end.min(lane_end),
            })
        })
        .collect()
}

impl AtomStore {
    /// Select a bounded hash/range page from ordered encrypted-key streams.
    pub(crate) async fn scan_hash_tip_window(
        &self,
        molecule: &str,
        filter: &HashRangeFilter,
        storage_prefix: Option<&str>,
        window: &KeyWindow,
        include_tombstones: bool,
    ) -> Result<Option<Vec<TipRow>>, SchemaError> {
        let codec = self.key_codec_for_molecule(molecule)?;
        let Some(selection) = HashTipSelection::new(&codec, molecule, filter, window)? else {
            return Ok(None);
        };
        if selection.empty {
            return Ok(Some(Vec::new()));
        }
        if let Some(rows) = self
            .preloaded_window(molecule, &selection, storage_prefix, include_tombstones)
            .await?
        {
            return Ok(Some(rows));
        }
        let prefix = codec
            .api_hash_range_scan_prefix_for_hash(molecule, selection.api_hash)
            .map_err(codec_err)?;
        let prefixes = [prefix];
        let limit = selection.limit;
        // A range call may decode legacy sidecars. Match larger requested
        // pages without repeatedly decoding them, while bounding offset walks
        // and large internal callers to at most 1,024 tips per stream.
        let batch_limit = limit.clamp(128, 1024);
        let lanes = hash_tip_lanes(&codec, &prefixes, &selection)?;
        let mut page = TipPageCursor {
            skip: selection.skip,
            after: selection.after.clone(),
            output: Vec::new(),
            limit,
            include_tombstones,
        };
        while page.output.len() < limit {
            let want = page
                .skip
                .saturating_add(limit - page.output.len())
                .clamp(1, batch_limit);
            let mut union = BTreeMap::new();
            if page.after.is_none()
                && selection.accepts_empty()
                && codec.range_encoding().writes_ope()
            {
                self.seed_empty_range_rows(&prefixes, storage_prefix, &mut union)
                    .await?;
            }
            let mut frontier: Option<String> = None;
            for lane in &lanes {
                let last = self
                    .merge_lane_page(
                        &codec,
                        lane,
                        page.after.as_deref(),
                        storage_prefix,
                        want,
                        &mut union,
                    )
                    .await?;
                if let Some(last) = last {
                    frontier = Some(frontier.map_or(last.clone(), |current| current.min(last)));
                }
            }
            let advanced = page.consume(&codec, union, frontier.as_ref())?;
            if !advanced || frontier.is_none() {
                break;
            }
        }
        Ok(Some(page.output))
    }

    /// Empty ranges are intentionally not OPE encoded, so no lane sees them.
    async fn seed_empty_range_rows(
        &self,
        prefixes: &[String],
        storage_prefix: Option<&str>,
        union: &mut BTreeMap<String, TipRow>,
    ) -> Result<(), SchemaError> {
        for prefix in prefixes {
            if let Some(row) = self.get_per_key(prefix, storage_prefix).await? {
                union.entry(String::new()).or_insert(row);
            }
        }
        Ok(())
    }

    /// Read one page of one lane into `union`. Returns the last range of the
    /// page when the page was full, so the caller can bound the safe frontier.
    async fn merge_lane_page(
        &self,
        codec: &molecule_key_codec::MoleculeKeyCodec,
        lane: &TipLane,
        after: Option<&str>,
        storage_prefix: Option<&str>,
        want: usize,
        union: &mut BTreeMap<String, TipRow>,
    ) -> Result<Option<String>, SchemaError> {
        let writes_ope = codec.range_encoding().writes_ope();
        let uid = molecule_key_codec::molecule_uuid_from_storage_key(&lane.prefix)
            .expect("codec molecule prefix");
        let cursor = after
            .filter(|range| !range.is_empty() || !writes_ope)
            .map(|range| {
                codec
                    .storage_range(uid, range)
                    .map(|range| format!("{}{range}", lane.prefix))
            })
            .transpose()
            .map_err(codec_err)?;
        let rows = self
            .scan_logical_tip_range(
                &lane.start,
                &lane.end,
                cursor.as_deref(),
                storage_prefix,
                want,
            )
            .await?;
        let full = rows.len() == want;
        let mut last = None;
        for (key, record) in rows {
            let (_, range) = molecule_key_codec::decode_hash_range_any(&key)
                .ok_or_else(|| SchemaError::InvalidData("invalid partition page key".into()))?;
            // The stream frontier uses the ordered stored payload. API text
            // order differs for typed integers and other scalar profiles.
            codec.api_range(&range).map_err(codec_err)?;
            last = Some(range.clone());
            union.entry(range).or_insert((key, record));
        }
        Ok(if full { last } else { None })
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
        let end = FilterUtils::create_prefix_end(prefix)?;
        self.scan_logical_tip_range(prefix, &end, after, storage_prefix, limit)
            .await
    }

    /// Both physical streams share the selector's half-open bounds. A later
    /// cursor is exclusive; the initial lower bound remains inclusive.
    async fn scan_logical_tip_range(
        &self,
        start: &str,
        end: &str,
        after: Option<&str>,
        storage_prefix: Option<&str>,
        limit: usize,
    ) -> Result<Vec<TipRow>, SchemaError> {
        if limit == 0 || start >= end || after.is_some_and(|after| after >= end) {
            return Ok(Vec::new());
        }
        let molecule = molecule_key_codec::molecule_uuid_from_storage_key(start)
            .ok_or_else(|| SchemaError::InvalidData("tip page needs a molecule prefix".into()))?;
        let changes = self.captured_tip_changes(storage_prefix, molecule)?;
        let pointer = self
            .active_molecule_generation(molecule, storage_prefix)
            .await?;
        let mut cursor = after.map(str::to_owned);
        let mut output = Vec::new();
        while output.len() < limit {
            let remaining = limit - output.len();
            let scan = TipScan {
                storage_prefix,
                start: cursor.as_deref().map_or(start, |cursor| start.max(cursor)),
                end,
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
