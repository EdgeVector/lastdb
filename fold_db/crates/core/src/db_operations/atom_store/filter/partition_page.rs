//! Bounded ordered tip reads, including immutable generation overlays.

use std::collections::BTreeMap;

use crate::atom::molecule_key_codec;
use crate::schema::types::field::{build_storage_key, FilterUtils};
use crate::schema::SchemaError;

use super::super::AtomStore;
use crate::records::molecule_record::{
    MoleculeGenerationDelete, MoleculeGenerationPointer, MoleculeGenerationSlot, PerKeyRecord,
};

mod live;

type TipRow = (String, PerKeyRecord);
type TipSlots = (Option<PerKeyRecord>, Option<MoleculeGenerationSlot>);
type TipRows = BTreeMap<String, TipSlots>;

/// Scan bounds for one round of `scan_logical_tip_page`.
struct TipScan<'a> {
    storage_prefix: Option<&'a str>,
    start: &'a str,
    end: &'a str,
    ask: usize,
    cursor: Option<&'a str>,
}

impl AtomStore {
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
                let record = crate::records::molecule_record::apply_generation_delete(
                    crate::records::molecule_record::merge_generation_slot(live, base),
                    deletion,
                );
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
