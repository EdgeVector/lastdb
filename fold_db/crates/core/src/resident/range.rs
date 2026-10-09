//! Range fill of one hash's ordered molecule view from [`laststore::LastStore::load_hash`].
//!
//! Tests call this path. Product range listing is unchanged. This module does
//! not import `Shard` and does not read `LASTDB_LOGICAL_RESIDENT_SET`.

use std::collections::{BTreeMap, HashSet};
use std::ops::Bound;

use super::logical_set::{HashCompleteness, LogicalResidentSet, MoleculeId, RangeNotResident, Tip};
use crate::atom::{molecule_key_codec, AtomEntry};
use laststore::{collections, LoadedTip};

/// Loader that copies one hash prefix and drops each unpinned group.
pub trait HashLoader {
    /// Disk and pin records under `prefix`. Tombstones are not applied.
    fn load_hash(&self, collection: &str, prefix: &[u8]) -> laststore::Result<Vec<LoadedTip>>;
}

#[allow(clippy::use_self)]
impl HashLoader for laststore::LastStore {
    fn load_hash(&self, collection: &str, prefix: &[u8]) -> laststore::Result<Vec<LoadedTip>> {
        laststore::LastStore::load_hash(self, collection, prefix)
    }
}

/// Owned page of live `(range, tip)` pairs after merge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangePage {
    /// Ordered live tips under the requested bounds.
    pub entries: Vec<(String, Tip)>,
}

/// Failure from the loader, a gate rejection, or a tip body that is not an
/// atom entry.
#[derive(Debug)]
pub enum RangeAdmitError {
    /// [`HashLoader::load_hash`] failed, including [`laststore::Error::UnanchoredRead`].
    Loader(laststore::Error),
    /// The in-memory view is not complete and this call did not fill.
    NotResident(RangeNotResident),
    /// Tip bytes were not a molecule per-key record or a bare atom entry.
    InvalidTipBody(String),
}

impl From<laststore::Error> for RangeAdmitError {
    fn from(err: laststore::Error) -> Self {
        Self::Loader(err)
    }
}

impl From<RangeNotResident> for RangeAdmitError {
    fn from(err: RangeNotResident) -> Self {
        Self::NotResident(err)
    }
}

impl std::fmt::Display for RangeAdmitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Loader(err) => write!(f, "hash loader: {err}"),
            Self::NotResident(err) => write!(f, "{err}"),
            Self::InvalidTipBody(msg) => write!(f, "invalid tip body: {msg}"),
        }
    }
}

impl std::error::Error for RangeAdmitError {}

impl LogicalResidentSet {
    /// Fill one hash from the loader, then return a bounded page.
    ///
    /// A `Complete` view does not open a group. This call admits only the
    /// keys in the returned page. A full read may mark the hash complete
    /// when every live key is still warm after the purge. A short page does
    /// not. The page comes from this load even when the hash stays incomplete.
    pub fn get_range<L: HashLoader>(
        &mut self,
        loader: &L,
        molecule: MoleculeId,
        hash: &str,
        start: Bound<&str>,
        end: Bound<&str>,
        limit: usize,
    ) -> Result<RangePage, RangeAdmitError> {
        if matches!(
            self.hash_completeness(molecule, hash),
            HashCompleteness::Complete { .. }
        ) {
            return self.range_page_with_holds(molecule, hash, start, end, limit);
        }

        let prefix = hash_scan_prefix(molecule, hash);
        let loaded = loader.load_hash(collections::TIPS, prefix.as_bytes())?;
        let mut loaded_bodies = std::collections::HashMap::new();
        for row in &loaded {
            let Some((row_hash, range)) = decode_tip_key(molecule, &row.storage_key) else {
                continue;
            };
            if row_hash != hash {
                continue;
            }
            if let Some(body) = row.body.as_ref() {
                loaded_bodies.insert(range, body.clone());
            }
        }
        let merged = self.merge_hash_rows(molecule, hash, loaded)?;
        let page = page_tips(&merged, start, end, limit);
        for (range, tip) in &page {
            if let Some(body) = loaded_bodies.get(range) {
                self.store_tip_body(molecule, hash, range, body.clone());
            }
            if self.tip(molecule, hash, range).is_some() {
                self.hold_tip(molecule, hash, range);
            } else {
                self.admit_tip(molecule, hash.to_string(), range.clone(), tip.clone());
            }
            self.release_tip(molecule, hash, range);
        }

        let live_ranges: Vec<String> = merged.keys().cloned().collect();
        let returned: Vec<String> = page.iter().map(|(range, _)| range.clone()).collect();
        let returned_every_live_key = !live_ranges.is_empty() && returned == live_ranges;
        // The complete mark is shared with the product hash read. That read
        // serves stored bodies and skips a key with no body. A tip with no
        // body, dirty or clean, must not certify the hash. This page still
        // returns that tip.
        let every_key_stays = returned.iter().all(|range| {
            self.tip(molecule, hash, range).is_some()
                && self.tip_body(molecule, hash, range).is_some()
        });
        if returned_every_live_key && every_key_stays {
            self.mark_hash_complete(molecule, hash, live_ranges);
        }
        Ok(RangePage { entries: page })
    }

    fn merge_hash_rows(
        &self,
        molecule: MoleculeId,
        hash: &str,
        loaded: Vec<LoadedTip>,
    ) -> Result<BTreeMap<String, Tip>, RangeAdmitError> {
        let dirty_tips = self.tips_for_hash(molecule, hash);
        let dirty_tombstones: HashSet<String> = self
            .tombstone_ranges_for_hash(molecule, hash)
            .into_iter()
            .collect();

        let mut merged = BTreeMap::new();
        for row in loaded {
            let Some((row_hash, range)) = decode_tip_key(molecule, &row.storage_key) else {
                continue;
            };
            if row_hash != hash {
                continue;
            }
            let Some(body) = row.body.as_deref() else {
                continue;
            };
            merged.insert(range, tip_from_body(body)?);
        }
        for (range, tip) in dirty_tips {
            if dirty_tombstones.contains(&range) {
                continue;
            }
            merged.insert(range, tip);
        }
        for range in &dirty_tombstones {
            merged.remove(range);
        }
        Ok(merged)
    }

    fn range_page_with_holds(
        &mut self,
        molecule: MoleculeId,
        hash: &str,
        start: Bound<&str>,
        end: Bound<&str>,
        limit: usize,
    ) -> Result<RangePage, RangeAdmitError> {
        let entries = self.range_page(molecule, hash, start, end, limit)?;
        let held: Vec<String> = entries.iter().map(|(range, _)| range.clone()).collect();
        for range in &held {
            self.hold_tip(molecule, hash, range);
        }
        self.release_range_holds(molecule, hash, &held);
        Ok(RangePage { entries })
    }

    fn release_range_holds(&mut self, molecule: MoleculeId, hash: &str, ranges: &[String]) {
        for range in ranges {
            self.release_tip_hold(molecule, hash, range);
        }
    }
}

fn page_tips(
    merged: &BTreeMap<String, Tip>,
    start: Bound<&str>,
    end: Bound<&str>,
    limit: usize,
) -> Vec<(String, Tip)> {
    if limit == 0 {
        return Vec::new();
    }
    merged
        .range::<str, _>((start, end))
        .take(limit)
        .map(|(range, tip)| (range.clone(), tip.clone()))
        .collect()
}

fn hash_scan_prefix(molecule: MoleculeId, hash: &str) -> String {
    molecule_key_codec::hash_range_scan_prefix_for_hash(&molecule.storage_spelling(), hash)
}

fn decode_tip_key(molecule: MoleculeId, storage_key: &str) -> Option<(String, String)> {
    molecule_key_codec::decode_hash_range(storage_key, &molecule.storage_spelling())
}

fn tip_from_body(body: &[u8]) -> Result<Tip, RangeAdmitError> {
    let value: serde_json::Value = serde_json::from_slice(body)
        .map_err(|err| RangeAdmitError::InvalidTipBody(err.to_string()))?;
    let entry_value = value.get("entry").cloned().unwrap_or(value);
    let entry: AtomEntry = serde_json::from_value(entry_value)
        .map_err(|err| RangeAdmitError::InvalidTipBody(err.to_string()))?;
    Ok(Tip {
        atom: super::logical_set::AtomId::new(entry.atom_uuid),
        written_at: entry.written_at,
        logical_counter: entry.logical_counter,
        device_id: entry.device_id,
        mutation_uuid: entry.mutation_uuid,
    })
}
