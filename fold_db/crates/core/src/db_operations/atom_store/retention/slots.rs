//! The purge side: the records of the slots that an erase removes.
//!
//! An erase must end the retention of every record of every slot it removes.
//! It releases up to [`RELEASE_RECORD_CAP`] records in its own batch, which
//! keeps the batch small beside the tips. For each slot that still holds
//! records after that, it writes one drain marker (`entry::DrainMarker`), due
//! at once, in the same batch. The janitor then releases the rest in bounded
//! pages. So no record of a removed slot waits for its window.
//!
//! The erase reads the directory of a slot inside the write gate, and a read
//! call may spend a fixed budget of pages and keys, deleted keys included. So
//! the erase reads the directories of at most [`MAX_SLOT_READS`] slots. A slot
//! after that, or after the cap of records is full, gets a marker and no read:
//! the janitor reads it, outside the gate, with a range walk that cannot fail
//! on deleted keys.

use super::entry::{DrainMarker, RetentionEntry, ENTRY_VERSION};
use super::keys;
use crate::atom::molecule_key_codec::header_key;
use crate::db_operations::atom_store::counter::{counts_of_header, ReleaseRequest};
use crate::db_operations::atom_store::AtomStore;
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

/// The most records one erase request releases in its own batch. The whole
/// request shares the cap, not each slot.
pub(crate) const RELEASE_RECORD_CAP: usize = 100;

/// The slots whose directory one erase reads in the write gate, at most. A
/// range read costs a few metadata pages of a budget of 4,096 for the whole
/// request, so the reads of a wide erase (one key of a wide schema has hundreds
/// of slots) would end the request with a read limit error. A slot beyond this
/// number gets a drain marker without a read.
pub(crate) const MAX_SLOT_READS: usize = 256;

/// Headers one batch read asks for, at most.
const HEADER_READ_CHUNK: usize = 10_000;
/// Rows one range read asks for while it looks for matching entries.
const SCAN_ROWS: usize = 64;
/// Rows a search may look at before it gives up and says "more may follow".
/// Entries of another slot (a range that holds a NUL) can sit between the
/// entries of this slot.
const SCAN_ROWS_MAX: usize = 512;

/// One removed slot, in storage form.
pub(crate) type RemovedSlots<'a> = (&'a str, &'a [(String, String)]);

/// What an erase adds to its batch for the kept records of its slots.
#[derive(Default)]
pub(crate) struct SlotReleases {
    /// Records to release in the batch of the erase.
    pub(crate) releases: Vec<ReleaseRequest>,
    /// Drain markers for the slots that hold more records.
    pub(crate) rows: Vec<(String, Value)>,
}

impl AtomStore {
    /// The releases and the drain markers for the records of the slots that
    /// one erase removes. The caller holds the write gate, so the reads see
    /// every accepted batch.
    ///
    /// The headers of the molecules say which of them hold records (`records`
    /// above zero). A molecule with none costs nothing more. For a molecule
    /// that holds records, a removed slot costs one range read under the hash
    /// of the molecule, for the first [`MAX_SLOT_READS`] slots while the cap of
    /// records has room. Any other slot costs one marker.
    pub(crate) async fn slot_release_requests(
        &self,
        scope: Option<&str>,
        removals: &[RemovedSlots<'_>],
    ) -> Result<SlotReleases, SchemaError> {
        let mut by_molecule: BTreeMap<&str, BTreeSet<&(String, String)>> = BTreeMap::new();
        for (molecule, slots) in removals {
            by_molecule
                .entry(molecule)
                .or_default()
                .extend(slots.iter());
        }
        let now = self.retention_state().clock_nanos();
        let names: Vec<&str> = by_molecule.keys().copied().collect();
        let with_records = self.molecules_with_records(scope, &names).await?;
        let mut entries: Vec<RetentionEntry> = Vec::new();
        // Two markers of one erase share the directory row of their bucket. The
        // map keeps the last row of a key, so the batch writes each key once.
        let mut rows: BTreeMap<String, Value> = BTreeMap::new();
        let mut markers = 0u64;
        let mut reads = 0usize;
        for (molecule, slots) in by_molecule {
            if !with_records.contains(molecule) {
                continue;
            }
            for (hash, range) in slots {
                let prefix = keys::slot_dir_prefix(scope, molecule, hash, range);
                let slot = keys::slot_key(scope, molecule, hash, range);
                if entries.len() >= RELEASE_RECORD_CAP || reads >= MAX_SLOT_READS {
                    // No room in this batch, or no read budget left: the
                    // janitor reads this slot.
                    rows.extend(self.drain_marker_rows(&prefix, &slot, now)?);
                    markers += 1;
                    continue;
                }
                reads += 1;
                let room = RELEASE_RECORD_CAP - entries.len();
                let (found, more) = self
                    .matching_entries(&prefix, &slot, room, u64::MAX)
                    .await?;
                entries.extend(found);
                if more {
                    rows.extend(self.drain_marker_rows(&prefix, &slot, now)?);
                    markers += 1;
                }
            }
        }
        if markers > 0 {
            tracing::warn!(
                slots = markers,
                cap = RELEASE_RECORD_CAP,
                "an erase passed the records of some slots to the janitor"
            );
            self.retention_state().note_capped_slots(markers);
        }
        Ok(SlotReleases {
            releases: entries.iter().map(RetentionEntry::request).collect(),
            rows: rows.into_iter().collect(),
        })
    }

    /// The molecules of `molecules` that may hold kept records: the header
    /// says `records` above zero, or says nothing that can be trusted (the
    /// counters are unset or the molecule is `suspect`). One batch read of the
    /// headers, by key, before the planner runs. A molecule whose header says
    /// `records` is zero costs nothing more.
    async fn molecules_with_records(
        &self,
        scope: Option<&str>,
        molecules: &[&str],
    ) -> Result<BTreeSet<String>, SchemaError> {
        let keys: Vec<String> = molecules
            .iter()
            .map(|molecule| build_storage_key(scope, &header_key(molecule)))
            .collect();
        let mut stored: Vec<Option<Value>> = Vec::with_capacity(keys.len());
        for chunk in keys.chunks(HEADER_READ_CHUNK) {
            stored.extend(
                self.main_store
                    .get_items::<Value>(chunk)
                    .await
                    .map_err(|error| {
                        SchemaError::InvalidData(format!("read molecule headers: {error}"))
                    })?,
            );
        }
        Ok(molecules
            .iter()
            .zip(stored)
            .filter(|(_, header)| {
                !header
                    .as_ref()
                    .and_then(counts_of_header)
                    .is_some_and(|counts| counts.records == 0 && !counts.suspect)
            })
            .map(|(molecule, _)| (*molecule).to_string())
            .collect())
    }

    /// The entry that one slot directory row holds, when it is a record of
    /// `slot` that was written at or before `created_max`.
    pub(super) fn slot_entry(
        &self,
        key: &[u8],
        value: &[u8],
        slot: &str,
        created_max: u64,
    ) -> Option<RetentionEntry> {
        let Ok(entry) = serde_json::from_slice::<RetentionEntry>(value) else {
            // An entry that does not decode names nothing to release. Its
            // record stays until its own window ends.
            tracing::error!(
                key = %String::from_utf8_lossy(key),
                "slot directory entry does not decode"
            );
            self.retention_state().note_undecodable();
            return None;
        };
        // A range that begins with the same bytes can belong to another slot
        // (a range that holds a NUL). Use an exact match only.
        (entry.slot == slot && entry.created <= created_max).then_some(entry)
    }

    /// The entries of one slot, at most `room`, that were written at or before
    /// `created_max`. The flag is true when more such entries may follow: more
    /// were seen, or the search looked at [`SCAN_ROWS_MAX`] rows and stopped.
    /// It is false only when the range of the slot has no more rows.
    async fn matching_entries(
        &self,
        dir_prefix: &str,
        slot: &str,
        room: usize,
        created_max: u64,
    ) -> Result<(Vec<RetentionEntry>, bool), SchemaError> {
        let (mut start, end) = keys::prefix_range(dir_prefix);
        let mut found = Vec::new();
        let mut examined = 0usize;
        loop {
            let rows = self
                .main_store
                .inner()
                .scan_range_paged(start.as_bytes(), end.as_bytes(), SCAN_ROWS)
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!("read slot directory: {error}"))
                })?;
            for (key, value) in &rows {
                examined += 1;
                let Some(entry) = self.slot_entry(key, value, slot, created_max) else {
                    continue;
                };
                if found.len() == room {
                    return Ok((found, true));
                }
                found.push(entry);
            }
            let Some((last, _)) = rows.last() else {
                return Ok((found, false));
            };
            if rows.len() < SCAN_ROWS {
                return Ok((found, false));
            }
            if examined >= SCAN_ROWS_MAX {
                return Ok((found, true));
            }
            start = format!("{}\0", String::from_utf8_lossy(last));
        }
    }
}

impl AtomStore {
    /// The release requests for rows of the slot directory of a molecule (any
    /// slot). A row that does not decode names nothing to release: it is counted
    /// and its record stays until its own window ends.
    pub(crate) fn directory_release_requests(
        &self,
        rows: &[(Vec<u8>, Vec<u8>)],
    ) -> Vec<ReleaseRequest> {
        rows.iter()
            .filter_map(|(key, value)| {
                let Ok(entry) = serde_json::from_slice::<RetentionEntry>(value) else {
                    tracing::error!(
                        key = %String::from_utf8_lossy(key),
                        "slot directory entry does not decode"
                    );
                    self.retention_state().note_undecodable();
                    return None;
                };
                Some(entry.request())
            })
            .collect()
    }

    /// The rows of the drain marker of one erased slot, due now: the marker in
    /// the bucket of now, and the directory row of that bucket.
    fn drain_marker_rows(
        &self,
        dir_prefix: &str,
        slot: &str,
        now: u64,
    ) -> Result<Vec<(String, Value)>, SchemaError> {
        let queue = self.retention_state().queue();
        let id = keys::drain_id(dir_prefix);
        let marker = DrainMarker {
            version: ENTRY_VERSION,
            drain: true,
            dir_prefix: dir_prefix.to_string(),
            slot: slot.to_string(),
            queue: queue.entry_key(now, &id),
            erased_at: now,
        };
        let value = serde_json::to_value(&marker).map_err(|error| {
            SchemaError::InvalidData(format!("serialize drain marker: {error}"))
        })?;
        Ok(queue.rows(now, &id, &value))
    }
}
