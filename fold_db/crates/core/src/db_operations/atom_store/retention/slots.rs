//! The purge side: the records of the slots that an erase removes.
//!
//! An erase must end the retention of every record of every slot it removes.
//! It releases up to [`RELEASE_RECORD_CAP`] records in its own batch, which
//! keeps the batch small beside the tips. For each slot that still holds
//! records after that, it writes one drain marker (`entry::DrainMarker`), due
//! at once, in the same batch. The janitor then releases the rest in bounded
//! pages. So no record of a removed slot waits for its window.

use super::entry::{DrainMarker, RetentionEntry, ENTRY_VERSION};
use super::keys;
use crate::db_operations::atom_store::counter::ReleaseRequest;
use crate::db_operations::atom_store::AtomStore;
use crate::schema::SchemaError;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

/// The most records one erase request releases in its own batch. The whole
/// request shares the cap, not each slot.
pub(crate) const RELEASE_RECORD_CAP: usize = 100;

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
    /// A molecule with no directory entry costs one read that finds nothing.
    /// For a molecule with entries, each removed slot costs one range read
    /// under the hash of the molecule.
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
        let mut entries: Vec<RetentionEntry> = Vec::new();
        let mut rows = Vec::new();
        for (molecule, slots) in by_molecule {
            if !self.molecule_has_directory(scope, molecule).await? {
                continue;
            }
            for (hash, range) in slots {
                let prefix = keys::slot_dir_prefix(scope, molecule, hash, range);
                let slot = keys::slot_key(scope, molecule, hash, range);
                let room = RELEASE_RECORD_CAP.saturating_sub(entries.len());
                let (found, more) = self
                    .matching_entries(&prefix, &slot, room, u64::MAX)
                    .await?;
                entries.extend(found);
                if more {
                    rows.push(drain_marker_row(&prefix, &slot, now)?);
                }
            }
        }
        if !rows.is_empty() {
            tracing::warn!(
                slots = rows.len(),
                cap = RELEASE_RECORD_CAP,
                "an erase passed the records of some slots to the janitor"
            );
            self.retention_state().note_capped_slots(rows.len() as u64);
        }
        Ok(SlotReleases {
            releases: entries.iter().map(RetentionEntry::request).collect(),
            rows,
        })
    }

    /// Whether the molecule has any directory entry. One range read, limit one.
    async fn molecule_has_directory(
        &self,
        scope: Option<&str>,
        molecule: &str,
    ) -> Result<bool, SchemaError> {
        let (start, end) = keys::prefix_range(&keys::molecule_dir_prefix(scope, molecule));
        let rows = self
            .main_store
            .inner()
            .scan_range_paged(start.as_bytes(), end.as_bytes(), 1)
            .await
            .map_err(|error| SchemaError::InvalidData(format!("probe slot directory: {error}")))?;
        Ok(!rows.is_empty())
    }

    /// The entries of one slot, at most `room`, that were written at or before
    /// `created_max`. The flag is true when more such entries may follow: more
    /// were seen, or the search looked at [`SCAN_ROWS_MAX`] rows and stopped.
    /// It is false only when the range of the slot has no more rows.
    pub(super) async fn matching_entries(
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
                let Ok(entry) = serde_json::from_slice::<RetentionEntry>(value) else {
                    // An entry that does not decode names nothing to release.
                    // Its record stays until its own window ends.
                    tracing::error!(
                        key = %String::from_utf8_lossy(key),
                        "slot directory entry does not decode"
                    );
                    self.retention_state().note_undecodable();
                    continue;
                };
                // A range that begins with the same bytes can belong to another
                // slot (a range that holds a NUL). Use an exact match only.
                if entry.slot != slot || entry.created > created_max {
                    continue;
                }
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

/// The drain marker row of one erased slot, due now.
fn drain_marker_row(
    dir_prefix: &str,
    slot: &str,
    now: u64,
) -> Result<(String, Value), SchemaError> {
    let queue = keys::queue_key(now, &keys::drain_id(dir_prefix));
    let marker = DrainMarker {
        version: ENTRY_VERSION,
        drain: true,
        dir_prefix: dir_prefix.to_string(),
        slot: slot.to_string(),
        queue: queue.clone(),
        erased_at: now,
    };
    let value = serde_json::to_value(&marker)
        .map_err(|error| SchemaError::InvalidData(format!("serialize drain marker: {error}")))?;
    Ok((queue, value))
}
