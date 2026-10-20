//! Conflict records in the planner: new records bring rows, released records
//! lower references.
//!
//! A conflict record is a history row. It holds one reference to each distinct
//! atom it names for as long as the row exists (invariant I1). Two things change
//! a record in a planner run:
//!
//! - A record that is new adds its references. The planner also writes the
//!   rows that only a new record writes (its retention queue entry and its slot
//!   directory entry), so a repeat of one decision adds nothing.
//! - A record that is released lowers its references. The planner reads the
//!   history row in the same write gate hold, takes the atoms from that row, and
//!   deletes the row in the same batch (invariant I3). A caller names the row,
//!   never the atoms. A release of a row that is already gone lowers nothing.
//!   A row that does not decode is kept, with its references, and the fault is
//!   reported: a leak is safe, a wrong decrement is not.

use super::commit::{RecordRefs, ReleaseRequest};
use super::refs::event_atoms;
use crate::atom::MutationEvent;
use crate::db_operations::atom_store::conflict_record::is_counted_record_key;
use crate::db_operations::atom_store::AtomStore;
use crate::schema::SchemaError;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

const READ_CHUNK: usize = 10_000;

/// The records of one run, once for each key. The first one of a key stays.
pub(super) fn dedupe_records(records: Vec<RecordRefs>) -> Vec<RecordRefs> {
    let mut seen = BTreeSet::new();
    records
        .into_iter()
        .filter(|record| seen.insert(record.key.clone()))
        .collect()
}

/// Add the rows of each new record to the rows of the batch.
pub(super) fn with_new_record_rows(
    mut rows: Vec<(String, Value)>,
    records: &[RecordRefs],
    new_records: &[bool],
) -> Vec<(String, Value)> {
    for (record, is_new) in records.iter().zip(new_records) {
        if *is_new {
            rows.extend(record.new_rows.iter().cloned());
        }
    }
    rows
}

/// What the planner found for the releases of one run.
#[derive(Default)]
pub(super) struct Released {
    /// The atoms of each released history row, as the row names them.
    atoms: Vec<Vec<String>>,
    /// The history rows and the rows that go with them, to delete.
    deletes: Vec<Vec<u8>>,
}

impl Released {
    /// The number of history rows this run releases.
    pub(super) fn count(&self) -> u64 {
        self.atoms.len() as u64
    }

    /// One reference less for each atom of each released row.
    pub(super) fn lower(&self, deltas: &mut BTreeMap<String, i64>) {
        for atoms in &self.atoms {
            for atom in atoms {
                *deltas.entry(atom.clone()).or_default() -= 1;
            }
        }
        deltas.retain(|_, delta| *delta != 0);
    }

    /// The keys the batch deletes for the releases.
    pub(super) fn into_deletes(self) -> Vec<Vec<u8>> {
        self.deletes
    }
}

impl AtomStore {
    /// Tell the verifier which atoms the releases of this batch change, while a
    /// run reads. One atomic load when no run reads.
    pub(super) fn note_released_atoms(&self, released: &Released) {
        let watch = self.counter_verify().watch();
        if watch.is_active() {
            watch.note(released.atoms.iter().flatten().map(String::as_str));
        }
    }

    /// Read the history row of each release in the write gate and decide what
    /// the batch lowers and deletes.
    pub(super) async fn read_released_records(
        &self,
        releases: Vec<ReleaseRequest>,
    ) -> Result<Released, SchemaError> {
        let mut seen = BTreeSet::new();
        let releases: Vec<ReleaseRequest> = releases
            .into_iter()
            .filter(|release| seen.insert(release.history.clone()))
            .collect();
        let mut out = Released::default();
        for chunk in releases.chunks(READ_CHUNK) {
            let keys: Vec<Vec<u8>> = chunk
                .iter()
                .map(|release| release.history.as_bytes().to_vec())
                .collect();
            let stored = self
                .main_store
                .inner()
                .get_many(keys)
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!("read released rows: {error}"))
                })?;
            for (release, bytes) in chunk.iter().zip(stored) {
                if !is_counted_record_key(&release.history) {
                    // Only a counted record holds references (invariant I1). A
                    // release of any other row would lower a count that this
                    // row never raised.
                    tracing::error!(history = %release.history, "release names a row that holds no references; nothing released");
                    continue;
                }
                let Some(bytes) = bytes else {
                    // The row is gone already. Its dependent rows still go.
                    out.deletes.extend(release.deletes.iter().cloned());
                    continue;
                };
                match serde_json::from_slice::<MutationEvent>(&bytes) {
                    Ok(event) => {
                        out.atoms.push(event_atoms(&event));
                        out.deletes.push(release.history.as_bytes().to_vec());
                        out.deletes.extend(release.deletes.iter().cloned());
                    }
                    Err(error) => {
                        // Keep the row and its references. The queue entry stays
                        // too, so the row is not lost to every later pass.
                        tracing::error!(
                            history = %release.history,
                            %error,
                            "released history row does not decode; the row and its references stay"
                        );
                        self.retention_state().note_undecodable();
                    }
                }
            }
        }
        Ok(out)
    }
}
