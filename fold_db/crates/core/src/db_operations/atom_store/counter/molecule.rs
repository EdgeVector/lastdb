//! The store side of the molecule counters: read the headers, plan, merge.
//!
//! One planner run changes the tips and the conflict records of some molecules.
//! This module turns that change into the counters of each molecule header and
//! puts the header rows in the same batch as the tips. It reads the old headers
//! by key (one batch read, inside the write gate) and never scans.
//!
//! Hook for the recorder (step S3): a conflict record offered to the planner as a
//! [`super::RecordRefs`] carries its molecule. When the record is new, the planner
//! raises `records` of that molecule in the batch of the record. A step that
//! releases records adds its removals to [`molecule_changes`] the same way.

use super::commit::RecordRefs;
use super::molecule_plan::{plan_molecule, Before, Change, MoleculeCounts, MoleculeFault};
use crate::atom::molecule_key_codec::{self, header_key, molecule_counter_row, MoleculeCounterRow};
use crate::db_operations::atom_store::{AtomStore, PerKeyRecord};
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};

/// The native get-many API accepts at most this many keys per call.
const READ_CHUNK: usize = 10_000;

const SLOTS: &str = "slots";
const RECORDS: &str = "records";
const SUSPECT: &str = "suspect";

/// What one run found wrong with the molecule counters.
#[derive(Debug, Default)]
pub(super) struct MoleculeRun {
    pub faults: Vec<(String, MoleculeFault)>,
    /// Each header row this run writes, with its stored and written length.
    pub headers: Vec<HeaderChange>,
}

/// One molecule header row a run writes, for the keep-small meters.
#[derive(Debug, Clone)]
pub(crate) struct HeaderChange {
    pub molecule: String,
    /// Length of the stored row, 0 when the run creates it.
    pub old_bytes: u64,
    pub new_bytes: u64,
}

/// The counters a header carries, or `None` when it carries none.
#[must_use]
pub fn counts_of_header(header: &Value) -> Option<MoleculeCounts> {
    let slots = header.get(SLOTS)?.as_u64()?;
    Some(MoleculeCounts {
        slots,
        records: header.get(RECORDS).and_then(Value::as_u64).unwrap_or(0),
        suspect: header
            .get(SUSPECT)
            .and_then(Value::as_bool)
            .unwrap_or(false),
    })
}

fn write_counts(header: &mut Map<String, Value>, counts: MoleculeCounts) {
    header.insert(SLOTS.into(), json!(counts.slots));
    header.insert(RECORDS.into(), json!(counts.records));
    if counts.suspect {
        header.insert(SUSPECT.into(), json!(true));
    } else {
        header.remove(SUSPECT);
    }
}

/// The net change of slots and records for each molecule of one run.
///
/// A tip that is new adds a slot. A tip that replaces an old tip changes
/// nothing. A tip that is removed takes one away. A conflict record that is new
/// adds one record to its molecule.
pub(super) fn molecule_changes(
    tips: &BTreeMap<String, Option<PerKeyRecord>>,
    old: &HashMap<String, PerKeyRecord>,
    records: &[RecordRefs],
    new_records: &[bool],
) -> BTreeMap<String, Change> {
    let mut changes: BTreeMap<String, Change> = BTreeMap::new();
    for (key, new) in tips {
        let Some(molecule) = molecule_key_codec::molecule_uuid_from_storage_key(key) else {
            continue;
        };
        let delta = i64::from(new.is_some()) - i64::from(old.contains_key(key));
        if delta != 0 {
            changes.entry(molecule.to_string()).or_default().slots += delta;
        }
    }
    for (record, is_new) in records.iter().zip(new_records) {
        if *is_new {
            changes.entry(record.molecule.clone()).or_default().records += 1;
        }
    }
    changes.retain(|_, change| !change.is_zero());
    changes
}

/// The rows of the run that are molecule headers of `scope`, by molecule.
fn header_rows_of(scope: Option<&str>, rows: &[(String, Value)]) -> BTreeMap<String, Vec<usize>> {
    let mut found: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for (at, (key, _)) in rows.iter().enumerate() {
        let Some((MoleculeCounterRow::Structure, molecule)) = molecule_counter_row(key) else {
            continue;
        };
        if *key == build_storage_key(scope, &header_key(molecule)) {
            found.entry(molecule.to_string()).or_default().push(at);
        }
    }
    found
}

impl AtomStore {
    /// Merge the molecule counters into the header rows of one planner run.
    ///
    /// A header row that the caller built carries the version and the time. This
    /// call adds the counters that the stored header holds plus the change of
    /// this run. A molecule whose counters change and that has no row in the run
    /// gets a header row made from the stored header. The returned rows replace
    /// the rows of the run.
    pub(super) async fn apply_molecule_counters(
        &self,
        scope: Option<&str>,
        changes: &BTreeMap<String, Change>,
        mut rows: Vec<(String, Value)>,
    ) -> Result<(Vec<(String, Value)>, MoleculeRun), SchemaError> {
        let given = header_rows_of(scope, &rows);
        let molecules: BTreeSet<&String> = changes.keys().chain(given.keys()).collect();
        let mut run = MoleculeRun::default();
        if molecules.is_empty() {
            return Ok((rows, run));
        }
        let keys: Vec<String> = molecules
            .iter()
            .map(|molecule| build_storage_key(scope, &header_key(molecule)))
            .collect();
        let stored = self.read_header_values(&keys).await?;
        for ((molecule, key), stored) in molecules.into_iter().zip(keys).zip(stored) {
            let change = changes.get(molecule).copied().unwrap_or_default();
            let before = Before {
                header_present: stored.is_some(),
                counts: stored.as_ref().and_then(counts_of_header),
            };
            let after = plan_molecule(&before, &change);
            let old_bytes = stored.as_ref().map_or(0, |value| {
                crate::db_operations::keep_small::molecule_counter_row_bytes(value).unwrap_or(0)
            });
            if let Some(fault) = after.fault {
                run.faults.push((molecule.clone(), fault));
            }
            let at = given.get(molecule);
            let base = at
                .and_then(|at| at.last())
                .map(|&at| rows[at].1.clone())
                .or(stored)
                .unwrap_or_else(|| json!({ "version": 0, "updated_at": chrono::Utc::now() }));
            let Value::Object(mut header) = base else {
                return Err(SchemaError::InvalidData(format!(
                    "molecule header of {molecule} is not an object"
                )));
            };
            write_counts(&mut header, after.counts);
            let merged = Value::Object(header);
            run.headers.push(HeaderChange {
                molecule: molecule.clone(),
                old_bytes,
                new_bytes: crate::db_operations::keep_small::molecule_counter_row_bytes(&merged)
                    .unwrap_or(0),
            });
            match at {
                Some(at) => {
                    for &at in at {
                        rows[at].1 = merged.clone();
                    }
                }
                None => rows.push((key, merged)),
            }
        }
        Ok((rows, run))
    }

    /// The stored header of each key, as raw JSON, in the order given.
    async fn read_header_values(&self, keys: &[String]) -> Result<Vec<Option<Value>>, SchemaError> {
        let mut out = Vec::with_capacity(keys.len());
        for chunk in keys.chunks(READ_CHUNK) {
            out.extend(
                self.main_store
                    .get_items::<Value>(chunk)
                    .await
                    .map_err(|error| {
                        SchemaError::InvalidData(format!("read molecule headers: {error}"))
                    })?,
            );
        }
        Ok(out)
    }
}
