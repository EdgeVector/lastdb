//! The `bindings` counter of a molecule, and the transitions that change it.
//!
//! A source binds a molecule through one binding record (`molecule_ref_edges.rs`).
//! The counter says how many records exist for the molecule in one scope. A bind
//! and an unbind are state-based: each reads which binding records exist, flips
//! only the ones that need it, and writes the flipped records and the new counter
//! in one batch inside the native write gate. The same bind twice changes
//! nothing. A crash keeps the whole batch or none of it, so the counter always
//! equals the number of records.
//!
//! The cost of a bind or an unbind follows the number of fields of the schema,
//! never the number of keys of the molecule. No tip and no atom is read or
//! written.
//!
//! The counter lives next to the records, not in the molecule header `mh:{M}`.
//! The presence of a header means "the molecule lives at this prefix" (the
//! write path, the filtered load and the share fan-out read it that way), and a
//! bind comes before the first tip.
//!
//! A counter record never holds zero, except a `suspect` one: the batch that
//! lowers the count to zero deletes the record. An absent record means no binding.

use super::counter::MoleculeFault;
use super::molecule_drain::Candidate as DrainCandidate;
use super::molecule_ref_edges::{molecule_bindings_key, MoleculeRefEdge};
use super::AtomStore;
use crate::schema::SchemaError;
use crate::storage::KvMutation;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Format version of a bindings record.
const BINDINGS_VERSION: u8 = 1;

/// The bindings record of one molecule in one scope.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MoleculeBindings {
    #[serde(rename = "v")]
    pub version: u8,
    /// Binding records that exist for the molecule in this scope.
    pub bindings: u64,
    /// An unbind asked for more records than the counter held. The molecule
    /// never counts as droppable while this is set.
    #[serde(default, skip_serializing_if = "is_false")]
    pub suspect: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// The direction of a transition.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Flip {
    Bind,
    Unbind,
}

impl AtomStore {
    /// Bind the molecules of `edges`: write each binding record that is absent
    /// and raise the counter of its molecule, in one batch. Returns how many
    /// records were new.
    pub(crate) async fn add_molecule_bindings(
        &self,
        edges: &[MoleculeRefEdge],
        storage_prefix: Option<&str>,
    ) -> Result<usize, SchemaError> {
        self.flip_molecule_bindings(edges, storage_prefix, Flip::Bind)
            .await
    }

    /// Unbind: delete each binding record that exists and lower the counter of
    /// its molecule, in one batch. Returns how many records were removed.
    pub(crate) async fn remove_molecule_bindings(
        &self,
        edges: &[MoleculeRefEdge],
        storage_prefix: Option<&str>,
    ) -> Result<usize, SchemaError> {
        self.flip_molecule_bindings(edges, storage_prefix, Flip::Unbind)
            .await
    }

    /// The stored `bindings` record of each molecule, in the order given.
    pub(crate) async fn read_binding_counters(
        &self,
        molecules: &[String],
        storage_prefix: Option<&str>,
    ) -> Result<Vec<Option<MoleculeBindings>>, SchemaError> {
        let keys: Vec<String> = molecules
            .iter()
            .map(|molecule| molecule_bindings_key(molecule, storage_prefix))
            .collect();
        self.read_stored_values(&keys, "read molecule bindings")
            .await
    }

    async fn flip_molecule_bindings(
        &self,
        edges: &[MoleculeRefEdge],
        storage_prefix: Option<&str>,
        flip: Flip,
    ) -> Result<usize, SchemaError> {
        let mut unique: BTreeMap<String, &MoleculeRefEdge> = BTreeMap::new();
        for edge in edges {
            unique.insert(edge.storage_key(storage_prefix), edge);
        }
        if unique.is_empty() {
            return Ok(0);
        }
        let asked = unique.len();
        self.in_write_scope(async move {
            let started = std::time::Instant::now();
            let present = self.binding_records_present(&unique).await?;
            let changed: Vec<(&String, &MoleculeRefEdge)> = unique
                .iter()
                .zip(present)
                .filter(|(_, present)| *present == (flip == Flip::Unbind))
                .map(|((key, edge), _)| (key, *edge))
                .collect();
            if changed.is_empty() {
                return Ok(0);
            }
            let mut by_molecule: BTreeMap<String, u64> = BTreeMap::new();
            for (_, edge) in &changed {
                *by_molecule.entry(edge.molecule_uuid.clone()).or_default() += 1;
            }
            let molecules: Vec<String> = by_molecule.keys().cloned().collect();
            let stored = self
                .read_binding_counters(&molecules, storage_prefix)
                .await?;
            let mut mutations = binding_record_mutations(&changed, flip)?;
            let mut candidates = 0usize;
            for ((molecule, count), stored) in by_molecule.iter().zip(stored) {
                let (next, fault) = lowered_or_raised(stored, *count, flip);
                if fault {
                    self.counter_report()
                        .note_molecule(molecule, MoleculeFault::Underflow);
                }
                let key = molecule_bindings_key(molecule, storage_prefix).into_bytes();
                let reached_zero = next.is_none();
                mutations.push(match next {
                    Some(record) => KvMutation::put(key, to_bytes(&record)?),
                    None => KvMutation::delete(key),
                });
                if reached_zero && flip == Flip::Unbind {
                    // The molecule lost its last binding: the candidate row for the
                    // drain joins this batch (`molecule_drain/candidate.rs`).
                    let (key, value) = DrainCandidate::new(storage_prefix, molecule).row();
                    mutations.push(KvMutation::put(key, value));
                    candidates += 1;
                }
            }
            self.main_store
                .inner()
                .batch_mutate(mutations)
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!("write molecule bindings: {error}"))
                })?;
            if candidates > 0 {
                self.wake_drain_after_commit();
            }
            tracing::debug!(
                target: "fold_db::molecule_bindings",
                bind = flip == Flip::Bind,
                asked,
                flipped = changed.len(),
                molecules = by_molecule.len(),
                elapsed_us = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
                "molecule bindings changed"
            );
            Ok(changed.len())
        })
        .await
    }

    /// Whether each binding record exists, in key order.
    async fn binding_records_present(
        &self,
        unique: &BTreeMap<String, &MoleculeRefEdge>,
    ) -> Result<Vec<bool>, SchemaError> {
        let keys: Vec<Vec<u8>> = unique.keys().map(|key| key.as_bytes().to_vec()).collect();
        self.stored_keys_exist(&keys, "read molecule binding records")
            .await
    }
}

/// The puts or deletes of the binding records that flip.
fn binding_record_mutations(
    changed: &[(&String, &MoleculeRefEdge)],
    flip: Flip,
) -> Result<Vec<KvMutation>, SchemaError> {
    let mut mutations = Vec::with_capacity(changed.len() * 2);
    for (key, edge) in changed {
        mutations.push(match flip {
            Flip::Bind => KvMutation::put(key.as_bytes().to_vec(), to_bytes(edge)?),
            Flip::Unbind => KvMutation::delete(key.as_bytes().to_vec()),
        });
    }
    Ok(mutations)
}

/// The record to write after `count` flips, or `None` to delete it. The second
/// value is true when an unbind found fewer bindings than it removes.
fn lowered_or_raised(
    stored: Option<MoleculeBindings>,
    count: u64,
    flip: Flip,
) -> (Option<MoleculeBindings>, bool) {
    let mut record = stored.unwrap_or_default();
    record.version = BINDINGS_VERSION;
    let mut fault = false;
    match flip {
        Flip::Bind => record.bindings = record.bindings.saturating_add(count),
        Flip::Unbind if record.bindings >= count => record.bindings -= count,
        Flip::Unbind => {
            record.bindings = 0;
            record.suspect = true;
            fault = true;
        }
    }
    if record.bindings == 0 && !record.suspect {
        return (None, fault);
    }
    (Some(record), fault)
}

fn to_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, SchemaError> {
    serde_json::to_vec(value)
        .map_err(|error| SchemaError::InvalidData(format!("serialize molecule binding: {error}")))
}
