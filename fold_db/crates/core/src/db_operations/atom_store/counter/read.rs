//! Reads of the planner: old tips, new records, counters, and bodies to offer.
//!
//! Every read here is a read by key, in batches. None scans a collection. All
//! of them run inside the native write request scope, so they see the pending
//! batch of the request and every accepted batch.

use super::commit::{BodySources, RecordRefs};
use super::plan::AtomBefore;
use super::AtomCounter;
use crate::atom::{atom_key_codec, Atom};
use crate::db_operations::atom_store::{AtomStore, PerKeyRecord};
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use std::collections::{BTreeMap, HashMap};

/// The native get-many API accepts at most this many keys per call.
const READ_CHUNK: usize = 10_000;

mod probes;

/// What the counter store says about one atom, for operators.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct AtomCounterView {
    pub atom: String,
    /// The count, or `None` when the atom has no counter.
    pub refs: Option<u64>,
    pub suspect: bool,
    /// Blob references held by the counter.
    pub blobs: usize,
    pub body_present: bool,
}

/// A body that the batch writes for an atom that gains its first reference.
pub(super) struct Offer {
    pub atom: Atom,
    pub blobs: Vec<String>,
    /// Meter size of the body, stored on the new counter.
    pub size: Option<super::AtomSize>,
}

impl AtomStore {
    /// The counter state and body presence of each atom, in the order given.
    /// Two reads by key, in batches. The atom ids name the atoms of one scope.
    pub async fn atom_counter_views(
        &self,
        atoms: &[String],
        scope: Option<&str>,
    ) -> Result<Vec<AtomCounterView>, SchemaError> {
        self.atom_read_scope(self.atom_counter_views_captured(atoms, scope))
            .await
    }

    async fn atom_counter_views_captured(
        &self,
        atoms: &[String],
        scope: Option<&str>,
    ) -> Result<Vec<AtomCounterView>, SchemaError> {
        let counter_keys: Vec<String> = atoms
            .iter()
            .map(|atom| build_storage_key(scope, &atom_key_codec::counter_key(atom)))
            .collect();
        let body_keys: Vec<Vec<u8>> = atoms
            .iter()
            .map(|atom| build_storage_key(scope, &atom_key_codec::body_key(atom)).into_bytes())
            .collect();
        let mut counters: Vec<Option<AtomCounter>> = Vec::with_capacity(atoms.len());
        let mut bodies: Vec<bool> = Vec::with_capacity(atoms.len());
        for (counter_chunk, body_chunk) in counter_keys
            .chunks(READ_CHUNK)
            .zip(body_keys.chunks(READ_CHUNK))
        {
            counters.extend(
                self.read_stored_values::<AtomCounter>(counter_chunk, "read atom counters")
                    .await?,
            );
            bodies.extend(
                self.main_store
                    .inner()
                    .exists_many(body_chunk.to_vec())
                    .await
                    .map_err(|error| {
                        SchemaError::InvalidData(format!("read atom body records: {error}"))
                    })?,
            );
        }
        Ok(atoms
            .iter()
            .zip(counters)
            .zip(bodies)
            .map(|((atom, counter), body_present)| AtomCounterView {
                atom: atom.clone(),
                refs: counter.as_ref().map(|counter| counter.refs),
                suspect: counter.as_ref().is_some_and(|counter| counter.suspect),
                blobs: counter.as_ref().map_or(0, |counter| counter.blobs.len()),
                body_present,
            })
            .collect())
    }

    /// The stored tip record of each key, for the keys that have one.
    pub(super) async fn read_old_tips(
        &self,
        tips: &BTreeMap<String, Option<PerKeyRecord>>,
    ) -> Result<HashMap<String, PerKeyRecord>, SchemaError> {
        let keys: Vec<String> = tips.keys().cloned().collect();
        let mut old = HashMap::new();
        for chunk in keys.chunks(READ_CHUNK) {
            let stored = self
                .read_stored_values::<PerKeyRecord>(chunk, "read old tips")
                .await?;
            for (key, record) in chunk.iter().zip(stored) {
                if let Some(record) = record {
                    old.insert(key.clone(), record);
                }
            }
        }
        Ok(old)
    }

    /// For each conflict record, whether it is new. A record that exists keeps
    /// its references; the same decision written twice adds nothing.
    pub(super) async fn read_new_records(
        &self,
        records: &[RecordRefs],
    ) -> Result<Vec<bool>, SchemaError> {
        let keys: Vec<Vec<u8>> = records
            .iter()
            .map(|record| record.key.as_bytes().to_vec())
            .collect();
        let mut is_new = Vec::with_capacity(keys.len());
        for chunk in keys.chunks(READ_CHUNK) {
            let exists = self
                .main_store
                .inner()
                .exists_many(chunk.to_vec())
                .await
                .map_err(|error| SchemaError::InvalidData(format!("read record keys: {error}")))?;
            is_new.extend(exists.into_iter().map(|exists| !exists));
        }
        Ok(is_new)
    }

    /// The counter state of each changed atom. For an atom with no counter the
    /// state also says whether a body record exists.
    pub(super) async fn read_counters(
        &self,
        deltas: &BTreeMap<String, i64>,
        scope: Option<&str>,
    ) -> Result<HashMap<String, AtomBefore>, SchemaError> {
        let atoms: Vec<&String> = deltas.keys().collect();
        let mut out = self.native_counter_probes(&atoms, scope).await?;
        let missing: Vec<_> = atoms
            .into_iter()
            .filter(|atom| !out.contains_key(*atom))
            .collect();
        let keys: Vec<String> = missing
            .iter()
            .map(|atom| build_storage_key(scope, &atom_key_codec::counter_key(atom)))
            .collect();
        let stored = self
            .read_stored_values::<AtomCounter>(&keys, "read atom counters")
            .await?;
        let bodies = self.body_records_exist(&missing, &stored, scope).await?;
        for ((atom, counter), body_without_counter) in missing.into_iter().zip(stored).zip(bodies) {
            out.insert(
                atom.clone(),
                AtomBefore {
                    counter,
                    body_without_counter,
                },
            );
        }
        Ok(out)
    }

    /// For each atom without a counter, whether its body record exists.
    async fn body_records_exist(
        &self,
        atoms: &[&String],
        counters: &[Option<AtomCounter>],
        scope: Option<&str>,
    ) -> Result<Vec<bool>, SchemaError> {
        let slots: Vec<usize> = counters
            .iter()
            .enumerate()
            .filter_map(|(slot, counter)| counter.is_none().then_some(slot))
            .collect();
        let keys: Vec<Vec<u8>> = slots
            .iter()
            .map(|&slot| {
                build_storage_key(scope, &atom_key_codec::body_key(atoms[slot])).into_bytes()
            })
            .collect();
        let mut found = Vec::with_capacity(keys.len());
        for chunk in keys.chunks(READ_CHUNK) {
            found.extend(
                self.main_store
                    .inner()
                    .exists_many(chunk.to_vec())
                    .await
                    .map_err(|error| {
                        SchemaError::InvalidData(format!("read atom body records: {error}"))
                    })?,
            );
        }
        let mut out = vec![false; counters.len()];
        for (slot, exists) in slots.into_iter().zip(found) {
            out[slot] = exists;
        }
        Ok(out)
    }

    /// The body to write for each atom that gains its first reference in this
    /// scope. The request brings most bodies. A scope that copies tips from
    /// another scope reads the missing body from that scope.
    pub(super) async fn offer_bodies(
        &self,
        deltas: &BTreeMap<String, i64>,
        before: &HashMap<String, AtomBefore>,
        sources: &BodySources<'_>,
    ) -> Result<HashMap<String, Offer>, SchemaError> {
        let brought: HashMap<&str, &Atom> = sources
            .bodies
            .iter()
            .map(|atom| (atom.uuid(), atom))
            .collect();
        let mut offers = HashMap::new();
        let mut missing: Vec<&String> = Vec::new();
        for (atom, delta) in deltas {
            let first = *delta > 0
                && before
                    .get(atom)
                    .is_some_and(|state| state.counter.is_none());
            if !first {
                continue;
            }
            match brought.get(atom.as_str()) {
                Some(body) => {
                    offers.insert(atom.clone(), offer_of(body));
                }
                None => missing.push(atom),
            }
        }
        if let Some(source) = sources.source {
            self.offer_from_source(&missing, source, &mut offers)
                .await?;
        }
        Ok(offers)
    }

    async fn offer_from_source(
        &self,
        missing: &[&String],
        source: Option<&str>,
        offers: &mut HashMap<String, Offer>,
    ) -> Result<(), SchemaError> {
        let keys: Vec<String> = missing
            .iter()
            .map(|atom| build_storage_key(source, &atom_key_codec::body_key(atom)))
            .collect();
        for (atom, body) in missing
            .iter()
            .zip(self.get_atoms_by_storage_keys(&keys).await?)
        {
            if let Some(body) = body {
                offers.insert((*atom).clone(), offer_of(&body));
            }
        }
        Ok(())
    }
}

fn offer_of(atom: &Atom) -> Offer {
    Offer {
        blobs: crate::atom::file_pointer::blob_refs_of_atom(atom.content(), atom.metadata()),
        size: super::AtomSize::of(atom),
        atom: atom.clone(),
    }
}
