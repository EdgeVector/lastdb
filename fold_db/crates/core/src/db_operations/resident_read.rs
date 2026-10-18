//! Legacy unprefixed atom reads can reuse ResidentGraph values. Native reads
//! use the captured typed core model, current authorized core values, and the
//! captured disk root. They do not inspect a second raw write buffer.
//!
//! Reuse preserves the full Atom metadata and selected physical copy; a UUID
//! alone does not certify interchangeable copies. Misses use the normal batch
//! loader. A selected tip without its Atom is an integrity failure.

use crate::atom::{Atom, AtomPartition};
use crate::db_operations::DbOperations;
use crate::schema::types::SchemaError;
use std::sync::Arc;

/// The mutable legacy typed graph is not part of a captured native read root.
/// Native reads use CoreWarmSet and its immutable typed read versions instead.
pub(crate) fn graph_reads_enabled(db_ops: &DbOperations) -> bool {
    db_ops.namespaced_store().disk_pack_runtime().is_none()
}

/// Legacy unprefixed reads consult the graph in every acknowledgement mode.
pub(crate) fn tip_reads_resident_first(
    db_ops: &DbOperations,
    storage_prefix: Option<&str>,
) -> bool {
    storage_prefix.is_none() && graph_reads_enabled(db_ops)
}

/// Resident-first counterpart of
/// [`crate::db_operations::atom_store::AtomStore::get_atoms_located`]:
/// same slots-in, same `Vec<Option<Atom>>` out, same order.
pub async fn get_atoms_located_resident_first(
    db_ops: &Arc<DbOperations>,
    slots: &[(&str, Option<AtomPartition>)],
    storage_prefix: Option<&str>,
) -> Result<Vec<Option<Atom>>, SchemaError> {
    if !tip_reads_resident_first(db_ops, storage_prefix) {
        let atoms = db_ops
            .atoms()
            .get_atoms_located(slots, storage_prefix)
            .await
            .map_err(SchemaError::read_failure)?;
        return require_tip_atoms(atoms, storage_prefix);
    }

    let resident = db_ops.resident();
    let mut out: Vec<Option<Atom>> = vec![None; slots.len()];
    let mut miss_idx: Vec<usize> = Vec::new();
    let mut first_slot = std::collections::HashMap::with_capacity(slots.len());
    let mut duplicates = Vec::new();
    for (i, (uuid, _)) in slots.iter().enumerate() {
        if let Some(&first) = first_slot.get(uuid) {
            duplicates.push((i, first));
            continue;
        }
        first_slot.insert(*uuid, i);
        // A resident entry without full fidelity declines `into_atom` and is
        // treated as a miss — storage stays the source of truth for it.
        match resident
            .resolve_atom(uuid)
            .and_then(|hit| hit.value.into_atom())
        {
            Some(atom) => out[i] = Some(atom),
            None => miss_idx.push(i),
        }
    }
    if !miss_idx.is_empty() {
        let miss_slots: Vec<(&str, Option<AtomPartition>)> = miss_idx
            .iter()
            .map(|&i| (slots[i].0, slots[i].1.clone()))
            .collect();
        let fetched = db_ops
            .atoms()
            .get_atoms_located(&miss_slots, None)
            .await
            .map_err(SchemaError::read_failure)?;
        for (i, atom) in miss_idx.into_iter().zip(fetched) {
            // A disk load admits the body on the storage path, into the warm
            // set only. The graph keeps an atom only while that write is dirty.
            out[i] = atom;
        }
    }
    // Preserve slot order while loading each immutable UUID only once.
    for (duplicate, first) in duplicates {
        out[duplicate] = out[first].clone();
    }
    require_tip_atoms(out, storage_prefix)
}

/// These slots come from selected tips. A missing key produces no slot;
/// missing scoped bodies remain an intentional filter.
fn require_tip_atoms(
    atoms: Vec<Option<Atom>>,
    storage_prefix: Option<&str>,
) -> Result<Vec<Option<Atom>>, SchemaError> {
    if storage_prefix.is_none() && atoms.iter().any(Option::is_none) {
        return Err(SchemaError::MissingLocalAtom);
    }
    Ok(atoms)
}

/// Single selected-tip counterpart, including the document-envelope path.
pub(crate) async fn get_atom_for_tip(
    db_ops: &DbOperations,
    atom_uuid: &str,
) -> Result<Atom, SchemaError> {
    if tip_reads_resident_first(db_ops, None) {
        if let Some(atom) = db_ops
            .resident()
            .resolve_atom(atom_uuid)
            .and_then(|hit| hit.value.into_atom())
        {
            return Ok(atom);
        }
    }
    db_ops
        .atoms()
        .get_atom_by_uuid(atom_uuid, None)
        .await
        .map_err(SchemaError::read_failure)?
        .ok_or(SchemaError::MissingLocalAtom)
}
