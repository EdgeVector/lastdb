//! Resident-first atom resolution — the T0 fast path for batch atom loads.
//!
//! Wraps [`crate::db_operations::atom_store::AtomStore::get_atoms_located`]
//! with a per-uuid resident check. Atoms are content-addressed, so serving a
//! known uuid from resident can never return a different answer than storage —
//! there is no completeness problem the way there is for tip/range sets. Hits
//! skip storage entirely; misses batch through the normal located load and
//! install into resident for next time.
//!
//! Gating: only unprefixed reads (`storage_prefix: None` — resident does not
//! model share namespaces) and only when `LASTDB_RESIDENT_MODE` enables
//! resident-first reads. Mode off = byte-identical passthrough.

use std::sync::Arc;
use std::sync::OnceLock;

use crate::atom::{Atom, AtomPartition};
use crate::db_operations::DbOperations;
use crate::resident::{ResidentAtom, ResidentMode, ResidentPolicy};
use crate::schema::types::SchemaError;

/// Process-resolved resident mode (env is stable for the process lifetime).
fn resident_mode() -> ResidentMode {
    static MODE: OnceLock<ResidentMode> = OnceLock::new();
    *MODE.get_or_init(|| ResidentPolicy::from_env().mode)
}

/// Whether an unprefixed tip lookup may consult resident before durable
/// storage. Tip probes use the current process env directly so tests that
/// exercise this narrow path do not inherit the atom-batch OnceLock's first
/// reader.
pub(crate) fn tip_reads_resident_first(storage_prefix: Option<&str>) -> bool {
    storage_prefix.is_none() && ResidentPolicy::from_env().mode.reads_resident_first()
}

/// Resident-first counterpart of
/// [`crate::db_operations::atom_store::AtomStore::get_atoms_located`]:
/// same slots-in, same `Vec<Option<Atom>>` out, same order.
pub async fn get_atoms_located_resident_first(
    db_ops: &Arc<DbOperations>,
    slots: &[(&str, Option<AtomPartition>)],
    storage_prefix: Option<&str>,
) -> Result<Vec<Option<Atom>>, SchemaError> {
    get_atoms_located_with_mode(db_ops, slots, storage_prefix, resident_mode()).await
}

/// Mode-explicit body ([`get_atoms_located_resident_first`] passes the
/// process-resolved mode; tests pass one directly — env vars race across
/// parallel test threads and the OnceLock pins first-read-wins).
pub(crate) async fn get_atoms_located_with_mode(
    db_ops: &Arc<DbOperations>,
    slots: &[(&str, Option<AtomPartition>)],
    storage_prefix: Option<&str>,
    mode: ResidentMode,
) -> Result<Vec<Option<Atom>>, SchemaError> {
    if storage_prefix.is_some() || !mode.reads_resident_first() {
        return db_ops
            .atoms()
            .get_atoms_located(slots, storage_prefix)
            .await
            .map_err(|e| SchemaError::InvalidField(format!("Failed to fetch atom batch: {e}")));
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
            .map_err(|e| SchemaError::InvalidField(format!("Failed to fetch atom batch: {e}")))?;
        for (i, atom) in miss_idx.into_iter().zip(fetched) {
            if let Some(ref a) = atom {
                resident.rehydrate_atom(ResidentAtom::from_atom(a));
            }
            out[i] = atom;
        }
    }
    // Preserve slot order while loading each immutable UUID only once.
    for (duplicate, first) in duplicates {
        out[duplicate] = out[first].clone();
    }
    Ok(out)
}
