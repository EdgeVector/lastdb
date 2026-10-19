//! Selected-tip atom reads use the typed core model and captured disk root.

use crate::atom::{Atom, AtomPartition};
use crate::db_operations::DbOperations;
use crate::schema::types::SchemaError;
use std::sync::Arc;

/// Load required bodies for selected tips in every storage scope.
/// A missing key produces no slot; a selected tip without its body is an error.
pub async fn get_atoms_for_tips(
    db_ops: &Arc<DbOperations>,
    slots: &[(&str, Option<AtomPartition>)],
    storage_prefix: Option<&str>,
) -> Result<Vec<Atom>, SchemaError> {
    let atoms = db_ops
        .atoms()
        .get_atoms_located(slots, storage_prefix)
        .await
        .map_err(SchemaError::read_failure)?;
    require_tip_atoms(atoms, slots.len())
}

/// Validate the selected-body batch before any caller pairs it with its tips.
pub(crate) fn require_tip_atoms(
    atoms: Vec<Option<Atom>>,
    expected: usize,
) -> Result<Vec<Atom>, SchemaError> {
    if atoms.len() != expected {
        return Err(SchemaError::StorageRead {
            detail: "selected atom batch returned a different number of results".into(),
        });
    }
    atoms
        .into_iter()
        .map(|atom| atom.ok_or(SchemaError::MissingSelectedAtom))
        .collect()
}
