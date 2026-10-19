//! Selected-tip atom reads use the typed core model and captured disk root.

use crate::atom::{Atom, AtomPartition};
use crate::db_operations::DbOperations;
use crate::schema::types::SchemaError;
use std::sync::Arc;

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

/// Load selected bodies in one batch and reject unresolved personal tips.
pub async fn get_atoms_for_tips(
    db_ops: &Arc<DbOperations>,
    slots: &[(&str, Option<AtomPartition>)],
    storage_prefix: Option<&str>,
) -> Result<Vec<Option<Atom>>, SchemaError> {
    let atoms = db_ops
        .atoms()
        .get_atoms_located(slots, storage_prefix)
        .await
        .map_err(SchemaError::read_failure)?;
    require_tip_atoms(atoms, storage_prefix)
}
