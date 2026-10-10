//! Pure decoding of shape-only metadata and displaced Delete history.

use super::super::types::{MoleculeGenerationPointer, MoleculeHeader};
use crate::atom::delete_barrier::{delete_barrier_key, DeleteBarrier};
use crate::schema::SchemaError;

fn invalid(message: impl Into<String>) -> SchemaError {
    SchemaError::InvalidData(message.into())
}

/// A Delete barrier never creates a body; its displaced atom remains history.
/// Exact production key validation preserves every storage scope and key byte.
pub fn offline_delete_history_atom(key: &str, value: &[u8]) -> Result<Option<String>, SchemaError> {
    let barrier: DeleteBarrier =
        serde_json::from_slice(value).map_err(|e| invalid(e.to_string()))?;
    if key != delete_barrier_key(barrier.mk_key.as_bytes()) {
        return Err(invalid("Delete barrier key differs from its value"));
    }
    if barrier
        .displaced_atom_uuid
        .as_ref()
        .is_some_and(String::is_empty)
    {
        return Err(invalid("Delete history has an empty atom identity"));
    }
    Ok(barrier.displaced_atom_uuid)
}

/// Headers contain shape/time only; generation pointers contain generation ids.
/// All physical generation rows and shadows are inspected independently.
pub fn offline_molecule_shape(bare: &str, value: &[u8]) -> Result<(), SchemaError> {
    if bare.starts_with("mh:") {
        let _: MoleculeHeader =
            serde_json::from_slice(value).map_err(|e| invalid(e.to_string()))?;
    } else if bare.starts_with("mgp:v1:") {
        let pointer: MoleculeGenerationPointer =
            serde_json::from_slice(value).map_err(|e| invalid(e.to_string()))?;
        if pointer.generation.is_empty()
            || pointer
                .previous_generation
                .as_ref()
                .is_some_and(String::is_empty)
        {
            return Err(invalid("generation pointer has an empty identity"));
        }
    } else {
        return Err(invalid("unsupported molecule shape key"));
    }
    Ok(())
}
