//! Atom UUID resolution for filtered field values.

use crate::db_operations::DbOperations;
use crate::schema::types::field::{field_value_from_atom, FieldValue, KeyedAtomMatch};
use crate::schema::types::key_value::KeyValue;
use crate::schema::types::SchemaError;
use std::collections::HashMap;
use std::sync::Arc;

mod batch;
pub(crate) use batch::{fetch_field_atom_batches, FieldAtomBatch};

/// Resolve atom UUID matches into concrete FieldValue map under an optional
/// storage prefix (`storage_prefix` / share `from:{sender}`). Exact-prefix only —
/// no bare dual-read. Every selected tip requires its body in that same scope.
///
/// Each match is a [`KeyedAtomMatch`].
pub async fn fetch_atoms_with_key_metadata_async_with_prefix(
    db_ops: &Arc<DbOperations>,
    matches: impl IntoIterator<Item = KeyedAtomMatch>,
    storage_prefix: Option<&str>,
    molecule_uuid: Option<&str>,
) -> Result<HashMap<KeyValue, FieldValue>, SchemaError> {
    let mut resolved_values: HashMap<KeyValue, FieldValue> = HashMap::new();

    let runtime = db_ops.namespaced_store().disk_pack_runtime();
    let mut misses = Vec::new();
    for entry in matches {
        if let Some(value) = crate::storage::disk_pack::logical_paths::field::lookup(
            runtime.clone(),
            storage_prefix,
            molecule_uuid,
            &crate::storage::disk_pack::logical_paths::field::AtomSelection {
                entry: &entry,
                primary: db_ops.atoms().body_storage_key(&entry.1, storage_prefix),
            },
        )? {
            resolved_values.insert(entry.0, value);
        } else {
            misses.push(entry);
        }
    }
    let uuids: Vec<&str> = misses
        .iter()
        .map(|(_, atom_uuid, _, _)| atom_uuid.as_str())
        .collect();
    let atoms =
        crate::db_operations::tip_atoms::get_atoms_for_tips(db_ops, &uuids, storage_prefix).await?;

    for ((key, atom_uuid, key_meta, writer_pubkey), atom) in misses.into_iter().zip(atoms) {
        let value = field_value_from_atom(&atom, atom_uuid, key_meta, writer_pubkey);
        crate::storage::disk_pack::logical_paths::field::admit(
            runtime.clone(),
            storage_prefix,
            molecule_uuid,
            &key,
            atom,
        )?;
        resolved_values.insert(key, value);
    }

    Ok(resolved_values)
}
