//! Exact logical membership for schemas that share physical field molecules.

use crate::atom::{Atom, AtomPartition};
use crate::db_operations::DbOperations;
use crate::schema::types::field::FieldVariant;
use crate::schema::types::key_value::scalar_key_coordinate;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{KeyValue, Schema};
use crate::schema::SchemaError;
use std::sync::Arc;

/// A present declared HashRange hash spine must prove both coordinates.
/// An absent spine permits the existing projection fallback; the caller still
/// establishes field presence. Hash and Single retain their current rules.
pub(crate) async fn logical_key_membership(
    db_ops: &Arc<DbOperations>,
    schema: &Schema,
    keys: &[KeyValue],
    storage_prefix: Option<&str>,
) -> Result<Vec<bool>, SchemaError> {
    let mut matches = vec![true; keys.len()];
    if schema.schema_type != DeclarativeSchemaType::HashRange || keys.is_empty() {
        return Ok(matches);
    }
    let Some(layout) = schema.key.as_ref() else {
        return Ok(matches);
    };
    let Some(hash_name) = layout.hash_field.as_deref() else {
        return Ok(matches);
    };
    let Some(hash) = schema.runtime_fields.get(hash_name) else {
        return Ok(matches);
    };
    let hash_atoms = field_atoms(db_ops, hash, keys, storage_prefix).await?;
    let mut range_keys = Vec::new();
    let mut range_indices = Vec::new();
    for (index, (key, atom)) in keys.iter().zip(hash_atoms).enumerate() {
        let Some(atom) = atom else {
            continue;
        };
        matches[index] = key.hash_matches_scalar(atom.content());
        if !matches[index] {
            continue;
        }
        if layout.range_field.as_deref() == Some(hash_name) {
            matches[index] =
                scalar_key_coordinate(atom.content()).as_deref() == key.range.as_deref();
        } else {
            range_keys.push(key.clone());
            range_indices.push(index);
        }
    }
    if range_keys.is_empty() {
        return Ok(matches);
    }
    let Some(range) = layout
        .range_field
        .as_ref()
        .and_then(|name| schema.runtime_fields.get(name))
    else {
        for index in range_indices {
            matches[index] = false;
        }
        return Ok(matches);
    };
    let range_atoms = field_atoms(db_ops, range, &range_keys, storage_prefix).await?;
    for ((index, key), atom) in range_indices.into_iter().zip(range_keys).zip(range_atoms) {
        matches[index] = atom.is_some_and(|atom| {
            scalar_key_coordinate(atom.content()).as_deref() == key.range.as_deref()
        });
    }
    Ok(matches)
}

/// Select each tip once and preserve its physical location through the shared
/// required-body reader. Missing tips remain absent; missing bodies refuse.
async fn field_atoms(
    db_ops: &Arc<DbOperations>,
    field: &FieldVariant,
    keys: &[KeyValue],
    storage_prefix: Option<&str>,
) -> Result<Vec<Option<Atom>>, SchemaError> {
    let mut atoms = vec![None; keys.len()];
    let Some(molecule) = field.common().molecule_uuid() else {
        return Ok(atoms);
    };
    let records = db_ops
        .atoms()
        .load_query_tip_records(storage_prefix, molecule, keys)
        .await?;
    if records.len() != keys.len() {
        return Err(SchemaError::StorageRead {
            detail: "logical membership tip count differs".into(),
        });
    }
    let mut selected = Vec::new();
    for (index, record) in records.into_iter().enumerate() {
        let Some((base, record)) = record else {
            continue;
        };
        if record.meta.as_ref().is_some_and(|meta| meta.tombstoned) {
            continue;
        }
        let partition =
            AtomPartition::from_record_key(&base).ok_or_else(|| SchemaError::StorageRead {
                detail: "logical membership tip has no physical atom location".into(),
            })?;
        selected.push((index, record.entry.atom_uuid, partition));
    }
    let tips: Vec<_> = selected
        .iter()
        .map(|(_, uuid, partition)| (uuid.as_str(), Some(partition.clone())))
        .collect();
    let fetched =
        crate::db_operations::tip_atoms::get_atoms_for_tips(db_ops, &tips, storage_prefix).await?;
    for ((index, _, _), atom) in selected.into_iter().zip(fetched) {
        atoms[index] = Some(atom);
    }
    Ok(atoms)
}
