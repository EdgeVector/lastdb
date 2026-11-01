//! Storage-free decisions for conditional writes and selected record state.

use crate::atom::Atom;
use crate::schema::types::{cas::CasExpectation, Mutation, Schema};
use crate::schema::SchemaError;

use super::cas::CurrentRowFields;

/// A selected field's live atom identity and value. Tombstones are absent.
#[derive(Default)]
pub(super) struct CurrentHead {
    pub(super) mutation_uuid: Option<String>,
    pub(super) atom_uuid: Option<String>,
    pub(super) value: Option<serde_json::Value>,
}

pub(super) fn check_expectation(
    mutation: &Mutation,
    expected: &CasExpectation,
    head: CurrentHead,
) -> Result<(), SchemaError> {
    let conflict = |exp: String, actual: Option<String>| SchemaError::CasConflict {
        schema: mutation.schema_name.clone(),
        field: expected.field().to_string(),
        key: mutation.key_value.to_storage_key(),
        expected: exp,
        actual,
    };
    match expected {
        CasExpectation::Mutation { uuid, .. } => match head.mutation_uuid {
            Some(current) if &current == uuid => Ok(()),
            current => Err(conflict(uuid.clone(), current)),
        },
        CasExpectation::Absent { .. } => {
            if let Some(value) = head.value {
                return Err(conflict("<absent>".to_string(), Some(value.to_string())));
            }
            Ok(())
        }
        CasExpectation::Value { value, .. } => match head.value {
            Some(current) if &current == value => Ok(()),
            Some(current) => Err(conflict(value.to_string(), Some(current.to_string()))),
            None => Err(conflict(value.to_string(), None)),
        },
        CasExpectation::ContentHash { hash, .. } => match head.atom_uuid {
            Some(current) if &current == hash => Ok(()),
            Some(current) => Err(conflict(hash.clone(), Some(current))),
            None => Err(conflict(hash.clone(), None)),
        },
    }
}

/// Probe written fields first. A missing written field must not hide a live
/// value elsewhere in the schema when a partial update requires an existing key.
pub(super) fn written_fields(mutation: &Mutation, schema: &Schema) -> Vec<String> {
    let mut fields: Vec<_> = mutation
        .fields_and_values
        .keys()
        .filter(|field| schema.runtime_fields.contains_key(*field))
        .cloned()
        .collect();
    fields.sort_unstable();
    fields
}

pub(super) fn remaining_fields(schema: &Schema, written: &[String]) -> Vec<String> {
    let mut fields: Vec<_> = schema
        .runtime_fields
        .keys()
        .filter(|field| !written.contains(field))
        .cloned()
        .collect();
    fields.sort_unstable();
    fields
}

/// Assemble only the selected fields. All tombstones mean absence; a mix of
/// live and tombstone values is corrupt.
pub(super) fn assemble_current_row(
    selected: usize,
    atoms: &[Atom],
) -> Result<CurrentRowFields, SchemaError> {
    if atoms.len() != selected {
        return Err(SchemaError::StorageRead {
            detail: "point-read atom count differs from selected field count".into(),
        });
    }
    let tombstones = atoms
        .iter()
        .filter(|atom| crate::atom::is_tombstone_value(atom.content()))
        .count();
    Ok(if tombstones == atoms.len() {
        CurrentRowFields::Absent
    } else if tombstones > 0 {
        CurrentRowFields::Corrupt
    } else {
        CurrentRowFields::Present
    })
}
