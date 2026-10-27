//! Storage-free decisions for conditional writes and selected record state.

use std::collections::BTreeMap;

use crate::atom::{Atom, AtomEntry};
use crate::schema::types::{cas::CasExpectation, AggregateWinner, Mutation, Schema};
use crate::schema::SchemaError;

use super::cas::CurrentRowFields;

/// A selected field's live atom identity and value. Tombstones are absent.
#[derive(Default)]
pub(super) struct CurrentHead {
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
/// live and tombstone values is corrupt. A winner comes only from live values.
pub(super) fn assemble_current_row(
    selected: Vec<(&String, AtomEntry)>,
    atoms: Vec<Atom>,
) -> Result<(CurrentRowFields, Option<AggregateWinner>), SchemaError> {
    if atoms.len() != selected.len() {
        return Err(SchemaError::StorageRead {
            detail: "point-read atom count differs from selected field count".into(),
        });
    }
    let mut values = BTreeMap::new();
    let mut latest_winner: Option<AggregateWinner> = None;
    let mut tombstone = None;
    for ((field, entry), atom) in selected.into_iter().zip(atoms) {
        if crate::atom::is_tombstone_value(atom.content()) {
            tombstone.get_or_insert_with(|| CurrentRowFields::Corrupt {
                field: field.clone(),
                atom_uuid: entry.atom_uuid,
            });
            continue;
        }
        let winner = AggregateWinner {
            logical_counter: entry.logical_counter,
            written_at: entry.written_at,
            writer_id: entry.lww_device().to_string(),
            mutation_uuid: entry.mutation_uuid.clone(),
        };
        match &mut latest_winner {
            Some(current) if winner.cmp_source(current).is_gt() => *current = winner,
            None => latest_winner = Some(winner),
            _ => {}
        }
        values.insert(field.clone(), atom.content().clone());
    }
    if values.is_empty() {
        return Ok((CurrentRowFields::Absent, None));
    }
    if let Some(tombstone) = tombstone {
        return Ok((tombstone, None));
    }
    Ok((CurrentRowFields::Present(values), latest_winner))
}
