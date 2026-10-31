//! Compare-and-set and must-exist decisions over a gathered head or row.
//!
//! The caller reads one head (or one fixed-field row) per guarded mutation.
//! These functions decide accept or the exact refusal from that snapshot.

use std::collections::BTreeMap;

use crate::atom::{Atom, AtomEntry, KeyMetadata};
use crate::schema::types::field::FieldVariant;
use crate::schema::types::operations::MutationType;
use crate::schema::types::{cas::CasExpectation, AggregateWinner, KeyValue, Mutation, Schema};
use crate::schema::SchemaError;

/// Integrity-aware result of an exact fixed-field record read.
#[derive(Debug)]
pub(in crate::fold_db_core::mutation_manager) enum CurrentRowFields {
    Absent,
    Present(BTreeMap<String, serde_json::Value>),
    Corrupt { field: String, atom_uuid: String },
}

impl CurrentRowFields {
    pub(in crate::fold_db_core::mutation_manager) fn has_live_field(&self) -> bool {
        matches!(self, Self::Present(_) | Self::Corrupt { .. })
    }
}

/// A selected field's live atom identity and value. Tombstones are absent.
#[derive(Default)]
pub(in crate::fold_db_core::mutation_manager) struct CurrentHead {
    pub(in crate::fold_db_core::mutation_manager) mutation_uuid: Option<String>,
    pub(in crate::fold_db_core::mutation_manager) atom_uuid: Option<String>,
    pub(in crate::fold_db_core::mutation_manager) value: Option<serde_json::Value>,
}

/// The schema field a CAS expectation names. Resolved independently of the
/// payload: Delete has no payload, and Update can compare an unchanged field.
pub(in crate::fold_db_core::mutation_manager) fn expected_field<'s>(
    schema: &'s Schema,
    mutation: &Mutation,
    field: &str,
) -> Result<&'s FieldVariant, SchemaError> {
    schema.runtime_fields.get(field).ok_or_else(|| {
        SchemaError::InvalidField(format!(
            "CAS expectation names field '{field}' absent from schema '{}'",
            mutation.schema_name
        ))
    })
}

/// Every field of a fixed-field row read must be declared by the schema.
pub(in crate::fold_db_core::mutation_manager) fn require_point_fields(
    schema_name: &str,
    schema: &Schema,
    fields: &[String],
) -> Result<(), SchemaError> {
    if let Some(field) = fields
        .iter()
        .find(|field| !schema.runtime_fields.contains_key(*field))
    {
        return Err(SchemaError::InvalidField(format!(
            "point read names field '{field}' absent from schema '{schema_name}'",
        )));
    }
    Ok(())
}

/// The molecule and exact key that hold one field's current tip.
pub(in crate::fold_db_core::mutation_manager) fn point_tip_key(
    schema_name: &str,
    field: &str,
    schema_field: &FieldVariant,
    key_value: &KeyValue,
) -> Result<(String, KeyValue), SchemaError> {
    let changed_key = schema_field.changed_key_for(key_value).ok_or_else(|| {
        SchemaError::InvalidData(format!(
            "point read cannot resolve field '{field}' key in schema '{schema_name}'"
        ))
    })?;
    let molecule_uuid = schema_field
        .common()
        .molecule_uuid()
        .cloned()
        .ok_or_else(|| {
            SchemaError::InvalidData(format!(
                "point read field '{schema_name}.{field}' has no molecule identity"
            ))
        })?;
    Ok((
        molecule_uuid,
        KeyValue::new(changed_key.hash, changed_key.range),
    ))
}

/// A tip whose per-key metadata marks it tombstoned has no live entry.
pub(in crate::fold_db_core::mutation_manager) fn live_tip_entry(
    entry: AtomEntry,
    meta: Option<&KeyMetadata>,
) -> Option<AtomEntry> {
    if meta.is_some_and(|meta| meta.tombstoned) {
        return None;
    }
    Some(entry)
}

/// The head a CAS expectation compares against. A tombstoned head reads as
/// absent: a deleted record has no live value to compare against.
pub(in crate::fold_db_core::mutation_manager) fn head_from_tip(
    entry: AtomEntry,
    atom: &Atom,
) -> CurrentHead {
    if crate::atom::is_tombstone_value(atom.content()) {
        return CurrentHead::default();
    }
    CurrentHead {
        mutation_uuid: Some(entry.mutation_uuid),
        atom_uuid: Some(entry.atom_uuid),
        value: Some(atom.content().clone()),
    }
}

/// Accept a CAS-guarded mutation when its expectation holds against the
/// current head, or return the exact conflict.
pub(in crate::fold_db_core::mutation_manager) fn check_expectation(
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

/// Is this a user `Update` carrying `must_exist: true`?
///
/// The one predicate both the lock set and the precondition gate key on,
/// so they can never disagree about which mutations are guarded.
pub(in crate::fold_db_core::mutation_manager) fn is_must_exist_update(mutation: &Mutation) -> bool {
    matches!(mutation.mutation_type, MutationType::Update) && mutation.must_exist == Some(true)
}

/// The row probes of a must-exist check, in read order.
///
/// A row is absent iff NO field of the schema resolves at its key, so the
/// check widens to every schema field rather than the written ones: a heal
/// write to a field the row never carried must not read as a phantom-row
/// create. The written fields come first because their molecules are already
/// restored by this write, so a live row usually settles on the first probe.
/// The caller skips an empty probe and stops at the first live one.
pub(in crate::fold_db_core::mutation_manager) fn must_exist_probes(
    mutation: &Mutation,
    schema: &Schema,
) -> [Vec<String>; 2] {
    let written = written_fields(mutation, schema);
    let rest = remaining_fields(schema, &written);
    [written, rest]
}

/// Accept a must-exist update whose row exists, or refuse the whole batch.
pub(in crate::fold_db_core::mutation_manager) fn must_exist_verdict(
    mutation: &Mutation,
    row_exists: bool,
) -> Result<(), SchemaError> {
    if row_exists {
        return Ok(());
    }
    Err(SchemaError::InvalidData(format!(
        "update target not found: schema '{}', key {} — refusing to silently create a \
         row from a partial update",
        mutation.schema_name,
        mutation.key_value.to_storage_key(),
    )))
}

/// Probe written fields first. A missing written field must not hide a live
/// value elsewhere in the schema when a partial update requires an existing key.
pub(in crate::fold_db_core::mutation_manager) fn written_fields(
    mutation: &Mutation,
    schema: &Schema,
) -> Vec<String> {
    let mut fields: Vec<_> = mutation
        .fields_and_values
        .keys()
        .filter(|field| schema.runtime_fields.contains_key(*field))
        .cloned()
        .collect();
    fields.sort_unstable();
    fields
}

pub(in crate::fold_db_core::mutation_manager) fn remaining_fields(
    schema: &Schema,
    written: &[String],
) -> Vec<String> {
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
pub(in crate::fold_db_core::mutation_manager) fn assemble_current_row(
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
