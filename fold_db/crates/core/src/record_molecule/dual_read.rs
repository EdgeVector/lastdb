//! Per-key dual-read: document envelope on R's tip, else today's field zip.

use std::collections::HashMap;

use super::envelope::{envelope_is_complete, parse_document_fields};
use super::is_record_molecule_field;
use super::read_batch::load_record_tip_atoms;
use crate::atom::Atom;
use crate::db_operations::DbOperations;
use crate::schema::types::field::FieldValue;
use crate::schema::types::key_value::KeyValue;
use crate::schema::Schema;
use crate::schema::SchemaError;

/// HashRangeKey projection from R when the tip is a document envelope.
///
/// `None` means zip field molecules (no R, missing tip, or non-envelope).
/// `Some` is the requested fields from the envelope; skip the field zip.
pub async fn hash_range_key_from_envelope(
    db_ops: &DbOperations,
    schema: &Schema,
    hash: &str,
    range: &str,
    requested: &[String],
    include_tombstones: bool,
) -> Result<Option<HashMap<String, HashMap<KeyValue, FieldValue>>>, SchemaError> {
    let Some(record_uuid) = schema.molecule_uuid.clone() else {
        return Ok(None);
    };
    let key = KeyValue::new(Some(hash.to_string()), Some(range.to_string()));
    let Some(atom) = load_record_tip_atoms(
        db_ops,
        &record_uuid,
        std::slice::from_ref(&key),
        record_scope(schema),
        Some(schema),
    )
    .await?
    .remove(&key) else {
        return Ok(None);
    };
    let Some(fields) = parse_document_fields(atom.content()) else {
        return Ok(None);
    };
    if !envelope_is_complete(atom.content()) {
        // Partial R must not hide live field-molecule tips. Zip instead.
        return Ok(None);
    }
    let names: Vec<String> = if requested.is_empty() {
        schema
            .runtime_fields
            .keys()
            .filter(|n| !is_record_molecule_field(n))
            .cloned()
            .collect()
    } else {
        requested.to_vec()
    };
    let mut results: HashMap<String, HashMap<KeyValue, FieldValue>> = HashMap::new();
    for name in &names {
        if is_record_molecule_field(name) {
            continue;
        }
        let Some(value) = fields.get(name) else {
            continue;
        };
        if !include_tombstones && crate::atom::is_tombstone_value(value) {
            continue;
        }
        let fv = document_field_value(value.clone(), &atom, &record_uuid);
        results
            .entry(name.clone())
            .or_default()
            .insert(key.clone(), fv);
    }
    Ok(Some(results))
}

/// Resolve document fields for an explicit candidate set in its selected scope.
///
/// For each key whose R tip is a **complete** document envelope (`v==1` and
/// `complete==true`), field values come from `fields`. A name absent from a
/// complete envelope is dropped (missing). An omitted name in an incomplete
/// envelope keeps its field-molecule value. An explicit tombstone follows the
/// requested visibility rule. Keys without an envelope keep the field values.
pub(crate) async fn overlay_document_fields(
    db_ops: &DbOperations,
    schema: &Schema,
    keys: &[KeyValue],
    scope: Option<&str>,
    results: &mut HashMap<String, HashMap<KeyValue, FieldValue>>,
    include_tombstones: bool,
) -> Result<(), SchemaError> {
    // Catalog `molecule_uuid` is set after compact mints R. Keys without an
    // envelope tip still zip — a missing tip is not an empty document.
    let Some(record_uuid) = schema.molecule_uuid.clone() else {
        return Ok(());
    };

    let atoms = load_record_tip_atoms(db_ops, &record_uuid, keys, scope, Some(schema)).await?;
    for (key, atom) in atoms {
        let Some(fields) = parse_document_fields(atom.content()) else {
            continue;
        };
        let complete = envelope_is_complete(atom.content());
        for (field_name, by_key) in results.iter_mut() {
            if is_record_molecule_field(field_name) {
                by_key.remove(&key);
                continue;
            }
            match fields.get(field_name) {
                Some(value) if include_tombstones || !crate::atom::is_tombstone_value(value) => {
                    by_key.insert(
                        key.clone(),
                        document_field_value(value.clone(), &atom, &record_uuid),
                    );
                }
                Some(_) => {
                    by_key.remove(&key);
                }
                None => {
                    if complete {
                        by_key.remove(&key);
                    }
                }
            }
        }
    }
    Ok(())
}

/// The envelope stores no per-field metadata. Do not attribute its fields to
/// either an older field atom or the writer of the whole document.
fn document_field_value(value: serde_json::Value, atom: &Atom, molecule: &str) -> FieldValue {
    FieldValue {
        value,
        atom_uuid: atom.uuid().to_string(),
        source_file_name: None,
        metadata: None,
        molecule_uuid: Some(molecule.to_owned()),
        molecule_version: None,
        writer_pubkey: None,
        written_at: None,
    }
}

pub(super) async fn load_record_tip_atom(
    db_ops: &DbOperations,
    record_uuid: &str,
    key: &KeyValue,
) -> Result<Option<Atom>, SchemaError> {
    Ok(
        load_record_tip_atoms(db_ops, record_uuid, std::slice::from_ref(key), None, None)
            .await?
            .remove(key),
    )
}

pub(crate) fn record_scope(schema: &Schema) -> Option<&str> {
    schema
        .runtime_fields
        .get(super::RECORD_SENTINEL)
        .or_else(|| {
            schema
                .key
                .as_ref()
                .and_then(|key| key.hash_field.as_deref())
                .and_then(|name| schema.runtime_fields.get(name))
        })
        .and_then(|field| field.common().storage_prefix())
}
