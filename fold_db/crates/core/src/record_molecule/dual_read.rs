//! Per-key dual-read: document envelope on R's tip, else today's field zip.

use std::collections::{HashMap, HashSet};

use super::envelope::{envelope_is_complete, parse_document_fields};
use super::is_record_molecule_field;
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
) -> Result<Option<HashMap<String, HashMap<KeyValue, FieldValue>>>, SchemaError> {
    let Some(record_uuid) = schema.molecule_uuid.clone() else {
        return Ok(None);
    };
    let key = KeyValue::new(Some(hash.to_string()), Some(range.to_string()));
    let Some(atom) = load_record_tip_atom(db_ops, &record_uuid, &key).await? else {
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
        let fv = FieldValue {
            value: value.clone(),
            atom_uuid: atom.uuid().to_string(),
            source_file_name: None,
            metadata: None,
            molecule_uuid: Some(record_uuid.clone()),
            molecule_version: None,
            writer_pubkey: None,
            written_at: None,
        };
        results
            .entry(name.clone())
            .or_default()
            .insert(key.clone(), fv);
    }
    Ok(Some(results))
}

/// Overlay envelope projection onto a zipped query result.
///
/// For each key whose R tip is a **complete** document envelope (`v==1` and
/// `complete==true`), field values come from `fields`. A name absent from a
/// complete envelope is dropped (missing). An incomplete envelope never drops
/// a zip value: compact used to stamp partial maps, and those must not hide
/// live field-molecule tips. Keys without an envelope keep the zip. A missing
/// R tip never empties a row.
pub async fn overlay_document_envelope(
    db_ops: &DbOperations,
    schema: &Schema,
    results: &mut HashMap<String, HashMap<KeyValue, FieldValue>>,
) -> Result<(), SchemaError> {
    // Catalog `molecule_uuid` is set after compact mints R. Keys without an
    // envelope tip still zip — a missing tip is not an empty document.
    let Some(record_uuid) = schema.molecule_uuid.clone() else {
        return Ok(());
    };

    let mut keys: HashSet<KeyValue> = HashSet::new();
    for (field_name, by_key) in results.iter() {
        if is_record_molecule_field(field_name) {
            continue;
        }
        keys.extend(by_key.keys().cloned());
    }

    for key in keys {
        let Some(atom) = load_record_tip_atom(db_ops, &record_uuid, &key).await? else {
            continue;
        };
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
                Some(value) => {
                    let entry = by_key.entry(key.clone()).or_insert_with(|| FieldValue {
                        value: value.clone(),
                        atom_uuid: atom.uuid().to_string(),
                        source_file_name: None,
                        metadata: None,
                        molecule_uuid: Some(record_uuid.clone()),
                        molecule_version: None,
                        writer_pubkey: None,
                        written_at: None,
                    });
                    entry.value = value.clone();
                    entry.atom_uuid = atom.uuid().to_string();
                    entry.molecule_uuid = Some(record_uuid.clone());
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

pub(super) async fn load_record_tip_atom(
    db_ops: &DbOperations,
    record_uuid: &str,
    key: &KeyValue,
) -> Result<Option<Atom>, SchemaError> {
    let hash = key.hash.as_deref().unwrap_or("");
    let range = key.range.as_deref().unwrap_or("");
    let mut candidates = vec![(hash.to_string(), range.to_string())];
    if let (Ok(sh), Ok(sr)) = (
        db_ops.atoms().storage_hash(record_uuid, hash),
        db_ops.atoms().storage_range(record_uuid, range),
    ) {
        if sh != hash || sr != range {
            candidates.push((sh, sr));
        }
    }
    let mut atom_uuid = None;
    for (h, r) in &candidates {
        if let Some(tip) = db_ops.resident().resolve_tip(record_uuid, h, r) {
            atom_uuid = Some(tip.value.atom_uuid);
            break;
        }
        let record_key = crate::atom::molecule_key_codec::hash_range_record_key(record_uuid, h, r);
        if let Some(rec) = db_ops
            .atoms()
            .raw()
            .get_item::<crate::db_operations::atom_store::PerKeyRecord>(&record_key)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("read record-molecule tip: {e}")))?
        {
            atom_uuid = Some(rec.entry.atom_uuid);
            break;
        }
    }
    let Some(atom_uuid) = atom_uuid else {
        return Ok(None);
    };
    db_ops.atoms().get_atom_by_uuid(&atom_uuid, None).await
}
