//! Compact field atoms onto one record molecule for keys under one hash.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde_json::{Map, Value};

use super::dual_read::load_record_tip_atom;
use super::envelope::stamp_document_envelope_marked;
use super::{
    declared_runtime_field_names, is_record_molecule_field, record_molecule_uuid, RECORD_SENTINEL,
};
use crate::fold_db_core::query::HashRangeQueryProcessor;
use crate::schema::types::field::{FieldValue, HashRangeFilter};
use crate::schema::types::operations::MutationType;
use crate::schema::types::{KeyValue, Mutation};
use crate::schema::{Schema, SchemaError};
use crate::FoldDB;

/// How many keys compact stamped onto R for one hash.
///
/// `keys_compacted` counts only keys whose envelope was written. A key whose
/// field zip was empty, or missed a live field tip, is left alone and counted
/// in `keys_skipped`. Before 2026-09-22 a skipped key was still reported as
/// compacted, which hid the Mini lane flake in
/// `compact_blindv1_hashrangekey_stamps_envelope_from_api_and_storage_hash`
/// (the seed was not durable yet, the zip came back empty, and the report said 1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactReport {
    pub schema_name: String,
    pub hash: String,
    pub keys_compacted: usize,
    pub keys_skipped: usize,
    pub record_molecule_uuid: String,
}

/// Compact every live key under `hash` on `schema_name` onto record molecule R.
///
/// Key discovery is a prefix list of the hash-field molecule
/// (`hash_range_scan_prefix_for_hash` / `list_live_record_keys_filtered`).
/// That is O(log M) under one hash, not a product scan.
pub async fn compact_record_molecule(
    db: &FoldDB,
    schema_name: &str,
    hash: &str,
) -> Result<CompactReport, SchemaError> {
    compact_record_molecule_key(db, schema_name, hash, None).await
}

/// Compact one `(hash, range)` or every range under `hash` when `range` is None.
pub async fn compact_record_molecule_key(
    db: &FoldDB,
    schema_name: &str,
    hash: &str,
    range: Option<&str>,
) -> Result<CompactReport, SchemaError> {
    let mut schema = db
        .schema_manager()
        .get_schema_metadata(schema_name)?
        .ok_or_else(|| SchemaError::InvalidData(format!("schema '{schema_name}' not found")))?;

    let hash_field = schema
        .key
        .as_ref()
        .and_then(|k| k.hash_field.as_ref())
        .cloned()
        .ok_or_else(|| {
            SchemaError::InvalidData(format!(
                "schema '{schema_name}' has no hash_field; compact lists keys under that molecule"
            ))
        })?;

    let hash_molecule = schema
        .runtime_fields
        .get(&hash_field)
        .and_then(|field| field.common().molecule_uuid().cloned())
        .unwrap_or_else(|| crate::atom::deterministic_molecule_uuid(schema_name, &hash_field));

    let record_uuid = schema
        .molecule_uuid
        .clone()
        .unwrap_or_else(|| record_molecule_uuid(schema_name));
    schema.molecule_uuid = Some(record_uuid.clone());
    schema.ensure_record_molecule_runtime_field();
    db.schema_manager().update_schema(&schema).await?;

    let declared = declared_runtime_field_names(&schema);
    if let Some(only_range) = range {
        let stamped =
            compact_one_key(db, schema_name, &declared, &hash_molecule, hash, only_range).await?;
        return Ok(CompactReport {
            schema_name: schema_name.to_string(),
            hash: hash.to_string(),
            keys_compacted: usize::from(stamped),
            keys_skipped: usize::from(!stamped),
            record_molecule_uuid: record_uuid,
        });
    }
    let mut after: Option<String> = None;
    let mut keys_compacted = 0usize;
    let mut keys_skipped = 0usize;
    const PAGE: usize = 256;

    loop {
        let (page, next, has_more) = db
            .db_ops()
            .atoms()
            .list_live_record_keys_filtered(
                &hash_molecule,
                PAGE,
                after.as_deref(),
                None,
                Some(hash),
            )
            .await?;

        for (page_hash, page_range) in &page {
            if page_hash != hash {
                continue;
            }
            if let Some(want) = range {
                if page_range != want {
                    continue;
                }
            }
            if compact_one_key(db, schema_name, &declared, &hash_molecule, hash, page_range).await?
            {
                keys_compacted += 1;
            } else {
                keys_skipped += 1;
            }
        }

        if !has_more {
            break;
        }
        after = next;
        if after.is_none() {
            break;
        }
    }

    Ok(CompactReport {
        schema_name: schema_name.to_string(),
        hash: hash.to_string(),
        keys_compacted,
        keys_skipped,
        record_molecule_uuid: record_uuid,
    })
}

async fn compact_one_key(
    db: &FoldDB,
    schema_name: &str,
    declared: &[String],
    hash_molecule: &str,
    hash: &str,
    range: &str,
) -> Result<bool, SchemaError> {
    let (api_hash, api_range) = recover_api_hash_range(db, hash_molecule, hash, range).await?;
    let mut schema = db
        .schema_manager()
        .get_schema_metadata(schema_name)?
        .ok_or_else(|| SchemaError::InvalidData(format!("schema '{schema_name}' not found")))?;

    // Zip field molecules. Do not use QueryExecutor::query: HashRangeKey is
    // envelope-first and would re-read a partial R tip instead of healing.
    let filter = HashRangeFilter::HashRangeKey {
        hash: api_hash.clone(),
        range: api_range.clone(),
    };
    let processor = HashRangeQueryProcessor::new(Arc::clone(db.db_ops()));
    let zipped = processor
        .query_with_filter(&mut schema, declared, Some(filter), None, false)
        .await?;

    let api_key = KeyValue::new(Some(api_hash.clone()), Some(api_range.clone()));
    let storage_key = storage_key_for(db, hash_molecule, &api_hash, &api_range);
    let mut fields = Map::new();
    let mut live_missing = HashSet::new();
    for name in declared {
        if is_record_molecule_field(name) {
            continue;
        }
        let fv = zipped
            .get(name)
            .and_then(|by_key| field_value_for_key(by_key, &api_key, storage_key.as_ref()));
        if let Some(fv) = fv {
            fields.insert(name.clone(), fv.value.clone());
            continue;
        }
        if field_has_live_tip(db, &schema, name, &api_hash, &api_range).await? {
            live_missing.insert(name.clone());
        }
    }
    let Some(fields) = envelope_from_zip(&fields, &live_missing) else {
        // Empty zip, or a live field tip is missing from the zip. Do not stamp
        // a partial envelope that later hides that tip.
        return Ok(false);
    };
    let envelope = stamp_document_envelope_marked(fields, true);

    let mut values = HashMap::new();
    values.insert(RECORD_SENTINEL.to_string(), envelope);
    let mut mutation = Mutation::new(
        schema_name.to_string(),
        values,
        api_key,
        "record-molecule-compact".to_string(),
        MutationType::Update,
    );
    mutation.synchronous = Some(true);
    db.mutation_manager()
        .write_mutations_batch_async(vec![mutation], None)
        .await?;
    Ok(true)
}

fn push_unique(keys: &mut Vec<String>, key: String) {
    if !keys.iter().any(|k| k == &key) {
        keys.push(key);
    }
}

/// Exact key lookup. Never `values().next()` — that can copy another row's
/// FieldValue into this key's envelope on a BlindV1 miss.
fn field_value_for_key<'a>(
    by_key: &'a HashMap<KeyValue, FieldValue>,
    api_key: &KeyValue,
    storage_key: Option<&KeyValue>,
) -> Option<&'a FieldValue> {
    by_key
        .get(api_key)
        .or_else(|| storage_key.and_then(|key| by_key.get(key)))
}

fn storage_key_for(
    db: &FoldDB,
    hash_molecule: &str,
    api_hash: &str,
    api_range: &str,
) -> Option<KeyValue> {
    let atoms = db.db_ops().atoms();
    let storage_hash = atoms.storage_hash(hash_molecule, api_hash).ok()?;
    let storage_range = atoms.storage_range(hash_molecule, api_range).ok()?;
    if storage_hash == api_hash && storage_range == api_range {
        return None;
    }
    Some(KeyValue::new(Some(storage_hash), Some(storage_range)))
}

/// Stamp only when the zip is non-empty and every live field tip is present.
fn envelope_from_zip(
    fields: &Map<String, Value>,
    live_missing: &HashSet<String>,
) -> Option<Map<String, Value>> {
    if !live_missing.is_empty() || fields.is_empty() {
        return None;
    }
    Some(fields.clone())
}

async fn field_has_live_tip(
    db: &FoldDB,
    schema: &Schema,
    field_name: &str,
    api_hash: &str,
    api_range: &str,
) -> Result<bool, SchemaError> {
    let mol = schema
        .runtime_fields
        .get(field_name)
        .and_then(|field| field.common().molecule_uuid().cloned())
        .unwrap_or_else(|| crate::atom::deterministic_molecule_uuid(&schema.name, field_name));
    let key = KeyValue::new(Some(api_hash.to_string()), Some(api_range.to_string()));
    Ok(load_record_tip_atom(db.db_ops(), &mol, &key)
        .await?
        .is_some())
}

/// API `(hash, range)` for HashRangeKey.
///
/// HashRangeKey blinds its hash. A Page walk on BlindV1 can emit the storage
/// token instead of the API hash; compact would zip empty and skip. Recover
/// the API hash from the hash-field atom at this slot (O(1) point reads).
async fn recover_api_hash_range(
    db: &FoldDB,
    hash_molecule: &str,
    hash: &str,
    range: &str,
) -> Result<(String, String), SchemaError> {
    let atoms = db.db_ops().atoms();
    let codec = atoms.key_codec_for_molecule(hash_molecule);
    let api_range = crate::crypto::E2eKeys::ope_decode_range_plaintext(range)
        .unwrap_or_else(|| range.to_string());

    if let Some(tip) = db.resident().resolve_tip(hash_molecule, hash, range) {
        if let Some(atom) = atoms.get_atom_by_uuid(&tip.value.atom_uuid, None).await? {
            if let Some(api_hash) = atom.content().as_str().filter(|s| !s.is_empty()) {
                return Ok((api_hash.to_string(), api_range));
            }
        }
    }

    let mut record_keys: Vec<String> = Vec::new();
    push_unique(
        &mut record_keys,
        crate::atom::molecule_key_codec::hash_range_record_key(hash_molecule, hash, range),
    );
    if let Ok(sr) = codec.storage_range(hash_molecule, range) {
        push_unique(
            &mut record_keys,
            crate::atom::molecule_key_codec::hash_range_record_key(hash_molecule, hash, &sr),
        );
    }
    if api_range != range {
        if let Ok(sr) = codec.storage_range(hash_molecule, &api_range) {
            push_unique(
                &mut record_keys,
                crate::atom::molecule_key_codec::hash_range_record_key(hash_molecule, hash, &sr),
            );
        }
    }
    for (h, r) in [(hash, range), (hash, api_range.as_str())] {
        if let Ok(keys) = codec.api_hash_range_record_keys_for_read(hash_molecule, h, r) {
            for k in keys {
                push_unique(&mut record_keys, k);
            }
        }
    }

    for rec_key in &record_keys {
        let rec: Option<crate::db_operations::atom_store::PerKeyRecord> = atoms
            .raw()
            .get_item(rec_key)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("hash-field tip: {e}")))?;
        let Some(rec) = rec else {
            continue;
        };
        let Some(atom) = atoms.get_atom_by_uuid(&rec.entry.atom_uuid, None).await? else {
            continue;
        };
        if let Some(api_hash) = atom.content().as_str().filter(|s| !s.is_empty()) {
            return Ok((api_hash.to_string(), api_range));
        }
    }

    Ok((hash.to_string(), api_range))
}
