use crate::schema::types::field::FieldValue;
use crate::schema::types::key_value::KeyValue;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// Metadata associated with a field value
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldMetadata {
    pub atom_uuid: String,
    pub source_file_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub molecule_uuid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub molecule_version: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub writer_pubkey: Option<String>,
}

/// Represents a single logical record keyed by `KeyValue`.
/// The `fields` map stores field_name -> value.
/// The `metadata` map stores field_name -> atom metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub fields: HashMap<String, Value>,
    pub metadata: HashMap<String, FieldMetadata>,
}

/// Convert field->(key->value) map into key->Record with field->value.
/// Does not return JSON; this is a typed structure for backend consumption.
///
/// Builds each `Record` directly in a single `HashMap<KeyValue, Record>`
/// instead of two parallel by-key maps joined at the end. The prior two-map
/// shape paid for a full `HashMap<String, FieldMetadata>` deep-clone per row
/// (`metadata_by_key.get(&k).cloned()`) purely to move ownership from one map
/// into the other — a redundant O(fields) clone on every single row of every
/// query result. Measured cost: `lastdb-hydrate-per-row-floor-measured-20260803`
/// (brain).
pub fn records_from_field_map(
    results: &HashMap<String, HashMap<KeyValue, FieldValue>>,
) -> HashMap<KeyValue, Record> {
    let mut records: HashMap<KeyValue, Record> = HashMap::new();

    for (field_name, key_map) in results {
        for (key_value, field_val) in key_map {
            let record = records.entry(key_value.clone()).or_insert_with(|| Record {
                fields: HashMap::new(),
                metadata: HashMap::new(),
            });
            record
                .fields
                .insert(field_name.clone(), field_val.value.clone());
            record.metadata.insert(
                field_name.clone(),
                FieldMetadata {
                    atom_uuid: field_val.atom_uuid.clone(),
                    source_file_name: field_val.source_file_name.clone(),
                    metadata: field_val.metadata.clone(),
                    molecule_uuid: field_val.molecule_uuid.clone(),
                    molecule_version: field_val.molecule_version,
                    writer_pubkey: field_val.writer_pubkey.clone(),
                },
            );
        }
    }

    records
}
