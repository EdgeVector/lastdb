use crate::schema::types::key_value::KeyValue;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// Metadata associated with a field value
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldMetadata {
    pub source_file_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub molecule_uuid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub writer_pubkey: Option<String>,
}

/// The selected fields of one record, keyed by `KeyValue` in [`QueryRecords`].
/// The `fields` map stores field_name -> value.
/// The `metadata` map stores field_name -> the record metadata (decision D7).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub fields: HashMap<String, Value>,
    pub metadata: HashMap<String, FieldMetadata>,
}

/// The reply of one record query: the selected fields of each record, by
/// its API key.
pub type QueryRecords = HashMap<KeyValue, Record>;
