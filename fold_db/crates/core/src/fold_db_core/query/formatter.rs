use crate::schema::types::key_value::KeyValue;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// The metadata of one record (decision D7): the source file name and the
/// metadata map of the last write that carried them. A field has no metadata
/// of its own.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RecordMetadata {
    pub source_file_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,
}

/// The selected fields of one record, keyed by `KeyValue` in [`QueryRecords`].
/// The `fields` map stores field_name -> value. `metadata` is the one
/// metadata block of the record, or `None` when the record has none.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub fields: HashMap<String, Value>,
    pub metadata: Option<RecordMetadata>,
}

/// The reply of one record query: the selected fields of each record, by
/// its API key.
pub type QueryRecords = HashMap<KeyValue, Record>;
