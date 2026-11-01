use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;

/// Resolved field value returned by query resolution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldValue {
    pub value: JsonValue,
    pub source_file_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field_key_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub writer_pubkey: Option<String>,
    /// Nanoseconds since the Unix epoch of the field stamp.
    /// Absent when the field has no stamp.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub written_at: Option<u64>,
}
