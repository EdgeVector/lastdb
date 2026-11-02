//! The edges from a schema field or an alternate key to its field key.

use serde::{Deserialize, Serialize};

/// One active source that binds a field key.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FieldKeyRefEdge {
    pub field_key_id: String,
    pub source: String,
    pub edge_type: String,
}

impl FieldKeyRefEdge {
    /// A schema field source. The JSON tuple preserves both names exactly,
    /// including separator text inside a schema or field name.
    #[must_use]
    pub fn schema_field(schema: &str, field: &str, field_key_id: &str) -> Self {
        Self {
            field_key_id: field_key_id.to_string(),
            source: format!(
                "schema:{}",
                serde_json::to_string(&(schema, field))
                    .expect("schema field identity is serializable")
            ),
            edge_type: "schema-field".to_string(),
        }
    }
}
