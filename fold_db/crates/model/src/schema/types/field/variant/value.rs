use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// The value of one field of a record, as the condition pass reads it. The
/// metadata belongs to the record, not to the field (decision D7).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldValue {
    pub value: JsonValue,
}
