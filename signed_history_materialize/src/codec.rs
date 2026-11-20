//! JSON encoding of the one-field `{"v": "..."}` record used by proofs.

use serde_json::{json, Value};

/// Field name carried by schemas from `ensure_hash_schema`.
pub const STRING_FIELD: &str = "v";

/// Encode a string as the JSON object value of a history record.
#[must_use]
pub fn encode_string_field(v: &str) -> Vec<u8> {
    json!({ STRING_FIELD: v }).to_string().into_bytes()
}

/// Read `v` back from a value made by [`encode_string_field`].
pub fn decode_string_field(bytes: &[u8]) -> Result<String, String> {
    let value: Value =
        serde_json::from_slice(bytes).map_err(|e| format!("value is not JSON: {e}"))?;
    value
        .get(STRING_FIELD)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| format!("value has no string field {STRING_FIELD:?}"))
}
