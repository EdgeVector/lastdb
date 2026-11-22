//! The one-field `v` record that proofs write, in the layout of `RecordValue`.

use crate::value::RecordValue;

/// Field name carried by schemas from `ensure_hash_schema`.
pub const STRING_FIELD: &str = "v";

/// Encode a string as the value of a history record with one field `v`, no
/// alternate addresses and no file.
#[must_use]
pub fn encode_string_field(v: &str) -> Vec<u8> {
    RecordValue {
        fields: std::iter::once((STRING_FIELD.to_owned(), v.into())).collect(),
        ..RecordValue::default()
    }
    .encode()
}

/// Read `v` back from a value made by [`encode_string_field`].
pub fn decode_string_field(bytes: &[u8]) -> Result<String, String> {
    RecordValue::decode(bytes)?
        .fields
        .get(STRING_FIELD)
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| format!("value has no string field {STRING_FIELD:?}"))
}
