//! Document envelope for one record molecule tip.
//!
//! Compact and document writes stamp `{ "v": 1, "fields": { ... } }`. Dual-read
//! classifies per key with this shape, not `Value::is_object()` — field atoms
//! are already JSON objects.

use serde_json::{json, Map, Value};

/// Envelope version stamped by compact and document writes.
pub const ENVELOPE_VERSION: u64 = 1;

/// True when `value` is a document envelope (`v == 1` and `fields` is an object).
/// Unknown `v` is not an envelope (forward-safe: dual-read zips).
#[must_use]
pub fn is_document_envelope(value: &Value) -> bool {
    parse_document_fields(value).is_some()
}

/// Project the `fields` object from a document envelope. `None` if this is not
/// an envelope (missing tip, field-shaped JSON, unknown `v`).
#[must_use]
pub fn parse_document_fields(value: &Value) -> Option<&Map<String, Value>> {
    let obj = value.as_object()?;
    let v = obj.get("v")?.as_u64()?;
    if v != ENVELOPE_VERSION {
        return None;
    }
    obj.get("fields")?.as_object()
}

/// Stamp a document envelope around `fields`.
///
/// Compact sets `complete: true` only when every declared field that still
/// has a live tip is in `fields`. Dual-read treats a missing `complete` flag
/// as incomplete (fail-closed): overlay keeps the zip, HashRangeKey does not
/// take the envelope-first path.
#[must_use]
pub fn stamp_document_envelope(fields: Map<String, Value>) -> Value {
    stamp_document_envelope_marked(fields, true)
}

/// Stamp a document envelope and set the completeness flag.
#[must_use]
pub fn stamp_document_envelope_marked(fields: Map<String, Value>, complete: bool) -> Value {
    json!({
        "v": ENVELOPE_VERSION,
        "fields": Value::Object(fields),
        "complete": complete,
    })
}

/// True when this envelope is a proven complete document (`complete == true`).
///
/// A `v==1` envelope without the flag is not complete. Compact used to stamp
/// partial `{v:1, fields}` maps; those must not hide live field-molecule tips.
#[must_use]
pub fn envelope_is_complete(value: &Value) -> bool {
    value
        .as_object()
        .and_then(|obj| obj.get("complete"))
        .and_then(Value::as_bool)
        == Some(true)
}

/// Merge sent mutation keys into an envelope `fields` object. Unsent keys stay.
/// `RECORD_SENTINEL` is never a document key.
pub fn merge_sent_fields(
    fields: &mut Map<String, Value>,
    sent: &std::collections::HashMap<String, Value>,
) {
    for (name, value) in sent {
        if name == crate::schema::types::RECORD_SENTINEL {
            continue;
        }
        fields.insert(name.clone(), value.clone());
    }
}
