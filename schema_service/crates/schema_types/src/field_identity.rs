//! Canonical field hashes identify reusable definitions and types.
//! A matching hash does not share record values or connect schema field keys.

use crate::FieldValueType;
use sha2::{Digest, Sha256};

/// Canonical field hash used on catalog schemas and local runtime metadata.
///
/// Serialization is stable: sorted JSON object with fixed keys. Description is
/// trimmed; name is trimmed (not lowercased — case is part of identity).
#[must_use]
pub fn compute_field_hash(
    name: &str,
    description: &str,
    field_type: &FieldValueType,
    version: u32,
) -> String {
    let type_json = serde_json::to_string(field_type).unwrap_or_else(|_| "\"Any\"".to_string());
    // Fixed key order — do not use HashMap.
    let payload = format!(
        r#"{{"description":{},"name":{},"type":{},"version":{}}}"#,
        serde_json::to_string(description.trim()).unwrap_or_else(|_| "\"\"".into()),
        serde_json::to_string(name.trim()).unwrap_or_else(|_| "\"\"".into()),
        type_json,
        version
    );
    let digest = Sha256::digest(payload.as_bytes());
    crate::hex::hex_lower(&digest)
}

// ===========================================================================
// Declared-field identity (v2) — scoped to an SS-minted declaration id.
// ===========================================================================

/// Version of the **declared**-field identity algorithm.
///
/// v1 is [`compute_field_hash`] above: network-global, keyed on
/// name + description + type + version. That scope is right for answering
/// *what does this field mean* (search, sensitivity, classification) and wrong
/// for answering *should a write here land there*. Measured on the live
/// primary: `created_at` / `"RFC 3339 timestamp"` / `String` is byte-identical
/// across **46 unrelated schemas**.
///
/// v2 scopes identity to a declaration instead of to meaning. Two fields are
/// the same field only when someone passed the same handle.
pub const DECLARED_FIELD_IDENTITY_ALGO_VERSION: u32 = 2;

#[must_use]
pub fn compute_declared_field_identity(
    declaration_id: &str,
    field_name: &str,
    field_type: &FieldValueType,
    version: u32,
) -> String {
    let type_json = serde_json::to_string(field_type).unwrap_or_else(|_| "\"Any\"".to_string());
    // Fixed key order — do not use HashMap. `algo` is inside the digest so a
    // future version cannot collide with this one even on identical inputs.
    let payload = format!(
        r#"{{"algo":{},"declaration":{},"name":{},"type":{},"version":{}}}"#,
        DECLARED_FIELD_IDENTITY_ALGO_VERSION,
        serde_json::to_string(declaration_id.trim()).unwrap_or_else(|_| "\"\"".into()),
        serde_json::to_string(field_name.trim()).unwrap_or_else(|_| "\"\"".into()),
        type_json,
        version
    );
    let digest = Sha256::digest(payload.as_bytes());
    crate::hex::hex_lower(&digest)
}
