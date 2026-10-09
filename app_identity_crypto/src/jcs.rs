use serde_json::Value;

use crate::error::CanonicalizeError;

/// Canonicalize a JSON value per RFC 8785 (JCS).
///
/// Returns the canonical byte representation — UTF-8, no whitespace,
/// object keys sorted by UTF-16 code units, numbers in ECMAScript
/// shortest-roundtrip form. The bytes are stable input for hashing or
/// Ed25519 signing across Rust and TypeScript implementations.
pub fn canonicalize(value: &Value) -> Result<Vec<u8>, CanonicalizeError> {
    json_canon::to_vec(value).map_err(CanonicalizeError::from)
}
