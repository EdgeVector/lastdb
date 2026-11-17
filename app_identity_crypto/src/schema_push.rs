//! Publisher statement for `POST /v1/schemas`.
//!
//! The CLI signs these bytes and Schema Service verifies the same bytes.

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use serde_json::{json, Value};

use super::{canonicalize, sha256_hex, verify, verifying_key_from_base64, SIGNATURE_LEN};

/// RFC 8785 bytes of the statement a publisher signs for one schema push.
///
/// `schemas` is the JSON array as sent, before normalization. `schemas_sha256`
/// is the lowercase hex SHA-256 of the RFC 8785 bytes of that array. The
/// returned bytes are the RFC 8785 encoding of an object whose only members
/// are `api_version`, `app_id`, `channel`, `schemas_sha256`, and `sha`.
///
/// # Errors
///
/// Returns an error when `schemas` is not a JSON array, or when either value
/// cannot be canonicalized.
pub fn schema_push_statement(
    api_version: u32,
    app_id: &str,
    channel: &str,
    schemas: &Value,
    sha: &str,
) -> Result<Vec<u8>, String> {
    if !schemas.is_array() {
        return Err("schemas must be a JSON array".into());
    }
    let canonical = canonicalize(schemas).map_err(|error| error.to_string())?;
    let schemas_sha256 = sha256_hex(&canonical);
    let statement = json!({
        "api_version": api_version,
        "app_id": app_id,
        "channel": channel,
        "schemas_sha256": schemas_sha256,
        "sha": sha,
    });
    canonicalize(&statement).map_err(|error| error.to_string())
}

/// Verify a publisher signature over [`schema_push_statement`] bytes.
///
/// `publisher_key` and `publisher_sig` are standard (padded) base64. The key
/// is 32 bytes and the signature is 64 bytes. Pass the statement bytes
/// unchanged.
///
/// # Errors
///
/// Returns an error when the key or signature is malformed, or when the
/// signature does not verify.
pub fn verify_schema_push(
    publisher_key: &str,
    publisher_sig: &str,
    statement: &[u8],
) -> Result<(), String> {
    let key = verifying_key_from_base64(publisher_key).map_err(|error| error.to_string())?;
    let raw = BASE64
        .decode(publisher_sig)
        .map_err(|error| error.to_string())?;
    let signature: [u8; SIGNATURE_LEN] = raw
        .try_into()
        .map_err(|_| "publisher signature must be 64 bytes".to_string())?;
    verify(&key, &signature, statement)
        .map_err(|_| "publisher signature does not verify".to_string())
}
