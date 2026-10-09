//! Field-level at-rest seal for [`Atom`](super::Atom) `content` only.
//!
//! Plain hash-group packaging leaves group files structural plaintext. Body
//! secrecy is this module: seal JSON content under the account E2E key while
//! leaving `uuid`, `source_schema_name`, `metadata`, and timestamps plain.
//!
//! ## Operation Trinity (Son)
//!
//! **Library / Mini binary default (unset env):** dual-read is **on** so
//! legacy plain content still opens during migrate. That is intentional so
//! an unresealed home is not bricked.
//!
//! **Trinity product posture (Tom's primary after reseal):** fail-closed
//! open via durable `LASTDB_ATOM_CONTENT_STRICT=1` on the LaunchAgent (or
//! `LASTDB_ATOM_CONTENT_DUAL_READ=0`). After `lastdb_reseal_atom_content`,
//! STRICT must remain set forever on product primaries — it is not a
//! temporary canary. Dual-read is the migrate-window escape, not the
//! long-term product default for a resealed home.

use crate::crypto::{
    is_sealed_at_rest, open_at_rest, seal_at_rest_deflate_raw, seal_at_rest_deflate_utf8,
    CryptoError, CryptoResult,
};
use serde_json::Value;

/// Binary atom-row marker: header length + JSON header + raw `ENB:` content.
pub const ATOM_BINARY_ROW_PREFIX: &[u8; 4] = b"ATB:";

const ATOM_BINARY_ROW_FIXED_BYTES: usize = ATOM_BINARY_ROW_PREFIX.len() + 4;
const ATOM_BINARY_HEADER_MAX_BYTES: usize = 16 * 1024 * 1024;

/// Whether new atom rows may use the binary content container.
///
/// Reads always accept the container. Writes default off so the reader can
/// reach every node before an older binary encounters the new row format.
#[must_use]
pub fn atom_content_binary_enabled() -> bool {
    matches!(
        env_flag::var_parse("LASTDB_ATOM_CONTENT_BINARY"),
        Some(true)
    )
}

/// When true, non-`ENC:` content is accepted (legacy migrate window).
///
/// # Default when env is unset
///
/// Returns **true** (dual-read). This is the safe migrate default so
/// pre-reseal rows still open. It is **not** the Trinity end-state.
///
/// # Trinity fail-closed (required on resealed product primaries)
///
/// Set `LASTDB_ATOM_CONTENT_STRICT=1` (or `true`/`yes`), or set
/// `LASTDB_ATOM_CONTENT_DUAL_READ=0`/`false`/`no`. STRICT wins when both
/// are set. Primary LaunchAgents that have completed reseal must keep
/// STRICT permanently.
#[must_use]
pub fn atom_content_dual_read_enabled() -> bool {
    if env_flag::var_truthy("LASTDB_ATOM_CONTENT_STRICT") {
        return false;
    }
    match env_flag::var_parse("LASTDB_ATOM_CONTENT_DUAL_READ") {
        Some(false) => false,
        // Explicit dual-read, or unset/unrecognized → dual-read (safe migrate default).
        _ => true,
    }
}

/// Seal atom content for storage. Returns a JSON string value `ENC:…` or
/// `ENZ:…` (the latter when compressing first was smaller).
///
/// Atom `content` is where LastDB's bytes actually live, and ciphertext cannot
/// be compressed after the fact — so this seals through
/// [`seal_at_rest_deflate`], which compresses before encrypting. On a real
/// sample of this workspace's brain + kanban atoms that returns ~52% of sealed
/// atom bytes while touching only ~7% of atoms; the rest fall under the size
/// floor and take the byte-identical `ENC:` path.
pub fn seal_content_value(key: &[u8; 32], content: &Value) -> CryptoResult<Value> {
    let plain = serde_json::to_vec(content)
        .map_err(|e| CryptoError::InvalidFormat(format!("serialize atom content: {e}")))?;
    let sealed = seal_at_rest_deflate_utf8(key, &plain)?;
    let s = String::from_utf8(sealed)
        .map_err(|e| CryptoError::InvalidFormat(format!("sealed content not utf8: {e}")))?;
    Ok(Value::String(s))
}

/// Seal an atom into a binary-safe row.
///
/// The JSON header keeps identity, schema, metadata, timestamps, and the
/// molecule-key-bundle selector readable. The content follows as a raw `ENB:`
/// envelope, so the inner seal has no base64 or JSON-string layer.
pub fn seal_atom_binary_row(key: &[u8; 32], atom: &Value) -> CryptoResult<Vec<u8>> {
    let mut header = atom.clone();
    let Some(object) = header.as_object_mut() else {
        return Err(CryptoError::InvalidFormat(
            "binary atom row must be a JSON object".to_string(),
        ));
    };
    let Some(content) = object.remove("content") else {
        return Err(CryptoError::InvalidFormat(
            "binary atom row is missing content".to_string(),
        ));
    };
    let plain = serde_json::to_vec(&content)
        .map_err(|e| CryptoError::InvalidFormat(format!("serialize atom content: {e}")))?;
    let sealed = seal_at_rest_deflate_raw(key, &plain)?;
    let header_bytes = serde_json::to_vec(&header)
        .map_err(|e| CryptoError::InvalidFormat(format!("serialize atom header: {e}")))?;
    if header_bytes.len() > ATOM_BINARY_HEADER_MAX_BYTES || header_bytes.len() > u32::MAX as usize {
        return Err(CryptoError::InvalidFormat(format!(
            "binary atom header exceeds {ATOM_BINARY_HEADER_MAX_BYTES} bytes"
        )));
    }

    let mut row =
        Vec::with_capacity(ATOM_BINARY_ROW_FIXED_BYTES + header_bytes.len() + sealed.len());
    row.extend_from_slice(ATOM_BINARY_ROW_PREFIX);
    row.extend_from_slice(&(header_bytes.len() as u32).to_be_bytes());
    row.extend_from_slice(&header_bytes);
    row.extend_from_slice(&sealed);
    Ok(row)
}

/// Parse a binary atom row into its plain header and sealed content bytes.
pub fn parse_atom_binary_row(stored: &[u8]) -> CryptoResult<Option<(Value, &[u8])>> {
    if !stored.starts_with(ATOM_BINARY_ROW_PREFIX) {
        return Ok(None);
    }
    if stored.len() < ATOM_BINARY_ROW_FIXED_BYTES {
        return Err(CryptoError::InvalidFormat(
            "binary atom row is missing its header length".to_string(),
        ));
    }
    let len_bytes: [u8; 4] = stored[ATOM_BINARY_ROW_PREFIX.len()..ATOM_BINARY_ROW_FIXED_BYTES]
        .try_into()
        .expect("four-byte slice");
    let header_len = u32::from_be_bytes(len_bytes) as usize;
    if header_len > ATOM_BINARY_HEADER_MAX_BYTES {
        return Err(CryptoError::InvalidFormat(format!(
            "binary atom header exceeds {ATOM_BINARY_HEADER_MAX_BYTES} bytes"
        )));
    }
    let content_start = ATOM_BINARY_ROW_FIXED_BYTES
        .checked_add(header_len)
        .filter(|end| *end <= stored.len())
        .ok_or_else(|| {
            CryptoError::InvalidFormat("binary atom row has a truncated header".to_string())
        })?;
    let header: Value = serde_json::from_slice(&stored[ATOM_BINARY_ROW_FIXED_BYTES..content_start])
        .map_err(|e| CryptoError::InvalidFormat(format!("deserialize atom header: {e}")))?;
    if !header.is_object() || header.get("content").is_some() {
        return Err(CryptoError::InvalidFormat(
            "binary atom header must be an object without content".to_string(),
        ));
    }
    let sealed = &stored[content_start..];
    if !sealed.starts_with(crate::crypto::AT_REST_ENC_BINARY_PREFIX.as_bytes()) {
        return Err(CryptoError::InvalidFormat(
            "binary atom content must use an ENB envelope".to_string(),
        ));
    }
    Ok(Some((header, sealed)))
}

/// Open a binary atom row and restore its logical JSON `content` field.
pub fn open_atom_binary_row(key: &[u8; 32], stored: &[u8]) -> CryptoResult<Option<Value>> {
    let Some((mut header, sealed)) = parse_atom_binary_row(stored)? else {
        return Ok(None);
    };
    let plain = open_at_rest(key, sealed)?;
    let content: Value = serde_json::from_slice(&plain)
        .map_err(|e| CryptoError::InvalidFormat(format!("deserialize sealed atom content: {e}")))?;
    header
        .as_object_mut()
        .expect("parser proved object")
        .insert("content".to_string(), content);
    Ok(Some(header))
}

/// Return an atom row's plain header without opening its content.
pub fn atom_row_header(stored: &[u8]) -> CryptoResult<Value> {
    if let Some((header, _)) = parse_atom_binary_row(stored)? {
        return Ok(header);
    }
    serde_json::from_slice(stored)
        .map_err(|e| CryptoError::InvalidFormat(format!("deserialize atom row: {e}")))
}

/// Open stored content.
///
/// - `ENC:…` → decrypt under `key`
/// - non-sealed → dual-read only when [`atom_content_dual_read_enabled`]; else fail closed
pub fn open_content_value(key: &[u8; 32], stored: Value) -> CryptoResult<Value> {
    match stored {
        Value::String(s) if is_sealed_at_rest(s.as_bytes()) => {
            let plain = open_at_rest(key, s.as_bytes())?;
            serde_json::from_slice(&plain).map_err(|e| {
                CryptoError::InvalidFormat(format!("deserialize sealed atom content: {e}"))
            })
        }
        other if atom_content_dual_read_enabled() => Ok(other),
        other => Err(CryptoError::InvalidFormat(format!(
            "atom content is not sealed (Operation Trinity strict open); got {}",
            content_kind(&other)
        ))),
    }
}

fn content_kind(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(s) if is_sealed_at_rest(s.as_bytes()) => "enc-string",
        Value::String(_) => "plain-string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Seal the `content` field of a serialized atom object in place.
pub fn seal_atom_json(key: &[u8; 32], atom: &mut Value) -> CryptoResult<()> {
    let Some(obj) = atom.as_object_mut() else {
        return Ok(());
    };
    let Some(content) = obj.get("content").cloned() else {
        return Ok(());
    };
    // Already sealed string — leave as-is (idempotent rewrite).
    if matches!(&content, Value::String(s) if is_sealed_at_rest(s.as_bytes())) {
        return Ok(());
    }
    obj.insert("content".to_string(), seal_content_value(key, &content)?);
    Ok(())
}

/// Open the `content` field of a serialized atom object in place.
pub fn open_atom_json(key: &[u8; 32], atom: &mut Value) -> CryptoResult<()> {
    let Some(obj) = atom.as_object_mut() else {
        return Ok(());
    };
    let Some(content) = obj.remove("content") else {
        return Ok(());
    };
    obj.insert("content".to_string(), open_content_value(key, content)?);
    Ok(())
}

/// Re-seal plain `content` in place (migrate helper). Idempotent for already-sealed.
pub fn reseal_atom_json_if_plain(key: &[u8; 32], atom: &mut Value) -> CryptoResult<bool> {
    let Some(obj) = atom.as_object_mut() else {
        return Ok(false);
    };
    let Some(content) = obj.get("content").cloned() else {
        return Ok(false);
    };
    if matches!(&content, Value::String(s) if is_sealed_at_rest(s.as_bytes())) {
        return Ok(false);
    }
    obj.insert("content".to_string(), seal_content_value(key, &content)?);
    Ok(true)
}
