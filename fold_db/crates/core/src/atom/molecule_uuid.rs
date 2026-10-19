//! Deterministic molecule UUIDs use the base64url form of SHA-256(schema:field).
//!
//! Logical reads and writes use the supplied UUID exactly. A hex string is
//! not an alias for its base64url digest. Only explicit legacy physical cleanup
//! enumerates both historical spellings through this module.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use sha2::{Digest, Sha256};

/// Lowercase hex spelling of a 32-byte digest (the pre-2026-08-26 write form).
const MOLECULE_UUID_HEX_LEN: usize = 64;
/// Unpadded base64url spelling of a 32-byte digest (the current write form).
const MOLECULE_UUID_B64URL_LEN: usize = 43;

const HEX: &[u8; 16] = b"0123456789abcdef";

/// SHA-256(`{schema}:{field}`) as 32 raw bytes.
#[must_use]
pub fn molecule_uuid_digest(schema_name: &str, field_name: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(format!("{schema_name}:{field_name}").as_bytes());
    hasher.finalize().into()
}

/// Current write form: unpadded base64url of the 32-byte digest (43 chars).
#[must_use]
pub fn encode_molecule_uuid_bytes(digest: &[u8; 32]) -> String {
    URL_SAFE_NO_PAD.encode(digest)
}

/// Legacy write form: lowercase hex of the 32-byte digest (64 chars).
#[must_use]
fn encode_molecule_uuid_hex(digest: &[u8; 32]) -> String {
    let mut out = String::with_capacity(MOLECULE_UUID_HEX_LEN);
    for &b in digest {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

/// Decode a historical UUID spelling for explicit physical cleanup.
#[must_use]
fn parse_molecule_uuid_bytes(uuid: &str) -> Option<[u8; 32]> {
    if uuid.len() == MOLECULE_UUID_HEX_LEN && uuid.bytes().all(|b| b.is_ascii_hexdigit()) {
        return crate::hex::hex_decode_array::<32>(uuid);
    }
    if uuid.len() == MOLECULE_UUID_B64URL_LEN {
        let raw = URL_SAFE_NO_PAD.decode(uuid.as_bytes()).ok()?;
        return <[u8; 32]>::try_from(raw).ok();
    }
    None
}

/// The other spelling of `uuid` when it is a 32-byte digest in write or hex form.
#[must_use]
fn legacy_alternate_spelling(uuid: &str) -> Option<String> {
    let bytes = parse_molecule_uuid_bytes(uuid)?;
    let write = encode_molecule_uuid_bytes(&bytes);
    let hex = encode_molecule_uuid_hex(&bytes);
    if uuid == write {
        (hex != write).then_some(hex)
    } else if uuid == hex {
        Some(write)
    } else {
        None
    }
}

/// Explicit physical cleanup only: requested identity, then its legacy twin.
/// Logical reads must use the supplied molecule identity without this expansion.
#[must_use]
pub(crate) fn legacy_molecule_uuid_spellings(uuid: &str) -> Vec<String> {
    let mut out = Vec::with_capacity(2);
    out.push(uuid.to_string());
    if let Some(alt) = legacy_alternate_spelling(uuid) {
        if alt != uuid {
            out.push(alt);
        }
    }
    out
}
