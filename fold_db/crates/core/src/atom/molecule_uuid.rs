//! Molecule UUID encoding — the 256-bit `sha256(schema:field)` identity that
//! sits in every `mk:` / `mord:` / `atom:mk:` key.
//!
//! ## Why this module exists
//!
//! Until 2026-08-26 the digest was written as 64 ASCII hex characters
//! (`format!("{:x}")`). That hex run is 89.7% of live key bytes on the
//! measured primary (brain
//! `decision-2026-08-23-key-encoding-is-the-term-worth-fixing-row-count-is-the-larger-lever`).
//! Unpadded base64url is 43 characters for the same 32 bytes — 21 bytes less
//! per occurrence, a 591 MiB ceiling on segment bytes, with no new structure.
//!
//! ## Write vs read
//!
//! - **Write form:** [`encode_molecule_uuid_bytes`] (base64url, no pad).
//! - **Legacy form:** [`encode_molecule_uuid_hex`] (lowercase hex).
//! - **Read:** [`molecule_uuid_read_candidates`] returns write form first,
//!   then the other encoding when the input is a known 32-byte digest
//!   spelling. Dual-read is the partition-prefix precedent: new writes use
//!   one encoding; lookups try both. Phase 2 (a later card) rekeys and
//!   retires the hex form.
//!
//! The UUID is a molecule *identity*, not a range segment. Prefix scans are
//! per-molecule (`mk:{M}:…`); they never order one molecule against another.
//! Range order *inside* a molecule lives after the `\0` separator and is
//! unchanged. Blind HashKey / OPE RangeKey HMAC-bind `M`, so a read
//! candidate must rebuild hash and range under the same `M` — do not mix
//! encodings inside one key.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use sha2::{Digest, Sha256};

/// Lowercase hex spelling of a 32-byte digest (the pre-2026-08-26 write form).
pub const MOLECULE_UUID_HEX_LEN: usize = 64;
/// Unpadded base64url spelling of a 32-byte digest (the current write form).
pub const MOLECULE_UUID_B64URL_LEN: usize = 43;

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
pub fn encode_molecule_uuid_hex(digest: &[u8; 32]) -> String {
    let mut out = String::with_capacity(MOLECULE_UUID_HEX_LEN);
    for &b in digest {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

/// Legacy hex identity — the form already on disk for existing homes.
#[must_use]
pub fn legacy_hex_molecule_uuid(schema_name: &str, field_name: &str) -> String {
    encode_molecule_uuid_hex(&molecule_uuid_digest(schema_name, field_name))
}

/// Decode a write-form or legacy-hex molecule UUID to the 32-byte digest.
#[must_use]
pub fn parse_molecule_uuid_bytes(uuid: &str) -> Option<[u8; 32]> {
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
pub fn molecule_uuid_alt_encoding(uuid: &str) -> Option<String> {
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

/// Read candidates for a molecule UUID: current spelling first, then the
/// other encoding when both name the same 32-byte digest.
#[must_use]
pub fn molecule_uuid_read_candidates(uuid: &str) -> Vec<String> {
    let mut out = Vec::with_capacity(2);
    out.push(uuid.to_string());
    if let Some(alt) = molecule_uuid_alt_encoding(uuid) {
        if alt != uuid {
            out.push(alt);
        }
    }
    out
}
