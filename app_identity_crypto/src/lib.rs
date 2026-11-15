//! Shared Ed25519, JCS (RFC 8785), and signed registry-list helpers.
//!
//! Used by `lastdb_node` for Ed25519 key parsing/signing/verification, RFC 8785
//! JSON canonicalization, and verification of signed app registry interface lists.
#![warn(missing_docs)]

mod ed25519;
mod registry_list;

use serde_json::Value;
use thiserror::Error;

pub use ed25519::{
    key_id, sign, verify, verifying_key_from_base64, SigningKey, VerifyingKey, PUBLIC_KEY_LEN,
    SIGNATURE_LEN,
};
pub use registry_list::{
    github_repo, interface_list_path, valid_app_id, valid_source, validate_channel,
    verify_index_bytes, IndexSignature, InterfaceList, ListEntry, LIST_CHANNELS, MAX_LIST_APPS,
    MAX_LIST_BYTES, REGISTRY_LIST_PUBKEY_B64,
};

#[derive(Debug, Error)]
/// JCS canonicalization failed.
pub enum CanonicalizeError {
    #[error("JCS canonicalization failed: {0}")]
    /// The JSON value could not be canonicalized.
    Serde(#[from] serde_json::Error),
}

/// Failure parsing a base64-encoded Ed25519 public key into a
/// [`VerifyingKey`].
#[derive(Debug, Error, PartialEq, Eq)]
pub enum KeyParseError {
    #[error("public key is not valid base64")]
    /// The public key text is not standard base64.
    NotBase64,
    #[error("public key is not 32 bytes")]
    /// The decoded public key is not 32 bytes.
    WrongLength,
    #[error("public key bytes are not a valid Ed25519 point")]
    /// The 32 bytes are not a canonical Ed25519 point.
    InvalidPoint,
    #[error("public key is a small-order (weak) Ed25519 point and cannot verify any signature")]
    /// The point is a small-order Ed25519 key and cannot verify a signature.
    WeakKey,
}

/// Encode `bytes` as lowercase hex (two characters per byte).
#[must_use]
pub fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

/// Canonicalize a JSON value per RFC 8785 (JCS).
///
/// Returns the canonical byte representation — UTF-8, no whitespace,
/// object keys sorted by UTF-16 code units, numbers in ECMAScript
/// shortest-roundtrip form. The bytes are stable input for hashing or
/// Ed25519 signing across Rust and TypeScript implementations.
pub fn canonicalize(value: &Value) -> Result<Vec<u8>, CanonicalizeError> {
    json_canon::to_vec(value).map_err(CanonicalizeError::from)
}
