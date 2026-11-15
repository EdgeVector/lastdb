//! Shared Ed25519, JCS (RFC 8785), and signed registry-list helpers.
//!
//! Used by `lastdb_node` for Ed25519 key parsing/signing/verification, RFC 8785
//! JSON canonicalization, and verification of signed app registry interface lists.
#![warn(missing_docs)]

mod registry_list;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use ed25519_dalek::{Signature, Signer};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;

pub use ed25519_dalek::{SigningKey, VerifyingKey};
pub use registry_list::{
    github_repo, interface_list_path, valid_app_id, valid_source, validate_channel, InterfaceList,
    ListEntry, LIST_CHANNELS, MAX_LIST_BYTES, REGISTRY_LIST_PUBKEY_B64,
};

const PUBLIC_KEY_LEN: usize = ed25519_dalek::PUBLIC_KEY_LENGTH;
pub(crate) const SIGNATURE_LEN: usize = ed25519_dalek::SIGNATURE_LENGTH;

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

/// Parse a base64 (standard alphabet, padded) Ed25519 public key into a
/// [`VerifyingKey`].
///
/// Rejects non-canonical y-coordinates (`y >= 2^255 - 19`) so [`key_id`] has
/// one accepted wire form per point, and rejects the eight small-order points
/// that fail [`VerifyingKey::verify_strict`].
pub fn verifying_key_from_base64(b64: &str) -> Result<VerifyingKey, KeyParseError> {
    let bytes = BASE64
        .decode(b64.as_bytes())
        .map_err(|_| KeyParseError::NotBase64)?;
    let arr: [u8; PUBLIC_KEY_LEN] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| KeyParseError::WrongLength)?;
    if !is_canonical_y(&arr) {
        return Err(KeyParseError::InvalidPoint);
    }
    let vk = VerifyingKey::from_bytes(&arr).map_err(|_| KeyParseError::InvalidPoint)?;
    if vk.is_weak() {
        return Err(KeyParseError::WeakKey);
    }
    Ok(vk)
}

fn is_canonical_y(bytes: &[u8; PUBLIC_KEY_LEN]) -> bool {
    for i in (0..PUBLIC_KEY_LEN).rev() {
        let y_byte = if i == 31 { bytes[i] & 0x7F } else { bytes[i] };
        let p_byte = match i {
            0 => 0xED,
            31 => 0x7F,
            _ => 0xFF,
        };
        if y_byte < p_byte {
            return true;
        }
        if y_byte > p_byte {
            return false;
        }
    }
    false
}

/// Sign `payload` with `signing_key`. Returns the raw 64-byte signature.
pub fn sign(signing_key: &SigningKey, payload: &[u8]) -> [u8; SIGNATURE_LEN] {
    signing_key.sign(payload).to_bytes()
}

/// Verify that `sig` is a valid strict Ed25519 signature over `payload` under
/// `verifying_key`.
pub fn verify(
    verifying_key: &VerifyingKey,
    sig: &[u8; SIGNATURE_LEN],
    payload: &[u8],
) -> Result<(), ed25519_dalek::SignatureError> {
    let signature = Signature::from_bytes(sig);
    verifying_key.verify_strict(payload, &signature)
}

/// Derive the envelope `key_id` for a verifying key: the lowercase hex
/// SHA-256 of the 32-byte Ed25519 public key.
#[must_use]
pub fn key_id(verifying_key: &VerifyingKey) -> String {
    let digest = Sha256::digest(verifying_key.to_bytes());
    hex_lower(&digest)
}
