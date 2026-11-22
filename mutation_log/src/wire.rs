//! Signature wire form and entry_hash.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::encode::signed_bytes;
use crate::entry::SignedEntry;
use crate::ids::EntryHash;

/// Encoding / wire parse failure.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum EncodeError {
    /// Signature is not valid base64url or not 64 bytes.
    #[error("signature must be base64url (no pad) encoding 64 bytes (86 chars)")]
    BadSignature,
    /// Too many seen_heads (> u16).
    #[error("too many seen_heads")]
    TooManySeenHeads,
    /// Duplicate hash in seen_heads.
    #[error("duplicate seen_head")]
    DuplicateSeenHead,
    /// Length-prefixed string exceeds u16.
    #[error("length-prefixed string too long")]
    StringTooLong,
    /// Payload exceeds u32.
    #[error("payload too long")]
    PayloadTooLong,
    /// grant_at flags set without both device and seq.
    #[error("grant_at requires both device and seq")]
    GrantAtIncomplete,
    /// Cutoffs exceed 256.
    #[error("too many cutoffs")]
    TooManyCutoffs,
    /// Heads tip count out of 1..=256.
    #[error("publisher_heads count must be 1..=256")]
    BadHeadsCount,
    /// Relays count out of 1..=32.
    #[error("relays count must be 1..=32")]
    BadRelaysCount,
    /// Snapshot positions: more than 1024, or not strictly ascending by device id.
    #[error("snapshot through must be at most 1024 positions, strictly ascending by device id")]
    BadSnapshotThrough,
}

/// Decode a wire signature (base64url, no pad, 86 chars → 64 bytes).
pub fn decode_signature_b64url(s: &str) -> Result<[u8; 64], EncodeError> {
    if s.len() != 86 {
        return Err(EncodeError::BadSignature);
    }
    let raw = URL_SAFE_NO_PAD
        .decode(s.as_bytes())
        .map_err(|_| EncodeError::BadSignature)?;
    raw.try_into().map_err(|_| EncodeError::BadSignature)
}

/// Encode raw 64-byte signature as base64url with no padding (86 chars).
#[must_use]
pub fn encode_signature_b64url(sig: &[u8; 64]) -> String {
    URL_SAFE_NO_PAD.encode(sig)
}

/// `entry_hash = SHA-256(signed_bytes || signature_raw_64)`.
pub fn entry_hash(entry: &SignedEntry) -> Result<EntryHash, EncodeError> {
    let bytes = signed_bytes(entry)?;
    let sig = decode_signature_b64url(&entry.header().signature)?;
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    hasher.update(sig);
    Ok(EntryHash::from_bytes(hasher.finalize().into()))
}

/// Verify Ed25519 signature over `signed_bytes` with `entry.node_key`.
pub fn verify_entry_signature(entry: &SignedEntry) -> Result<(), VerifyError> {
    let bytes = signed_bytes(entry).map_err(VerifyError::Encode)?;
    let sig = decode_signature_b64url(&entry.header().signature).map_err(VerifyError::Encode)?;
    let vk = app_identity_crypto::VerifyingKey::from_bytes(entry.header().node_key.as_bytes())
        .map_err(|_| VerifyError::BadNodeKey)?;
    if vk.is_weak() {
        return Err(VerifyError::WeakNodeKey);
    }
    app_identity_crypto::verify(&vk, &sig, &bytes).map_err(|_| VerifyError::BadSignature)
}

/// Signature verification failure.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum VerifyError {
    /// Could not encode signed bytes or decode wire signature.
    #[error(transparent)]
    Encode(#[from] EncodeError),
    /// `node_key` is not a valid Ed25519 point.
    #[error("node_key is not a valid Ed25519 public key")]
    BadNodeKey,
    /// Weak / small-order node key.
    #[error("node_key is a weak Ed25519 point")]
    WeakNodeKey,
    /// Signature does not verify.
    #[error("entry signature does not verify")]
    BadSignature,
}
