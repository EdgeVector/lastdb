//! Writer signature carried in front of a sealed log payload.
//!
//! A log entry has no signature field. The wire JSON rejects an unknown field,
//! and an existing payload starts with the sealed-envelope version `0x01`.
//! A direct-log writer therefore puts an Ed25519 signature in a frame in front
//! of that envelope. ExeMem stores `Entry.payload` and returns it. ExeMem does
//! not read the frame. A payload whose first byte is `0x01` is today's sealed
//! envelope, with no signature in front of it.
//!
//! The signature cannot sit inside the ciphertext. It covers the hash of the
//! sealed bytes. The frame is outside those bytes, so the stored payload still
//! begins with a version this crate understands.
//!
//! The signature is Ed25519 over the raw 32-byte digest from
//! [`writer_entry_digest`]. It is not over the hex of that digest. A share
//! settings statement signs hex. This digest does not.

use sha2::{Digest as _, Sha256};

use crate::LogError;

/// Domain string inside the writer-entry digest frame.
pub const WRITER_ENTRY_DOMAIN: &str = "lastdb:mutation-log:writer-entry:v1";
/// First byte of a payload that carries a writer signature.
pub const WRITER_FRAME_VERSION: u8 = 0x02;
/// First byte of a sealed envelope. This is `ENVELOPE_VERSION` in `fold_db`.
pub const SEALED_ENVELOPE_VERSION: u8 = 0x01;
/// Raw Ed25519 signature length. The frame stores these bytes, not hex.
pub const WRITER_SIGNATURE_LEN: usize = 64;
/// Version byte plus [`WRITER_SIGNATURE_LEN`]. The sealed envelope follows.
pub const WRITER_FRAME_PREFIX: usize = 1 + WRITER_SIGNATURE_LEN;

/// Values the writer signs. `incorporated_from` is not one of them.
///
/// `sealed_sha256` is the SHA-256 of the sealed envelope only. It is not the
/// hash of this frame, and it is not the hash of the whole payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WriterEntryBinding<'a> {
    /// Database id, UTF-8, the same label the entry stores.
    pub database_id: &'a str,
    /// Writer node key, UTF-8.
    pub writer_key: &'a str,
    /// Writer device id, UTF-8.
    pub device_id: &'a str,
    /// Writer sequence. Zero is rejected.
    pub writer_seq: u64,
    /// Author time in Unix milliseconds.
    pub authored_at_ms: u64,
    /// Namespace, without the schema name.
    pub namespace: &'a str,
    /// SHA-256 of the sealed envelope bytes.
    pub sealed_sha256: &'a [u8; 32],
}

/// What [`carried_payload`] found. Borrows the stored payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CarriedPayload<'a> {
    /// The payload is a sealed envelope. Its first byte is `0x01`.
    Sealed(&'a [u8]),
    /// A writer signature, then the sealed envelope.
    Framed {
        /// Raw 64-byte Ed25519 signature.
        signature: &'a [u8; WRITER_SIGNATURE_LEN],
        /// Sealed envelope. Its first byte is `0x01`.
        sealed: &'a [u8],
    },
}

/// SHA-256 of a sealed envelope. Pass the result to [`WriterEntryBinding`].
#[must_use]
pub fn sealed_sha256(sealed: &[u8]) -> [u8; 32] {
    copy_hash(&Sha256::digest(sealed))
}

/// Domain-separated SHA-256 of the writer binding.
///
/// The outer frame matches a protocol `input_digest`: a big-endian u32 domain
/// length, the UTF-8 domain [`WRITER_ENTRY_DOMAIN`], a big-endian u64 body
/// length, then the body. The body is not JSON. In order it is a big-endian
/// u32 byte length and UTF-8 for `database_id`, `writer_key`, and `device_id`,
/// then `writer_seq` and `authored_at_ms` as big-endian u64, then a big-endian
/// u32 byte length and UTF-8 for `namespace`, then the 32 raw hash bytes.
///
/// Sign the returned 32 bytes. Do not sign their hex.
pub fn writer_entry_digest(binding: &WriterEntryBinding<'_>) -> Result<[u8; 32], LogError> {
    if binding.writer_seq == 0 || empty_label(binding) {
        return Err(LogError::InvalidEntry);
    }
    let mut body = Vec::new();
    push_str(&mut body, binding.database_id)?;
    push_str(&mut body, binding.writer_key)?;
    push_str(&mut body, binding.device_id)?;
    body.extend_from_slice(&binding.writer_seq.to_be_bytes());
    body.extend_from_slice(&binding.authored_at_ms.to_be_bytes());
    push_str(&mut body, binding.namespace)?;
    body.extend_from_slice(binding.sealed_sha256);
    framed_sha256(&body)
}

/// Split a stored payload into a sealed envelope, or a frame plus that envelope.
///
/// This does not verify the signature. A first byte of `0x01` returns the
/// whole slice. A first byte of `0x02` requires the 64 signature bytes and a
/// remainder that starts with `0x01`. Any other shape is [`LogError::InvalidEntry`].
pub fn carried_payload(payload: &[u8]) -> Result<CarriedPayload<'_>, LogError> {
    match payload.first().copied() {
        Some(SEALED_ENVELOPE_VERSION) => Ok(CarriedPayload::Sealed(payload)),
        Some(WRITER_FRAME_VERSION) => framed_payload(payload),
        _ => Err(LogError::InvalidEntry),
    }
}

/// Put `signature` in front of `sealed`. `sealed` must start with `0x01`.
pub fn attach_writer_signature(
    signature: &[u8; WRITER_SIGNATURE_LEN],
    sealed: &[u8],
) -> Result<Vec<u8>, LogError> {
    if sealed.first() != Some(&SEALED_ENVELOPE_VERSION) {
        return Err(LogError::InvalidEntry);
    }
    let mut payload = Vec::with_capacity(WRITER_FRAME_PREFIX + sealed.len());
    payload.push(WRITER_FRAME_VERSION);
    payload.extend_from_slice(signature);
    payload.extend_from_slice(sealed);
    Ok(payload)
}

fn framed_payload(payload: &[u8]) -> Result<CarriedPayload<'_>, LogError> {
    if payload.len() < WRITER_FRAME_PREFIX {
        return Err(LogError::InvalidEntry);
    }
    let signature: &[u8; WRITER_SIGNATURE_LEN] = payload[1..WRITER_FRAME_PREFIX]
        .try_into()
        .map_err(|_| LogError::InvalidEntry)?;
    let sealed = &payload[WRITER_FRAME_PREFIX..];
    if sealed.first() != Some(&SEALED_ENVELOPE_VERSION) {
        return Err(LogError::InvalidEntry);
    }
    Ok(CarriedPayload::Framed { signature, sealed })
}

fn empty_label(binding: &WriterEntryBinding<'_>) -> bool {
    binding.database_id.is_empty()
        || binding.writer_key.is_empty()
        || binding.device_id.is_empty()
        || binding.namespace.is_empty()
}

fn push_str(body: &mut Vec<u8>, value: &str) -> Result<(), LogError> {
    let length = u32::try_from(value.len()).map_err(|_| LogError::InvalidEntry)?;
    body.extend_from_slice(&length.to_be_bytes());
    body.extend_from_slice(value.as_bytes());
    Ok(())
}

fn framed_sha256(body: &[u8]) -> Result<[u8; 32], LogError> {
    let domain_length =
        u32::try_from(WRITER_ENTRY_DOMAIN.len()).map_err(|_| LogError::InvalidEntry)?;
    let body_length = u64::try_from(body.len()).map_err(|_| LogError::InvalidEntry)?;
    let mut hash = Sha256::new();
    hash.update(domain_length.to_be_bytes());
    hash.update(WRITER_ENTRY_DOMAIN.as_bytes());
    hash.update(body_length.to_be_bytes());
    hash.update(body);
    Ok(copy_hash(&hash.finalize()))
}

fn copy_hash(hash: &[u8]) -> [u8; 32] {
    let mut out = [0u8; 32];
    out.copy_from_slice(hash);
    out
}
