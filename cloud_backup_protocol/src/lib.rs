//! Canonical native cloud wire types. This crate performs no I/O and enables no capability.
//!
//! Both the client and service consume this source. The service binding follows
//! through its existing LastDB submodule after the canonical source lands.
#![warn(missing_docs)]

mod common;
mod error;
/// Request size bound re-exported from [`v2::limits`].
pub mod limits {
    /// Maximum canonical JSON bytes of a request.
    pub use crate::v2::limits::CONTROL_JSON_MAX_BYTES as REQUEST_JSON_MAX_BYTES;
}
mod primitives;
pub mod v2;

pub use common::*;
pub use error::{ErrorCode, PendingReason, ProtocolError, TerminalFailure, TerminalFailureCode};
pub use primitives::{AccountId, DecimalU64, Digest, Id, RelativePath, Text};

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

/// RFC 8785 bytes. Protocol u64 fields are strings, never binary64 numbers.
pub fn canonical_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, ProtocolError> {
    canonical_json_bytes(value, "json", "canonical_json")
}

/// Hash framed domain and canonical input; this is not an origin-mutation signature.
pub fn input_digest<T: Serialize>(domain: &str, value: &T) -> Result<Digest, ProtocolError> {
    let bytes = canonical_bytes(value)?;
    let domain_length =
        u32::try_from(domain.len()).map_err(|_| ProtocolError::invalid("digest.domain"))?;
    let body_length =
        u64::try_from(bytes.len()).map_err(|_| ProtocolError::invalid("digest.body"))?;
    framed_sha256_digest(domain_length, domain, body_length, &bytes)
}

/// SHA-256 of the exact stored bytes, without a domain frame.
pub fn bytes_sha256(bytes: &[u8]) -> Digest {
    let hash = Sha256::digest(bytes);
    Digest::try_from(encode_hex(&hash)).expect("SHA-256 produces 32 bytes")
}

/// Mini's existing keys and formats, not a new cryptographic construction.
/// BIP39 supplies 32 raw entropy bytes for the Ed25519 seed. The existing
/// E2eKeys derivation supplies the content and hash keys. Manifests use existing
/// envelope v1; embedded store keyrings are outside this profile.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryProfile {
    /// Mini's existing Ed25519 seed and E2E key derivation. This is not a new construction.
    MiniPortableV1,
}

/// Public recovery description. It holds no seed or per-file key.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryDescriptor {
    /// Which recovery construction this descriptor uses.
    pub profile: RecoveryProfile,
    /// Raw 32-byte Ed25519 public key, lowercase hex; this value is not a secret.
    pub account_public_key: Digest,
}

impl RecoveryDescriptor {
    /// Check that the first 16 bytes of SHA-256 over the raw public key equal the scope account id.
    pub fn validate_for(&self, scope: &CloudScope) -> Result<(), ProtocolError> {
        scope.validate()?;
        verify_account_public_key(
            &self.account_public_key,
            &scope.account_id,
            "recovery_descriptor.account_public_key",
        )
    }
}

pub(crate) fn canonical_json_bytes<T: Serialize>(
    value: &T,
    json_field: &'static str,
    canon_field: &'static str,
) -> Result<Vec<u8>, ProtocolError> {
    let value = serde_json::to_value(value).map_err(|_| ProtocolError::invalid(json_field))?;
    json_canon::to_vec(&value).map_err(|_| ProtocolError::invalid(canon_field))
}

pub(crate) fn framed_sha256_digest(
    domain_length: u32,
    domain: &str,
    body_length: u64,
    bytes: &[u8],
) -> Result<Digest, ProtocolError> {
    let mut hash = Sha256::new();
    hash.update(domain_length.to_be_bytes());
    hash.update(domain.as_bytes());
    hash.update(body_length.to_be_bytes());
    hash.update(bytes);
    Digest::try_from(encode_hex(&hash.finalize()))
}

pub(crate) fn verify_account_public_key(
    public_key: &Digest,
    account: &AccountId,
    field: &'static str,
) -> Result<(), ProtocolError> {
    let mut raw = [0u8; 32];
    for (out, pair) in raw
        .iter_mut()
        .zip(public_key.as_str().as_bytes().chunks_exact(2))
    {
        let pair = std::str::from_utf8(pair).map_err(|_| ProtocolError::invalid(field))?;
        *out = u8::from_str_radix(pair, 16).map_err(|_| ProtocolError::invalid(field))?;
    }
    let hash = Sha256::digest(raw);
    if encode_hex(&hash[..16]) != account.as_str() {
        return Err(ProtocolError::invalid(field));
    }
    Ok(())
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}
