//! Canonical native cloud wire types. This crate performs no I/O and enables no capability.
//!
//! Both the client and service consume this source. The service binding follows
//! through its existing LastDB submodule after the canonical source lands.
#![warn(missing_docs)]

mod common;
mod primitives;
pub mod v2;

pub use common::*;
pub use primitives::{AccountId, DecimalU64, Digest, Id, Text};

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

/// Safe wire error class. The value carries no credential, URL, key, or record body.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// A field failed a protocol check. `ProtocolError::invalid` uses this code.
    InvalidInput,
    /// The requested capability is not available.
    UnsupportedCapability,
    /// The caller is not authorized for this operation.
    Unauthorized,
    /// The named object is absent. Callers distinguish a missing stream by `field`.
    NotFound,
    /// The account generation does not match.
    GenerationConflict,
    /// An identity does not match the frozen input.
    IdentityConflict,
    /// The predecessor head does not match.
    PredecessorConflict,
    /// A size or count limit was exceeded. `ProtocolError::limit` uses this code.
    ResourceLimit,
    /// The account quota was exceeded.
    QuotaExceeded,
    /// A required dependency is absent.
    MissingDependency,
    /// Stored bytes do not match the declared digest.
    IntegrityMismatch,
    /// The provider has not finished. This is not a terminal failure.
    ProviderPending,
    /// The provider outcome is not known. This is not a terminal failure.
    OutcomeUnknown,
    /// The operation can be retried. This is not a terminal failure.
    RetryLater,
}

/// A safe error classification. No credential, URL, key, or record body is present.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProtocolError {
    /// Which class of failure this is.
    pub code: ErrorCode,
    /// Protocol field the error refers to. This is a static name, not a caller value.
    pub field: String,
}

impl ProtocolError {
    /// Build an `InvalidInput` error naming `field`.
    pub fn invalid(field: &'static str) -> Self {
        Self {
            code: ErrorCode::InvalidInput,
            field: field.into(),
        }
    }

    /// Build a `ResourceLimit` error naming `field`.
    pub fn limit(field: &'static str) -> Self {
        Self {
            code: ErrorCode::ResourceLimit,
            field: field.into(),
        }
    }

    /// Build an `Unauthorized` error naming `field`.
    pub fn unauthorized(field: &'static str) -> Self {
        Self {
            code: ErrorCode::Unauthorized,
            field: field.into(),
        }
    }

    /// True when the service refused a paid cloud operation for an inactive subscription.
    #[must_use]
    pub fn is_payment_subscription(&self) -> bool {
        self.code == ErrorCode::Unauthorized && self.field == "payment.subscription"
    }
}

impl std::fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code, self.field)
    }
}

impl std::error::Error for ProtocolError {}

/// RFC 8785 bytes. Protocol u64 fields are strings, never binary64 numbers.
pub fn canonical_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, ProtocolError> {
    let value = serde_json::to_value(value).map_err(|_| ProtocolError::invalid("json"))?;
    json_canon::to_vec(&value).map_err(|_| ProtocolError::invalid("canonical_json"))
}

/// Hash framed domain and canonical input; this is not an origin-mutation signature.
pub(crate) fn input_digest<T: Serialize>(domain: &str, value: &T) -> Result<Digest, ProtocolError> {
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
