use thiserror::Error;

#[derive(Debug, Error)]
pub enum CanonicalizeError {
    #[error("JCS canonicalization failed: {0}")]
    Serde(#[from] serde_json::Error),
}

#[derive(Debug, Error)]
pub enum SignError {
    #[error("envelope to sign must have sig = None")]
    SigAlreadySet,
    #[error(transparent)]
    Canonicalize(#[from] CanonicalizeError),
}

/// Failure parsing a base64-encoded Ed25519 public key into a
/// [`VerifyingKey`](crate::VerifyingKey).
#[derive(Debug, Error, PartialEq, Eq)]
pub enum KeyParseError {
    #[error("public key is not valid base64")]
    NotBase64,
    #[error("public key is not 32 bytes")]
    WrongLength,
    #[error("public key bytes are not a valid Ed25519 point")]
    InvalidPoint,
    #[error("public key is a small-order (weak) Ed25519 point and cannot verify any signature")]
    WeakKey,
}

/// Failure verifying a `DevCert` (ES256 / ECDSA P-256) against a
/// candidate exemem root public key. Distinct from [`VerifyError`]
/// because DevCerts are the one non-Ed25519, non-envelope signed object
/// this crate handles (see [`crate::dev_cert`]).
#[derive(Debug, Error, PartialEq, Eq)]
pub enum DevCertVerifyError {
    #[error("unsupported dev cert version: {0}")]
    UnsupportedVersion(u32),
    #[error("dev cert purpose is not \"dev_cert\"")]
    PurposeMismatch,
    #[error("dev cert alg is not ES256")]
    AlgMismatch,
    #[error("dev cert key_id does not match the candidate root key")]
    KeyIdMismatch,
    #[error("dev cert expired")]
    Expired,
    #[error("dev cert is not yet valid (issued_at is in the future)")]
    NotYetValid,
    #[error("dev cert issued_at or expires_at is not a valid RFC 3339 timestamp")]
    BadTimestamp,
    #[error("candidate root key is not a valid P-256 SubjectPublicKeyInfo")]
    MalformedRootKey,
    #[error("dev cert sig is not valid base64 or not a valid DER ECDSA signature")]
    MalformedSig,
    #[error("dev cert signature did not verify against the root key")]
    BadSig,
    #[error("failed to canonicalize the dev cert for verification")]
    Canonicalize,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum VerifyError {
    #[error("unsupported envelope version: {0}")]
    UnsupportedVersion(u32),
    #[error("envelope alg does not match Ed25519")]
    AlgMismatch,
    #[error("envelope key_id does not match the verifying key")]
    KeyIdMismatch,
    #[error("envelope expired")]
    Expired,
    #[error("envelope is not yet valid (issued_at is in the future)")]
    NotYetValid,
    #[error("envelope is missing the sig field")]
    MissingSig,
    #[error("envelope sig is not valid base64 or wrong length")]
    MalformedSig,
    #[error("envelope signature did not verify against the verifying key")]
    BadSig,
    #[error("failed to re-canonicalize the envelope for verification")]
    Canonicalize,
}
