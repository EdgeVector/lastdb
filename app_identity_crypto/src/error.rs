use thiserror::Error;

#[derive(Debug, Error)]
/// JCS canonicalization failed.
pub enum CanonicalizeError {
    #[error("JCS canonicalization failed: {0}")]
    /// The JSON value could not be canonicalized.
    Serde(#[from] serde_json::Error),
}

#[derive(Debug, Error)]
/// Signing an envelope failed before a signature was produced.
pub enum SignError {
    #[error("envelope to sign must have sig = None")]
    /// The envelope already had `sig` set. Signing requires `sig = None`.
    SigAlreadySet,
    #[error(transparent)]
    /// The unsigned envelope could not be canonicalized.
    Canonicalize(#[from] CanonicalizeError),
}

/// Failure parsing a base64-encoded Ed25519 public key into a
/// [`VerifyingKey`](crate::VerifyingKey).
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

/// Failure verifying a `DevCert` (ES256 / ECDSA P-256) against a
/// candidate exemem root public key. Distinct from [`VerifyError`]
/// because DevCerts are the one non-Ed25519, non-envelope signed object
/// this crate handles (see [`crate::dev_cert`]).
#[derive(Debug, Error, PartialEq, Eq)]
pub enum DevCertVerifyError {
    #[error("unsupported dev cert version: {0}")]
    /// The cert `version` is not `DEV_CERT_VERSION`.
    UnsupportedVersion(u32),
    #[error("dev cert purpose is not \"dev_cert\"")]
    /// The cert `purpose` is not `"dev_cert"`.
    PurposeMismatch,
    #[error("dev cert alg is not ES256")]
    /// The cert `alg` is not `"ES256"`.
    AlgMismatch,
    #[error("dev cert key_id does not match the candidate root key")]
    /// The cert `key_id` is not the SHA-256 of the candidate root key.
    KeyIdMismatch,
    #[error("dev cert expired")]
    /// The cert's expiry is in the past or equal to now.
    Expired,
    #[error("dev cert is not yet valid (issued_at is in the future)")]
    /// `issued_at` is after now.
    NotYetValid,
    #[error("dev cert issued_at or expires_at is not a valid RFC 3339 timestamp")]
    /// `issued_at` or `expires_at` is not an RFC 3339 timestamp.
    BadTimestamp,
    #[error("candidate root key is not a valid P-256 SubjectPublicKeyInfo")]
    /// The candidate root key is not a P-256 SubjectPublicKeyInfo.
    MalformedRootKey,
    #[error("dev cert sig is not valid base64 or not a valid DER ECDSA signature")]
    /// The cert signature is not base64 or not a DER ECDSA signature.
    MalformedSig,
    #[error("dev cert signature did not verify against the root key")]
    /// The ECDSA signature did not verify against the root key.
    BadSig,
    #[error("failed to canonicalize the dev cert for verification")]
    /// The cert could not be canonicalized with `sig` removed.
    Canonicalize,
}

#[derive(Debug, Error, PartialEq, Eq)]
/// An envelope failed an intrinsic check.
pub enum VerifyError {
    #[error("unsupported envelope version: {0}")]
    /// The envelope `version` is not `ENVELOPE_VERSION`.
    UnsupportedVersion(u32),
    #[error("envelope alg does not match Ed25519")]
    /// The envelope `alg` is not `"Ed25519"`.
    AlgMismatch,
    #[error("envelope key_id does not match the verifying key")]
    /// The envelope `key_id` is not the SHA-256 of the verifying key.
    KeyIdMismatch,
    #[error("envelope expired")]
    /// The envelope's expiry is in the past or equal to now.
    Expired,
    #[error("envelope is not yet valid (issued_at is in the future)")]
    /// `issued_at` is after now.
    NotYetValid,
    #[error("envelope is missing the sig field")]
    /// The envelope has no `sig`.
    MissingSig,
    #[error("envelope sig is not valid base64 or wrong length")]
    /// The signature is not base64 or is not 64 bytes.
    MalformedSig,
    #[error("envelope signature did not verify against the verifying key")]
    /// The Ed25519 signature did not verify over the envelope without `sig`.
    BadSig,
    #[error("failed to re-canonicalize the envelope for verification")]
    /// The envelope could not be canonicalized with `sig` removed.
    Canonicalize,
}
