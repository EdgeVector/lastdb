use thiserror::Error;

#[derive(Debug, Error)]
/// JCS canonicalization failed.
pub enum CanonicalizeError {
    #[error("JCS canonicalization failed: {0}")]
    /// The JSON value could not be canonicalized.
    Serde(#[from] serde_json::Error),
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
