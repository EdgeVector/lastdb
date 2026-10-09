//! Lowercase hex encoding and SHA-256 hex digests shared by the schema service
//! crates, so none of them keeps its own copy.

use sha2::{Digest, Sha256};

pub use app_identity_crypto::hex_lower;

/// Lowercase hex of the SHA-256 digest of `bytes` (64 characters).
pub fn sha256_hex(bytes: impl AsRef<[u8]>) -> String {
    hex_lower(&Sha256::digest(bytes.as_ref()))
}
