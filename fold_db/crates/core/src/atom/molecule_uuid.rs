//! Deterministic molecule UUIDs use the base64url form of SHA-256(schema:field).
//!
//! Logical reads and writes use the supplied UUID exactly. A hex string is
//! not an alias for its base64url digest.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use sha2::{Digest, Sha256};

/// SHA-256(`{schema}:{field}`) as 32 raw bytes.
#[must_use]
pub fn molecule_uuid_digest(schema_name: &str, field_name: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(format!("{schema_name}:{field_name}").as_bytes());
    hasher.finalize().into()
}

/// Current write form: unpadded base64url of the 32-byte digest (43 chars).
#[must_use]
pub fn encode_molecule_uuid_bytes(digest: &[u8; 32]) -> String {
    URL_SAFE_NO_PAD.encode(digest)
}
