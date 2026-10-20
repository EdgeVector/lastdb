//! Deterministic molecule UUIDs hash a framed domain, schema and field tuple.
//! The digest uses the current unpadded base64url spelling.
//!
//! Reads, writes, and physical cleanup use the supplied UUID exactly.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use sha2::{Digest, Sha256};

/// SHA-256 of the domain, schema and field, each with its own length prefix.
#[must_use]
pub fn molecule_uuid_digest(schema_name: &str, field_name: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for part in [
        b"lastdb:molecule-uuid:v1".as_slice(),
        schema_name.as_bytes(),
        field_name.as_bytes(),
    ] {
        crate::canonical::push_field(&mut hasher, part);
    }
    hasher.finalize().into()
}

/// Current write form: unpadded base64url of the 32-byte digest (43 chars).
#[must_use]
pub fn encode_molecule_uuid_bytes(digest: &[u8; 32]) -> String {
    URL_SAFE_NO_PAD.encode(digest)
}

/// Accept only the current unpadded base64url spelling of a 32-byte digest.
#[must_use]
pub fn is_canonical_molecule_uuid(molecule_uuid: &str) -> bool {
    if molecule_uuid.len() != 43 {
        return false;
    }
    URL_SAFE_NO_PAD
        .decode(molecule_uuid)
        .is_ok_and(|digest| digest.len() == 32 && URL_SAFE_NO_PAD.encode(digest) == molecule_uuid)
}
