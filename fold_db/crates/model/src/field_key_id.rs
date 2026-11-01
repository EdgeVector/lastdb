//! Deterministic field key UUIDs hash a framed domain, schema and field tuple.
//! The digest uses the current unpadded base64url spelling.
//!
//! Reads, writes, and physical cleanup use the supplied UUID exactly.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use sha2::{Digest, Sha256};

/// SHA-256 of the domain, schema and field, each with its own length prefix.
#[must_use]
pub fn field_key_id_digest(schema_name: &str, field_name: &str) -> [u8; 32] {
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
pub fn encode_field_key_id_bytes(digest: &[u8; 32]) -> String {
    URL_SAFE_NO_PAD.encode(digest)
}

/// The field key id of one schema field. The spelling is the current base64url form.
#[must_use]
pub fn deterministic_field_key_id(schema_name: &str, field_name: &str) -> String {
    encode_field_key_id_bytes(&field_key_id_digest(schema_name, field_name))
}

/// Accept only the current unpadded base64url spelling of a 32-byte digest.
#[must_use]
pub fn is_canonical_field_key_id(field_key_id: &str) -> bool {
    if field_key_id.len() != 43 {
        return false;
    }
    URL_SAFE_NO_PAD
        .decode(field_key_id)
        .is_ok_and(|digest| digest.len() == 32 && URL_SAFE_NO_PAD.encode(digest) == field_key_id)
}

/// The key family of the alternate key entries:
/// `ak:{alternate_field_key_id}:{esc(hash)}\0{range}`. A schema drop releases
/// the entries of each declared alternate by this prefix.
pub const ALTERNATE_ENTRY_PREFIX: &str = "ak:";

/// The stable identity of one named alternate in its exact declaration revision.
#[must_use]
pub fn alternate_field_key_id(schema: &crate::schema::types::Schema, name: &str) -> String {
    let revision = schema.alternate_key_revision();
    let mut hash = Sha256::new();
    hash.update(b"lastdb:alternate-molecule:v1");
    for part in [schema.name.as_str(), revision.as_str(), name] {
        hash.update(
            u64::try_from(part.len())
                .expect("identity length fits u64")
                .to_be_bytes(),
        );
        hash.update(part.as_bytes());
    }
    URL_SAFE_NO_PAD.encode(hash.finalize())
}
