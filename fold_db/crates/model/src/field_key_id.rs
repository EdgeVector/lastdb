//! Identities for record-space and alternate-key bundles.
//!
//! Both use the current unpadded base64url spelling of a 32-byte digest.
//! A payload field has no field key. Reads, writes, and physical cleanup use
//! the supplied id exactly.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use sha2::{Digest, Sha256};

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
/// `ak:{alternate_field_key_id}:{esc(hash)}\0{range}\0{primary_digest}`. A schema drop releases
/// the entries of each declared alternate by this prefix.
pub const ALTERNATE_ENTRY_PREFIX: &str = "ak:";

/// The stable identity of one named alternate in its exact declaration revision.
#[must_use]
pub fn alternate_field_key_id(schema: &crate::schema::types::Schema, name: &str) -> String {
    let revision = schema.alternate_key_revision();
    let mut hash = Sha256::new();
    hash.update(b"lastdb:alternate-key:v1");
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
