//! Identities for record-space and alternate-key bundles.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use sha2::{Digest, Sha256};

#[must_use]
pub fn is_canonical_field_key_id(field_key_id: &str) -> bool {
    field_key_id.len() == 43
        && URL_SAFE_NO_PAD
            .decode(field_key_id)
            .is_ok_and(|d| d.len() == 32 && URL_SAFE_NO_PAD.encode(d) == field_key_id)
}

pub const ALTERNATE_ENTRY_PREFIX: &str = "ak:";

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
