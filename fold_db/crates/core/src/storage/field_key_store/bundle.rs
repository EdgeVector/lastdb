//! Field key material keeps the existing three-key wire form.

use lastdb_model::field_key_codec::FieldKeyCodec;
use rand::{rngs::OsRng, RngCore};
use schema_types::field_range_profile::FieldRangeProfile;
use serde::{Deserialize, Serialize};
use zeroize::{Zeroize, ZeroizeOnDrop};

#[derive(Clone, Deserialize, Serialize, Zeroize, ZeroizeOnDrop)]
pub struct FieldKeyBundle {
    /// Kept only for the three-key wire form. No record row is sealed with it:
    /// a disk format v2 record row is plain JSON.
    content_dek: [u8; 32],
    blind_key: [u8; 32],
    /// Kept only for the three-key wire form. No range is OPE encoded.
    ope_key: [u8; 32],
}

impl std::fmt::Debug for FieldKeyBundle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FieldKeyBundle(<redacted>)")
    }
}

impl FieldKeyBundle {
    pub(super) fn generate() -> Self {
        let mut bundle = Self {
            content_dek: [0; 32],
            blind_key: [0; 32],
            ope_key: [0; 32],
        };
        OsRng.fill_bytes(&mut bundle.content_dek);
        OsRng.fill_bytes(&mut bundle.blind_key);
        OsRng.fill_bytes(&mut bundle.ope_key);
        bundle
    }

    pub(crate) fn key_codec(&self, profile: FieldRangeProfile) -> FieldKeyCodec {
        FieldKeyCodec::new(self.blind_key, profile)
    }
}
