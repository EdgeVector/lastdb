//! One blind key per record-space or alternate bundle.
//!
//! Older sealed bundles also carry `content_dek` and `ope_key`. Decode ignores
//! those fields. A stored record is plain JSON, and no range is order-preserving
//! encoded, so a newly minted bundle writes only `blind_key`.

use lastdb_model::field_key_codec::FieldKeyCodec;
use rand::{rngs::OsRng, RngCore};
use schema_types::field_range_profile::FieldRangeProfile;
use serde::{Deserialize, Serialize};
use zeroize::{Zeroize, ZeroizeOnDrop};

#[derive(Clone, Deserialize, Serialize, Zeroize, ZeroizeOnDrop)]
pub struct FieldKeyBundle {
    blind_key: [u8; 32],
}

impl std::fmt::Debug for FieldKeyBundle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FieldKeyBundle(<redacted>)")
    }
}

impl FieldKeyBundle {
    pub(super) fn generate() -> Self {
        let mut bundle = Self { blind_key: [0; 32] };
        OsRng.fill_bytes(&mut bundle.blind_key);
        bundle
    }

    pub(crate) fn key_codec(&self, profile: FieldRangeProfile) -> FieldKeyCodec {
        FieldKeyCodec::new(self.blind_key, profile)
    }
}
