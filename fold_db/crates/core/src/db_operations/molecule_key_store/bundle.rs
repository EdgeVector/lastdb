//! Molecule key material keeps the existing three-key wire form.

use crate::atom::MoleculeKeyCodec;
use rand::{rngs::OsRng, RngCore};
use serde::{Deserialize, Serialize};
use zeroize::{Zeroize, ZeroizeOnDrop};

#[derive(Clone, Deserialize, Serialize, Zeroize, ZeroizeOnDrop)]
pub struct MoleculeKeyBundle {
    content_dek: [u8; 32],
    blind_key: [u8; 32],
    ope_key: [u8; 32],
}

impl std::fmt::Debug for MoleculeKeyBundle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MoleculeKeyBundle(<redacted>)")
    }
}

impl MoleculeKeyBundle {
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

    pub(crate) fn content_dek(&self) -> [u8; 32] {
        self.content_dek
    }

    pub(crate) fn key_codec(&self, configured: &MoleculeKeyCodec) -> MoleculeKeyCodec {
        MoleculeKeyCodec::with_encodings(
            configured.encoding(),
            configured.range_encoding(),
            Some(self.blind_key),
            Some(self.ope_key),
        )
    }
}
