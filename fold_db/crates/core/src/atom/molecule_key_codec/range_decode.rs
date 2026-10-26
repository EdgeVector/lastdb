//! Decode only the selected representation under the exact molecule identity.

use super::{MoleculeKeyCodec, MoleculeKeyCodecError, RangeKeyEncoding};
use crate::crypto::E2eKeys;

impl MoleculeKeyCodec {
    /// Never infer OPE from a string's alphabet. Re-encoding detects a key or
    /// molecule mismatch; this check does not make OPE authenticated encryption.
    pub fn api_range(&self, molecule: &str, stored: &str) -> Result<String, MoleculeKeyCodecError> {
        match self.range_encoding() {
            RangeKeyEncoding::Plain => Ok(stored.to_owned()),
            RangeKeyEncoding::OpeV1 => {
                if self.ope_key.is_none() {
                    return Err(MoleculeKeyCodecError::MissingOpeKey);
                }
                let decoded = E2eKeys::ope_decode_range_plaintext(stored).ok_or_else(|| {
                    MoleculeKeyCodecError::InvalidRange("malformed OPE-v1 range".into())
                })?;
                if self.storage_range(molecule, &decoded)? != stored {
                    return Err(MoleculeKeyCodecError::InvalidRange(
                        "OPE-v1 molecule or key does not match".into(),
                    ));
                }
                Ok(decoded)
            }
        }
    }
}
