//! Decode only the selected representation under the exact molecule identity.

use super::{MoleculeKeyCodec, MoleculeKeyCodecError};

impl MoleculeKeyCodec {
    /// V2 block encryption supplies authentication. The molecule's typed
    /// range profile decodes the stored payload.
    pub fn api_range(&self, stored: &str) -> Result<String, MoleculeKeyCodecError> {
        self.range_profile
            .decode_value(stored)
            .map_err(MoleculeKeyCodecError::InvalidRange)
    }
}
