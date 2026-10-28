//! Decode only the selected representation under the exact molecule identity.

use super::{MoleculeKeyCodec, MoleculeKeyCodecError};

impl MoleculeKeyCodec {
    /// V2 block encryption supplies authentication. A molecule without a
    /// selected profile stores the API range as it is.
    pub fn api_range(&self, stored: &str) -> Result<String, MoleculeKeyCodecError> {
        match self.range_profile {
            Some(profile) => profile
                .decode_value(stored)
                .map_err(MoleculeKeyCodecError::InvalidRange),
            None => Ok(stored.to_owned()),
        }
    }
}
