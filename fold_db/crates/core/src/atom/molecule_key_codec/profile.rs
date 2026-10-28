//! The molecule contract selects its typed range payloads.

use super::{MoleculeKeyCodec, MoleculeKeyCodecError};
use schema_types::molecule_range_profile::MoleculeRangeProfile;

impl MoleculeKeyCodec {
    #[must_use]
    pub fn range_profile(&self) -> MoleculeRangeProfile {
        self.range_profile
    }

    pub fn require_text_range_prefix(&self) -> Result<(), MoleculeKeyCodecError> {
        self.range_profile
            .require_text_prefix()
            .map_err(MoleculeKeyCodecError::InvalidRange)
    }

    pub fn canonical_api_range(&self, api: &str) -> Result<String, MoleculeKeyCodecError> {
        let profile = self.range_profile;
        profile
            .encode_value(api)
            .and_then(|stored| profile.decode_value(&stored))
            .map_err(MoleculeKeyCodecError::InvalidRange)
    }
}
