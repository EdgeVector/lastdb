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
}
