//! An explicit molecule contract selects typed payloads without OPE.

use super::{HashKeyEncoding, MoleculeKeyCodec, MoleculeKeyCodecError, RangeKeyEncoding};
use schema_types::molecule_range_profile::MoleculeRangeProfile;

impl MoleculeKeyCodec {
    pub fn with_range_profile(
        &self,
        profile: MoleculeRangeProfile,
    ) -> Result<Self, MoleculeKeyCodecError> {
        profile
            .validate()
            .map_err(MoleculeKeyCodecError::InvalidRange)?;
        if self.range_encoding != RangeKeyEncoding::Plain {
            return Err(MoleculeKeyCodecError::InvalidRange(
                "a typed molecule range requires the plain payload representation".into(),
            ));
        }
        Ok(Self {
            range_profile: Some(profile),
            ..self.clone()
        })
    }

    #[must_use]
    pub fn range_profile(&self) -> Option<MoleculeRangeProfile> {
        self.range_profile
    }

    pub fn require_text_range_prefix(&self) -> Result<(), MoleculeKeyCodecError> {
        self.range_profile.map_or(Ok(()), |profile| {
            profile
                .require_text_prefix()
                .map_err(MoleculeKeyCodecError::InvalidRange)
        })
    }

    pub fn canonical_api_range(&self, api: &str) -> Result<String, MoleculeKeyCodecError> {
        self.range_profile.map_or_else(
            || Ok(api.to_owned()),
            |profile| {
                profile
                    .encode_value(api)
                    .and_then(|stored| profile.decode_value(&stored))
                    .map_err(MoleculeKeyCodecError::InvalidRange)
            },
        )
    }

    /// Explicit incompatible environment settings are refused by node admission.
    pub(crate) fn for_typed_home(&self) -> Self {
        Self {
            hash_encoding: HashKeyEncoding::BlindV1,
            range_encoding: RangeKeyEncoding::Plain,
            range_profile: None,
            ..self.clone()
        }
    }

    /// Supplied-key conversion selects its scalar profile separately.
    pub(super) fn without_range_profile(&self) -> Self {
        Self {
            range_profile: None,
            ..self.clone()
        }
    }
}
