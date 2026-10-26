//! Explicit typed order composed with the existing molecule range encoding.
//!
//! This codec is deliberately separate from the active node codec. No startup,
//! schema read or writer selects a typed profile in this package. Whole-home
//! activation requires an offline publication/recovery boundary first.

use super::{MoleculeKeyCodec, MoleculeKeyCodecError, RangeKeyEncoding};
pub use schema_types::range_storage_profile::RangeStorageProfile;

/// One range representation, with no second key or copy of the original text.
#[derive(Debug, Clone)]
pub struct ProfiledRangeCodec {
    profile: RangeStorageProfile,
    codec: MoleculeKeyCodec,
}

impl ProfiledRangeCodec {
    /// Retain the same molecule keys and encryption modes. Only order changes.
    #[must_use]
    pub fn new(profile: RangeStorageProfile, existing: &MoleculeKeyCodec) -> Self {
        Self {
            profile,
            codec: existing.clone(),
        }
    }

    #[must_use]
    pub fn profile(&self) -> RangeStorageProfile {
        self.profile
    }

    #[must_use]
    pub fn range_encoding(&self) -> RangeKeyEncoding {
        self.codec.range_encoding()
    }

    /// Encode a real range value. An absent hash-only/Single range slot is not a
    /// value; its caller must handle that slot without this value codec.
    pub fn encode_range(
        &self,
        molecule: &str,
        api: &str,
        max_bytes: usize,
    ) -> Result<String, MoleculeKeyCodecError> {
        if self.codec.range_encoding() == RangeKeyEncoding::OpeV1 && self.codec.ope_key.is_none() {
            return Err(MoleculeKeyCodecError::MissingOpeKey);
        }
        let bytes = self.profile.fixed_payload_bytes().unwrap_or(api.len());
        let expansion = match self.codec.range_encoding() {
            RangeKeyEncoding::Plain => 1,
            RangeKeyEncoding::OpeV1 => 8,
        };
        if bytes
            .checked_mul(expansion)
            .is_none_or(|bytes| bytes > max_bytes)
        {
            return Err(MoleculeKeyCodecError::RangeKeyTooLarge);
        }
        let ordered = self
            .profile
            .encode_value(api)
            .map_err(MoleculeKeyCodecError::InvalidRange)?;
        self.codec.storage_range(molecule, &ordered)
    }

    /// Strict decode: a plain key that resembles hex stays plain. An OPE-v1
    /// token must match the selected molecule/key as well as the selected type.
    /// Re-encoding detects a format/key mismatch; it is not authentication.
    pub fn decode_range(
        &self,
        molecule: &str,
        stored: &str,
    ) -> Result<String, MoleculeKeyCodecError> {
        let ordered = self.codec.api_range(molecule, stored)?;
        self.profile
            .decode_value(&ordered)
            .map_err(MoleculeKeyCodecError::InvalidRange)
    }
}
