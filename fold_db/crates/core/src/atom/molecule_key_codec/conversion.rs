//! Bounded, all-or-nothing conversion of a supplied molecule range-key set.
//!
//! This is a key conversion plan, not a home converter. The caller must supply
//! the complete logical range-key set from one fixed snapshot. Publication must
//! also preserve tips, delete barriers, counters, conflicts and file references.
//! No callback can publish a partial set before validation finishes here.

use super::storage_profile::{ProfiledRangeCodec, RangeStorageProfile};
use super::{
    decode_hash_range, hash_range_record_key, MoleculeKeyCodec, MoleculeKeyCodecError,
    RangeKeyEncoding,
};

/// Hard bounds on transient conversion metadata, independent of atom size.
#[derive(Debug, Clone, Copy)]
pub struct KeyConversionLimits {
    pub max_keys: usize,
    pub max_key_bytes: usize,
    /// Sum of source and target key bytes retained by the plan.
    pub max_total_key_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyConversionError {
    InvalidLimits,
    InvalidMolecule,
    InvalidSourceKey,
    LimitExceeded,
    /// Also refuses duplicate input keys. No value is selected or overwritten.
    Collision,
    Codec(MoleculeKeyCodecError),
}

impl std::fmt::Display for KeyConversionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidLimits => f.write_str("key conversion requires nonzero limits"),
            Self::InvalidMolecule => f.write_str("key conversion requires one molecule identity"),
            Self::InvalidSourceKey => {
                f.write_str("source key is outside the selected molecule format")
            }
            Self::LimitExceeded => f.write_str("key conversion exceeds its metadata limit"),
            Self::Collision => {
                f.write_str("key conversion maps multiple source keys to one target")
            }
            Self::Codec(error) => std::fmt::Display::fmt(error, f),
        }
    }
}

impl std::error::Error for KeyConversionError {}

impl From<MoleculeKeyCodecError> for KeyConversionError {
    fn from(error: MoleculeKeyCodecError) -> Self {
        Self::Codec(error)
    }
}

/// An exact framed key mapping; atom bytes never enter this object.
#[derive(Debug)]
pub struct ConvertedRangeKey {
    pub source_key: String,
    pub target_key: String,
}

/// Validated supplied keys in target byte order, not the old source order.
/// This does not certify snapshot completeness or authorize home publication.
#[derive(Debug)]
pub struct RangeKeyConversionPlan {
    molecule: String,
    source_profile: RangeStorageProfile,
    target_profile: RangeStorageProfile,
    source_range_encoding: RangeKeyEncoding,
    target_range_encoding: RangeKeyEncoding,
    key_bytes: usize,
    keys: Vec<ConvertedRangeKey>,
}

impl RangeKeyConversionPlan {
    #[must_use]
    pub fn molecule(&self) -> &str {
        &self.molecule
    }

    #[must_use]
    pub fn profiles(&self) -> (RangeStorageProfile, RangeStorageProfile) {
        (self.source_profile, self.target_profile)
    }

    #[must_use]
    pub fn range_encodings(&self) -> (RangeKeyEncoding, RangeKeyEncoding) {
        (self.source_range_encoding, self.target_range_encoding)
    }

    #[must_use]
    pub fn key_bytes(&self) -> usize {
        self.key_bytes
    }

    #[must_use]
    pub fn keys(&self) -> &[ConvertedRangeKey] {
        &self.keys
    }
}

/// Source and target use explicit range representations with the same keys.
/// Hash bytes stay unchanged. No profile or encoding is inferred from stored bytes.
pub struct RangeKeyConversion {
    molecule: String,
    source: ProfiledRangeCodec,
    target: ProfiledRangeCodec,
    limits: KeyConversionLimits,
}

impl RangeKeyConversion {
    pub fn new(
        molecule: &str,
        existing: &MoleculeKeyCodec,
        target_range_encoding: RangeKeyEncoding,
        source_profile: RangeStorageProfile,
        target_profile: RangeStorageProfile,
        limits: KeyConversionLimits,
    ) -> Result<Self, KeyConversionError> {
        if molecule.is_empty() || molecule.contains([':', '\0']) {
            return Err(KeyConversionError::InvalidMolecule);
        }
        if limits.max_keys == 0 || limits.max_key_bytes == 0 || limits.max_total_key_bytes == 0 {
            return Err(KeyConversionError::InvalidLimits);
        }
        Ok(Self {
            molecule: molecule.to_owned(),
            source: ProfiledRangeCodec::new(source_profile, existing),
            target: ProfiledRangeCodec::new(
                target_profile,
                &existing.with_range_encoding(target_range_encoding),
            ),
            limits,
        })
    }

    /// Convert unscoped `mk:` keys for real range values, not absent range slots.
    /// The output appears only after every input passes validation and bounds.
    /// Distinct spellings such as `02` and `2` must not silently coalesce.
    pub fn plan_keys(
        &self,
        source_keys: impl IntoIterator<Item = String>,
    ) -> Result<RangeKeyConversionPlan, KeyConversionError> {
        let mut keys = Vec::new();
        let mut key_bytes = 0_usize;
        for source_key in source_keys {
            if keys.len() >= self.limits.max_keys || source_key.len() > self.limits.max_key_bytes {
                return Err(KeyConversionError::LimitExceeded);
            }
            let remaining = self
                .limits
                .max_total_key_bytes
                .checked_sub(key_bytes)
                .and_then(|bytes| bytes.checked_sub(source_key.len()))
                .ok_or(KeyConversionError::LimitExceeded)?;
            let target_key =
                self.convert_key(&source_key, remaining.min(self.limits.max_key_bytes))?;
            key_bytes = key_bytes
                .checked_add(source_key.len())
                .and_then(|bytes| bytes.checked_add(target_key.len()))
                .filter(|bytes| *bytes <= self.limits.max_total_key_bytes)
                .ok_or(KeyConversionError::LimitExceeded)?;
            keys.push(ConvertedRangeKey {
                source_key,
                target_key,
            });
        }
        keys.sort_unstable_by(|left, right| left.target_key.cmp(&right.target_key));
        if keys
            .windows(2)
            .any(|pair| pair[0].target_key == pair[1].target_key)
        {
            return Err(KeyConversionError::Collision);
        }
        Ok(RangeKeyConversionPlan {
            molecule: self.molecule.clone(),
            source_profile: self.source.profile(),
            target_profile: self.target.profile(),
            source_range_encoding: self.source.range_encoding(),
            target_range_encoding: self.target.range_encoding(),
            key_bytes,
            keys,
        })
    }

    fn convert_key(
        &self,
        source_key: &str,
        max_target_bytes: usize,
    ) -> Result<String, KeyConversionError> {
        let (hash, range) = decode_hash_range(source_key, &self.molecule)
            .ok_or(KeyConversionError::InvalidSourceKey)?;
        // Preserve the exact framed hash, including an existing blinded hash.
        if hash_range_record_key(&self.molecule, &hash, &range) != source_key {
            return Err(KeyConversionError::InvalidSourceKey);
        }
        let api = self.source.decode_range(&self.molecule, &range)?;
        let prefix_bytes = source_key.len() - range.len();
        let range_limit = max_target_bytes
            .checked_sub(prefix_bytes)
            .ok_or(KeyConversionError::LimitExceeded)?;
        let range = self
            .target
            .encode_range(&self.molecule, &api, range_limit)?;
        Ok(hash_range_record_key(&self.molecule, &hash, &range))
    }
}
