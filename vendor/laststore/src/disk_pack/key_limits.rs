//! Feature-bound storage-key limits. Collection framing remains u16-sized.

/// Accepted maximum before the explicit extended-key feature.
pub const LEGACY_STORAGE_KEY_BYTES: usize = 65_535;
/// Covers full old logical ranges and their hex-expanded delete identities.
pub const EXTENDED_STORAGE_KEY_BYTES: usize = 32 * 1024 * 1024;
pub(super) const MAX_OVERFLOW_PAGES: u64 =
    (EXTENDED_STORAGE_KEY_BYTES - 128).div_ceil(super::PAGE_PAYLOAD) as u64;
pub(super) const COLLECTION_BYTES: usize = LEGACY_STORAGE_KEY_BYTES - 4;

pub(super) fn key_byte_limit(features: u64) -> usize {
    if features & super::FEATURE_EXTENDED_KEYS != 0 {
        EXTENDED_STORAGE_KEY_BYTES
    } else {
        LEGACY_STORAGE_KEY_BYTES
    }
}

impl super::Current {
    /// The exact key limit selected by this complete durable generation.
    pub fn storage_key_byte_limit(&self) -> usize {
        key_byte_limit(self.required_features)
    }
}
