//! Explicit physical capacity selection for fresh conversion targets.

use super::{HomeBuilder, HomeDescriptor};
use crate::disk_pack::Current;
use crate::Result;
use std::path::Path;

impl HomeBuilder {
    /// Explicit opt-in for a new empty native home; existing homes never upgrade.
    pub fn create_empty_with_extended_keys(
        root: &Path,
        descriptor: HomeDescriptor,
    ) -> Result<Current> {
        let mut builder = Self::create_fenced(root, descriptor, 0)?;
        builder.inner.required_features |= crate::disk_pack::FEATURE_SCHEMA_RECORD_REFS
            | crate::disk_pack::FEATURE_ATOM_RECORDS
            | crate::disk_pack::FEATURE_EXTENDED_KEYS;
        builder.finish()
    }

    /// A separate logical converter owns complete source validation and ordering.
    /// It preserves native authority and enables extended keys before publication.
    pub fn create_extended_conversion(
        root: &Path,
        descriptor: HomeDescriptor,
        frontier: u64,
        source_features: u64,
    ) -> Result<Self> {
        Self::create_native_transfer(
            root,
            descriptor,
            frontier,
            source_features | crate::disk_pack::FEATURE_EXTENDED_KEYS,
        )
    }
}
