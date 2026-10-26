//! Explicit stopped-home inventory before a typed range conversion.
//!
//! The inventory captures the authoritative metadata from one held publication,
//! resolves current generation winners, and checks proposed key mappings. It
//! never creates a writer, changes a profile, or claims a home is converted.

use crate::atom::{MoleculeKeyCodec, RangeKeyEncoding};
use crate::crypto::E2eKeys;
use crate::storage::{error::StorageResult, StorageError};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::Path;

mod binding;
mod capture;
mod catalog;
mod conversion_copy;
mod conversion_plan;
mod conversion_references;
mod conversion_target;
mod conversion_validate;
mod convert;
mod keys;
mod logical;
mod retention;
mod run_pages;
mod source;

pub use catalog::MoleculeInventory;
pub use convert::{convert_typed_ranges, RangeConversionOptions, RangeConversionReport};
pub use source::{SourceFacts, SourceRoot};

#[derive(Debug, Clone, Copy, Serialize)]
pub struct InventoryLimits {
    pub max_records: usize,
    pub max_metadata_bytes: usize,
    pub max_key_bytes: usize,
}

impl Default for InventoryLimits {
    fn default() -> Self {
        Self {
            max_records: 1_000_000,
            max_metadata_bytes: 128 * 1024 * 1024,
            max_key_bytes: 64 * 1024,
        }
    }
}

pub(super) struct MetadataRecord {
    collection: String,
    key: String,
    value: Vec<u8>,
}

#[derive(Debug, Serialize)]
pub struct RangeConversionInventory {
    pub version: u8,
    pub source_format: String,
    pub source_facts: SourceFacts,
    pub source_publications_sha256: String,
    pub metadata_sha256: String,
    pub source_range_encoding: String,
    pub target_range_encoding: String,
    pub limits: InventoryLimits,
    pub metadata_records: usize,
    pub metadata_bytes: usize,
    pub record_families: BTreeMap<String, u64>,
    pub schemas: usize,
    pub molecules: Vec<MoleculeInventory>,
    /// These gates remain false until the separate publication/recovery stage.
    pub database_converted: bool,
    pub activation_ready: bool,
}

/// Caller owns stopped-home/primary guards. The storage owner lock is acquired
/// here before the snapshot. The ordinary node never calls this function.
pub fn inspect_range_conversion(
    root: &Path,
    keys: &E2eKeys,
    source_range_encoding: RangeKeyEncoding,
    target_range_encoding: RangeKeyEncoding,
    limits: InventoryLimits,
) -> StorageResult<RangeConversionInventory> {
    if limits.max_records == 0 || limits.max_metadata_bytes == 0 || limits.max_key_bytes == 0 {
        return Err(invalid("inventory requires positive limits"));
    }
    let mut source = source::FrozenSource::open(root, &keys.encryption_key())?;
    let records = source.collect(limits)?;
    let codec = MoleculeKeyCodec::with_encodings(
        crate::atom::HashKeyEncoding::Plain,
        source_range_encoding,
        Some(keys.index_key()),
        Some(keys.ope_key()),
    );
    let mut inventory = catalog::Catalog::load(&records, &keys.encryption_key(), &codec, limits)?;
    let mut families = BTreeMap::new();
    let mut digest = Sha256::new();
    let mut bytes = 0_usize;
    for record in &records {
        for part in [
            record.collection.as_bytes(),
            record.key.as_bytes(),
            record.value.as_slice(),
        ] {
            digest.update((part.len() as u64).to_be_bytes());
            digest.update(part);
            bytes += part.len();
        }
        keys::observe(&mut inventory, record, &mut families, limits)?;
    }
    let molecules = inventory.finish(limits, target_range_encoding)?;
    source.check_binding()?;
    let mut publications = Sha256::new();
    for binding in &source.bindings {
        publications.update(binding.digest.as_bytes());
    }
    Ok(RangeConversionInventory {
        version: 2,
        source_format: source.format().into(),
        source_facts: source.facts(),
        source_publications_sha256: format!("{:x}", publications.finalize()),
        metadata_sha256: format!("{:x}", digest.finalize()),
        source_range_encoding: source_range_encoding.as_str().into(),
        target_range_encoding: target_range_encoding.as_str().into(),
        limits,
        metadata_records: records.len(),
        metadata_bytes: bytes,
        record_families: families,
        schemas: inventory.schemas,
        molecules,
        database_converted: false,
        activation_ready: false,
    })
}

fn invalid(message: &str) -> StorageError {
    StorageError::InvalidOperation(format!("range conversion inventory: {message}"))
}
