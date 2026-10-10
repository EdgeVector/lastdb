//! Range fill of one hash's ordered molecule view from [`laststore::LastStore::load_hash`].
//!
//! Tests call this path. Product range listing is unchanged. This module does
//! not import `Shard` and does not read `LASTDB_LOGICAL_RESIDENT_SET`.

use super::logical_set::LogicalResidentSet;
use laststore::LoadedTip;

/// Loader that copies one hash prefix and drops each unpinned group.
pub trait HashLoader {
    /// Disk and pin records under `prefix`. Tombstones are not applied.
    fn load_hash(&self, collection: &str, prefix: &[u8]) -> laststore::Result<Vec<LoadedTip>>;
}

#[allow(clippy::use_self)]
impl HashLoader for laststore::LastStore {
    fn load_hash(&self, collection: &str, prefix: &[u8]) -> laststore::Result<Vec<LoadedTip>> {
        laststore::LastStore::load_hash(self, collection, prefix)
    }
}

impl LogicalResidentSet {}
