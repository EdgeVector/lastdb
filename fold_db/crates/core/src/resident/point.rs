//! Point-get admission from [`laststore::LastStore::load_point`].
//!
//! Tests call this path. Product [`laststore::LastStore::get`] is unchanged.
//! This module does not import `Shard` and does not read
//! `LASTDB_LOGICAL_RESIDENT_SET`.

use super::logical_set::LogicalResidentSet;
use laststore::LoadedPoint;

/// Loader that copies one storage record and drops an unpinned group.
pub trait PointLoader {
    /// One point load. `collection` is `tips` or `atoms`.
    fn load_point(
        &self,
        collection: &str,
        storage_key: &str,
    ) -> laststore::Result<Option<LoadedPoint>>;
}

#[allow(clippy::use_self)]
impl PointLoader for laststore::LastStore {
    fn load_point(
        &self,
        collection: &str,
        storage_key: &str,
    ) -> laststore::Result<Option<LoadedPoint>> {
        // Inherent LastStore::load_point. `Self::load_point` would recurse.
        laststore::LastStore::load_point(self, collection, storage_key)
    }
}

impl LogicalResidentSet {}
