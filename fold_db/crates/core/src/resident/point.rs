//! Point-get admission from [`laststore::LastStore::load_point`].
//!
//! Tests call this path. Product [`laststore::LastStore::get`] is unchanged.
//! This module does not import `Shard` and does not read
//! `LASTDB_LOGICAL_RESIDENT_SET`.

use super::logical_set::{LogicalResidentSet, Tip};
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

/// Result of one point admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PointGetOutcome {
    /// No live tip. A dirty tombstone also yields this.
    Absent,
    /// One owned tip. `value` is the atom body when the caller asked for it
    /// and the body was present or loaded.
    Present { tip: Tip, value: Option<Vec<u8>> },
}

/// Failure from the loader or from a tip body that is not an atom entry.
#[derive(Debug)]
pub enum PointAdmitError {
    /// [`PointLoader::load_point`] failed.
    Loader(laststore::Error),
    /// Tip bytes were not a molecule per-key record or a bare atom entry.
    InvalidTipBody(String),
}

impl From<laststore::Error> for PointAdmitError {
    fn from(err: laststore::Error) -> Self {
        Self::Loader(err)
    }
}

impl std::fmt::Display for PointAdmitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Loader(err) => write!(f, "point loader: {err}"),
            Self::InvalidTipBody(msg) => write!(f, "invalid tip body: {msg}"),
        }
    }
}

impl std::error::Error for PointAdmitError {}

impl LogicalResidentSet {}
