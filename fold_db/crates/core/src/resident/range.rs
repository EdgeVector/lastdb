//! Range fill of one hash's ordered molecule view from [`laststore::LastStore::load_hash`].
//!
//! Tests call this path. Product range listing is unchanged. This module does
//! not import `Shard` and does not read `LASTDB_LOGICAL_RESIDENT_SET`.

use super::logical_set::{LogicalResidentSet, RangeNotResident, Tip};
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

/// Owned page of live `(range, tip)` pairs after merge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangePage {
    /// Ordered live tips under the requested bounds.
    pub entries: Vec<(String, Tip)>,
}

/// Failure from the loader, a gate rejection, or a tip body that is not an
/// atom entry.
#[derive(Debug)]
pub enum RangeAdmitError {
    /// [`HashLoader::load_hash`] failed, including [`laststore::Error::UnanchoredRead`].
    Loader(laststore::Error),
    /// The in-memory view is not complete and this call did not fill.
    NotResident(RangeNotResident),
    /// Tip bytes were not a molecule per-key record or a bare atom entry.
    InvalidTipBody(String),
}

impl From<laststore::Error> for RangeAdmitError {
    fn from(err: laststore::Error) -> Self {
        Self::Loader(err)
    }
}

impl From<RangeNotResident> for RangeAdmitError {
    fn from(err: RangeNotResident) -> Self {
        Self::NotResident(err)
    }
}

impl std::fmt::Display for RangeAdmitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Loader(err) => write!(f, "hash loader: {err}"),
            Self::NotResident(err) => write!(f, "{err}"),
            Self::InvalidTipBody(msg) => write!(f, "invalid tip body: {msg}"),
        }
    }
}

impl std::error::Error for RangeAdmitError {}

impl LogicalResidentSet {}
