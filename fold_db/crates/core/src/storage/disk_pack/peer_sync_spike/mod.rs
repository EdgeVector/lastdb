//! Experimental logical peer protocol over one isolated native namespace.
//!
//! This feature does not serve LastDB molecules, atoms, schemas, or the node API.
//! It accepts signed, unconditional Put/Delete outcomes only. A retained Delete
//! participates in the same deterministic winner rule as a Put. There is no
//! Purge, TTL, blob, aggregate, protein, share, or production cloud integration.

mod adoption;
mod control;
mod generation;
mod keys;
mod model;
mod replica;
mod snapshot;
mod stream;

pub use adoption::AdoptionReport;
pub use model::{Change, Entry, ImportReport, Position, RangePage, Writer};
pub use replica::Replica;
pub use snapshot::SnapshotInfo;

use crate::storage::error::{StorageError, StorageResult};
use serde::{de::DeserializeOwned, Serialize};

pub const MAX_CHANGES: usize = 128;
pub const MAX_ENTRY_BYTES: usize = 256 * 1024;
pub const MAX_PAGE: usize = 128;
pub const MAX_EXPORT_BATCH: usize = 4;
pub const MAX_ORIGINS: usize = 128;
pub const MAX_IMPORT_BYTES: usize = 1024 * 1024;
pub const MAX_IMPORT_CHANGES: usize = 1024;
pub(super) const NAMESPACE: &str = "__peer_sync_spike_v1";

pub(super) fn invalid(message: impl Into<String>) -> StorageError {
    StorageError::InvalidOperation(message.into())
}

pub(super) fn encode<T: Serialize>(value: &T) -> StorageResult<Vec<u8>> {
    serde_json::to_vec(value).map_err(|error| StorageError::SerializationError(error.to_string()))
}

pub(super) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> StorageResult<T> {
    serde_json::from_slice(bytes)
        .map_err(|error| StorageError::SerializationError(error.to_string()))
}
