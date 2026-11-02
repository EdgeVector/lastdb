//! The value of a queue entry.

use super::super::counter::ReleaseRequest;
use crate::schema::SchemaError;
use serde::{Deserialize, Serialize};

/// The entry of one kept history row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RetentionEntry {
    /// The full storage key of the history row.
    pub(crate) history: String,
    /// The full storage key of the queue entry.
    pub(crate) queue: String,
    /// The full storage key of the record that the conflict row belongs to.
    pub(crate) slot: String,
    /// When the row was written, in nanoseconds of the retention clock.
    pub(crate) created: u64,
}

impl RetentionEntry {
    /// The storage scope of the record, read from the key of its history row.
    pub(crate) fn scope(&self) -> Result<Option<&str>, SchemaError> {
        crate::storage::current_keys::exact_scope(&self.history).map_err(|error| {
            SchemaError::InvalidData(format!("retention entry {}: {error}", self.history))
        })
    }

    /// The release that a row commit runs for this record.
    pub(crate) fn request(&self) -> ReleaseRequest {
        let mut deletes = Vec::new();
        if !self.queue.is_empty() {
            deletes.push(self.queue.as_bytes().to_vec());
        }
        ReleaseRequest {
            history: self.history.clone(),
            deletes,
        }
    }
}
