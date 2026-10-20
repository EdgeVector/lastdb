//! The value of a queue entry and of a directory entry.

use super::super::counter::ReleaseRequest;
use crate::schema::SchemaError;
use serde::{Deserialize, Serialize};

pub(super) const ENTRY_VERSION: u8 = 1;

/// Both entries of one record hold the same text, so a release that starts
/// from either one finds every row of the record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RetentionEntry {
    #[serde(rename = "v")]
    pub(crate) version: u8,
    /// The full storage key of the history row. It holds the references.
    pub(crate) history: String,
    /// The full storage key of the conflict row, when the record has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) conflict: Option<String>,
    /// The full storage key of the directory entry.
    pub(crate) dir: String,
    /// The full storage key of the queue entry.
    pub(crate) queue: String,
    /// The full storage key of the `mk:` slot that the record belongs to. A
    /// purge compares it with the slot it removes, so a range that merely
    /// begins with the same bytes cannot release the record of another slot.
    pub(crate) slot: String,
}

impl RetentionEntry {
    /// The storage scope of the record, read from the key of its history row.
    pub(crate) fn scope(&self) -> Result<Option<&str>, SchemaError> {
        crate::storage::current_keys::exact_scope(&self.history).map_err(|error| {
            SchemaError::InvalidData(format!("retention entry {}: {error}", self.history))
        })
    }

    /// The release the planner runs for this record.
    pub(crate) fn request(&self) -> ReleaseRequest {
        let mut deletes = vec![self.dir.as_bytes().to_vec(), self.queue.as_bytes().to_vec()];
        if let Some(conflict) = &self.conflict {
            deletes.push(conflict.as_bytes().to_vec());
        }
        ReleaseRequest {
            history: self.history.clone(),
            deletes,
        }
    }
}
