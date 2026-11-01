//! The value of a queue entry.

use super::super::counter::ReleaseRequest;
use crate::schema::SchemaError;
use serde::{Deserialize, Serialize};

pub(super) const ENTRY_VERSION: u8 = 1;

/// The entry of one kept conflict row. A format 7 row writes it with no
/// conflict key and no directory entry (`rows.rs`). The fields keep the shape
/// that the release and older decoders read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RetentionEntry {
    #[serde(rename = "v")]
    pub(crate) version: u8,
    /// The full storage key of the conflict row.
    pub(crate) history: String,
    /// Another row that the release deletes. A format 7 entry has none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) conflict: Option<String>,
    /// The full storage key of a slot directory entry. Empty: no format 7 path
    /// writes one.
    pub(crate) dir: String,
    /// The full storage key of the queue entry.
    pub(crate) queue: String,
    /// The full storage key of the record that the conflict row belongs to.
    pub(crate) slot: String,
    /// When the row was written, in nanoseconds of the retention clock.
    #[serde(default)]
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
        // A format 7 entry has no slot directory entry (`dir` is empty): there
        // is no key to delete.
        let mut deletes = Vec::new();
        if !self.queue.is_empty() {
            deletes.push(self.queue.as_bytes().to_vec());
        }
        if !self.dir.is_empty() {
            deletes.push(self.dir.as_bytes().to_vec());
        }
        if let Some(conflict) = &self.conflict {
            deletes.push(conflict.as_bytes().to_vec());
        }
        ReleaseRequest {
            history: self.history.clone(),
            deletes,
        }
    }
}
