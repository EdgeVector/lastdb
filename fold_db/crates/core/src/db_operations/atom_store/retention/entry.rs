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
    /// When the record was written, in nanoseconds of the retention clock. A
    /// drain of an erased slot releases only the records written before the
    /// erase. Zero (a row without the field) counts as old.
    #[serde(default)]
    pub(crate) created: u64,
}

/// A queue entry that makes the janitor release every record of one erased
/// slot. An erase releases a bounded number of records in its own batch. For
/// each slot that still holds records it writes one marker, due at once, in
/// that batch. The marker costs one operation for each slot, however many
/// records the slot holds. The janitor reads the directory of the slot in
/// bounded pages and releases the records until none is left.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DrainMarker {
    #[serde(default)]
    pub(crate) preserve_archives: bool,
    #[serde(rename = "v")]
    pub(crate) version: u8,
    /// Always true. It tells a marker from a record entry.
    pub(crate) drain: bool,
    /// The full storage prefix of the directory entries of the slot.
    pub(crate) dir_prefix: String,
    /// The full storage key of the `mk:` slot. An entry counts only when its
    /// own slot key is equal.
    pub(crate) slot: String,
    /// The full storage key of this queue entry.
    pub(crate) queue: String,
    /// When the erase ran, in nanoseconds of the retention clock.
    pub(crate) erased_at: u64,
    /// The retention window of the node when it wrote the marker, in nanoseconds.
    /// Retained for older record decoders. It is not a completion deadline:
    /// a marker stays until its exact slot directory is drained.
    #[serde(default)]
    pub(crate) window_nanos: u64,
}

/// The value of a queue entry.
pub(crate) enum QueueValue {
    Record(RetentionEntry),
    Drain(DrainMarker),
}

impl QueueValue {
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        let value: serde_json::Value = serde_json::from_slice(bytes)?;
        if value.get("drain").is_some() {
            serde_json::from_value(value).map(Self::Drain)
        } else {
            serde_json::from_value(value).map(Self::Record)
        }
    }
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
        // An entry that a repair wrote again has no slot directory entry of its own
        // (`dir` is empty): there is no key to delete.
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
