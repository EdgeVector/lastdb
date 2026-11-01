//! The rows that a new conflict record writes beside its history row: one
//! queue entry, the directory row of its queue bucket, and one slot directory
//! entry.

use super::entry::{RetentionEntry, ENTRY_VERSION};
use super::keys;
use crate::db_operations::atom_store::AtomStore;
use crate::schema::SchemaError;
use serde_json::Value;

impl AtomStore {
    /// The queue entry and the directory row of its bucket for one conflict
    /// row of a format 7 home. The entry names the row as its history row and
    /// the record as its slot. It has no slot directory entry: no erase lane
    /// drains a record.
    pub(crate) fn record_conflict_retention_rows(
        &self,
        row_key: &str,
        record_key: &str,
        created: u64,
        expires: u64,
    ) -> Result<Vec<(String, Value)>, SchemaError> {
        let id = keys::record_id(row_key);
        let queue = self.retention_state().queue();
        let entry = RetentionEntry {
            version: ENTRY_VERSION,
            history: row_key.to_string(),
            conflict: None,
            dir: String::new(),
            queue: queue.entry_key(expires, &id),
            slot: record_key.to_string(),
            created,
        };
        let value = serde_json::to_value(&entry).map_err(|error| {
            SchemaError::InvalidData(format!("serialize retention entry: {error}"))
        })?;
        Ok(queue.rows(expires, &id, &value))
    }
}

impl AtomStore {
    /// The queue entry key of the conflict row `row_key` that expires at
    /// `expires`. A purge deletes the entry with its row. The directory row of
    /// the bucket is shared, and the janitor retires the bucket.
    pub(crate) fn record_conflict_queue_key(&self, row_key: &str, expires: u64) -> String {
        self.retention_state()
            .queue()
            .entry_key(expires, &keys::record_id(row_key))
    }
}
