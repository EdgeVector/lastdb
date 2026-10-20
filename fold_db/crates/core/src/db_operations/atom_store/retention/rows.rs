//! The rows that a new conflict record writes beside its history row: one
//! queue entry and one slot directory entry.

use super::entry::{RetentionEntry, ENTRY_VERSION};
use super::keys;
use crate::db_operations::atom_store::AtomStore;
use crate::schema::SchemaError;
use serde_json::Value;

/// The slot of a record, in storage form: the same coordinates that the `mk:`
/// key of the slot uses.
pub(crate) struct RecordSlot<'a> {
    pub(crate) molecule: &'a str,
    pub(crate) hash: &'a str,
    pub(crate) range: &'a str,
}

impl AtomStore {
    /// The queue entry and the directory entry of a record. The caller hands
    /// them to the planner as the `new_rows` of the record, so they are written
    /// in the batch of the history row, and only when the record is new.
    ///
    /// The queue entry expires at the node clock plus the retention window.
    pub(crate) fn retention_rows(
        &self,
        scope: Option<&str>,
        history_key: &str,
        slot: &RecordSlot<'_>,
        conflict: Option<String>,
    ) -> Result<Vec<(String, Value)>, SchemaError> {
        let id = keys::record_id(history_key);
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX);
        let expires = u64::try_from(now)
            .unwrap_or(0)
            .saturating_add(self.retention_state().window_nanos());
        let entry = RetentionEntry {
            version: ENTRY_VERSION,
            history: history_key.to_string(),
            conflict,
            dir: keys::dir_key(scope, slot.molecule, slot.hash, slot.range, &id),
            queue: keys::queue_key(expires, &id),
            slot: keys::slot_key(scope, slot.molecule, slot.hash, slot.range),
        };
        let value = serde_json::to_value(&entry).map_err(|error| {
            SchemaError::InvalidData(format!("serialize retention entry: {error}"))
        })?;
        Ok(vec![
            (entry.queue.clone(), value.clone()),
            (entry.dir.clone(), value),
        ])
    }
}
