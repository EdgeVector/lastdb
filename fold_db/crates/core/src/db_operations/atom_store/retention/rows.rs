//! The rows that a new conflict record writes beside its history row: one
//! queue entry, the directory row of its queue bucket, and one slot directory
//! entry.

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
    /// The queue entry, the directory row of its bucket, and the slot directory
    /// entry of a record. The caller hands them to the planner as the
    /// `new_rows` of the record, so they are written in the batch of the history
    /// row, and only when the record is new.
    ///
    /// The queue entry expires at the retention clock plus the retention
    /// window (`RetentionState::clock_nanos`, which ignores a forward
    /// excursion of the wall clock of more than a day). It goes to the bucket of
    /// its expiry (`time_queue`).
    pub(crate) fn retention_rows(
        &self,
        scope: Option<&str>,
        history_key: &str,
        slot: &RecordSlot<'_>,
        conflict: Option<String>,
    ) -> Result<Vec<(String, Value)>, SchemaError> {
        let id = keys::record_id(history_key);
        let queue = self.retention_state().queue();
        let created = self.retention_state().clock_nanos();
        let expires = created.saturating_add(self.retention_state().window_nanos());
        let entry = RetentionEntry {
            version: ENTRY_VERSION,
            history: history_key.to_string(),
            conflict,
            dir: keys::dir_key(scope, slot.molecule, slot.hash, slot.range, &id),
            queue: queue.entry_key(expires, &id),
            slot: keys::slot_key(scope, slot.molecule, slot.hash, slot.range),
            created,
        };
        let value = serde_json::to_value(&entry).map_err(|error| {
            SchemaError::InvalidData(format!("serialize retention entry: {error}"))
        })?;
        let mut rows = queue.rows(expires, &id, &value);
        rows.push((entry.dir, value));
        Ok(rows)
    }
}
