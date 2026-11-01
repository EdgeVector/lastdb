//! The slot directory side of a drain: the entry that one directory row holds.
//!
//! A format 6 erase wrote a drain marker for each slot that still held records.
//! No format 7 path writes a slot directory entry or a drain marker; the drain
//! lane reads the markers that a home may still hold.

use super::entry::RetentionEntry;
use crate::db_operations::atom_store::AtomStore;

impl AtomStore {
    /// The entry that one slot directory row holds, when it is a record of
    /// `slot` that was written at or before `created_max`.
    pub(super) fn slot_entry(
        &self,
        key: &[u8],
        value: &[u8],
        slot: &str,
        created_max: u64,
    ) -> Option<RetentionEntry> {
        let Ok(entry) = serde_json::from_slice::<RetentionEntry>(value) else {
            // An entry that does not decode names nothing to release. Its
            // record stays until its own window ends.
            tracing::error!(
                key = %String::from_utf8_lossy(key),
                "slot directory entry does not decode"
            );
            self.retention_state().note_undecodable();
            return None;
        };
        // A range that begins with the same bytes can belong to another slot
        // (a range that holds a NUL). Use an exact match only.
        (entry.slot == slot && entry.created <= created_max).then_some(entry)
    }
}
