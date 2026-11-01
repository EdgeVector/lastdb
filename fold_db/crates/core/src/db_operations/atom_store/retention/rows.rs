//! The queue key of one stored conflict row.

use super::keys;
use crate::db_operations::atom_store::AtomStore;

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
