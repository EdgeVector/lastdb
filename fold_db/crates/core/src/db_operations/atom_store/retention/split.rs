//! Split a page of due queue rows into records and unfinished slot drains.

use super::entry::{DrainMarker, QueueValue};
use super::{Retention, RetentionPorts};
use crate::db_operations::atom_store::time_queue::DuePage;

impl<S: RetentionPorts> Retention<'_, S> {
    /// The keys of the record entries of a page and its drain markers.
    /// An entry that does not decode,
    /// or whose key has no readable expiry, is counted and left alone: the cursor
    /// moves past it, and the entry stays.
    ///
    /// Age cannot prove that a drain is complete: older records can have a
    /// longer retention window than the window at the erase. Only the drain's
    /// exact directory walk may retire its marker, in the final release batch.
    pub(super) fn split_due_rows(&self, page: &DuePage) -> (Vec<String>, Vec<DrainMarker>) {
        let queue = self.store.retention_state().queue();
        let mut records = Vec::new();
        let mut markers = Vec::new();
        for (key, value) in &page.rows {
            if queue.entry_expiry(key).is_none() {
                tracing::error!(%key, "retention queue key has no readable expiry");
                self.store.retention_state().note_undecodable();
                continue;
            }
            match QueueValue::decode(value) {
                Ok(QueueValue::Record(_)) => records.push(key.clone()),
                Ok(QueueValue::Drain(marker)) => markers.push(marker),
                Err(error) => {
                    tracing::error!(%key, %error, "retention queue entry does not decode");
                    self.store.retention_state().note_undecodable();
                }
            }
        }
        (records, markers)
    }
}
