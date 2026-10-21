//! Split a page of due queue rows into records, drain markers and dead markers.

use super::entry::{DrainMarker, QueueValue};
use crate::db_operations::atom_store::time_queue::DuePage;
use crate::db_operations::atom_store::AtomStore;

impl AtomStore {
    /// The keys of the record entries of a page, its drain markers, and the keys
    /// of the markers that are too old to drain. An entry that does not decode,
    /// or whose key has no readable expiry, is counted and left alone: the cursor
    /// moves past it, and the entry stays.
    ///
    /// A marker lives at most one retention window. Every record of an erased slot
    /// was written before the erase, so its own queue entry is due one window after
    /// the erase at the latest, and the ordinary lane releases it. A marker that is
    /// older than that has nothing left to speed up. The janitor deletes it without
    /// a drain, so no marker outlives the range that the safety net probes (one
    /// window and one slice before now).
    pub(super) fn split_due_rows(
        &self,
        page: &DuePage,
        now_nanos: u64,
    ) -> (Vec<String>, Vec<DrainMarker>, Vec<String>) {
        let queue = self.retention_state().queue();
        let window = self.retention_state().window_nanos();
        let mut records = Vec::new();
        let mut markers = Vec::new();
        let mut expired = Vec::new();
        for (key, value) in &page.rows {
            if queue.entry_expiry(key).is_none() {
                tracing::error!(%key, "retention queue key has no readable expiry");
                self.retention_state().note_undecodable();
                continue;
            }
            match QueueValue::decode(value) {
                Ok(QueueValue::Record(_)) => records.push(key.clone()),
                Ok(QueueValue::Drain(marker))
                    if marker.erased_at.saturating_add(window) < now_nanos =>
                {
                    expired.push(key.clone());
                }
                Ok(QueueValue::Drain(marker)) => markers.push(marker),
                Err(error) => {
                    tracing::error!(%key, %error, "retention queue entry does not decode");
                    self.retention_state().note_undecodable();
                }
            }
        }
        (records, markers, expired)
    }
}
