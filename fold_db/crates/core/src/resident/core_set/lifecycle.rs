//! The purge removes the least recently used unheld records.

use super::*;

/// Victims selected per scan of the recency list.
const PURGE_BATCH: usize = 64;

impl CoreWarmSet {
    /// Explicit pressure relief to a byte target below the configured limit.
    /// A record that a call still holds stays.
    pub(crate) fn purge_clean_to_bytes(&mut self, target: usize) -> (usize, usize, usize) {
        let before = self.byte_stats().used_bytes;
        let purged = self.purge_clean_until(target);
        (purged, before, self.byte_stats().used_bytes)
    }

    fn fits(&self, incoming: usize) -> bool {
        self.bytes.used().saturating_add(incoming) <= crate::resident::core_warm_bytes()
    }

    /// Purge least recently used unheld records until `incoming` more bytes fit.
    pub(super) fn trim_to_limit(&mut self, incoming: usize) {
        let limit = crate::resident::core_warm_bytes();
        if self.fits(incoming) {
            return;
        }
        self.purge_clean_until(limit.saturating_sub(incoming));
        if !self.fits(incoming) {
            self.bytes.note_over_limit();
        }
    }

    /// Remove unheld records oldest first until the charge is at or under
    /// `target`. Returns the number of records removed.
    fn purge_clean_until(&mut self, target: usize) -> usize {
        let before = self.bytes.used();
        let mut purged = 0;
        // Bounded batches: the order list is not borrowed while records leave.
        'scan: loop {
            let records = self.record_victims(PURGE_BATCH);
            if records.is_empty() {
                break;
            }
            for (_, key) in records {
                if self.bytes.used() <= target {
                    break 'scan;
                }
                if self.purge_record(&key) {
                    purged += 1;
                }
            }
        }
        self.purged = self.purged.saturating_add(purged as u64);
        self.bytes
            .note_purged(before.saturating_sub(self.bytes.used()));
        purged
    }
}
