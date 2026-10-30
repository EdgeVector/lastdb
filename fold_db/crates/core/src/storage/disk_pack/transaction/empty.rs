//! A read-only retry must not create a new write or publication boundary.

use super::{PendingBatch, StorageResult};

impl PendingBatch {
    pub(super) fn is_empty(&self) -> StorageResult<bool> {
        let unchanged = self
            .data
            .lock()
            .expect("native pending batch")
            .operations
            .is_empty()
            && self.memory.is_empty()
            && self
                .after_commit
                .lock()
                .expect("native post-commit work")
                .is_empty()
            && self
                .retained
                .lock()
                .expect("native request resources")
                .is_empty();
        Ok(unchanged && self.publications.is_empty()?)
    }
}
