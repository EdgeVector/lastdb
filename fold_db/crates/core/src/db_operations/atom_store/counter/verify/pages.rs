//! The range reads of the blob phases. Nothing here takes the write gate and
//! nothing here writes.

use super::report::Phase;
use crate::db_operations::atom_store::blob_counter::{counter_range, BlobQueue};
use crate::db_operations::atom_store::AtomStore;
use crate::schema::SchemaError;
use crate::storage::traits::YieldedScan;

/// The keys that a page read keeps for the reads it makes for each row it gets:
/// the queue entry and the blob row of a counter, or the counter of a queue
/// entry.
pub(super) const VERIFY_KEYS_PER_ROW: usize = 4;

/// A read the walk asked for: start, end and the number of rows.
pub(super) type Request = (Vec<u8>, Vec<u8>, usize);

/// Where a blob phase reads: the start and the exclusive end of its range. The
/// queue phase and the end have no range.
pub(super) fn phase_range(phase: Phase) -> Option<(Vec<u8>, Vec<u8>)> {
    match phase {
        Phase::Blobs => Some(counter_range()),
        Phase::BlobQueue => Some(BlobQueue::entries_range()),
        Phase::Queue | Phase::Done => None,
    }
}

impl AtomStore {
    /// Read one verify page. The key budget stays [`VERIFY_KEYS_PER_ROW`].
    /// The caller keeps its own read scope.
    pub(super) async fn read_verify_page(
        &self,
        start: &[u8],
        end: &[u8],
        asked: usize,
    ) -> Result<YieldedScan, SchemaError> {
        self.raw()
            .inner()
            .scan_range_yielding(start, end, asked, VERIFY_KEYS_PER_ROW)
            .await
            .map_err(SchemaError::from)
    }
}
