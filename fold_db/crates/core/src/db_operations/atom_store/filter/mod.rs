//! Filtered molecule loads and HashRange page-index scans.

mod load;
mod page_index;
mod partition_page;
mod per_key;

use super::types::PerKeyRecord;

/// Whether a bounded page window counts **stored** rows or **live** rows.
///
/// A tombstoned key is a stored row: it holds a slot in the molecule's key
/// order until it is compacted away. Applying `offset`/`limit` to stored rows
/// and dropping tombstones *afterwards* returns `limit × live_fraction` rows
/// and reports success — measured on the primary as 23 rows for `limit=100`,
/// and as a cursor walk that ended after 40 of 3127 rows because the host reads
/// "page shorter than the limit" as "set exhausted".
///
/// [`PageFill::LiveRows`] moves the window onto the rows the caller will
/// actually be shown, scanning forward past tombstones to fill it. It uses the
/// same `KeyMetadata.tombstoned` gate as `count_rows`, so `total_count` and the
/// page it describes are counted by one rule.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum PageFill {
    /// Every stored row counts against the window, tombstoned or not — what an
    /// `include_tombstones = true` read asks for.
    StoredRows,
    /// Only rows whose `KeyMetadata.tombstoned` is unset count against the
    /// window.
    LiveRows,
}

impl PageFill {
    /// Whether `record` occupies a slot in this window.
    pub(crate) fn keeps(self, record: &PerKeyRecord) -> bool {
        self.keeps_meta(record.meta.as_ref())
    }

    /// Whether a row carrying `meta` occupies a slot in this window.
    pub(crate) fn keeps_meta(self, meta: Option<&crate::atom::KeyMetadata>) -> bool {
        match self {
            Self::StoredRows => true,
            Self::LiveRows => !meta.is_some_and(|meta| meta.tombstoned),
        }
    }

    /// How many stored rows to ask for when `need` more in-window rows are
    /// wanted.
    ///
    /// Over-asking keeps a sparse live fraction from turning one page into a
    /// long chain of tiny scans; `saturating_mul` leaves the `usize::MAX`
    /// full-span request (the exact-count path) as the single unbounded scan it
    /// already was.
    pub(crate) fn chunk_for(self, need: usize) -> usize {
        match self {
            Self::StoredRows => need,
            Self::LiveRows => need.saturating_mul(2).max(64),
        }
    }
}
