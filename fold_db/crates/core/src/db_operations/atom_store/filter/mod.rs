//! Filtered molecule loads and selected page records.

mod load;
mod page_records;
mod partition_page;
mod per_key;

pub(crate) use super::hash_selection::HashTipSelection;
pub(crate) use per_key::SchemaKeyMembership;

use super::ports::{
    AtomReads, DeleteBarrierReads, KeyCodecs, MainRows, MoleculeReads, NativeRuntime, QueryTips,
    ReadyKeys,
};
use super::types::PerKeyRecord;

/// The ports the filter concept reads through. The filter names no store type.
pub(crate) trait FilterPorts:
    AtomReads
    + DeleteBarrierReads
    + KeyCodecs
    + MainRows
    + MoleculeReads
    + NativeRuntime
    + QueryTips
    + ReadyKeys
{
}

impl<T> FilterPorts for T where
    T: AtomReads
        + DeleteBarrierReads
        + KeyCodecs
        + MainRows
        + MoleculeReads
        + NativeRuntime
        + QueryTips
        + ReadyKeys
{
}

/// Filtered molecule loads, per-key reads and key pages over the ports of one store.
pub(crate) struct Filter<'s, S> {
    store: &'s S,
}

impl<'s, S: FilterPorts> Filter<'s, S> {
    pub(crate) fn new(store: &'s S) -> Self {
        Self { store }
    }
}

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

/// A ready key page replaces the stored scan. A miss still runs that scan.
async fn prefer_preloaded_rows<Fut>(
    preloaded: Result<Option<Vec<(String, PerKeyRecord)>>, crate::schema::SchemaError>,
    scan: impl FnOnce() -> Fut,
) -> Result<Vec<(String, PerKeyRecord)>, crate::schema::SchemaError>
where
    Fut: std::future::Future<
        Output = Result<Vec<(String, PerKeyRecord)>, crate::schema::SchemaError>,
    >,
{
    if let Some(rows) = preloaded? {
        Ok(rows)
    } else {
        scan().await
    }
}
