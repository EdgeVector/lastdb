//! Search-plane change feed and offline rebuild helpers.
//!
//! Mini no longer hosts an in-process native embedding index. Live writes
//! deliver [`IndexChangeBatch`] JSON to the first-party Search app inbox;
//! offline rebuild pages product records into the same inbox. Semantic
//! recall itself lives outside Mini.

pub(crate) mod search_outbox;
pub(crate) mod search_rebuild;
pub(crate) mod sink;

pub use search_outbox::{
    deliver_search_outbox_best_effort, resolve_search_inbox_dir, resolve_search_inbox_dir_for_home,
    search_inbox_dir_for_home, SearchOutboxSink,
};
pub use search_rebuild::{
    page_searchable_records_for_rebuild, rebuild_search_outbox_from_source,
    rebuild_search_outbox_into_inbox, run_search_rebuild_for_home, write_batches_to_search_inbox,
    FixtureBatchSource, SearchRebuildCursor, SearchRebuildPage, SearchRebuildReport,
};
pub use sink::{IndexChange, IndexChangeBatch, IndexChangeKind, IndexSink};
