//! Search-plane outbox delivery helpers.
//!
//! Live writes deliver [`IndexChangeBatch`] JSON to the first-party Search app
//! inbox when `--enable-search` or `LASTDB_SEARCH_ENABLED=1` enables it.

pub(crate) mod inbox_fields;
pub mod queue_policy;
pub(crate) mod search_outbox;
pub(crate) mod sink;

pub use search_outbox::{resolve_search_inbox_dir, search_inbox_dir_for_home, SearchOutboxSink};
pub use sink::{IndexChange, IndexChangeBatch, IndexChangeKind};
