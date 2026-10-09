//! Query Module - Dedicated query processing for FoldDB
//!
//! This module contains all query-related functionality extracted from the main FoldDB core,
//! providing a clean separation of concerns for query operations.

pub mod formatter;
pub mod hash_range_query;
pub mod query_executor;

// Re-export main query functionality
pub use formatter::{records_from_field_map, FieldMetadata, Record};
pub use hash_range_query::{
    HashRangeQueryProcessor, HashRangeWatch, HashRangeWatchBounds, HashRangeWatchError,
    HashRangeWatchEvent,
};
pub use query_executor::QueryExecutor;
