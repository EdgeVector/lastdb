//! Captured query entry points. Reads and counts share schema preparation;
//! each path keeps its namespace, window, and count-fallback decisions explicit.

use crate::db_operations::DbOperations;
use crate::schema::types::field::KeyWindow;
use crate::schema::types::key_value::KeyValue;
use crate::schema::types::Query;
use crate::schema::SchemaCore;
use crate::schema::SchemaError;
use std::sync::Arc;

use super::QueryRecords;

use super::hash_range_query::{FieldQueryOptions, HashRangeQueryProcessor};

mod alternate;
mod count;
mod current;
pub use alternate::AlternatePage;
mod prepare;

/// Main query executor that handles all query operations
pub struct QueryExecutor {
    db_ops: Arc<DbOperations>,
    schema_manager: Arc<SchemaCore>,
    hash_range_processor: HashRangeQueryProcessor,
}

impl QueryExecutor {
    /// Create a new query executor with storage abstraction.
    pub fn new(db_ops: &Arc<DbOperations>, schema_manager: Arc<SchemaCore>) -> Self {
        let hash_range_processor = HashRangeQueryProcessor::new(Arc::clone(db_ops));

        Self {
            db_ops: Arc::clone(db_ops),
            schema_manager,
            hash_range_processor,
        }
    }

    /// Read each stored record the query names.
    ///
    /// Local access control is consent-only. The reply is the stored record.
    /// It is not a field projection.
    pub async fn query(&self, query: Query) -> Result<QueryRecords, SchemaError> {
        self.query_internal(query, None).await
    }

    /// [`Self::query`], hydrating only one page of the matched rows.
    ///
    /// `window` is `(offset, limit)` over the canonical
    /// `KeyValuePageOrder::cmp_page_order` enumeration of the keys `query.filter`
    /// matched. It is applied before the records are read, so a keyed read of
    /// a large partition costs a page rather than the partition.
    ///
    /// Only pass a window for a key-restricted `query.filter` (see
    /// [`HashRangeFilter::is_key_restricted`]) with no `value_filters`,
    /// `field_predicates`, `order_by` or `predicate_limit`: each of those
    /// decides which rows survive *after* the key set is read, so a window
    /// applied first would answer from an arbitrary slice of their input. The
    /// node's push-down branch enforces exactly that precondition.
    pub async fn query_windowed(
        &self,
        query: Query,
        window: (usize, usize),
    ) -> Result<QueryRecords, SchemaError> {
        self.query_internal(query, Some(KeyWindow::offset(window.0, window.1)))
            .await
    }

    /// [`Self::query`], hydrating one keyset page after `after`.
    ///
    /// The same preconditions as [`Self::query_windowed`] apply:
    /// the caller's filter selects the key set, while this window only selects
    /// the continuation slice inside that key set.
    pub async fn query_keyset_windowed(
        &self,
        query: Query,
        after: KeyValue,
        limit: usize,
    ) -> Result<QueryRecords, SchemaError> {
        self.query_internal(query, Some(KeyWindow::after(after, limit)))
            .await
    }

    /// [`Self::query`], hydrating one page of `window`: an
    /// offset page, a keyset page, or a record walk page after a record key
    /// position ([`KeyWindow::AfterRecord`], an unfiltered read only). The
    /// preconditions of [`Self::query_windowed`] apply.
    pub async fn query_window(
        &self,
        query: Query,
        window: KeyWindow,
    ) -> Result<QueryRecords, SchemaError> {
        self.query_internal(query, Some(window)).await
    }

    async fn query_internal(
        &self,
        query: Query,
        window: Option<KeyWindow>,
    ) -> Result<QueryRecords, SchemaError> {
        let scope = self.db_ops.read_scope().await?;
        let future = Box::pin(self.query_current(query, window));
        scope.run_result(future).await
    }
}
