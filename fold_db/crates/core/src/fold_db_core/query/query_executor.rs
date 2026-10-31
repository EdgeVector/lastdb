//! Captured query entry points. Reads and counts share schema preparation;
//! each path keeps its namespace, window, and count-fallback decisions explicit.

use crate::access::AccessContext;
use crate::db_operations::DbOperations;
use crate::schema::types::field::{FieldValue, KeyWindow};
use crate::schema::types::key_value::KeyValue;
use crate::schema::types::Query;
use crate::schema::SchemaCore;
use crate::schema::SchemaError;
use std::collections::HashMap;
use std::sync::Arc;

use super::hash_range_query::{
    FieldQueryOptions, HashRangeQueryProcessor, HashRangeWatch, HashRangeWatchBounds,
};

mod alternate;
mod count;
mod current;
pub use alternate::AlternatePage;
mod prepare;
mod rules;

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

    /// Start a live watch for one HashRange partition.
    pub fn watch_hash_range(
        &self,
        schema: impl Into<String>,
        hash: impl Into<String>,
        bounds: Option<HashRangeWatchBounds>,
    ) -> HashRangeWatch {
        self.hash_range_processor.watch(schema, hash, bounds)
    }

    /// Start a watch without range bounds.
    pub fn watch_partition(
        &self,
        schema: impl Into<String>,
        hash: impl Into<String>,
    ) -> HashRangeWatch {
        self.hash_range_processor.watch_partition(schema, hash)
    }

    /// Start a watch with inclusive-start/exclusive-end range bounds.
    pub fn watch_range(
        &self,
        schema: impl Into<String>,
        hash: impl Into<String>,
        start: Option<String>,
        end: Option<String>,
    ) -> HashRangeWatch {
        self.hash_range_processor
            .watch_range(schema, hash, start, end)
    }

    /// Query multiple fields from a schema (legacy -- no access control)
    pub async fn query(
        &self,
        query: Query,
    ) -> Result<HashMap<String, HashMap<KeyValue, FieldValue>>, SchemaError> {
        self.query_internal(query, None, None).await
    }

    /// Query with local caller context.
    ///
    /// Local access control is consent-only. Fields are no longer filtered by
    /// trust tiers, capability quotas, or payment gates.
    /// The internal field map can include key and ordering helpers. Response
    /// formatters remove an unprojected ordering helper after the final sort.
    pub async fn query_with_access(
        &self,
        query: Query,
        access_context: &AccessContext,
    ) -> Result<HashMap<String, HashMap<KeyValue, FieldValue>>, SchemaError> {
        self.query_internal(query, None, Some(access_context)).await
    }

    /// [`Self::query_with_access`], hydrating only one page of the matched rows.
    ///
    /// `window` is `(offset, limit)` over the canonical
    /// `KeyValuePageOrder::cmp_page_order` enumeration of the keys `query.filter`
    /// matched. It is applied before atom bodies are loaded, so a keyed read of
    /// a large partition costs a page rather than the partition.
    ///
    /// Only pass a window for a key-restricted `query.filter` (see
    /// [`HashRangeFilter::is_key_restricted`]) with no `value_filters`,
    /// `field_predicates`, `order_by` or `predicate_limit`: each of those
    /// decides which rows survive *after* the key set is read, so a window
    /// applied first would answer from an arbitrary slice of their input. The
    /// node's push-down branch enforces exactly that precondition.
    pub async fn query_with_access_windowed(
        &self,
        query: Query,
        window: (usize, usize),
        access_context: &AccessContext,
    ) -> Result<HashMap<String, HashMap<KeyValue, FieldValue>>, SchemaError> {
        self.query_internal(
            query,
            Some(KeyWindow::offset(window.0, window.1)),
            Some(access_context),
        )
        .await
    }

    /// [`Self::query_with_access`], hydrating one keyset page after `after`.
    ///
    /// The same preconditions as [`Self::query_with_access_windowed`] apply:
    /// the caller's filter selects the key set, while this window only selects
    /// the continuation slice inside that key set.
    pub async fn query_with_access_keyset_windowed(
        &self,
        query: Query,
        after: KeyValue,
        limit: usize,
        access_context: &AccessContext,
    ) -> Result<HashMap<String, HashMap<KeyValue, FieldValue>>, SchemaError> {
        self.query_internal(
            query,
            Some(KeyWindow::after(after, limit)),
            Some(access_context),
        )
        .await
    }

    async fn query_internal(
        &self,
        query: Query,
        window: Option<KeyWindow>,
        access_context: Option<&AccessContext>,
    ) -> Result<HashMap<String, HashMap<KeyValue, FieldValue>>, SchemaError> {
        let scope = self.db_ops.read_scope().await?;
        let future = Box::pin(
            crate::storage::disk_pack::logical_paths::field::query_scope(self.query_current(
                query,
                window,
                access_context,
            )),
        );
        scope.run_result(future).await
    }
}
