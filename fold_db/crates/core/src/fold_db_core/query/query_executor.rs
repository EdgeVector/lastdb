//! Query Executor
//!
//! Main query execution logic extracted from FoldDB core, handling all query types
//! including HashRange schemas with proper delegation to specialized processors.

use crate::access::AccessContext;
use crate::db_operations::DbOperations;
use crate::schema::types::field::{FieldValue, KeyWindow};
use crate::schema::types::key_value::KeyValue;
use crate::schema::types::Query;
use crate::schema::Schema;
use crate::schema::SchemaCore;
use crate::schema::SchemaError;
use std::collections::HashMap;
use std::sync::Arc;

use super::hash_range_query::{
    FieldQueryOptions, HashRangeQueryProcessor, HashRangeWatch, HashRangeWatchBounds,
};

mod current;
mod run;

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
    /// [`KeyValue::cmp_page_order`] enumeration of the keys `query.filter`
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

    /// Count live keys for the same supported keyed query and projection.
    /// Uses molecule keys and tombstone flags; mapped HashRange layouts can
    /// also require the declared hash-field values to distinguish their keys.
    /// See
    /// [`HashRangeQueryProcessor::count_rows`](super::hash_range_query::HashRangeQueryProcessor::count_rows).
    ///
    /// Returns `Ok(None)` for a missing schema or field-dependent controls, so
    /// the caller uses the regular captured query and counts its selected keys.
    ///
    /// The required key filter scopes `total_count` to the requested keys.
    /// Unsupported layouts and filters receive the same refusal as reads.
    /// Conditions, value ordering, predicate limits and tombstone reads decline
    /// this route rather than report an unfiltered count.
    pub async fn count_query_rows(
        &self,
        query: &Query,
        access_context: &AccessContext,
    ) -> Result<Option<usize>, SchemaError> {
        let scope = self.db_ops.read_scope().await?;
        let future = Box::pin(self.count_query_rows_scoped(query, access_context));
        scope.run_result(future).await
    }

    async fn count_query_rows_scoped(
        &self,
        query: &Query,
        access_context: &AccessContext,
    ) -> Result<Option<usize>, SchemaError> {
        // App reads are unrestricted once the app is approved
        // (decision-2026-07-10-apps-read-all-write-own-namespace): no per-read
        // namespace gate. Multi-DB storage_prefix still scopes the count.
        let storage_prefix = self
            .db_ops
            .db_catalog()
            .resolve_storage_prefix(
                access_context.db_locator.as_deref(),
                &query.schema_name,
                access_context.storage_prefix.as_deref(),
            )
            .await?;

        match self
            .schema_manager
            // Hot list-count path: take the molecule-stripped read clone, exactly
            // as `query_internal` does. `count_rows` re-hydrates every counted
            // field from storage (`resolve_value`, and `cloned_without_molecule`
            // for shared namespaces), so the registry's hydrated molecule would
            // only be discarded before use — cloning it per count was the same
            // O(field cardinality) deep clone the read variant avoids (#905).
            .get_schema_following_supersession_for_read(&query.schema_name)
            .await?
        {
            Some(mut schema) => {
                self.schema_manager.require_queryable_schema(&schema.name)?;
                crate::schema::require_logical_layout(&schema.schema_type)?;
                crate::schema::require_logical_filter(query.filter.as_ref())?;
                crate::schema::require_logical_query_controls(query)?;
                self.db_ops
                    .molecule_keys()
                    .load_schema(&schema)
                    .await
                    .map_err(|error| {
                        SchemaError::InvalidData(format!(
                            "load molecule key bundles for schema '{}': {error}",
                            schema.name
                        ))
                    })?;
                Self::reject_unknown_projection(&schema, &query.fields)?;
                if query.value_filters.is_some()
                    || query.field_predicates.is_some()
                    || query.order_by.is_some()
                    || query.predicate_limit.is_some()
                    || query.include_tombstones
                {
                    return Ok(None);
                }
                if let Some(prefix) = storage_prefix.as_deref() {
                    for field in schema.runtime_fields.values_mut() {
                        field
                            .common_mut()
                            .set_storage_prefix(Some(prefix.to_string()));
                    }
                }
                let count_filter = query.filter.as_ref().filter(|f| f.is_key_restricted());
                let count = self
                    .hash_range_processor
                    .count_rows(&mut schema, &query.fields, count_filter)
                    .await?;
                Ok(Some(count))
            }
            None => Ok(None),
        }
    }

    /// Reject a projection naming a field the schema does not declare.
    ///
    /// Previously an unknown name was dropped on the floor: every consumer
    /// looks fields up with `runtime_fields.get(name)` and `continue`s past a
    /// miss, so a typo'd projection produced a byte-identical response to the
    /// correct one. Measured on the live primary against fkanban `Milestone`:
    ///
    /// ```text
    /// fields = ["slug"]                              -> 3 rows, total_count 14,191
    /// fields = ["slug","actor_that_does_not_exist"]  -> 3 rows, total_count 14,191
    /// ```
    ///
    /// Silence is the wrong answer twice over. A caller that misspells a field
    /// gets a plausible result set with that column missing and no way to tell
    /// the difference from a field that is legitimately empty — and a caller
    /// debugging a row set that came back short cannot rule the projection out.
    /// `SchemaError::InvalidField` renders as a `400`, which is what a
    /// malformed request should have been all along.
    ///
    /// An EMPTY projection stays legal: it means "every field", and every
    /// caller relying on that is asking for a set the schema does define.
    fn reject_unknown_projection(schema: &Schema, fields: &[String]) -> Result<(), SchemaError> {
        let mut unknown: Vec<&str> = fields
            .iter()
            .filter(|name| !schema.runtime_fields.contains_key(*name))
            .map(String::as_str)
            .collect();
        if unknown.is_empty() {
            return Ok(());
        }
        unknown.sort_unstable();
        unknown.dedup();
        Err(SchemaError::InvalidField(format!(
            "schema '{}' has no field(s): {}",
            schema.name,
            unknown.join(", ")
        )))
    }

    async fn query_internal(
        &self,
        query: Query,
        window: Option<KeyWindow>,
        access_context: Option<&AccessContext>,
    ) -> Result<HashMap<String, HashMap<KeyValue, FieldValue>>, SchemaError> {
        let scope = self.db_ops.read_scope().await?;
        let future = Box::pin(self.query_internal_scoped(query, window, access_context));
        scope.run_result(future).await
    }
}
