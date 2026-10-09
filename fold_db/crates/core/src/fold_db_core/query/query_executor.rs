//! Query Executor
//!
//! Main query execution logic extracted from FoldDB core, handling all query types
//! including HashRange schemas with proper delegation to specialized processors.

use crate::access::AccessContext;
use crate::db_operations::DbOperations;
use crate::schema::types::field::{FieldValue, HashRangeFilter, KeyWindow};
use crate::schema::types::key_value::KeyValue;
use crate::schema::types::Query;
use crate::schema::Schema;
use crate::schema::SchemaError;
use crate::schema::{SchemaCore, SchemaState};
use std::collections::HashMap;
use std::sync::Arc;

use super::hash_range_query::{HashRangeQueryProcessor, HashRangeWatch, HashRangeWatchBounds};
use value_filters::apply_value_filters;

mod value_filters;

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

    /// Count the **live** (non-tombstone) rows a list query over
    /// `query.schema_name` / `query.fields` would return — the exact
    /// `total_count` for the paginated [`HashRangeFilter::Page`] path.
    ///
    /// Counts from the molecule key index + `KeyMetadata.tombstoned` flag
    /// (no atom body loads). See
    /// [`HashRangeQueryProcessor::count_rows`](super::hash_range_query::HashRangeQueryProcessor::count_rows).
    ///
    /// Returns `Ok(None)` when the schema is not found, so the caller falls back
    /// to the regular materialize-then-count path.
    ///
    /// A **key-restricted** `query.filter` scopes the count to the rows that
    /// filter selects, so a keyed read's `total_count` describes its partition.
    /// Any other filter shape is ignored and the whole live row set is counted:
    /// `Page` / `PageAfter` bound how many rows come back rather than which
    /// ones exist, so counting under one would report the page size as the
    /// total. `value_filters` are likewise ignored — they are applied to rows
    /// after hydration, which is past the point a key count can see.
    pub async fn count_query_rows(
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
                let resolved_state = self.schema_manager.get_schema_state_cached(&schema.name)?;
                if resolved_state == SchemaState::Blocked {
                    return Err(SchemaError::Blocked(format!(
                        "Schema '{}' is blocked and cannot be queried",
                        schema.name
                    )));
                }
                if storage_prefix.is_some() {
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
                }
                Self::reject_unknown_projection(&schema, &query.fields)?;
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
        // Approved local callers read all local fields. Context is identity +
        // optional multi-DB storage_prefix (X-LastDB-Db → org cohabitation).
        let storage_prefix = match access_context {
            Some(context) => {
                self.db_ops
                    .db_catalog()
                    .resolve_storage_prefix(
                        context.db_locator.as_deref(),
                        &query.schema_name,
                        context.storage_prefix.as_deref(),
                    )
                    .await?
            }
            None => None,
        };

        match self
            .schema_manager
            // Hot keyed-read path: take the molecule-stripped clone. The
            // executor mutates `schema.runtime_fields` while `resolve_value`
            // re-hydrates each field from storage, so the registry's hydrated
            // molecule would only be overwritten before use — cloning it per
            // query was the O(field cardinality) residual this avoids (#905
            // follow-up).
            .get_schema_following_supersession_for_read(&query.schema_name)
            .await?
        {
            Some(mut schema) => {
                let owner = schema.owner_app_id.clone();
                crate::warm_admit::with_schema_owner(owner.as_deref(), async {
                    // Enforce Blocked state
                    let resolved_state =
                        self.schema_manager.get_schema_state_cached(&schema.name)?;
                    if resolved_state == SchemaState::Blocked {
                        return Err(SchemaError::Blocked(format!(
                            "Schema '{}' is blocked and cannot be queried",
                            schema.name
                        )));
                    }

                    if storage_prefix.is_some() {
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
                    }

                    Self::reject_unknown_projection(&schema, &query.fields)?;

                    // Scope molecule reads to the request DB handle (personal /
                    // org). Clone-only schema — never written back to the registry.
                    if let Some(prefix) = storage_prefix.as_deref() {
                        for field in schema.runtime_fields.values_mut() {
                            field
                                .common_mut()
                                .set_storage_prefix(Some(prefix.to_string()));
                        }
                    }

                    let envelope_first = if query.as_of.is_none() {
                        match query.filter.as_ref() {
                            Some(HashRangeFilter::HashRangeKey { hash, range }) => {
                                crate::record_molecule::hash_range_key_from_envelope(
                                    &self.db_ops,
                                    &schema,
                                    hash,
                                    range,
                                    &query.fields,
                                )
                                .await?
                            }
                            _ => None,
                        }
                    } else {
                        None
                    };
                    let results = if let Some(from_r) = envelope_first {
                        from_r
                    } else {
                        let mut zipped = self
                            .hash_range_processor
                            .query_with_field_predicates(
                                &mut schema,
                                &query.fields,
                                query.filter,
                                window,
                                query.field_predicates.as_deref(),
                                query.order_by.as_ref(),
                                query.predicate_limit,
                                query.as_of,
                                query.include_tombstones,
                                query.secondary_concurrency,
                            )
                            .await?;
                        crate::record_molecule::overlay_document_envelope(
                            &self.db_ops,
                            &schema,
                            &mut zipped,
                        )
                        .await?;
                        zipped
                    };

                    // Numeric post-filters on field values (`value_filters`) — the
                    // LLM agent populates this for prompts like "price < 600". The
                    // struct + LLM prompt have advertised the field for a while
                    // but no execution path applied it, so queries silently
                    // returned the unfiltered key-based result set.
                    let results = apply_value_filters(results, query.value_filters.as_deref());
                    // include_tombstones exposes legacy tombstone *values*. A
                    // resident Delete is record absence, including while its
                    // durable erase is queued. It must never reveal the old body.
                    // Overlay stamps cover personal and org-prefix erasures;
                    // count already filters both, so the page must too.
                    let results = filter_schema_key_tombstones(
                        results,
                        self.db_ops.resident(),
                        &query.schema_name,
                    );

                    Ok(results)
                })
                .await
            }
            None => Err(SchemaError::InvalidData(format!(
                "Schema '{}' not found",
                query.schema_name
            ))),
        }
    }
}

fn filter_schema_key_tombstones(
    mut results: HashMap<String, HashMap<KeyValue, FieldValue>>,
    resident: &crate::resident::ResidentGraph,
    schema_name: &str,
) -> HashMap<String, HashMap<KeyValue, FieldValue>> {
    for field_map in results.values_mut() {
        field_map.retain(|kv, _| {
            let hash = kv.hash.as_deref().unwrap_or("");
            let range = kv.range.as_deref().unwrap_or("");
            !resident.is_schema_key_tombstoned(schema_name, hash, range)
        });
    }
    results
}
