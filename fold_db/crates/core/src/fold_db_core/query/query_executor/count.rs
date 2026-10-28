//! Live-key counts use the same schema gates as reads, then decline field-dependent controls.

use super::*;

impl QueryExecutor {
    /// Count live keys for the same supported keyed query and projection.
    /// Uses molecule keys and tombstone flags; mapped HashRange layouts can
    /// also require the declared hash-field values to distinguish their keys.
    /// See
    /// [`HashRangeQueryProcessor::count_rows`].
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
            // Counted fields reload their molecules, so keep the registry clone
            // free of hydrated molecule data, as on the read path.
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
                rules::reject_unknown_projection(&schema, &query.fields)?;
                if query.requires_post_key_work() || query.include_tombstones {
                    return Ok(None);
                }
                rules::scope_fields(&mut schema, storage_prefix.as_deref());
                if let Some(runtime) = self.db_ops.namespaced_store().disk_pack_runtime() {
                    runtime
                        .preload_schema_keys(Arc::clone(&self.db_ops), &schema)
                        .await?;
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
}
