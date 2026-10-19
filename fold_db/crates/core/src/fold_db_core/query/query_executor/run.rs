//! Execution of one query against an already-resolved schema.

use super::*;

impl QueryExecutor {
    /// Gate, scope and read one schema, then apply value filters and drop
    /// key-tombstoned records.
    pub(super) async fn run_query_on_schema(
        &self,
        mut schema: Schema,
        mut query: Query,
        window: Option<KeyWindow>,
        storage_prefix: Option<String>,
    ) -> Result<HashMap<String, HashMap<KeyValue, FieldValue>>, SchemaError> {
        // Enforce Blocked state
        let resolved_state = self.schema_manager.get_schema_state_cached(&schema.name)?;
        if resolved_state == SchemaState::Blocked {
            return Err(SchemaError::Blocked(format!(
                "Schema '{}' is blocked and cannot be queried",
                schema.name
            )));
        }
        crate::schema::require_logical_layout(&schema.schema_type)?;
        crate::schema::require_logical_filter(query.filter.as_ref())?;
        crate::schema::require_current_read(query.as_of.is_some())?;

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

        // Scope molecule reads to the request DB handle (personal /
        // org). Clone-only schema — never written back to the registry.
        if let Some(prefix) = storage_prefix.as_deref() {
            for field in schema.runtime_fields.values_mut() {
                field
                    .common_mut()
                    .set_storage_prefix(Some(prefix.to_string()));
            }
        }

        if let Some(runtime) = self.db_ops.namespaced_store().disk_pack_runtime() {
            runtime
                .preload_schema_keys(Arc::clone(&self.db_ops), &schema)
                .await?;
        }

        let results = self
            .read_scoped_results(&mut schema, &mut query, window)
            .await?;

        // Numeric post-filters on field values (`value_filters`) — the
        // LLM agent populates this for prompts like "price < 600". The
        // struct + LLM prompt have advertised the field for a while
        // but no execution path applied it, so queries silently
        // returned the unfiltered key-based result set.
        let results = apply_value_filters(results, query.value_filters.as_deref());
        Ok(results)
    }

    /// Read every query shape through the same field-molecule processor.
    async fn read_scoped_results(
        &self,
        schema: &mut Schema,
        query: &mut Query,
        window: Option<KeyWindow>,
    ) -> Result<HashMap<String, HashMap<KeyValue, FieldValue>>, SchemaError> {
        self.hash_range_processor
            .query_with_field_predicates(
                schema,
                &query.fields,
                query.filter.take(),
                window,
                query.field_predicates.as_deref(),
                query.order_by.as_ref(),
                query.predicate_limit,
                query.as_of,
                query.include_tombstones,
                query.secondary_concurrency,
            )
            .await
    }
}
