//! Execution of one query against an already-resolved schema.

use super::*;

impl QueryExecutor {
    /// Gate, scope and read one schema through the current field conditions.
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
        crate::schema::require_logical_query_controls(&query)?;

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

        self.read_scoped_results(&mut schema, &mut query, window)
            .await
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
                FieldQueryOptions {
                    predicates: query.field_predicates.as_deref(),
                    value_filters: query.value_filters.as_deref(),
                    order_by: query.order_by.as_ref(),
                    predicate_limit: query.predicate_limit,
                    include_tombstones: query.include_tombstones,
                    secondary_concurrency: query.secondary_concurrency,
                },
            )
            .await
    }
}
