use super::*;

impl QueryExecutor {
    pub(super) async fn query_internal_scoped(
        &self,
        query: Query,
        window: Option<KeyWindow>,
        access_context: Option<&AccessContext>,
    ) -> Result<HashMap<String, HashMap<KeyValue, FieldValue>>, SchemaError> {
        crate::storage::disk_pack::logical_paths::field::query_scope(self.query_current(
            query,
            window,
            access_context,
        ))
        .await
    }

    async fn query_current(
        &self,
        mut query: Query,
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

        let Some(mut schema) = self
            .schema_manager
            // Resolved fields replace their molecules. Avoid a deep clone of
            // molecule data that the read would discard.
            .get_schema_following_supersession_for_read(&query.schema_name)
            .await?
        else {
            return Err(SchemaError::InvalidData(format!(
                "Schema '{}' not found",
                query.schema_name
            )));
        };
        self.schema_manager.require_queryable_schema(&schema.name)?;
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

        rules::reject_unknown_projection(&schema, &query.fields)?;

        // Scope molecule reads to the request DB handle (personal /
        // org). Clone-only schema — never written back to the registry.
        rules::scope_fields(&mut schema, storage_prefix.as_deref());

        if let Some(runtime) = self.db_ops.namespaced_store().disk_pack_runtime() {
            runtime
                .preload_schema_keys(Arc::clone(&self.db_ops), &schema)
                .await?;
        }

        self.hash_range_processor
            .query_with_field_predicates(
                &mut schema,
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
