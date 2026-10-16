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

        let mut results = self
            .read_scoped_results(&mut schema, &mut query, window)
            .await?;
        crate::storage::disk_pack::logical_paths::admit_query_results(
            self.db_ops.namespaced_store().disk_pack_runtime(),
            &schema,
            &mut results,
        )?;

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
        let results = if crate::db_operations::resident_read::graph_reads_enabled(&self.db_ops) {
            filter_schema_key_tombstones(results, self.db_ops.resident(), &query.schema_name)
        } else {
            results
        };

        Ok(results)
    }

    /// Read the matching records: the document-envelope fast path for an
    /// exact hash+range key, otherwise the field-predicate processor.
    async fn read_scoped_results(
        &self,
        schema: &mut Schema,
        query: &mut Query,
        window: Option<KeyWindow>,
    ) -> Result<HashMap<String, HashMap<KeyValue, FieldValue>>, SchemaError> {
        let envelope_first = if query.as_of.is_none() {
            match query.filter.as_ref() {
                Some(HashRangeFilter::HashRangeKey { hash, range }) => {
                    crate::record_molecule::hash_range_key_from_envelope(
                        &self.db_ops,
                        schema,
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
                .await?;
            crate::record_molecule::overlay_document_envelope(&self.db_ops, schema, &mut zipped)
                .await?;
            zipped
        };
        Ok(results)
    }
}
