//! Filtered and windowed queries, including field-predicate queries.

use super::*;

struct QuerySelection<'a> {
    schema: &'a mut Schema,
    fields: &'a [String],
    filter: Option<HashRangeFilter>,
    window: Option<KeyWindow>,
    include_tombstones: bool,
    request_concurrency: Option<usize>,
}

/// Current field conditions and projection controls for one captured query.
#[derive(Default)]
pub struct FieldQueryOptions<'a> {
    pub predicates: Option<&'a [FieldPredicate]>,
    pub value_filters: Option<&'a [crate::schema::types::ValueFilter]>,
    pub order_by: Option<&'a QueryOrderBy>,
    pub predicate_limit: Option<usize>,
    pub include_tombstones: bool,
    pub secondary_concurrency: Option<usize>,
}

impl HashRangeQueryProcessor {
    /// Two-pass field predicate query.
    ///
    /// Pass A loads field and numeric conditions plus the optional `order_by`
    /// field. Pass B fetches the requested projection for
    /// keys that survived the predicate set. This is a two-pass scan, not an
    /// index: pass A is still O(M) over the candidate set.
    pub async fn query_with_field_predicates(
        &self,
        schema: &mut Schema,
        fields: &[String],
        filter: Option<HashRangeFilter>,
        window: Option<KeyWindow>,
        options: FieldQueryOptions<'_>,
    ) -> Result<HashMap<String, HashMap<KeyValue, FieldValue>>, SchemaError> {
        Self::require_supported_query(schema, filter.as_ref())?;
        let query = QuerySelection {
            schema,
            fields,
            filter,
            window,
            include_tombstones: options.include_tombstones,
            request_concurrency: options.secondary_concurrency,
        };
        self.current_query_scope(self.predicate_query_captured(query, options).boxed())
            .await
    }

    async fn predicate_query_captured(
        &self,
        query: QuerySelection<'_>,
        options: FieldQueryOptions<'_>,
    ) -> Result<HashMap<String, HashMap<KeyValue, FieldValue>>, SchemaError> {
        let FieldQueryOptions {
            predicates,
            value_filters,
            order_by,
            predicate_limit,
            ..
        } = options;
        let has_predicates = predicates.is_some_and(|p| !p.is_empty());
        let has_numeric_filters = value_filters.is_some_and(|filters| !filters.is_empty());
        let has_two_pass_shape = has_predicates
            || has_numeric_filters
            || order_by.is_some()
            || predicate_limit.is_some();
        if !has_two_pass_shape {
            return self.query_fields_captured(query).await;
        }
        let QuerySelection {
            schema,
            fields,
            filter,
            window: _,
            include_tombstones,
            request_concurrency,
        } = query;
        let current_user = crate::user_context::get_current_user_id();
        let current_user_redacted = current_user
            .as_deref()
            .map(|u| observability::redact_id!(u));
        tracing::debug!(
            "HashRangeQueryProcessor: schema={}, filter={}, field_predicates={}, order_by={:?}, user_context={:?}",
            schema.name,
            OptRedactedFilter(&filter),
            predicates.map_or(0, <[FieldPredicate]>::len),
            order_by.as_ref().map(|o| (&o.field, &o.order)),
            current_user_redacted
        );

        let received_from_namespaces: Vec<String> = self.collect_received_from_namespaces().await?;
        let scan_fields = self.scan_fields(
            schema,
            predicates.unwrap_or(&[]),
            value_filters.unwrap_or(&[]),
            order_by,
        )?;
        let pass_a_filter = filter.or_else(|| Some(full_span_page()));
        // Pass A must see every candidate: the predicate set, the ordering and
        // `predicate_limit` are all applied to what survives it, so a window
        // here would decide the answer from an arbitrary slice of the input.
        // The two-pass shape keeps the unwindowed path.
        let pass_a = self
            .query_cokey_with_sources(
                schema,
                CokeySelection {
                    fields: &scan_fields,
                    key_fields: Some(fields),
                    filter: pass_a_filter,
                    window: None,
                    include_tombstones,
                    received_from_namespaces: &received_from_namespaces,
                    request_concurrency,
                },
            )
            .await?;
        let kept_keys = self.select_condition_keys(&pass_a, &options);

        // Reuse captured secondaries only after conditions choose the API keys.
        // Remaining fields use the same exact reader and selected namespaces.
        self.query_exact_keys(schema, fields, &kept_keys, pass_a, &options)
            .await
    }

    fn select_condition_keys(
        &self,
        pass_a: &CokeyRows,
        options: &FieldQueryOptions<'_>,
    ) -> Vec<KeyValue> {
        let condition_started = std::time::Instant::now();
        let mut keys = self.matching_field_predicate_keys(
            pass_a,
            options.predicates.unwrap_or(&[]),
            options.value_filters.unwrap_or(&[]),
        );
        crate::request_phases::add_phase(
            crate::request_phases::RequestPhase::HydrateFilter,
            condition_started.elapsed(),
        );
        if let Some(order_by) = options.order_by {
            self.sort_keys_by_field(&mut keys, &pass_a.fields, order_by);
        }
        if let Some(limit) = options.predicate_limit {
            keys.truncate(limit);
        }
        keys
    }

    /// All values come from the selected field molecules in the captured read.
    async fn query_fields_captured(
        &self,
        query: QuerySelection<'_>,
    ) -> Result<HashMap<String, HashMap<KeyValue, FieldValue>>, SchemaError> {
        let QuerySelection {
            schema,
            fields,
            filter,
            window,
            include_tombstones,
            request_concurrency,
        } = query;
        let received = self.collect_received_from_namespaces().await?;
        self.query_cokey(
            schema,
            CokeySelection {
                fields,
                key_fields: None,
                filter,
                window,
                include_tombstones,
                received_from_namespaces: &received,
                request_concurrency,
            },
        )
        .await
    }
}
