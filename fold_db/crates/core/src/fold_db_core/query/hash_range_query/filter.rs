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

struct PredicateSelection<'a> {
    query: QuerySelection<'a>,
    predicates: Option<&'a [FieldPredicate]>,
    order_by: Option<&'a QueryOrderBy>,
    predicate_limit: Option<usize>,
}

impl HashRangeQueryProcessor {
    /// Two-pass field predicate query.
    ///
    /// Pass A scans candidate keys while loading only predicate fields and the
    /// optional `order_by` field. Pass B fetches the requested projection for
    /// keys that survived the predicate set. This is a two-pass scan, not an
    /// index: pass A is still O(M) over the candidate set.
    #[allow(clippy::too_many_arguments)]
    pub async fn query_with_field_predicates(
        &self,
        schema: &mut Schema,
        fields: &[String],
        filter: Option<HashRangeFilter>,
        window: Option<KeyWindow>,
        predicates: Option<&[FieldPredicate]>,
        order_by: Option<&QueryOrderBy>,
        predicate_limit: Option<usize>,
        include_tombstones: bool,
        request_concurrency: Option<usize>,
    ) -> Result<HashMap<String, HashMap<KeyValue, FieldValue>>, SchemaError> {
        Self::require_supported_query(schema, filter.as_ref())?;
        let selection = PredicateSelection {
            query: QuerySelection {
                schema,
                fields,
                filter,
                window,
                include_tombstones,
                request_concurrency,
            },
            predicates,
            order_by,
            predicate_limit,
        };
        self.current_query_scope(self.predicate_query_captured(selection).boxed())
            .await
    }

    async fn predicate_query_captured(
        &self,
        selection: PredicateSelection<'_>,
    ) -> Result<HashMap<String, HashMap<KeyValue, FieldValue>>, SchemaError> {
        let PredicateSelection {
            query,
            predicates,
            order_by,
            predicate_limit,
        } = selection;
        let has_predicates = predicates.is_some_and(|p| !p.is_empty());
        let has_two_pass_shape = has_predicates || order_by.is_some() || predicate_limit.is_some();
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
        let scan_fields = self.scan_fields(schema, predicates.unwrap_or(&[]), order_by)?;
        let pass_a_filter = filter.or_else(|| Some(full_span_page()));
        // Pass A must see every candidate: the predicate set, the ordering and
        // `predicate_limit` are all applied to what survives it, so a window
        // here would decide the answer from an arbitrary slice of the input.
        // The two-pass shape keeps the unwindowed path.
        let pass_a = self
            .query_cokey_with_sources(
                schema,
                &scan_fields,
                pass_a_filter,
                None,
                include_tombstones,
                &received_from_namespaces,
                request_concurrency,
            )
            .await?;
        let mut kept_keys = self.matching_field_predicate_keys(&pass_a, predicates.unwrap_or(&[]));
        if let Some(order_by) = order_by {
            self.sort_keys_by_field(&mut kept_keys, &pass_a.fields, order_by);
        }
        if let Some(limit) = predicate_limit {
            kept_keys.truncate(limit);
        }

        // Pass B resolves every projected field as a secondary, which blinds what
        // it is given — so it must be handed API form or it misses on every field
        // and the whole filtered query comes back empty on a blinded home. Pass A
        // already returns API form, so the keys carry straight through.
        self.query_exact_keys(
            schema,
            fields,
            &kept_keys,
            &pass_a.key_sources,
            include_tombstones,
            request_concurrency,
        )
        .await
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
            fields,
            filter,
            window,
            include_tombstones,
            &received,
            request_concurrency,
        )
        .await
    }
}
