//! Filtered and windowed queries, including field-predicate queries.

use super::*;

impl HashRangeQueryProcessor {
    /// Single co-key query path for all filters, field counts, share
    /// namespaces, and optional `as_of`.
    pub async fn query_with_filter(
        &self,
        schema: &mut Schema,
        fields: &[String],
        filter: Option<HashRangeFilter>,
        as_of: Option<DateTime<Utc>>,
        include_tombstones: bool,
    ) -> Result<HashMap<String, HashMap<KeyValue, FieldValue>>, SchemaError> {
        self.query_with_filter_windowed(schema, fields, filter, None, as_of, include_tombstones)
            .await
    }

    /// [`Self::query_with_filter`], bounded to one page of the matched keys.
    ///
    /// `window` is `(offset, limit)` over the [`KeyValue::cmp_page_order`]
    /// enumeration of the keys `filter` matched, applied before any atom body
    /// is loaded. Callers pass it only for a key-restricted filter — one that
    /// selects a key set rather than bounding a scan — because `Page` and
    /// `PageAfter` already carry their own bounds and windowing them twice
    /// would page a page.
    #[allow(
        clippy::too_many_arguments,
        reason = "the window rides alongside the filter it bounds; bundling them into a struct would only move the argument list"
    )]
    pub async fn query_with_filter_windowed(
        &self,
        schema: &mut Schema,
        fields: &[String],
        filter: Option<HashRangeFilter>,
        window: Option<KeyWindow>,
        as_of: Option<DateTime<Utc>>,
        include_tombstones: bool,
    ) -> Result<HashMap<String, HashMap<KeyValue, FieldValue>>, SchemaError> {
        self.query_with_filter_windowed_override(
            schema,
            fields,
            filter,
            window,
            as_of,
            include_tombstones,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn query_with_filter_windowed_override(
        &self,
        schema: &mut Schema,
        fields: &[String],
        filter: Option<HashRangeFilter>,
        window: Option<KeyWindow>,
        as_of: Option<DateTime<Utc>>,
        include_tombstones: bool,
        request_concurrency: Option<usize>,
    ) -> Result<HashMap<String, HashMap<KeyValue, FieldValue>>, SchemaError> {
        let current_user = crate::user_context::get_current_user_id();
        let current_user_redacted = current_user
            .as_deref()
            .map(|u| observability::redact_id!(u));
        tracing::debug!(
            "HashRangeQueryProcessor: schema={}, filter={}, window={:?}, user_context={:?}",
            schema.name,
            OptRedactedFilter(&filter),
            window,
            current_user_redacted
        );

        let received_from_namespaces: Vec<String> = self.collect_received_from_namespaces().await;
        self.query_cokey(
            schema,
            fields,
            filter,
            window,
            as_of,
            include_tombstones,
            &received_from_namespaces,
            request_concurrency,
        )
        .await
    }

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
        as_of: Option<DateTime<Utc>>,
        include_tombstones: bool,
        request_concurrency: Option<usize>,
    ) -> Result<HashMap<String, HashMap<KeyValue, FieldValue>>, SchemaError> {
        let has_predicates = predicates.is_some_and(|p| !p.is_empty());
        let has_two_pass_shape = has_predicates || order_by.is_some() || predicate_limit.is_some();
        if !has_two_pass_shape {
            return self
                .query_with_filter_windowed_override(
                    schema,
                    fields,
                    filter,
                    window,
                    as_of,
                    include_tombstones,
                    request_concurrency,
                )
                .await;
        }

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

        let received_from_namespaces: Vec<String> = self.collect_received_from_namespaces().await;
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
                as_of,
                include_tombstones,
                &received_from_namespaces,
                request_concurrency,
            )
            .await?;

        let mut kept_keys =
            self.matching_field_predicate_keys(&pass_a, predicates.unwrap_or(&[]))?;
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
        let kept_sources: HashMap<KeyValue, KeySource> = kept_keys
            .iter()
            .filter_map(|key| {
                pass_a
                    .key_sources
                    .get(key)
                    .cloned()
                    .map(|source| (key.clone(), source))
            })
            .collect();

        self.query_exact_keys(
            schema,
            fields,
            &kept_keys,
            &kept_sources,
            as_of,
            include_tombstones,
        )
        .await
    }
}
