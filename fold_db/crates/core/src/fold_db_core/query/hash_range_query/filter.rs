//! Captured query flow: select candidates, match conditions, then project values.

use std::collections::HashMap;

use futures::FutureExt;

use crate::schema::types::field::{FieldValue, HashRangeFilter, KeyWindow};
use crate::schema::types::operations::FieldPredicate;
use crate::schema::types::KeyValue;
use crate::schema::{Schema, SchemaError};

use super::{
    condition_fields, full_span_page, matching_field_predicate_keys, order_condition_keys,
    CokeySelection, FieldQueryOptions, HashRangeQueryProcessor, OptRedactedFilter,
};

impl HashRangeQueryProcessor {
    /// Capture the key comparison before either the direct or condition path.
    /// Conditions examine every keyed candidate before order, limit and projection.
    pub async fn query_with_field_predicates(
        &self,
        schema: &mut Schema,
        fields: &[String],
        filter: Option<HashRangeFilter>,
        window: Option<KeyWindow>,
        options: FieldQueryOptions<'_>,
    ) -> Result<HashMap<String, HashMap<KeyValue, FieldValue>>, SchemaError> {
        Self::require_supported_query(schema, filter.as_ref())?;
        let comparison = self.db_ops.atoms().key_comparison_context(schema).await?;
        let filter = comparison.canonical_filter(filter)?;
        let window = comparison.canonical_window(window)?;
        self.current_query_scope(
            comparison,
            self.query_captured(schema, fields, filter, window, options)
                .boxed(),
        )
        .await
    }

    async fn query_captured(
        &self,
        schema: &mut Schema,
        fields: &[String],
        filter: Option<HashRangeFilter>,
        window: Option<KeyWindow>,
        options: FieldQueryOptions<'_>,
    ) -> Result<HashMap<String, HashMap<KeyValue, FieldValue>>, SchemaError> {
        let needs_conditions = options.requires_condition_pass();
        if needs_conditions {
            let current_user = crate::user_context::get_current_user_id();
            let current_user_redacted = current_user
                .as_deref()
                .map(|u| observability::redact_id!(u));
            tracing::debug!(
                "HashRangeQueryProcessor: schema={}, filter={}, field_predicates={}, order_by={:?}, user_context={:?}",
                schema.name,
                OptRedactedFilter(&filter),
                options.predicates.map_or(0, <[FieldPredicate]>::len),
                options.order_by.as_ref().map(|o| (&o.field, &o.order)),
                current_user_redacted
            );
        }
        let received = self.collect_received_from_namespaces().await?;
        if !needs_conditions {
            return self
                .query_cokey_with_sources(
                    schema,
                    CokeySelection {
                        fields,
                        key_fields: None,
                        filter,
                        window,
                        include_tombstones: options.include_tombstones,
                        received_from_namespaces: &received,
                        request_concurrency: options.secondary_concurrency,
                    },
                )
                .await
                .map(|rows| rows.fields);
        }

        let candidate_fields = condition_fields(
            schema,
            options.predicates.unwrap_or(&[]),
            options.value_filters.unwrap_or(&[]),
            options.order_by,
        )?;
        // Conditions must see all candidates; an early window could discard
        // keys that survive the conditions, value order and explicit limit.
        let pass_a = self
            .query_cokey_with_sources(
                schema,
                CokeySelection {
                    fields: &candidate_fields,
                    key_fields: Some(fields),
                    filter: filter.or_else(|| Some(full_span_page())),
                    window: None,
                    include_tombstones: options.include_tombstones,
                    received_from_namespaces: &received,
                    request_concurrency: options.secondary_concurrency,
                },
            )
            .await?;
        let condition_started = std::time::Instant::now();
        let keys = matching_field_predicate_keys(
            &pass_a,
            options.predicates.unwrap_or(&[]),
            options.value_filters.unwrap_or(&[]),
        );
        crate::request_phases::add_phase(
            crate::request_phases::RequestPhase::HydrateFilter,
            condition_started.elapsed(),
        );
        let kept_keys = order_condition_keys(
            keys,
            &pass_a.fields,
            options.order_by,
            options.predicate_limit,
        );
        self.query_exact_keys(schema, fields, &kept_keys, pass_a, &options)
            .await
    }
}
