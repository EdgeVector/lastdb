//! Captured query flow: select candidates, match conditions, then project values.

use futures::FutureExt;

use crate::fold_db_core::query::QueryRecords;
use crate::schema::types::field::{HashRangeFilter, KeyWindow};
use crate::schema::types::operations::FieldPredicate;
use crate::schema::{Schema, SchemaError};

use super::{FieldQueryOptions, HashRangeQueryProcessor, OptRedactedFilter};

impl HashRangeQueryProcessor {
    /// Capture the key comparison before the read. Conditions examine every
    /// keyed candidate before order, limit and projection.
    pub async fn query_with_field_predicates(
        &self,
        schema: &Schema,
        fields: &[String],
        filter: Option<HashRangeFilter>,
        window: Option<KeyWindow>,
        options: FieldQueryOptions<'_>,
    ) -> Result<QueryRecords, SchemaError> {
        Self::require_supported_query(schema, filter.as_ref())?;
        let comparison = self.db_ops.atoms().key_comparison_context(schema).await?;
        let filter = comparison.canonical_filter(filter)?;
        let window = comparison.canonical_window(window)?;
        if options.requires_condition_pass() {
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
        let context = std::sync::Arc::clone(&comparison);
        self.current_query_scope(
            comparison,
            async move {
                self.query_records(schema, fields, filter, window, options, &context)
                    .await
            }
            .boxed(),
        )
        .await
    }
}
