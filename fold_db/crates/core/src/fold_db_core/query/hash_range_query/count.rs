use super::{FutureExt, HashRangeQueryProcessor};
use crate::schema::types::field::HashRangeFilter;
use crate::schema::{Schema, SchemaError};

impl HashRangeQueryProcessor {
    /// Count the distinct **live** records a query over `schema` / `fields`
    /// would return -- the exact `total_count` for the paginated list path.
    ///
    /// A deleted record does not count. A record counts when it has the key
    /// field or one of the selected fields, as the page does. While a sender
    /// is active, the received scopes join the count.
    ///
    /// `filter` scopes the count to the same rows the page path will read.
    /// `None` counts the schema's whole live record set. Passing a
    /// key-restricted filter counts that **partition** — which is what a
    /// `HashKey` read's `total_count` has to mean, or the caller pages a
    /// partition against the schema's record count and `has_more` never goes
    /// false. Only key-restricted filters belong here: `Page` / `PageAfter`
    /// bound how many rows come back rather than which ones exist, so
    /// counting under one would report the page size as the total.
    pub async fn count_rows(
        &self,
        schema: &Schema,
        fields: &[String],
        filter: Option<&HashRangeFilter>,
    ) -> Result<usize, SchemaError> {
        Self::require_supported_query(schema, filter)?;
        let comparison = self.db_ops.records().key_comparison_context(schema).await?;
        let filter = comparison.canonical_filter(filter.cloned())?;
        let context = std::sync::Arc::clone(&comparison);
        self.current_query_scope(
            comparison,
            async move {
                self.count_records(schema, fields, filter.as_ref(), &context)
                    .await
            }
            .boxed(),
        )
        .await
    }
}
