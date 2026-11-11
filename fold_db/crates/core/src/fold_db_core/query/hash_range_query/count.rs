use futures::FutureExt;

use super::HashRangeQueryProcessor;
use crate::schema::types::field::HashRangeFilter;
use crate::schema::{Schema, SchemaError};

impl HashRangeQueryProcessor {
    /// Count the distinct **live** records a keyed query over `schema` returns.
    pub async fn count_rows(
        &self,
        schema: &Schema,
        filter: Option<&HashRangeFilter>,
    ) -> Result<usize, SchemaError> {
        Self::require_supported_query(schema, filter)?;
        let comparison = self.db_ops.records().key_comparison_context(schema).await?;
        let filter = comparison
            .canonical_filter(filter.cloned())?
            .ok_or_else(|| {
                SchemaError::InvalidData("LastDB requires a Hash or HashRange key selector".into())
            })?;
        self.current_query_scope(
            comparison,
            async move { self.count_records(schema, &filter).await }.boxed(),
        )
        .await
    }
}
