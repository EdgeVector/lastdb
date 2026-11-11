//! Captured keyed query flow.

use futures::FutureExt;

use crate::fold_db_core::query::QueryRecords;
use crate::schema::types::field::{HashRangeFilter, KeyWindow};
use crate::schema::{Schema, SchemaError};

use super::HashRangeQueryProcessor;

impl HashRangeQueryProcessor {
    /// Capture the key comparison before reading the keyed candidates.
    pub async fn query_with_filter(
        &self,
        schema: &Schema,
        filter: Option<HashRangeFilter>,
        window: Option<KeyWindow>,
    ) -> Result<QueryRecords, SchemaError> {
        Self::require_supported_query(schema, filter.as_ref())?;
        let comparison = self.db_ops.records().key_comparison_context(schema).await?;
        let filter = comparison.canonical_filter(filter)?.ok_or_else(|| {
            SchemaError::InvalidData("LastDB requires a Hash or HashRange key selector".into())
        })?;
        let window = comparison.canonical_window(window)?;
        self.current_query_scope(
            comparison,
            async move { self.query_records(schema, &filter, window).await }.boxed(),
        )
        .await
    }
}
