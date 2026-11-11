//! HashRange Query Processor
//!
//! A home keeps one record at each primary key. A query reads the
//! records that its key selector names (`records.rs`), applies the keyed
//! window, and returns each stored record.

use crate::db_operations::DbOperations;
use crate::schema::types::field::HashRangeFilter;
use crate::schema::{Schema, SchemaError};
use std::sync::Arc;

mod count;
mod filter;
mod records;

/// Processor for keyed record queries of a home.
pub struct HashRangeQueryProcessor {
    db_ops: Arc<DbOperations>,
}

impl HashRangeQueryProcessor {
    /// Create a new HashRange query processor.
    pub fn new(db_ops: Arc<DbOperations>) -> Self {
        Self { db_ops }
    }

    fn require_supported_query(
        schema: &Schema,
        filter: Option<&HashRangeFilter>,
    ) -> Result<(), SchemaError> {
        crate::schema::require_logical_layout(&schema.schema_type)?;
        crate::schema::require_logical_filter(filter)
    }

    async fn current_query_scope<T>(
        &self,
        comparison: Arc<crate::schema::types::key_comparison::KeyComparisonContext>,
        future: impl std::future::Future<Output = Result<T, SchemaError>>,
    ) -> Result<T, SchemaError> {
        let scope = self.db_ops.read_scope().await?;
        comparison.run(scope.run_result(future)).await
    }
}
