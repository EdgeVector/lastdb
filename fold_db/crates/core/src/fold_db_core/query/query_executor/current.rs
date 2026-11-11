//! Prepare the caller's schema, then read each stored record.

use crate::schema::types::field::KeyWindow;
use crate::schema::types::Query;
use crate::schema::SchemaError;

use super::{QueryExecutor, QueryRecords};

impl QueryExecutor {
    pub(super) async fn query_current(
        &self,
        mut query: Query,
        window: Option<KeyWindow>,
    ) -> Result<QueryRecords, SchemaError> {
        if query.key_name.is_some() {
            return Err(SchemaError::InvalidData(
                "use the named alternate page reader".into(),
            ));
        }
        let Some(schema) = self.prepare_query_schema(&query).await? else {
            return Err(SchemaError::InvalidData(format!(
                "Schema '{}' not found",
                query.schema_name
            )));
        };

        self.hash_range_processor
            .query_with_filter(&schema, query.filter.take(), window)
            .await
    }
}
