//! Shared schema reads and gates, in their original rejection order.

use crate::schema::{types::Query, Schema, SchemaError};

use super::{rules, QueryExecutor};

impl QueryExecutor {
    /// Both callers resolve their namespace first. A read rejects an absent
    /// schema; a count declines it. Projection checks follow key-bundle loads,
    /// even when the count later declines field-dependent controls.
    pub(super) async fn prepare_query_schema(
        &self,
        query: &Query,
    ) -> Result<Option<Schema>, SchemaError> {
        // Reads and counts replace their field keys. Keep the registry clone
        // free of hydrated field key data that either path would discard.
        let Some(schema) = self
            .schema_manager
            .get_schema_following_supersession_for_read(&query.schema_name)
            .await?
        else {
            return Ok(None);
        };
        self.schema_manager.require_queryable_schema(&schema.name)?;
        crate::schema::require_logical_layout(&schema.schema_type)?;
        crate::schema::require_logical_filter(query.filter.as_ref())?;
        crate::schema::require_logical_query_controls(query)?;
        self.db_ops
            .field_keys()
            .load_schema(&schema)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "load field key bundles for schema '{}': {error}",
                    schema.name
                ))
            })?;
        rules::reject_unknown_projection(&schema, &query.fields)?;
        Ok(Some(schema))
    }
}
