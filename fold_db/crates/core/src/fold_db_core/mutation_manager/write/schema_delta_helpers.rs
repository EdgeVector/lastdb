//! Current schema definitions for native write preparation.

use super::*;

impl MutationManager {
    /// Load the schema's current field definitions for private mutation preparation.
    pub(in crate::fold_db_core::mutation_manager) async fn load_schema_for_delta(
        &self,
        schema_name: &str,
    ) -> Result<crate::schema::types::Schema, SchemaError> {
        let snapshot = self.db_ops.schemas().read_snapshot(schema_name).await?;
        if snapshot.state == SchemaState::Blocked {
            return Err(SchemaError::Blocked(format!(
                "Schema '{schema_name}' is blocked and cannot be mutated (a drop is running)"
            )));
        }
        let schema = snapshot
            .schema
            .ok_or_else(|| SchemaError::InvalidData(format!("Schema '{schema_name}' not found")))?;
        crate::schema::require_logical_layout(&schema.schema_type)?;
        self.db_ops.field_keys().load_schema(&schema).await?;
        Ok(schema)
    }
}
