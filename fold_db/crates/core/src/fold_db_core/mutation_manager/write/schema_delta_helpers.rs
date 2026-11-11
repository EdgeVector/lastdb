//! Current schema definitions for native write preparation.

use super::*;

impl MutationManager {
    /// Load the schema's current field definitions for private mutation preparation.
    pub(in crate::fold_db_core::mutation_manager) async fn load_schema_for_delta(
        &self,
        schema_name: &str,
    ) -> Result<crate::schema::types::Schema, SchemaError> {
        let (mut schema, mut state) = self.schema_manager.get_schema_and_state(schema_name)?;
        if schema.is_none() && state != SchemaState::Blocked {
            schema = self.db_ops.schemas().get_schema(schema_name).await?;
            if schema.is_some() {
                state = self
                    .db_ops
                    .schemas()
                    .get_schema_state(schema_name)
                    .await?
                    .unwrap_or_default();
            }
        }
        if state == SchemaState::Blocked {
            return Err(SchemaError::Blocked(format!(
                "Schema '{schema_name}' is blocked and cannot be mutated (a drop is running)"
            )));
        }
        let schema = schema
            .ok_or_else(|| SchemaError::InvalidData(format!("Schema '{schema_name}' not found")))?;
        crate::schema::require_logical_layout(&schema.schema_type)?;
        Ok(schema)
    }
}
