//! Current schema definitions and share prefixes for native write preparation.

use super::*;

impl MutationManager {
    /// Load the schema's current field definitions for private mutation preparation.
    pub(in crate::fold_db_core::mutation_manager) async fn load_schema_for_delta(
        &self,
        schema_name: &str,
        storage_prefix: Option<&str>,
    ) -> Result<crate::schema::types::Schema, SchemaError> {
        let snapshot = self.db_ops.schemas().read_snapshot(schema_name).await?;
        if snapshot.state == SchemaState::Blocked {
            return Err(SchemaError::Blocked(format!(
                "Schema '{schema_name}' is blocked and cannot be mutated"
            )));
        }
        let mut schema = snapshot
            .schema
            .ok_or_else(|| SchemaError::InvalidData(format!("Schema '{schema_name}' not found")))?;
        crate::schema::require_logical_layout(&schema.schema_type)?;
        self.db_ops.molecule_keys().load_schema(&schema).await?;
        apply_storage_prefix_to_schema(&mut schema, storage_prefix);
        Ok(schema)
    }

    /// Active share-rule prefixes that apply to this schema (personal namespace only).
    pub(super) async fn active_share_prefixes(
        &self,
        schema_name: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<String>, SchemaError> {
        if storage_prefix.is_some() {
            return Ok(Vec::new());
        }
        Ok(crate::sharing::store::list_share_rules_in_ops(&self.db_ops)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "load share rules for schema '{schema_name}': {error}"
                ))
            })?
            .into_iter()
            .filter(|rule| rule.active && rule.scope_matches(schema_name))
            .map(|rule| rule.share_prefix)
            .collect())
    }
}
