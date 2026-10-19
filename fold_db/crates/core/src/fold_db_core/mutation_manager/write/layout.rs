//! Reject unsupported logical layouts before any write preparation.

use super::*;
use std::collections::BTreeSet;

impl MutationManager {
    pub(in crate::fold_db_core::mutation_manager) fn validate_mutation_layouts(
        &self,
        mutations: &[Mutation],
    ) -> Result<(), SchemaError> {
        let schemas: BTreeSet<_> = mutations
            .iter()
            .map(|mutation| mutation.schema_name.as_str())
            .collect();
        for name in schemas {
            let schema = self
                .schema_manager
                .get_schema_metadata(name)?
                .ok_or_else(|| SchemaError::InvalidData(format!("Schema '{name}' not found")))?;
            crate::schema::require_logical_layout(&schema.schema_type)?;
        }
        Ok(())
    }
}
