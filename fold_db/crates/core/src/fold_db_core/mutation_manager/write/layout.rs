//! Validate lookup keys and supported layouts before any write preparation.

use super::*;
use std::collections::BTreeMap;

impl MutationManager {
    pub(in crate::fold_db_core::mutation_manager) fn validate_mutation_keys_and_layouts(
        &self,
        mutations: &[Mutation],
    ) -> Result<(), SchemaError> {
        for field in mutations
            .iter()
            .flat_map(|mutation| mutation.fields_and_values.keys())
        {
            crate::schema::Schema::validate_field_name(field).map_err(SchemaError::InvalidData)?;
        }
        let mut schemas = BTreeMap::new();
        for mutation in mutations {
            let name = mutation.schema_name.as_str();
            let schema = match schemas.entry(name) {
                std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::btree_map::Entry::Vacant(entry) => {
                    let schema =
                        self.schema_manager
                            .get_schema_metadata(name)?
                            .ok_or_else(|| {
                                SchemaError::InvalidData(format!("Schema '{name}' not found"))
                            })?;
                    crate::schema::require_logical_layout(&schema.schema_type)?;
                    entry.insert(schema)
                }
            };
            Self::resolve_mutation_key_value(name, schema, mutation)?;
        }
        Ok(())
    }
}
