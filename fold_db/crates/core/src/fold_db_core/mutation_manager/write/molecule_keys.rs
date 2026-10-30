use super::{Mutation, MutationManager, SchemaError};

impl MutationManager {
    /// Establish one key authority before the first write in any database.
    pub(in crate::fold_db_core::mutation_manager) async fn prepare_batch_molecule_keys(
        &self,
        mutations: &[Mutation],
    ) -> Result<(), SchemaError> {
        let mut schema_names: Vec<&str> = mutations
            .iter()
            .map(|mutation| mutation.schema_name.as_str())
            .collect();
        schema_names.sort_unstable();
        schema_names.dedup();
        for schema_name in schema_names {
            let schema = self.schema_manager.require_schema_metadata(schema_name)?;
            let keys = self.db_ops.molecule_keys();
            keys.ensure_schema(&schema).await.map_err(|error| {
                SchemaError::InvalidData(format!(
                    "prepare molecule key bundles for schema '{schema_name}': {error}"
                ))
            })?;
        }
        Ok(())
    }
}
