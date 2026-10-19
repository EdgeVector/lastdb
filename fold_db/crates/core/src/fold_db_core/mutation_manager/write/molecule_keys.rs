use super::{Mutation, MutationManager, SchemaError};

impl MutationManager {
    /// Resolve durable key authority before a batch reads or plans molecule keys.
    pub(in crate::fold_db_core::mutation_manager) async fn prepare_batch_molecule_keys(
        &self,
        mutations: &[Mutation],
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        let mut schema_names: Vec<&str> = mutations
            .iter()
            .map(|mutation| mutation.schema_name.as_str())
            .collect();
        schema_names.sort_unstable();
        schema_names.dedup();
        for schema_name in schema_names {
            let schema = self
                .schema_manager
                .get_schema_metadata(schema_name)?
                .ok_or_else(|| {
                    SchemaError::InvalidData(format!("Schema '{schema_name}' not found"))
                })?;
            let keys = self.db_ops.molecule_keys();
            let prepared = if storage_prefix.is_some() {
                keys.ensure_schema(&schema).await
            } else {
                keys.load_schema(&schema).await
            };
            prepared.map_err(|error| {
                SchemaError::InvalidData(format!(
                    "prepare molecule key bundles for schema '{schema_name}': {error}"
                ))
            })?;
        }
        Ok(())
    }
}
