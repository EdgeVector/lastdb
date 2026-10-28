//! Typed publication resolves mapper chains before it mints molecule contracts.

use super::SchemaCore;
use crate::schema::types::SchemaError;

mod typed;

impl SchemaCore {
    /// Publish one schema through the typed loader.
    /// The loader resolves shared-field molecules before it mints contracts.
    pub async fn apply_field_mappers(&self, schema_name: &str) -> Result<(), SchemaError> {
        let schema = self.db_ops.get_schema(schema_name).await?.ok_or_else(|| {
            SchemaError::InvalidData(format!("Schema '{schema_name}' not found in database"))
        })?;
        // The profile read rejects key types that do not match the typed contract.
        self.db_ops.molecule_keys().schema_profile(&schema)?;
        self.load_schema_internal(schema).await
    }
}
