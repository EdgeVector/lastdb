//! Typed mapper publication uses the normal schema load and key-contract checks.

use super::SchemaCore;
use crate::schema::types::SchemaError;

mod typed;

impl SchemaCore {
    /// Resolve shared-field molecules through the typed schema publication path.
    pub async fn apply_field_mappers(&self, schema_name: &str) -> Result<(), SchemaError> {
        let schema = self.db_ops.get_schema(schema_name).await?.ok_or_else(|| {
            SchemaError::InvalidData(format!("Schema '{schema_name}' not found in database"))
        })?;
        self.db_ops.molecule_keys().schema_profile(&schema)?;
        self.load_schema_internal(schema).await
    }
}
