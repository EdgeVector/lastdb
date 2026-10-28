//! Format 4 mapper adoption uses the normal typed schema load and publication.

use super::SchemaCore;
use crate::schema::types::SchemaError;

mod typed;

impl SchemaCore {
    /// Resolve typed mapper compatibility before publishing the schema.
    pub async fn apply_field_mappers(&self, schema_name: &str) -> Result<(), SchemaError> {
        let schema = self.db_ops.get_schema(schema_name).await?.ok_or_else(|| {
            SchemaError::InvalidData(format!("Schema '{schema_name}' not found in database"))
        })?;
        self.db_ops.molecule_keys().schema_profile(&schema)?;
        self.load_schema_internal(schema).await
    }
}
