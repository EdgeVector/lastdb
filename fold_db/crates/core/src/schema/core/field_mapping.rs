//! Every home is typed. Expansion reloads the raw stored schema, and the load
//! resolves typed mappers before it mints the molecule contract.

use super::SchemaCore;
use crate::schema::types::SchemaError;

mod typed;

impl SchemaCore {
    /// Resolve shared-field molecules after mapper compatibility validation.
    /// New fields keep their molecules. Repeated calls remain safe.
    pub async fn apply_field_mappers(&self, schema_name: &str) -> Result<(), SchemaError> {
        let schema = self.db_ops.get_schema(schema_name).await?.ok_or_else(|| {
            SchemaError::InvalidData(format!("Schema '{schema_name}' not found in database"))
        })?;
        self.load_schema_internal(schema).await
    }
}
