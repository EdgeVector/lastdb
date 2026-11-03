//! Exact schema and state reads, including the existing catalog repair path.

use super::*;

impl SchemaStore {
    /// Get a specific schema by name.
    ///
    /// Query falls back here on a cache miss (`get_schema_following_supersession_for_read`).
    /// A row that does not decode is an error.
    pub async fn get_schema(&self, schema_name: &str) -> Result<Option<Schema>, SchemaError> {
        if let Some(snapshot) = self.prepared_snapshot(schema_name)? {
            return Ok(snapshot.schema.clone());
        }
        let Some(bytes) = self
            .schemas_store
            .inner()
            .get(schema_name.as_bytes())
            .await?
        else {
            return Ok(None);
        };

        let mut schema =
            decode_catalog_schema(&bytes).map_err(|error| SchemaError::StorageRead {
                detail: format!("schema catalog row {schema_name} is unreadable: {error}"),
            })?;
        schema.populate_runtime_fields()?;

        Ok(Some(schema))
    }

    /// Get the state of a specific schema. A row that does not decode is an
    /// error.
    pub async fn get_schema_state(
        &self,
        schema_name: &str,
    ) -> Result<Option<SchemaState>, SchemaError> {
        let Some(bytes) = self
            .schema_states_store
            .inner()
            .get(schema_name.as_bytes())
            .await?
        else {
            return Ok(None);
        };
        let state = decode_catalog_json::<SchemaState>(&bytes).map_err(|error| {
            SchemaError::StorageRead {
                detail: format!("schema state row {schema_name} is unreadable: {error}"),
            }
        })?;
        Ok(Some(state))
    }

    pub(super) async fn require_installed_schema(
        &self,
        schema_name: &str,
    ) -> Result<(), SchemaError> {
        if self.get_schema(schema_name).await?.is_some() {
            Ok(())
        } else {
            Err(SchemaError::NotFound(schema_name.to_string()))
        }
    }
}
