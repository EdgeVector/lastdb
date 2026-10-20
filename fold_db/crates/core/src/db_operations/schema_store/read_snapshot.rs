//! Strict read-only catalog values from the caller's captured storage view.

use super::*;

mod names;

pub(crate) struct SchemaReadSnapshot {
    pub(crate) schema: Option<Schema>,
    pub(crate) state: SchemaState,
    pub(crate) successor: Option<String>,
}

impl SchemaStore {
    /// No cache refresh, catalog repair, or durable write occurs on this path.
    pub(crate) async fn read_snapshot(
        &self,
        name: &str,
    ) -> Result<SchemaReadSnapshot, SchemaError> {
        let (schema, state) = tokio::try_join!(
            self.schemas_store.inner().get(name.as_bytes()),
            self.snapshot_state_bytes(name),
        )
        .map_err(SchemaError::from)?;
        let mut schema = schema
            .map(|bytes| decode_catalog_schema(&bytes, self.catalog_unwrap_key.as_ref()))
            .transpose()
            .map_err(|error| SchemaError::StorageRead { detail: error })?;
        if let Some(schema) = &mut schema {
            schema.populate_runtime_fields()?;
        }
        let state = state
            .map(|bytes| {
                decode_catalog_json::<SchemaState>(&bytes, self.catalog_unwrap_key.as_ref())
            })
            .transpose()
            .map_err(|error| SchemaError::StorageRead { detail: error })?
            .unwrap_or_default();
        let successor = if state == SchemaState::Blocked {
            self.superseded_by_store
                .inner()
                .get(name.as_bytes())
                .await?
                .map(|bytes| {
                    decode_catalog_json::<String>(&bytes, self.catalog_unwrap_key.as_ref())
                })
                .transpose()
                .map_err(|error| SchemaError::StorageRead { detail: error })?
        } else {
            None
        };
        Ok(SchemaReadSnapshot {
            schema,
            state,
            successor,
        })
    }

    async fn snapshot_state_bytes(
        &self,
        name: &str,
    ) -> crate::storage::error::StorageResult<Option<Vec<u8>>> {
        self.schema_states_store.inner().get(name.as_bytes()).await
    }
}
