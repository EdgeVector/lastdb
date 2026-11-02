//! Strict read-only catalog values from the caller's captured storage view.

use super::*;

mod names;

#[derive(Clone)]
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
        if let Some(snapshot) = self.prepared_snapshot(name)? {
            crate::write_preparation::count("schema_snapshot_prepared_reuses", 1);
            return Ok((**snapshot).clone());
        }
        // An ordinary record write does not change these catalog sources.
        // A private or accepted catalog/state/successor change refuses reuse,
        // so the existing captured storage path remains authoritative.
        if let Some(snapshot) =
            crate::storage::disk_pack::core_memory_get_for_write::<SchemaReadSnapshot>(
                &self.runtime,
                self.snapshot_owner(),
                name,
                &super::snapshot_sources(name),
            )?
        {
            crate::write_preparation::count("schema_snapshot_core_reuses", 1);
            return Ok((**snapshot).clone());
        }
        crate::write_preparation::count("schema_snapshot_decode_attempts", 1);
        let (schema, state) = tokio::try_join!(
            self.schemas_store.inner().get(name.as_bytes()),
            self.snapshot_state_bytes(name),
        )
        .map_err(SchemaError::from)?;
        let mut schema = schema
            .map(|bytes| decode_catalog_schema(&bytes))
            .transpose()
            .map_err(|error| SchemaError::StorageRead { detail: error })?;
        if let Some(schema) = &mut schema {
            schema.populate_runtime_fields()?;
        }
        let state = state
            .map(|bytes| decode_catalog_json::<SchemaState>(&bytes))
            .transpose()
            .map_err(|error| SchemaError::StorageRead { detail: error })?
            .unwrap_or_default();
        let successor = if state == SchemaState::Blocked {
            self.superseded_by_store
                .inner()
                .get(name.as_bytes())
                .await?
                .map(|bytes| decode_catalog_json::<String>(&bytes))
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
