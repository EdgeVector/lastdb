//! Typed schema writes and their complete native publication scope.

use super::*;
use futures::FutureExt;

impl SchemaStore {
    /// Store a schema (no-op if durable catalog bytes already match).
    pub async fn store_schema(
        &self,
        schema_name: &str,
        schema: &Schema,
    ) -> Result<(), SchemaError> {
        crate::storage::disk_pack::transaction::scope(
            Arc::clone(&self.runtime),
            self.store_schema_native(schema_name, schema).boxed(),
        )
        .await
    }

    async fn store_schema_native(
        &self,
        schema_name: &str,
        schema: &Schema,
    ) -> Result<(), SchemaError> {
        let input = schema_input(schema_name, schema)?;
        if let Some(existing) = self.get_schema(schema_name).await? {
            if existing.file_required != schema.file_required {
                return Err(SchemaError::InvalidData(
                    "changing the file requirement needs a new schema identity".into(),
                ));
            }
            if durable_schema_eq(&existing, schema) {
                return Ok(());
            }
        }
        self.format.put_schema(input).await?;
        self.format.flush_schemas().await?;
        Ok(())
    }

    /// Store schema state (no-op if durable state already matches).
    pub async fn store_schema_state(
        &self,
        schema_name: &str,
        state: &SchemaState,
    ) -> Result<(), SchemaError> {
        crate::storage::disk_pack::transaction::scope(
            Arc::clone(&self.runtime),
            self.store_schema_state_native(schema_name, state).boxed(),
        )
        .await
    }

    async fn store_schema_state_native(
        &self,
        schema_name: &str,
        state: &SchemaState,
    ) -> Result<(), SchemaError> {
        if self
            .schema_states_store
            .get_item::<SchemaState>(schema_name)
            .await?
            .is_some_and(|existing| existing == *state)
        {
            return Ok(());
        }
        self.format
            .put_state(StateChange::State {
                schema: schema_name,
                state,
            })
            .await?;
        self.format.flush_states().await?;
        Ok(())
    }

    /// Remove one installed schema identity from the catalog.
    pub async fn drop_schema(&self, schema_name: &str) -> Result<bool, SchemaError> {
        self.release_drop_tracking(schema_name).await?;
        crate::storage::disk_pack::transaction::scope(
            Arc::clone(&self.runtime),
            self.drop_schema_native(schema_name).boxed(),
        )
        .await
    }

    async fn drop_schema_native(&self, schema_name: &str) -> Result<bool, SchemaError> {
        let existed = self.get_schema(schema_name).await?.is_some();
        let state_keys = vec![
            StateDelete::Key {
                schema: schema_name,
                kind: StateKey::State,
            },
            StateDelete::Key {
                schema: schema_name,
                kind: StateKey::Claim,
            },
            StateDelete::Key {
                schema: schema_name,
                kind: StateKey::Policy,
            },
        ];
        self.format.delete_schema(schema_name).await?;
        self.format.delete_states(state_keys).await?;
        self.flush().await?;
        Ok(existed)
    }
}
