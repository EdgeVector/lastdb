//! Typed schema writes and their complete native publication scope.

use super::*;
use futures::FutureExt;

impl SchemaStore {
    /// Store a schema.
    ///
    /// **Skip-if-unchanged:** when the durable catalog body already matches
    /// `schema` (JSON-equality of the serialized `Schema`, which skips
    /// runtime-only fields), this is a no-op. Product mutations used to call
    /// this on every write; without the skip, LastStore append-only segments
    /// retained every full catalog rewrite and the `schemas` plane ballooned
    /// (~354× historical copies measured on a daily-driver home). An existing
    /// body that does not decode fails the write.
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
        schema
            .validate_definition()
            .map_err(SchemaError::InvalidData)?;
        self.require_schema_key_format(schema)?;
        if let Some(existing) = self.get_schema(schema_name).await? {
            if durable_schema_eq(&existing, schema) {
                self.stage_native_snapshot(schema_name).await?;
                return Ok(());
            }
        }
        self.schemas_store.put_item(schema_name, schema).await?;
        self.stage_native_snapshot(schema_name).await?;
        self.schemas_store.inner().flush().await?;
        Ok(())
    }

    /// Store schema state.
    ///
    /// Same skip-if-unchanged contract as [`Self::store_schema`].
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
            self.stage_native_snapshot(schema_name).await?;
            return Ok(());
        }
        self.schema_states_store
            .put_item(schema_name, state)
            .await?;
        self.stage_native_snapshot(schema_name).await?;
        self.schema_states_store.inner().flush().await?;
        Ok(())
    }

    /// Remove one installed schema identity from the catalog.
    ///
    /// Point Deletes of the `schemas` row, the `schema_states` row, the
    /// name-claim row, and the retention keys for this identity. The caller
    /// releases the product records first. Returns whether a
    /// catalog body was present before the Deletes. A miss still runs the
    /// Deletes so a retry is idempotent.
    pub async fn drop_schema(&self, schema_name: &str) -> Result<bool, SchemaError> {
        crate::storage::disk_pack::transaction::scope(
            Arc::clone(&self.runtime),
            self.drop_schema_native(schema_name).boxed(),
        )
        .await
    }

    async fn drop_schema_native(&self, schema_name: &str) -> Result<bool, SchemaError> {
        let existed = self.get_schema(schema_name).await?.is_some();
        let mut state_keys = vec![
            schema_name.to_string(),
            name_claim_key(schema_name),
            retention_policy_key(schema_name),
            retention_anchor_key(schema_name),
        ];
        state_keys.extend(
            self.schema_states_store
                .list_keys_with_prefix(&retention_age_prefix(schema_name))
                .await?,
        );
        state_keys.extend(
            self.schema_states_store
                .list_keys_with_prefix(&retention_age_latest_prefix(schema_name))
                .await?,
        );
        let _ = self.schemas_store.delete_item(schema_name).await?;
        let _ = self.superseded_by_store.delete_item(schema_name).await?;
        self.schema_states_store
            .batch_delete_keys(state_keys)
            .await?;
        self.stage_native_snapshot(schema_name).await?;
        self.flush().await?;
        Ok(existed)
    }
}
