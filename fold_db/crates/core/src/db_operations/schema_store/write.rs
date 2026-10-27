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
    /// (~354× historical copies measured on a daily-driver home).
    ///
    /// **Won't-undo:** the skip read must never be able to fail the write. An
    /// undeserializable durable body (empty row, legacy `ENC:` value read
    /// through a plain seam, truncated segment) used to propagate out of the
    /// `get_item` and abort `store_schema` entirely, so the one write that
    /// would have replaced the bad row was the one write that could not run.
    /// On Tom's primary that latched two kanban schemas into permanent
    /// `Serialization error: expected value at line 1 column 1` failures,
    /// which blinded the BoardCards projection and stalled the whole factory.
    /// The skip is an optimization; treat an unreadable existing body as
    /// "not equal" and fall through to the put that heals it.
    pub async fn store_schema(
        &self,
        schema_name: &str,
        schema: &Schema,
    ) -> Result<(), SchemaError> {
        crate::storage::disk_pack::transaction::scope(
            self.runtime.clone(),
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
        match self.schemas_store.get_item::<Schema>(schema_name).await {
            Ok(Some(existing)) if durable_schema_eq(&existing, schema) => {
                self.stage_native_snapshot(schema_name).await?;
                return Ok(());
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    schema = %schema_name,
                    "durable schema body unreadable; overwriting it instead of \
                     failing the write (skip-if-unchanged is an optimization)"
                );
            }
        }
        self.schemas_store.put_item(schema_name, schema).await?;
        self.stage_native_snapshot(schema_name).await?;
        self.schemas_store.inner().flush().await?;
        Ok(())
    }

    /// Store schema state.
    ///
    /// Same skip-if-unchanged contract as [`Self::store_schema`], including the
    /// won't-undo: an unreadable durable body must not veto the write that
    /// replaces it.
    pub async fn store_schema_state(
        &self,
        schema_name: &str,
        state: &SchemaState,
    ) -> Result<(), SchemaError> {
        crate::storage::disk_pack::transaction::scope(
            self.runtime.clone(),
            self.store_schema_state_native(schema_name, state).boxed(),
        )
        .await
    }

    async fn store_schema_state_native(
        &self,
        schema_name: &str,
        state: &SchemaState,
    ) -> Result<(), SchemaError> {
        match self
            .schema_states_store
            .get_item::<SchemaState>(schema_name)
            .await
        {
            Ok(Some(existing)) if existing == *state => {
                self.stage_native_snapshot(schema_name).await?;
                return Ok(());
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    schema = %schema_name,
                    "durable schema state unreadable; overwriting it instead of \
                     failing the write (skip-if-unchanged is an optimization)"
                );
            }
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
    /// name-claim row, and the retention keys for this identity. Product
    /// tips and atoms are left for a later janitor. Returns whether a
    /// catalog body was present before the Deletes. A miss still runs the
    /// Deletes so a retry is idempotent.
    pub async fn drop_schema(&self, schema_name: &str) -> Result<bool, SchemaError> {
        crate::storage::disk_pack::transaction::scope(
            self.runtime.clone(),
            self.drop_schema_native(schema_name).boxed(),
        )
        .await
    }

    async fn drop_schema_native(&self, schema_name: &str) -> Result<bool, SchemaError> {
        let existing = self.get_schema(schema_name).await?;
        let existed = existing.is_some();
        if let Some(mut schema) = existing {
            let mut field_molecule_uuids: Vec<String> = schema
                .field_molecule_uuids
                .take()
                .unwrap_or_default()
                .into_values()
                .collect();
            field_molecule_uuids.sort();
            field_molecule_uuids.dedup();
            let receipt = SchemaDropReceipt {
                identity: schema_name.to_string(),
                owner_app: schema.owner_app_id.clone(),
                field_molecule_uuids,
                dropped_at_unix_ms: crate::clock::unix_millis(),
            };
            self.schema_states_store
                .put_item(&schema_drop_receipt_key(schema_name), &receipt)
                .await?;
        }
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

    /// Point-get the janitor receipt for one dropped identity.
    pub async fn get_schema_drop_receipt(
        &self,
        schema_name: &str,
    ) -> Result<Option<SchemaDropReceipt>, SchemaError> {
        Ok(self
            .schema_states_store
            .get_item(&schema_drop_receipt_key(schema_name))
            .await?)
    }

    /// Store a schema superseded-by mapping (old_name → new_name)
    pub async fn store_superseded_by(
        &self,
        old_name: &str,
        new_name: &str,
    ) -> Result<(), SchemaError> {
        crate::storage::disk_pack::transaction::scope(
            self.runtime.clone(),
            self.store_superseded_by_native(old_name, new_name).boxed(),
        )
        .await
    }

    async fn store_superseded_by_native(
        &self,
        old_name: &str,
        new_name: &str,
    ) -> Result<(), SchemaError> {
        self.superseded_by_store
            .put_item(old_name, &new_name.to_string())
            .await?;
        self.stage_native_snapshot(old_name).await?;
        self.superseded_by_store.inner().flush().await?;
        Ok(())
    }
}
