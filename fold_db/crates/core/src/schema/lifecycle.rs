//! Schema lifecycle operations: loading, resolution, RCU publication, and drop cleanup.

use super::{CachedSchema, Schema, SchemaCore, SchemaError, SchemaState};
use crate::db_operations::schema_store::SchemaDrop;
use crate::storage::disk_pack::transaction::{self, publication};
use crate::storage::format::metadata::SchemaReplayDrop;
use crate::storage::StorageError;
use ::schema_types::IdentityRecompute;
use std::sync::Arc;

impl SchemaCore {
    pub async fn drop_schema(&self, schema_name: &str) -> Result<bool, SchemaError> {
        self.drop_with_receipt(schema_name, None).await
    }

    /// Apply a signed schema removal once, using bounded native cleanup transactions.
    pub async fn drop_replay_schema(
        &self,
        schema_name: &str,
        receipt_id: &[u8; 32],
        digest: &[u8; 32],
    ) -> Result<(), SchemaError> {
        let runtime = self.db_ops.native_runtime();
        runtime
            .replay_accepted(self.drop_with_receipt(
                schema_name,
                Some(&SchemaReplayDrop {
                    id: receipt_id,
                    digest,
                }),
            ))
            .await?;
        Ok(())
    }

    async fn drop_with_receipt(
        &self,
        schema_name: &str,
        replay: Option<&SchemaReplayDrop<'_>>,
    ) -> Result<bool, SchemaError> {
        let _drop = self.drops.lock().await;
        let SchemaDrop::Ready(schema) = self
            .db_ops
            .schemas()
            .block_for_drop(schema_name, replay, |s| {
                self.publish_catalog(schema_name.to_owned(), s.cloned(), SchemaState::Blocked)
            })
            .await?
        else {
            return Ok(false);
        };
        if let Some(schema) = schema {
            let r = self.db_ops.records().release_schema_rows(&schema).await?;
            tracing::info!(
                target: "fold_node::database",
                schema = schema_name, records = r.records, alternates = r.alternates,
                "schema drop released the stored rows of the schema"
            );
        }
        self.db_ops
            .schemas()
            .drop_reserved_schema(schema_name, replay, || {
                self.publish_catalog(schema_name.to_owned(), None, SchemaState::Available)
            })
            .await
    }

    pub async fn resolve_schema(&self, schema_name: &str) -> Result<Option<Schema>, SchemaError> {
        if let Some(s) = self.get_schema_metadata(schema_name)? {
            return Ok(Some(s));
        }
        if self
            .db_ops
            .schemas()
            .get_schema(schema_name)
            .await?
            .is_none()
        {
            return Ok(None);
        }
        transaction::scope(self.db_ops.native_runtime(), async {
            let Some(schema) = self.db_ops.schemas().get_schema(schema_name).await? else {
                return Ok(None);
            };
            let state = self
                .db_ops
                .schemas()
                .get_schema_state(schema_name)
                .await?
                .unwrap_or_default();
            require_explicit_record_keys(&schema)?;
            self.publish_catalog(schema_name.to_owned(), Some(schema.clone()), state)?;
            Ok(Some(schema))
        })
        .await
    }

    pub async fn load_schema_internal(&self, mut schema: Schema) -> Result<(), SchemaError> {
        require_explicit_record_keys(&schema)?;
        schema.validate()?;
        schema.ensure_field_hashes();
        transaction::scope(self.db_ops.native_runtime(), self.save_schema(schema)).await
    }

    async fn save_schema(&self, mut schema: Schema) -> Result<(), SchemaError> {
        let existing = self.db_ops.schemas().get_schema(&schema.name).await?;
        stamp_identity_for_load(&mut schema, existing.as_ref());
        let refresh = existing.is_some();
        if let Some(stored) = existing {
            if schema.record_keys != stored.record_keys
                || schema.alternate_keys != stored.alternate_keys
            {
                return Err(SchemaError::InvalidData(
                    "explicit primary/alternate key declarations are immutable; declare a new schema and copy explicitly".into(),
                ));
            }
            schema.validate()?;
        }
        let name = schema.name.clone();
        self.db_ops.schemas().store_schema(&name, &schema).await?;
        let state = if refresh {
            match self.db_ops.schemas().get_schema_state(&name).await? {
                Some(st) => st,
                None => self.get_schema_and_state(&name)?.1,
            }
        } else {
            self.db_ops
                .schemas()
                .store_schema_state(&name, &SchemaState::Available)
                .await?;
            SchemaState::Available
        };
        self.publish_catalog(name, Some(schema), state)
    }

    fn publish_catalog(
        &self,
        key: String,
        schema: Option<Schema>,
        state: SchemaState,
    ) -> Result<(), SchemaError> {
        let catalog = Arc::clone(&self.catalog);
        let staged = CachedSchema { schema, state };
        publication::publish(
            &self.db_ops.native_runtime(),
            self.catalog_id(),
            key.clone(),
            staged.clone(),
            move || {
                let mut g = catalog.write().map_err(|e| {
                    StorageError::BackendError(format!("schema catalog lock poisoned: {e}"))
                })?;
                staged.update(Arc::make_mut(&mut *g), key);
                Ok(())
            },
        )?;
        Ok(())
    }
}

pub(super) fn require_explicit_record_keys(schema: &Schema) -> Result<(), SchemaError> {
    if schema.record_keys.is_none() {
        return Err(SchemaError::InvalidData(
            "this home requires explicit record_keys; field-derived keys are unsupported".into(),
        ));
    }
    schema
        .validate_owned_schema()
        .map_err(SchemaError::InvalidData)
}

fn stamp_identity_for_load(schema: &mut Schema, existing: Option<&Schema>) {
    if let IdentityRecompute::RefusedDowngrade { stored, binary } =
        schema.recompute_identity_hash_unless_newer()
    {
        tracing::warn!(
            schema = %schema.name, stored_algo_version = stored, binary_algo_version = binary,
            identity_hash = %schema.identity_hash.as_deref().unwrap_or("<none>"),
            "refusing to downgrade schema identity minted by a newer algorithm",
        );
    }
    if let Some((s, stored)) = existing.and_then(|s| {
        ::schema_types::refuses_identity_downgrade(
            s.identity_hash.as_deref(),
            s.identity_hash_algo_version,
        )
        .map(|v| (s, v))
    }) {
        schema.identity_hash.clone_from(&s.identity_hash);
        schema.identity_hash_algo_version = s.identity_hash_algo_version;
        tracing::warn!(
            schema = %schema.name, stored_algo_version = stored,
            binary_algo_version = ::schema_types::IDENTITY_HASH_ALGO_VERSION,
            "refusing to overwrite a stored schema identity minted by a newer algorithm",
        );
    }
}
