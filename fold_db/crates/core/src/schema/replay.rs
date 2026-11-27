//! Schema drop lifecycle, interrupted-drop recovery, and replay compatibility checks.

use super::{require_explicit_record_keys, Schema, SchemaCore, SchemaError, SchemaState};
use crate::db_operations::schema_store::{is_compatible_schema_update, SchemaDrop};
use crate::storage::format::metadata::{ReplayStamp, SchemaReplayDrop};

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
        stamp: &ReplayStamp,
    ) -> Result<(), SchemaError> {
        let drop_op = SchemaReplayDrop {
            id: receipt_id,
            digest,
            stamp,
        };
        let runtime = self.db_ops.native_runtime();
        runtime
            .replay_accepted(self.drop_with_receipt(schema_name, Some(&drop_op)))
            .await
            .map(|_| ())
    }

    /// Wait for any in-flight drop of `schema_name`, and finish a crash-interrupted drop
    /// so replay never stalls on `SchemaState::Blocked`.
    pub async fn ensure_unblocked(&self, schema_name: &str) -> Result<(), SchemaError> {
        if !self.is_schema_blocked(schema_name)? {
            return Ok(());
        }
        let _drop = self.drops.lock().await;
        self.recover_interrupted_drop(schema_name, None).await
    }

    pub async fn replay_receipt_exists(
        &self,
        receipt_id: &[u8; 32],
        digest: &[u8; 32],
    ) -> Result<bool, SchemaError> {
        self.db_ops
            .schemas()
            .replay_receipt_exists(receipt_id, digest)
            .await
    }

    pub fn prepare_replay_schema(schema: &mut Schema) -> Result<(), SchemaError> {
        require_explicit_record_keys(schema)?;
        schema.validate()?;
        schema.ensure_field_hashes();
        Ok(())
    }

    pub fn is_compatible_update(existing: &Schema, next: &Schema) -> Result<bool, SchemaError> {
        let mut existing = existing.clone();
        let mut next = next.clone();
        existing.ensure_field_hashes();
        next.ensure_field_hashes();
        is_compatible_schema_update(&existing, &next)
    }

    async fn drop_with_receipt(
        &self,
        schema_name: &str,
        replay: Option<&SchemaReplayDrop<'_>>,
    ) -> Result<bool, SchemaError> {
        let _drop = self.drops.lock().await;
        self.recover_interrupted_drop(schema_name, replay).await?;
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
                schema = schema_name,
                records = r.records,
                alternates = r.alternates,
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

    async fn recover_interrupted_drop(
        &self,
        schema_name: &str,
        current: Option<&SchemaReplayDrop<'_>>,
    ) -> Result<(), SchemaError> {
        let schemas = self.db_ops.schemas();
        let blocked = schemas.get_schema_state(schema_name).await? == Some(SchemaState::Blocked);
        let pending = schemas.pending_drop_intent(schema_name).await?;
        if !blocked && pending.is_none() {
            return Ok(());
        }
        if let (Some(op), Some((id, digest))) = (current, pending) {
            if (&id, &digest) == (op.id, op.digest) {
                return Ok(());
            }
        }
        let complete = async {
            if let Some(schema) = schemas.get_schema(schema_name).await? {
                let r = self.db_ops.records().release_schema_rows(&schema).await?;
                tracing::info!(
                    target: "fold_node::database",
                    schema = schema_name,
                    records = r.records,
                    alternates = r.alternates,
                    "recovered interrupted schema drop"
                );
            }
            schemas
                .finish_interrupted_drop(schema_name, pending, || {
                    self.publish_catalog(schema_name.to_owned(), None, SchemaState::Available)
                })
                .await
        };
        if pending.is_some() {
            self.db_ops.native_runtime().replay_accepted(complete).await
        } else {
            complete.await
        }
    }
}
