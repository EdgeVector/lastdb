//! Reserve a schema for paged removal and finish with a durable replay receipt.

use super::SchemaStore;
use crate::schema::types::schema_types::SchemaState;
use crate::schema::types::{Schema, SchemaError};
use crate::storage::disk_backup::accepted;
use crate::storage::disk_pack::transaction;
use crate::storage::format::metadata::{ReplayTarget, SchemaReplayDrop};
use crate::storage::format::schemas::{StateDelete, StateKey};
use std::sync::Arc;

pub(crate) enum SchemaDrop {
    Done,
    Ready(Option<Box<Schema>>),
}

impl SchemaStore {
    pub(crate) async fn block_for_drop(
        &self,
        name: &str,
        replay: Option<&SchemaReplayDrop<'_>>,
        publish: impl FnOnce(Option<&Schema>) -> Result<(), SchemaError>,
    ) -> Result<SchemaDrop, SchemaError> {
        transaction::scope(Arc::clone(&self.runtime), async {
            let state = self.get_schema_state(name).await?;
            let schema = self.get_schema(name).await?;
            if let Some(replay) = replay {
                if schema.is_none() && state.is_none() && self.pending_local_drop(name).await? {
                    self.metadata
                        .stage_replay_receipt(replay.id, replay.digest)
                        .await?;
                    return Ok(SchemaDrop::Done);
                }
            }
            if !self.metadata.guard_schema_drop(name, replay, true).await? {
                return Ok(SchemaDrop::Done);
            }
            if state != Some(SchemaState::Blocked) {
                self.store_schema_state(name, &SchemaState::Blocked).await?;
            }
            publish(schema.as_ref())?;
            Ok(SchemaDrop::Ready(schema.map(Box::new)))
        })
        .await
    }

    pub(crate) async fn pending_local_drop(&self, name: &str) -> Result<bool, SchemaError> {
        let Some(pending) = self
            .metadata
            .pending_replay(ReplayTarget::Schema(name))
            .await?
        else {
            return Ok(false);
        };
        Ok(pending == accepted::schema_drop_id(name))
    }

    pub(super) async fn finish_drop(
        &self,
        name: &str,
        replay: Option<&SchemaReplayDrop<'_>>,
        publish: impl FnOnce() -> Result<(), SchemaError>,
    ) -> Result<bool, SchemaError> {
        transaction::scope(Arc::clone(&self.runtime), async {
            if !self.metadata.guard_schema_drop(name, replay, false).await? {
                return Ok(false);
            }
            let existed = self.get_schema(name).await?.is_some();
            self.format.delete_schema(name).await?;
            self.format
                .delete_states(vec![
                    StateDelete::Key {
                        schema: name,
                        kind: StateKey::State,
                    },
                    StateDelete::Key {
                        schema: name,
                        kind: StateKey::Policy,
                    },
                ])
                .await?;
            if let Some(replay) = replay {
                self.metadata.finish_schema_drop(name, replay).await?;
            } else if existed {
                let mutation_id =
                    accepted::stage_schema_drop(&self.runtime, &self.content_key, name)?;
                self.note_local_replay(name, mutation_id).await?;
            }
            publish()?;
            Ok(existed)
        })
        .await
    }
}
