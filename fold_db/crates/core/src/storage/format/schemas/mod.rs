//! Schema/state format capability. Providers receive read-only namespace views.

mod delete;
mod input;
pub(crate) mod keys;
mod state;

use super::read::ReadStore;
use crate::storage::disk_pack::{transaction, PackRuntime};
use crate::storage::error::StorageResult;
use crate::storage::{KvStore, StorageError, TypedKvStore};
use serde::Serialize;
use std::sync::Arc;

pub(crate) use delete::{StateDelete, StateKey, Tracking};
pub(crate) use input::{schema_input, SchemaInput};
pub(crate) use state::StateChange;

type Row = (Vec<u8>, Vec<u8>);

enum EncodedSchema {
    SchemaPut(Row),
    SchemaDelete(Vec<u8>),
    StatePut(Row),
    StateBatch(Vec<Row>),
    StateOneDelete(Vec<u8>),
    StateDelete(Vec<Vec<u8>>),
}

#[derive(Clone)]
pub(crate) struct SchemaFormat {
    schemas_store: Arc<TypedKvStore<dyn KvStore>>,
    schema_states_store: Arc<TypedKvStore<dyn KvStore>>,
    runtime: Arc<PackRuntime>,
}

impl SchemaFormat {
    pub(in crate::storage::format) fn new(
        schemas: Arc<dyn KvStore>,
        states: Arc<dyn KvStore>,
        runtime: Arc<PackRuntime>,
    ) -> Self {
        Self {
            schemas_store: Arc::new(TypedKvStore::new(schemas)),
            schema_states_store: Arc::new(TypedKvStore::new(states)),
            runtime,
        }
    }

    pub(crate) fn readers(&self) -> (ReadStore, ReadStore) {
        (
            ReadStore::new(Arc::clone(&self.schemas_store)),
            ReadStore::new(Arc::clone(&self.schema_states_store)),
        )
    }

    pub(crate) fn runtime(&self) -> Arc<PackRuntime> {
        Arc::clone(&self.runtime)
    }

    pub(crate) async fn put_schema(&self, input: SchemaInput<'_>) -> StorageResult<()> {
        self.write(EncodedSchema::SchemaPut(row(
            input.schema.name.clone(),
            input.schema,
        )?))
        .await
    }

    pub(crate) async fn delete_schema(&self, name: &str) -> StorageResult<()> {
        require_name(name)?;
        self.write(EncodedSchema::SchemaDelete(name.as_bytes().to_vec()))
            .await
    }

    pub(crate) async fn put_state(&self, change: StateChange<'_>) -> StorageResult<()> {
        self.write(EncodedSchema::StatePut(state::encode(change)?))
            .await
    }

    pub(crate) async fn batch_states(&self, changes: Vec<StateChange<'_>>) -> StorageResult<()> {
        let rows = changes
            .into_iter()
            .map(state::encode)
            .collect::<StorageResult<Vec<_>>>()?;
        self.write(EncodedSchema::StateBatch(rows)).await
    }

    pub(crate) async fn delete_states(&self, changes: Vec<StateDelete<'_>>) -> StorageResult<()> {
        self.write(EncodedSchema::StateDelete(delete::encode(changes)?))
            .await
    }

    pub(crate) async fn delete_state(&self, schema: &str, kind: StateKey) -> StorageResult<()> {
        self.write(EncodedSchema::StateOneDelete(delete::key(schema, kind)?))
            .await
    }

    pub(crate) async fn flush_schemas(&self) -> StorageResult<()> {
        self.schemas_store.inner().flush().await
    }
    pub(crate) async fn flush_states(&self) -> StorageResult<()> {
        self.schema_states_store.inner().flush().await
    }

    /// Preserve each old standalone KvStore call's request boundary; callers
    /// already inside a complete native request join it instead.
    async fn write(&self, encoded: EncodedSchema) -> StorageResult<()> {
        transaction::scope(Arc::clone(&self.runtime), self.stage(encoded)).await
    }

    async fn stage(&self, encoded: EncodedSchema) -> StorageResult<()> {
        if !transaction::publication::active(&self.runtime) {
            return Err(StorageError::InvalidOperation(
                "schema write requires its native scope".into(),
            ));
        }
        match encoded {
            EncodedSchema::SchemaPut((key, bytes)) => {
                self.schemas_store.inner().put(&key, bytes).await
            }
            EncodedSchema::SchemaDelete(key) => {
                self.schemas_store.inner().delete(&key).await.map(|_| ())
            }
            EncodedSchema::StatePut((key, bytes)) => {
                self.schema_states_store.inner().put(&key, bytes).await
            }
            EncodedSchema::StateBatch(rows) => {
                self.schema_states_store.inner().batch_put(rows).await
            }
            EncodedSchema::StateOneDelete(key) => self
                .schema_states_store
                .inner()
                .delete(&key)
                .await
                .map(|_| ()),
            EncodedSchema::StateDelete(keys) => {
                self.schema_states_store.inner().batch_delete(keys).await
            }
        }
    }
}

fn row(key: String, value: &impl Serialize) -> StorageResult<Row> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| StorageError::SerializationError(error.to_string()))?;
    Ok((key.into_bytes(), bytes))
}

fn require_name(name: &str) -> StorageResult<()> {
    if name.is_empty() || name.contains('\0') {
        return Err(StorageError::InvalidOperation(
            "schema keys require a nonempty name without NUL".into(),
        ));
    }
    Ok(())
}
