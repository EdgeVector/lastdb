//! Schema/state format capability. Providers receive read-only namespace views.

pub(crate) mod keys;

use std::sync::Arc;

use serde::Serialize;

use super::read::ReadStore;
use crate::schema::{Schema, SchemaError, SchemaRetentionPolicy, SchemaState};
use crate::storage::disk_pack::{transaction, PackKvStore, PackRuntime};
use crate::storage::error::StorageResult;
use crate::storage::StorageError;
use keys::{
    retention_age_key, retention_age_latest_key, retention_hash_partition_key,
    retention_policy_key, SchemaRetentionAgeEntry, SchemaRetentionHashPartition,
};
pub(crate) use keys::{StateDelete, StateKey, Tracking};

type Row = (Vec<u8>, Vec<u8>);

pub(crate) struct SchemaInput<'a> {
    schema: &'a Schema,
}

pub(crate) fn schema_input<'a>(
    name: &str,
    schema: &'a Schema,
) -> Result<SchemaInput<'a>, SchemaError> {
    require_name(name)?;
    if schema.name != name {
        return Err(SchemaError::InvalidData(
            "schema catalog key differs from its payload name".into(),
        ));
    }
    schema
        .validate_definition()
        .map_err(SchemaError::InvalidData)?;
    schema
        .validate_key_field_types()
        .map_err(SchemaError::InvalidData)?;
    schema
        .require_record_keys()?
        .validate()
        .map_err(SchemaError::InvalidData)?;
    if schema.file_required {
        let mut canonical = schema.to_wire();
        canonical.compute_identity_hash();
        if schema.identity_hash != canonical.identity_hash
            || schema.identity_hash_algo_version != canonical.identity_hash_algo_version
        {
            return Err(SchemaError::InvalidData(
                "required-file schema needs its exact catalog identity".into(),
            ));
        }
    }
    Ok(SchemaInput { schema })
}

pub(crate) enum StateChange<'a> {
    State {
        schema: &'a str,
        state: &'a SchemaState,
    },
    Policy {
        schema: &'a str,
        policy: &'a SchemaRetentionPolicy,
    },
    Age {
        schema: &'a str,
        entry: SchemaRetentionAgeEntry,
        latest: bool,
    },
    Partition {
        schema: &'a str,
        entry: SchemaRetentionHashPartition,
    },
}

fn tracking_owner(change: &StateChange<'_>) -> Option<String> {
    match change {
        StateChange::Age { schema, .. } | StateChange::Partition { schema, .. } => {
            Some((*schema).to_owned())
        }
        _ => None,
    }
}

fn encode_state(change: StateChange<'_>) -> StorageResult<Row> {
    let schema = match &change {
        StateChange::State { schema, .. }
        | StateChange::Policy { schema, .. }
        | StateChange::Age { schema, .. }
        | StateChange::Partition { schema, .. } => *schema,
    };
    require_name(schema)?;
    match change {
        StateChange::State { state, .. } => row(schema.to_owned(), state),
        StateChange::Policy { policy, .. } => {
            if policy.ttl_seconds == 0 || policy.hash_partitions.iter().any(String::is_empty) {
                return Err(StorageError::InvalidOperation(
                    "invalid schema retention policy".into(),
                ));
            }
            row(retention_policy_key(schema), policy)
        }
        StateChange::Age { entry, latest, .. } => {
            let key = if latest {
                retention_age_latest_key(schema, &entry.key)
            } else {
                retention_age_key(schema, entry.written_at, &entry.key)
            };
            row(key, &entry)
        }
        StateChange::Partition { entry, .. } => {
            row(retention_hash_partition_key(schema, &entry.hash), &entry)
        }
    }
}

enum EncodedSchema {
    SchemaPut(Row),
    SchemaDelete(Vec<u8>),
    StatePut(Row, Option<String>),
    StateBatch(Vec<Row>, Vec<String>),
    StateDelete(Vec<Vec<u8>>),
}

#[derive(Clone)]
pub(crate) struct SchemaFormat {
    schemas_store: PackKvStore,
    schema_states_store: PackKvStore,
    runtime: Arc<PackRuntime>,
}

impl SchemaFormat {
    pub(in crate::storage::format) fn new(
        schemas_store: PackKvStore,
        schema_states_store: PackKvStore,
        runtime: Arc<PackRuntime>,
    ) -> Self {
        Self {
            schemas_store,
            schema_states_store,
            runtime,
        }
    }

    pub(crate) fn readers(&self) -> (ReadStore, ReadStore) {
        (
            ReadStore::new(self.schemas_store.clone()),
            ReadStore::new(self.schema_states_store.clone()),
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
        let owner = tracking_owner(&change);
        self.write(EncodedSchema::StatePut(encode_state(change)?, owner))
            .await
    }

    pub(crate) async fn batch_states(&self, changes: Vec<StateChange<'_>>) -> StorageResult<()> {
        let mut owners: Vec<_> = changes.iter().filter_map(tracking_owner).collect();
        owners.sort();
        owners.dedup();
        let rows = changes
            .into_iter()
            .map(encode_state)
            .collect::<StorageResult<Vec<_>>>()?;
        self.write(EncodedSchema::StateBatch(rows, owners)).await
    }

    pub(crate) async fn delete_states(&self, changes: Vec<StateDelete<'_>>) -> StorageResult<()> {
        self.write(EncodedSchema::StateDelete(keys::encode_deletes(changes)?))
            .await
    }

    pub(crate) async fn flush_schemas(&self) -> StorageResult<()> {
        self.schemas_store.flush().await
    }
    pub(crate) async fn flush_states(&self) -> StorageResult<()> {
        self.schema_states_store.flush().await
    }

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
            EncodedSchema::SchemaPut((key, bytes)) => self.schemas_store.put(&key, bytes).await,
            EncodedSchema::SchemaDelete(key) => self.schemas_store.delete(&key).await.map(|_| ()),
            EncodedSchema::StatePut((key, bytes), owner) => {
                self.require_tracking_writable(&owner.into_iter().collect::<Vec<_>>())
                    .await?;
                self.schema_states_store.put(&key, bytes).await
            }
            EncodedSchema::StateBatch(rows, owners) => {
                self.require_tracking_writable(&owners).await?;
                self.schema_states_store.batch_put(rows).await
            }
            EncodedSchema::StateDelete(keys) => self.schema_states_store.batch_delete(keys).await,
        }
    }

    async fn require_tracking_writable(&self, schemas: &[String]) -> StorageResult<()> {
        if schemas.is_empty() {
            return Ok(());
        }
        let keys = schemas
            .iter()
            .map(|name| name.as_bytes().to_vec())
            .collect();
        let states_reader = ReadStore::new(self.schema_states_store.clone());
        let (states, definitions) = tokio::try_join!(
            states_reader.get_items::<SchemaState>(schemas),
            self.schemas_store.get_many(keys),
        )?;
        if states.len() != schemas.len() || definitions.len() != schemas.len() {
            return Err(StorageError::BackendError(
                "tracking owner read is incomplete".into(),
            ));
        }
        for ((schema, state), definition) in schemas.iter().zip(states).zip(definitions) {
            if definition.is_none() || state == Some(SchemaState::Blocked) {
                return Err(StorageError::InvalidOperation(format!(
                    "expiry tracking owner {schema} is blocked or not installed"
                )));
            }
        }
        Ok(())
    }
}

fn row(key: String, value: &impl Serialize) -> StorageResult<Row> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| StorageError::SerializationError(error.to_string()))?;
    Ok((key.into_bytes(), bytes))
}

pub(super) fn require_name(name: &str) -> StorageResult<()> {
    if name.is_empty() || name.contains('\0') {
        return Err(StorageError::InvalidOperation(
            "schema keys require a nonempty name without NUL".into(),
        ));
    }
    Ok(())
}
