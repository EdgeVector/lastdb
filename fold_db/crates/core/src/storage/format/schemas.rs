//! Schema/state format capability and catalog key framing.

use std::sync::Arc;

use lastdb_model::hex::sha256_hex as fp;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::read::ReadStore;
use crate::schema::types::KeyValue;
use crate::schema::{Schema, SchemaError, SchemaRetentionPolicy, SchemaState};
use crate::storage::disk_pack::{transaction, PackKvStore, PackRuntime};
use crate::storage::error::StorageResult;
pub(crate) use crate::storage::format::{
    RETENTION_AGE_KEY_PREFIX, RETENTION_AGE_LATEST_PREFIX, RETENTION_HASH_PARTITION_KEY_PREFIX,
    RETENTION_POLICY_KEY_PREFIX,
};
use crate::storage::{KvMutation, StorageError};

pub(crate) const CATALOG_PAGE_ROWS: usize = 256;
pub(crate) const CATALOG_SCAN_END: &[u8] = &[0xff];
pub(crate) const CATALOG_STATE_SCAN_START: &[u8] = &[0x01];

pub(crate) mod keys {
    pub(crate) use super::*;
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(crate) struct SchemaRetentionAgeEntry {
    pub(crate) key: KeyValue,
    pub(crate) written_at: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(crate) struct SchemaRetentionHashPartition {
    pub(crate) hash: String,
}

#[derive(Clone, Copy)]
pub(crate) enum StateKey {
    State,
    Policy,
}

#[derive(Clone, Copy)]
pub(crate) enum Tracking {
    Age,
    Latest,
    Partition,
}

pub(crate) enum StateDelete<'a> {
    Key {
        schema: &'a str,
        kind: StateKey,
    },
    Selected {
        schema: &'a str,
        kind: Tracking,
        keys: Vec<String>,
    },
}

type Staged = (String, Option<Vec<u8>>);

pub(crate) fn retention_policy_key(schema: &str) -> String {
    format!("{RETENTION_POLICY_KEY_PREFIX}{schema}")
}

pub(crate) fn retention_age_prefix(schema: &str) -> String {
    format!("{RETENTION_AGE_KEY_PREFIX}{}\0", fp(schema))
}

pub(crate) fn retention_age_latest_prefix(schema: &str) -> String {
    format!("{RETENTION_AGE_LATEST_PREFIX}{}\0", fp(schema))
}

pub(crate) fn retention_age_key(schema: &str, written_at: u64, key: &KeyValue) -> String {
    format!(
        "{}{written_at:020}\0{}",
        retention_age_prefix(schema),
        fp(key.to_storage_key())
    )
}

pub(crate) fn retention_age_latest_key(schema: &str, key: &KeyValue) -> String {
    format!(
        "{}{}",
        retention_age_latest_prefix(schema),
        fp(key.to_storage_key())
    )
}

pub(crate) fn retention_hash_partition_prefix(schema: &str) -> String {
    format!("{RETENTION_HASH_PARTITION_KEY_PREFIX}{}\0", fp(schema))
}

pub(crate) fn retention_hash_partition_key(schema: &str, hash: &str) -> String {
    format!("{}{}", retention_hash_partition_prefix(schema), fp(hash))
}

pub(crate) fn durable_schema_eq(a: &Schema, b: &Schema) -> bool {
    a == b
}

pub(crate) fn decode_catalog_json<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    serde_json::from_slice::<T>(bytes).map_err(|error| error.to_string())
}

pub(crate) fn decode_catalog_schema(bytes: &[u8]) -> Result<Schema, String> {
    decode_catalog_json(bytes)
}

fn encode_deletes(changes: Vec<StateDelete<'_>>) -> StorageResult<Vec<Staged>> {
    let bad = || {
        StorageError::InvalidOperation("retention delete key differs from its owner/family".into())
    };
    let mut output = Vec::new();
    for change in changes {
        match change {
            StateDelete::Key { schema, kind } => {
                require_name(schema)?;
                let raw = match kind {
                    StateKey::State => schema.to_owned(),
                    StateKey::Policy => retention_policy_key(schema),
                };
                output.push((raw, None));
            }
            StateDelete::Selected { schema, kind, keys } => {
                require_name(schema)?;
                let prefix = match kind {
                    Tracking::Age => retention_age_prefix(schema),
                    Tracking::Latest => retention_age_latest_prefix(schema),
                    Tracking::Partition => retention_hash_partition_prefix(schema),
                };
                for key in keys {
                    let suffix = key.strip_prefix(&prefix).ok_or_else(bad)?;
                    let digest = if matches!(kind, Tracking::Age) {
                        let (c, d) = suffix.split_once('\0').ok_or_else(bad)?;
                        if c.len() != 20
                            || !c.bytes().all(|b| b.is_ascii_digit())
                            || c.parse::<u64>().is_err()
                        {
                            return Err(bad());
                        }
                        d
                    } else {
                        suffix
                    };
                    if digest.len() != 64 || !digest.bytes().all(|b| b.is_ascii_hexdigit()) {
                        return Err(bad());
                    }
                    output.push((key, None));
                }
            }
        }
    }
    Ok(output)
}

pub(crate) struct SchemaInput<'a>(&'a Schema);

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
    let keys = schema.require_record_keys()?;
    schema
        .validate_definition()
        .and_then(|()| schema.validate_key_field_types())
        .and_then(|()| keys.validate())
        .map_err(SchemaError::InvalidData)?;
    if schema.file_required {
        let mut canonical = schema.to_wire();
        canonical.compute_identity_hash();
        if (&schema.identity_hash, schema.identity_hash_algo_version)
            != (
                &canonical.identity_hash,
                canonical.identity_hash_algo_version,
            )
        {
            return Err(SchemaError::InvalidData(
                "required-file schema needs its exact catalog identity".into(),
            ));
        }
    }
    Ok(SchemaInput(schema))
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

fn encode_state(change: StateChange<'_>) -> StorageResult<(Staged, Option<String>)> {
    match change {
        StateChange::State { schema, state } => {
            require_name(schema)?;
            Ok((put_json(schema.to_owned(), state)?, None))
        }
        StateChange::Policy { schema, policy } => {
            require_name(schema)?;
            if policy.ttl_seconds == 0 || policy.hash_partitions.iter().any(String::is_empty) {
                return Err(StorageError::InvalidOperation(
                    "invalid schema retention policy".into(),
                ));
            }
            Ok((put_json(retention_policy_key(schema), policy)?, None))
        }
        StateChange::Age {
            schema,
            entry,
            latest,
        } => {
            require_name(schema)?;
            let key = if latest {
                retention_age_latest_key(schema, &entry.key)
            } else {
                retention_age_key(schema, entry.written_at, &entry.key)
            };
            Ok((put_json(key, &entry)?, Some(schema.to_owned())))
        }
        StateChange::Partition { schema, entry } => {
            require_name(schema)?;
            Ok((
                put_json(retention_hash_partition_key(schema, &entry.hash), &entry)?,
                Some(schema.to_owned()),
            ))
        }
    }
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
        let (s, st) = (self.schemas_store.clone(), self.schema_states_store.clone());
        (ReadStore::new(s), ReadStore::new(st))
    }

    pub(crate) fn runtime(&self) -> Arc<PackRuntime> {
        Arc::clone(&self.runtime)
    }

    pub(crate) async fn put_schema(&self, input: SchemaInput<'_>) -> StorageResult<()> {
        self.stage(true, vec![put_json(input.0.name.clone(), input.0)?], &[])
            .await
    }

    pub(crate) async fn delete_schema(&self, name: &str) -> StorageResult<()> {
        require_name(name)?;
        self.stage(true, vec![(name.to_owned(), None)], &[]).await
    }

    pub(crate) async fn put_state(&self, change: StateChange<'_>) -> StorageResult<()> {
        self.batch_states(vec![change]).await
    }

    pub(crate) async fn batch_states(&self, changes: Vec<StateChange<'_>>) -> StorageResult<()> {
        let (mut entries, mut owners) = (Vec::with_capacity(changes.len()), Vec::new());
        for change in changes {
            let (entry, owner) = encode_state(change)?;
            entries.push(entry);
            owners.extend(owner);
        }
        owners.sort();
        owners.dedup();
        self.stage(false, entries, &owners).await
    }

    pub(crate) async fn delete_states(&self, changes: Vec<StateDelete<'_>>) -> StorageResult<()> {
        self.stage(false, encode_deletes(changes)?, &[]).await
    }

    pub(crate) async fn flush_schemas(&self) -> StorageResult<()> {
        self.schemas_store.flush().await
    }

    pub(crate) async fn flush_states(&self) -> StorageResult<()> {
        self.schema_states_store.flush().await
    }

    async fn stage(
        &self,
        schemas: bool,
        entries: Vec<Staged>,
        owners: &[String],
    ) -> StorageResult<()> {
        transaction::scope(self.runtime(), async {
            if !transaction::publication::active(&self.runtime) {
                let msg = "schema write requires its native scope";
                return Err(StorageError::InvalidOperation(msg.into()));
            }
            self.require_tracking_writable(owners).await?;
            let mutations = entries
                .into_iter()
                .map(|(k, v)| match v {
                    Some(b) => KvMutation::put(k, b),
                    None => KvMutation::delete(k),
                })
                .collect();
            let store = if schemas {
                &self.schemas_store
            } else {
                &self.schema_states_store
            };
            store.batch_mutate(mutations).await
        })
        .await
    }

    async fn require_tracking_writable(&self, schemas: &[String]) -> StorageResult<()> {
        if schemas.is_empty() {
            return Ok(());
        }
        let keys = schemas.iter().map(|n| n.as_bytes().to_vec()).collect();
        let reader = ReadStore::new(self.schema_states_store.clone());
        let (states, definitions) = tokio::try_join!(
            reader.get_items::<SchemaState>(schemas),
            self.schemas_store.get_many(keys),
        )?;
        if states.len() != schemas.len() || definitions.len() != schemas.len() {
            return Err(StorageError::BackendError(
                "tracking owner read is incomplete".into(),
            ));
        }
        for ((schema, state), def) in schemas.iter().zip(states).zip(definitions) {
            if def.is_none() || state == Some(SchemaState::Blocked) {
                return Err(StorageError::InvalidOperation(format!(
                    "expiry tracking owner {schema} is blocked or not installed"
                )));
            }
        }
        Ok(())
    }
}

fn put_json(key: String, value: &impl Serialize) -> StorageResult<Staged> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| StorageError::SerializationError(error.to_string()))?;
    Ok((key, Some(bytes)))
}

fn require_name(name: &str) -> StorageResult<()> {
    if name.is_empty() || name.contains('\0') {
        return Err(StorageError::InvalidOperation(
            "schema keys require a nonempty name without NUL".into(),
        ));
    }
    Ok(())
}
