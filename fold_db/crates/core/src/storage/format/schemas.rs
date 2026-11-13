//! Schema/state format capability and catalog key framing.

use super::read::ReadStore;
use crate::schema::types::KeyValue;
use crate::schema::{Schema, SchemaError, SchemaRetentionPolicy, SchemaState};
use crate::storage::disk_pack::{transaction, PackKvStore, PackRuntime};
pub(crate) use crate::storage::format::{
    RETENTION_AGE_KEY_PREFIX, RETENTION_AGE_LATEST_PREFIX, RETENTION_HASH_PARTITION_KEY_PREFIX,
    RETENTION_POLICY_KEY_PREFIX,
};
use crate::storage::{error::StorageResult, KvMutation, StorageError};
use lastdb_model::hex::sha256_hex as fp;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

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

fn inv(m: impl Into<String>) -> StorageError {
    StorageError::InvalidOperation(m.into())
}

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
pub(crate) fn durable_schema_eq(a: &Schema, b: &Schema) -> bool {
    a == b
}
pub(crate) fn decode_catalog_json<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    serde_json::from_slice::<T>(bytes).map_err(|e| e.to_string())
}
pub(crate) fn decode_catalog_schema(bytes: &[u8]) -> Result<Schema, String> {
    decode_catalog_json(bytes)
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
        let mut c = schema.to_wire();
        c.compute_identity_hash();
        if (&schema.identity_hash, schema.identity_hash_algo_version)
            != (&c.identity_hash, c.identity_hash_algo_version)
        {
            return Err(SchemaError::InvalidData(
                "required-file schema needs its exact catalog identity".into(),
            ));
        }
    }
    Ok(SchemaInput(schema))
}

#[derive(Clone)]
pub(crate) struct SchemaFormat(PackKvStore, PackKvStore, Arc<PackRuntime>);

impl SchemaFormat {
    pub(in crate::storage::format) fn new(
        schemas: PackKvStore,
        states: PackKvStore,
        runtime: Arc<PackRuntime>,
    ) -> Self {
        Self(schemas, states, runtime)
    }

    pub(crate) fn readers(&self) -> (ReadStore, ReadStore) {
        (self.0.clone(), self.1.clone())
    }
    pub(crate) fn runtime(&self) -> Arc<PackRuntime> {
        Arc::clone(&self.2)
    }
    pub(crate) async fn flush_schemas(&self) -> StorageResult<()> {
        self.2.flush().await
    }
    pub(crate) async fn flush_states(&self) -> StorageResult<()> {
        self.2.flush().await
    }
    pub(crate) async fn put_schema(&self, input: SchemaInput<'_>) -> StorageResult<()> {
        self.stage(true, vec![put_json(input.0.name.clone(), input.0)?], &[])
            .await
    }
    pub(crate) async fn delete_schema(&self, name: &str) -> StorageResult<()> {
        require_name(name)?;
        self.stage(true, vec![KvMutation::delete(name)], &[]).await
    }
    pub(crate) async fn put_state(&self, change: StateChange<'_>) -> StorageResult<()> {
        self.batch_states(vec![change]).await
    }

    pub(crate) async fn batch_states(&self, changes: Vec<StateChange<'_>>) -> StorageResult<()> {
        let (mut entries, mut owners) = (Vec::with_capacity(changes.len()), Vec::new());
        for change in changes {
            let (schema, entry, tracked) = match change {
                StateChange::State { schema, state } => {
                    (schema, put_json(schema.into(), state)?, false)
                }
                StateChange::Policy { schema, policy } => {
                    if policy.ttl_seconds == 0
                        || policy.hash_partitions.iter().any(String::is_empty)
                    {
                        return Err(inv("invalid schema retention policy"));
                    }
                    (
                        schema,
                        put_json(retention_policy_key(schema), policy)?,
                        false,
                    )
                }
                StateChange::Age {
                    schema,
                    entry,
                    latest,
                } => {
                    let k = if latest {
                        retention_age_latest_key(schema, &entry.key)
                    } else {
                        retention_age_key(schema, entry.written_at, &entry.key)
                    };
                    (schema, put_json(k, &entry)?, true)
                }
                StateChange::Partition { schema, entry } => {
                    let k = format!(
                        "{}{}",
                        retention_hash_partition_prefix(schema),
                        fp(&entry.hash)
                    );
                    (schema, put_json(k, &entry)?, true)
                }
            };
            require_name(schema)?;
            entries.push(entry);
            if tracked {
                owners.push(schema.to_owned());
            }
        }
        owners.sort();
        owners.dedup();
        self.stage(false, entries, &owners).await
    }

    pub(crate) async fn delete_states(&self, changes: Vec<StateDelete<'_>>) -> StorageResult<()> {
        let bad = || inv("retention delete key differs from its owner/family");
        let mut output = Vec::new();
        for change in changes {
            match change {
                StateDelete::Key { schema, kind } => {
                    require_name(schema)?;
                    let k = match kind {
                        StateKey::State => schema.to_owned(),
                        StateKey::Policy => retention_policy_key(schema),
                    };
                    output.push(KvMutation::delete(k));
                }
                StateDelete::Selected { schema, kind, keys } => {
                    require_name(schema)?;
                    let pfx = match kind {
                        Tracking::Age => retention_age_prefix(schema),
                        Tracking::Latest => retention_age_latest_prefix(schema),
                        Tracking::Partition => retention_hash_partition_prefix(schema),
                    };
                    for key in keys {
                        let sfx = key.strip_prefix(&pfx).ok_or_else(bad)?;
                        let dig = match kind {
                            Tracking::Age => {
                                let valid = |(c, _): &(&str, &str)| {
                                    c.len() == 20 && c.parse::<u64>().is_ok()
                                };
                                sfx.split_once('\0').filter(valid).ok_or_else(bad)?.1
                            }
                            Tracking::Latest | Tracking::Partition => sfx,
                        };
                        if dig.len() != 64 || !dig.bytes().all(|b| b.is_ascii_hexdigit()) {
                            return Err(bad());
                        }
                        output.push(KvMutation::delete(key));
                    }
                }
            }
        }
        self.stage(false, output, &[]).await
    }

    async fn stage(
        &self,
        schemas: bool,
        mutations: Vec<KvMutation>,
        owners: &[String],
    ) -> StorageResult<()> {
        transaction::scope(self.runtime(), async {
            if transaction::PendingBatch::current(&self.2).is_none() {
                return Err(inv("schema write requires its native scope"));
            }
            if !owners.is_empty() {
                let keys = owners.iter().map(|n| n.as_bytes().to_vec()).collect();
                let (states, defs) = tokio::try_join!(
                    self.1.get_items::<SchemaState>(owners),
                    self.0.get_many(keys)
                )?;
                if states.len() != owners.len() || defs.len() != owners.len() {
                    return Err(StorageError::backend("tracking owner read is incomplete"));
                }
                for ((schema, state), def) in owners.iter().zip(states).zip(defs) {
                    if def.is_none() || state == Some(SchemaState::Blocked) {
                        return Err(inv(format!(
                            "expiry tracking owner {schema} is blocked or not installed"
                        )));
                    }
                }
            }
            (if schemas { &self.0 } else { &self.1 })
                .batch_mutate(mutations)
                .await
        })
        .await
    }
}

fn put_json(key: String, value: &impl Serialize) -> StorageResult<KvMutation> {
    serde_json::to_vec(value)
        .map(|bytes| KvMutation::put(key, bytes))
        .map_err(|e| StorageError::SerializationError(e.to_string()))
}

fn require_name(name: &str) -> StorageResult<()> {
    (!name.is_empty() && !name.contains('\0'))
        .then_some(())
        .ok_or_else(|| inv("schema keys require a nonempty name without NUL"))
}
