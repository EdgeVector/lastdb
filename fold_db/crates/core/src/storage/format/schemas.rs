//! Schema/state format capability, catalog key framing, and schema release families.

pub(crate) mod applied_deletes {
    use crate::storage::{error::StorageResult, KvMutation, StorageError};
    use lastdb_model::portable_mutation::{AppliedDelete, Stamp};

    pub(in crate::storage) const APPLIED_DELETE_FAMILY: &str = "ad";

    pub(in crate::storage::format) fn encode(k: &str, s: &Stamp) -> StorageResult<KvMutation> {
        serde_json::to_vec(&AppliedDelete::from(s.clone()))
            .map(|v| KvMutation::put(format!("ad:{k}"), v))
            .map_err(inv)
    }
    pub(in crate::storage::format) fn decode(bytes: &[u8]) -> StorageResult<Stamp> {
        if bytes.len() > 512 {
            return Err(inv("encoded value exceeds its bound"));
        }
        let e: AppliedDelete = serde_json::from_slice(bytes).map_err(inv)?;
        e.delete_position.validate().map_err(inv).map(|()| e.into())
    }
    pub(in crate::storage::format) fn inv(e: impl std::fmt::Display) -> StorageError {
        StorageError::invalid(format!("applied delete: {e}"))
    }
}

use super::{inline_files::INLINE_FILE_FAMILY, read::ReadStore};
pub(crate) use super::{
    RETENTION_AGE_KEY_PREFIX, RETENTION_AGE_LATEST_PREFIX, RETENTION_HASH_PARTITION_KEY_PREFIX,
    RETENTION_POLICY_KEY_PREFIX,
};
use crate::schema::types::KeyValue;
use crate::schema::{Schema, SchemaError, SchemaRetentionPolicy, SchemaState};
use crate::storage::disk_pack::{transaction, PackKvStore, PackRuntime};
use crate::storage::{error::StorageResult, KvMutation, StorageError};
use lastdb_model::field_key_id::{alternate_field_key_id, ALTERNATE_ENTRY_PREFIX};
use lastdb_model::hex::sha256_hex as fp;
use lastdb_model::record_space::{record_space_uuid, RECORD_PREFIX};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub(crate) const CATALOG_PAGE_ROWS: usize = 256;
pub(crate) const CATALOG_SCAN_END: &[u8] = &[0xff];
pub(crate) const CATALOG_STATE_SCAN_START: &[u8] = &[0x01];

pub(crate) mod keys {
    pub(crate) use super::*;
}

#[derive(Clone, Copy)]
pub(crate) enum ReleaseKind {
    Records,
    Alternates,
    AppliedDeletes,
    InlineFiles,
}

pub(crate) struct ReleaseFamily(
    pub(in crate::storage::format) String,
    pub(in crate::storage::format) ReleaseKind,
);

impl ReleaseFamily {
    pub(crate) fn prefix(&self) -> &str {
        &self.0
    }
    pub(crate) fn kind(&self) -> ReleaseKind {
        self.1
    }
}

pub(crate) fn release_families(schema: &Schema) -> Result<Vec<ReleaseFamily>, SchemaError> {
    let base = format!("{RECORD_PREFIX}{}:", record_space_uuid(&schema.name));
    let ad = format!("{}:{base}", applied_deletes::APPLIED_DELETE_FAMILY);
    let fi = format!("{INLINE_FILE_FAMILY}:{base}");
    let mut out = vec![
        ReleaseFamily(base, ReleaseKind::Records),
        ReleaseFamily(ad, ReleaseKind::AppliedDeletes),
        ReleaseFamily(fi, ReleaseKind::InlineFiles),
    ];
    for name in schema.require_record_keys()?.alternates.keys() {
        let fk = alternate_field_key_id(schema, name);
        out.push(ReleaseFamily(
            format!("{ALTERNATE_ENTRY_PREFIX}{fk}:"),
            ReleaseKind::Alternates,
        ));
    }
    Ok(out)
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

pub(crate) fn retention_policy_key(s: &str) -> String {
    format!("{RETENTION_POLICY_KEY_PREFIX}{s}")
}
pub(crate) fn retention_age_prefix(s: &str) -> String {
    format!("{RETENTION_AGE_KEY_PREFIX}{}\0", fp(s))
}
pub(crate) fn retention_age_latest_prefix(s: &str) -> String {
    format!("{RETENTION_AGE_LATEST_PREFIX}{}\0", fp(s))
}
pub(crate) fn retention_age_key(s: &str, at: u64, k: &KeyValue) -> String {
    let pfx = retention_age_prefix(s);
    format!("{pfx}{at:020}\0{}", fp(k.to_storage_key()))
}
pub(crate) fn retention_age_latest_key(s: &str, k: &KeyValue) -> String {
    let pfx = retention_age_latest_prefix(s);
    format!("{pfx}{}", fp(k.to_storage_key()))
}
pub(crate) fn retention_hash_partition_prefix(s: &str) -> String {
    format!("{RETENTION_HASH_PARTITION_KEY_PREFIX}{}\0", fp(s))
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

pub(crate) fn schema_input<'a>(name: &str, s: &'a Schema) -> Result<SchemaInput<'a>, SchemaError> {
    require_name(name)?;
    if s.name != name {
        let msg = "schema catalog key differs from payload name";
        return Err(SchemaError::InvalidData(msg.into()));
    }
    let keys = s.require_record_keys()?;
    s.validate_definition()
        .and_then(|()| s.validate_key_field_types())
        .and_then(|()| keys.validate())
        .map_err(SchemaError::InvalidData)?;
    if s.file_required {
        let mut c = s.to_wire();
        c.compute_identity_hash();
        if (&s.identity_hash, s.identity_hash_algo_version)
            != (&c.identity_hash, c.identity_hash_algo_version)
        {
            let msg = "required-file schema needs its exact catalog identity";
            return Err(SchemaError::InvalidData(msg.into()));
        }
    }
    Ok(SchemaInput(s))
}

type Rt = Arc<PackRuntime>;

#[derive(Clone)]
pub(crate) struct SchemaFormat(PackKvStore, PackKvStore, Rt);

impl SchemaFormat {
    pub(in crate::storage::format) fn new(s: PackKvStore, st: PackKvStore, rt: Rt) -> Self {
        Self(s, st, rt)
    }
    pub(crate) fn readers(&self) -> (ReadStore, ReadStore) {
        (self.0.clone(), self.1.clone())
    }
    pub(crate) fn runtime(&self) -> Rt {
        Arc::clone(&self.2)
    }
    pub(crate) async fn flush_schemas(&self) -> StorageResult<()> {
        self.2.flush().await
    }
    pub(crate) async fn flush_states(&self) -> StorageResult<()> {
        self.2.flush().await
    }
    pub(crate) async fn put_schema(&self, input: SchemaInput<'_>) -> StorageResult<()> {
        let ops = vec![put_json(input.0.name.clone(), input.0)?];
        self.stage(true, ops, &[]).await
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
        for c in changes {
            let (schema, op, tracked) = encode_state(c)?;
            require_name(schema)?;
            entries.push(op);
            if tracked {
                owners.push(schema.to_owned());
            }
        }
        owners.sort();
        owners.dedup();
        self.stage(false, entries, &owners).await
    }
    pub(crate) async fn delete_states(&self, changes: Vec<StateDelete<'_>>) -> StorageResult<()> {
        let mut out = Vec::new();
        for c in changes {
            encode_delete(c, &mut out)?;
        }
        self.stage(false, out, &[]).await
    }
    async fn stage(
        &self,
        schemas: bool,
        ops: Vec<KvMutation>,
        owners: &[String],
    ) -> StorageResult<()> {
        transaction::scope(self.runtime(), async {
            if transaction::PendingBatch::current(&self.2).is_none() {
                let msg = "schema write requires its native scope";
                return Err(StorageError::invalid(msg));
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
                for ((s, st), d) in owners.iter().zip(states).zip(defs) {
                    if d.is_none() || st == Some(SchemaState::Blocked) {
                        let msg = format!("expiry tracking owner {s} is blocked or not installed");
                        return Err(StorageError::invalid(msg));
                    }
                }
            }
            let dst = if schemas { &self.0 } else { &self.1 };
            dst.batch_mutate(ops).await
        })
        .await
    }
}

fn encode_state(change: StateChange<'_>) -> StorageResult<(&str, KvMutation, bool)> {
    match change {
        StateChange::State { schema, state } => {
            Ok((schema, put_json(schema.into(), state)?, false))
        }
        StateChange::Policy { schema, policy } => {
            if policy.ttl_seconds == 0 || policy.hash_partitions.iter().any(String::is_empty) {
                return Err(StorageError::invalid("invalid schema retention policy"));
            }
            let op = put_json(retention_policy_key(schema), policy)?;
            Ok((schema, op, false))
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
            Ok((schema, put_json(k, &entry)?, true))
        }
        StateChange::Partition { schema, entry } => {
            let pfx = retention_hash_partition_prefix(schema);
            let k = format!("{pfx}{}", fp(&entry.hash));
            Ok((schema, put_json(k, &entry)?, true))
        }
    }
}

fn encode_delete(change: StateDelete<'_>, out: &mut Vec<KvMutation>) -> StorageResult<()> {
    let bad = || StorageError::invalid("retention delete key differs from its owner/family");
    match change {
        StateDelete::Key { schema, kind } => {
            require_name(schema)?;
            let k = match kind {
                StateKey::State => schema.to_owned(),
                StateKey::Policy => retention_policy_key(schema),
            };
            out.push(KvMutation::delete(k));
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
                let dig = if matches!(kind, Tracking::Age) {
                    let (c, d) = sfx.split_once('\0').ok_or_else(bad)?;
                    if c.len() != 20 || c.parse::<u64>().is_err() {
                        return Err(bad());
                    }
                    d
                } else {
                    sfx
                };
                if dig.len() != 64 || !dig.bytes().all(|b| b.is_ascii_hexdigit()) {
                    return Err(bad());
                }
                out.push(KvMutation::delete(key));
            }
        }
    }
    Ok(())
}

fn put_json(key: String, value: &impl Serialize) -> StorageResult<KvMutation> {
    serde_json::to_vec(value)
        .map(|b| KvMutation::put(key, b))
        .map_err(|e| StorageError::SerializationError(e.to_string()))
}

fn require_name(name: &str) -> StorageResult<()> {
    (!name.is_empty() && !name.contains('\0'))
        .then_some(())
        .ok_or_else(|| StorageError::invalid("schema keys require a nonempty name without NUL"))
}
