//! Schema/state format capability, catalog key framing, and schema release families.

mod retention;

pub(crate) use super::{
    RETENTION_AGE_KEY_PREFIX, RETENTION_AGE_LATEST_PREFIX, RETENTION_HASH_PARTITION_KEY_PREFIX,
    RETENTION_POLICY_KEY_PREFIX,
};
pub(crate) use retention::{
    retention_age_key, retention_age_latest_key, retention_age_latest_prefix, retention_age_prefix,
    retention_hash_partition_prefix, retention_policy_key, SchemaRetentionAgeEntry,
    SchemaRetentionHashPartition, StateChange, StateDelete, StateKey, Tracking,
};

pub(crate) mod applied_deletes {
    use crate::storage::{error::StorageResult, KvMutation, StorageError};
    use lastdb_model::portable_mutation::{AppliedDelete, Stamp};

    pub(in crate::storage) const APPLIED_DELETE_FAMILY: &str = "ad";

    pub(in crate::storage::format) fn encode(
        key: &str,
        stamp: &Stamp,
    ) -> StorageResult<KvMutation> {
        serde_json::to_vec(&AppliedDelete::from(stamp.clone()))
            .map(|value| KvMutation::put(format!("ad:{key}"), value))
            .map_err(inv)
    }

    pub(in crate::storage::format) fn decode(bytes: &[u8]) -> StorageResult<Stamp> {
        if bytes.len() > 512 {
            return Err(inv("encoded value exceeds its bound"));
        }
        let entry: AppliedDelete = serde_json::from_slice(bytes).map_err(inv)?;
        entry
            .delete_position
            .validate()
            .map_err(inv)
            .map(|()| entry.into())
    }

    pub(in crate::storage::format) fn inv(error: impl std::fmt::Display) -> StorageError {
        StorageError::invalid(format!("applied delete: {error}"))
    }
}

use super::{inline_files::INLINE_FILE_FAMILY, read::ReadStore};
use crate::schema::types::catalog_types::SchemaState;
use crate::schema::types::{Schema, SchemaError};
use crate::storage::disk_pack::{transaction, PackKvStore, PackRuntime};
use crate::storage::{error::StorageResult, KvMutation, StorageError};
use lastdb_model::field_key_id::{alternate_field_key_id, ALTERNATE_ENTRY_PREFIX};
use lastdb_model::record_space::{record_space_uuid, RECORD_PREFIX};
use serde::de::DeserializeOwned;
use serde::Serialize;
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

pub(crate) struct ReleaseFamily {
    pub(in crate::storage::format) prefix: String,
    pub(in crate::storage::format) kind: ReleaseKind,
}

impl ReleaseFamily {
    pub(crate) fn prefix(&self) -> &str {
        &self.prefix
    }

    pub(crate) fn kind(&self) -> ReleaseKind {
        self.kind
    }
}

pub(crate) fn release_families(schema: &Schema) -> Result<Vec<ReleaseFamily>, SchemaError> {
    let base = format!("{RECORD_PREFIX}{}:", record_space_uuid(&schema.name));
    let applied_delete = format!("{}:{base}", applied_deletes::APPLIED_DELETE_FAMILY);
    let inline_file = format!("{INLINE_FILE_FAMILY}:{base}");
    let mut out = vec![
        ReleaseFamily {
            prefix: base,
            kind: ReleaseKind::Records,
        },
        ReleaseFamily {
            prefix: applied_delete,
            kind: ReleaseKind::AppliedDeletes,
        },
        ReleaseFamily {
            prefix: inline_file,
            kind: ReleaseKind::InlineFiles,
        },
    ];
    for name in schema.require_record_keys()?.alternates.keys() {
        let field_key = alternate_field_key_id(schema, name);
        out.push(ReleaseFamily {
            prefix: format!("{ALTERNATE_ENTRY_PREFIX}{field_key}:"),
            kind: ReleaseKind::Alternates,
        });
    }
    Ok(out)
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
            "schema catalog key differs from payload name".into(),
        ));
    }
    let keys = schema.require_record_keys()?;
    schema
        .validate_definition()
        .and_then(|()| schema.validate_key_field_types())
        .and_then(|()| keys.validate())
        .map_err(SchemaError::InvalidData)?;
    if schema.file_required {
        let mut wire = schema.to_wire();
        wire.compute_identity_hash();
        if (&schema.identity_hash, schema.identity_hash_algo_version)
            != (&wire.identity_hash, wire.identity_hash_algo_version)
        {
            return Err(SchemaError::InvalidData(
                "required-file schema needs its exact catalog identity".into(),
            ));
        }
    }
    Ok(SchemaInput { schema })
}

#[derive(Clone)]
pub(crate) struct SchemaFormat {
    schemas: PackKvStore,
    states: PackKvStore,
    runtime: Arc<PackRuntime>,
}

impl SchemaFormat {
    pub(in crate::storage::format) fn new(
        schemas: PackKvStore,
        states: PackKvStore,
        runtime: Arc<PackRuntime>,
    ) -> Self {
        Self {
            schemas,
            states,
            runtime,
        }
    }

    pub(crate) fn readers(&self) -> (ReadStore, ReadStore) {
        (self.schemas.clone(), self.states.clone())
    }

    pub(crate) fn runtime(&self) -> Arc<PackRuntime> {
        Arc::clone(&self.runtime)
    }

    pub(crate) async fn flush_schemas(&self) -> StorageResult<()> {
        self.runtime.flush().await
    }

    pub(crate) async fn flush_states(&self) -> StorageResult<()> {
        self.runtime.flush().await
    }

    pub(crate) async fn put_schema(&self, input: SchemaInput<'_>) -> StorageResult<()> {
        let ops = vec![put_json(input.schema.name.clone(), input.schema)?];
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
        for change in changes {
            let (schema, op, tracked) = retention::encode_state(change)?;
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
        for change in changes {
            retention::encode_delete(change, &mut out)?;
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
            if transaction::PendingBatch::current(&self.runtime).is_none() {
                return Err(StorageError::invalid(
                    "schema write requires its native scope",
                ));
            }
            if !owners.is_empty() {
                let keys = owners.iter().map(|name| name.as_bytes().to_vec()).collect();
                let (states, defs) = tokio::try_join!(
                    self.states.get_items::<SchemaState>(owners),
                    self.schemas.get_many(keys)
                )?;
                if states.len() != owners.len() || defs.len() != owners.len() {
                    return Err(StorageError::backend("tracking owner read is incomplete"));
                }
                for ((schema, state), def) in owners.iter().zip(states).zip(defs) {
                    if def.is_none() || state == Some(SchemaState::Blocked) {
                        return Err(StorageError::invalid(format!(
                            "expiry tracking owner {schema} is blocked or not installed"
                        )));
                    }
                }
            }
            let target = if schemas { &self.schemas } else { &self.states };
            target.batch_mutate(ops).await
        })
        .await
    }
}

pub(super) fn put_json(key: String, value: &impl Serialize) -> StorageResult<KvMutation> {
    serde_json::to_vec(value)
        .map(|bytes| KvMutation::put(key, bytes))
        .map_err(|e| StorageError::SerializationError(e.to_string()))
}

pub(super) fn require_name(name: &str) -> StorageResult<()> {
    (!name.is_empty() && !name.contains('\0'))
        .then_some(())
        .ok_or_else(|| StorageError::invalid("schema keys require a nonempty name without NUL"))
}
