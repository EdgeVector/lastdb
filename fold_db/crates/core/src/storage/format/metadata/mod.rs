//! Metadata namespace capability, local author clocks, receipts, and stamped stream state.

mod files;
mod replay;

pub(in crate::storage::format) use files::FileChange;
pub(crate) use files::{decrypt_file_row, require_row, seal_row};
pub(in crate::storage) use files::{FILE_RETENTION_COMPLETE, FILE_RETENTION_KEY};
pub use replay::ReplayStamp;
pub(crate) use replay::{ReplayTarget, SchemaReplayDrop};

use super::METADATA_NAMESPACE;
use crate::schema::types::{MutationAuthorClockState, SchemaError};
use crate::storage::disk_pack::{transaction, PackKvStore, PackRuntime};
use crate::storage::{error::StorageResult, KvMutation, StorageError};
use lastdb_model::hex::hex_lower;
use lastdb_model::portable_mutation::{Position, Stamp, StreamId, StreamProgress};
use lastdb_model::record_value::{RecordValueError, MAX_RECORD_BYTES};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub(in crate::storage) const LOCAL_AUTHOR_KEY: &str = "portable-local-author";
const MAX_LOCAL_AUTHOR_BYTES: usize = 8192;

fn schema_inv(msg: impl Into<String>) -> SchemaError {
    SchemaError::InvalidData(msg.into())
}

fn read_err(error: impl std::fmt::Display) -> SchemaError {
    SchemaError::StorageRead {
        detail: error.to_string(),
    }
}

fn invalid_author(error: impl std::fmt::Display) -> StorageError {
    StorageError::invalid(format!("portable author state: {error}"))
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LocalAuthor {
    stream: StreamId,
    progress: StreamProgress,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    clock_floor_ms: Option<u64>,
}

pub(in crate::storage) fn initial_local_author(home: uuid::Uuid) -> StorageResult<Vec<u8>> {
    let stream = StreamId::try_from(home.to_string()).map_err(invalid_author)?;
    let state = LocalAuthor {
        stream,
        progress: StreamProgress::default(),
        clock_floor_ms: None,
    };
    serde_json::to_vec(&state).map_err(invalid_author)
}

pub(in crate::storage) fn validate_local_author(bytes: Option<&[u8]>) -> StorageResult<()> {
    decode_author(bytes).map(|_| ())
}

fn decode_author(bytes: Option<&[u8]>) -> StorageResult<LocalAuthor> {
    let raw = bytes
        .filter(|b| b.len() <= MAX_LOCAL_AUTHOR_BYTES)
        .ok_or_else(|| invalid_author("local stream state missing or exceeds bound"))?;
    let state: LocalAuthor = serde_json::from_slice(raw).map_err(invalid_author)?;
    state
        .progress
        .validate()
        .map_err(invalid_author)
        .map(|()| state)
}

pub(in crate::storage::format) fn record_value_error(error: &RecordValueError) -> SchemaError {
    match *error {
        RecordValueError::TooLarge { bytes } => SchemaError::RecordTooLarge {
            size: bytes,
            limit: MAX_RECORD_BYTES,
        },
        _ => schema_inv(format!("the record write is refused: {error}")),
    }
}

#[derive(Clone)]
pub(crate) struct Metadata {
    store: PackKvStore,
    runtime: Arc<PackRuntime>,
}

impl Metadata {
    pub(in crate::storage::format) fn new(store: PackKvStore, runtime: Arc<PackRuntime>) -> Self {
        Self { store, runtime }
    }

    pub(crate) async fn flush(&self) -> StorageResult<()> {
        self.runtime.flush().await
    }

    pub(in crate::storage) async fn fork_restored_author(
        runtime: &Arc<PackRuntime>,
    ) -> StorageResult<()> {
        let store = runtime.open_store(METADATA_NAMESPACE).await?;
        let prev = decode_author(store.get(LOCAL_AUTHOR_KEY.as_bytes()).await?.as_deref())?;
        let mut next = decode_author(Some(&initial_local_author(uuid::Uuid::new_v4())?))?;
        next.clock_floor_ms = prev.clock_floor_ms.max(prev.progress.applied_through_ms);
        let value = serde_json::to_vec(&next).map_err(invalid_author)?;
        Self::new(store, Arc::clone(runtime))
            .stage(vec![(LOCAL_AUTHOR_KEY.to_owned(), Some(value))])
            .await
    }

    pub(crate) async fn author_clock(
        &self,
        writer: &str,
    ) -> Result<Option<MutationAuthorClockState>, SchemaError> {
        let key = super::mutation_author_clock_key(writer);
        self.store
            .get_item(&key)
            .await
            .map_err(|e| schema_inv(format!("metadata get {key}: {e}")))
    }

    pub(crate) async fn stage_author_clock(
        &self,
        writer: &str,
        state: &MutationAuthorClockState,
    ) -> Result<(), SchemaError> {
        let key = super::mutation_author_clock_key(writer);
        let bytes = serde_json::to_vec(state)
            .map_err(|e| schema_inv(format!("metadata put {key}: Serialization error: {e}")))?;
        self.stage(vec![(key.clone(), Some(bytes))])
            .await
            .map_err(|e| match e {
                StorageError::WriteResourceLimit { detail } => {
                    SchemaError::WriteResourceLimit { detail }
                }
                _ => schema_inv(format!("metadata put {key}: {e}")),
            })
    }

    pub(crate) async fn stage_contribution_receipt(
        &self,
        id: &[u8; 32],
        digest: &[u8; 32],
    ) -> Result<bool, SchemaError> {
        let key = format!("share-contribution-receipt:{}", hex_lower(id));
        self.stage_receipt(key, digest, "share mutation ID reused")
            .await
    }

    async fn stage_receipt(
        &self,
        key: String,
        digest: &[u8; 32],
        msg: &str,
    ) -> Result<bool, SchemaError> {
        if self.receipt_exists(&key, digest, msg).await? {
            return Ok(true);
        }
        self.stage(vec![(key, Some(digest.to_vec()))])
            .await
            .map_err(read_err)
            .map(|()| false)
    }

    async fn receipt_exists(
        &self,
        key: &str,
        digest: &[u8; 32],
        msg: &str,
    ) -> Result<bool, SchemaError> {
        self.require_replay_scope()?;
        match self.store.get(key.as_bytes()).await.map_err(read_err)? {
            Some(saved) if saved.as_slice() == digest => Ok(true),
            Some(_) => Err(SchemaError::InvalidField(msg.into())),
            None => Ok(false),
        }
    }

    pub(crate) fn require_replay_scope(&self) -> Result<(), SchemaError> {
        self.require_scope().map_err(read_err)
    }

    pub(crate) async fn stage_local_stamps(&self, schemas: &[&str]) -> StorageResult<Vec<Stamp>> {
        self.require_scope()?;
        if schemas.is_empty() {
            return Ok(Vec::new());
        }
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(invalid_author)
            .and_then(|d| u64::try_from(d.as_millis()).map_err(invalid_author))?;
        let mut state = decode_author(
            self.store
                .get(LOCAL_AUTHOR_KEY.as_bytes())
                .await?
                .as_deref(),
        )?;
        let mut stamps = Vec::with_capacity(schemas.len());
        for _ in schemas {
            let sequence = state
                .progress
                .applied_through
                .checked_add(1)
                .ok_or_else(|| invalid_author("local stream sequence exhausted"))?;
            let floor = state
                .clock_floor_ms
                .unwrap_or(0)
                .max(state.progress.applied_through_ms.unwrap_or(0));
            let authored_at_ms = now_ms.max(floor);
            state.progress.applied_through = sequence;
            state.progress.applied_through_ms = Some(authored_at_ms);
            stamps.push(Stamp {
                authored_at_ms,
                position: Position {
                    source_stream: state.stream.clone(),
                    sequence,
                },
            });
        }
        let bytes = serde_json::to_vec(&state).map_err(invalid_author)?;
        if bytes.len() > MAX_LOCAL_AUTHOR_BYTES {
            return Err(invalid_author("local stream state exceeds its bound"));
        }
        self.stage(vec![(LOCAL_AUTHOR_KEY.to_owned(), Some(bytes))])
            .await
            .map(|()| stamps)
    }

    fn require_scope(&self) -> StorageResult<()> {
        transaction::PendingBatch::current(&self.runtime)
            .map(|_| ())
            .ok_or_else(|| StorageError::invalid("metadata write requires native write scope"))
    }

    async fn stage(&self, entries: Vec<(String, Option<Vec<u8>>)>) -> StorageResult<()> {
        self.require_scope()?;
        if !entries.is_empty() {
            let ops = entries
                .into_iter()
                .map(|(key, value)| match value {
                    Some(bytes) => KvMutation::put(key, bytes),
                    None => KvMutation::delete(key),
                })
                .collect();
            self.store.batch_mutate(ops).await?;
        }
        Ok(())
    }
}
