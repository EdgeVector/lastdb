//! The metadata namespace's typed capability and its only staging writer.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::schema::types::MutationAuthorClockState;
use crate::schema::SchemaError;
use crate::storage::disk_pack::{transaction, PackKvStore, PackRuntime};
use crate::storage::error::StorageResult;
use crate::storage::{KvMutation, StorageError};
use lastdb_model::portable_mutation::{Position, Stamp, StreamId, StreamProgress};
use serde::{Deserialize, Serialize};

pub(in crate::storage) const FILE_REFERENCE_PREFIX: &str = "portable-file-reference:";
pub(in crate::storage) const FILE_RETENTION_KEY: &str = "portable-file-retention";
pub(in crate::storage) const FILE_RETENTION_COMPLETE: &[u8] = b"\"complete\"";
pub(in crate::storage) const LOCAL_AUTHOR_KEY: &str = "portable-local-author";

const MAX_LOCAL_AUTHOR_BYTES: usize = 8192;
type Staged = (String, Option<Vec<u8>>);

#[derive(Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
struct Reference {
    live: bool,
    pending_through: u64,
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
    let state = LocalAuthor {
        stream: StreamId::try_from(home.to_string()).map_err(invalid_author)?,
        progress: StreamProgress::default(),
        clock_floor_ms: None,
    };
    serde_json::to_vec(&state).map_err(invalid_author)
}

pub(in crate::storage) fn validate_local_author(bytes: Option<&[u8]>) -> StorageResult<()> {
    LocalAuthor::decode(bytes).map(|_| ())
}

impl LocalAuthor {
    fn decode(bytes: Option<&[u8]>) -> StorageResult<Self> {
        let bytes = bytes.ok_or_else(|| invalid_author("local stream state is missing"))?;
        if bytes.len() > MAX_LOCAL_AUTHOR_BYTES {
            return Err(invalid_author("local stream state exceeds its bound"));
        }
        let state: Self = serde_json::from_slice(bytes).map_err(invalid_author)?;
        state.progress.validate().map_err(invalid_author)?;
        Ok(state)
    }

    fn author(&mut self, now_ms: u64) -> StorageResult<Stamp> {
        let now_ms = now_ms.max(self.clock_floor_ms.unwrap_or_default());
        let sequence = self
            .progress
            .applied_through
            .checked_add(1)
            .ok_or_else(|| invalid_author("local stream sequence exhausted"))?;
        let authored_at_ms = now_ms.max(self.progress.applied_through_ms.unwrap_or_default());
        self.progress.applied_through = sequence;
        self.progress.applied_through_ms = Some(authored_at_ms);
        Ok(Stamp {
            authored_at_ms,
            position: Position {
                source_stream: self.stream.clone(),
                sequence,
            },
        })
    }
}

#[derive(Clone)]
pub(crate) struct Metadata {
    metadata_store: PackKvStore,
    runtime: Arc<PackRuntime>,
}

impl Metadata {
    pub(in crate::storage::format) fn new(
        metadata_store: PackKvStore,
        runtime: Arc<PackRuntime>,
    ) -> Self {
        Self {
            metadata_store,
            runtime,
        }
    }

    pub(crate) async fn flush(&self) -> StorageResult<()> {
        self.metadata_store.flush().await
    }

    pub(crate) async fn author_clock(
        &self,
        writer: &str,
    ) -> Result<Option<MutationAuthorClockState>, SchemaError> {
        let key = super::mutation_author_clock_key(writer);
        let raw = self
            .metadata_store
            .get(key.as_bytes())
            .await
            .map_err(|error| SchemaError::InvalidData(format!("metadata get {key}: {error}")))?;
        raw.map(|bytes| {
            serde_json::from_slice(&bytes).map_err(|error| {
                SchemaError::InvalidData(format!(
                    "metadata get {key}: Serialization error: {error}"
                ))
            })
        })
        .transpose()
    }

    pub(crate) async fn stage_author_clock(
        &self,
        writer: &str,
        state: &MutationAuthorClockState,
    ) -> Result<(), SchemaError> {
        let key = super::mutation_author_clock_key(writer);
        let bytes = serde_json::to_vec(state).map_err(|error| {
            SchemaError::InvalidData(format!("metadata put {key}: Serialization error: {error}"))
        })?;
        self.stage(vec![(key.clone(), Some(bytes))])
            .await
            .map_err(|error| match error {
                StorageError::WriteResourceLimit { detail } => {
                    SchemaError::WriteResourceLimit { detail }
                }
                other => SchemaError::InvalidData(format!("metadata put {key}: {other}")),
            })
    }

    /// Return true for a replay, or stage the opaque acceptance in this native request.
    pub(crate) async fn stage_contribution_receipt(
        &self,
        receipt_id: &[u8; 32],
        intent_digest: &[u8; 32],
    ) -> Result<bool, SchemaError> {
        self.require_scope().map_err(contribution_storage_error)?;
        let key = format!(
            "share-contribution-receipt:{}",
            lastdb_model::hex::hex_lower(receipt_id)
        );
        let saved = self
            .metadata_store
            .get(key.as_bytes())
            .await
            .map_err(contribution_storage_error)?;
        if let Some(saved) = saved {
            if saved.as_slice() == intent_digest {
                return Ok(true);
            }
            return Err(SchemaError::InvalidField("share mutation ID reused".into()));
        }
        self.stage(vec![(key, Some(intent_digest.to_vec()))])
            .await
            .map_err(contribution_storage_error)?;
        Ok(false)
    }

    pub(crate) async fn stage_file_references(
        &self,
        changes: BTreeMap<String, bool>,
    ) -> StorageResult<Vec<Vec<u8>>> {
        if changes.is_empty() {
            return Ok(Vec::new());
        }
        self.require_scope()?;
        let keys: Vec<_> = changes
            .keys()
            .map(|path| format!("{FILE_REFERENCE_PREFIX}{path}"))
            .collect();
        let rows = self
            .metadata_store
            .get_many(keys.iter().map(|key| key.as_bytes().to_vec()).collect())
            .await?;
        if rows.len() != keys.len() {
            return Err(invalid_file_ref("incomplete file reference read"));
        }
        let mut entries = Vec::new();
        let mut retired = Vec::new();
        for (((path, live), key), row) in changes.into_iter().zip(keys).zip(rows) {
            let mut reference: Reference = row
                .as_deref()
                .map(serde_json::from_slice)
                .transpose()
                .map_err(invalid_file_ref)?
                .unwrap_or_default();
            reference.live = live;
            reference.pending_through = 0;
            if reference.live {
                entries.push((
                    key,
                    Some(serde_json::to_vec(&reference).map_err(invalid_file_ref)?),
                ));
            } else {
                if row.is_some() {
                    entries.push((key, None));
                }
                retired.push(path.into_bytes());
            }
        }
        self.stage(entries).await?;
        Ok(retired)
    }

    pub(crate) async fn stage_local_stamps(&self, schemas: &[&str]) -> StorageResult<Vec<Stamp>> {
        self.require_scope()?;
        if schemas.is_empty() {
            return Ok(Vec::new());
        }
        let now_ms = u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(invalid_author)?
                .as_millis(),
        )
        .map_err(invalid_author)?;
        let raw = self.metadata_store.get(LOCAL_AUTHOR_KEY.as_bytes()).await?;
        let mut state = LocalAuthor::decode(raw.as_deref())?;
        let stamps = schemas
            .iter()
            .map(|_| state.author(now_ms))
            .collect::<StorageResult<Vec<_>>>()?;
        let bytes = serde_json::to_vec(&state).map_err(invalid_author)?;
        if bytes.len() > MAX_LOCAL_AUTHOR_BYTES {
            return Err(invalid_author("local stream state exceeds its bound"));
        }
        self.stage(vec![(LOCAL_AUTHOR_KEY.to_owned(), Some(bytes))])
            .await?;
        Ok(stamps)
    }

    fn require_scope(&self) -> StorageResult<()> {
        if !transaction::publication::active(&self.runtime) {
            return Err(StorageError::InvalidOperation(
                "a metadata write requires a complete native write scope".into(),
            ));
        }
        Ok(())
    }

    async fn stage(&self, entries: Vec<Staged>) -> StorageResult<()> {
        self.require_scope()?;
        if !entries.is_empty() {
            let mutations = entries
                .into_iter()
                .map(|(k, v)| match v {
                    Some(b) => KvMutation::put(k, b),
                    None => KvMutation::delete(k),
                })
                .collect();
            self.metadata_store.batch_mutate(mutations).await?;
        }
        Ok(())
    }
}

fn invalid_file_ref(detail: impl std::fmt::Display) -> StorageError {
    StorageError::InvalidOperation(format!("file reference index: {detail}"))
}

fn invalid_author(error: impl std::fmt::Display) -> StorageError {
    StorageError::InvalidOperation(format!("portable author state: {error}"))
}

fn contribution_storage_error(error: impl std::fmt::Display) -> SchemaError {
    SchemaError::StorageRead {
        detail: error.to_string(),
    }
}
