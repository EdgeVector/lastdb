//! The metadata namespace's typed capability and its only staging writer.
//!
//! Callers supply clock values. Nested encoders receive data, never this
//! namespace handle. The existing native request owns commit and publication;
//! staging here never creates a request or flush.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::schema::types::MutationAuthorClockState;
use crate::schema::SchemaError;
use crate::storage::disk_pack::{transaction, PackKvStore, PackRuntime};
use crate::storage::error::StorageResult;
use crate::storage::traits::YieldedScan;
use crate::storage::StorageError;
use serde::{Deserialize, Serialize};

mod portable_author;
pub(in crate::storage) const NATIVE_SHARED_SCOPE_KEY: &str = "native-shared-schema-scope";
pub(in crate::storage) const FILE_REFERENCE_PREFIX: &str = "portable-file-reference:";
pub(in crate::storage) const FILE_RETENTION_KEY: &str = "portable-file-retention";
pub(in crate::storage) const FILE_RETENTION_COMPLETE: &[u8] = b"\"complete\"";
pub(in crate::storage) use portable_author::{
    initial_local_author, shared_metadata_key, validate_local_author, LOCAL_AUTHOR_KEY,
};
pub(crate) use portable_author::{CheckpointDirectory, DefinitionSet, JournalSchema};
pub use portable_author::{OutgoingPortablePage, PortablePeerProgress};

#[derive(Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
struct Reference {
    live: bool,
    pending_through: u64,
}

#[derive(Default)]
pub(crate) struct FileReferenceChange {
    pub(crate) live: Option<bool>,
    pub(crate) pending_through: u64,
    pub(crate) completed_through: u64,
    pub(crate) local_detach: bool,
}

#[derive(Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum FileRetention {
    #[default]
    Uninitialized,
    Log(Vec<u8>),
    Records(Vec<u8>),
    Files(Vec<u8>),
    Complete,
}

#[derive(Clone)]
pub(crate) struct Metadata {
    metadata_store: PackKvStore,
    runtime: Arc<PackRuntime>,
}

/// Private encoded output. No caller can construct raw metadata destinations.
struct EncodedMetadata {
    key: String,
    bytes: Vec<u8>,
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
        let value = async {
            self.metadata_store
                .get(key.as_bytes())
                .await?
                .map(|bytes| {
                    serde_json::from_slice(&bytes)
                        .map_err(|error| StorageError::SerializationError(error.to_string()))
                })
                .transpose()
        }
        .await;
        value.map_err(|error| SchemaError::InvalidData(format!("metadata get {key}: {error}")))
    }

    pub(crate) async fn stage_author_clock(
        &self,
        writer: &str,
        state: &MutationAuthorClockState,
    ) -> Result<(), SchemaError> {
        let key = super::mutation_author_clock_key(writer);
        let result = async {
            let bytes = serde_json::to_vec(state)
                .map_err(|error| StorageError::SerializationError(error.to_string()))?;
            self.stage(EncodedMetadata {
                key: key.clone(),
                bytes,
            })
            .await
        }
        .await;
        result.map_err(|error| match error {
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
        self.require_scope()
            .map_err(|error| contribution_storage_error(&error))?;
        let key = format!(
            "share-contribution-receipt:{}",
            lastdb_model::hex::hex_lower(receipt_id)
        );
        let saved = self
            .metadata_store
            .get(key.as_bytes())
            .await
            .map_err(|error| contribution_storage_error(&error))?;
        if let Some(saved) = saved {
            if saved.as_slice() == intent_digest {
                return Ok(true);
            }
            return Err(SchemaError::InvalidField("share mutation ID reused".into()));
        }
        self.stage(EncodedMetadata {
            key,
            bytes: intent_digest.to_vec(),
        })
        .await
        .map_err(|error| contribution_storage_error(&error))?;
        Ok(false)
    }

    /// Grow the shared scope by aliases that no schema in this home uses yet.
    pub(crate) async fn stage_shared_aliases<'a>(
        &self,
        names: impl IntoIterator<Item = &'a str>,
    ) -> StorageResult<()> {
        if let Some(bytes) = self.runtime.grown_shared_aliases(names)? {
            self.stage(EncodedMetadata {
                key: NATIVE_SHARED_SCOPE_KEY.into(),
                bytes,
            })
            .await?;
        }
        Ok(())
    }

    pub(crate) async fn stage_reset_rejoin_file_references(
        &self,
        after: Option<&[u8]>,
    ) -> StorageResult<Option<Vec<u8>>> {
        if after.is_none() && !matches!(self.file_retention().await?, FileRetention::Uninitialized)
        {
            self.stage_file_retention(&FileRetention::Uninitialized)
                .await?;
        }
        let page = self
            .rejoin_prefix_page(FILE_REFERENCE_PREFIX, after)
            .await?;
        if !page.rows.is_empty() {
            self.stage_deletes(page.rows.into_iter().map(|(key, _)| key).collect())
                .await?;
        }
        Ok(page.resume)
    }

    pub(crate) async fn stage_file_references(
        &self,
        changes: BTreeMap<String, FileReferenceChange>,
    ) -> StorageResult<Vec<Vec<u8>>> {
        if changes.is_empty() {
            return Ok(Vec::new());
        }
        self.require_scope()?;
        let keys: Vec<_> = changes.keys().map(|path| reference_key(path)).collect();
        let (state, rows) = futures::try_join!(
            self.file_retention(),
            self.metadata_store
                .get_many(keys.iter().map(|key| key.as_bytes().to_vec()).collect()),
        )?;
        if rows.len() != keys.len() {
            return Err(invalid_file_ref("incomplete file reference read"));
        }
        let can_retire = matches!(state, FileRetention::Files(_) | FileRetention::Complete);
        let mut puts = Vec::new();
        let mut deletes = Vec::new();
        let mut retired = Vec::new();
        for (((path, change), key), row) in changes.into_iter().zip(keys).zip(rows) {
            let mut reference: Reference = row
                .as_deref()
                .map(serde_json::from_slice)
                .transpose()
                .map_err(invalid_file_ref)?
                .unwrap_or_default();
            reference.live = change.live.unwrap_or(reference.live);
            reference.pending_through = reference.pending_through.max(change.pending_through);
            if reference.pending_through <= change.completed_through {
                reference.pending_through = 0;
            }
            if reference.live || reference.pending_through != 0 {
                puts.push(EncodedMetadata {
                    key,
                    bytes: serde_json::to_vec(&reference).map_err(invalid_file_ref)?,
                });
            } else {
                if row.is_some() {
                    deletes.push(key.into_bytes());
                }
                if can_retire || change.local_detach {
                    retired.push(path.into_bytes());
                }
            }
        }
        if !puts.is_empty() {
            self.stage_batch(puts).await?;
        }
        if !deletes.is_empty() {
            self.stage_deletes(deletes).await?;
        }
        Ok(retired)
    }

    pub(crate) async fn file_retention(&self) -> StorageResult<FileRetention> {
        let row = self
            .metadata_store
            .get(FILE_RETENTION_KEY.as_bytes())
            .await?;
        Ok(row
            .as_deref()
            .map(serde_json::from_slice)
            .transpose()
            .map_err(invalid_file_ref)?
            .unwrap_or_default())
    }

    pub(crate) async fn stage_file_retention(&self, state: &FileRetention) -> StorageResult<()> {
        self.stage(EncodedMetadata {
            key: FILE_RETENTION_KEY.into(),
            bytes: serde_json::to_vec(state).map_err(invalid_file_ref)?,
        })
        .await
    }

    pub(crate) async fn has_file_reference(&self, path: &str) -> StorageResult<bool> {
        self.metadata_store
            .exists(reference_key(path).as_bytes())
            .await
    }

    fn require_scope(&self) -> StorageResult<()> {
        if !transaction::publication::active(&self.runtime) {
            return Err(StorageError::InvalidOperation(
                "a metadata write requires a complete native write scope".into(),
            ));
        }
        Ok(())
    }

    async fn stage(&self, encoded: EncodedMetadata) -> StorageResult<()> {
        self.require_scope()?;
        self.metadata_store
            .put(encoded.key.as_bytes(), encoded.bytes)
            .await
    }

    async fn stage_batch(&self, rows: Vec<EncodedMetadata>) -> StorageResult<()> {
        self.require_scope()?;
        self.metadata_store
            .batch_put(
                rows.into_iter()
                    .map(|row| (row.key.into_bytes(), row.bytes))
                    .collect(),
            )
            .await
    }

    async fn stage_deletes(&self, keys: Vec<Vec<u8>>) -> StorageResult<()> {
        self.require_scope()?;
        self.metadata_store.batch_delete(keys).await
    }

    /// Read one bounded page of a private metadata family for rejoin.
    async fn rejoin_prefix_page(
        &self,
        prefix: &str,
        after: Option<&[u8]>,
    ) -> StorageResult<YieldedScan> {
        const PAGE: usize = 64;
        self.require_scope()?;
        let Some((&b':', stem)) = prefix.as_bytes().split_last() else {
            return Err(StorageError::InvalidOperation(
                "invalid metadata prefix".into(),
            ));
        };
        let start = after.map_or_else(|| prefix.as_bytes().to_vec(), |key| [key, &[0]].concat());
        let end = [stem, b";"].concat();
        self.metadata_store
            .scan_range_yielding(&start, &end, PAGE, 1)
            .await
    }
}

fn reference_key(path: &str) -> String {
    format!("{FILE_REFERENCE_PREFIX}{path}")
}

fn invalid_file_ref(detail: impl std::fmt::Display) -> StorageError {
    StorageError::InvalidOperation(format!("file reference index: {detail}"))
}

fn contribution_storage_error(error: &StorageError) -> SchemaError {
    SchemaError::StorageRead {
        detail: error.to_string(),
    }
}
