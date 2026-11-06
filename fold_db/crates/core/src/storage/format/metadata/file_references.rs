//! The local file-version index tracks the live record and the last mutation
//! that needs each exact version. Both references change in the native commit.

use super::{EncodedMetadata, Metadata};
use crate::storage::error::{StorageError, StorageResult};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

const REFERENCE_PREFIX: &str = "portable-file-reference:";
pub(in crate::storage) const FILE_RETENTION_KEY: &str = "portable-file-retention";
pub(in crate::storage) const FILE_RETENTION_COMPLETE: &[u8] = b"\"complete\"";

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

impl Metadata {
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
            return Err(invalid("incomplete file reference read"));
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
                .map_err(invalid)?
                .unwrap_or_default();
            reference.live = change.live.unwrap_or(reference.live);
            reference.pending_through = reference.pending_through.max(change.pending_through);
            if reference.pending_through <= change.completed_through {
                reference.pending_through = 0;
            }
            if reference.live || reference.pending_through != 0 {
                puts.push(EncodedMetadata {
                    key,
                    bytes: serde_json::to_vec(&reference).map_err(invalid)?,
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
            .map_err(invalid)?
            .unwrap_or_default())
    }

    pub(crate) async fn stage_file_retention(&self, state: &FileRetention) -> StorageResult<()> {
        self.stage(EncodedMetadata {
            key: FILE_RETENTION_KEY.into(),
            bytes: serde_json::to_vec(state).map_err(invalid)?,
        })
        .await
    }

    pub(crate) async fn has_file_reference(&self, path: &str) -> StorageResult<bool> {
        self.metadata_store
            .exists(reference_key(path).as_bytes())
            .await
    }
}

fn reference_key(path: &str) -> String {
    format!("{REFERENCE_PREFIX}{path}")
}

fn invalid(detail: impl std::fmt::Display) -> StorageError {
    StorageError::InvalidOperation(format!("file reference index: {detail}"))
}
