//! The metadata namespace's typed capability and its only staging writer.
//!
//! Callers supply clock values. Nested encoders
//! receive data, never this namespace handle. The existing native request
//! owns commit and publication; staging here never creates a request or flush.

use std::sync::Arc;

use crate::schema::types::MutationAuthorClockState;
use crate::schema::SchemaError;
use crate::storage::disk_pack::{transaction, PackRuntime};
use crate::storage::error::StorageResult;
use crate::storage::traits::YieldedScan;
use crate::storage::{KvStore, StorageError};

mod file_references;
mod portable_author;
pub(in crate::storage) const NATIVE_SHARED_SCOPE_KEY: &str = "native-shared-schema-scope";
pub(crate) use file_references::{FileReferenceChange, FileRetention};
pub(in crate::storage) use file_references::{FILE_RETENTION_COMPLETE, FILE_RETENTION_KEY};
pub(in crate::storage) use portable_author::{
    initial_local_author, shared_metadata_key, validate_local_author, LOCAL_AUTHOR_KEY,
};
pub(crate) use portable_author::{AuthoredStamp, CheckpointDirectory, InboxWindow, JournalSchema};
pub use portable_author::{OutgoingPortablePage, PortablePeerProgress};

#[derive(Clone)]
pub(crate) struct Metadata {
    metadata_store: Arc<dyn KvStore>,
    runtime: Arc<PackRuntime>,
}

/// Private encoded output. No caller can construct raw metadata destinations.
struct EncodedMetadata {
    key: String,
    bytes: Vec<u8>,
}

impl Metadata {
    pub(in crate::storage::format) fn new(
        metadata_store: Arc<dyn KvStore>,
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
            // Preserve capacity refusals for the clock queue's bounded retry.
            StorageError::WriteResourceLimit { detail } => {
                SchemaError::WriteResourceLimit { detail }
            }
            other => SchemaError::InvalidData(format!("metadata put {key}: {other}")),
        })
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
