//! The metadata namespace's typed capability and its only staging writer.
//!
//! Callers supply clock values or logical retry receipts. Nested encoders
//! receive data, never this namespace handle. The existing native request
//! owns commit and publication; staging here never creates a request or flush.

mod retry;

use std::sync::Arc;

use crate::schema::types::MutationAuthorClockState;
use crate::schema::SchemaError;
use crate::storage::disk_pack::{transaction, PackRuntime};
use crate::storage::error::StorageResult;
use crate::storage::{KvStore, StorageError};

#[derive(Clone)]
pub(crate) struct Metadata {
    metadata_store: Arc<dyn KvStore>,
    runtime: Arc<PackRuntime>,
}

/// Private encoded output. No caller can construct raw metadata destinations.
enum EncodedMetadata {
    Clock { key: String, bytes: Vec<u8> },
    Retry(Vec<(Vec<u8>, Vec<u8>)>),
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
            self.stage(EncodedMetadata::Clock {
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

    pub(crate) fn retry_cache_status(&self) -> serde_json::Value {
        retry::status()
    }

    pub(crate) async fn idempotency_ids(
        &self,
        keys: &[String],
    ) -> StorageResult<Vec<Option<String>>> {
        let lookup = retry::Lookup::new(keys);
        let bytes = self.metadata_store.get_many(lookup.keys()).await?;
        lookup.decode(bytes)
    }

    pub(crate) async fn stage_idempotency(
        &self,
        entries: Vec<(String, String)>,
    ) -> StorageResult<()> {
        let batch = retry::Batch::new(entries);
        if batch.is_empty() {
            return Ok(());
        }
        self.require_scope()?;
        let old = self.metadata_store.get_many(batch.keys()).await?;
        self.stage(batch.encode(old)?).await
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
        match encoded {
            EncodedMetadata::Clock { key, bytes } => {
                self.metadata_store.put(key.as_bytes(), bytes).await
            }
            EncodedMetadata::Retry(rows) => self.metadata_store.batch_put(rows).await,
        }
    }
}
