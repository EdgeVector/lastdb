//! Typed metadata operations. The format owns its namespace handle and codec.

use crate::schema::types::MutationAuthorClockState;
use crate::schema::SchemaError;
use crate::storage::format::metadata::Metadata;

/// Domain access to clocks and bounded retry receipts; no raw store handle.
#[derive(Clone)]
pub struct MetadataStore {
    format: Metadata,
}

impl MetadataStore {
    pub(crate) fn new(format: Metadata) -> Self {
        Self { format }
    }

    /// Explicit durability fence. Normal staging joins the existing native
    /// request, whose commit and publication finish before its acknowledgement.
    pub(crate) async fn flush(&self) -> Result<(), SchemaError> {
        self.format.flush().await?;
        Ok(())
    }

    pub(crate) async fn author_clock(
        &self,
        writer: &str,
    ) -> Result<Option<MutationAuthorClockState>, SchemaError> {
        self.format.author_clock(writer).await
    }

    /// Stage a typed state inside the caller's native request. The format
    /// chooses its key and bytes; this method does not commit independently.
    pub(crate) async fn stage_author_clock(
        &self,
        writer: &str,
        state: &MutationAuthorClockState,
    ) -> Result<(), SchemaError> {
        self.format.stage_author_clock(writer, state).await
    }

    pub fn retry_cache_status(&self) -> serde_json::Value {
        self.format.retry_cache_status()
    }

    /// Batch-read receipts in input order. Hits still need current-value checks.
    pub async fn get_idempotency_ids(
        &self,
        keys: &[String],
    ) -> Result<Vec<Option<lastdb_model::record_replacement::RetryReceipt>>, SchemaError> {
        self.format.idempotency_ids(keys).await.map_err(Into::into)
    }

    /// Native requests serialize bounded bucket replacement with record writes.
    pub async fn batch_put_idempotency(
        &self,
        entries: Vec<(String, lastdb_model::record_replacement::RetryReceipt)>,
    ) -> Result<(), SchemaError> {
        self.format
            .stage_idempotency(entries)
            .await
            .map_err(Into::into)
    }
}
