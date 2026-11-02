//! Metadata domain store.
//!
//! Owns the `metadata` and `idempotency` namespaces.
//! External callers reach these via `DbOperations::metadata()`.
//!
//! Responsibilities:
//! - Idempotency cache for mutations

use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::Serialize;

mod idempotency;

use crate::schema::SchemaError;
use crate::storage::traits::KvStore;
use crate::storage::StorageError;
use crate::storage::TypedKvStore;

/// Domain store for node metadata / idempotency persistence.
#[derive(Clone)]
pub struct MetadataStore {
    metadata_store: Arc<TypedKvStore<dyn KvStore>>,
    idempotency_store: Arc<TypedKvStore<dyn KvStore>>,
}

impl MetadataStore {
    pub(crate) fn new(
        metadata_store: Arc<TypedKvStore<dyn KvStore>>,
        idempotency_store: Arc<TypedKvStore<dyn KvStore>>,
    ) -> Self {
        Self {
            metadata_store,
            idempotency_store,
        }
    }

    /// Flush every metadata-owned namespace to durable storage.
    pub(crate) async fn flush(&self) -> Result<(), SchemaError> {
        self.metadata_store.inner().flush().await?;
        self.idempotency_store.inner().flush().await?;
        Ok(())
    }

    /// Point-get one JSON value from the metadata namespace.
    ///
    /// Missing key → `Ok(None)`. This is not a prefix walk.
    pub async fn get_typed<T: DeserializeOwned + Send + Sync>(
        &self,
        key: &str,
    ) -> Result<Option<T>, SchemaError> {
        self.metadata_store
            .get_item::<T>(key)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("metadata get {key}: {e}")))
    }

    /// Point-put one JSON value into the metadata namespace.
    ///
    /// Does not flush: LastStore's own persist cadence makes the row durable,
    /// matching the keep-small meter write.
    pub async fn put_typed<T: Serialize + Send + Sync>(
        &self,
        key: &str,
        value: &T,
    ) -> Result<(), SchemaError> {
        self.metadata_store
            .put_item(key, value)
            .await
            .map_err(|error| match error {
                // A capacity refusal keeps its type. The author-clock queue and the
                // HTTP layer answer it with a retry hint, not as an invalid payload.
                StorageError::WriteResourceLimit { detail } => {
                    SchemaError::WriteResourceLimit { detail }
                }
                other => SchemaError::InvalidData(format!("metadata put {key}: {other}")),
            })
    }

    /// Point-put one JSON value and flush the metadata namespace before return.
    pub async fn put_typed_durable<T: Serialize + Send + Sync>(
        &self,
        key: &str,
        value: &T,
    ) -> Result<(), SchemaError> {
        self.put_typed(key, value).await?;
        self.metadata_store.inner().flush().await?;
        Ok(())
    }

    // ===== Idempotency store =====

    /// Read bounded cache limits and old-cache cleanup counters, without keys or values.
    pub async fn retry_cache_status(&self) -> Result<serde_json::Value, SchemaError> {
        idempotency::status(&self.idempotency_store)
            .await
            .map_err(Into::into)
    }

    /// Batch-read the bounded retry cache, preserving input order and scope.
    pub async fn get_idempotency_ids(
        &self,
        keys: &[String],
    ) -> Result<Vec<Option<String>>, SchemaError> {
        idempotency::get_many(&self.idempotency_store, keys)
            .await
            .map_err(Into::into)
    }

    /// Store retry receipts in 256 buckets, at most 32 entries and 8 KiB each.
    pub async fn batch_put_idempotency(
        &self,
        entries: Vec<(String, String)>,
    ) -> Result<(), SchemaError> {
        idempotency::put(&self.idempotency_store, entries)
            .await
            .map_err(Into::into)
    }
}
