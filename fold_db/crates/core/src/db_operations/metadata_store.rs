//! Metadata domain store.
//!
//! Owns the `metadata` and `idempotency` namespaces.
//! External callers reach these via `DbOperations::metadata()`.
//!
//! Responsibilities:
//! - Node-level metadata (e.g. `node_id`)
//! - Idempotency cache for mutations

use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::Serialize;
use uuid::Uuid;

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

    // ===== Node-id =====

    /// Retrieves or generates and persists the node identifier.
    pub async fn get_node_id(&self) -> Result<String, SchemaError> {
        match self.metadata_store.get_item::<String>("node_id").await {
            Ok(Some(id)) if !id.is_empty() => {
                return Ok(id);
            }
            Ok(Some(_) | None) => {}
            Err(StorageError::SerializationError(e)) => {
                tracing::warn!(
                    "Failed to deserialize node_id (possibly old format): {}, creating new",
                    e
                );
            }
            Err(e) => return Err(e.into()),
        }

        let new_id = Uuid::new_v4().to_string();
        self.set_node_id(&new_id).await?;
        Ok(new_id)
    }

    /// Sets the node identifier
    pub async fn set_node_id(&self, node_id: &str) -> Result<(), SchemaError> {
        self.metadata_store
            .put_item("node_id", &node_id.to_string())
            .await?;
        self.metadata_store.inner().flush().await?;
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

    /// Check exact keys in bounded batches, without decoding or changing guards.
    pub(crate) async fn contains_any(&self, keys: &[String]) -> Result<bool, SchemaError> {
        // Match the native get-many key ceiling without adding a total-key cap.
        for batch in keys.chunks(10_000) {
            let values = self
                .metadata_store
                .inner()
                .get_many(batch.iter().map(|key| key.as_bytes().to_vec()).collect())
                .await?;
            if values.len() != batch.len() {
                return Err(SchemaError::InvalidData(
                    "metadata presence read returned an incomplete key batch".into(),
                ));
            }
            if values.iter().any(Option::is_some) {
                return Ok(true);
            }
        }
        Ok(false)
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

    /// Delete one metadata value and flush the namespace before return.
    pub async fn delete_typed_durable(&self, key: &str) -> Result<(), SchemaError> {
        self.metadata_store
            .delete_item(key)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("metadata delete {key}: {e}")))?;
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
