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
            .map_err(|e| SchemaError::InvalidData(format!("metadata put {key}: {e}")))
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

    /// Retrieve an item from the idempotency store by key.
    pub async fn get_idempotency_item<T: DeserializeOwned + Send + Sync>(
        &self,
        key: &str,
    ) -> Result<Option<T>, SchemaError> {
        self.idempotency_store
            .get_item::<T>(key)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("Failed to get idempotency item: {e}")))
    }

    /// Batch store idempotency entries (`(key, uuid)` pairs).
    pub async fn batch_put_idempotency(
        &self,
        entries: Vec<(String, String)>,
    ) -> Result<(), SchemaError> {
        self.idempotency_store
            .batch_put_items(entries)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("Idempotency store failed: {e}")))
    }
}
