//! Public-key domain store.
//!
//! Owns the `public_keys` namespace for the system-wide node public key.
//! External callers use the `DbOperations` public-key delegators.

use std::sync::Arc;

use crate::constants::SINGLE_PUBLIC_KEY_ID;
use crate::schema::SchemaError;
use crate::security::PublicKeyInfo;
use crate::storage::traits::KvStore;
use crate::storage::TypedKvStore;

/// Domain store for system public-key persistence.
#[derive(Clone)]
pub struct PublicKeyStore {
    public_keys_store: Arc<TypedKvStore<dyn KvStore>>,
}

impl PublicKeyStore {
    pub(crate) fn new(public_keys_store: Arc<TypedKvStore<dyn KvStore>>) -> Self {
        Self { public_keys_store }
    }

    /// Flush public-key writes to durable storage.
    pub(crate) async fn flush(&self) -> Result<(), SchemaError> {
        self.public_keys_store.inner().flush().await?;
        Ok(())
    }

    /// Gets the system-wide public key
    pub async fn get_system_public_key(&self) -> Result<Option<PublicKeyInfo>, SchemaError> {
        Ok(self
            .public_keys_store
            .get_item(SINGLE_PUBLIC_KEY_ID)
            .await?)
    }

    /// Stores the system-wide public key
    pub async fn store_system_public_key(
        &self,
        key_info: &PublicKeyInfo,
    ) -> Result<(), SchemaError> {
        self.public_keys_store
            .put_item(SINGLE_PUBLIC_KEY_ID, key_info)
            .await?;
        self.public_keys_store.inner().flush().await?;
        Ok(())
    }

    /// Deletes the system-wide public key
    pub async fn delete_system_public_key(&self) -> Result<bool, SchemaError> {
        Ok(self
            .public_keys_store
            .delete_item(SINGLE_PUBLIC_KEY_ID)
            .await?)
    }
}
