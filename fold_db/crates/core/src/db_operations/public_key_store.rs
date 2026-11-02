//! Public-key domain store.
//!
//! Owns the `public_keys` namespace. Open and flush stay with the other stores.

use std::sync::Arc;

use crate::schema::SchemaError;
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
}
