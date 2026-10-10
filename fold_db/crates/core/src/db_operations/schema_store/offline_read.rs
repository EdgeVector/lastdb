//! Read-only open of the schema catalog for an offline tool.
//!
//! An offline tool has no `DbOperations`. Building one runs boot recovery
//! that can write. This open touches the three catalog namespaces only, and
//! the strict catalog read it serves never writes.

use super::SchemaStore;
use crate::storage::traits::NamespacedStore;
use crate::storage::{StorageError, TypedKvStore};
use std::sync::Arc;

/// Key prefix of the durable drop receipts in `schema_states`.
pub const SCHEMA_DROP_RECEIPT_KEY_PREFIX: &str =
    super::catalog_keys::SCHEMA_DROP_RECEIPT_KEY_PREFIX;

impl SchemaStore {
    /// Open the catalog namespaces of `store` for reads.
    ///
    /// `catalog_unwrap_key` is the account content key. With it, a catalog
    /// row that an old writer sealed can open. Without it, such a row fails
    /// the strict read, and the caller must stop.
    pub async fn open_for_offline_read(
        store: &dyn NamespacedStore,
        catalog_unwrap_key: Option<[u8; 32]>,
    ) -> Result<Self, StorageError> {
        let schemas = store.open_namespace("schemas").await?;
        let states = store.open_namespace("schema_states").await?;
        let superseded = store.open_namespace("schema_superseded_by").await?;
        let catalog = Self::new(
            Arc::new(TypedKvStore::new(schemas)),
            Arc::new(TypedKvStore::new(states)),
            Arc::new(TypedKvStore::new(superseded)),
        );
        Ok(match catalog_unwrap_key {
            Some(key) => catalog.with_catalog_unwrap_key(key),
            None => catalog,
        })
    }
}
