//! Logical store construction consumes only format-bound capabilities.

use super::super::{record_store::RecordStore, schema_store::SchemaStore, DbOperations};
use crate::storage::disk_pack::PackNamespacedStore;
use crate::storage::error::StorageError;
use crate::storage::format::bind::{self, BoundFormat};

impl DbOperations {
    /// Bind one concrete format with the node key.
    pub async fn from_namespaced_store_with_content_and_field_keys(
        store: PackNamespacedStore,
        node_key: [u8; 32],
    ) -> Result<Self, StorageError> {
        let BoundFormat {
            runtime,
            schemas,
            records,
            metadata,
        } = bind::open(store, node_key).await?;
        Ok(Self {
            runtime,
            schema_store: SchemaStore::new(schemas),
            record_store: RecordStore::new(records, node_key),
            metadata,
        })
    }
}
