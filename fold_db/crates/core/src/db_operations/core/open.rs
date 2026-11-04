//! Logical store construction consumes only format-bound capabilities.

use super::super::change_feed::ChangeFeedStore;
use super::super::metadata_store::MetadataStore;
use super::super::record_store::RecordStore;
use super::super::schema_store::SchemaStore;
use super::DbOperations;
use crate::storage::field_key_store::FieldKeyStore;
use crate::storage::format::bind::{self, BoundFormat};
use crate::storage::NamespacedStore;
use std::sync::Arc;

impl DbOperations {
    /// Trusted native bootstrap; ordinary components receive logical stores.
    pub async fn from_namespaced_store(
        store: Arc<dyn NamespacedStore>,
    ) -> Result<Self, crate::storage::StorageError> {
        Self::from_namespaced_store_with_content_and_field_keys(store, None, None).await
    }

    /// Bind one concrete format, including the account content/wrapping context.
    pub async fn from_namespaced_store_with_content_and_field_keys(
        store: Arc<dyn NamespacedStore>,
        content_key: Option<[u8; 32]>,
        field_key_wrap_key: Option<[u8; 32]>,
    ) -> Result<Self, crate::storage::StorageError> {
        let BoundFormat {
            runtime,
            schemas,
            field_keys,
            records,
            metadata,
            feed,
        } = bind::open(store, content_key, field_key_wrap_key).await?;
        let field_keys = FieldKeyStore::new(field_keys);
        let record_store = RecordStore::new(records).with_field_keys(field_keys.clone());
        Ok(Self {
            runtime,
            schema_store: SchemaStore::new(schemas),
            field_keys,
            record_store,
            metadata_store: MetadataStore::new(metadata),
            change_feed: ChangeFeedStore::new(feed).await?,
        })
    }

    /// Open typed operations for explicit stopped-home maintenance, without
    /// starting serving workers. Native ownership and format checks still run.
    pub async fn from_stopped_native_data(
        data: &std::path::Path,
        node_key: &[u8; 32],
    ) -> Result<Self, crate::storage::StorageError> {
        let store = crate::storage::disk_pack::PackNamespacedStore::open_existing(data, node_key)?;
        Self::from_namespaced_store(Arc::new(store)).await
    }
}
