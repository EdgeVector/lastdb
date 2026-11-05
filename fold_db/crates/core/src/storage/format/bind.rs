//! The one trusted assembly of this concrete home format's writer capabilities.
//!
//! Namespace names, runtime and key context come from one home here. Providers
//! receive typed writers/read views; child raw constructors are format-private.

use super::{change_feed::ChangeFeed, field_keys::FieldKeyFormat, metadata::Metadata};
use super::{records::RecordFormat, schemas::SchemaFormat};
use crate::storage::disk_pack::{transaction, PackRuntime};
use crate::storage::error::StorageResult;
use crate::storage::NamespacedStore;
use std::sync::Arc;

pub(crate) struct BoundFormat {
    pub(crate) runtime: Arc<PackRuntime>,
    pub(crate) schemas: SchemaFormat,
    pub(crate) field_keys: FieldKeyFormat,
    pub(crate) records: RecordFormat,
    pub(crate) metadata: Metadata,
    pub(crate) feed: ChangeFeed,
}

pub(crate) async fn open(
    store: Arc<dyn NamespacedStore>,
    content_key: Option<[u8; 32]>,
    wrapping_key: Option<[u8; 32]>,
) -> StorageResult<BoundFormat> {
    let runtime = store.disk_pack_runtime();
    // Preserve the existing namespace-open order. Files remain lazy.
    let main = store.open_namespace(super::MAIN_NAMESPACE).await?;
    let metadata = store.open_namespace(super::METADATA_NAMESPACE).await?;
    let states = store.open_namespace(super::SCHEMA_STATES_NAMESPACE).await?;
    let schemas = store.open_namespace(super::SCHEMAS_NAMESPACE).await?;
    let fields = store.open_namespace(super::FIELD_KEYS_NAMESPACE).await?;
    let feed = store.open_namespace(super::CHANGE_FEED_NAMESPACE).await?;
    let schemas = SchemaFormat::new(schemas, states, Arc::clone(&runtime));
    let field_keys = FieldKeyFormat::new(fields, Arc::clone(&runtime), wrapping_key);
    let metadata = Metadata::new(metadata, Arc::clone(&runtime));
    let records = RecordFormat::new(
        main,
        Arc::clone(&store),
        Arc::clone(&runtime),
        content_key,
        metadata.clone(),
    );
    transaction::scope(Arc::clone(&runtime), async {
        records.require_current_key_format().await?;
        if let Some(key) = content_key.or(wrapping_key) {
            crate::storage::content_key_proof::verify_stored(store.as_ref(), &key).await?;
        }
        Ok::<_, crate::storage::StorageError>(())
    })
    .await?;
    runtime.finish_fresh_initialization().await?;
    Ok(BoundFormat {
        schemas,
        field_keys,
        records,
        metadata,
        feed: ChangeFeed::new(feed, Arc::clone(&runtime)),
        runtime,
    })
}
