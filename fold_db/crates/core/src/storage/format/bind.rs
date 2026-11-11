//! The one trusted assembly of this concrete home format's writer capabilities.
//!
//! Namespace names, runtime and key context come from one home here. Providers
//! receive typed writers/read views; child raw constructors are format-private.

use super::{change_feed::ChangeFeed, metadata::Metadata};
use super::{records::RecordFormat, schemas::SchemaFormat};
use crate::storage::disk_pack::{transaction, PackNamespacedStore, PackRuntime};
use crate::storage::error::StorageResult;
use std::sync::Arc;

pub(crate) struct BoundFormat {
    pub(crate) runtime: Arc<PackRuntime>,
    pub(crate) schemas: SchemaFormat,
    pub(crate) records: RecordFormat,
    pub(crate) metadata: Metadata,
    pub(crate) feed: ChangeFeed,
}

pub(crate) async fn open(
    store: PackNamespacedStore,
    content_key: [u8; 32],
) -> StorageResult<BoundFormat> {
    let runtime = store.disk_pack_runtime();
    // Preserve the existing namespace-open order. Files remain lazy.
    let main = store.open_namespace(super::MAIN_NAMESPACE).await?;
    let metadata = store.open_namespace(super::METADATA_NAMESPACE).await?;
    let states = store.open_namespace(super::SCHEMA_STATES_NAMESPACE).await?;
    let schemas = store.open_namespace(super::SCHEMAS_NAMESPACE).await?;
    let feed = store.open_namespace(super::CHANGE_FEED_NAMESPACE).await?;
    let schemas = SchemaFormat::new(schemas, states, Arc::clone(&runtime));
    let metadata = Metadata::new(metadata, Arc::clone(&runtime));
    let records = RecordFormat::new(
        main,
        store.clone(),
        Arc::clone(&runtime),
        content_key,
        metadata.clone(),
    );
    transaction::scope(Arc::clone(&runtime), async {
        records.require_current_key_format().await?;
        crate::storage::content_key_proof::verify_stored(&store, &content_key).await?;
        Ok::<_, crate::storage::StorageError>(())
    })
    .await?;
    runtime.finish_fresh_initialization().await?;
    Ok(BoundFormat {
        schemas,
        records,
        metadata,
        feed: ChangeFeed::new(feed, Arc::clone(&runtime)),
        runtime,
    })
}
