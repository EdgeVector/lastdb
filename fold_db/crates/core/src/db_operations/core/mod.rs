use super::change_feed::ChangeFeedStore;
use super::metadata_store::MetadataStore;
use super::record_store::RecordStore;
use super::schema_store::SchemaStore;
use crate::schema::SchemaError;
use crate::storage::disk_pack::PackRuntime;
use crate::storage::field_key_store::FieldKeyStore;
use std::sync::Arc;

mod open;
mod page_tally;

pub use page_tally::{
    note_page_key_forms, note_page_resume, note_walk_positions, page_can_resume,
    with_query_page_tally, with_query_tally, KeyForm, PageEnd, PageKeyForms, PageResume,
};

/// Database operations with pluggable storage backend
///
/// Uses the storage abstraction layer (Last Store).
///
/// The format owns persistence. Domain stores hold typed writer/read rights;
/// this coordinator exposes no raw namespace or factory. External callers reach them through
/// `schemas()`, `records()`, and `metadata()`.
#[derive(Clone)]
pub struct DbOperations {
    /// Native coordination/status capability, without a namespace factory.
    runtime: Arc<PackRuntime>,
    /// Schema / schema-state namespaces
    schema_store: SchemaStore,
    /// per-field-key key bundles and node wraps.
    field_keys: FieldKeyStore,
    /// Main namespace: records, alternate keys, field history.
    record_store: RecordStore,
    /// Metadata namespace, including the mutation retry cache
    metadata_store: MetadataStore,
    /// Durable app change feed. Product rows remain authoritative. It rides
    /// cloud backup sealed (`ENC:`).
    change_feed: ChangeFeedStore,
}

impl DbOperations {
    // ===== Domain store accessors (public) =====

    /// Access the schema domain store.
    pub fn schemas(&self) -> &SchemaStore {
        &self.schema_store
    }

    /// Access durable per-field-key key bundles and node wraps.
    pub fn field_keys(&self) -> &FieldKeyStore {
        &self.field_keys
    }

    /// Access the durable node-local app change feed.
    pub fn change_feed(&self) -> &ChangeFeedStore {
        &self.change_feed
    }

    /// Access the record store.
    pub fn records(&self) -> &RecordStore {
        &self.record_store
    }

    /// Logical queries require one captured core state and matching disk root.
    pub async fn read_scope(
        &self,
    ) -> crate::storage::error::StorageResult<crate::storage::disk_pack::PackReadScope> {
        crate::storage::disk_pack::PackReadScope::for_runtime(&self.runtime).await
    }

    /// Access the metadata domain store (including the retry cache).
    pub fn metadata(&self) -> &MetadataStore {
        &self.metadata_store
    }

    /// Native status, cleanup and request coordination; no logical raw writer.
    pub fn native_runtime(&self) -> Arc<PackRuntime> {
        Arc::clone(&self.runtime)
    }

    /// Most operations that one complete batch holds on the native home.
    #[must_use]
    pub fn batch_operation_limit(&self) -> usize {
        self.runtime.batch_operation_limit()
    }

    /// Flush all pending writes to durable storage
    pub async fn flush(&self) -> Result<(), SchemaError> {
        self.schema_store.flush().await?;
        self.field_keys.flush().await?;
        self.record_store.flush().await?;
        self.metadata_store.flush().await?;
        Ok(())
    }
}
