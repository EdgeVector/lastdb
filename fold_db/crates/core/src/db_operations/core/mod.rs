use super::record_store::RecordStore;
use super::schema_store::SchemaStore;
use crate::schema::SchemaError;
use crate::storage::disk_pack::PackRuntime;
use crate::storage::format::metadata::Metadata;
use std::sync::Arc;

mod open;
mod page_tally;

pub use page_tally::{
    note_page_key_forms, note_page_resume, page_can_resume, with_query_page_tally,
    with_query_tally, KeyForm, PageEnd, PageKeyForms, PageResume,
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
    /// Main namespace: records, alternate keys, and delete metadata.
    record_store: RecordStore,
    /// Metadata namespace for mutation author clocks
    metadata: Metadata,
}

impl DbOperations {
    // ===== Domain store accessors (public) =====

    /// Access the schema domain store.
    pub fn schemas(&self) -> &SchemaStore {
        &self.schema_store
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

    /// Access the typed author-clock metadata format.
    pub(crate) fn metadata(&self) -> &Metadata {
        &self.metadata
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
        self.record_store.flush().await?;
        self.metadata.flush().await?;
        Ok(())
    }
}
