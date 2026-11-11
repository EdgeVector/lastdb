use super::record_store::RecordStore;
use super::schema_store::SchemaStore;
use crate::schema::types::KeyValue;
use crate::schema::SchemaError;
use crate::storage::disk_pack::PackRuntime;
use crate::storage::format::metadata::Metadata;
use std::cell::RefCell;
use std::sync::Arc;

mod open;

/// Database operations coordinator over typed domain stores.
#[derive(Clone)]
pub struct DbOperations {
    runtime: Arc<PackRuntime>,
    schema_store: SchemaStore,
    record_store: RecordStore,
    metadata: Metadata,
}

impl DbOperations {
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

    /// Flush all pending writes to durable storage.
    pub async fn flush(&self) -> Result<(), SchemaError> {
        self.schema_store.flush().await?;
        self.record_store.flush().await?;
        self.metadata.flush().await?;
        Ok(())
    }
}

tokio::task_local! {
    static PAGE_END: RefCell<PageEnd>;
}

/// Where one resumable page ended when key-examination budget ran out early.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PageEnd {
    pub resume: Option<PageResume>,
}

/// Position after which the next page of a resumable read starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PageResume {
    Key(KeyValue),
}

/// True when the caller installed a resumable page scope (`with_query_page_tally`).
pub fn page_can_resume() -> bool {
    PAGE_END.try_with(|_| ()).is_ok()
}

/// Record where an early-terminated page stopped, if a page scope is installed.
pub fn note_page_resume(resume: PageResume) {
    let _ = PAGE_END.try_with(|end| end.borrow_mut().resume = Some(resume));
}

/// Run `fut` in a resumable page scope, returning its output plus [`PageEnd`].
pub async fn with_query_page_tally<F>(fut: F) -> (F::Output, PageEnd)
where
    F: std::future::Future,
{
    PAGE_END
        .scope(RefCell::new(PageEnd::default()), async move {
            let out = fut.await;
            (out, PAGE_END.with(RefCell::take))
        })
        .await
}
