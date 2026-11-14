//! Database operations coordinator, alternate-key surface, writer watch, and page scopes.

mod core {
    mod open;
}
pub mod record_store;
mod reports;
pub mod schema_store;
pub mod search_index;

pub use record_store::{RecordDeleteLedgerEntry, RecordStore};
pub use reports::{SchemaRecordKey, SchemaRecordKeysReport, SchemaStorage, SchemaStorageReport};
pub use schema_store::SchemaStore;

pub mod alternate_keys {
    pub use crate::storage::format::records::{AlternateEntry, MAX_ALTERNATE_ENTRIES};
    pub use lastdb_model::field_key_id::alternate_field_key_id as field_key;

    pub(crate) fn invalid(message: &str) -> crate::schema::SchemaError {
        crate::schema::SchemaError::InvalidData(message.into())
    }
}

use crate::platform::env_flag;
use crate::schema::types::{KeyValue, SchemaError};
use crate::storage::disk_pack::{
    transaction::{self, blocked::write_scopes},
    PackReadScope, PackRuntime,
};
use crate::storage::error::StorageResult;
use crate::storage::format::metadata::Metadata;
use chrono::Utc;
use sha2::{Digest, Sha256};
use std::cell::RefCell;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct WriterWatch {
    mark: u64,
    last_other: Option<Instant>,
    quiet: Duration,
}

impl Default for WriterWatch {
    fn default() -> Self {
        let secs = env_flag::var_or("LASTDB_MAINTENANCE_QUIET_SECS", 60);
        Self {
            mark: write_scopes(),
            last_other: None,
            quiet: Duration::from_secs(secs),
        }
    }
}

impl WriterWatch {
    pub async fn measure<F: std::future::Future>(work: F) -> (F::Output, Duration, u64) {
        let (output, cost) = transaction::blocked::measure(work).await;
        (output, cost.blocked, cost.scopes)
    }

    pub fn writer_present(&mut self, own: u64) -> bool {
        let now = write_scopes();
        let others = now.saturating_sub(self.mark).saturating_sub(own);
        self.mark = now;
        if others > 0 {
            self.last_other = Some(Instant::now());
        }
        others > 0 || self.last_other.is_some_and(|at| at.elapsed() < self.quiet)
    }

    pub fn rest_may_end(&mut self) -> bool {
        !self.writer_present(0)
    }
}

impl RecordDeleteLedgerEntry {
    pub fn erasure(verb: &str, caller: &str, schema: &str, described_key: &str) -> Self {
        let mut h = Sha256::new();
        h.update(schema.as_bytes());
        h.update([0u8]);
        h.update(described_key.as_bytes());
        Self {
            verb: verb.to_owned(),
            at: Utc::now().to_rfc3339(),
            caller: caller.to_owned(),
            schema: schema.to_owned(),
            key_fingerprint: format!("{:x}", h.finalize()),
            records_erased: 0,
            file_records_erased: 0,
        }
    }
}

#[derive(Clone)]
pub struct DbOperations {
    runtime: Arc<PackRuntime>,
    schema_store: SchemaStore,
    record_store: RecordStore,
    metadata: Metadata,
}

impl DbOperations {
    pub fn schemas(&self) -> &SchemaStore {
        &self.schema_store
    }

    pub fn records(&self) -> &RecordStore {
        &self.record_store
    }

    pub async fn read_scope(&self) -> StorageResult<PackReadScope> {
        PackReadScope::for_runtime(&self.runtime).await
    }

    pub(crate) fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    pub fn native_runtime(&self) -> Arc<PackRuntime> {
        Arc::clone(&self.runtime)
    }

    #[must_use]
    pub fn batch_operation_limit(&self) -> usize {
        self.runtime.batch_operation_limit()
    }

    pub async fn flush(&self) -> Result<(), SchemaError> {
        Ok(self.metadata.flush().await?)
    }
}

tokio::task_local! {
    static PAGE_END: RefCell<PageEnd>;
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PageEnd {
    pub resume: Option<PageResume>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PageResume {
    Key(KeyValue),
}

pub fn page_can_resume() -> bool {
    PAGE_END.try_with(|_| ()).is_ok()
}

pub fn note_page_resume(resume: PageResume) {
    let _ = PAGE_END.try_with(|end| end.borrow_mut().resume = Some(resume));
}

pub async fn with_query_page_tally<F: std::future::Future>(fut: F) -> (F::Output, PageEnd) {
    PAGE_END
        .scope(RefCell::new(PageEnd::default()), async move {
            (fut.await, PAGE_END.with(RefCell::take))
        })
        .await
}
