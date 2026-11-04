//! Record domain store.
//!
//! Supplies record values and read coordination to the format-owned main/file
//! writer. It has no raw store or namespace factory. External callers use
//! `DbOperations::records()` to reach these operations.
//!
//! Layout:
//! - [`construct`] — `RecordStore` constructors and builder hooks
//! - [`time_queue`] — the time-bucketed queue layout, reader and retirement
//! - [`records`], [`record_reads`], [`record_walk`] — record writes and reads
//! - [`schema_release`] — the release of the rows of a dropped schema
//! - [`record_files`] — the one file of a record, at a path made from its key
//! - [`read_scope`] — one memory and disk boundary for a read
//! - [`locks`] — write scopes and field key commit locks
//! - [`key_format`] — the key format marker of a new empty home
//! - [`erase_meters`] — the erase audit row

mod bounded_range;
mod construct;
mod delete_replay;
pub mod field_history;
pub use delete_replay::ReplayCutoffReport;
mod erase_meters;
mod key_comparison;
mod locks;
mod read_scope;
mod record_cap;
pub(crate) mod record_files;
mod record_reads;
mod record_spans;
mod record_walk;
mod records;
mod schema_release;
pub(crate) mod time_queue;
mod writer_watch;

pub use bounded_range::WalkStats;
pub(crate) use bounded_range::{RangeWalk, WalkTotals};
pub(crate) use field_history::RecordHistoryRows;
pub(crate) use record_reads::KeyedRecord;
pub(crate) use record_spans::{RecordBound, RecordSpan, SpanKey};
pub use record_walk::RECORD_WALK_ROWS;
pub(crate) use record_walk::{WalkWindow, PAGE_READ_KEYS};
pub(crate) use records::RecordChange;
pub use writer_watch::{WriterWatch, DEFAULT_QUIET_SECS, QUIET_SECS_ENV};

use crate::storage::disk_pack::PackRuntime;
use crate::storage::format::{read::ReadStore, records::RecordFormat};
use std::sync::Arc;

/// Record store for a home. One record at each primary key.
///
/// Backed by the `main` namespace.
#[derive(Clone)]
pub struct RecordStore {
    main_store: ReadStore,
    format: Arc<RecordFormat>,
    runtime: Arc<PackRuntime>,
    /// Validated once before serving. Only stopped-home maintenance can move
    /// this floor; replay cannot write its two metadata records.
    serving_replay_cutoff: Arc<std::sync::OnceLock<u64>>,
    /// Durable bundle resolver. `None` until `with_field_keys` attaches it.
    field_keys: Option<crate::db_operations::FieldKeyStore>,
    /// The clock of the field history expiry queue. Shared by clones.
    queue_clock: Arc<time_queue::QueueClock>,
    /// Wake expiry after a history policy activation attempt. Shared by clones.
    history_activation: Arc<tokio::sync::Notify>,
    /// Bounded range reads made by the admin walks since this process started.
    range_walks: Arc<WalkTotals>,
}

impl RecordStore {
    /// The clock of the field history expiry queue.
    pub(crate) fn queue_clock(&self) -> &time_queue::QueueClock {
        &self.queue_clock
    }

    /// Totals of the bounded range reads of the admin walks.
    pub(crate) fn range_walks(&self) -> &WalkTotals {
        &self.range_walks
    }

    pub(crate) fn key_codec_for_field_key(
        &self,
        field_key_id: &str,
    ) -> Result<lastdb_model::field_key_codec::FieldKeyCodec, crate::schema::SchemaError> {
        let codec = self
            .field_keys
            .as_ref()
            .map(|store| store.codec_for_field_key(field_key_id))
            .transpose()?
            .flatten();
        codec.ok_or_else(|| {
            crate::schema::SchemaError::InvalidData(
                "the typed field key has no admitted range profile".into(),
            )
        })
    }

    /// A read-only view with no raw-handle extraction or write methods.
    pub(crate) fn reader(&self) -> &ReadStore {
        &self.main_store
    }

    /// Flush all pending writes in the record namespace to durable storage.
    pub async fn flush(&self) -> Result<(), crate::storage::StorageError> {
        self.format.flush().await
    }
}
