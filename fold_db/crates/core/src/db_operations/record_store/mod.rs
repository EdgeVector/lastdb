//! Record domain store.
//!
//! Owns the main storage namespace where the records of a home, their
//! alternate keys, field history and conflict rows live. External callers use
//! `DbOperations::records()` to reach these operations.
//!
//! Layout:
//! - [`construct`] — `RecordStore` constructors and builder hooks
//! - [`counter`] — the row commit, the offline recount and the reference verifier
//! - [`retention`] — queue and release of kept conflict records
//! - [`time_queue`] — the time-bucketed queue layout, reader and retirement
//! - [`records`], [`record_reads`], [`record_walk`] — record writes and reads
//! - [`schema_release`] — the release of the rows of a dropped schema
//! - [`record_files`] — the one file of a record, at a path made from its key
//! - [`read_scope`] — one memory and disk boundary for a read
//! - [`locks`] — write scopes and field key commit locks
//! - [`key_format`] — the key format marker of a new empty home
//! - [`erase_meters`] — the erase audit row

mod bounded_range;
mod conflict_record;
mod construct;
pub(crate) mod counter;
mod delete_replay;
pub mod field_history;
pub use delete_replay::ReplayCutoffReport;
mod erase_meters;
mod file_blob_seal;
mod key_comparison;
pub(crate) mod key_format;
mod locks;
mod read_scope;
mod record_cap;
mod record_conflicts;
pub(crate) mod record_files;
mod record_reads;
mod record_spans;
mod record_walk;
mod records;
mod retention;
mod schema_release;
pub(crate) mod time_queue;
mod writer_watch;

pub use bounded_range::WalkStats;
pub(crate) use bounded_range::{RangeWalk, WalkTotals};
pub use counter::{RecountOptions, RecountReport, VerifyReport, VerifyRequest, VerifySummary};
pub(crate) use field_history::RecordHistoryRows;
pub use record_conflicts::{RecordConflictEntry, RecordFieldConflicts};
pub(crate) use record_reads::KeyedRecord;
pub(crate) use record_spans::{RecordBound, RecordSpan, SpanKey};
pub use record_walk::RECORD_WALK_ROWS;
pub(crate) use record_walk::{WalkWindow, PAGE_READ_KEYS};
pub(crate) use records::{record_scope, RecordChange};
pub use retention::ConflictRetentionReport;
pub(crate) use retention::{RetentionCursor, StepOutcome};
pub use writer_watch::{WriterWatch, DEFAULT_QUIET_SECS, QUIET_SECS_ENV};

use crate::storage::{
    traits::{KvStore, NamespacedStore},
    TypedKvStore,
};
use std::sync::Arc;

/// Record store for a home. One record at each primary key.
///
/// Backed by the `main` namespace.
#[derive(Clone)]
pub struct RecordStore {
    main_store: Arc<TypedKvStore<dyn KvStore>>,
    /// Validated once before serving. Only stopped-home maintenance can move
    /// this floor; replay cannot write its two metadata records.
    serving_replay_cutoff: Arc<std::sync::OnceLock<u64>>,
    /// Owner namespace factory for physical-plane maintenance.
    namespaced_store: Arc<dyn NamespacedStore>,
    /// The account key that seals a local CAS file blob (see
    /// `file_blob_seal.rs`). Record rows do not use it: a record row is plain
    /// JSON, and the block encryption of each run file protects it at rest.
    content_key: Option<[u8; 32]>,
    /// Durable bundle resolver. `None` until `with_field_keys` attaches it.
    field_keys: Option<crate::db_operations::FieldKeyStore>,
    /// Per-field-key guards. Schema delete and schema share take the guards
    /// of every field key of a schema in sorted order, so the two cannot
    /// interleave on one field key or deadlock.
    field_key_commit_locks:
        Arc<std::sync::Mutex<std::collections::HashMap<String, Arc<tokio::sync::RwLock<()>>>>>,
    /// The retention window of kept conflict records and the janitor report.
    retention: Arc<retention::RetentionState>,
    /// Wake expiry after a history policy activation attempt. Shared by clones.
    history_activation: Arc<tokio::sync::Notify>,
    /// The verifier: the run in progress and the last result. Memory only.
    counter_verify: Arc<counter::VerifyState>,
    /// Bounded range reads made by the admin walks since this process started.
    range_walks: Arc<WalkTotals>,
    /// Sync device id of this node. Empty until a sync engine attaches. The
    /// field merge treats a write from any other non-empty device as a write
    /// from a peer. See [`RecordStore::local_origin_device`].
    local_origin_device: Arc<std::sync::RwLock<String>>,
}

impl RecordStore {
    /// The retention window and the report of kept conflict records.
    pub(crate) fn retention_state(&self) -> &retention::RetentionState {
        &self.retention
    }

    /// The verifier state: the run in progress and the last result.
    pub(crate) fn counter_verify(&self) -> &counter::VerifyState {
        &self.counter_verify
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

    /// Access the underlying typed KV store. Crate-internal helper for
    /// code paths that need generic typed access (field loading, conflicts,
    /// org purge).
    pub(crate) fn raw(&self) -> &Arc<TypedKvStore<dyn KvStore>> {
        &self.main_store
    }

    /// Flush all pending writes in the record namespace to durable storage.
    pub async fn flush(&self) -> Result<(), crate::storage::StorageError> {
        self.main_store.inner().flush().await
    }
}
