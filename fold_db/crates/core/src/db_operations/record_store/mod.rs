//! Record domain store.
//!
//! Owns the main storage namespace where the records of a format 7 home, their
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
//! - [`read_scope`] — one memory and disk boundary for a read
//! - [`locks`] — write scopes, field key commit and blob liveness locks
//! - [`blob_counter`] — the blob counter, its dwell queue and the dwell pass
//! - [`key_format`] — the key format marker of a new empty home
//! - [`erase_meters`], [`keep_small`] — maintenance passes

pub(crate) mod blob_counter;
mod bounded_range;
mod conflict_record;
mod construct;
pub(crate) mod counter;
mod delete_replay;
pub mod field_history;
pub use delete_replay::ReplayCutoffReport;
mod erase_meters;
mod file_blob_seal;
mod keep_small;
mod key_comparison;
pub(crate) mod key_format;
mod locks;
mod read_scope;
mod record_cap;
mod record_conflicts;
mod record_reads;
mod record_spans;
mod record_walk;
mod records;
mod retention;
mod schema_release;
pub(crate) mod time_queue;
mod writer_watch;

pub use blob_counter::{
    BlobCounter, BlobCounterReport, BlobCounterView, BlobRecountReport, DwellCursor, DwellGuard,
    DwellStep, NetStep, DWELL_CHUNK,
};
pub use bounded_range::WalkStats;
pub(crate) use bounded_range::{RangeWalk, WalkTotals};
pub use counter::{
    RecountOptions, RecountReport, VerifyRefsReport, VerifyReport, VerifyRequest, VerifySummary,
};
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
use std::{
    collections::HashSet,
    hash::{DefaultHasher, Hash, Hasher},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use super::keep_small::{
    KeepSmallMeters, KeepSmallSchemaShard, MeterDomainTrust, MeterTrustPayload,
};

async fn lock_target_stripes(
    locks: &[Arc<tokio::sync::Mutex<()>>],
    target_ids: &[String],
) -> Vec<tokio::sync::OwnedMutexGuard<()>> {
    if target_ids.is_empty() {
        return Vec::new();
    }
    let mut stripes = target_ids
        .iter()
        .map(|target| {
            let mut hasher = DefaultHasher::new();
            target.hash(&mut hasher);
            hasher.finish() as usize % locks.len()
        })
        .collect::<Vec<_>>();
    stripes.sort_unstable();
    stripes.dedup();
    let mut guards = Vec::with_capacity(stripes.len());
    for stripe in stripes {
        guards.push(Arc::clone(&locks[stripe]).lock_owned().await);
    }
    guards
}

/// Record store for a format 7 home. One record at each primary key.
///
/// Backed by the `main` namespace.
#[derive(Clone)]
pub struct RecordStore {
    main_store: Arc<TypedKvStore<dyn KvStore>>,
    /// Validated once before serving. Only stopped-home maintenance can move
    /// this floor; replay cannot write its two metadata records.
    serving_replay_cutoff: Arc<std::sync::OnceLock<u64>>,
    /// Owner namespace factory for physical-plane maintenance.
    ///
    /// Legacy constructors leave this unset. Production construction and the
    /// drain tests attach it explicitly.
    namespaced_store: Option<Arc<dyn NamespacedStore>>,
    /// The account key that seals a local CAS file blob (see
    /// `file_blob_seal.rs`). Record rows do not use it: a record row is plain
    /// JSON, and the block encryption of each run file protects it at rest.
    content_key: Option<[u8; 32]>,
    /// Durable bundle resolver. `None` keeps legacy/test construction intact.
    field_keys: Option<crate::db_operations::FieldKeyStore>,
    /// per-field-key commit guards: the SHORT critical section that makes
    /// (read `moc:` → build items → commit batch) atomic for one field key.
    ///
    /// The field key order log is a **dense** sequence — `mord:{M}:{seq}` with
    /// `moc:{M}` as its length — so the base is read at persist time and
    /// re-stamped in the same batch. Two writers interleaved between that read
    /// and that put would stamp the same seq and the second would clobber the
    /// first, silently: both `mk:` records survive, and only `SampleN`, which
    /// walks the log, sees the hole. The field key header (`version`,
    /// `updated_at`) rides the same batch and is field-key-wide state too, so it
    /// is covered by the same guard rather than a second one.
    ///
    /// This is deliberately NOT the write gate. The long-held gate is
    /// `MutationManager::acquire_field_key_write_locks`, now per-`(field key,
    /// hash, range)`; this one is taken *inside* the durable put and released
    /// as soon as the batch lands. Lock order is always key-then-field-key, so
    /// the two cannot deadlock.
    field_key_commit_locks:
        Arc<std::sync::Mutex<std::collections::HashMap<String, Arc<tokio::sync::RwLock<()>>>>>,
    /// Incremental live-budget / churn meters. Point-read cheap.
    keep_small: Arc<KeepSmallMeters>,
    /// Metadata namespace used to persist [`KeepSmallMeters`] across reopen.
    keep_small_persist: Option<Arc<TypedKvStore<dyn KvStore>>>,
    /// Debounce clock for the keep-small projection: when the last durable
    /// write landed. `None` until the first write, so a fresh store persists
    /// once immediately and a reopen always sees a snapshot.
    keep_small_last_persist: Arc<std::sync::Mutex<Option<Instant>>>,
    /// Meters have changed since the last durable write. Set by every
    /// debounced call, cleared by the write that lands them.
    keep_small_dirty: Arc<AtomicBool>,
    /// Set by [`RecordStore::flush_keep_small_for_clean_stop`]; a metered write
    /// that lands afterwards persists at once with `clean_stop = false`.
    keep_small_clean_stop_written: Arc<AtomicBool>,
    /// Serialize whole-snapshot writes so an older export cannot land after a
    /// newer export under concurrent field key commits.
    keep_small_persist_lock: Arc<tokio::sync::Mutex<()>>,
    /// Totals, the last queue reading and recent faults of the blob counters.
    blob_events: Arc<blob_counter::BlobEvents>,
    /// The retention window of kept conflict records and the janitor report.
    retention: Arc<retention::RetentionState>,
    /// Wake expiry after a history policy activation attempt. Shared by clones.
    history_activation: Arc<tokio::sync::Notify>,
    /// The verifier: the run in progress and the last result. Memory only.
    counter_verify: Arc<counter::VerifyState>,
    /// Bounded range reads made by the admin walks since this process started.
    range_walks: Arc<WalkTotals>,
    /// Sync device id of this node. Empty until a sync engine attaches. The
    /// tip decision treats a write from any other non-empty device as a write
    /// from a peer. See [`RecordStore::local_origin_device`].
    local_origin_device: Arc<std::sync::RwLock<String>>,
    /// Fixed target gates for blob source transitions and reclaim.
    blob_liveness_locks: Arc<Vec<Arc<tokio::sync::Mutex<()>>>>,
}

impl RecordStore {
    /// The blob counter report: totals, the queue reading of the last dwell pass.
    #[must_use]
    pub fn blob_counter_report(&self) -> BlobCounterReport {
        self.blob_events.snapshot()
    }

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
