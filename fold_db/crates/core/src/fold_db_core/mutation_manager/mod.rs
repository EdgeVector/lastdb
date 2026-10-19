//! Mutation Manager — handles all mutation operations.
//!
//! Layout:
//! - [`helpers`] — free helpers (tombstone, key normalize, native-index allowlist, derived guard)
//! - [`write`] — public write entry points, purge peel, batch pipeline, finalize
//! - [`cas`] — CAS locks, purge barriers, preconditions, idempotency
//! - [`dedupe`] — per-molecule write dedupe (`LASTDB_WRITE_DEDUPE`)
//! - [`molecules`] — atom prep, molecule apply/persist
//! - [`receipt`] — what one logical resident commit answers to its caller
//! - [`index`] — best-effort native-index side effects

mod aggregate;
mod author_clock;
mod cas;
mod cas_current_state;
mod cas_idempotency;
mod cas_locks;
mod cas_preconditions;
pub mod dedupe;
pub(super) mod helpers;
mod index;
mod molecules;
pub mod receipt;
mod retired_replay;
mod write;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

pub use aggregate::AggregateRepairReceipt;
pub(crate) use author_clock::AuthorClockPersistBarrier;
pub use receipt::{
    CloudCapturePolicy, CloudCaptureState, CloudMutationReceipt, CloudPublicationState,
    CloudPublicationTarget, ResidentCommitOperations, ResidentCommitReceipt, ResidentCommitStages,
    ResidentDurability,
};

use crate::db_operations::DbOperations;
use crate::memory_budget::{
    DeferReservation, DeferredPersistGauge, RuntimeMemoryBudgetObservation,
};
use crate::schema::SchemaCore;

/// Process-lifetime work totals for one schema's purges and deletes.
///
/// An erase takes the native write gate and no schema barrier, so the stats
/// carry counts and no lock time.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PurgeStats {
    /// Completed erase passes over this schema.
    pub purges: u64,
    /// Records removed across those passes.
    pub records_purged: u64,
    /// Unix milliseconds at which the most recent pass finished.
    /// Zero means no erase has run on this schema since process start.
    pub last_purge_finished_unix_ms: u64,
    /// Exact molecule slots removed by erase passes.
    pub purge_target_slots: u64,
    /// Atom bodies deleted because their count reached zero.
    pub purge_atoms_deleted: u64,
}

/// Cumulative [`PurgeStats`] per schema name, since process start.
///
/// A separate type rather than a bare map on the manager so the accounting can
/// be exercised without standing up a whole `MutationManager`, and so the one
/// place that has to get the saturating arithmetic right is the one place that
/// does it.
#[derive(Debug, Default)]
pub struct PurgeLedger {
    by_schema: std::sync::Mutex<HashMap<String, PurgeStats>>,
}

impl PurgeLedger {
    /// Fold one completed purge pass into `schema_name`'s totals.
    ///
    /// `finished_unix_ms` is passed in rather than read from the clock here so
    /// the ledger stays a pure fold — the caller owns the one `SystemTime`
    /// read, and tests own the timeline.
    pub fn record(&self, schema_name: &str, records_purged: u64, finished_unix_ms: u64) {
        let mut map = self.by_schema.lock().expect("purge_stats poisoned");
        let entry = map.entry(schema_name.to_string()).or_default();
        entry.purges = entry.purges.saturating_add(1);
        entry.records_purged = entry.records_purged.saturating_add(records_purged);
        // Last-writer-wins rather than max: a clock that steps backwards should
        // report the most recent purge this process actually ran, and a purge
        // is rare enough that two passes never race for a meaningful ordering.
        entry.last_purge_finished_unix_ms = finished_unix_ms;
    }

    pub fn record_path(&self, schema_name: &str, target_slots: u64, atoms_deleted: u64) {
        let mut map = self.by_schema.lock().expect("purge_stats poisoned");
        let entry = map.entry(schema_name.to_string()).or_default();
        entry.purge_target_slots = entry.purge_target_slots.saturating_add(target_slots);
        entry.purge_atoms_deleted = entry.purge_atoms_deleted.saturating_add(atoms_deleted);
    }

    /// Every schema that has been purged since process start. Cumulative, so
    /// consumers delta it themselves — matching the request-ops aggregates.
    #[must_use]
    pub fn snapshot(&self) -> HashMap<String, PurgeStats> {
        self.by_schema.lock().expect("purge_stats poisoned").clone()
    }

    /// One schema's totals; all-zero for a schema never purged.
    #[must_use]
    pub fn for_schema(&self, schema_name: &str) -> PurgeStats {
        self.by_schema
            .lock()
            .expect("purge_stats poisoned")
            .get(schema_name)
            .copied()
            .unwrap_or_default()
    }
}

/// Manages mutation operations for the FoldDB system
pub struct MutationManager {
    /// Database operations for persistence
    db_ops: Arc<DbOperations>,
    /// Schema manager for schema operations
    schema_manager: Arc<SchemaCore>,
    /// Signing keypair for molecule signatures
    signer: Arc<crate::security::Ed25519KeyPair>,
    /// Tracks search side effects and queued purge work until completion.
    /// Shutdown waits through `FoldDb::wait_for_background_tasks` even when
    /// the mutation returns before those tasks finish.
    pending_tasks: Arc<super::pending_task_tracker::PendingTaskTracker>,
    /// Explicit Search app inbox for this database instance. Daemon boots pass
    /// this so two `Host::boot` calls in one process do not race through
    /// process-global `LASTDB_HOME`.
    search_outbox_inbox: Option<PathBuf>,
    /// Byte cap on in-flight deferred persist work, sized from the process
    /// memory budget (`crate::memory_budget`).
    ///
    /// The count cap above and this cap bound **different resources**, which is
    /// why #966's count-only window did not stop the balloon: 512 tasks each
    /// holding a 512 KiB atom batch is 256 MiB of acked-but-unpersisted state,
    /// and the same 512 tasks holding one small record each is under a
    /// megabyte. The 2026-07-29 restart loop was the first shape passing a cap
    /// written for the second (brain
    /// `incident-primary-lastdbd-restart-loop-10-11gb-rss-after-0231-183-cutover`).
    defer_gauge: Arc<DeferredPersistGauge>,
    /// Per-key async locks that serialize the compare-and-apply of CAS
    /// mutations on this node. A CAS mutation's read of the current head and
    /// its write must not interleave with another same-key writer, or two
    /// concurrent CAS writers could both observe the same "expected" state and
    /// both apply (silent last-write-wins — exactly what CAS must prevent).
    ///
    /// The map hands out one `tokio::sync::Mutex` per `(schema, key)` string;
    /// a batch containing CAS mutations grabs the locks for its keys (sorted,
    /// to avoid deadlock) before it checks preconditions and holds them until
    /// the write commits. Non-CAS writes never touch this map, so the default
    /// write path is unchanged. Entries are cleaned up when the last holder
    /// drops (`Arc` strong count reaches 1 under the map guard), so the map
    /// does not grow without bound.
    cas_locks: std::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// Resident high-water author clock for this device. Boot point-loads the
    /// durable state once; requests only serialize the small in-memory
    /// advance/sign step. Ordered persistence runs behind the T0 ACK.
    author_clock_state: std::sync::Mutex<crate::schema::types::MutationAuthorClockState>,
    /// Bounded FIFO for the author-clock metadata row. Queue admission occurs
    /// before resident apply, so overload rejects without changing product
    /// state. The worker owns a pending-task guard until the row lands.
    author_clock_persist: Option<author_clock::AuthorClockPersistQueue>,
    /// Per-schema purge accounting; see [`PurgeStats`]. Written once per
    /// completed purge pass (a rare verb), read by the status/ops surfaces, so
    /// a plain mutex is cheaper than the atomics a hot-path counter would need.
    purge_stats: Arc<PurgeLedger>,
    /// Per-molecule write dedupe (`LASTDB_WRITE_DEDUPE`). When true, a field
    /// write whose value already equals the stored tip is dropped before the
    /// apply/fold/persist steps ever see it. Off by default: the skip also
    /// skips the tip's `written_at` refresh, which is the per-slot LWW
    /// tie-break. See [`dedupe`] for the full contract.
    write_dedupe: bool,
    /// Sync engine used by administrative capture status and direct callers.
    capture_engine: std::sync::RwLock<Option<std::sync::Arc<crate::sync::SyncEngine>>>,
    /// Bounded post-ack capture queue shared with the serving store wrapper.
    ///
    /// Arc so the persist-lane surface sees `set_capture_router` after
    /// construction. Deferred persist runs after the commit's task-local
    /// suppress ends; router depth is the suppress those puts can see.
    capture_router: std::sync::Arc<
        std::sync::RwLock<Option<std::sync::Arc<crate::sync::capture::MutationLogCaptureRouter>>>,
    >,
    /// Per-schema FIFO lanes for durable purge work.
    persist_lanes: std::sync::Arc<crate::resident::PersistLaneSet<molecules::LanePersistJob>>,
}

impl MutationManager {
    /// Creates a new MutationManager instance
    pub fn new(
        db_ops: Arc<DbOperations>,
        schema_manager: Arc<SchemaCore>,
        signer: Arc<crate::security::Ed25519KeyPair>,
        pending_tasks: Arc<super::pending_task_tracker::PendingTaskTracker>,
        search_outbox_inbox: Option<PathBuf>,
        author_clock_state: crate::schema::types::MutationAuthorClockState,
    ) -> Self {
        let policy = crate::resident::ResidentPolicy::from_env();
        let defer_gauge = Arc::new(DeferredPersistGauge::from_process_budget_with_count(
            policy.max_deferred_persists,
        ));
        let write_dedupe = dedupe::parse_write_dedupe(std::env::var(dedupe::WRITE_DEDUPE_ENV).ok());
        let purge_stats = Arc::new(PurgeLedger::default());
        let capture_router = Arc::new(std::sync::RwLock::new(None));
        let writer = Arc::new(molecules::DeferredLaneWriter {
            mm: Self::persist_surface(
                Arc::clone(&db_ops),
                Arc::clone(&schema_manager),
                Arc::clone(&signer),
                Arc::clone(&pending_tasks),
                search_outbox_inbox.clone(),
                Arc::clone(&defer_gauge),
                write_dedupe,
                Arc::clone(&purge_stats),
                Arc::clone(&capture_router),
            ),
        });
        let lane_fair_share = crate::memory_budget::lane_fair_share_bytes(
            defer_gauge.cap_bytes(),
            crate::memory_budget::lane_fair_share_percent(),
        );
        let persist_lanes = Arc::new(crate::resident::PersistLaneSet::new(
            policy.max_deferred_persists.max(1),
            lane_fair_share.max(1),
            writer,
            Some(Arc::clone(db_ops.persist_metrics())),
        ));
        let author_clock_state_key = crate::schema::types::author_clock::mutation_author_clock_key(
            &signer.public_key_base64(),
        );
        let author_clock_persist = author_clock::AuthorClockPersistQueue::new(
            Arc::clone(&db_ops),
            author_clock_state_key,
            author_clock_state,
            Arc::clone(&pending_tasks),
        );
        Self {
            db_ops,
            schema_manager,
            signer,
            pending_tasks,
            search_outbox_inbox,
            defer_gauge,
            cas_locks: std::sync::Mutex::new(HashMap::new()),
            author_clock_state: std::sync::Mutex::new(author_clock_state),
            author_clock_persist: Some(author_clock_persist),
            purge_stats,
            write_dedupe,
            capture_engine: std::sync::RwLock::new(None),
            capture_router,
            persist_lanes,
        }
    }

    /// Persist-only manager surface: empty lock maps, disabled lanes.
    /// Lane workers use this so they do not re-enter the live lane set.
    ///
    /// `capture_router` is the serving manager's slot. Deferred persist
    /// must raise that router's suppress depth: task-local suppress from
    /// the request does not follow the persist-lane task or `spawn_blocking`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn persist_surface(
        db_ops: Arc<DbOperations>,
        schema_manager: Arc<SchemaCore>,
        signer: Arc<crate::security::Ed25519KeyPair>,
        pending_tasks: Arc<super::pending_task_tracker::PendingTaskTracker>,
        search_outbox_inbox: Option<PathBuf>,
        defer_gauge: Arc<DeferredPersistGauge>,
        write_dedupe: bool,
        purge_stats: Arc<PurgeLedger>,
        capture_router: std::sync::Arc<
            std::sync::RwLock<
                Option<std::sync::Arc<crate::sync::capture::MutationLogCaptureRouter>>,
            >,
        >,
    ) -> Self {
        Self {
            db_ops,
            schema_manager,
            signer,
            pending_tasks,
            search_outbox_inbox,
            defer_gauge,
            cas_locks: std::sync::Mutex::new(HashMap::new()),
            author_clock_state: std::sync::Mutex::new(
                crate::schema::types::MutationAuthorClockState::default(),
            ),
            author_clock_persist: None,
            purge_stats,
            write_dedupe,
            capture_engine: std::sync::RwLock::new(None),
            capture_router,
            persist_lanes: Arc::new(crate::resident::PersistLaneSet::disabled()),
        }
    }

    pub(crate) fn persist_lanes(
        &self,
    ) -> &Arc<crate::resident::PersistLaneSet<molecules::LanePersistJob>> {
        &self.persist_lanes
    }

    /// Persist-lane occupancy and refusal counters for `lastdb status` / ops.
    #[must_use]
    pub fn persist_lane_pressure(&self) -> crate::resident::PersistLanePressure {
        self.persist_lanes.pressure()
    }

    /// Wire the serving mutation-log engine (called from FoldDB::set_sync_engine).
    pub(crate) fn set_capture_engine(&self, engine: std::sync::Arc<crate::sync::SyncEngine>) {
        self.set_capture_engine_metadata(std::sync::Arc::clone(&engine));
        let mut router = self
            .capture_router
            .write()
            .expect("capture router lock poisoned");
        if let Some(router) = router.as_ref() {
            router.set_engine(engine);
        } else {
            let created =
                std::sync::Arc::new(crate::sync::capture::MutationLogCaptureRouter::default());
            created.set_engine(engine);
            *router = Some(created);
        }
    }

    /// Make new capture roots visible to atom GC before the router admits work.
    pub(crate) fn set_capture_engine_metadata(
        &self,
        engine: std::sync::Arc<crate::sync::SyncEngine>,
    ) {
        *self
            .capture_engine
            .write()
            .expect("capture engine lock poisoned") = Some(engine);
    }

    pub(crate) fn set_capture_router(
        &self,
        router: std::sync::Arc<crate::sync::capture::MutationLogCaptureRouter>,
    ) {
        if let Some(engine) = self.capture_engine() {
            router.set_engine(engine);
        }
        *self
            .capture_router
            .write()
            .expect("capture router lock poisoned") = Some(router);
    }

    pub(crate) fn capture_router(
        &self,
    ) -> Option<std::sync::Arc<crate::sync::capture::MutationLogCaptureRouter>> {
        self.capture_router
            .read()
            .expect("capture router lock poisoned")
            .clone()
    }

    pub(crate) async fn wait_for_capture_tasks(&self, timeout: std::time::Duration) -> bool {
        match self.capture_router() {
            Some(router) => router.wait_for_completion(timeout).await,
            None => true,
        }
    }

    pub(crate) fn capture_engine(&self) -> Option<std::sync::Arc<crate::sync::SyncEngine>> {
        self.capture_engine
            .read()
            .expect("capture engine lock poisoned")
            .clone()
    }

    /// Admit one deferred persist of `bytes`, or refuse.
    ///
    /// Both caps must pass: the deferred-persist task **count** window
    /// (`LASTDB_RESIDENT_MAX_DEFERRED`, which bounds held write guards and the
    /// per-task state bytes cannot cheaply measure) and the **byte** window
    /// derived from the process memory budget. `Some(reservation)` means the
    /// bytes are charged until the reservation drops; `None` means this batch
    /// pays the inline durable path, which is always correct and always makes
    /// progress.
    pub(crate) fn try_reserve_defer(&self, bytes: u64) -> Option<DeferReservation> {
        if !self.defer_gauge.try_reserve(bytes) {
            return None;
        }
        Some(DeferReservation::new(Arc::clone(&self.defer_gauge), bytes))
    }

    /// In-flight deferred work and the caps it is charged against — for the
    /// degrade log, and so tests can assert the window actually bounds.
    #[inline]
    pub fn defer_window_in_flight(&self) -> (usize, usize, u64, u64) {
        (
            self.defer_gauge.in_flight_count(),
            self.defer_gauge.cap_count(),
            self.defer_gauge.in_flight_bytes(),
            self.defer_gauge.cap_bytes(),
        )
    }

    /// Why the defer window would refuse right now — `Disabled` (zero window)
    /// or `WindowFull` (live backpressure). The degrade log must separate
    /// these: only one of them is something an operator can act on.
    #[inline]
    #[must_use]
    pub fn defer_refusal_kind(&self) -> crate::memory_budget::DeferRefusal {
        self.defer_gauge.refusal_kind()
    }

    /// Feed the periodic physical-footprint sample back into the defer window.
    /// This is deliberately a cheap atomic-only control loop: status and
    /// self-metrics must never scan storage to decide whether writes are safe.
    #[must_use]
    pub fn observe_memory_footprint(
        &self,
        measured_footprint_bytes: u64,
    ) -> RuntimeMemoryBudgetObservation {
        self.defer_gauge.observe_footprint(
            measured_footprint_bytes,
            crate::memory_budget::process_memory_budget(),
        )
    }

    /// Wait until no deferred persist tasks remain in flight, or `timeout`.
    ///
    /// Shed uses this so already-admitted writes land before the warm set is
    /// trimmed. New requests are never refused; they persist inline if the
    /// window is empty.
    pub async fn wait_deferred_idle(&self, timeout: std::time::Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if self.defer_gauge.in_flight_count() == 0 {
                return true;
            }
            if std::time::Instant::now() >= deadline {
                return self.defer_gauge.in_flight_count() == 0;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }
}
