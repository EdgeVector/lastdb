//! Mutation Manager — handles all mutation operations.
//!
//! Layout:
//! - [`helpers`] — free helpers (tombstone, key normalize, native-index allowlist, derived guard)
//! - [`write`] — public write entry points, purge peel, batch pipeline, finalize
//! - [`cas`] — write preconditions and idempotency
//! - [`molecules`] — atom prep, molecule apply/persist
//! - [`receipt`] — what one logical resident commit answers to its caller
//! - [`index`] — best-effort native-index side effects

mod author_clock;
mod cas;
mod cas_current_state;
mod cas_idempotency;
mod cas_preconditions;
mod cas_rules;
pub(super) mod helpers;
mod index;
mod logical_capture;
mod molecules;
mod peer_import;
mod purge_locks;
pub mod receipt;
mod write;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

pub use crate::storage::disk_pack::logical_capture::ImportReceipt;
pub use receipt::{
    ResidentCommitOperations, ResidentCommitReceipt, ResidentCommitStages, ResidentDurability,
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
}

/// Services the persist-lane surface shares with the serving manager.
pub(crate) struct PersistSurfaceParts {
    db_ops: Arc<DbOperations>,
    schema_manager: Arc<SchemaCore>,
    signer: Arc<crate::security::Ed25519KeyPair>,
    pending_tasks: Arc<super::pending_task_tracker::PendingTaskTracker>,
    search_outbox_inbox: Option<PathBuf>,
    defer_gauge: Arc<DeferredPersistGauge>,
    purge_stats: Arc<PurgeLedger>,
    capture_router: std::sync::Arc<
        std::sync::RwLock<Option<std::sync::Arc<crate::sync::capture::MutationLogCaptureRouter>>>,
    >,
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
    /// Sync engine used by administrative capture status and direct callers.
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
        let purge_stats = Arc::new(PurgeLedger::default());
        let capture_router = Arc::new(std::sync::RwLock::new(None));
        let writer = Arc::new(molecules::DeferredLaneWriter {
            mm: Self::persist_surface(PersistSurfaceParts {
                db_ops: Arc::clone(&db_ops),
                schema_manager: Arc::clone(&schema_manager),
                signer: Arc::clone(&signer),
                pending_tasks: Arc::clone(&pending_tasks),
                search_outbox_inbox: search_outbox_inbox.clone(),
                defer_gauge: Arc::clone(&defer_gauge),
                purge_stats: Arc::clone(&purge_stats),
                capture_router: Arc::clone(&capture_router),
            }),
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
            author_clock_state: std::sync::Mutex::new(author_clock_state),
            author_clock_persist: Some(author_clock_persist),
            purge_stats,
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
    pub(crate) fn persist_surface(parts: PersistSurfaceParts) -> Self {
        Self {
            db_ops: parts.db_ops,
            schema_manager: parts.schema_manager,
            signer: parts.signer,
            pending_tasks: parts.pending_tasks,
            search_outbox_inbox: parts.search_outbox_inbox,
            defer_gauge: parts.defer_gauge,
            author_clock_state: std::sync::Mutex::new(
                crate::schema::types::MutationAuthorClockState::default(),
            ),
            author_clock_persist: None,
            purge_stats: parts.purge_stats,
            capture_router: parts.capture_router,
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

    pub(crate) fn set_capture_router(
        &self,
        router: std::sync::Arc<crate::sync::capture::MutationLogCaptureRouter>,
    ) {
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
}
