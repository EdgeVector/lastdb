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
mod write;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

pub use aggregate::AggregateRepairReceipt;
#[cfg(feature = "cloud-sync")]
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
use crate::resident::ResidentMode;
use crate::schema::SchemaCore;

/// Process-lifetime work totals for one schema's purges.
///
/// [`crate::request_phases::RequestPhase::PurgeBarrier`] measures the purge's
/// exclusive acquisition wait. `exclusive_hold_us` measures the guarded
/// critical sections after acquisition. Ordinary writes do not take this
/// barrier; their durable order comes from the schema persist lane.
///
/// Keyed, like the barrier itself, by schema NAME. See the `purge_barrier`
/// field docs for why that scope is load-bearing rather than incidental.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PurgeStats {
    /// Completed purge passes over this schema.
    pub purges: u64,
    /// Records removed across those passes.
    pub records_purged: u64,
    /// Total time in guarded purge critical sections.
    ///
    /// The wire name stays unchanged for compatibility. This is not blocked-
    /// writer time because ordinary writes do not take the purge barrier.
    pub exclusive_hold_us: u64,
    /// Unix milliseconds at which the most recent pass released the barrier.
    /// Zero means no purge has run on this schema since process start.
    pub last_purge_finished_unix_ms: u64,
    /// Schema-wide exclusive barrier acquisitions since process start.
    pub schema_barrier_acquisitions: u64,
    /// Exact molecule slots addressed by purge passes.
    pub purge_target_slots: u64,
    /// Distinct candidate atoms considered for deletion.
    pub purge_candidate_atoms: u64,
    /// Candidate `AtomRefEdges` partitions read by barrierless purge.
    pub purge_reverse_edge_reads: u64,
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
    pub fn record(
        &self,
        schema_name: &str,
        records_purged: u64,
        exclusive_hold: std::time::Duration,
        finished_unix_ms: u64,
    ) {
        let hold_us = u64::try_from(exclusive_hold.as_micros()).unwrap_or(u64::MAX);
        let mut map = self.by_schema.lock().expect("purge_stats poisoned");
        let entry = map.entry(schema_name.to_string()).or_default();
        entry.purges = entry.purges.saturating_add(1);
        entry.records_purged = entry.records_purged.saturating_add(records_purged);
        entry.exclusive_hold_us = entry.exclusive_hold_us.saturating_add(hold_us);
        // Last-writer-wins rather than max: a clock that steps backwards should
        // report the most recent purge this process actually ran, and a purge
        // is rare enough that two passes never race for a meaningful ordering.
        entry.last_purge_finished_unix_ms = finished_unix_ms;
    }

    pub fn record_schema_barrier_acquisition(&self, schema_name: &str) {
        let mut map = self.by_schema.lock().expect("purge_stats poisoned");
        let entry = map.entry(schema_name.to_string()).or_default();
        entry.schema_barrier_acquisitions = entry.schema_barrier_acquisitions.saturating_add(1);
    }

    pub fn record_path(
        &self,
        schema_name: &str,
        target_slots: u64,
        candidate_atoms: u64,
        reverse_edge_reads: u64,
    ) {
        let mut map = self.by_schema.lock().expect("purge_stats poisoned");
        let entry = map.entry(schema_name.to_string()).or_default();
        entry.purge_target_slots = entry.purge_target_slots.saturating_add(target_slots);
        entry.purge_candidate_atoms = entry.purge_candidate_atoms.saturating_add(candidate_atoms);
        entry.purge_reverse_edge_reads = entry
            .purge_reverse_edge_reads
            .saturating_add(reverse_edge_reads);
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
    /// WaitGroup for best-effort side-effects spawned off the write path
    /// (semantic indexing + schema-lane durable store when
    /// `LASTDB_RESIDENT_MODE=write`). Lets shutdown and tests block via
    /// `FoldDb::wait_for_background_tasks` even though the mutation itself
    /// returns before those tasks finish.
    pending_tasks: Arc<super::pending_task_tracker::PendingTaskTracker>,
    /// Explicit Search app inbox for this database instance. Daemon boots pass
    /// this so two `Host::boot` calls in one process do not race through
    /// process-global `LASTDB_HOME`.
    search_outbox_inbox: Option<PathBuf>,
    /// Resident-primary routing mode (`LASTDB_RESIDENT_MODE`). When `Write`,
    /// mutations install tip+atom into the resident graph and can acknowledge
    /// after schema-lane enqueue.
    resident_mode: ResidentMode,
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
    /// Per-schema barrier for guarded destructive purge critical sections.
    ///
    /// A guarded-complement purge uses a schema-wide reachability snapshot.
    /// Distinct persist lanes can address one schema name, so the barrier
    /// serializes their guarded purge sections in the shared atom namespace.
    ///
    /// A guarded purge takes this barrier in EXCLUSIVE (`write`) mode for its
    /// snapshot-to-delete window. The schema persist lane gives ordinary
    /// writes their durable order, so they do not take this barrier.
    ///
    /// Keyed by schema NAME, and that scope is load-bearing — do not "fix" it
    /// to a schema id. Atom identity is `SHA256(schema_NAME, content)`
    /// (`Atom::generate_content_uuid`), so two distinct schema *ids* that share
    /// a display name share one content-addressed atom space. The barrier
    /// prevents concurrent guarded purge sections in that shared space. Any
    /// change to `generate_content_uuid`'s scope must also change this key.
    ///
    /// Same-named schemas from different apps serialize their guarded purges.
    /// [`PurgeStats`] reports the cost of those critical sections.
    ///
    /// Schema-name cardinality is small and bounded, so entries are not reaped
    /// — unlike the per-key `cas_locks` map, this one does not grow without
    /// bound.
    purge_barrier: Arc<std::sync::Mutex<HashMap<String, Arc<tokio::sync::RwLock<()>>>>>,
    /// Per-schema purge accounting; see [`PurgeStats`]. Written once per
    /// completed purge pass (a rare verb), read by the status/ops surfaces, so
    /// a plain mutex is cheaper than the atomics a hot-path counter would need.
    purge_stats: Arc<PurgeLedger>,
    /// Per-molecule write locks serialize restore/apply/persist for one
    /// molecule/storage-prefix pair.
    ///
    /// This covers both the first-write fallback and the established-molecule
    /// O(changed) path. The latter loads the persisted append-log count before
    /// applying a changed key; two concurrent writers must not both append a
    /// different key at the same `mord:{M}:{seq}` slot.
    molecule_persist_locks: std::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// Per-molecule write dedupe (`LASTDB_WRITE_DEDUPE`). When true, a field
    /// write whose value already equals the stored tip is dropped before the
    /// apply/fold/persist steps ever see it. Off by default: the skip also
    /// skips the tip's `written_at` refresh, which is the per-slot LWW
    /// tie-break. See [`dedupe`] for the full contract.
    write_dedupe: bool,
    /// Coarse explicit policy hook for callers that require per-slot `as_of`
    /// history. Mini leaves this false by default; schema-scoped retention can
    /// replace the DB-wide switch when that policy surface lands.
    tip_history_enabled: std::sync::atomic::AtomicBool,
    /// Sync engine used by administrative capture status and direct callers.
    #[cfg(feature = "cloud-sync")]
    capture_engine: std::sync::RwLock<Option<std::sync::Arc<crate::sync::SyncEngine>>>,
    /// Bounded post-ack capture queue shared with the serving store wrapper.
    ///
    /// Arc so the persist-lane surface sees `set_capture_router` after
    /// construction. Deferred persist runs after the commit's task-local
    /// suppress ends; router depth is the suppress those puts can see.
    #[cfg(feature = "cloud-sync")]
    capture_router: std::sync::Arc<
        std::sync::RwLock<Option<std::sync::Arc<crate::sync::capture::MutationLogCaptureRouter>>>,
    >,
    /// Per-schema FIFO persist lanes. Deferred mode=write jobs enqueue here
    /// instead of each spawning an unordered Tokio task.
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
        let purge_barrier = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let purge_stats = Arc::new(PurgeLedger::default());
        #[cfg(feature = "cloud-sync")]
        let capture_router = Arc::new(std::sync::RwLock::new(None));
        let writer = Arc::new(molecules::DeferredLaneWriter {
            mm: Self::persist_surface(
                Arc::clone(&db_ops),
                Arc::clone(&schema_manager),
                Arc::clone(&signer),
                Arc::clone(&pending_tasks),
                search_outbox_inbox.clone(),
                policy.mode,
                Arc::clone(&defer_gauge),
                write_dedupe,
                Arc::clone(&purge_barrier),
                Arc::clone(&purge_stats),
                #[cfg(feature = "cloud-sync")]
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
            Some(Arc::clone(db_ops.resident().metrics())),
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
            resident_mode: policy.mode,
            defer_gauge,
            cas_locks: std::sync::Mutex::new(HashMap::new()),
            author_clock_state: std::sync::Mutex::new(author_clock_state),
            author_clock_persist: Some(author_clock_persist),
            purge_barrier,
            purge_stats,
            molecule_persist_locks: std::sync::Mutex::new(HashMap::new()),
            write_dedupe,
            tip_history_enabled: std::sync::atomic::AtomicBool::new(false),
            #[cfg(feature = "cloud-sync")]
            capture_engine: std::sync::RwLock::new(None),
            #[cfg(feature = "cloud-sync")]
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
        resident_mode: ResidentMode,
        defer_gauge: Arc<DeferredPersistGauge>,
        write_dedupe: bool,
        purge_barrier: Arc<std::sync::Mutex<HashMap<String, Arc<tokio::sync::RwLock<()>>>>>,
        purge_stats: Arc<PurgeLedger>,
        #[cfg(feature = "cloud-sync")] capture_router: std::sync::Arc<
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
            resident_mode,
            defer_gauge,
            cas_locks: std::sync::Mutex::new(HashMap::new()),
            author_clock_state: std::sync::Mutex::new(
                crate::schema::types::MutationAuthorClockState::default(),
            ),
            author_clock_persist: None,
            purge_barrier,
            purge_stats,
            molecule_persist_locks: std::sync::Mutex::new(HashMap::new()),
            write_dedupe,
            tip_history_enabled: std::sync::atomic::AtomicBool::new(false),
            #[cfg(feature = "cloud-sync")]
            capture_engine: std::sync::RwLock::new(None),
            #[cfg(feature = "cloud-sync")]
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

    /// Resident graph handle. Named so the apply-gate module can read slot
    /// revisions without naming `db_ops` in that file (structural test).
    pub(crate) fn resident_graph(&self) -> &crate::resident::ResidentGraph {
        self.db_ops.resident()
    }

    /// Wire the serving mutation-log engine (called from FoldDB::set_sync_engine).
    #[cfg(feature = "cloud-sync")]
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
    #[cfg(feature = "cloud-sync")]
    pub(crate) fn set_capture_engine_metadata(
        &self,
        engine: std::sync::Arc<crate::sync::SyncEngine>,
    ) {
        *self
            .capture_engine
            .write()
            .expect("capture engine lock poisoned") = Some(engine);
    }

    #[cfg(feature = "cloud-sync")]
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

    #[cfg(feature = "cloud-sync")]
    pub(crate) fn capture_router(
        &self,
    ) -> Option<std::sync::Arc<crate::sync::capture::MutationLogCaptureRouter>> {
        self.capture_router
            .read()
            .expect("capture router lock poisoned")
            .clone()
    }

    #[cfg(feature = "cloud-sync")]
    pub(crate) async fn wait_for_capture_tasks(&self, timeout: std::time::Duration) -> bool {
        match self.capture_router() {
            Some(router) => router.wait_for_completion(timeout).await,
            None => true,
        }
    }

    #[cfg(feature = "cloud-sync")]
    pub(crate) fn capture_engine(&self) -> Option<std::sync::Arc<crate::sync::SyncEngine>> {
        self.capture_engine
            .read()
            .expect("capture engine lock poisoned")
            .clone()
    }

    /// Atom uuids the durable pin log still references.
    ///
    /// Empty when the build has no cloud-sync engine or no capture engine is
    /// installed — both mean no pin-log plane exists to protect. Shared by
    /// [`crate::fold_db_core::fold_db::FoldDB::gc_orphan_atoms_with`] (the
    /// manual verb) and [`crate::fold_db_core::atom_reclaim_janitor`] (the
    /// background janitor) so both feed atom GC the same extra reference
    /// roots — a pin-log-blind reclaim pass is how an acked write silently
    /// never reaches the cloud.
    #[cfg_attr(not(feature = "cloud-sync"), allow(clippy::unused_async))]
    pub(crate) async fn pending_pin_log_atom_roots(
        &self,
    ) -> Result<std::collections::HashSet<String>, crate::schema::SchemaError> {
        #[cfg(feature = "cloud-sync")]
        {
            let Some(engine) = self.capture_engine() else {
                return Ok(std::collections::HashSet::new());
            };
            // Fail the pass rather than sweep with a reference set known to be
            // short: an unreadable pin log is missing referrers, not zero of
            // them, and atom GC deletes on this answer.
            engine
                .pin_log
                .pending_pin_log_atom_uuids()
                .await
                .map_err(|e| {
                    crate::schema::SchemaError::InvalidData(format!(
                        "collect pin-log atom references: {e}"
                    ))
                })
        }
        #[cfg(not(feature = "cloud-sync"))]
        {
            Ok(std::collections::HashSet::new())
        }
    }

    /// Wait for mutation work that existed at the start of an atom-GC cut.
    ///
    /// Deferred envelopes can persist an atom body before the matching tip.
    /// A reclaim pass must not classify that body during this window. The
    /// pending-task tracker uses a watermark, so later writes do not extend
    /// this cut; the GC timestamp guard protects bodies from those writes.
    pub(crate) async fn wait_for_atom_gc_persist_cut(&self, timeout: std::time::Duration) -> bool {
        self.pending_tasks.wait_for_completion(timeout).await
    }

    /// Explicitly opt subsequent writes into or out of tip-version history.
    ///
    /// The settled Mini default is `false`. This DB-wide seam keeps callers
    /// that genuinely require `as_of` history explicit until schema-level
    /// retention policy replaces it.
    pub fn set_tip_history_enabled_for_writes(&self, enabled: bool) {
        self.tip_history_enabled
            .store(enabled, std::sync::atomic::Ordering::Relaxed);
    }

    #[inline]
    fn tip_history_enabled_for_writes(&self) -> bool {
        self.tip_history_enabled
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// When true, mutation acks after resident install; durable LastStore puts
    /// run under `pending_tasks` (see `LASTDB_RESIDENT_MODE=write`).
    #[inline]
    pub(crate) fn acks_on_resident(&self) -> bool {
        self.resident_mode.acks_on_resident()
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
