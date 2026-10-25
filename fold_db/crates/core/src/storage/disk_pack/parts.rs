//! Dependency-part ledger and high-water maintenance for the native home.
//!
//! Every published generation names the file parts it needs in one receipt. The
//! receipt holds at most `MAX_DEPENDENCY_PARTS` entries, each publication writes
//! it again in full, and each publication omits parts with no live references.
//! The count is therefore a hard limit and a per-write cost. This ledger records
//! the count after every publication. One worker copies outside the write gate
//! and catches up before a short handoff. Admission refuses new work at the
//! 85-percent soft limit if writers outrun compaction. A 30-second idle interval
//! schedules obsolete-generation reclaim even when the live count stays flat.
//! Cleanup holds the installed root before it releases the write gate.

use super::*;
use laststore::disk_pack::MAX_DEPENDENCY_PARTS;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Mutex, Weak};
use tokio::sync::Notify;

mod orphans;
mod progress;
mod retry;
pub use progress::NativeCompactionStatus;
mod worker;

/// Parts after which maintenance starts. Every publication writes the receipt
/// in full (80 bytes a part for each batch), so a write costs more as the
/// count grows. A receipt of this size is about 330 KiB. A smaller mark costs
/// more maintenance runs, including recent-tier copies and periodic full copies.
const DEFAULT_HIGH_WATER: u64 = 4096;
/// Never trigger above this share of the limit (percent), so a write that needs
/// room for its own parts still fits while maintenance runs.
const TRIGGER_CEILING_PERCENT: u64 = 75;
/// Parts that must accrue after a run before the next normal run starts
/// (at most half the high-water mark). Soft-limit recovery cannot require growth.
const MIN_GROWTH: u64 = 1024;
/// Above this share of the limit (percent) admission refuses a new write until
/// maintenance restores room, before the hard receipt limit.
const SOFT_LIMIT_PERCENT: u64 = 85;

/// Counters and settings. Atomics only: a status read never takes a lock that a
/// writer holds.
pub(super) struct PartsLedger {
    pub(super) progress: Arc<progress::Progress>,
    current: AtomicU64,
    /// Last root seen by a publication or a maintenance run. A status read uses
    /// it while maintenance holds the view lock, so status never waits for reclaim.
    generation: AtomicU64,
    frontier: AtomicU64,
    peak: AtomicU64,
    /// Parts left by the last run. A live set that needs this many parts moves
    /// the next trigger up, so maintenance cannot loop on an incompressible home.
    baseline: AtomicU64,
    /// Records (live and tombstones) that the last run copied. A run costs about
    /// as much as the records it copies, so a larger home packs less often.
    records: AtomicU64,
    high_water: u64,
    auto: bool,
    grace_ms: u64,
    /// Youngest unreferenced part file that the orphan sweep removes, in milliseconds.
    orphan_min_age_ms: u64,
    /// Wait before the first pack of a process, in seconds.
    start_delay_secs: u64,
    wake: Notify,
    /// Set once the worker was started. A started worker that died is restarted.
    worker: AtomicBool,
    /// True while the worker task runs. A panic clears it until the restart.
    alive: AtomicBool,
    worker_restarts: AtomicU64,
    /// Old generations remain, or a reclaim waits for its next try.
    reclaim_pending: AtomicBool,
    /// Set by a failed reclaim, cleared by a reclaim that ran to its end. While it
    /// is set, maintenance starts at the ceiling instead of the mark: a pack that
    /// cannot be followed by a reclaim only adds a full copy of the live data.
    reclaim_failing: AtomicBool,
    reclaim_failures: AtomicU64,
    reclaim_error: Mutex<Option<String>>,
    running: AtomicBool,
    /// Set by a failed run, cleared by a published replacement.
    failing: AtomicBool,
    runs: AtomicU64,
    failures: AtomicU64,
    last_before: AtomicU64,
    last_after: AtomicU64,
    last_millis: AtomicU64,
    last_error: Mutex<Option<String>>,
}

/// What one pack step left behind, for the ledger.
pub(super) struct RunCounts {
    pub(super) parts_before: u64,
    pub(super) parts_after: u64,
    pub(super) records: u64,
}

/// Part count and maintenance state, served in the native disk status.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct NativePartsStatus {
    /// Parts the current generation receipt names.
    pub dependency_parts: u64,
    /// Hard limit; a write that would pass it is refused.
    pub dependency_parts_limit: u64,
    /// Configured high-water mark.
    pub dependency_high_water: u64,
    /// Count at which maintenance starts now (the mark, or higher for a large live set).
    pub dependency_trigger: u64,
    /// Highest count seen since this process started.
    pub dependency_parts_peak: u64,
    /// Whether the high-water worker is enabled.
    pub auto_maintenance: bool,
    pub maintenance_running: bool,
    pub maintenance_runs: u64,
    pub maintenance_failures: u64,
    pub maintenance_last_parts_before: u64,
    pub maintenance_last_parts_after: u64,
    /// Records (live and tombstones) that the last run copied.
    pub maintenance_last_records: u64,
    pub maintenance_last_millis: u64,
    pub maintenance_last_error: Option<String>,
    /// Whether the worker task runs now. False with automatic maintenance on means
    /// the worker died and waits for its restart.
    pub maintenance_worker_alive: bool,
    /// Times the supervisor restarted the worker after a panic.
    pub maintenance_worker_restarts: u64,
    /// Old generations wait for reclaim, or a reclaim waits for its next try.
    pub reclaim_pending: bool,
    pub reclaim_failures: u64,
    /// The last reclaim error, until a reclaim ends without error. A reclaim that
    /// cannot run leaves old generations on disk, so the disk grows with each pack.
    pub reclaim_last_error: Option<String>,
    #[serde(flatten)]
    pub compaction: NativeCompactionStatus,
}

impl PartsLedger {
    pub(super) fn from_env(opened: u64, generation: u64, frontier: u64) -> Arc<Self> {
        let ceiling = MAX_DEPENDENCY_PARTS * TRIGGER_CEILING_PERCENT / 100;
        let high_water = env_flag::var_parsed::<u64>("LASTDB_DEPENDENCY_HIGH_WATER")
            .filter(|mark| *mark > 0)
            .unwrap_or(DEFAULT_HIGH_WATER)
            .min(ceiling);
        Arc::new(Self {
            progress: Arc::default(),
            current: AtomicU64::new(opened),
            generation: AtomicU64::new(generation),
            frontier: AtomicU64::new(frontier),
            peak: AtomicU64::new(opened),
            baseline: AtomicU64::new(0),
            records: AtomicU64::new(0),
            high_water,
            auto: !matches!(env_flag::var_parse("LASTDB_AUTO_MAINTENANCE"), Some(false)),
            grace_ms: env_flag::var_or("LASTDB_AUTO_MAINTENANCE_GRACE_MS", 0),
            orphan_min_age_ms: env_flag::var_or("LASTDB_ORPHAN_MIN_AGE_MS", 600_000),
            start_delay_secs: env_flag::var_or("LASTDB_AUTO_MAINTENANCE_START_DELAY_SECS", 30),
            wake: Notify::new(),
            worker: AtomicBool::new(false),
            alive: AtomicBool::new(false),
            worker_restarts: AtomicU64::new(0),
            reclaim_pending: AtomicBool::new(false),
            reclaim_failing: AtomicBool::new(false),
            reclaim_failures: AtomicU64::new(0),
            reclaim_error: Mutex::new(None),
            running: AtomicBool::new(false),
            failing: AtomicBool::new(false),
            runs: AtomicU64::new(0),
            failures: AtomicU64::new(0),
            last_before: AtomicU64::new(0),
            last_after: AtomicU64::new(0),
            last_millis: AtomicU64::new(0),
            last_error: Mutex::new(None),
        })
    }

    /// Parts at which maintenance starts: the mark, or twice the parts the last
    /// run could not remove, or the square root of twenty times its records
    /// (where the cost of a larger receipt equals the cost of the pack), whichever
    /// is highest, below the ceiling.
    fn trigger(&self) -> u64 {
        let ceiling = MAX_DEPENDENCY_PARTS * TRIGGER_CEILING_PERCENT / 100;
        if self.reclaim_failing.load(Relaxed) {
            return ceiling;
        }
        let by_records = (self.records.load(Relaxed).saturating_mul(20) as f64).sqrt() as u64;
        let configured = self
            .high_water
            .max(self.baseline.load(Relaxed).saturating_mul(2))
            .max(by_records)
            .min(ceiling.min(MAX_DEPENDENCY_PARTS / 2));
        let minimum = self
            .baseline
            .load(Relaxed)
            .saturating_add(MIN_GROWTH.min(self.high_water / 2));
        self.progress.start_before_exhaustion(configured, minimum)
    }

    /// True when the count leaves too little room for another write.
    fn over_soft_limit(&self) -> bool {
        self.current.load(Relaxed) >= MAX_DEPENDENCY_PARTS * SOFT_LIMIT_PERCENT / 100
    }

    /// Normal repeats need growth. A refused home cannot grow, so soft-limit
    /// recovery bypasses that suppression and the worker bounds its retry rate.
    fn wants_maintenance(&self) -> bool {
        let current = self.current.load(Relaxed);
        self.auto
            && (self.over_soft_limit()
                || (current >= self.trigger()
                    && current
                        >= self
                            .baseline
                            .load(Relaxed)
                            .saturating_add(MIN_GROWTH.min(self.high_water / 2))))
    }

    /// Record the count of a new publication and wake the worker at the mark.
    pub(super) fn observe(&self, parts: u64, root: &Current) {
        let previous = self.current.swap(parts, Relaxed);
        self.progress.observe_growth(parts.saturating_sub(previous));
        self.generation.store(root.generation, Relaxed);
        self.frontier.store(root.frontier, Relaxed);
        self.peak.fetch_max(parts, Relaxed);
        if self.wants_maintenance() {
            self.wake.notify_one();
        }
    }

    /// A refused write is a sign that maintenance is due, even when the mark is
    /// higher than the count the refusal saw.
    pub(super) fn wake_for_refusal(&self) {
        if self.auto {
            self.wake.notify_one();
        }
    }

    pub(super) fn record_run(&self, run: &RunCounts, millis: u64, root: &Current) {
        let (before, after) = (run.parts_before, run.parts_after);
        self.records.store(run.records, Relaxed);
        self.current.store(after, Relaxed);
        self.generation.store(root.generation, Relaxed);
        self.frontier.store(root.frontier, Relaxed);
        self.baseline.store(after, Relaxed);
        self.last_before.store(before, Relaxed);
        self.last_after.store(after, Relaxed);
        self.last_millis.store(millis, Relaxed);
        self.runs.fetch_add(1, Relaxed);
        self.failing.store(false, Relaxed);
        *self.last_error.lock().expect("parts last error") = None;
    }

    /// Reclaim time belongs to the run that published the replacement.
    pub(super) fn add_run_millis(&self, millis: u64) {
        self.last_millis.fetch_add(millis, Relaxed);
    }

    /// A reclaim failed after the pack: the count is final, and only the disk is at risk.
    pub(super) fn record_reclaim_failure(&self, error: &StorageError) {
        self.reclaim_failures.fetch_add(1, Relaxed);
        self.reclaim_failing.store(true, Relaxed);
        *self.reclaim_error.lock().expect("parts reclaim error") = Some(error.to_string());
    }

    /// The worker task panicked. The supervisor restarts it, and a writer above the
    /// soft limit remains refused until a run restores room.
    pub(super) fn record_worker_panic(&self, detail: &str) {
        self.worker_restarts.fetch_add(1, Relaxed);
        self.failures.fetch_add(1, Relaxed);
        self.failing.store(true, Relaxed);
        *self.last_error.lock().expect("parts last error") =
            Some(format!("maintenance worker panicked: {detail}"));
    }

    /// A reclaim call ended without an error.
    pub(super) fn record_reclaim_ok(&self) {
        self.reclaim_failing.store(false, Relaxed);
        *self.reclaim_error.lock().expect("parts reclaim error") = None;
    }

    pub(super) fn record_failure(&self, error: &StorageError) {
        self.failures.fetch_add(1, Relaxed);
        self.failing.store(true, Relaxed);
        *self.last_error.lock().expect("parts last error") = Some(error.to_string());
    }

    /// Record a v2 root, which has no dependency parts.
    /// A commit and a merge adoption can race; the newer generation wins.
    pub(super) fn observe_root(&self, generation: u64, frontier: u64) {
        if self.generation.fetch_max(generation, Relaxed) <= generation {
            self.frontier.store(frontier, Relaxed);
        }
    }

    /// Generation and frontier of the last root this ledger saw.
    pub(super) fn root(&self) -> (u64, u64) {
        (self.generation.load(Relaxed), self.frontier.load(Relaxed))
    }

    pub(super) fn status(&self) -> NativePartsStatus {
        NativePartsStatus {
            compaction: self.progress.status(),
            dependency_parts: self.current.load(Relaxed),
            dependency_parts_limit: MAX_DEPENDENCY_PARTS,
            dependency_high_water: self.high_water,
            dependency_trigger: self.trigger(),
            dependency_parts_peak: self.peak.load(Relaxed),
            auto_maintenance: self.auto,
            maintenance_running: self.running.load(Relaxed),
            maintenance_runs: self.runs.load(Relaxed),
            maintenance_failures: self.failures.load(Relaxed),
            maintenance_last_parts_before: self.last_before.load(Relaxed),
            maintenance_last_parts_after: self.last_after.load(Relaxed),
            maintenance_last_records: self.records.load(Relaxed),
            maintenance_last_millis: self.last_millis.load(Relaxed),
            maintenance_last_error: self.last_error.lock().expect("parts last error").clone(),
            maintenance_worker_alive: self.alive.load(Relaxed),
            maintenance_worker_restarts: self.worker_restarts.load(Relaxed),
            reclaim_pending: self.reclaim_pending.load(Relaxed),
            reclaim_failures: self.reclaim_failures.load(Relaxed),
            reclaim_last_error: self
                .reclaim_error
                .lock()
                .expect("parts reclaim error")
                .clone(),
        }
    }
}
