//! The single task that runs maintenance when the part count passes its mark.
//!
//! The worker is a loop over bounded steps. A pack has priority: it runs whenever
//! the count passes the trigger, even while old generations wait for reclaim or
//! a reclaim waits for its next try. Reclaim, the drain of old generations, and
//! the orphan sweep run only when no pack is due. Cleanup releases the write gate
//! after its installed-root capture, before file inventories and deletion.

use super::*;
use std::time::{Duration, Instant};
use tracing::Instrument;

/// Longest pause between two failed packs.
const MAX_BACKOFF_SECS: u64 = 60;
/// Tries of a reclaim that another process deferred. The waits grow from 1 s to 30 s,
/// so the worker keeps trying for about 20 minutes before it waits for the next pack.
const RECLAIM_RETRIES: u32 = 40;
/// Old generations that the reclaim step of a pack retires.
const PACK_SLICE_GENERATIONS: usize = 32;
/// Old generations that a drain call retires at first, and its bounds. After a call
/// the size follows the call time, so a drain of thousands of generations does not
/// pay the directory syncs of a call for each 32, and a call stays near the target.
const DRAIN_SLICE_START: usize = 32;
const DRAIN_SLICE_MIN: usize = 8;
const DRAIN_SLICE_MAX: usize = 1024;
const DRAIN_TARGET_MILLIS: u64 = 2_000;
/// Waits between two failed reclaim calls: they start at 30 s and double to 15 minutes.
const RECLAIM_FAILURE_MIN_SECS: u64 = 30;
const RECLAIM_FAILURE_MAX_SECS: u64 = 900;
/// Longest wait before a restart of a worker that panicked.
const RESTART_MAX_SECS: u64 = 60;

impl PackRuntime {
    /// Start the maintenance worker on first use. A runtime that opened above the
    /// trigger starts its first run here. A supervisor restarts the worker after a panic.
    pub(in crate::storage::disk_pack) fn ensure_parts_worker(self: &Arc<Self>) {
        if !self.parts.auto
            || self.parts.worker.load(Relaxed)
            || self.parts.worker.swap(true, Relaxed)
        {
            return;
        }
        let ledger = Arc::clone(&self.parts);
        let runtime = Arc::downgrade(self);
        tokio::spawn(
            supervise(ledger, runtime).instrument(tracing::info_span!("native_parts_supervisor")),
        );
        if self.parts.wants_maintenance() {
            self.parts.wake.notify_one();
        }
    }

    /// Backpressure before the hard limit: above 90% of the limit a new write
    /// waits for the worker, for at most a minute, and then proceeds to its own
    /// admission, which refuses it with a retry hint if no room appeared.
    /// Call it before the write gate: maintenance needs that gate to make room.
    pub(in crate::storage::disk_pack) async fn wait_for_part_room(
        self: &Arc<Self>,
    ) -> StorageResult<()> {
        self.ensure_parts_worker();
        let ledger = &self.parts;
        if ledger.auto && ledger.over_soft_limit() {
            ledger.wake.notify_one();
            return Err(StorageError::WriteResourceLimit {
                detail: "compaction_behind: storage parts reached the 85 percent soft limit; retry after maintenance".into(),
            });
        }
        Ok(())
    }

    /// Wake the worker when a write was refused for part capacity.
    pub(in crate::storage::disk_pack) fn note_capacity_refusal(&self, error: &laststore::Error) {
        if matches!(error, laststore::Error::WriteCapacity(reason) if reason.contains("dependency capacity"))
        {
            self.parts.wake_for_refusal();
        }
    }
}

/// Runs the worker and starts it again after a panic. A panic would otherwise end
/// the task for good, with the ledger still reporting a worker and every writer
/// above the soft limit waiting a minute for it.
async fn supervise(ledger: Arc<PartsLedger>, runtime: Weak<PackRuntime>) {
    let mut restarts = 0u32;
    loop {
        let runs = ledger.runs.load(Relaxed);
        ledger.alive.store(true, Relaxed);
        let task = tokio::spawn(
            parts_worker(Arc::clone(&ledger), runtime.clone())
                .instrument(tracing::info_span!("native_parts_worker")),
        );
        let outcome = task.await;
        ledger.alive.store(false, Relaxed);
        ledger.running.store(false, Relaxed);
        let error = match outcome {
            // The runtime is gone, or the process shuts down.
            Ok(()) => return,
            Err(error) if error.is_cancelled() => return,
            Err(error) => error,
        };
        let detail = panic_detail(error);
        tracing::error!(%detail, "native storage maintenance worker panicked; restarting");
        ledger.record_worker_panic(&detail);
        if ledger.runs.load(Relaxed) > runs {
            restarts = 0;
        }
        restarts = restarts.saturating_add(1);
        let pause = (1u64 << restarts.min(6)).min(RESTART_MAX_SECS);
        tokio::time::sleep(Duration::from_secs(pause)).await;
        if runtime.strong_count() == 0 {
            return;
        }
        // Writers above the soft limit wait for a run, so the restart starts one.
        ledger.wake.notify_one();
    }
}

fn panic_detail(error: tokio::task::JoinError) -> String {
    let payload = error.into_panic();
    payload
        .downcast_ref::<&str>()
        .map(|text| (*text).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-text panic".to_string())
}

/// What the worker still owes the home after a pack or a drain call.
struct Debt {
    /// When the next reclaim call is due. `None`: no old generation waits.
    reclaim_at: Option<Instant>,
    deferrals: u32,
    failures: u32,
    slice: usize,
    /// The orphan sweep is due once no old generation waits. A crash can leave
    /// part files that no receipt names, so each process sweeps once after its drain.
    sweep: bool,
    sweep_deferrals: u32,
    sweep_at: Instant,
}

impl Debt {
    /// A stop or a crash can leave old generations from an earlier run.
    fn at_start() -> Self {
        Self {
            reclaim_at: Some(Instant::now()),
            deferrals: 0,
            failures: 0,
            slice: DRAIN_SLICE_START,
            sweep: true,
            sweep_deferrals: 0,
            sweep_at: Instant::now(),
        }
    }

    /// Time to the next step that is not a pack. `None` waits for a wake.
    fn wait(&self) -> Option<Duration> {
        let now = Instant::now();
        let at = match (self.reclaim_at, self.sweep.then_some(self.sweep_at)) {
            (Some(reclaim), _) => reclaim,
            (None, Some(sweep)) => sweep,
            (None, None) => return None,
        };
        Some(at.saturating_duration_since(now))
    }

    fn reclaim_due(&self) -> bool {
        self.reclaim_at.is_some_and(|at| at <= Instant::now())
    }

    /// The sweep follows the drain: it waits while any old generation waits.
    fn sweep_due(&self) -> bool {
        self.reclaim_at.is_none() && self.sweep && self.sweep_at <= Instant::now()
    }

    /// Record the result of a pack or a drain call.
    fn after_call(&mut self, report: &NativeMaintenanceReport, pack: bool) {
        if report.reclaim_error.is_some() {
            self.failed();
        } else if report.reclaim_deferred {
            self.deferred();
        } else if report.reclaim_remaining && report.files_removed == 0 {
            // Old generations remain and the call removed nothing: a repeat would
            // do the same work again. Wait like a failed call instead of looping.
            tracing::warn!("native storage reclaim made no progress; it waits before its next try");
            self.failed();
        } else {
            self.failures = 0;
            self.deferrals = 0;
            self.reclaim_at = report.reclaim_remaining.then(Instant::now);
        }
        if !pack && !report.reclaim_deferred && report.reclaim_error.is_none() {
            self.adapt(report.reclaim_millis);
        }
    }

    /// A deferral means that another process holds the home for a read or a backup.
    /// No pack waits for it, and the retries wait on a timer.
    fn deferred(&mut self) {
        if self.deferrals >= RECLAIM_RETRIES {
            tracing::warn!("native storage reclaim stayed deferred; it waits for the next pack");
            self.reclaim_at = None;
            self.deferrals = 0;
            return;
        }
        let pause = (1u64 << self.deferrals.min(5)).min(30);
        self.deferrals += 1;
        self.reclaim_at = Some(Instant::now() + Duration::from_secs(pause));
    }

    /// A failed reclaim repeats on a long back-off. The named error stays in the
    /// status, and the trigger of the next pack rises while it fails.
    fn failed(&mut self) {
        let pause = RECLAIM_FAILURE_MIN_SECS
            .saturating_mul(1u64 << self.failures.min(5))
            .min(RECLAIM_FAILURE_MAX_SECS);
        self.failures = self.failures.saturating_add(1);
        self.reclaim_at = Some(Instant::now() + Duration::from_secs(pause));
    }

    fn adapt(&mut self, millis: u64) {
        if millis < DRAIN_TARGET_MILLIS / 2 {
            self.slice = (self.slice * 2).min(DRAIN_SLICE_MAX);
        } else if millis > DRAIN_TARGET_MILLIS * 2 {
            self.slice = (self.slice / 2).max(DRAIN_SLICE_MIN);
        }
    }
}

async fn parts_worker(ledger: Arc<PartsLedger>, runtime: Weak<PackRuntime>) {
    let mut debt = Debt::at_start();
    let mut pack_failures = 0u32;
    let mut booting = true;
    // A pack that did not shrink the count waits for the next write.
    let mut held = false;
    loop {
        let pack_due = !held && ledger.wants_maintenance();
        if !pack_due && !debt.reclaim_due() && !debt.sweep_due() {
            if let Some(pause) = debt.wait() {
                tokio::select! {
                    () = ledger.wake.notified() => held = false,
                    () = tokio::time::sleep(pause) => {}
                }
            } else {
                ledger.wake.notified().await;
                held = false;
            }
            continue;
        }
        let Some(strong) = runtime.upgrade() else {
            return;
        };
        if std::mem::take(&mut booting) {
            // Let the node finish its own start before the first step holds the gate.
            // A home above the soft limit cannot take a start-up write, so it does not wait.
            boot_delay(&ledger).await;
            continue;
        }
        if pack_due {
            match run_once(&strong, &ledger, Call::Pack).await {
                Ok(report) => {
                    pack_failures = 0;
                    tracing::info!(
                        parts_before = report.parts_before,
                        parts_after = report.parts_after,
                        files_removed = report.files_removed,
                        pack_millis = report.pack_millis,
                        reclaim_millis = report.reclaim_millis,
                        "native storage maintenance reached the dependency high-water mark"
                    );
                    held = report.parts_after >= report.parts_before;
                    debt.after_call(&report, true);
                }
                Err(error) => {
                    pack_failures = pack_failures.saturating_add(1);
                    tracing::warn!(%error, failures = pack_failures, "native storage maintenance failed");
                    let pause = 1u64 << pack_failures.min(6);
                    tokio::time::sleep(Duration::from_secs(pause.min(MAX_BACKOFF_SECS))).await;
                }
            }
        } else if debt.reclaim_due() {
            let slice = debt.slice;
            match run_once(&strong, &ledger, Call::Drain(slice)).await {
                Ok(report) => {
                    ledger.add_run_millis(report.reclaim_millis);
                    debt.after_call(&report, false);
                }
                Err(error) => {
                    tracing::warn!(%error, "native storage reclaim failed");
                    debt.failed();
                }
            }
        } else {
            sweep_orphans(&strong, &ledger, &mut debt).await;
        }
        ledger
            .reclaim_pending
            .store(debt.reclaim_at.is_some(), Relaxed);
        // A step ends, and a writer that waited for the gate runs before the next step.
        tokio::task::yield_now().await;
    }
}

/// Wait before the first step of a process. A write that takes the count over the
/// soft limit ends the wait: that write cannot proceed without a pack.
async fn boot_delay(ledger: &PartsLedger) {
    let deadline = Instant::now() + Duration::from_secs(ledger.start_delay_secs);
    while !ledger.over_soft_limit() {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return;
        }
        tokio::time::sleep(left.min(Duration::from_secs(1))).await;
    }
}

enum Call {
    /// Pack the live set, then retire a first slice of old generations.
    Pack,
    /// Retire old generations only, at most this many.
    Drain(usize),
}

/// One bounded maintenance call. Each call holds the write gate and the view
/// lock for a bounded time, so waiting writers and readers run between calls.
async fn run_once(
    runtime: &Arc<PackRuntime>,
    ledger: &PartsLedger,
    call: Call,
) -> StorageResult<NativeMaintenanceReport> {
    ledger.running.store(true, Relaxed);
    let (reclaim_only, generations) = match call {
        Call::Pack => (false, PACK_SLICE_GENERATIONS),
        Call::Drain(generations) => (true, generations),
    };
    let options = NativeMaintenanceOptions {
        dry_run: false,
        reclaim_only,
        grace_ms: Some(ledger.grace_ms),
        report_usage: false,
        reclaim_generations: Some(generations),
    };
    let result = runtime.maintain_native_for_worker(options).await;
    ledger.running.store(false, Relaxed);
    result
}

/// A crash can leave output files that no receipt names. Remove those older than
/// `LASTDB_ORPHAN_MIN_AGE_MS` (default ten minutes), or the grace when that is longer.
/// Another process that holds the home defers the sweep, so it repeats on a timer.
async fn sweep_orphans(runtime: &Arc<PackRuntime>, ledger: &PartsLedger, debt: &mut Debt) {
    let min_age = ledger.grace_ms.max(ledger.orphan_min_age_ms);
    match runtime.sweep_orphans_native(min_age).await {
        Ok((_, _, true)) => {
            if debt.sweep_deferrals >= RECLAIM_RETRIES {
                tracing::warn!("native storage orphan sweep stayed deferred");
                debt.sweep = false;
                return;
            }
            let pause = (1u64 << debt.sweep_deferrals.min(5)).min(30);
            debt.sweep_deferrals += 1;
            debt.sweep_at = Instant::now() + Duration::from_secs(pause);
        }
        Ok((files, bytes, false)) => {
            if files > 0 {
                tracing::info!(
                    files,
                    bytes,
                    "native storage removed unreferenced part files"
                );
            }
            debt.sweep = false;
        }
        Err(error) => {
            tracing::warn!(%error, "native storage orphan sweep failed");
            debt.sweep = false;
        }
    }
}
