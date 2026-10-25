//! One task for high-water packing and periodic obsolete-generation reclaim.
//!
//! The worker is a loop over bounded steps. A pack has priority: it runs whenever
//! the count passes the trigger, even while old generations wait for reclaim or
//! a reclaim waits for its next try. Reclaim, the drain of old generations, and
//! the orphan sweep run only when no pack is due. Cleanup captures the installed
//! root under the writer mutex, then inventories and deletes files without it.

use super::retry::{after_pack, MAX_BACKOFF_SECS};
use super::*;
use std::time::{Duration, Instant};
use tracing::Instrument;

/// Tries of a reclaim that another process deferred. The waits grow from 1 s to 30 s,
/// so the worker tries for about 20 minutes before it returns to the idle interval.
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
/// Obsolete receipts can accrue while the current live-part count stays flat.
const IDLE_RECLAIM_SECS: u64 = 30;
/// Longest wait before a restart of a worker that panicked.
const RESTART_MAX_SECS: u64 = 60;

impl PackRuntime {
    /// Start the maintenance worker on first use. A runtime that opened above the
    /// trigger starts its first run here. A supervisor restarts the worker after a panic.
    pub(in crate::storage::disk_pack) fn ensure_parts_worker(self: &Arc<Self>) {
        // A v2 home has no dependency parts; its merges run after commits.
        if !self.parts.auto
            || self.is_disk_format_v2()
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

    /// Refuse new work at the 85-percent soft limit and wake the worker.
    /// The caller retries after maintenance; no admission waits behind the copy.
    pub(in crate::storage::disk_pack) fn check_part_room(self: &Arc<Self>) -> StorageResult<()> {
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
/// the task for good, with the ledger still reporting a worker while admission
/// above the soft limit remains refused.
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
        // Admission above the soft limit needs room, so the restart starts a run.
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

/// Explicit drain work and periodic reclaim, independent of live-part growth.
struct Debt {
    /// Explicit drain or retry deadline. It takes precedence over the idle timer.
    reclaim_at: Option<Instant>,
    idle_reclaim_at: Instant,
    deferrals: u32,
    failures: u32,
    slice: usize,
    /// The orphan sweep follows the drain and repeats. Superseded private
    /// payloads and interrupted output can leave parts that no receipt names.
    sweep: bool,
    sweep_deferrals: u32,
    sweep_at: Instant,
}

impl Debt {
    /// A stop or a crash can leave old generations from an earlier run.
    fn at_start() -> Self {
        let now = Instant::now();
        Self {
            reclaim_at: Some(now),
            idle_reclaim_at: now + Duration::from_secs(IDLE_RECLAIM_SECS),
            deferrals: 0,
            failures: 0,
            slice: DRAIN_SLICE_START,
            sweep: true,
            sweep_deferrals: 0,
            sweep_at: now,
        }
    }

    /// An explicit retry keeps its backoff; otherwise the earliest idle or sweep
    /// deadline wakes the worker without a publication notification.
    fn wait(&self) -> Duration {
        let now = Instant::now();
        let at = match (self.reclaim_at, self.sweep.then_some(self.sweep_at)) {
            (Some(reclaim), _) => reclaim,
            (None, Some(sweep)) => sweep.min(self.idle_reclaim_at),
            (None, None) => self.idle_reclaim_at,
        };
        at.saturating_duration_since(now)
    }

    fn reclaim_due(&self) -> bool {
        self.reclaim_at.unwrap_or(self.idle_reclaim_at) <= Instant::now()
    }

    /// The sweep follows the drain: it waits while any old generation waits.
    fn sweep_due(&self) -> bool {
        self.reclaim_at.is_none() && self.sweep && self.sweep_at <= Instant::now()
    }

    /// Record the result of a pack or a drain call.
    fn after_call(&mut self, report: &NativeMaintenanceReport, pack: bool) {
        self.idle_reclaim_at = Instant::now() + Duration::from_secs(IDLE_RECLAIM_SECS);
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
            tracing::warn!(
                "native storage reclaim stayed deferred; it waits for the next idle interval or pack"
            );
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
    // A normal pack that did not shrink waits for a write. Soft-limit recovery
    // cannot wait for writes, because admission refuses them.
    let mut held = false;
    loop {
        let pack_due = (!held || ledger.over_soft_limit()) && ledger.wants_maintenance();
        if !pack_due && !debt.reclaim_due() && !debt.sweep_due() {
            tokio::select! {
                () = ledger.wake.notified() => held = false,
                () = tokio::time::sleep(debt.wait()) => {}
            }
            continue;
        }
        let Some(strong) = runtime.upgrade() else {
            return;
        };
        if std::mem::take(&mut booting) {
            // Let the node finish its own start before the first maintenance step.
            // A home above the soft limit cannot take a start-up write, so it does not wait.
            // The pause needs only the ledger, not ownership of the open home.
            drop(strong);
            boot_delay(&ledger).await;
            continue;
        }
        if pack_due {
            match run_once(&strong, &ledger, Call::Pack).await {
                Ok(report) => {
                    tracing::info!(
                        parts_before = report.parts_before,
                        parts_after = report.parts_after,
                        files_removed = report.files_removed,
                        pack_millis = report.pack_millis,
                        reclaim_millis = report.reclaim_millis,
                        "native storage maintenance reached the dependency high-water mark"
                    );
                    held = after_pack(&ledger, &report, &mut pack_failures).await;
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

/// One maintenance call. Copy and reclaim run outside the write gate. Only
/// conditional publication takes it, so ordinary writes proceed during the copy.
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
        // A recent copy retains the older grafted base. Once admission refuses
        // writes, a full copy can consolidate that quiesced source without growth.
        full_copy: !reclaim_only && ledger.over_soft_limit(),
        grace_ms: Some(ledger.grace_ms),
        report_usage: false,
        reclaim_generations: Some(generations),
    };
    let result = runtime.maintain_native_for_worker(options).await;
    ledger.running.store(false, Relaxed);
    result
}

/// Private output can leave files that no receipt names. Remove those older than
/// `LASTDB_ORPHAN_MIN_AGE_MS` (default ten minutes), or the grace when that is longer.
/// Repeat after 30..600 seconds, independent of the unchanged deletion-age cutoff.
/// A deferral keeps the existing shorter retry deadline until its retry limit.
async fn sweep_orphans(runtime: &Arc<PackRuntime>, ledger: &PartsLedger, debt: &mut Debt) {
    let min_age = ledger.grace_ms.max(ledger.orphan_min_age_ms);
    let result = runtime.sweep_orphans_native(min_age).await;
    debt.sweep_at = Instant::now() + Duration::from_millis(min_age.clamp(30_000, 600_000));
    match result {
        Ok((_, _, true)) => {
            if debt.sweep_deferrals >= RECLAIM_RETRIES {
                tracing::warn!("native storage orphan sweep stayed deferred");
                debt.sweep_deferrals = 0;
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
            debt.sweep_deferrals = 0;
        }
        Err(error) => {
            tracing::warn!(%error, "native storage orphan sweep failed");
            debt.sweep_deferrals = 0;
        }
    }
}
