//! The single task that runs maintenance when the part count passes its mark.

use super::*;
use tracing::Instrument;

/// Longest pause between two failed maintenance attempts.
const MAX_BACKOFF_SECS: u64 = 60;
/// Retries of a reclaim that active reads deferred. The waits grow from 1 s to 30 s,
/// so the worker keeps trying for about 20 minutes before it gives up until its next run.
const RECLAIM_RETRIES: u32 = 40;
/// Old generations one reclaim call retires. A call plans from their receipts,
/// so the bound also bounds the call's reads.
const RECLAIM_SLICE_GENERATIONS: usize = 32;

impl PackRuntime {
    /// Start the single maintenance worker on first use. A runtime that opened
    /// above the trigger starts its first run here.
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
            parts_worker(ledger, runtime).instrument(tracing::info_span!("native_parts_worker")),
        );
        if self.parts.wants_maintenance() {
            self.parts.wake.notify_one();
        }
    }

    /// Backpressure before the hard limit: above 90% of the limit a new write
    /// waits for the worker, for at most a minute, and then proceeds to its own
    /// admission, which refuses it with a retry hint if no room appeared.
    /// Call it before the write gate: maintenance needs that gate to make room.
    pub(in crate::storage::disk_pack) async fn wait_for_part_room(self: &Arc<Self>) {
        self.ensure_parts_worker();
        let ledger = &self.parts;
        if !ledger.auto || !ledger.over_soft_limit() {
            return;
        }
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(60);
        while ledger.over_soft_limit()
            && !ledger.failing.load(Relaxed)
            && tokio::time::Instant::now() < deadline
        {
            ledger.wake.notify_one();
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    /// Wake the worker when a write was refused for part capacity.
    pub(in crate::storage::disk_pack) fn note_capacity_refusal(&self, error: &laststore::Error) {
        if matches!(error, laststore::Error::WriteCapacity(reason) if reason.contains("dependency capacity"))
        {
            self.parts.wake_for_refusal();
        }
    }
}

async fn parts_worker(ledger: Arc<PartsLedger>, runtime: Weak<PackRuntime>) {
    let mut failures = 0u32;
    // A stop or a crash can leave old generations from an earlier run. Retire them
    // now, a slice at a time, instead of waiting for the next high-water run.
    if let Some(strong) = runtime.upgrade() {
        if let Ok(report) = run_once(&strong, &ledger, true).await {
            if finish_reclaim(&strong, &ledger, report).await {
                sweep_orphans(&strong, &ledger).await;
            }
        }
    }
    let mut booting = true;
    loop {
        ledger.wake.notified().await;
        if std::mem::take(&mut booting) && !ledger.over_soft_limit() {
            // Let the node finish its own start before the first pack holds the gate.
            // A home above the soft limit cannot take a start-up write, so it does not wait.
            tokio::time::sleep(std::time::Duration::from_secs(ledger.start_delay_secs)).await;
        }
        while ledger.wants_maintenance() {
            let Some(strong) = runtime.upgrade() else {
                return;
            };
            match run_once(&strong, &ledger, false).await {
                Ok(report) => {
                    failures = 0;
                    tracing::info!(
                        parts_before = report.parts_before,
                        parts_after = report.parts_after,
                        files_removed = report.files_removed,
                        pack_millis = report.pack_millis,
                        reclaim_millis = report.reclaim_millis,
                        "native storage maintenance reached the dependency high-water mark"
                    );
                    let shrank = report.parts_after < report.parts_before;
                    finish_reclaim(&strong, &ledger, report).await;
                    if !shrank {
                        break;
                    }
                }
                Err(error) => {
                    failures = failures.saturating_add(1);
                    tracing::warn!(%error, failures, "native storage maintenance failed");
                    let pause = 1u64 << failures.min(6);
                    tokio::time::sleep(std::time::Duration::from_secs(pause.min(MAX_BACKOFF_SECS)))
                        .await;
                }
            }
        }
    }
}

/// One bounded maintenance call. Each call holds the write gate and the view
/// lock for a bounded time, so waiting writers and readers run between calls.
async fn run_once(
    runtime: &Arc<PackRuntime>,
    ledger: &PartsLedger,
    reclaim_only: bool,
) -> StorageResult<NativeMaintenanceReport> {
    ledger.running.store(true, Relaxed);
    let options = NativeMaintenanceOptions {
        dry_run: false,
        reclaim_only,
        grace_ms: Some(ledger.grace_ms),
        report_usage: false,
        reclaim_generations: Some(RECLAIM_SLICE_GENERATIONS),
    };
    let result = runtime.maintain_native(options).await;
    ledger.running.store(false, Relaxed);
    result
}

/// The replacement is already published. Retire the old generations that the
/// first call left, a slice at a time, and repeat a reclaim that active reads
/// deferred until the read ends.
async fn finish_reclaim(
    runtime: &Arc<PackRuntime>,
    ledger: &PartsLedger,
    mut report: NativeMaintenanceReport,
) -> bool {
    let mut deferrals = 0u32;
    loop {
        if report.reclaim_deferred {
            if deferrals >= RECLAIM_RETRIES {
                tracing::warn!("native storage reclaim stayed deferred by active reads");
                return false;
            }
            let pause = (1u64 << deferrals).min(30);
            deferrals += 1;
            tokio::time::sleep(std::time::Duration::from_secs(pause)).await;
        } else if report.reclaim_remaining {
            tokio::task::yield_now().await;
        } else {
            return true;
        }
        match run_once(runtime, ledger, true).await {
            Ok(next) => {
                ledger.add_run_millis(next.reclaim_millis);
                report = next;
            }
            Err(error) => {
                tracing::warn!(%error, "native storage reclaim failed");
                return false;
            }
        }
    }
}

/// A crash can leave output files that no receipt names. Remove those older than
/// `LASTDB_ORPHAN_MIN_AGE_MS` (default ten minutes), or the grace when that is longer.
/// Active reads defer the sweep, so it repeats like a deferred reclaim.
async fn sweep_orphans(runtime: &Arc<PackRuntime>, ledger: &PartsLedger) {
    let min_age = ledger.grace_ms.max(ledger.orphan_min_age_ms);
    for attempt in 0..RECLAIM_RETRIES {
        match runtime.sweep_orphans_native(min_age).await {
            Ok((_, _, true)) => {
                let pause = (1u64 << attempt.min(5)).min(30);
                tokio::time::sleep(std::time::Duration::from_secs(pause)).await;
            }
            Ok((files, bytes, false)) => {
                if files > 0 {
                    tracing::info!(
                        files,
                        bytes,
                        "native storage removed unreferenced part files"
                    );
                }
                return;
            }
            Err(error) => {
                tracing::warn!(%error, "native storage orphan sweep failed");
                return;
            }
        }
    }
    tracing::warn!("native storage orphan sweep stayed deferred by active reads");
}
