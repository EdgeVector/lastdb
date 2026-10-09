//! SyncCoordinator - manages the optional cloud sync engine lifecycle.
//!
//! Sync is opt-in. In local mode, the coordinator holds no engine and all
//! operations are no-ops (or return None). This type encapsulates the
//! interior mutability (RwLock + Mutex) needed so FoldDB can expose sync
//! operations via `&self`.

use crate::clock::unix_secs;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};

use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use std::sync::Arc;

use crate::db_operations::{
    AtomGcAuditDecision, AtomGcReapOptions, AtomGcReapReport, AtomStore,
    AutomaticGcAtomsDeleteOptions, AutomaticGcAtomsProbeOptions, AutomaticGcAtomsProbeResult,
    DbOperations, DEFAULT_ATOM_GC_GRACE_WINDOW,
};
use crate::sync::{SyncEngine, SyncError, SyncState, SyncStatus};

/// Cap on the exponential backoff between sync cycles while the engine is in
/// [`SyncState::Offline`]. Ten minutes is a balance between responsiveness
/// (user opens the lid, expects sync within a reasonable time) and not
/// hammering Exemem / the device battery during a sustained outage.
const MAX_OFFLINE_BACKOFF: tokio::time::Duration = tokio::time::Duration::from_secs(600);

/// Cap on the exponential backoff between sync cycles while authentication is
/// failing. Auth failures are usually operator-actionable (revoked key, missing
/// locked credential store), so probe much less often than transient network
/// outages after the first few failures.
const MAX_AUTH_BACKOFF: tokio::time::Duration = tokio::time::Duration::from_secs(3600);

/// Cap on backoff after replay reaches a corrupt/undecryptable log entry.
///
/// Unlike transient network failures, a replay failure usually cannot make
/// progress until cloud state or local key material changes. Keep the cursor
/// pinned before the bad entry, but do not let write wakeups turn the same
/// corrupt object into a continuous retry loop.
const MAX_REPLAY_BACKOFF: tokio::time::Duration = tokio::time::Duration::from_secs(3600);

/// Minimum spacing between forced syncs (`force_sync` / `stop`) WHILE a replay
/// blocker is armed. A UI/API "sync now" must not be able to hot-retrigger
/// replay of the same corrupt/undecryptable object faster than the failure
/// backoff would — that is how a poisoned prefix turned into a busy-loop before
/// PR #304's backoff, and force_sync bypasses that backoff entirely.
const MIN_FORCED_SYNC_UNDER_BLOCKER: std::time::Duration = std::time::Duration::from_secs(30);

/// Seconds between local plane self-compaction probes.
///
/// A probe is a per-plane directory stat walk plus the per-plane rate limits
/// each trigger already enforces, so the cadence can be far shorter than the
/// probe intervals themselves without adding rewrites: a plane whose own
/// interval has not elapsed returns immediately. 60 s matches the tightest
/// useful reaction time on a busy local node while staying an order of
/// magnitude above the walk's cost.
///
/// `LASTDB_PLANE_COMPACTION_INTERVAL_SECS=0` disables the cadence and hands
/// reclaim back to `lastdb db compact --collection <plane> --execute`.
const DEFAULT_PLANE_COMPACTION_INTERVAL_SECS: u64 = 60;

/// Read the local plane-compaction cadence once, at start.
fn plane_compaction_interval_secs() -> u64 {
    env_flag::var_or(
        "LASTDB_PLANE_COMPACTION_INTERVAL_SECS",
        DEFAULT_PLANE_COMPACTION_INTERVAL_SECS,
    )
}

mod gc_atoms;
use gc_atoms::*;

fn capture_reexport_drain_delay(
    stats: &crate::sync::engine::CaptureTickStats,
    lap_progress: &mut bool,
) -> tokio::time::Duration {
    let made_progress =
        stats.puts_staged > 0 || stats.deletes_staged > 0 || stats.poison_dropped > 0;
    *lap_progress |= made_progress;
    if stats.scan_more {
        return tokio::time::Duration::from_secs(2);
    }
    let delay = if *lap_progress {
        tokio::time::Duration::from_secs(2)
    } else {
        // A complete lap that could not retire one marker must not retry the
        // same failed rows every two seconds.
        tokio::time::Duration::from_secs(30)
    };
    *lap_progress = false;
    delay
}

/// Coordinates the optional cloud sync engine lifecycle.
pub struct SyncCoordinator {
    engine: Arc<RwLock<Option<Arc<SyncEngine>>>>,
    task: Mutex<Option<JoinHandle<()>>>,
    /// Bounded marker-plane drain. It runs outside the serial cloud target
    /// cycle so a large residual queue does not wait for 40 scoped uploads.
    capture_reexport_task: Mutex<Option<JoinHandle<()>>>,
    /// Local gc-atoms cadence. Independent of `engine.sync()` success.
    gc_atoms_task: Mutex<Option<JoinHandle<()>>>,
    /// Local plane self-compaction cadence. Independent of cloud sync: a node
    /// booted without `cloud_sync.json` runs the identical sweeps.
    plane_compaction_task: Mutex<Option<JoinHandle<()>>>,
    /// The compactor the local cadence steps while no engine is configured.
    /// Kept so `lastdb status` can read its last-fire reports.
    local_plane_compactor: RwLock<Option<Arc<crate::sync::capture::PlaneCompactor>>>,
    /// One packing lock for the process. Local sweeps and a later
    /// `SyncEngine` cut must share this Arc; a private dummy mutex at
    /// cadence start is the split-lock bug.
    backup_publish_target: crate::sync::capture::plane_compactor::BackupPublishTargetSlot,
    /// Cloud-pause slot paired with [`Self::backup_publish_target`].
    cloud_sync_disabled_at: Arc<tokio::sync::Mutex<Option<u64>>>,
    /// Last time a forced sync was actually dispatched while a replay blocker
    /// was armed — used to rate-limit `force_sync` under a blocker.
    last_forced_under_blocker: Mutex<Option<std::time::Instant>>,
}

impl SyncCoordinator {
    pub fn new() -> Self {
        Self::new_with_packing(
            Arc::new(tokio::sync::Mutex::new(None)),
            Arc::new(tokio::sync::Mutex::new(None)),
        )
    }

    /// Build a coordinator that already shares packing slots with a
    /// `SyncEngine` created first (factory cloud-on-at-boot).
    pub(crate) fn new_with_packing(
        backup_publish_target: crate::sync::capture::plane_compactor::BackupPublishTargetSlot,
        cloud_sync_disabled_at: Arc<tokio::sync::Mutex<Option<u64>>>,
    ) -> Self {
        Self {
            engine: Arc::new(RwLock::new(None)),
            task: Mutex::new(None),
            capture_reexport_task: Mutex::new(None),
            gc_atoms_task: Mutex::new(None),
            plane_compaction_task: Mutex::new(None),
            local_plane_compactor: RwLock::new(None),
            backup_publish_target,
            cloud_sync_disabled_at,
            last_forced_under_blocker: Mutex::new(None),
        }
    }

    /// Packing lock and cloud-pause slot for [`SyncEngine::new_with_shared_packing`].
    pub(crate) fn packing_slots(
        &self,
    ) -> (
        crate::sync::capture::plane_compactor::BackupPublishTargetSlot,
        Arc<tokio::sync::Mutex<Option<u64>>>,
    ) {
        (
            Arc::clone(&self.backup_publish_target),
            Arc::clone(&self.cloud_sync_disabled_at),
        )
    }

    /// Store the sync engine. Caller is responsible for registering any
    /// reloader callbacks on the engine before calling this.
    pub fn set_engine(&self, engine: Arc<SyncEngine>) {
        *self.engine.write().unwrap() = Some(engine);
    }

    /// Returns a clone of the sync engine Arc, if configured.
    pub fn engine(&self) -> Option<Arc<SyncEngine>> {
        self.engine.read().unwrap().clone()
    }

    /// Returns true if a sync engine is configured.
    pub fn is_enabled(&self) -> bool {
        self.engine.read().unwrap().is_some()
    }

    /// Spawn the background sync timer task. No-op if no engine is configured.
    ///
    /// ### Offline backoff
    ///
    /// While the engine state is [`SyncState::Offline`] (i.e. the last sync
    /// failed with a network-class error), the inter-cycle delay doubles on
    /// each consecutive failure — `interval_ms`, 2×, 4×, … — capped at
    /// [`MAX_OFFLINE_BACKOFF`] (10 minutes). On the next successful cycle the
    /// delay resets to `interval_ms`. This matters for laptops resuming from
    /// sleep and phones with flaky connectivity: without the backoff, the
    /// coordinator would hammer the presign endpoint every `interval_ms` for
    /// the entire offline window (CPU and battery cost, plus noisy retries on
    /// cold Lambda).
    ///
    /// Auth failures also back off. The engine still does an immediate
    /// refresh-and-retry inside a single cycle, but once that fails, repeating
    /// the same rejected credential or locked credential-store read every base
    /// interval can exhaust local resources without making progress.
    ///
    /// Idempotent: a second call while a loop is already running is a no-op.
    /// The `task` slot is checked and reserved under one lock acquisition so a
    /// racing caller (e.g. two near-simultaneous `start_sync_engine_runtime`
    /// calls) cannot both observe an empty slot and both spawn a loop — the
    /// older loop would then be unabortable, since `abort_task`/`stop` can
    /// only abort whatever handle is currently stored.
    pub fn start_background_sync(&self, interval_ms: u64) {
        let mut slot = self.task.lock().unwrap();
        if slot.is_some() {
            return;
        }

        let engine = match &*self.engine.read().unwrap() {
            Some(e) => Arc::clone(e),
            None => return,
        };

        let wake = engine.wake_handle();

        // lint:spawn-bare-ok boot-time sync poll loop — perpetual worker, no per-request parent span.
        let handle = tokio::spawn(async move {
            let base_interval = tokio::time::Duration::from_millis(interval_ms);
            let max_delay = MAX_OFFLINE_BACKOFF;
            let mut current_delay = base_interval;
            let mut suppress_wake_once = false;
            loop {
                // Sleep up to `current_delay`, or wake early if a local write
                // arrived. A write fires `engine.wake.notify_one()`, which
                // resolves the `notified()` future and aborts the timeout so
                // the flush happens near-immediately instead of waiting the
                // full polling interval. `timeout` returns `Err` on the timer
                // path and `Ok(())` on the wake path — either way, the same
                // check-and-sync logic below fires.
                if suppress_wake_once {
                    tokio::time::sleep(current_delay).await;
                } else {
                    let _ = tokio::time::timeout(current_delay, wake.notified()).await;
                }
                // Always call sync() when a sync engine is configured. A
                // passive reader on a personal prefix (another device
                // restored from the same mnemonic) needs the poll to see
                // peer writes, even when locally clean and without org
                // memberships. Previous "skip if nothing to upload and no
                // orgs" check broke multi-device convergence — it matched
                // an equivalent bailout inside `sync()` that #607 removed,
                // but that was only half the fix. `sync()` is cheap on a
                // no-op cycle (one list request per target with an
                // already-advanced cursor → typically 0 matches).
                //
                // Automatic gc-atoms does **not** run here. A stalled or
                // failed sync cycle must not starve the local cadence; that
                // timer starts from [`Self::start_automatic_gc_atoms`].
                match engine.sync().await {
                    Ok(_) => {
                        // A coalesce hold asks for a short retry so the group
                        // flushes when the quiet window or the max hold ends.
                        // A write wake still aborts this wait. `0` keeps the
                        // normal interval.
                        let retry_ms = engine
                            .mutation_log_coalesce_retry_ms
                            .load(std::sync::atomic::Ordering::Acquire);
                        current_delay = if retry_ms > 0 {
                            tokio::time::Duration::from_millis(retry_ms)
                        } else {
                            base_interval
                        };
                        suppress_wake_once = false;
                    }
                    Err(e) => {
                        warn!("sync cycle failed: {e}");
                        let suppress_wake = failure_should_suppress_wakeups(&e);
                        current_delay = next_backoff_on_failure(
                            current_delay,
                            base_interval,
                            max_delay,
                            MAX_AUTH_BACKOFF,
                            &e,
                        );
                        suppress_wake_once = suppress_wake;
                    }
                }
            }
        });

        *slot = Some(handle);
        drop(slot);
        self.start_capture_reexport_drain();
    }

    fn start_capture_reexport_drain(&self) {
        let mut slot = self.capture_reexport_task.lock().unwrap();
        if slot.is_some() {
            return;
        }
        let Some(engine) = self.engine() else {
            return;
        };
        if !matches!(
            engine.config.capture_mode,
            crate::sync::engine::CaptureMode::MutationLog
        ) {
            return;
        }
        // lint:spawn-bare-ok process-lifetime bounded capture drain — not request-scoped.
        let handle = tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            let mut lap_progress = false;
            loop {
                if !engine.should_stage_cloud_mutations().await {
                    lap_progress = false;
                    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                    continue;
                }
                let delay = match engine.run_capture_marker_tick().await {
                    Ok(stats) => capture_reexport_drain_delay(&stats, &mut lap_progress),
                    Err(error) => {
                        lap_progress = false;
                        warn!("capture re-export drain failed: {error}");
                        tokio::time::Duration::from_secs(30)
                    }
                };
                tokio::time::sleep(delay).await;
            }
        });
        *slot = Some(handle);
    }

    /// Start the local automatic `gc-atoms` cadence.
    ///
    /// This timer does not wait for [`SyncEngine::sync`] to succeed. A
    /// sync-disabled node and a node whose peer-apply is stalled still step.
    /// Idempotent: a second call is a no-op. A zero byte cap does not spawn.
    pub fn start_automatic_gc_atoms(&self, db_ops: Arc<DbOperations>) {
        let settings = AutomaticGcAtomsSettings::from_env();
        if !settings.enabled() {
            info!(
                target: "fold_db::gc_atoms",
                "automatic gc-atoms local cadence disabled (byte cap is 0)"
            );
            return;
        }
        let mut slot = self.gc_atoms_task.lock().unwrap();
        if slot.is_some() {
            return;
        }
        let engine_slot = Arc::clone(&self.engine);
        let interval = tokio::time::Duration::from_secs(settings.step_interval_secs.max(1));
        // lint:spawn-bare-ok process-lifetime local gc-atoms cadence — not request-scoped.
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // Skip the immediate first tick so FoldDB construction in tests
            // does not start a probe before the caller stores a first atom.
            ticker.tick().await;
            let last_step = AtomicU64::new(0);
            let restart_lap_at = AtomicU64::new(0);
            loop {
                ticker.tick().await;
                let engine = engine_slot.read().unwrap().clone();
                let deadline = tokio::time::Instant::now()
                    + tokio::time::Duration::from_secs(settings.step_budget_secs.max(1));
                let now = unix_secs();
                if let Some(engine) = engine {
                    advance_automatic_gc_atoms(
                        AutomaticGcAtomsRuntime {
                            atoms: db_ops.atoms(),
                            engine: Some(engine.as_ref()),
                        },
                        &last_step,
                        &restart_lap_at,
                        settings,
                        now,
                        deadline,
                        || async {
                            let guard = engine.lock_backup_publish_target().await;
                            if guard.is_some() {
                                None
                            } else {
                                Some(guard)
                            }
                        },
                    )
                    .await;
                } else {
                    advance_automatic_gc_atoms(
                        AutomaticGcAtomsRuntime {
                            atoms: db_ops.atoms(),
                            engine: None,
                        },
                        &last_step,
                        &restart_lap_at,
                        settings,
                        now,
                        deadline,
                        || async { Some(()) },
                    )
                    .await;
                }
            }
        });
        info!(
            target: "fold_db::gc_atoms",
            interval_secs = settings.step_interval_secs,
            step_budget_secs = settings.step_budget_secs,
            max_unreferenced_bytes = settings.max_unreferenced_bytes,
            "automatic gc-atoms local cadence started"
        );
        *slot = Some(handle);
    }

    /// Start the local automatic plane self-compaction cadence.
    ///
    /// Every sweep in [`crate::sync::capture::PlaneCompactor`] used to run only
    /// inside `SyncEngine::do_sync`. A node booted without `cloud_sync.json`
    /// has no engine and therefore compacted nothing: `tips` and `atoms` grew
    /// with no automatic byte-return path at all, while `gc-atoms` — which does
    /// have a local cadence — kept stepping and made the growth look tended.
    ///
    /// This timer does not wait for a sync cycle, exactly like
    /// [`Self::start_automatic_gc_atoms`]. Idempotent: a second call is a
    /// no-op. A zero probe interval does not spawn.
    ///
    /// Each tick prefers the *engine's* compactor when an engine exists, so a
    /// cloud-synced node keeps one set of trigger state and one packing lock
    /// rather than a second sweeper racing `do_sync`. The engine can be set
    /// after this timer starts (`cloud on`), so the choice is made per tick,
    /// not once at spawn.
    pub fn start_automatic_plane_compaction(&self, db_ops: &Arc<DbOperations>) {
        let interval_secs = plane_compaction_interval_secs();
        if interval_secs == 0 {
            info!(
                target: "fold_db::plane_compaction",
                "automatic plane self-compaction cadence disabled (probe interval is 0)"
            );
            return;
        }
        let mut slot = self.plane_compaction_task.lock().unwrap();
        if slot.is_some() {
            return;
        }
        let local = Arc::new(crate::sync::capture::PlaneCompactor::new(
            db_ops.namespaced_store(),
            Arc::clone(&self.backup_publish_target),
            Arc::clone(&self.cloud_sync_disabled_at),
        ));
        *self.local_plane_compactor.write().unwrap() = Some(Arc::clone(&local));
        let engine_slot = Arc::clone(&self.engine);
        let interval = tokio::time::Duration::from_secs(interval_secs);
        // lint:spawn-bare-ok process-lifetime local plane-compaction cadence — not request-scoped.
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // Skip the immediate first tick: a probe before the caller has
            // written anything measures an empty store and claims the
            // rate-limit slot for nothing.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let engine = engine_slot.read().unwrap().clone();
                if let Some(engine) = engine {
                    Arc::clone(&engine.compaction).run_all_sweeps().await;
                } else if engine_slot.read().unwrap().is_some() {
                    // `set_engine` won between the clone and this arm.
                    // Refuse the local sweep so a dummy-lock rewrite cannot
                    // start under a live cut. The next tick uses the engine.
                    continue;
                } else {
                    local.run_all_sweeps().await;
                }
            }
        });
        info!(
            target: "fold_db::plane_compaction",
            interval_secs,
            "automatic plane self-compaction local cadence started"
        );
        *slot = Some(handle);
    }

    /// Last-fire automatic compaction reports for `lastdb status`.
    ///
    /// Reads the engine's compactor when cloud sync is on and the local one
    /// otherwise, so the status line has one shape on both node shapes.
    /// `None` means no cadence has started in this process.
    pub async fn automatic_compaction_status(
        &self,
    ) -> Option<std::collections::BTreeMap<String, crate::sync::engine::AutomaticCompactionStatus>>
    {
        if let Some(engine) = self.engine() {
            return Some(engine.compaction.status_snapshot().await);
        }
        let local = self.local_plane_compactor.read().unwrap().clone()?;
        Some(local.status_snapshot().await)
    }

    /// Force an immediate sync. No-op if no engine is configured.
    ///
    /// While a replay blocker is armed (a corrupt cloud object or a failed
    /// pre-upload decrypt proof), forced syncs are rate-limited to
    /// [`MIN_FORCED_SYNC_UNDER_BLOCKER`]. Without this, a UI/API "sync now" (or
    /// a rapid succession of them) bypasses the inter-cycle failure backoff and
    /// hot-retriggers replay of the same poison object. Normal (unblocked)
    /// forced syncs are never throttled.
    pub async fn force_sync(&self) -> Result<(), SyncError> {
        self.force_sync_inner(false).await
    }

    async fn force_sync_inner(
        &self,
        bypass_replay_blocker_rate_limit: bool,
    ) -> Result<(), SyncError> {
        if let Some(engine) = self.engine() {
            let blocker_armed = engine.status().await.replay_blocker.is_some();
            if blocker_armed {
                let now = std::time::Instant::now();
                let suppress = {
                    // Scope the std Mutex guard so it is dropped before any await.
                    let mut last = self.last_forced_under_blocker.lock().unwrap();
                    replay_blocker_forced_sync_suppressed(
                        &mut last,
                        now,
                        bypass_replay_blocker_rate_limit,
                    )
                };
                if suppress {
                    debug!(
                        "force_sync suppressed: replay blocker armed and forced-sync rate limit not yet elapsed"
                    );
                    return Ok(());
                }
            }
            engine.sync().await?;
        }
        Ok(())
    }

    /// Stop the background sync task and run a final sync.
    ///
    /// Join aborted tasks before `force_sync`. Plane compaction may have
    /// stamped a temporary Cloud Sync Off; Drop of that guard restores it, but
    /// tokio does not run cancelled-task Drop until the handle is polled.
    /// Abort-without-join left Off latched across this final upload.
    pub async fn stop(&self) -> Result<(), SyncError> {
        self.abort_and_join_background_tasks().await;
        self.force_sync_inner(true).await
    }

    async fn abort_and_join_background_tasks(&self) {
        let handles = {
            let mut out = Vec::new();
            if let Some(handle) = self.task.lock().unwrap().take() {
                out.push(handle);
            }
            if let Some(handle) = self.capture_reexport_task.lock().unwrap().take() {
                out.push(handle);
            }
            if let Some(handle) = self.gc_atoms_task.lock().unwrap().take() {
                out.push(handle);
            }
            if let Some(handle) = self.plane_compaction_task.lock().unwrap().take() {
                out.push(handle);
            }
            out
        };
        for handle in handles {
            handle.abort();
            let _ = handle.await;
        }
    }

    /// Get the sync engine state, if configured.
    pub async fn state(&self) -> Option<SyncState> {
        Some(self.engine()?.state().await)
    }

    /// Get a full sync status snapshot, if configured.
    pub async fn status(&self) -> Option<SyncStatus> {
        Some(self.engine()?.status().await)
    }

    /// Get the number of pending (unsynced) log entries, if configured.
    pub async fn pending_count(&self) -> Option<usize> {
        Some(self.engine()?.pending_count().await)
    }

    /// Abort the background sync, gc-atoms, and plane-compaction tasks without
    /// a final sync. Called from Drop to avoid tokio panics.
    pub(crate) fn abort_task(&self) {
        if let Some(handle) = self.task.lock().unwrap().take() {
            debug!("SyncCoordinator: aborting background sync task on drop");
            handle.abort();
        }
        if let Some(handle) = self.capture_reexport_task.lock().unwrap().take() {
            debug!("SyncCoordinator: aborting capture re-export drain on drop");
            handle.abort();
        }
        if let Some(handle) = self.gc_atoms_task.lock().unwrap().take() {
            debug!("SyncCoordinator: aborting automatic gc-atoms cadence on drop");
            handle.abort();
        }
        if let Some(handle) = self.plane_compaction_task.lock().unwrap().take() {
            debug!("SyncCoordinator: aborting automatic plane-compaction cadence on drop");
            handle.abort();
        }
    }
}

impl Default for SyncCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

/// Compute the next inter-cycle sleep duration after a sync failure.
///
/// EVERY failure class now backs off exponentially — only a *successful* cycle
/// (handled by the caller) resets the delay to `base`. This matters because the
/// previous "reset to base unless Offline" rule let whole classes of failure
/// retry at the base interval forever:
/// - An S3/R2 `503` (or any `Storage`/`Crypto`/`SequenceGap`) only reached
///   `SyncState::Offline` for `Network` errors, so a sustained R2 outage
///   retried a full cycle every base interval indefinitely.
///
/// The per-class cap differs by how operator-actionable the failure is:
/// - `Auth` / `Banned`: `auth_max` (revoked/locked credential — probe rarely).
/// - `QuotaExceeded`: normal offline cap; a paid upgrade or quota increase
///   should be observed on the next ordinary retry window, not the auth cap.
/// - `CorruptEntry` / `KeyProofFailed`: [`MAX_REPLAY_BACKOFF`] (a poison object
///   or a wrong local key — cannot make progress until cloud state or key
///   material changes; do not hammer the same object).
/// - Everything else (network, storage, crypto, S3 5xx, sequence gap):
///   `offline_max`.
fn next_backoff_on_failure(
    current: tokio::time::Duration,
    _base: tokio::time::Duration,
    offline_max: tokio::time::Duration,
    auth_max: tokio::time::Duration,
    error: &SyncError,
) -> tokio::time::Duration {
    let cap = match error {
        SyncError::Auth(_) | SyncError::Banned(_) => auth_max,
        SyncError::CorruptEntry { .. } | SyncError::KeyProofFailed { .. } => MAX_REPLAY_BACKOFF,
        _ => offline_max,
    };
    current.saturating_mul(2).min(cap)
}

/// Whether local-write wakeups should be suppressed for one cycle after this
/// failure. Now `true` for ALL failures: since every failure arms an
/// exponential backoff (see [`next_backoff_on_failure`]), a steady stream of
/// local writes must not collapse that backoff back to the write cadence. Left
/// unsuppressed, a revoked API key (Auth) or an R2 outage on a busy node would
/// retry at write rate and never reach the auth/offline cap — the historical
/// 401-loop cadence. The next successful cycle re-enables prompt wake-driven
/// flushing.
fn failure_should_suppress_wakeups(_error: &SyncError) -> bool {
    true
}

fn replay_blocker_forced_sync_suppressed(
    last_forced_under_blocker: &mut Option<std::time::Instant>,
    now: std::time::Instant,
    bypass_rate_limit: bool,
) -> bool {
    if bypass_rate_limit {
        return false;
    }

    match *last_forced_under_blocker {
        Some(prev) if now.duration_since(prev) < MIN_FORCED_SYNC_UNDER_BLOCKER => true,
        _ => {
            *last_forced_under_blocker = Some(now);
            false
        }
    }
}
