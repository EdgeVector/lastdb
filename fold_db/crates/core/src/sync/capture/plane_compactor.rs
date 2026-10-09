//! Plane self-compaction, independent of cloud sync.
//!
//! Every automatic byte-return sweep in this file used to be a
//! [`crate::sync::engine::SyncEngine`] method reachable only from
//! `SyncEngine::do_sync`. A node booted without `cloud_sync.json` has no
//! engine, so it ran none of them: `gc-atoms` stepped, `tips` and `atoms` grew,
//! and the only byte-return path was an operator typing
//! `lastdb db compact --collection <plane> --execute`. Measured 2026-09-07 on a
//! `lastdb-dev` clone (brain
//! `papercut-lastdb-local-only-node-has-no-automatic-compaction-sweep`).
//!
//! The sweeps need the store, the photograph packing lock, the trigger knobs,
//! and the status map — not a cloud target, not credentials, and not a sync
//! cycle. [`PlaneCompactor`] owns exactly those four things, so
//! [`crate::fold_db_core::sync_coordinator::SyncCoordinator`] can step the same
//! cadence with `engine: None`, the same shape `start_automatic_gc_atoms`
//! already uses.
//!
//! There is one implementation. `SyncEngine` holds an `Arc<PlaneCompactor>` and
//! its `maybe_compact_*` methods delegate to it, sharing the *same*
//! `backup_publish_target` mutex and `cloud_sync_disabled_at` slot the engine
//! uses — so a cloud-synced node still honours the packing lock and still
//! stamps a cloud pause around the planes that require isolation. The
//! coordinator owns those Arcs from boot so a local cadence that started
//! before `cloud on` still serializes against a later cut. On a local-only
//! node nothing holds the lock, which is the correct answer: no cut can be
//! in flight.

use super::worker::{
    automatic_large_plane_pauses_cloud, compact_footprint_spike_estimate, compact_lacks_headroom,
    overhang_bps, overhang_ratio_met, overhang_trigger_met, photograph_compact_budget_exhausted,
    raise_plane_floor, ChurnPlaneTrigger, PhotographDirtyPlane, PhotographPlaneKind,
    ATOMS_NAMESPACE, CAPTURE_REEXPORT_NAMESPACE, LARGE_CAPTURED_SELF_COMPACT_PLANES,
    LOCATOR_NAMESPACE, ORDER_LOG_NAMESPACE, TIPS_NAMESPACE,
};
use super::{ResidualPlaneTrigger, RESIDUAL_SELF_COMPACT_PLANES};
use crate::storage::traits::NamespacedStore;
use crate::sync::engine::AutomaticCompactionStatus;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// The photograph packing lock. A held cut skips every rewrite in this module.
pub(crate) type BackupPublishTargetSlot =
    Arc<Mutex<Option<crate::sync::engine::backup_uploader::BackupPublishTarget>>>;

/// Every automatic plane sweep, and the state each one's trigger needs.
///
/// Field names and semantics are unchanged from when these lived on
/// `SyncEngine`; only their owner moved.
pub(crate) struct PlaneCompactor {
    pub(crate) store: Arc<dyn NamespacedStore>,
    /// Shared with the engine when one exists, so a rewrite and a backup cut
    /// still serialize on one mutex rather than two.
    pub(crate) backup_publish_target: BackupPublishTargetSlot,
    /// Shared with the engine when one exists. Planes that require isolation
    /// stamp a live cloud-disabled pause here across their rewrite.
    pub(crate) cloud_sync_disabled_at: Arc<Mutex<Option<u64>>>,
    /// Unix seconds of the last on-disk size probe of the marker plane. Rate
    /// limits the stat walk in
    /// [`SyncEngine::maybe_compact_capture_reexport_plane`]; `0` means never.
    pub(crate) capture_reexport_last_probe_unix_s: AtomicU64,
    /// Size the marker plane must exceed before the next self-compaction. Rises
    /// to twice the post-compaction size so a plane whose live set is
    /// legitimately large cannot become a rewrite treadmill.
    pub(crate) capture_reexport_compact_floor_bytes: AtomicU64,
    /// On-disk cap for the marker plane, read once from
    /// `LASTDB_CAPTURE_REEXPORT_COMPACT_MAX_BYTES` at construction. A field
    /// rather than a per-probe `env::var` so the trigger cannot be changed
    /// under a running daemon and tests need no process-global state.
    pub(crate) capture_reexport_compact_max_bytes: AtomicU64,
    /// Minimum seconds between size probes of the marker plane, read once from
    /// `LASTDB_CAPTURE_REEXPORT_COMPACT_PROBE_INTERVAL_SECS`.
    pub(crate) capture_reexport_probe_interval_s: AtomicU64,
    /// Unix seconds of the last on-disk size probe of the `atom_locators`
    /// plane, rate limiting the stat walk in
    /// [`SyncEngine::maybe_compact_locator_plane`]; `0` means never.
    pub(crate) locator_last_probe_unix_s: AtomicU64,
    /// Proportional filesystem allocation overhang that triggers locator
    /// compaction, read once from
    /// `LASTDB_ATOM_LOCATORS_COMPACT_MIN_OVERHANG_BPS` at construction.
    pub(crate) locator_compact_min_overhang_bps: AtomicU64,
    /// Minimum seconds between size probes of the locator plane, read once
    /// from `LASTDB_ATOM_LOCATORS_COMPACT_PROBE_INTERVAL_SECS`.
    pub(crate) locator_probe_interval_s: AtomicU64,
    /// Rate limit and proportional filesystem-overhang trigger for the large
    /// captured-but-physical-compact-neutral `tips` plane.
    pub(crate) tips_last_probe_unix_s: AtomicU64,
    pub(crate) tips_compact_min_overhang_bytes: AtomicU64,
    pub(crate) tips_compact_min_overhang_bps: AtomicU64,
    /// Status-only tips budget alarm (`LASTDB_TIPS_COMPACT_MAX_BYTES`).
    pub(crate) tips_compact_max_bytes: AtomicU64,
    pub(crate) tips_probe_interval_s: AtomicU64,
    /// Unix seconds when `tips` compaction first found a backup publish
    /// target held, or 0 when it is not currently being skipped for that
    /// reason.
    ///
    /// A held cut is normal for the length of one publish; it is a capacity
    /// problem when a cut cannot complete, because the largest plane in the
    /// store then loses its only automatic byte-return path and nothing says
    /// so. This field is what lets the skip escalate from `debug` to a
    /// rate-limited `warn` that names the duration.
    pub(crate) tips_backup_starved_since_unix_s: AtomicU64,
    /// Unix seconds of the last starvation warning, so the escalation above
    /// is emitted once per probe interval rather than once per sync cycle.
    pub(crate) tips_starved_last_warn_unix_s: AtomicU64,
    /// Unix seconds of the last photograph-aligned compact-if-dirty pass (D3).
    /// `0` means this process has not run the pass yet.
    pub(crate) photograph_compact_last_unix_s: AtomicU64,
    /// Cadence for D3 (default 6h). Zero means every cycle (tests).
    pub(crate) photograph_compact_interval_s: AtomicU64,
    /// Wall-clock budget for D3 (default 5 min). Zero skips compact.
    pub(crate) photograph_compact_budget_secs: AtomicU64,
    /// Size-trigger state for each plane in
    /// [`crate::sync::capture::RESIDUAL_SELF_COMPACT_PLANES`], keyed by
    /// collection name. A map rather than four more fields per plane: those
    /// three planes are the same shape as each other and as the two above, and
    /// the next capture-free plane admitted to `COMPACT_ALLOWLIST` should cost
    /// one string, not one more copy of this block.
    pub(crate) residual_plane_triggers:
        std::collections::BTreeMap<&'static str, crate::sync::capture::ResidualPlaneTrigger>,
    /// Overhang-ratio triggers for captured planes whose physical rewrite is
    /// capture-neutral but must serialize against a backup photograph.
    pub(crate) large_captured_plane_triggers:
        std::collections::BTreeMap<&'static str, crate::sync::capture::ResidualPlaneTrigger>,
    /// Last successful automatic compaction reports for operator status.
    pub(crate) automatic_compaction_status: Arc<
        Mutex<std::collections::BTreeMap<String, crate::sync::engine::AutomaticCompactionStatus>>,
    >,
    /// Per-collection disk-usage memo for [`Self::status_snapshot`]. Only the
    /// local cadence reads it; the engine passes its own so a cloud-synced
    /// node keeps one cache rather than two walking the same directories.
    pub(crate) status_disk_usage_cache: Arc<crate::sync::engine::StatusDiskUsageCache>,
}

/// Restores `cloud_sync_disabled_at` even when the compact task is aborted.
///
/// Large-plane compact stamps a temporary Cloud Sync Off so a photograph cannot
/// capture a torn collection. Restore used to live after the compact loop.
/// Cooperative abort at the next `.await` skipped that write and left a healthy
/// node Off (`cloud_plane_allows_upload` false until the next process start).
///
/// Drop restores `previous`. `restore` is the happy-path await. An existing Off
/// is left alone: `arm` does not overwrite a set slot.
#[must_use]
pub(crate) struct CloudOffPauseGuard {
    slot: Arc<Mutex<Option<u64>>>,
    previous: Option<u64>,
    armed: bool,
}

impl CloudOffPauseGuard {
    /// Stamp Off only when the slot is currently `None`.
    pub(crate) async fn arm(slot: Arc<Mutex<Option<u64>>>, now_s: u64) -> Self {
        let mut disabled_at = slot.lock().await;
        let previous = *disabled_at;
        let armed = previous.is_none();
        if armed {
            *disabled_at = Some(now_s.max(1));
        }
        drop(disabled_at);
        Self {
            slot,
            previous,
            armed,
        }
    }

    /// Awaited restore on the success path. Drop is a no-op afterwards.
    pub(crate) async fn restore(&mut self) {
        if !self.armed {
            return;
        }
        *self.slot.lock().await = self.previous;
        self.armed = false;
    }
}

impl Drop for CloudOffPauseGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.armed = false;
        let slot = Arc::clone(&self.slot);
        let previous = self.previous;
        if let Ok(mut guard) = slot.try_lock() {
            *guard = previous;
            return;
        }
        // Compact does not hold this slot across `.await`. If it is locked at
        // Drop, restore on the runtime so abort still clears a leftover pause.
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                // lint:spawn-bare-ok abort-path restore of a compact Cloud Sync
                // pause — Drop cannot await the tokio Mutex.
                std::mem::drop(handle.spawn(async move {
                    *slot.lock().await = previous;
                }));
            }
            Err(_) => {
                tracing::error!(
                    target: "fold_db::sync::mutation_log",
                    "cloud-off compact pause could not restore: slot locked and no runtime"
                );
            }
        }
    }
}

impl PlaneCompactor {
    /// Build a compactor over `store`, reading every trigger from the
    /// environment exactly once (the same read `SyncEngine::new` used to do).
    ///
    /// `backup_publish_target` and `cloud_sync_disabled_at` are passed in
    /// rather than created here so the engine can hand over its own slots. A
    /// local-only node passes fresh ones: nothing else can hold that lock, and
    /// there is no cloud staging to pause.
    pub(crate) fn new(
        store: Arc<dyn NamespacedStore>,
        backup_publish_target: BackupPublishTargetSlot,
        cloud_sync_disabled_at: Arc<Mutex<Option<u64>>>,
    ) -> Self {
        Self {
            store,
            backup_publish_target,
            cloud_sync_disabled_at,
            capture_reexport_last_probe_unix_s: AtomicU64::new(0),
            capture_reexport_compact_floor_bytes: AtomicU64::new(0),
            capture_reexport_compact_max_bytes: AtomicU64::new(
                super::capture_reexport_compact_max_plane_bytes(),
            ),
            capture_reexport_probe_interval_s: AtomicU64::new(
                super::capture_reexport_probe_interval_s(),
            ),
            locator_last_probe_unix_s: AtomicU64::new(0),
            locator_compact_min_overhang_bps: AtomicU64::new(
                super::locator_compact_min_overhang_bps(),
            ),
            locator_probe_interval_s: AtomicU64::new(super::locator_probe_interval_s()),
            tips_last_probe_unix_s: AtomicU64::new(0),
            tips_compact_min_overhang_bytes: AtomicU64::new(
                super::tips_compact_min_overhang_bytes(),
            ),
            tips_compact_min_overhang_bps: AtomicU64::new(super::tips_compact_min_overhang_bps()),
            tips_compact_max_bytes: AtomicU64::new(super::tips_compact_max_bytes()),
            tips_probe_interval_s: AtomicU64::new(super::tips_compact_probe_interval_s()),
            tips_backup_starved_since_unix_s: AtomicU64::new(0),
            tips_starved_last_warn_unix_s: AtomicU64::new(0),
            photograph_compact_last_unix_s: AtomicU64::new(0),
            photograph_compact_interval_s: AtomicU64::new(
                super::photograph_aligned_compact_interval_s(),
            ),
            photograph_compact_budget_secs: AtomicU64::new(
                super::photograph_aligned_compact_budget_secs(),
            ),
            residual_plane_triggers: RESIDUAL_SELF_COMPACT_PLANES
                .iter()
                .map(|plane| {
                    let trigger = if *plane == "keep_small" {
                        ResidualPlaneTrigger::keep_small_from_env()
                    } else {
                        ResidualPlaneTrigger::from_env()
                    };
                    (*plane, trigger)
                })
                .collect(),
            large_captured_plane_triggers: [
                (
                    ATOMS_NAMESPACE,
                    ResidualPlaneTrigger::new(
                        super::atoms_compact_min_overhang_bps(),
                        super::atoms_compact_min_overhang_bytes(),
                        super::atoms_compact_max_bytes(),
                        super::large_captured_plane_probe_interval_s(),
                    ),
                ),
                (
                    ORDER_LOG_NAMESPACE,
                    ResidualPlaneTrigger::new(
                        super::order_log_compact_min_overhang_bps(),
                        super::order_log_compact_min_overhang_bytes(),
                        super::order_log_compact_max_bytes(),
                        super::large_captured_plane_probe_interval_s(),
                    ),
                ),
            ]
            .into_iter()
            .collect(),
            automatic_compaction_status: Arc::new(Mutex::new(BTreeMap::new())),
            status_disk_usage_cache: Arc::new(crate::sync::engine::StatusDiskUsageCache::new()),
        }
    }

    /// Serialize a physical rewrite against cutting or retiring a backup
    /// publish target. Holding this guard across compaction prevents the
    /// check-then-act race where a cut begins after an `is_some()` probe.
    pub(crate) async fn lock_backup_publish_target(
        &self,
    ) -> tokio::sync::MutexGuard<
        '_,
        Option<crate::sync::engine::backup_uploader::BackupPublishTarget>,
    > {
        self.backup_publish_target.lock().await
    }

    /// Every automatic plane sweep, in the order `do_sync` runs them.
    ///
    /// This is the whole cadence body. `do_sync` calls it, and so does the
    /// local timer on a node with no engine — one implementation, one order,
    /// so the two node shapes cannot drift into reclaiming different planes.
    pub(crate) async fn run_all_sweeps(&self) {
        self.maybe_photograph_aligned_compact_if_dirty().await;
        self.maybe_compact_capture_reexport_plane().await;
        self.maybe_compact_locator_plane().await;
        self.maybe_compact_tips_plane().await;
        self.maybe_compact_large_captured_planes().await;
        self.maybe_compact_residual_capture_free_planes().await;
        self.maybe_compact_retired_groups().await;
    }

    /// Allow-list compact of groups a retire receipt names.
    ///
    /// Same cadence as the other automatic rewrites. The store skips the pass
    /// when host pressure is high or `phys_footprint` is over the 4 GiB
    /// pressure stop. A missing receipt is a no-op. This does not list `tips`
    /// and it does not target the 3.2 GiB non-CAS gate.
    ///
    /// The backup-publish guard is held across the rewrite. A check-then-act
    /// probe would let a cut start under the rewrite, which is the gen-502
    /// livelock the other sweeps already refuse.
    pub(crate) async fn maybe_compact_retired_groups(&self) {
        let backup_target = self.lock_backup_publish_target().await;
        if backup_target.is_some() {
            return;
        }
        if let Err(error) = self.store.compact_retired_groups().await {
            tracing::warn!(
                target: "fold_db::sync::mutation_log",
                %error,
                "retired-group compaction failed; retried on a later probe"
            );
        }
    }

    /// Headroom gate for every *automatic* plane rewrite: defer when the
    /// rewrite's expected transient footprint would push this process over the
    /// external kill guard's ceiling.
    ///
    /// Starting a rewrite that gets the process SIGKILLed reclaims nothing —
    /// the interrupted rewrite's work is lost, the overhang survives, and the
    /// next probe repeats the kill an hour later. That exact loop held the
    /// primary at 02:39Z/05:03Z/06:06Z on 2026-08-31 (tips: 11 M live keys,
    /// 5.2 GiB apparent, ~2.9 GiB spike against a 14.4 GiB baseline under the
    /// 16 GiB guard) and turned the canary soak RED each pass. Deferring keeps
    /// the trigger armed; the rewrite runs on a later probe when the baseline
    /// fits (fresh post-restart processes compacted the same planes fine).
    ///
    /// The rewrite that produced those kills is gone — it now buffers one
    /// bounded frame at a time, so what this reserves is a flat
    /// [`compact_footprint_spike_estimate`], not a share of the plane. The gate
    /// stays because a process genuinely sitting at the guard still cannot
    /// afford even that; it just no longer defers a plane the node can rewrite.
    ///
    /// No footprint reading (non-macOS) means no guard to collide with here —
    /// allow. Operator-invoked admin compaction is deliberately not gated.
    fn automatic_compact_headroom_denied(&self, plane: &str, apparent_bytes: u64) -> bool {
        let Some(footprint_bytes) = crate::memory_budget::current_phys_footprint_bytes() else {
            return false;
        };
        let limit_bytes = crate::memory_budget::process_memory_budget().rss_limit_bytes;
        if !compact_lacks_headroom(footprint_bytes, limit_bytes, apparent_bytes) {
            return false;
        }
        tracing::warn!(
            target: "fold_db::sync::mutation_log",
            collection = plane,
            footprint_mb = footprint_bytes / (1024 * 1024),
            limit_mb = limit_bytes / (1024 * 1024),
            apparent_mb = apparent_bytes / (1024 * 1024),
            estimated_spike_mb = compact_footprint_spike_estimate(apparent_bytes) / (1024 * 1024),
            "deferring automatic plane compaction: rewrite would cross the memory \
             guard and get this process killed mid-rewrite; retried on a later probe"
        );
        true
    }
}

mod large;
mod locator;
mod photograph;
mod reexport;
mod status;
mod tips;
