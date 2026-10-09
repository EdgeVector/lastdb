//! Capture re-export and self-compaction policy: tunables, overhang triggers and
//! plane lists. The sync engine in the parent module reads them.

use super::*;

pub(crate) const CAPTURE_REEXPORT_NAMESPACE: &str = "sync_capture_reexport";
pub(super) const CAPTURE_REEXPORT_PAGE_SIZE: usize = 256;
pub(super) const CAPTURE_REEXPORT_BATCH_SIZE: usize = 128;
pub(super) const CAPTURE_REEXPORT_TICK_BUDGET: Duration = Duration::from_secs(20);
pub(super) const CAPTURE_PRESENCE_MASK: u64 = 3;
pub(super) const CAPTURE_PRESENCE_NONEMPTY: u64 = 1;
pub(super) const CAPTURE_PRESENCE_EMPTY: u64 = 2;
pub(super) static CAPTURE_REEXPORT_NONCE: AtomicU64 = AtomicU64::new(0);

/// On-disk bytes above which the marker plane is self-compacted.
///
/// Async capture can leave a large live marker backlog. The byte trigger
/// reclaims dead records only; it cannot reduce live markers. On the primary,
/// 258,963 live markers remained after a 370 MB compaction on 2026-10-02.
/// The 16 MiB default bounds dead-record churn after that backlog drains.
///
/// Set `LASTDB_CAPTURE_REEXPORT_COMPACT_MAX_BYTES=0` to disable self-compaction
/// and hand reclaim back to
/// `lastdb db compact --collection sync_capture_reexport --execute`.
pub(crate) fn capture_reexport_compact_max_plane_bytes() -> u64 {
    env_flag::var_or(
        "LASTDB_CAPTURE_REEXPORT_COMPACT_MAX_BYTES",
        16 * 1024 * 1024,
    )
}

/// Minimum seconds between on-disk size probes of the marker plane.
///
/// The probe is a directory stat walk, cheap next to a sync cycle, but it runs
/// on that cycle and there is nothing to gain from walking the same files every
/// time. Override with `LASTDB_CAPTURE_REEXPORT_COMPACT_PROBE_INTERVAL_SECS`.
pub(crate) fn capture_reexport_probe_interval_s() -> u64 {
    env_flag::var_or(
        "LASTDB_CAPTURE_REEXPORT_COMPACT_PROBE_INTERVAL_SECS",
        300u64,
    )
}

pub(crate) const LOCATOR_NAMESPACE: &str = crate::atom::atom_locator_codec::LOCATOR_COLLECTION;
pub(crate) const TIPS_NAMESPACE: &str = "tips";
pub(crate) const ATOMS_NAMESPACE: &str = "atoms";
pub(crate) const ORDER_LOG_NAMESPACE: &str = "field_update_order_log";

/// Large captured planes with crash-safe physical compaction contracts.
///
/// These planes do not belong in the capture-free residual sweep. Their
/// physical rewrite runs under capture suppression and the backup-publish-target
/// packing lock. Atoms (D5) does not pause Cloud Sync: capture-suppress plus
/// the signed retired-chunk-sha receipt is the isolation. Order-log automatic
/// compact still pauses uploads. Owner `lastdb db compact --execute` isolation
/// for `atoms` is a separate CLI contract and stays on.
pub(crate) const LARGE_CAPTURED_SELF_COMPACT_PLANES: &[&str] =
    &[ATOMS_NAMESPACE, ORDER_LOG_NAMESPACE];
// `tips` stays a dedicated trigger: same overhang-ratio + floor as D2, plus
// the backup-starvation warn and a no-pause compact. Do not add it here —
// `automatic_large_plane_pauses_cloud` would pause it.

/// Automatic self-compaction pauses Cloud Sync for a large captured plane
/// only when that plane still requires isolation.
///
/// Atoms is capture-neutral via `with_capture_suppressed` plus the signed
/// retirement receipt, so it rewrites with sync on. The owner compact verb
/// still pauses `atoms` (`compact_requires_cloud_isolation`); this helper
/// is the automatic-path exception only.
pub(crate) fn automatic_large_plane_pauses_cloud(plane: &str) -> bool {
    plane != ATOMS_NAMESPACE && crate::storage::laststore::compact_requires_cloud_isolation(plane)
}

/// Default reclaim ratio: compact when the expected reclaim — filesystem
/// slack plus the dead-record share of the plane
/// ([`CollectionDiskUsage::reclaimable_estimate_bytes`]) — is ≥ 15% of
/// allocation. The knob names keep "overhang" because that was the whole
/// signal before the store counted dead record bytes; the number now
/// includes what a delete leaves behind.
pub(crate) const DEFAULT_OVERHANG_BPS: u64 = 1_500;
/// Aggressive-plane ratio (atoms): 10% of allocation.
pub(crate) const AGGRESSIVE_OVERHANG_BPS: u64 = 1_000;
/// Absolute floor for ordinary planes: skip a rewrite that reclaims less.
pub(crate) const DEFAULT_OVERHANG_FLOOR_BYTES: u64 = 256 * 1024 * 1024;
/// Absolute floor for large planes (atoms, tips).
pub(crate) const LARGE_OVERHANG_FLOOR_BYTES: u64 = 512 * 1024 * 1024;
/// Photograph-aligned compact-if-dirty cadence (D3). Not the 120s continuous cut.
pub(crate) const DEFAULT_PHOTOGRAPH_COMPACT_INTERVAL_SECS: u64 = 6 * 60 * 60;
/// Wall-clock budget for one D3 pass. Remaining dirty planes wait for the next cycle.
pub(crate) const DEFAULT_PHOTOGRAPH_COMPACT_BUDGET_SECS: u64 = 5 * 60;

/// Seconds between photograph-aligned compact-if-dirty passes.
///
/// Default six hours matches the photograph cadence. Zero fires every cycle
/// (tests). The 120s snapshot+log publisher never calls this pass.
pub(crate) fn photograph_aligned_compact_interval_s() -> u64 {
    env_flag::var_or(
        "LASTDB_PHOTOGRAPH_COMPACT_INTERVAL_SECS",
        DEFAULT_PHOTOGRAPH_COMPACT_INTERVAL_SECS,
    )
}

/// Wall-clock budget for one photograph-aligned compact-if-dirty pass.
///
/// Default five minutes. Zero skips compact so the photograph is not delayed.
pub(crate) fn photograph_aligned_compact_budget_secs() -> u64 {
    env_flag::var_or(
        "LASTDB_PHOTOGRAPH_COMPACT_BUDGET_SECS",
        DEFAULT_PHOTOGRAPH_COMPACT_BUDGET_SECS,
    )
}

/// True when the D3 pass must skip remaining planes and let the photograph proceed.
pub(crate) fn photograph_compact_budget_exhausted(
    started: Instant,
    budget: Duration,
    now: Instant,
) -> bool {
    now.saturating_duration_since(started) >= budget
}

#[derive(Clone, Copy)]
pub(crate) enum PhotographPlaneKind {
    Tips,
    LargeCaptured,
    Residual,
    Locator,
}

pub(crate) struct PhotographDirtyPlane {
    pub(crate) name: &'static str,
    pub(crate) usage: crate::storage::traits::CollectionDiskUsage,
    pub(crate) kind: PhotographPlaneKind,
}

impl PhotographDirtyPlane {
    pub(crate) fn overhang(&self) -> u64 {
        self.usage
            .allocated_bytes
            .saturating_sub(self.usage.apparent_bytes)
    }
}

/// Status-only atom-plane budget alarm.
///
/// This is no longer a compaction trigger (D2). The rewrite fires on
/// overhang-ratio + floor. The cap still appears in `lastdb status` so a
/// plane that has grown past the original 3 GiB guess is visible. Set zero
/// to silence the alarm.
pub(crate) fn atoms_compact_max_bytes() -> u64 {
    env_flag::var_or("LASTDB_ATOMS_COMPACT_MAX_BYTES", 3 * 1024 * 1024 * 1024)
}

/// Status-only order-log budget alarm. Not a compaction trigger (D2).
pub(crate) fn order_log_compact_max_bytes() -> u64 {
    env_flag::var_or(
        "LASTDB_FIELD_UPDATE_ORDER_LOG_COMPACT_MAX_BYTES",
        256 * 1024 * 1024,
    )
}

/// Atoms overhang ratio in basis points. Zero disables unattended atoms compact.
pub(crate) fn atoms_compact_min_overhang_bps() -> u64 {
    env_flag::var_or(
        "LASTDB_ATOMS_COMPACT_MIN_OVERHANG_BPS",
        AGGRESSIVE_OVERHANG_BPS,
    )
}

/// Atoms absolute overhang floor. Zero disables unattended atoms compact.
pub(crate) fn atoms_compact_min_overhang_bytes() -> u64 {
    env_flag::var_or(
        "LASTDB_ATOMS_COMPACT_MIN_OVERHANG_BYTES",
        LARGE_OVERHANG_FLOOR_BYTES,
    )
}

/// Order-log overhang ratio in basis points. Zero disables the trigger.
pub(crate) fn order_log_compact_min_overhang_bps() -> u64 {
    env_flag::var_or(
        "LASTDB_FIELD_UPDATE_ORDER_LOG_COMPACT_MIN_OVERHANG_BPS",
        DEFAULT_OVERHANG_BPS,
    )
}

/// Order-log absolute overhang floor. Zero disables the trigger.
pub(crate) fn order_log_compact_min_overhang_bytes() -> u64 {
    env_flag::var_or(
        "LASTDB_FIELD_UPDATE_ORDER_LOG_COMPACT_MIN_OVERHANG_BYTES",
        DEFAULT_OVERHANG_FLOOR_BYTES,
    )
}

/// Residual-plane overhang ratio. Zero disables the residual sweep trigger.
pub(crate) fn residual_plane_compact_min_overhang_bps() -> u64 {
    env_flag::var_or(
        "LASTDB_RESIDUAL_PLANE_COMPACT_MIN_OVERHANG_BPS",
        DEFAULT_OVERHANG_BPS,
    )
}

/// Residual-plane absolute overhang floor. Zero disables the residual sweep.
pub(crate) fn residual_plane_compact_min_overhang_bytes() -> u64 {
    env_flag::var_or(
        "LASTDB_RESIDUAL_PLANE_COMPACT_MIN_OVERHANG_BYTES",
        DEFAULT_OVERHANG_FLOOR_BYTES,
    )
}

/// Compact when the expected reclaim is both ≥ `min_overhang_bps` of
/// allocation and ≥ `min_overhang_bytes`. Either knob at zero is the operator
/// disable hatch.
///
/// "Expected reclaim" is [`CollectionDiskUsage::reclaimable_estimate_bytes`]:
/// filesystem block slack **plus** the dead-record share of the plane. The
/// slack alone was the whole trigger before residue accounting existed, and
/// a delete does not move it — a LastStore delete is an append — so a plane
/// could hold gigabytes of deleted rows and never trip. The dead-record
/// share is what a delete actually leaves behind.
///
/// Ratio uses u128 so a multi-gigabyte plane cannot overflow the multiply.
pub(crate) fn overhang_trigger_met(
    usage: &crate::storage::traits::CollectionDiskUsage,
    min_overhang_bps: u64,
    min_overhang_bytes: u64,
) -> bool {
    if min_overhang_bps == 0 || min_overhang_bytes == 0 {
        return false;
    }
    if usage.reclaimable_estimate_bytes() < min_overhang_bytes {
        return false;
    }
    overhang_ratio_met(usage, min_overhang_bps)
}

/// Ratio half of the trigger. Locators still fire on ratio alone.
pub(crate) fn overhang_ratio_met(
    usage: &crate::storage::traits::CollectionDiskUsage,
    min_overhang_bps: u64,
) -> bool {
    if min_overhang_bps == 0 || usage.allocated_bytes == 0 {
        return false;
    }
    u128::from(usage.reclaimable_estimate_bytes()) * 10_000
        >= u128::from(usage.allocated_bytes) * u128::from(min_overhang_bps)
}

/// Expected reclaim in basis points of allocation. Zero on an empty plane.
pub(crate) fn overhang_bps(usage: &crate::storage::traits::CollectionDiskUsage) -> u64 {
    usage.reclaimable_bps()
}

/// Transient footprint a plane rewrite is expected to add on top of the
/// process's current footprint while it runs.
///
/// Flat, not a fraction of the plane. The rewrite this gate protects buffers
/// one bounded frame at a time, so its peak is a function of that frame budget
/// plus one shard's index copies — and a hash-group shard's index is one
/// group's keys, not the plane's. `laststore` publishes that bound as
/// [`laststore::COMPACT_REWRITE_PEAK_BUDGET_BYTES`] and its
/// `compact_memory_bound` test enforces it at any plane size, so this reserves
/// the published number instead of modelling the rewrite a second time.
///
/// The 60% figure this replaced was measured against the *old* rewrite, which
/// built the whole shard's live set as one buffer, encoded a second full copy,
/// and held two more: the 2026-08-31 tips rewrite (5.26 GiB apparent) spiked
/// ~2.9 GiB and the 16 GiB guard SIGKILLed the primary mid-rewrite twice
/// (05:03:47Z, 06:06:42Z). That cost tracked live bytes; this one does not.
/// Keeping the fraction after the rewrite was bounded over-estimated a
/// multi-GiB plane by orders of magnitude and deferred rewrites that now fit,
/// which let overhang grow at the primary's steady baseline.
///
/// An empty plane has nothing to rewrite, so it reserves nothing.
pub(crate) fn compact_footprint_spike_estimate(apparent_bytes: u64) -> u64 {
    if apparent_bytes == 0 {
        return 0;
    }
    laststore::COMPACT_REWRITE_PEAK_BUDGET_BYTES
}

/// True when starting a plane rewrite now is expected to cross the external
/// kill guard: `footprint + spike estimate > limit`.
///
/// Pure decision half of the automatic-compaction headroom gate; the caller
/// owns reading the live footprint and the configured limit.
pub(crate) fn compact_lacks_headroom(
    footprint_bytes: u64,
    limit_bytes: u64,
    apparent_bytes: u64,
) -> bool {
    if limit_bytes == 0 {
        return false;
    }
    footprint_bytes.saturating_add(compact_footprint_spike_estimate(apparent_bytes)) > limit_bytes
}

/// Copy the residue half of a measurement into a plane's status entry.
pub(super) fn fill_residue_status(
    entry: &mut crate::sync::engine::AutomaticCompactionStatus,
    usage: &crate::storage::traits::CollectionDiskUsage,
) {
    entry.live_bytes = usage.live_bytes;
    entry.dead_bytes = usage.dead_bytes;
    entry.dead_bps = usage.dead_bytes.map(|_| usage.dead_bps());
    entry.residue_unknown_bytes = Some(usage.residue_unknown_bytes);
    entry.reclaimable_estimate_bytes = Some(usage.reclaimable_estimate_bytes());
    entry.reclaimable_bps = Some(usage.reclaimable_bps());
}

/// Merge live overhang into one plane's operator status without wiping last-compact.
pub(crate) fn fill_overhang_status(
    map: &mut std::collections::BTreeMap<String, crate::sync::engine::AutomaticCompactionStatus>,
    plane: &str,
    usage: Option<crate::storage::traits::CollectionDiskUsage>,
    min_bps: u64,
    min_bytes: u64,
    alarm_max: u64,
) {
    let entry = map.entry(plane.to_string()).or_default();
    entry.trigger_overhang_bps = min_bps;
    entry.trigger_overhang_bytes = min_bytes;
    entry.configured_max_bytes = alarm_max;
    let Some(usage) = usage else {
        return;
    };
    let overhang = usage.allocated_bytes.saturating_sub(usage.apparent_bytes);
    entry.allocated_bytes = Some(usage.allocated_bytes);
    entry.apparent_bytes = Some(usage.apparent_bytes);
    entry.overhang_bytes = Some(overhang);
    entry.overhang_bps = Some(overhang_bps(&usage));
    fill_residue_status(entry, &usage);
    entry.above_trigger = if min_bytes == 0 {
        overhang_ratio_met(&usage, min_bps)
    } else {
        overhang_trigger_met(&usage, min_bps, min_bytes)
    };
    entry.cap_alarm = alarm_max > 0 && usage.allocated_bytes > alarm_max;
}
