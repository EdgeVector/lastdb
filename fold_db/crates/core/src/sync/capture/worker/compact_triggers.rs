//! Plane lists, per-plane compact tunables and trigger types for capture
//! re-export self-compaction.

use super::*;

/// Minimum seconds between stat-only probes of each large captured plane.
pub(crate) fn large_captured_plane_probe_interval_s() -> u64 {
    env_flag::var_or("LASTDB_LARGE_CAPTURED_COMPACT_PROBE_INTERVAL_SECS", 3_600)
}

/// Capture-free `COMPACT_ALLOWLIST` planes that have no self-compactor of their
/// own, swept together by [`SyncEngine::maybe_compact_residual_capture_free_planes`].
///
/// Three planes reached 2026-08-17 with a byte-return *verb* and no byte-return
/// *policy*: `lastdb db compact --collection <p> --execute` returns their bytes,
/// but nothing in the daemon ever calls it, so the reclaim only happens if an
/// operator happens to think of that plane. Measured on Tom's primary that day:
/// `schema_index` 265 MiB, `change_feed` 143 MiB, `idempotency` 39 MiB, all of
/// it accumulating because a `LastStore` overwrite or delete is an append and
/// the superseded record stays in the segment.
///
/// Every entry must clear the same bar the three armed planes cleared —
/// `sync::policy::compaction_is_capture_free`, checked again at the rewrite site
/// — so a sweep can never rewrite a captured plane into the mutation log. The
/// membership itself is asserted from the other direction by
/// `sync::policy::every_capture_free_allowlisted_plane_is_self_compacted`: a new
/// capture-free allowlist entry fails the build until it is armed here or given
/// its own compactor. That bar exists because three reclaims in this family
/// shipped un-armed before it.
pub(crate) const RESIDUAL_SELF_COMPACT_PLANES: &[&str] = &[
    "schema_index",
    "idempotency",
    "change_feed",
    // Keep-small meter snapshot (own plane since 2026-09-21). A single-key
    // gauge whose every debounced put supersedes the last, so the plane is
    // almost entirely dead bytes between sweeps and compacts back to one
    // record. Armed here so no operator has to remember it.
    "keep_small",
    "indexes",
    "atom_ref_edges",
    "atom_ref_edges_v2",
    "molecule_ref_edges",
    "blob_ref_edges",
];

/// Status-only tips-plane budget alarm. Not a compaction trigger (D2).
///
/// Compaction uses [`tips_compact_min_overhang_bps`] plus
/// [`tips_compact_min_overhang_bytes`]. This cap only lights `cap_alarm` on
/// `lastdb status` (`automatic_compactions.tips.max`). Set zero to silence.
pub(crate) fn tips_compact_max_bytes() -> u64 {
    env_flag::var_or("LASTDB_TIPS_COMPACT_MAX_BYTES", 3 * 1024 * 1024 * 1024)
}

/// Minimum filesystem overhang before `tips` can self-compact.
///
/// The ratio below is the primary trigger; this 512 MiB floor prevents block
/// rounding on a small store from causing a large live-set rewrite. Set to `0`
/// to disable unattended tips compaction.
pub(crate) fn tips_compact_min_overhang_bytes() -> u64 {
    env_flag::var_or(
        "LASTDB_TIPS_COMPACT_MIN_OVERHANG_BYTES",
        LARGE_OVERHANG_FLOOR_BYTES,
    )
}

/// Minimum allocation overhang in basis points of allocated bytes.
///
/// Fifteen percent matches the operator-visible "held past record length"
/// metric and keeps the trigger proportional to the live tips plane rather
/// than baking today's multi-gigabyte plane size into a flat cap.
pub(crate) fn tips_compact_min_overhang_bps() -> u64 {
    env_flag::var_or("LASTDB_TIPS_COMPACT_MIN_OVERHANG_BPS", 1_500)
}

pub(crate) fn tips_compact_probe_interval_s() -> u64 {
    env_flag::var_or("LASTDB_TIPS_COMPACT_PROBE_INTERVAL_SECS", 3_600)
}

/// Status-only residual-plane budget alarm. Not a compaction trigger (D2).
///
/// Compaction uses [`residual_plane_compact_min_overhang_bps`] plus
/// [`residual_plane_compact_min_overhang_bytes`]. This cap only lights
/// `cap_alarm` on `lastdb status`.
pub(crate) fn residual_plane_compact_max_bytes() -> u64 {
    env_flag::var_or("LASTDB_RESIDUAL_PLANE_COMPACT_MAX_BYTES", 64 * 1024 * 1024)
}

/// keep_small compaction floor: rewrite once 32 MiB is reclaimable (about
/// five superseded snapshots), far below the 1 GiB soft cold-load cap.
pub(crate) const KEEP_SMALL_COMPACT_FLOOR_BYTES: u64 = 32 * 1024 * 1024;
/// keep_small probe interval: two minutes, about 35 MB of snapshot puts at
/// the primary's measured rate.
pub(crate) const KEEP_SMALL_COMPACT_PROBE_INTERVAL_SECS: u64 = 120;

/// Minimum seconds between on-disk size probes of each residual plane.
///
/// An hour, matching the locator plane: these planes grow at the rate rows are
/// superseded, not at the rate writes are captured. Override with
/// `LASTDB_RESIDUAL_PLANE_COMPACT_PROBE_INTERVAL_SECS`.
pub(crate) fn residual_plane_probe_interval_s() -> u64 {
    env_flag::var_or("LASTDB_RESIDUAL_PLANE_COMPACT_PROBE_INTERVAL_SECS", 3600u64)
}

/// The per-plane overhang trigger a residual or large-captured plane needs.
///
/// One of these per entry in [`RESIDUAL_SELF_COMPACT_PLANES`] (and the two
/// large captured planes). Adding a plane costs a string rather than more
/// engine fields. Compaction fires on ratio + floor; `alarm_max_bytes` is
/// status-only.
#[derive(Debug)]
pub(crate) struct ResidualPlaneTrigger {
    pub last_probe_unix_s: AtomicU64,
    pub min_overhang_bps: AtomicU64,
    pub min_overhang_bytes: AtomicU64,
    /// Status-only absolute budget. Does not gate compaction.
    pub alarm_max_bytes: AtomicU64,
    pub probe_interval_s: AtomicU64,
}

impl ResidualPlaneTrigger {
    /// Read knobs once at construction so a running daemon cannot have them
    /// changed under it, and tests need no process-global state.
    pub(crate) fn from_env() -> Self {
        Self::new(
            residual_plane_compact_min_overhang_bps(),
            residual_plane_compact_min_overhang_bytes(),
            residual_plane_compact_max_bytes(),
            residual_plane_probe_interval_s(),
        )
    }

    /// The keep_small plane's own trigger. A whole-map snapshot put used to
    /// append a 6-36 MB copy of one key, about 1 GB/h on the primary, so the
    /// shared residual defaults (probe hourly, 256 MiB floor) let the group
    /// pass the 1 GiB soft cold-load cap between probes: on 2026-09-24 it
    /// froze at 1.08 GB and new builds could not boot the home. Persists now
    /// rewrite only dirty schema shards; the fast probe still reclaims
    /// superseded copies of those keys. Probing every
    /// [`KEEP_SMALL_COMPACT_PROBE_INTERVAL_SECS`] with a
    /// [`KEEP_SMALL_COMPACT_FLOOR_BYTES`] floor compacts it while resident and
    /// keeps it near one live copy per key.
    ///
    /// The shared disable hatch still covers it: an explicit
    /// `LASTDB_RESIDUAL_PLANE_COMPACT_MIN_OVERHANG_BYTES=0` (or bps `0`) stops
    /// keep_small rewrites too.
    pub(crate) fn keep_small_from_env() -> Self {
        let shared_floor_disabled =
            env_flag::var_parsed::<u64>("LASTDB_RESIDUAL_PLANE_COMPACT_MIN_OVERHANG_BYTES")
                == Some(0);
        let floor = if shared_floor_disabled {
            0
        } else {
            env_flag::var_or(
                "LASTDB_KEEP_SMALL_COMPACT_MIN_OVERHANG_BYTES",
                KEEP_SMALL_COMPACT_FLOOR_BYTES,
            )
        };
        Self::new(
            residual_plane_compact_min_overhang_bps(),
            floor,
            residual_plane_compact_max_bytes(),
            env_flag::var_or(
                "LASTDB_KEEP_SMALL_COMPACT_PROBE_INTERVAL_SECS",
                KEEP_SMALL_COMPACT_PROBE_INTERVAL_SECS,
            ),
        )
    }

    pub(crate) fn new(
        min_overhang_bps: u64,
        min_overhang_bytes: u64,
        alarm_max_bytes: u64,
        probe_interval_s: u64,
    ) -> Self {
        Self {
            last_probe_unix_s: AtomicU64::new(0),
            min_overhang_bps: AtomicU64::new(min_overhang_bps),
            min_overhang_bytes: AtomicU64::new(min_overhang_bytes),
            alarm_max_bytes: AtomicU64::new(alarm_max_bytes),
            probe_interval_s: AtomicU64::new(probe_interval_s),
        }
    }
}

/// Minimum filesystem allocation overhang for `atom_locators`, in basis points
/// of allocated bytes.
///
/// Unlike marker planes, `atom_locators` has a material live set: measured on
/// Tom's primary 2026-08-18 at 584,246 rows / 151 MiB after compaction. A flat
/// cap must either sit dangerously close to that changing live set or take
/// longer than a normal daemon session to fire. The filesystem already exposes
/// the residue directly as allocated bytes held past apparent record length, so
/// use the same proportional trigger as `tips`. At the default 15%, the measured
/// 11.8 MiB/hour churn reaches the trigger in roughly two hours.
///
/// Set `LASTDB_ATOM_LOCATORS_COMPACT_MIN_OVERHANG_BPS=0` to disable unattended
/// compaction and hand reclaim back to
/// `lastdb db compact --collection atom_locators --execute`.
pub(crate) fn locator_compact_min_overhang_bps() -> u64 {
    env_flag::var_or("LASTDB_ATOM_LOCATORS_COMPACT_MIN_OVERHANG_BPS", 1_500)
}

/// Minimum seconds between on-disk size probes of the locator plane.
///
/// An hour rather than the marker plane's five minutes: the locator plane
/// changes at the rate atoms are re-addressed, not at the rate writes are
/// captured, and its compaction rewrites millions of live rows rather than a
/// handful. Override with `LASTDB_ATOM_LOCATORS_COMPACT_PROBE_INTERVAL_SECS`.
pub(crate) fn locator_probe_interval_s() -> u64 {
    env_flag::var_or("LASTDB_ATOM_LOCATORS_COMPACT_PROBE_INTERVAL_SECS", 3600u64)
}

/// The four atomics that make a churn plane's size trigger, borrowed for one
/// probe. Everything else about the trigger is identical across planes, so this
/// is the whole per-plane surface.
#[derive(Clone, Copy)]
pub(crate) struct ChurnPlaneTrigger<'a> {
    pub namespace: &'a str,
    pub last_probe_unix_s: &'a AtomicU64,
    pub floor_bytes: &'a AtomicU64,
    pub max_bytes: &'a AtomicU64,
    pub probe_interval_s: &'a AtomicU64,
}

/// Raise a plane's size trigger floor so a large live set cannot turn the cap
/// into a rewrite treadmill.
///
/// The cap is a guess at the live set. When the guess is low the plane
/// legitimately exceeds it in *live* records, and without a floor the trigger
/// would rewrite the whole plane every probe interval and reclaim nothing.
/// Doubling the post-compaction size makes the next size-triggered rewrite wait
/// for the plane to double again, so growth stays bounded and the rewrite rate
/// falls as the live set grows. The floor never drops below the cap, so a
/// compaction that emptied the plane leaves the trigger at default sensitivity.
pub(crate) fn raise_plane_floor(
    floor_bytes: &AtomicU64,
    max_bytes: &AtomicU64,
    bytes_after: Option<u64>,
) {
    let Some(after) = bytes_after else {
        return;
    };
    let floor = after
        .saturating_mul(2)
        .max(max_bytes.load(Ordering::Relaxed));
    floor_bytes.store(floor, Ordering::Relaxed);
}
