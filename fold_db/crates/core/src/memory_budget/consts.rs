//! Env names and numeric budget constants.

/// Env: RSS ceiling enforced by `lastdbd-memory-guard`.
pub const RSS_LIMIT_MB_ENV: &str = "LASTDBD_RSS_LIMIT_MB";

/// Env: which gauge `lastdbd-memory-guard` samples against
/// [`RSS_LIMIT_MB_ENV`] — `rss` or `footprint`, defaulting to `footprint`.
///
/// The limit and the gauge are one policy and must be read together. The
/// ceiling alone does not say what it is a ceiling *on*, and the two gauges
/// have measured 6x apart on the live primary
/// (see [`current_phys_footprint_bytes`]).
pub const GUARD_METRIC_ENV: &str = "LASTDBD_GUARD_METRIC";

/// Env: hash-group warm-set (body) budget for non-logical collections.
/// Does not size the logical resident set.
pub const WARM_BYTES_ENV: &str = "LASTDB_HASH_GROUP_WARM_BYTES";

/// Env: hash-group key-index cache budget.
pub const KEY_CACHE_BYTES_ENV: &str = "LASTDB_HASH_GROUP_KEY_CACHE_BYTES";

/// Env: charged-bytes → RSS multiplier.
pub const RSS_MULTIPLIER_ENV: &str = "LASTDB_RSS_BUDGET_MULTIPLIER";

/// Env: explicit deferred-persist byte cap (overrides the derived value).
pub const DEFERRED_BYTES_ENV: &str = "LASTDB_RESIDENT_MAX_DEFERRED_BYTES";

/// Env: percent of the deferred window one persist lane may occupy (1–100).
pub const LANE_FAIR_SHARE_PERCENT_ENV: &str = "LASTDB_DEFER_LANE_FAIR_SHARE_PERCENT";

/// Env: batches at or above this size persist inline (no deferred reservation).
pub const WRITE_THROUGH_BYTES_ENV: &str = "LASTDB_DEFER_WRITE_THROUGH_BYTES";

/// The two states behind a `DeferredPersistGauge::try_reserve` refusal.
///
/// They are not degrees of the same thing. `Disabled` is a configuration an
/// operator chose (or the memory guard's hard latch chose for them): inline
/// persist is the only defined behavior and there is nothing to act on.
/// `WindowFull` is live backpressure against a positive cap, and raising the
/// window is an action that would change it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeferRefusal {
    /// The window is zero — deferral is switched off.
    Disabled,
    /// A positive window that this batch could not fit under.
    WindowFull,
}

/// Default process memory ceiling in MiB.
///
/// Tom ratified (2026-08-08) that the primary kill guard measures
/// `phys_footprint` against **16 GiB**, metric and limit together
/// (`decision-2026-08-08-memory-guard-footprint-16gib-limit`). The old 6 GiB
/// default was an RSS-era number that disagreed with both the live LaunchAgent
/// (12 GiB) and the ratified phys_footprint policy.
pub const DEFAULT_RSS_LIMIT_MB: u64 = 16384;

/// Fraction of the configured limit that measured footprint must overrun
/// before the defer window is hard-latched closed.
///
/// A 1–2 MiB jitter against a tight ceiling (Sentry RUST-3M: 12290 vs 12288)
/// must not permanently disable mode=write. Only a real runaway — at least this
/// fraction, and not less than [`FOOTPRINT_HARD_OVERRUN_MIN_BYTES`] — latches.
pub const FOOTPRINT_HARD_OVERRUN_FRACTION: f64 = 0.05;

/// Floor on the hard-overrun margin so small limits still require a meaningful
/// excursion before the safety latch closes the defer window.
pub const FOOTPRINT_HARD_OVERRUN_MIN_BYTES: u64 = 256 * 1024 * 1024;

/// Hysteresis band under the configured limit. Once the over-limit alarm has
/// fired, measured footprint must fall this far below the limit before the
/// alarm re-arms.
///
/// Without the band the alarm re-arms on the first sample under the ceiling, so
/// a footprint parked at the guard re-alarms on every crossing. That is a poll
/// counter, not an incident count (Sentry 7671016505: 104 events in 14 days,
/// zero users affected).
pub const FOOTPRINT_OVER_LIMIT_CLEAR_FRACTION: f64 = 0.02;

/// Floor on the hysteresis band so a small configured limit still needs a real
/// recovery, not a rounding step, before the alarm re-arms.
pub const FOOTPRINT_OVER_LIMIT_CLEAR_MIN_BYTES: u64 = 16 * 1024 * 1024;

/// How long measured footprint must stay under the recovery line before a
/// hard-latched defer window reopens.
///
/// The latch used to be one-way for the life of the process. That was written
/// when nothing else answered a runaway, so "never trust the projection again"
/// was the only safe reading. The external guard now sheds instead of killing
/// (`shed_recovered` at 2026-09-14T02:59:48Z: 17.3 GiB → 15.8 GiB in 4 s,
/// 7.5 GiB a minute later) and the footprint governor evicts on the measured
/// number, so a runaway is an episode, not a verdict. The primary then sat at
/// 46% of the guard for six hours with every write on the inline path because
/// one sample an hour after boot had latched it (Sentry 7671016505).
///
/// Two guard cycles under the line is long enough to prove the shed held and
/// short enough that the 200 ms mutation-ack goal is back the same hour.
pub const FOOTPRINT_LATCH_RECOVERY_HOLD_SECS: u64 = 120;

/// Soft line for measured-footprint LRU eviction. Above this the node trims
/// the warm set until the footprint falls under [`FOOTPRINT_EVICT_TARGET_BYTES`].
pub const FOOTPRINT_EVICT_SOFT_BYTES: u64 = 10 * 1024 * 1024 * 1024;

/// Hard line for measured-footprint defence. A sticky-footprint cooldown may
/// release the warm floor below this line, but never at or above it.
pub const FOOTPRINT_EVICT_HARD_BYTES: u64 = 12 * 1024 * 1024 * 1024;

/// Target physical footprint after an eviction pass.
pub const FOOTPRINT_EVICT_TARGET_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// Implied-multiplier line an eviction pass must also clear (alongside
/// [`FOOTPRINT_EVICT_TARGET_BYTES`]) before stepwise eviction stops. Footprint
/// alone can sit under the target while the charge/measured ratio is still
/// climbing (uncounted per-handle overhead outpacing the charged budget); both
/// conditions must hold or the trim stops too early.
pub const FOOTPRINT_EVICT_STOP_MULTIPLIER: f64 = 1.3;

/// Bound on stepwise eviction passes per [`crate`] measured-footprint defense
/// call. Each pass drops one LRU warm group and re-measures; this caps
/// worst-case work per sampler tick so a pathological footprint cannot loop
/// the sampler thread indefinitely.
pub const FOOTPRINT_EVICT_MAX_STEPS: u32 = 12;

/// Capture `vmmap -summary` when measured footprint crosses this line.
pub const FOOTPRINT_VMMAP_CAPTURE_BYTES: u64 = 12 * 1024 * 1024 * 1024;

/// Floor on the effective warm-set budget while the footprint is over the
/// soft line. The configured budget is never raised to this floor.
pub const EFFECTIVE_WARM_FLOOR_BYTES: u64 = 1024 * 1024 * 1024;

/// One sampler tick's warm-budget recovery while pressure is clear and the
/// purge predicate passed. The configured env stays the maximum input.
/// Recovery stops at [`EFFECTIVE_WARM_CLEAR_CAP_BYTES`].
pub const EFFECTIVE_WARM_GROW_STEP_BYTES: u64 = 256 * 1024 * 1024;

/// Ceiling on grow-back once pressure is clear and the purge predicate passed.
/// `min(configured, this)` is the cap. The configured env is not itself the
/// value `fits` climbs back to.
pub const EFFECTIVE_WARM_CLEAR_CAP_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Footprint stop while host pressure is high, before the RAM percent.
/// The tick applies `min(this, 15% of RAM)`.
pub const PRESSURE_FOOTPRINT_TARGET_BYTES: u64 = 4 * 1024 * 1024 * 1024;

/// Hard line while host pressure is high, before the RAM percent.
/// The tick applies `min(this, 20% of RAM)`. Also the grow permission: the
/// 12 GiB backstop does not authorize grow-back.
pub const PRESSURE_FOOTPRINT_HARD_BYTES: u64 = 6 * 1024 * 1024 * 1024;

/// Share of RAM that caps [`PRESSURE_FOOTPRINT_TARGET_BYTES`].
pub const PRESSURE_TARGET_RAM_PERCENT: u64 = 15;

/// Share of RAM that caps [`PRESSURE_FOOTPRINT_HARD_BYTES`].
pub const PRESSURE_HARD_RAM_PERCENT: u64 = 20;

/// Swap used above this is host pressure. The sample is `vm.swapusage`.
pub const PRESSURE_SWAP_HIGH_BYTES: u64 = 1024 * 1024 * 1024;

/// Compressor occupancy above this is host pressure.
pub const PRESSURE_COMPRESSOR_HIGH_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Free plus inactive pages under this percent of RAM is host pressure.
pub const PRESSURE_FREE_INACTIVE_MIN_PERCENT: u64 = 10;

/// Purge success: `phys_footprint - footprint_net` at or under this.
/// Not the trim-response ratio, and not `malloc_bytes_held_free`.
pub const PURGE_SLACK_LIMIT_BYTES: u64 = 512 * 1024 * 1024;

/// Environment override for the sticky-footprint eviction cooldown.
pub const FOOTPRINT_STICKY_COOLDOWN_SECS_ENV: &str = "LASTDB_FOOTPRINT_STICKY_COOLDOWN_SECS";

/// Default sticky-footprint cooldown. During this interval the governor stops
/// ineffective eviction below the hard line and lets the warm budget recover.
pub const DEFAULT_FOOTPRINT_STICKY_COOLDOWN_SECS: u64 = 10 * 60;

/// Consecutive footprint-defense passes that must fail to move the arming
/// metric by [`FOOTPRINT_EVICT_MIN_RESPONSE_RATIO`] before the module concludes
/// trimming cannot help and releases the effective warm budget back toward its
/// configured value. Measured 2026-09-05: 18.2M cold shard loads
/// against 17.8M eviction events on the primary — eviction ran on almost
/// every load and never lowered the metric arming it.
pub const FOOTPRINT_EVICT_INEFFECTIVE_STREAK_LIMIT: u32 = 3;

/// Minimum raw-footprint drop per warm byte freed to count as real progress.
pub const FOOTPRINT_EVICT_MIN_RESPONSE_RATIO: f64 = 0.25;

/// Warm-set preset default, mirroring `LastStoreOptions::hash_group()`.
/// Pinned to the real preset by `presets_match_laststore_hash_group`.
pub const PRESET_WARM_BYTES: u64 = 256 * 1024 * 1024;

/// Key-cache preset default, mirroring `LastStoreOptions::hash_group()`.
/// Pinned to the real preset by `presets_match_laststore_hash_group`.
pub const PRESET_KEY_CACHE_BYTES: u64 = 64 * 1024 * 1024;

/// Measured charge → RSS ratio on the live primary (4.00 GiB charged warm set,
/// 6.40 GiB settled RSS, 2026-07-29).
pub const DEFAULT_RSS_MULTIPLIER: f64 = 1.6;

/// A multiplier below 1.0 would claim the process uses less than it budgeted.
pub const MIN_RSS_MULTIPLIER: f64 = 1.0;

/// Above this a multiplier is a typo, not a measurement.
pub const MAX_RSS_MULTIPLIER: f64 = 8.0;

/// Share of remaining headroom the deferred-persist window may claim. The
/// other half is slack: allocator fragmentation, request working sets, and the
/// per-task state the byte cap does not measure all land there.
pub const DEFER_HEADROOM_FRACTION: f64 = 0.5;

/// A physical-footprint sample may move a little without invalidating the
/// boot projection. Only a sustained shape outside this band is called a
/// projection divergence. Crossing the configured guard raises an edge-triggered
/// alarm; the defer window is hard-latched closed only on a large overrun or a
/// boot budget that already cannot fit (see [`DeferredPersistGauge::observe_footprint`]).
pub const FOOTPRINT_PROJECTION_TOLERANCE_FRACTION: f64 = 0.10;

/// Below this a defer window buys no batching, so headroom this small is
/// reported as "cannot fit" rather than granted.
pub const MIN_DEFERRED_BYTES: u64 = 8 * 1024 * 1024;

/// Floor on the derived window when remaining headroom can fund it.
///
/// Live RSS on the primary often sits far under the 16 GiB guard. Treating 64
/// MiB as a ceiling then left unused headroom while a single pack reservation
/// starved every other writer (`persist_queue_full`). 64 MiB is now the
/// **minimum** derived window, not the maximum.
pub const FLOOR_DEFERRED_BYTES: u64 = 64 * 1024 * 1024;

/// Crash-window ceiling on the derived window.
///
/// Headroom may raise the cap above [`FLOOR_DEFERRED_BYTES`], but not without
/// bound: a 512 MiB ceiling was this module's first bug and re-admitted the
/// 2026-07-29 256 MiB balloon (`the_incident_burst_stops_at_the_byte_window_not_the_count_window`).
/// Fair share plus write-through stop one pack from occupying the whole
/// window; this ceiling still bounds acked-but-unpersisted state.
pub const MAX_DEFERRED_BYTES: u64 = 128 * 1024 * 1024;

/// Default share of [`ProcessMemoryBudget::deferred_cap_bytes`] one persist
/// lane may occupy. A lastgit pack lane at 100% of 64 MiB was the 2026-09-02
/// starvation shape.
pub const DEFAULT_LANE_FAIR_SHARE_PERCENT: u8 = 50;

/// Batches at or above this size skip the deferred window and persist inline.
///
/// A multi-MiB pack reservation is the wrong tenant for a window sized to
/// absorb one 500 ms cadence of ordinary records. Write-through keeps the
/// pack durable without occupying bytes that small writers need.
pub const DEFAULT_WRITE_THROUGH_BYTES: u64 = 4 * 1024 * 1024;

/// Fixed charge per deferred task for state the byte cap cannot measure
/// cheaply (the `Schema` clone, mutation events, held write guards).
///
/// A floor, not a measurement — the atom bodies are the term that actually
/// scales with load, and they are measured exactly. The task **count** cap
/// (`LASTDB_RESIDENT_MAX_DEFERRED`) is what bounds this unmeasured state.
pub const PER_DEFERRED_TASK_BYTES: u64 = 64 * 1024;
