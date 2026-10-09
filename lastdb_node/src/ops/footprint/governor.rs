//! Governor snapshot, state and classification. Moved verbatim from `footprint.rs`.

use super::*;

/// Cheap operator view of the current measured-footprint governor state.
#[derive(Debug, Clone)]
pub struct FootprintGovernorSnapshot {
    pub footprint_sticky: bool,
    pub footprint_delta_per_evicted_byte: Option<f64>,
    pub footprint_net_bytes: u64,
    pub governor_state: &'static str,
    /// Unix seconds when `governor_state` last CHANGED value; 0 before the
    /// first tick stamps it.
    ///
    /// The state string alone cannot separate a one-tick blip from a latch
    /// that has held for hours, and `classify_governor_state` returns
    /// `purge-failed` at the HIGHEST precedence -- so the one label most
    /// likely to be read as news is also the one that masks every other
    /// state while it holds. Measured on the primary 2026-10-07:
    /// `governor_state=purge-failed` across every sample of a 13h episode,
    /// with the only duration evidence two `became_failed` WARN lines in a
    /// rotating log. Stamp the transition so the age is in the reading.
    pub governor_state_since_epoch_secs: u64,
    /// Latched host pressure: `high` or `clear`. Not the raw sample.
    pub host_pressure: &'static str,
    pub swap_used_bytes: u64,
    pub compressor_bytes: u64,
    /// Latest scored purge failed the 512 MiB slack predicate.
    pub purge_failed: bool,
    /// Drop in allocator `committed` measured ACROSS the last `purge()`
    /// call. Not the predicate, and not every release: `purge()` collects
    /// the calling thread's heap plus shared arenas, so a worker heap's
    /// release lands in a later `committed` reading and is never counted
    /// here. A 0 therefore means "this call reclaimed nothing", never "the
    /// allocator reclaimed nothing". Measured on the primary 2026-10-07:
    /// `committed` held 2732589056 B byte-identical across five forced
    /// purges, so the 0 was honest there -- but only a reading of
    /// `allocator_committed_bytes` over time can establish that.
    pub malloc_bytes_released_last_purge: u64,
    /// Lifetime warm bytes released by eviction steps. The footprint proof
    /// scores each increase against the `phys_footprint` drop.
    pub warm_bytes_freed: u64,
}

impl Default for FootprintGovernorSnapshot {
    fn default() -> Self {
        Self {
            footprint_sticky: false,
            footprint_delta_per_evicted_byte: None,
            footprint_net_bytes: 0,
            governor_state: "under",
            governor_state_since_epoch_secs: 0,
            host_pressure: "clear",
            swap_used_bytes: 0,
            compressor_bytes: 0,
            purge_failed: false,
            malloc_bytes_released_last_purge: 0,
            warm_bytes_freed: 0,
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct FootprintGovernorState {
    pub(super) trim_response: IneffectiveTrimTracker,
    pub(super) sticky_until_epoch_secs: u64,
    pub(super) shed_until_epoch_secs: u64,
    /// Cooldown for the allocator-slack purge (see
    /// [`allocator_slack_purge_due`]), tracked separately from
    /// `sticky_until_epoch_secs` because it fires on a disjoint condition
    /// (under the soft line, not over it) and must not reset or be reset by
    /// the sticky/shed bookkeeping below.
    pub(super) allocator_slack_purge_until_epoch_secs: u64,
    pub(super) pressure: PressureLatch,
    pub(super) swap_used_bytes: u64,
    pub(super) compressor_bytes: u64,
    pub(super) ram_bytes: u64,
    /// False until a purge has been scored. A clear host does not grow before that.
    pub(super) purge_ok: bool,
    pub(super) purge_failed: bool,
    /// Last purge ran while requests were in flight. Score it on a later quiet tick.
    pub(super) purge_awaiting_collect: bool,
    pub(super) malloc_bytes_released_last_purge: u64,
    pub(super) snapshot: FootprintGovernorSnapshot,
}

/// Result of one shed pass.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ShedReport {
    pub deferred_idle: bool,
    pub groups_evicted: u64,
    pub warm_bytes_before: u64,
    pub warm_bytes_after: u64,
    pub effective_warm_bytes: u64,
    pub malloc_bytes_released: u64,
    pub phys_footprint_bytes: Option<u64>,
}

pub(super) fn footprint_governor() -> &'static Mutex<FootprintGovernorState> {
    static GOVERNOR: OnceLock<Mutex<FootprintGovernorState>> = OnceLock::new();
    GOVERNOR.get_or_init(|| Mutex::new(FootprintGovernorState::default()))
}

pub(super) fn sticky_cooldown_secs() -> u64 {
    fold_db::memory_budget::footprint_sticky_cooldown_secs(
        std::env::var(fold_db::memory_budget::FOOTPRINT_STICKY_COOLDOWN_SECS_ENV)
            .ok()
            .as_deref(),
    )
}

/// Whether the allocator-slack purge cooldown has elapsed. Locks the same
/// governor mutex as every other read here; a poisoned lock fails open (the
/// purge is cheap and safe to run an extra time, unlike skipping it forever).
pub(super) fn allocator_slack_purge_cooldown_elapsed(now: u64) -> bool {
    footprint_governor().lock().map_or(true, |state| {
        state.allocator_slack_purge_until_epoch_secs <= now
    })
}

pub(super) fn mark_allocator_slack_purge(now: u64) {
    if let Ok(mut state) = footprint_governor().lock() {
        state.allocator_slack_purge_until_epoch_secs =
            now.saturating_add(fold_db::memory_budget::ALLOCATOR_SLACK_PURGE_COOLDOWN_SECS);
    }
}

/// Decide whether this tick should ask the allocator to purge free pages even
/// though the raw footprint has not crossed
/// [`fold_db::memory_budget::FOOTPRINT_EVICT_SOFT_BYTES`].
///
/// Pure and side-effect-free so the threshold/cooldown interplay is testable
/// without a live `Host`. `over_soft_line` short-circuits this to `false`
/// because that case already purges unconditionally, every tick, as part of
/// footprint defense — this function only owns the gap that path leaves:
/// allocator retention the raw footprint never gets large enough to react to.
pub(super) fn allocator_slack_purge_due(
    measured_footprint_bytes: u64,
    malloc: Option<fold_db::memory_budget::MallocZoneStats>,
    over_soft_line: bool,
    cooldown_elapsed: bool,
) -> bool {
    if over_soft_line || !cooldown_elapsed {
        return false;
    }
    let footprint_net_bytes =
        fold_db::memory_budget::footprint_net_bytes(measured_footprint_bytes, malloc);
    let footprint_malloc_slack_bytes = measured_footprint_bytes.saturating_sub(footprint_net_bytes);
    footprint_malloc_slack_bytes >= fold_db::memory_budget::ALLOCATOR_SLACK_PURGE_BYTES
}

/// The home-conflict fold runs only while the governor is `under` and a live
/// footprint sample is at or below the soft line. A missing sample is not
/// clear. The default snapshot is already `under`, so the footprint check is
/// required. This does not read `now_epoch_secs`.
#[must_use]
pub fn conflict_fold_pressure_clear() -> bool {
    let under = footprint_governor()
        .lock()
        .is_ok_and(|state| state.snapshot.governor_state == "under");
    if !under {
        return false;
    }
    match fold_db::memory_budget::current_phys_footprint_bytes() {
        Some(bytes) => bytes <= fold_db::memory_budget::FOOTPRINT_EVICT_SOFT_BYTES,
        None => false,
    }
}

/// Whether this tick's footprint-visible slack is at the request-end collect
/// line. Independent of the soft-line short-circuit in
/// [`allocator_slack_purge_due`]: a tick over the soft line can still be
/// under this line, and the reverse is also true.
pub(super) fn tick_over_slack_line(
    measured_footprint_bytes: u64,
    footprint_net_bytes: u64,
) -> bool {
    measured_footprint_bytes.saturating_sub(footprint_net_bytes)
        >= fold_db::memory_budget::ALLOCATOR_SLACK_PURGE_BYTES
}

/// Snapshot used by `/api/status` and self-metrics after the governor tick.
#[must_use]
pub fn governor_snapshot() -> FootprintGovernorSnapshot {
    footprint_governor().lock().map_or_else(
        |_| FootprintGovernorSnapshot::default(),
        |state| state.snapshot.clone(),
    )
}

pub(super) fn begin_shed_cooldown() {
    if let Ok(mut state) = footprint_governor().lock() {
        let now = unix_secs();
        state.shed_until_epoch_secs = now.saturating_add(sticky_cooldown_secs());
        state.snapshot.footprint_sticky = false;
        set_governor_state(&mut state.snapshot, "evicting", now);
    }
}

pub(super) fn uds_in_flight(host: &Host) -> usize {
    host.uds_workers
        .get()
        .map_or(0, lastdb_uds::UdsWorkerPool::in_flight)
}

/// Score `measured - footprint_net`. A failure does not touch the trim streak.
pub(super) fn record_purge_score(state: &mut FootprintGovernorState, measured: u64) {
    let net = fold_db::memory_budget::footprint_net_bytes(measured, crate::allocator::occupancy());
    let ok = fold_db::memory_budget::purge_slack_ok(measured, net);
    let became_failed = !ok && !state.purge_failed;
    state.purge_ok = ok;
    state.purge_failed = !ok;
    state.purge_awaiting_collect = false;
    state.snapshot.purge_failed = !ok;
    state.snapshot.footprint_net_bytes = net;
    state.snapshot.malloc_bytes_released_last_purge = state.malloc_bytes_released_last_purge;
    if became_failed {
        tracing::warn!(
            target: "lastdbd::footprint",
            phys_footprint = measured,
            footprint_net_bytes = net,
            slack_bytes = measured.saturating_sub(net),
            malloc_bytes_released_last_purge = state.malloc_bytes_released_last_purge,
            "footprint purge failed the slack predicate"
        );
    }
}

pub(super) fn note_host_pressure(state: &mut FootprintGovernorState) {
    state.snapshot.host_pressure = if state.pressure.is_high() {
        "high"
    } else {
        "clear"
    };
    state.snapshot.swap_used_bytes = state.swap_used_bytes;
    state.snapshot.compressor_bytes = state.compressor_bytes;
    state.snapshot.purge_failed = state.purge_failed;
    state.snapshot.malloc_bytes_released_last_purge = state.malloc_bytes_released_last_purge;
}

pub(super) struct GovernorClass {
    pub(super) pressure_high: bool,
    pub(super) purge_failed: bool,
    pub(super) footprint_bytes: u64,
    pub(super) hard_bytes: u64,
    pub(super) target_bytes: u64,
    pub(super) footprint_sticky: bool,
    /// A step stopped because no unpinned group was left. That is not success.
    pub(super) evict_stalled: bool,
    pub(super) shed_hold: bool,
}

/// The ONE place `governor_state` is assigned.
///
/// Stamps `governor_state_since_epoch_secs` on a transition and leaves it
/// alone while the value is unchanged, so the field measures how long the
/// current state has HELD rather than when it was last recomputed -- the
/// governor reclassifies on every tick, so a per-tick stamp would always
/// read "just now". Routing every write through one function is deliberate:
/// three call sites assign this field and a fourth that forgot the stamp
/// would read as a fresh episode forever.
pub(super) fn set_governor_state(
    snapshot: &mut FootprintGovernorSnapshot,
    next: &'static str,
    now_epoch_secs: u64,
) {
    if snapshot.governor_state == next && snapshot.governor_state_since_epoch_secs != 0 {
        return;
    }
    snapshot.governor_state = next;
    snapshot.governor_state_since_epoch_secs = now_epoch_secs;
}

pub(super) fn classify_governor_state(class: &GovernorClass) -> &'static str {
    if class.purge_failed {
        return "purge-failed";
    }
    if class.footprint_bytes >= class.hard_bytes {
        return if class.footprint_sticky {
            "over-hard-trim-ineffective"
        } else {
            "over-hard"
        };
    }
    if class.pressure_high
        && (class.footprint_sticky
            || class.footprint_bytes <= class.target_bytes
            || class.evict_stalled)
    {
        return "pressure-hold";
    }
    if class.pressure_high && class.footprint_bytes > class.target_bytes {
        return "pressure-shed";
    }
    if class.footprint_sticky
        && !class.pressure_high
        && class.footprint_bytes <= fold_db::memory_budget::FOOTPRINT_EVICT_SOFT_BYTES
    {
        return "sticky-cooldown";
    }
    if class.footprint_bytes > fold_db::memory_budget::FOOTPRINT_EVICT_SOFT_BYTES || class.shed_hold
    {
        return "evicting";
    }
    "under"
}
