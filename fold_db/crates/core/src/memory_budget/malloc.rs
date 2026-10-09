//! Allocator slack, trim tracking, and malloc zone stats.

use super::*;

/// Footprint-visible allocator retention (the gap [`footprint_net_bytes`]
/// subtracts) above which the governor asks the allocator to purge even while
/// the raw footprint sits under [`FOOTPRINT_EVICT_SOFT_BYTES`].
///
/// `defend_measured_footprint` already purges every tick once the raw
/// footprint crosses the soft line, as a side effect of defending it. That
/// leaves the common case unaddressed: a primary mostly sits well under the
/// 10 GiB line, so the allocator can hold pages that visibly inflate the
/// measured footprint for as long as it takes to next cross that line, which
/// on a healthy node can be never. Measured live 2026-09-25 on the primary
/// (`governor_state=under`, footprint 6.81 GiB): `footprint_net_bytes` read
/// 4.56 GiB, a 2.25 GiB gap the allocator held free and macOS still charged
/// to the process footprint, entirely outside the soft-line trigger. A purge
/// costs no cache, so this closes the gap on its own schedule instead of
/// waiting for the footprint to get bad enough to notice on its own.
pub const ALLOCATOR_SLACK_PURGE_BYTES: u64 = 1024 * 1024 * 1024;

/// Minimum interval between allocator-slack purges triggered independently of
/// the footprint soft line (see [`ALLOCATOR_SLACK_PURGE_BYTES`]).
/// `mi_collect(true)` is cheap but not free, and the governor tick this feeds
/// runs on every on-demand `/api/status` read as well as the sampler's own
/// interval, so this bounds the worst case to one forced collect per minute
/// no matter how often a status client polls.
pub const ALLOCATOR_SLACK_PURGE_COOLDOWN_SECS: u64 = 60;

/// Parse the sticky cooldown without reading process-global environment state.
#[must_use]
pub fn footprint_sticky_cooldown_secs(value: Option<&str>) -> u64 {
    value
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|seconds| *seconds > 0)
        .unwrap_or(DEFAULT_FOOTPRINT_STICKY_COOLDOWN_SECS)
}

/// Tracks whether repeated footprint-defense passes are moving the arming
/// metric, so the caller can release the effective-warm floor once trimming
/// stops helping instead of pinning it there forever.
///
/// Pure and stateful-but-not-atomic: the caller owns synchronization (a
/// `Mutex` across sampler ticks in the running node), which keeps this type
/// testable without a live footprint reader.
#[derive(Debug, Default, Clone, Copy)]
pub struct IneffectiveTrimTracker {
    streak: u32,
    last_response_ratio: Option<f64>,
}

impl IneffectiveTrimTracker {
    /// Record one eviction step that freed warm bytes. Returns `true` once
    /// [`FOOTPRINT_EVICT_INEFFECTIVE_STREAK_LIMIT`] consecutive passes failed
    /// to lower `phys_footprint` by at least 25% of the warm bytes freed.
    ///
    /// A step with `warm_bytes_freed == 0` is not a trim result (a purge-only
    /// step, or a pass that dropped no group). It does not move the streak
    /// and does not store a ratio.
    pub fn observe(
        &mut self,
        warm_bytes_freed: u64,
        footprint_before: u64,
        footprint_after: u64,
    ) -> bool {
        if warm_bytes_freed == 0 {
            return false;
        }
        let footprint_delta = footprint_before.saturating_sub(footprint_after);
        let response_ratio = footprint_delta as f64 / warm_bytes_freed as f64;
        self.last_response_ratio = Some(response_ratio);
        self.streak = if response_ratio >= FOOTPRINT_EVICT_MIN_RESPONSE_RATIO {
            0
        } else {
            self.streak.saturating_add(1)
        };
        self.streak >= FOOTPRINT_EVICT_INEFFECTIVE_STREAK_LIMIT
    }

    /// Clear the streak. Call this once the arming metric falls back under the
    /// soft line so the next episode starts fresh rather than inheriting a
    /// stale count.
    pub fn reset(&mut self) {
        self.streak = 0;
        self.last_response_ratio = None;
    }

    /// Ratio from the most recent eviction step.
    #[must_use]
    pub fn last_response_ratio(&self) -> Option<f64> {
        self.last_response_ratio
    }
}

/// Live malloc-zone statistics for the default zone.
#[cfg(target_os = "macos")]
#[must_use]
pub fn malloc_zone_stats() -> Option<MallocZoneStats> {
    #[repr(C)]
    struct MallocStatistics {
        _blocks_in_use: u32,
        size_in_use: usize,
        _max_size_in_use: usize,
        size_allocated: usize,
    }
    #[repr(C)]
    struct MallocZone {
        _opaque: [u8; 0],
    }
    unsafe extern "C" {
        fn malloc_default_zone() -> *mut MallocZone;
        fn malloc_zone_statistics(zone: *mut MallocZone, stats: *mut MallocStatistics);
    }
    // SAFETY: malloc_default_zone returns the process default zone for the
    // life of the process. malloc_zone_statistics writes a statistics struct
    // of the size we pass.
    unsafe {
        let zone = malloc_default_zone();
        if zone.is_null() {
            return None;
        }
        let mut stats = MallocStatistics {
            _blocks_in_use: 0,
            size_in_use: 0,
            _max_size_in_use: 0,
            size_allocated: 0,
        };
        malloc_zone_statistics(zone, &mut stats);
        let in_use = stats.size_in_use as u64;
        let allocated = stats.size_allocated as u64;
        Some(MallocZoneStats {
            bytes_in_use: in_use,
            bytes_held_free: allocated.saturating_sub(in_use),
        })
    }
}

#[cfg(not(target_os = "macos"))]
#[must_use]
pub fn malloc_zone_stats() -> Option<MallocZoneStats> {
    None
}

/// Ask the default malloc zone to return free magazines to the OS.
///
/// `goal` is a hint; `0` means "as much as you can". Returns bytes released,
/// or `0` when the platform has no such call.
#[cfg(target_os = "macos")]
#[must_use]
pub fn malloc_zone_pressure_relief(goal: usize) -> u64 {
    #[repr(C)]
    struct MallocZone {
        _opaque: [u8; 0],
    }
    unsafe extern "C" {
        fn malloc_default_zone() -> *mut MallocZone;
        fn malloc_zone_pressure_relief(zone: *mut MallocZone, goal: usize) -> usize;
    }
    // SAFETY: same default-zone lifetime as [`malloc_zone_stats`]. The
    // function is a hint to the allocator; a null zone is a no-op.
    unsafe {
        let zone = malloc_default_zone();
        if zone.is_null() {
            return 0;
        }
        malloc_zone_pressure_relief(zone, goal) as u64
    }
}

#[cfg(not(target_os = "macos"))]
#[must_use]
pub fn malloc_zone_pressure_relief(_goal: usize) -> u64 {
    0
}
