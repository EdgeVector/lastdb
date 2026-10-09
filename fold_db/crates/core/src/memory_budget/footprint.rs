//! Footprint overrun thresholds and effective warm sizing.

use super::*;

/// True when measured footprint exceeds the configured limit by enough to
/// count as a runaway rather than near-limit jitter.
#[must_use]
pub fn footprint_hard_overrun(measured_bytes: u64, limit_bytes: u64) -> bool {
    if measured_bytes <= limit_bytes {
        return false;
    }
    let margin = scale_bytes(limit_bytes, FOOTPRINT_HARD_OVERRUN_FRACTION)
        .max(FOOTPRINT_HARD_OVERRUN_MIN_BYTES);
    measured_bytes > limit_bytes.saturating_add(margin)
}

/// Footprint an alarmed process must fall back to before the over-limit alarm
/// re-arms. Sitting between this and the limit keeps the current episode open
/// instead of opening a new one.
#[must_use]
pub fn footprint_over_limit_clear_threshold(limit_bytes: u64) -> u64 {
    let band = scale_bytes(limit_bytes, FOOTPRINT_OVER_LIMIT_CLEAR_FRACTION)
        .max(FOOTPRINT_OVER_LIMIT_CLEAR_MIN_BYTES);
    limit_bytes.saturating_sub(band)
}

/// Footprint a hard-latched process must fall back under, and hold, before the
/// defer window reopens: the projection ceiling (the model the cap was derived
/// from, plus its tolerance), never above the alarm clear threshold.
///
/// Reopening at the clear threshold alone would hand the window back to a
/// process still parked 2% under the guard; the projection ceiling says the
/// footprint is back inside the numbers the cap was sized against.
#[must_use]
pub fn footprint_latch_recovery_line(budget: &ProcessMemoryBudget) -> u64 {
    let projection_ceiling = budget.projected_total_bytes.saturating_add(scale_bytes(
        budget.projected_total_bytes,
        FOOTPRINT_PROJECTION_TOLERANCE_FRACTION,
    ));
    projection_ceiling.min(footprint_over_limit_clear_threshold(budget.rss_limit_bytes))
}

/// Milliseconds since the first call, from a monotonic clock. The latch
/// recovery hold is measured on this so a wall-clock step cannot reopen or
/// pin a window.
pub(super) fn monotonic_millis() -> u64 {
    static EPOCH: OnceLock<std::time::Instant> = OnceLock::new();
    let elapsed = EPOCH.get_or_init(std::time::Instant::now).elapsed();
    u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
}

/// Current `phys_footprint` of this process, the metric the external kill
/// guard enforces (see [`DEFAULT_RSS_LIMIT_MB`]). RSS is not a substitute:
/// compressed anonymous pages leave RSS while staying in the footprint, and
/// the live primary has measured a 6× gap between the two.
///
/// `None` means the platform offers no footprint accounting (only macOS
/// reports one); callers must treat that as "no reading", not zero.
#[cfg(target_os = "macos")]
#[must_use]
pub fn current_phys_footprint_bytes() -> Option<u64> {
    let mut info = std::mem::MaybeUninit::<libc::rusage_info_v4>::uninit();
    // SAFETY: proc_pid_rusage writes a `rusage_info_v4` for flavor
    // RUSAGE_INFO_V4 into the provided buffer, which is sized for exactly that.
    let rc = unsafe {
        libc::proc_pid_rusage(
            std::process::id() as libc::c_int,
            libc::RUSAGE_INFO_V4,
            info.as_mut_ptr().cast::<libc::rusage_info_t>(),
        )
    };
    if rc != 0 {
        return None;
    }
    // SAFETY: rc == 0 means the buffer was initialized.
    let info = unsafe { info.assume_init() };
    // A zero footprint is not a real reading — treat it as unavailable rather
    // than reporting a process that uses no memory.
    (info.ri_phys_footprint > 0).then_some(info.ri_phys_footprint)
}

/// No footprint accounting outside macOS; Linux RSS is genuinely resident and
/// the external guard there (if any) measures something else entirely.
#[cfg(not(target_os = "macos"))]
#[must_use]
pub fn current_phys_footprint_bytes() -> Option<u64> {
    None
}

/// Default-zone malloc occupancy. `size_in_use` is live, `size_allocated -
/// size_in_use` is held but free (magazine retention).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MallocZoneStats {
    /// Bytes currently in use by live allocations.
    pub bytes_in_use: u64,
    /// Bytes the zone holds but has not returned to the OS.
    pub bytes_held_free: u64,
}

/// Lowest allowed effective warm budget: the configured value, but never
/// below [`EFFECTIVE_WARM_FLOOR_BYTES`] unless the configured value is
/// already smaller (test/preset nodes).
#[must_use]
pub fn effective_warm_floor_bytes(configured_warm_bytes: u64) -> u64 {
    if configured_warm_bytes == 0 {
        return EFFECTIVE_WARM_FLOOR_BYTES;
    }
    configured_warm_bytes.min(EFFECTIVE_WARM_FLOOR_BYTES)
}

/// Inputs for one warm-budget decision.
///
/// `post_step_resident` is the number the drain stores for `fits`: the step
/// target before the eviction, and `bytes_after` after it. It is not the
/// configured env and not `floor_bytes`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EffectiveWarmInput {
    pub configured_warm_bytes: u64,
    pub current_effective_bytes: u64,
    pub post_step_resident: u64,
    pub pressure_high: bool,
    /// `phys_footprint` is above the pressure stop (4 GiB, after the RAM min).
    pub footprint_above_pressure_target: bool,
    /// Latest scored purge: `measured - footprint_net` ≤ 512 MiB.
    /// Stays false until a score exists. A failed score does not grow.
    pub purge_ok: bool,
    /// At or above the operating hard line (6 GiB, after the RAM min).
    /// The 12 GiB backstop is not this flag and does not permit grow-back.
    pub footprint_at_or_above_operating_hard: bool,
}

/// Next effective warm budget after one sample.
///
/// `fits` treats 0 as "no budget" and admits every group. A configured budget
/// of 0 still means enforcement is off. Every other result is at least 1.
///
/// While pressure is high and footprint is above the 4 GiB stop, the result
/// is the post-step resident. The 7 GiB env is not returned on that path.
/// Grow-back is one [`EFFECTIVE_WARM_GROW_STEP_BYTES`] step, and only when
/// pressure is clear, the purge predicate passed, and footprint is under the
/// operating hard line. The cap is `min(configured, 2 GiB)`.
#[must_use]
pub fn next_effective_warm_bytes(input: EffectiveWarmInput) -> u64 {
    if input.configured_warm_bytes == 0 {
        return 0;
    }
    if input.pressure_high && input.footprint_above_pressure_target {
        return input.post_step_resident.max(1);
    }
    if input.pressure_high || !input.purge_ok || input.footprint_at_or_above_operating_hard {
        return input
            .current_effective_bytes
            .min(input.post_step_resident)
            .max(1);
    }
    let cap = input
        .configured_warm_bytes
        .min(EFFECTIVE_WARM_CLEAR_CAP_BYTES);
    input
        .current_effective_bytes
        .saturating_add(EFFECTIVE_WARM_GROW_STEP_BYTES)
        .min(cap)
        .max(1)
}
