//! Pressure relief for the native warm set.

use fold_db::storage::disk_pack::PackRuntime;
use std::sync::Arc;

use super::*;

pub(super) struct EvictContext {
    pub(super) runtime: Arc<PackRuntime>,
    pub(super) lines: PressureLines,
}

pub(super) struct TickStart {
    pub(super) now: u64,
    pub(super) in_flight: usize,
    pub(super) measured_footprint_bytes: u64,
    pub(super) footprint_net_bytes: u64,
    pub(super) initial_released: u64,
    pub(super) footprint_sticky: bool,
}

pub(super) struct EvictPass {
    pub(super) footprint: u64,
    pub(super) footprint_net_bytes: u64,
    pub(super) footprint_sticky: bool,
    pub(super) evict_stalled: bool,
    pub(super) steps: u32,
    pub(super) records_purged: u64,
    pub(super) warm_bytes_before_pass: Option<u64>,
    pub(super) warm_bytes_after_pass: u64,
    pub(super) malloc_bytes_released: u64,
}

/// A pass purges only the measured warm record bytes. Definitions, held
/// values, and other process allocations can leave pressure high.
pub(super) fn run_evict_pass(
    ctx: &EvictContext,
    start: &TickStart,
    mut resident: u64,
) -> EvictPass {
    let mut pass = EvictPass {
        footprint: start.measured_footprint_bytes,
        footprint_net_bytes: start.footprint_net_bytes,
        footprint_sticky: start.footprint_sticky,
        evict_stalled: false,
        steps: 0,
        records_purged: 0,
        warm_bytes_before_pass: None,
        warm_bytes_after_pass: resident,
        malloc_bytes_released: start.initial_released,
    };
    while pass.steps < fold_db::memory_budget::FOOTPRINT_EVICT_MAX_STEPS
        && pass.footprint > ctx.lines.evict_target_bytes
    {
        let step = footprint_evict_step_bytes(pass.footprint, ctx.lines.evict_target_bytes);
        let target = resident.saturating_sub(step);
        let report = match ctx.runtime.purge_clean_memory(target) {
            Ok(report) => report,
            Err(error) => {
                tracing::warn!(target: "lastdbd::footprint", %error, "native warm set purge failed");
                pass.evict_stalled = true;
                break;
            }
        };
        pass.warm_bytes_before_pass
            .get_or_insert(report.bytes_before);
        pass.warm_bytes_after_pass = report.bytes_after;
        pass.records_purged += report.records_purged;
        pass.steps += 1;
        let freed = report.bytes_before.saturating_sub(report.bytes_after);
        let released = crate::allocator::purge();
        pass.malloc_bytes_released = pass.malloc_bytes_released.saturating_add(released);
        let before = pass.footprint;
        pass.footprint = fold_db::memory_budget::current_phys_footprint_bytes().unwrap_or(before);
        pass.footprint_net_bytes = fold_db::memory_budget::footprint_net_bytes(
            pass.footprint,
            crate::allocator::occupancy(),
        );
        score_step(&mut pass, start, freed, released, before);
        tracing::warn!(
            target: "lastdbd::footprint",
            records_purged = report.records_purged,
            warm_bytes_before = report.bytes_before,
            warm_bytes_after = report.bytes_after,
            target_bytes = target,
            target_met = report.target_met,
            held_records = report.remaining.held_records,
            definition_bytes = report.remaining.definition_bytes,
            write_metadata_bytes = report.remaining.write_metadata_bytes,
            "native warm set pressure relief"
        );
        resident = report.bytes_after;
        if report.records_purged == 0 || !report.target_met {
            pass.evict_stalled = true;
            break;
        }
    }
    pass
}

/// Record one step in the governor snapshot and feed the trim-response tracker.
pub(super) fn score_step(
    pass: &mut EvictPass,
    start: &TickStart,
    warm_bytes_freed: u64,
    released: u64,
    footprint_before: u64,
) {
    let Ok(mut state) = footprint_governor().lock() else {
        return;
    };
    state.snapshot.warm_bytes_freed = state
        .snapshot
        .warm_bytes_freed
        .saturating_add(warm_bytes_freed);
    state.malloc_bytes_released_last_purge = released;
    state.snapshot.footprint_net_bytes = pass.footprint_net_bytes;
    state.snapshot.malloc_bytes_released_last_purge = released;
    // Zero warm bytes freed is not a trim result. Requests still in flight
    // have not had a chance to return pages, so that step is not a miss either.
    if warm_bytes_freed > 0 && start.in_flight == 0 {
        let sticky =
            state
                .trim_response
                .observe(warm_bytes_freed, footprint_before, pass.footprint);
        state.snapshot.footprint_delta_per_evicted_byte = state.trim_response.last_response_ratio();
        if sticky {
            state.sticky_until_epoch_secs = start.now.saturating_add(sticky_cooldown_secs());
            state.snapshot.footprint_sticky = true;
            pass.footprint_sticky = true;
        }
    }
}

/// Classify the result, including pressure that the clean-record purge cannot resolve.
pub(super) fn finish_tick(ctx: &EvictContext, pass: &EvictPass, shed_hold: bool) {
    if let Ok(mut state) = footprint_governor().lock() {
        let purge_failed = state.purge_failed;
        let sticky = pass.footprint_sticky || state.snapshot.footprint_sticky;
        let governor_state = classify_governor_state(&GovernorClass {
            pressure_high: ctx.lines.pressure_high,
            purge_failed,
            footprint_bytes: pass.footprint,
            hard_bytes: ctx.lines.state_hard_bytes,
            target_bytes: ctx.lines.pressure_target,
            footprint_sticky: sticky,
            evict_stalled: pass.evict_stalled,
            shed_hold,
        });
        state.snapshot.footprint_sticky = sticky;
        state.snapshot.footprint_net_bytes = pass.footprint_net_bytes;
        set_governor_state(&mut state.snapshot, governor_state, unix_secs());
        note_host_pressure(&mut state);
    }
}

pub(super) fn log_pass(pass: &EvictPass, measured_footprint_bytes: u64) {
    if pass.steps == 0 {
        return;
    }
    tracing::warn!(
        target: "lastdbd::footprint",
        measured_footprint_mb = measured_footprint_bytes / (1024 * 1024),
        final_footprint_mb = pass.footprint / (1024 * 1024),
        steps = pass.steps,
        records_purged = pass.records_purged,
        warm_bytes_before = pass.warm_bytes_before_pass.unwrap_or(0),
        warm_bytes_after = pass.warm_bytes_after_pass,
        byte_admission_limit_enforced = false,
        malloc_bytes_released = pass.malloc_bytes_released,
        evict_stalled = pass.evict_stalled,
        "measured footprint defence purged clean warm records"
    );
}
