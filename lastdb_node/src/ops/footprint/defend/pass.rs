//! The stepwise eviction pass of the footprint tick and the bookkeeping around it.

use fold_db::memory_budget::EffectiveWarmInput;
use fold_db::storage::traits::WarmSetEvictionReport;
use fold_db::storage::NamespacedStore;

use super::*;

/// Warm-budget inputs that stay fixed for one tick.
pub(super) struct EvictContext<'a> {
    pub(super) store: &'a dyn NamespacedStore,
    pub(super) lines: PressureLines,
    pub(super) warm_bytes: u64,
    pub(super) charged_bytes: u64,
    pub(super) floor: u64,
    pub(super) clear_ceiling: u64,
}

impl EvictContext<'_> {
    pub(super) fn should_stop(&self, footprint: u64, resident: u64) -> bool {
        footprint_evict_should_stop(
            footprint,
            self.charged_bytes,
            resident,
            self.floor,
            self.lines.evict_target_bytes,
            self.lines.suppress_floor(footprint),
        )
    }
}

/// Per-tick readings the eviction pass starts from.
pub(super) struct TickStart {
    pub(super) now: u64,
    pub(super) in_flight: usize,
    pub(super) measured_footprint_bytes: u64,
    pub(super) footprint_net_bytes: u64,
    pub(super) initial_released: u64,
    pub(super) footprint_sticky: bool,
}

/// Result of the stepwise eviction pass.
pub(super) struct EvictPass {
    pub(super) footprint: u64,
    pub(super) footprint_net_bytes: u64,
    pub(super) footprint_sticky: bool,
    pub(super) evict_stalled: bool,
    pub(super) steps: u32,
    pub(super) total_groups_evicted: u64,
    pub(super) warm_bytes_before_pass: Option<u64>,
    pub(super) warm_bytes_after_pass: u64,
    pub(super) malloc_bytes_released: u64,
    pub(super) final_resident: u64,
}

/// Trim bodies while draining, or grow the effective budget on a clear host,
/// and record the ceiling the eviction loop must not publish above.
pub(super) fn adjust_budget_before_pass(
    ctx: &mut EvictContext<'_>,
    draining: bool,
    current_effective_bytes: u64,
    resident: u64,
    purge_ok: bool,
    measured_footprint_bytes: u64,
) {
    // The clear-host eviction loop must not publish a budget above this.
    // A miss used to store the pre-step resident and put the 7 GiB pin back.
    ctx.clear_ceiling = current_effective_bytes;
    if draining {
        // Trim bodies while the effective budget is still the old ceiling.
        // The trim's trailing evict uses that ceiling; lowering it first would
        // drop groups inside the trim and the next step would look stalled.
        if let Err(error) = ctx.store.trim_warm_cache_for_pressure() {
            tracing::warn!(
                target: "lastdbd::footprint",
                error = %error,
                "pressure body trim failed"
            );
        }
    } else if ctx.warm_bytes > 0 {
        let next = fold_db::memory_budget::next_effective_warm_bytes(EffectiveWarmInput {
            configured_warm_bytes: ctx.warm_bytes,
            current_effective_bytes,
            post_step_resident: resident,
            pressure_high: ctx.lines.pressure_high,
            footprint_above_pressure_target: measured_footprint_bytes > ctx.lines.pressure_target,
            purge_ok,
            footprint_at_or_above_operating_hard: measured_footprint_bytes
                >= ctx.lines.pressure_hard,
        });
        if next != current_effective_bytes {
            ctx.store.set_effective_warm_bytes(next);
        }
        ctx.clear_ceiling = next;
    }
}

/// Publish the warm budget for `fits` after one step, when the budget is on.
pub(super) fn publish_step_budget(ctx: &EvictContext<'_>, bytes_after: u64, evict_to: u64) {
    if ctx.warm_bytes > 0 {
        ctx.store
            .set_effective_warm_bytes(published_warm_budget_after_step(
                ctx.lines.pressure_high,
                bytes_after,
                evict_to,
                ctx.clear_ceiling,
                ctx.floor,
            ));
    }
}

/// Aim one eviction step and run it. `None` means the backend has no warm set
/// or the step failed, so the pass is stalled.
fn evict_step(
    ctx: &EvictContext<'_>,
    footprint: u64,
    current_resident: u64,
) -> Option<(u64, WarmSetEvictionReport)> {
    let step = footprint_evict_step_bytes(footprint, ctx.lines.evict_target_bytes);
    // Pressure does not raise the target back to the 1 GiB floor or the
    // configured env. A clear host stays at the floor and under the
    // ceiling `next_effective_warm_bytes` just stored.
    let evict_to = footprint_evict_to_bytes(
        ctx.lines.pressure_high,
        current_resident,
        step,
        ctx.floor,
        ctx.clear_ceiling,
    );
    publish_step_budget(ctx, evict_to, evict_to);
    match ctx.store.evict_warm_set_to_bytes(evict_to) {
        Ok(report) => report.map(|report| (evict_to, report)),
        Err(error) => {
            tracing::warn!(
                target: "lastdbd::footprint",
                error = %error,
                "footprint-driven LRU eviction step failed"
            );
            None
        }
    }
}

/// Evict in steps until the stop rule holds, a step stalls, or the step cap.
pub(super) fn run_evict_pass(
    host: &Host,
    ctx: &EvictContext<'_>,
    start: &TickStart,
    mut current_resident: u64,
) -> EvictPass {
    let mut pass = EvictPass {
        footprint: start.measured_footprint_bytes,
        footprint_net_bytes: start.footprint_net_bytes,
        footprint_sticky: start.footprint_sticky,
        evict_stalled: false,
        steps: 0,
        total_groups_evicted: 0,
        warm_bytes_before_pass: None,
        warm_bytes_after_pass: 0,
        malloc_bytes_released: start.initial_released,
        final_resident: current_resident,
    };

    while pass.steps < fold_db::memory_budget::FOOTPRINT_EVICT_MAX_STEPS {
        if ctx.should_stop(pass.footprint, current_resident) {
            break;
        }
        let Some((evict_to, report)) = evict_step(ctx, pass.footprint, current_resident) else {
            pass.evict_stalled = true;
            break;
        };
        if pass.warm_bytes_before_pass.is_none() {
            pass.warm_bytes_before_pass = Some(report.bytes_before);
        }
        pass.warm_bytes_after_pass = report.bytes_after;
        pass.total_groups_evicted = pass
            .total_groups_evicted
            .saturating_add(report.groups_evicted);
        pass.steps += 1;
        let warm_bytes_freed = report.bytes_before.saturating_sub(report.bytes_after);
        // A miss leaves bytes_after at the pre-step resident. Storing that
        // would hand `fits` the full pin again. Keep the lower of the step
        // target, the clear-host ceiling, and what the step actually left.
        publish_step_budget(ctx, report.bytes_after, evict_to);
        if report.groups_evicted == 0 && warm_bytes_freed == 0 {
            pass.evict_stalled = true;
            break;
        }
        let released = crate::allocator::purge();
        pass.malloc_bytes_released = pass.malloc_bytes_released.saturating_add(released);
        let footprint_before = pass.footprint;
        pass.footprint =
            fold_db::memory_budget::current_phys_footprint_bytes().unwrap_or(pass.footprint);
        pass.footprint_net_bytes = fold_db::memory_budget::footprint_net_bytes(
            pass.footprint,
            crate::allocator::occupancy(),
        );
        score_step(
            &mut pass,
            start,
            warm_bytes_freed,
            released,
            footprint_before,
        );
        tracing::warn!(
            target: "lastdbd::footprint",
            step = pass.steps,
            groups_evicted = report.groups_evicted,
            warm_bytes_before = report.bytes_before,
            warm_bytes_after = report.bytes_after,
            warm_bytes_freed,
            phys_footprint_before = footprint_before,
            phys_footprint_after = pass.footprint,
            footprint_net_bytes = pass.footprint_net_bytes,
            "footprint eviction step"
        );
        if report.groups_evicted == 0 {
            pass.evict_stalled = true;
            break;
        }
        current_resident = warm_resident_bytes(host, report.bytes_after);
    }
    pass.final_resident = warm_resident_bytes(host, current_resident);
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

/// Release the drain hold when the pass met the stop rule, then classify the
/// final governor state.
pub(super) fn finish_tick(ctx: &EvictContext<'_>, pass: &EvictPass, shed_hold: bool) {
    let stopped = ctx.should_stop(pass.footprint, pass.final_resident);
    ctx.store
        .set_warm_drain_hold(ctx.lines.pressure_high && !stopped);

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

pub(super) fn log_pass(ctx: &EvictContext<'_>, pass: &EvictPass, measured_footprint_bytes: u64) {
    if pass.steps == 0 {
        return;
    }
    let effective_warm_bytes = ctx
        .store
        .warm_set_admission_stats()
        .map_or(0, |stats| stats.effective_warm_bytes);
    tracing::warn!(
        target: "lastdbd::footprint",
        measured_footprint_mb = measured_footprint_bytes / (1024 * 1024),
        final_footprint_mb = pass.footprint / (1024 * 1024),
        steps = pass.steps,
        groups_evicted = pass.total_groups_evicted,
        warm_bytes_before = pass.warm_bytes_before_pass.unwrap_or(0),
        warm_bytes_after = pass.warm_bytes_after_pass,
        effective_warm_bytes,
        malloc_bytes_released = pass.malloc_bytes_released,
        evict_stalled = pass.evict_stalled,
        "measured footprint defence evicted LRU warm groups stepwise"
    );
}
