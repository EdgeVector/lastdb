//! The measured-footprint defence tick, split into its phases.
//!
//! [`defend_measured_footprint`] reads like the tick: sample the host, purge
//! the allocator, refresh the governor snapshot, move the warm budget, run the
//! stepwise eviction pass, then record the result. Each phase is a function
//! here so the order stays visible and each phase owns one lock scope.

#[path = "defend/pass.rs"]
mod pass;

use pass::*;

use super::*;

/// The footprint lines that depend on whether host pressure is high.
#[derive(Clone, Copy)]
struct PressureLines {
    pressure_high: bool,
    pressure_target: u64,
    pressure_hard: u64,
    evict_target_bytes: u64,
    state_hard_bytes: u64,
}

impl PressureLines {
    fn new(pressure_high: bool, ram_bytes: u64) -> Self {
        let pressure_target = fold_db::memory_budget::pressure_footprint_target_bytes(ram_bytes);
        let pressure_hard = fold_db::memory_budget::pressure_footprint_hard_bytes(ram_bytes);
        Self {
            pressure_high,
            pressure_target,
            pressure_hard,
            evict_target_bytes: if pressure_high {
                pressure_target
            } else {
                fold_db::memory_budget::FOOTPRINT_EVICT_TARGET_BYTES
            },
            state_hard_bytes: if pressure_high {
                pressure_hard
            } else {
                fold_db::memory_budget::FOOTPRINT_EVICT_HARD_BYTES
            },
        }
    }

    /// Pressure drain: while footprint is above the pressure target, a resident
    /// set at the floor is not success.
    fn suppress_floor(&self, footprint: u64) -> bool {
        self.pressure_high && footprint > self.pressure_target
    }
}

/// What the governor shows about shed and sticky holds after this tick's reset.
struct HoldState {
    footprint_sticky: bool,
    shed_hold: bool,
}

fn warm_resident_bytes(host: &Host, default: u64) -> u64 {
    host.db
        .db_ops()
        .read_cost()
        .map_or(default, |c| c.warm_resident_bytes)
}

/// Evict idle warm groups and hold or grow the effective budget from a live
/// physical-footprint sample. Never refuses a request.
///
/// Host pressure replaces the 10 / 12 / 8 GiB lines with a 4 GiB stop and a
/// 6 GiB hard line. The eviction target is `resident - step` with no floor,
/// and `fits` receives that post-step resident (never 0). The configured warm
/// env stays the maximum. It is not passed to `fits` during the drain.
///
/// Pressure-clear keeps the 10 / 12 / 8 GiB backstop. The budget grows only
/// after pressure has been clear and a scored purge left at most 512 MiB of
/// slack inside `phys_footprint`, one 256 MiB step, capped at `min(env, 2 GiB)`.
///
/// A sticky trim brake does not skip the pressure loop. `sticky-cooldown` is
/// only a pressure-clear state under the 10 GiB soft line.
pub fn defend_measured_footprint(host: &Host, measured_footprint_bytes: u64) {
    let in_flight = uds_in_flight(host);
    let now = unix_secs();
    observe_host_pressure(in_flight, now, measured_footprint_bytes);

    let pressure_high = footprint_governor()
        .lock()
        .is_ok_and(|state| state.pressure.is_high());
    let (measured_footprint_bytes, initial_released) =
        purge_for_tick(in_flight, now, pressure_high, measured_footprint_bytes);

    maybe_capture_vmmap(&host.home, measured_footprint_bytes);
    let store = host.db.db_ops().namespaced_store();
    // Body budget is 0 for the whole time pressure is high, including after
    // the drain stops under 4 GiB. This is not the drain-hold flag.
    store.set_host_pressure_high(pressure_high);
    let budget = fold_db::memory_budget::process_memory_budget();
    let stats = store.warm_set_admission_stats().unwrap_or_default();
    let resident = warm_resident_bytes(host, 0);
    let malloc = crate::allocator::occupancy();
    let footprint_net_bytes =
        fold_db::memory_budget::footprint_net_bytes(measured_footprint_bytes, malloc);
    let (ram_bytes, purge_ok, purge_failed) = footprint_governor()
        .lock()
        .map_or((0, false, false), |state| {
            (state.ram_bytes, state.purge_ok, state.purge_failed)
        });
    let lines = PressureLines::new(pressure_high, ram_bytes);
    let floor = fold_db::memory_budget::effective_warm_floor_bytes(budget.warm_bytes);
    // Stays until the next tick so a request that finishes before that tick
    // can collect. Do not clear it from a later reading in this same tick.
    crate::allocator::note_tick_over_slack_line(tick_over_slack_line(
        measured_footprint_bytes,
        footprint_net_bytes,
    ));
    let hold = refresh_holds(
        &lines,
        purge_failed,
        measured_footprint_bytes,
        footprint_net_bytes,
        now,
    );
    trace_tick(
        measured_footprint_bytes,
        footprint_net_bytes,
        hold.footprint_sticky,
        pressure_high,
    );

    // Shed cooldown skips eviction on a clear host. Pressure still drains:
    // the cooldown must not freeze an 11 GiB pin while the machine is in swap.
    if hold.shed_hold && !pressure_high {
        store.set_warm_drain_hold(false);
        return;
    }

    let mut ctx = EvictContext {
        store: &*store,
        lines,
        warm_bytes: budget.warm_bytes,
        charged_bytes: budget.charged_bytes,
        floor,
        clear_ceiling: stats.effective_warm_bytes,
    };
    let draining = pressure_high && !ctx.should_stop(measured_footprint_bytes, resident);
    // Hold before the body trim. The point over-budget arm must not publish
    // while this tick is about to lower the budget.
    store.set_warm_drain_hold(draining);
    adjust_budget_before_pass(
        &mut ctx,
        draining,
        stats.effective_warm_bytes,
        resident,
        purge_ok,
        measured_footprint_bytes,
    );

    let run_evict = draining
        || (!pressure_high
            && measured_footprint_bytes > fold_db::memory_budget::FOOTPRINT_EVICT_SOFT_BYTES);
    if !run_evict {
        store.set_warm_drain_hold(false);
        return;
    }

    let start = TickStart {
        now,
        in_flight,
        measured_footprint_bytes,
        footprint_net_bytes,
        initial_released,
        footprint_sticky: hold.footprint_sticky,
    };
    // Re-read after the body trim. The stop check above used the pre-trim resident.
    let pass = run_evict_pass(host, &ctx, &start, warm_resident_bytes(host, resident));
    finish_tick(&ctx, &pass, hold.shed_hold);
    log_pass(&ctx, &pass, start.measured_footprint_bytes);
}

fn trace_tick(
    measured_footprint_bytes: u64,
    footprint_net_bytes: u64,
    footprint_sticky: bool,
    pressure_high: bool,
) {
    let malloc = crate::allocator::occupancy();
    tracing::debug!(
        target: "lastdbd::footprint",
        phys_footprint_bytes = measured_footprint_bytes,
        footprint_net_bytes,
        malloc_bytes_in_use = malloc.map_or(0, |stats| stats.bytes_in_use),
        malloc_bytes_held_free = malloc.map_or(0, |stats| stats.bytes_held_free),
        footprint_sticky,
        host_pressure = if pressure_high { "high" } else { "clear" },
        "footprint governor tick"
    );
}

/// Feed the host sample into the latch and score a pending purge.
fn observe_host_pressure(in_flight: usize, now: u64, measured_footprint_bytes: u64) {
    let sample = fold_db::memory_budget::sample_host_pressure();
    // A failed sample must not look like a clear host. Leave the latch alone.
    if let Ok(mut state) = footprint_governor().lock() {
        if let Some(sample) = sample {
            state.pressure = state.pressure.observe(sample.is_raw_high(), now);
            state.swap_used_bytes = sample.swap_used_bytes;
            state.compressor_bytes = sample.compressor_bytes;
            state.ram_bytes = sample.ram_bytes;
        }
        if state.purge_awaiting_collect && in_flight == 0 {
            record_purge_score(&mut state, measured_footprint_bytes);
        }
        note_host_pressure(&mut state);
    }
}

/// Purge the allocator when the soft line, slack, or host pressure asks for it.
/// Returns the footprint to use for the rest of the tick and the bytes released.
fn purge_for_tick(
    in_flight: usize,
    now: u64,
    pressure_high: bool,
    mut measured_footprint_bytes: u64,
) -> (u64, u64) {
    let over_soft_line =
        measured_footprint_bytes > fold_db::memory_budget::FOOTPRINT_EVICT_SOFT_BYTES;
    // Below the soft line, footprint defense never runs, so it would never
    // purge either — unless the host is already in swap. Pressure purges
    // before any cache trim, on the same path as the soft line.
    let slack_purge_due = allocator_slack_purge_due(
        measured_footprint_bytes,
        crate::allocator::occupancy(),
        over_soft_line,
        allocator_slack_purge_cooldown_elapsed(now),
    );
    let purge_this_tick = over_soft_line || slack_purge_due || pressure_high;
    let initial_released = if purge_this_tick {
        if slack_purge_due {
            mark_allocator_slack_purge(now);
        }
        crate::allocator::purge()
    } else {
        0
    };
    if purge_this_tick {
        measured_footprint_bytes = fold_db::memory_budget::current_phys_footprint_bytes()
            .unwrap_or(measured_footprint_bytes);
    }
    if let Ok(mut state) = footprint_governor().lock() {
        if purge_this_tick {
            state.malloc_bytes_released_last_purge = initial_released;
            state.snapshot.malloc_bytes_released_last_purge = initial_released;
            if in_flight == 0 {
                // No request is in flight: the sampler heap is the only one,
                // and purge() already collected it. Score this tick.
                record_purge_score(&mut state, measured_footprint_bytes);
            } else {
                // Workers may still hold freed pages. Do not score, and do
                // not grow, until a later tick sees no request in flight.
                state.purge_awaiting_collect = true;
                state.purge_ok = false;
            }
        }
        note_host_pressure(&mut state);
    }
    (measured_footprint_bytes, initial_released)
}

/// Expire the shed and sticky holds, end a finished episode, and classify the
/// governor state for the snapshot.
fn refresh_holds(
    lines: &PressureLines,
    purge_failed: bool,
    measured_footprint_bytes: u64,
    footprint_net_bytes: u64,
    now: u64,
) -> HoldState {
    let mut hold = HoldState {
        footprint_sticky: false,
        shed_hold: false,
    };
    if let Ok(mut state) = footprint_governor().lock() {
        if state.shed_until_epoch_secs > now {
            hold.shed_hold = true;
        } else {
            state.shed_until_epoch_secs = 0;
        }
        if state.sticky_until_epoch_secs > now {
            hold.footprint_sticky = true;
        } else if state.sticky_until_epoch_secs != 0 {
            state.sticky_until_epoch_secs = 0;
            state.trim_response.reset();
            state.snapshot.footprint_delta_per_evicted_byte = None;
        }
        // The episode is over only when the host is clear and under the
        // pressure-clear soft line. Resetting at 9 GiB while swap is still
        // high would drop the trim brake in the middle of the drain.
        if !lines.pressure_high
            && measured_footprint_bytes <= fold_db::memory_budget::FOOTPRINT_EVICT_SOFT_BYTES
        {
            state.trim_response.reset();
            state.snapshot.footprint_delta_per_evicted_byte = None;
        }
        let governor_state = classify_governor_state(&GovernorClass {
            pressure_high: lines.pressure_high,
            purge_failed,
            footprint_bytes: measured_footprint_bytes,
            hard_bytes: lines.state_hard_bytes,
            target_bytes: lines.pressure_target,
            footprint_sticky: hold.footprint_sticky,
            evict_stalled: false,
            shed_hold: hold.shed_hold,
        });
        state.snapshot.footprint_sticky = hold.footprint_sticky;
        state.snapshot.footprint_net_bytes = footprint_net_bytes;
        set_governor_state(&mut state.snapshot, governor_state, now);
        note_host_pressure(&mut state);
    }
    hold
}
