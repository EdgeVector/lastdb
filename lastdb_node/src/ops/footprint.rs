//! Measured-footprint LRU defence and operator shed.
//!
//! The node never refuses a request because memory is high. When the host is
//! under memory pressure, or `phys_footprint` crosses the soft line, it evicts
//! idle warm groups in LRU order, holds the effective warm budget at the
//! post-step resident, and asks malloc to return free pages. `/api/admin/shed`
//! is the same path driven by the external guard. The configured warm env is
//! a maximum. It is not the budget `fits` sees during a pressure drain.

use fold_db::clock::unix_secs;
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};

use fold_db::memory_budget::{EffectiveWarmInput, IneffectiveTrimTracker, PressureLatch};

use crate::host::Host;

#[path = "footprint/evict.rs"]
mod evict;
#[path = "footprint/governor.rs"]
mod governor;

pub use evict::*;
pub use governor::*;

const GIB: u64 = 1024 * 1024 * 1024;

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
pub fn defend_measured_footprint(host: &Host, mut measured_footprint_bytes: u64) {
    let in_flight = uds_in_flight(host);
    let now = unix_secs();
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

    let pressure_high = footprint_governor()
        .lock()
        .is_ok_and(|state| state.pressure.is_high());
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

    maybe_capture_vmmap(&host.home, measured_footprint_bytes);
    let store = host.db.db_ops().namespaced_store();
    // Body budget is 0 for the whole time pressure is high, including after
    // the drain stops under 4 GiB. This is not the drain-hold flag.
    store.set_host_pressure_high(pressure_high);
    let budget = fold_db::memory_budget::process_memory_budget();
    let stats = store.warm_set_admission_stats().unwrap_or_default();
    let resident = host
        .db
        .db_ops()
        .read_cost()
        .map_or(0, |c| c.warm_resident_bytes);
    let malloc = crate::allocator::occupancy();
    let mut footprint_net_bytes =
        fold_db::memory_budget::footprint_net_bytes(measured_footprint_bytes, malloc);
    let (ram_bytes, purge_ok, purge_failed) = footprint_governor()
        .lock()
        .map_or((0, false, false), |state| {
            (state.ram_bytes, state.purge_ok, state.purge_failed)
        });
    let pressure_target = fold_db::memory_budget::pressure_footprint_target_bytes(ram_bytes);
    let pressure_hard = fold_db::memory_budget::pressure_footprint_hard_bytes(ram_bytes);
    let evict_target_bytes = if pressure_high {
        pressure_target
    } else {
        fold_db::memory_budget::FOOTPRINT_EVICT_TARGET_BYTES
    };
    let state_hard_bytes = if pressure_high {
        pressure_hard
    } else {
        fold_db::memory_budget::FOOTPRINT_EVICT_HARD_BYTES
    };
    let floor = fold_db::memory_budget::effective_warm_floor_bytes(budget.warm_bytes);
    // Stays until the next tick so a request that finishes before that tick
    // can collect. Do not clear it from a later reading in this same tick.
    crate::allocator::note_tick_over_slack_line(tick_over_slack_line(
        measured_footprint_bytes,
        footprint_net_bytes,
    ));
    let mut footprint_sticky = false;
    let mut shed_hold = false;
    if let Ok(mut state) = footprint_governor().lock() {
        if state.shed_until_epoch_secs > now {
            shed_hold = true;
        } else {
            state.shed_until_epoch_secs = 0;
        }
        if state.sticky_until_epoch_secs > now {
            footprint_sticky = true;
        } else if state.sticky_until_epoch_secs != 0 {
            state.sticky_until_epoch_secs = 0;
            state.trim_response.reset();
            state.snapshot.footprint_delta_per_evicted_byte = None;
        }
        // The episode is over only when the host is clear and under the
        // pressure-clear soft line. Resetting at 9 GiB while swap is still
        // high would drop the trim brake in the middle of the drain.
        if !pressure_high
            && measured_footprint_bytes <= fold_db::memory_budget::FOOTPRINT_EVICT_SOFT_BYTES
        {
            state.trim_response.reset();
            state.snapshot.footprint_delta_per_evicted_byte = None;
        }
        let governor_state = classify_governor_state(&GovernorClass {
            pressure_high,
            purge_failed,
            footprint_bytes: measured_footprint_bytes,
            hard_bytes: state_hard_bytes,
            target_bytes: pressure_target,
            footprint_sticky,
            evict_stalled: false,
            shed_hold,
        });
        state.snapshot.footprint_sticky = footprint_sticky;
        state.snapshot.footprint_net_bytes = footprint_net_bytes;
        set_governor_state(&mut state.snapshot, governor_state, now);
        note_host_pressure(&mut state);
    }
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

    // Shed cooldown skips eviction on a clear host. Pressure still drains:
    // the cooldown must not freeze an 11 GiB pin while the machine is in swap.
    if shed_hold && !pressure_high {
        store.set_warm_drain_hold(false);
        return;
    }

    let suppress_floor = pressure_high && measured_footprint_bytes > pressure_target;
    let already_stopped = footprint_evict_should_stop(
        measured_footprint_bytes,
        budget.charged_bytes,
        resident,
        floor,
        evict_target_bytes,
        suppress_floor,
    );
    let draining = pressure_high && !already_stopped;
    // Hold before the body trim. The point over-budget arm must not publish
    // while this tick is about to lower the budget.
    store.set_warm_drain_hold(draining);

    // The clear-host eviction loop must not publish a budget above this.
    // A miss used to store the pre-step resident and put the 7 GiB pin back.
    let mut clear_ceiling = stats.effective_warm_bytes;
    if draining {
        // Trim bodies while the effective budget is still the old ceiling.
        // The trim's trailing evict uses that ceiling; lowering it first would
        // drop groups inside the trim and the next step would look stalled.
        if let Err(error) = store.trim_warm_cache_for_pressure() {
            tracing::warn!(
                target: "lastdbd::footprint",
                error = %error,
                "pressure body trim failed"
            );
        }
    } else if budget.warm_bytes > 0 {
        let next = fold_db::memory_budget::next_effective_warm_bytes(EffectiveWarmInput {
            configured_warm_bytes: budget.warm_bytes,
            current_effective_bytes: stats.effective_warm_bytes,
            post_step_resident: resident,
            pressure_high,
            footprint_above_pressure_target: measured_footprint_bytes > pressure_target,
            purge_ok,
            footprint_at_or_above_operating_hard: measured_footprint_bytes >= pressure_hard,
        });
        if next != stats.effective_warm_bytes {
            store.set_effective_warm_bytes(next);
        }
        clear_ceiling = next;
    }

    let run_evict = draining
        || (!pressure_high
            && measured_footprint_bytes > fold_db::memory_budget::FOOTPRINT_EVICT_SOFT_BYTES);
    if !run_evict {
        store.set_warm_drain_hold(false);
        return;
    }

    let mut footprint = measured_footprint_bytes;
    let mut steps = 0u32;
    let mut total_groups_evicted = 0u64;
    let mut warm_bytes_before_pass = None;
    let mut warm_bytes_after_pass = 0u64;
    let mut malloc_bytes_released = initial_released;
    let mut evict_stalled = false;
    // Re-read after the body trim. The stop check above used the pre-trim resident.
    let mut current_resident = host
        .db
        .db_ops()
        .read_cost()
        .map_or(resident, |c| c.warm_resident_bytes);

    while steps < fold_db::memory_budget::FOOTPRINT_EVICT_MAX_STEPS {
        let suppress_floor = pressure_high && footprint > pressure_target;
        if footprint_evict_should_stop(
            footprint,
            budget.charged_bytes,
            current_resident,
            floor,
            evict_target_bytes,
            suppress_floor,
        ) {
            break;
        }
        let step = footprint_evict_step_bytes(footprint, evict_target_bytes);
        // Pressure does not raise the target back to the 1 GiB floor or the
        // configured env. A clear host stays at the floor and under the
        // ceiling `next_effective_warm_bytes` just stored.
        let evict_to =
            footprint_evict_to_bytes(pressure_high, current_resident, step, floor, clear_ceiling);
        if budget.warm_bytes > 0 {
            store.set_effective_warm_bytes(published_warm_budget_after_step(
                pressure_high,
                evict_to,
                evict_to,
                clear_ceiling,
                floor,
            ));
        }
        let report = match store.evict_warm_set_to_bytes(evict_to) {
            Ok(Some(report)) => report,
            Ok(None) => {
                evict_stalled = true;
                break;
            }
            Err(error) => {
                tracing::warn!(
                    target: "lastdbd::footprint",
                    error = %error,
                    "footprint-driven LRU eviction step failed"
                );
                evict_stalled = true;
                break;
            }
        };
        if warm_bytes_before_pass.is_none() {
            warm_bytes_before_pass = Some(report.bytes_before);
        }
        warm_bytes_after_pass = report.bytes_after;
        total_groups_evicted = total_groups_evicted.saturating_add(report.groups_evicted);
        steps += 1;
        let warm_bytes_freed = report.bytes_before.saturating_sub(report.bytes_after);
        if budget.warm_bytes > 0 {
            // A miss leaves bytes_after at the pre-step resident. Storing that
            // would hand `fits` the full pin again. Keep the lower of the step
            // target, the clear-host ceiling, and what the step actually left.
            store.set_effective_warm_bytes(published_warm_budget_after_step(
                pressure_high,
                report.bytes_after,
                evict_to,
                clear_ceiling,
                floor,
            ));
        }
        if report.groups_evicted == 0 && warm_bytes_freed == 0 {
            evict_stalled = true;
            break;
        }
        let released = crate::allocator::purge();
        malloc_bytes_released = malloc_bytes_released.saturating_add(released);
        let footprint_before = footprint;
        footprint = fold_db::memory_budget::current_phys_footprint_bytes().unwrap_or(footprint);
        footprint_net_bytes =
            fold_db::memory_budget::footprint_net_bytes(footprint, crate::allocator::occupancy());
        if let Ok(mut state) = footprint_governor().lock() {
            state.snapshot.warm_bytes_freed = state
                .snapshot
                .warm_bytes_freed
                .saturating_add(warm_bytes_freed);
            state.malloc_bytes_released_last_purge = released;
            state.snapshot.footprint_net_bytes = footprint_net_bytes;
            state.snapshot.malloc_bytes_released_last_purge = released;
            // Zero warm bytes freed is not a trim result. Requests still in flight
            // have not had a chance to return pages, so that step is not a miss either.
            if warm_bytes_freed > 0 && in_flight == 0 {
                let sticky =
                    state
                        .trim_response
                        .observe(warm_bytes_freed, footprint_before, footprint);
                state.snapshot.footprint_delta_per_evicted_byte =
                    state.trim_response.last_response_ratio();
                if sticky {
                    state.sticky_until_epoch_secs = now.saturating_add(sticky_cooldown_secs());
                    state.snapshot.footprint_sticky = true;
                    footprint_sticky = true;
                }
            }
        }
        tracing::warn!(
            target: "lastdbd::footprint",
            step = steps,
            groups_evicted = report.groups_evicted,
            warm_bytes_before = report.bytes_before,
            warm_bytes_after = report.bytes_after,
            warm_bytes_freed,
            phys_footprint_before = footprint_before,
            phys_footprint_after = footprint,
            footprint_net_bytes,
            "footprint eviction step"
        );
        if report.groups_evicted == 0 {
            evict_stalled = true;
            break;
        }
        current_resident = host
            .db
            .db_ops()
            .read_cost()
            .map_or(report.bytes_after, |c| c.warm_resident_bytes);
    }

    let final_resident = host
        .db
        .db_ops()
        .read_cost()
        .map_or(current_resident, |c| c.warm_resident_bytes);
    let suppress_final = pressure_high && footprint > pressure_target;
    let stopped = footprint_evict_should_stop(
        footprint,
        budget.charged_bytes,
        final_resident,
        floor,
        evict_target_bytes,
        suppress_final,
    );
    store.set_warm_drain_hold(pressure_high && !stopped);

    if let Ok(mut state) = footprint_governor().lock() {
        let purge_failed = state.purge_failed;
        let governor_state = classify_governor_state(&GovernorClass {
            pressure_high,
            purge_failed,
            footprint_bytes: footprint,
            hard_bytes: state_hard_bytes,
            target_bytes: pressure_target,
            footprint_sticky: footprint_sticky || state.snapshot.footprint_sticky,
            evict_stalled,
            shed_hold,
        });
        state.snapshot.footprint_sticky = footprint_sticky || state.snapshot.footprint_sticky;
        state.snapshot.footprint_net_bytes = footprint_net_bytes;
        set_governor_state(&mut state.snapshot, governor_state, unix_secs());
        note_host_pressure(&mut state);
    }

    if steps > 0 {
        let effective_warm_bytes = store
            .warm_set_admission_stats()
            .map_or(0, |stats| stats.effective_warm_bytes);
        tracing::warn!(
            target: "lastdbd::footprint",
            measured_footprint_mb = measured_footprint_bytes / (1024 * 1024),
            final_footprint_mb = footprint / (1024 * 1024),
            steps,
            groups_evicted = total_groups_evicted,
            warm_bytes_before = warm_bytes_before_pass.unwrap_or(0),
            warm_bytes_after = warm_bytes_after_pass,
            effective_warm_bytes,
            malloc_bytes_released,
            evict_stalled,
            "measured footprint defence evicted LRU warm groups stepwise"
        );
    }
}
