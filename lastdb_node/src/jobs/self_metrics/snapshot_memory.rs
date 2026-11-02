use super::*;

/// Feed the measured footprint into the defer window, log each transition the
/// observation reports, and return the observation for the status snapshot.
pub(super) fn observe_runtime_memory(
    host: &Host,
    footprint: PhysFootprint,
) -> fold_db::memory_budget::RuntimeMemoryBudgetObservation {
    let observation = host
        .db
        .mutation_manager()
        .observe_memory_footprint(footprint.current_bytes);
    crate::jobs::footprint::defend_measured_footprint(host, footprint.current_bytes);
    let budget = fold_db::memory_budget::process_memory_budget();
    let implied_multiplier = if budget.charged_bytes == 0 {
        0.0
    } else {
        footprint.current_bytes as f64 / budget.charged_bytes as f64
    };
    log_projection_warning(footprint, &observation, implied_multiplier);
    log_over_limit_alarm(footprint, &observation, budget, implied_multiplier);
    log_runtime_recovered(footprint, &observation, budget);
    observation
}

fn log_projection_warning(
    footprint: PhysFootprint,
    observation: &fold_db::memory_budget::RuntimeMemoryBudgetObservation,
    implied_multiplier: f64,
) {
    if observation.projection_warning_now {
        tracing::warn!(
            target: "lastdbd::self_metrics",
            measured_footprint_mb = footprint.current_bytes / (1024 * 1024),
            projected_total_mb = observation.projected_total_bytes / (1024 * 1024),
            projection_tolerance_mb = observation.projection_tolerance_bytes / (1024 * 1024),
            implied_footprint_multiplier = implied_multiplier,
            "measured physical footprint diverges from the process memory projection"
        );
    }
}

fn log_over_limit_alarm(
    footprint: PhysFootprint,
    observation: &fold_db::memory_budget::RuntimeMemoryBudgetObservation,
    budget: &fold_db::memory_budget::ProcessMemoryBudget,
    implied_multiplier: f64,
) {
    if observation.over_limit_alarm_now {
        // Only a hard latch actually closes the defer window. A near-limit
        // trip against a fitted boot budget keeps it open, so reporting
        // "deferred writes are disabled" there paged on-call for a
        // condition that had not happened (Sentry 7671016505).
        if observation.runtime_degraded {
            tracing::error!(
                target: "lastdbd::self_metrics",
                measured_footprint_mb = footprint.current_bytes / (1024 * 1024),
                guard_limit_mb = budget.rss_limit_bytes / (1024 * 1024),
                projected_total_mb = observation.projected_total_bytes / (1024 * 1024),
                implied_footprint_multiplier = implied_multiplier,
                effective_deferred_cap_bytes = observation.effective_deferred_cap_bytes,
                "measured physical footprint exceeds the process guard; new deferred writes are disabled and persist inline"
            );
        } else {
            tracing::warn!(
                target: "lastdbd::self_metrics",
                measured_footprint_mb = footprint.current_bytes / (1024 * 1024),
                guard_limit_mb = budget.rss_limit_bytes / (1024 * 1024),
                projected_total_mb = observation.projected_total_bytes / (1024 * 1024),
                implied_footprint_multiplier = implied_multiplier,
                effective_deferred_cap_bytes = observation.effective_deferred_cap_bytes,
                "measured physical footprint crossed the process guard; the derived defer window stays open because the boot budget still fits"
            );
        }
    }
}

fn log_runtime_recovered(
    footprint: PhysFootprint,
    observation: &fold_db::memory_budget::RuntimeMemoryBudgetObservation,
    budget: &fold_db::memory_budget::ProcessMemoryBudget,
) {
    if observation.runtime_recovered_now {
        // The counterpart of the error above: the same episode closes here.
        // Without this line the Sentry record reads "disabled" with no end.
        tracing::info!(
            target: "lastdbd::self_metrics",
            measured_footprint_mb = footprint.current_bytes / (1024 * 1024),
            guard_limit_mb = budget.rss_limit_bytes / (1024 * 1024),
            projected_total_mb = observation.projected_total_bytes / (1024 * 1024),
            recovery_line_mb =
                fold_db::memory_budget::footprint_latch_recovery_line(budget) / (1024 * 1024),
            hold_secs = fold_db::memory_budget::FOOTPRINT_LATCH_RECOVERY_HOLD_SECS,
            effective_deferred_cap_bytes = observation.effective_deferred_cap_bytes,
            "measured physical footprint held under the recovery line; the deferred-write window is open again"
        );
    }
}
