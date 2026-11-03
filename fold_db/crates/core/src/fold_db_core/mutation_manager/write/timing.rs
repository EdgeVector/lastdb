//! Per-step timing accumulation and request-phase reporting for the write pipeline.

use std::collections::HashMap;

use tracing::debug;

use super::*;

impl MutationManager {
    /// Report this pipeline run as ONE logical resident commit and its
    /// operation count.
    ///
    /// Split out so the all-duplicates early return and the applied path
    /// cannot disagree about what a commit is: both reach the gates' owner
    /// exactly once per caller batch, so both count 1.
    pub(super) fn report_resident_commit_counts(operations: &ResidentCommitOperations) {
        use crate::request_phases::{add_counter, RequestCounter};
        add_counter(RequestCounter::ResidentCommits, 1);
        add_counter(RequestCounter::ResidentOperations, operations.total());
    }

    /// Accumulates `elapsed` into the timing map under `key`.
    pub(in crate::fold_db_core::mutation_manager) fn add_timing(
        map: &mut HashMap<&str, std::time::Duration>,
        key: &'static str,
        elapsed: std::time::Duration,
    ) {
        *map.entry(key).or_insert(std::time::Duration::ZERO) += elapsed;
    }

    /// Report the batch write's request phases from timers it already ran.
    ///
    /// The write pipeline times two named steps into `timing_breakdown`:
    /// `"grouping"` (schema grouping before staging) and `"idempotency_check"`
    /// (the duplicate filter inside the native request gate). A missing key
    /// means the step did not run, for example grouping in an all-duplicates
    /// batch, and reports nothing. Each is reported as its own phase. `Apply`
    /// is the residual: `total_time` less those two, saturating so clock
    /// coarseness cannot underflow it. It is not a direct CPU or disk
    /// measurement. The subtraction happens on `Duration`s, before
    /// microsecond conversion, so truncation does not compound.
    pub(in crate::fold_db_core::mutation_manager) fn report_phase_totals(
        timing_breakdown: &HashMap<&str, std::time::Duration>,
        total_time: std::time::Duration,
    ) {
        use crate::request_phases::{add_phase, RequestPhase};

        let timed = |key: &str| timing_breakdown.get(key).copied().unwrap_or_default();
        let grouping = timed("grouping");
        let idempotency_check = timed("idempotency_check");
        let apply = total_time
            .saturating_sub(grouping)
            .saturating_sub(idempotency_check);

        add_phase(RequestPhase::Grouping, grouping);
        add_phase(RequestPhase::IdempotencyCheck, idempotency_check);
        add_phase(RequestPhase::Apply, apply);
    }

    /// Log a sorted timing breakdown for batch mutation phases.
    pub(in crate::fold_db_core::mutation_manager) fn log_timing_breakdown(
        timing_breakdown: &HashMap<&str, std::time::Duration>,
        total_time: std::time::Duration,
    ) {
        tracing::debug!(
            "Batch mutation timing breakdown (total: {:.2}ms):",
            total_time.as_millis()
        );
        let mut sorted_timings: Vec<_> = timing_breakdown.iter().collect();
        sorted_timings.sort_by(|a, b| b.1.cmp(a.1));
        let total_ms = total_time.as_millis() as f64;
        for (operation, duration) in sorted_timings {
            let percentage = (duration.as_millis() as f64 / total_ms) * 100.0;
            debug!(
                "  - {}: {:.2}ms ({:.1}%)",
                operation,
                duration.as_millis(),
                percentage
            );
        }
    }
}
