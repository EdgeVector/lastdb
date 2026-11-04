//! Per-step timing accumulation and request-phase reporting for the write pipeline.

use std::collections::HashMap;

use tracing::debug;

use super::*;

impl MutationManager {
    /// Report one accepted native batch and its complete operation count.
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
    /// Grouping is measured directly. Apply is the remaining wall time,
    /// including staging, commit and durability; it is not a CPU-only measure.
    pub(in crate::fold_db_core::mutation_manager) fn report_phase_totals(
        timing_breakdown: &HashMap<&str, std::time::Duration>,
        total_time: std::time::Duration,
    ) {
        use crate::request_phases::{add_phase, RequestPhase};

        let timed = |key: &str| timing_breakdown.get(key).copied().unwrap_or_default();
        let grouping = timed("grouping");
        let apply = total_time.saturating_sub(grouping);

        add_phase(RequestPhase::Grouping, grouping);
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
