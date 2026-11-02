use super::*;

/// Cheap runtime view of the memory budget declared at boot.
///
/// The `Option` fields stay off the wire when the platform has no such gauge.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemoryBudgetHealth {
    pub deferred_persist_bytes: u64,
    pub deferred_persist_cap_bytes: u64,
    pub charged_budget_bytes: u64,
    pub projected_total_bytes: u64,
    pub projection_tolerance_bytes: u64,
    pub projection_diverged: bool,
    pub footprint_over_limit: bool,
    pub runtime_degraded: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phys_footprint_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub implied_footprint_multiplier: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub malloc_bytes_in_use: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub malloc_bytes_held_free: Option<u64>,
    pub footprint_net_bytes: u64,
    /// Lifetime warm bytes released by eviction steps.
    pub warm_bytes_freed: u64,
    pub footprint_sticky: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub footprint_delta_per_evicted_byte: Option<f64>,
    pub governor_state: String,
    /// Seconds `governor_state` has HELD its current value at sample time.
    ///
    /// `governor_state` alone cannot separate a one-tick blip from a latch,
    /// and `purge-failed` is the governor's highest-precedence label, so the
    /// reading most likely to be taken as news is the one that masks the
    /// rest while it holds. 0 means the governor has not stamped a
    /// transition yet (a cold process), not that the state just changed.
    pub governor_state_held_secs: u64,
    /// Latched host pressure: `high` or `clear`.
    pub host_pressure: String,
    pub swap_used_bytes: u64,
    pub compressor_bytes: u64,
    pub purge_failed: bool,
    /// Drop in allocator `committed` from the last `purge()`. Not the slack predicate.
    pub malloc_bytes_released_last_purge: u64,
    pub allocator_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allocator_committed_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allocator_reserved_bytes: Option<u64>,
    pub peak_request_bytes: u64,
    pub slowest_request_ms: u64,
    /// Warm set byte charge and limit from the native core.
    pub core_warm: CoreWarmBytesHealth,
}

/// Charged warm set bytes next to the process footprint. The ratio of
/// `used_bytes` growth to footprint growth checks the charge estimate.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoreWarmBytesHealth {
    pub limit_bytes: u64,
    pub used_bytes: u64,
    pub peak_bytes: u64,
    pub purged_bytes: u64,
    pub over_limit_admissions: u64,
}

impl CoreWarmBytesHealth {
    /// One `lastdb status` line. `None` when the status has no warm set limit.
    pub(super) fn status_line(&self) -> Option<String> {
        if self.limit_bytes == 0 {
            return None;
        }
        Some(format!(
            "Warm set bytes: used={} / limit={} peak={} purged={} over_limit_admissions={}",
            format_bytes(self.used_bytes),
            format_bytes(self.limit_bytes),
            format_bytes(self.peak_bytes),
            format_bytes(self.purged_bytes),
            self.over_limit_admissions,
        ))
    }
}

impl From<&fold_db::resident::CoreWarmByteStats> for CoreWarmBytesHealth {
    fn from(stats: &fold_db::resident::CoreWarmByteStats) -> Self {
        Self {
            limit_bytes: stats.limit_bytes as u64,
            used_bytes: stats.used_bytes as u64,
            peak_bytes: stats.peak_bytes as u64,
            purged_bytes: stats.purged_bytes,
            over_limit_admissions: stats.over_limit_admissions,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct MemoryBudgetAttribution {
    pub(crate) allocator: crate::allocator::AllocatorMetrics,
    pub(crate) peak_request_bytes: u64,
    pub(crate) slowest_request_ms: u64,
    /// The footprint governor state, read by the caller after its tick.
    pub(crate) governor: FootprintGovernorSnapshot,
}

impl MemoryBudgetHealth {
    pub(crate) fn measured(
        deferred_persist_bytes: u64,
        deferred_persist_cap_bytes: u64,
        phys_footprint_bytes: Option<u64>,
        runtime: Option<fold_db::memory_budget::RuntimeMemoryBudgetObservation>,
        attribution: MemoryBudgetAttribution,
    ) -> Self {
        let budget = fold_db::memory_budget::process_memory_budget();
        let charged_budget_bytes = budget.charged_bytes;
        let governor = attribution.governor;
        Self {
            deferred_persist_bytes,
            deferred_persist_cap_bytes,
            charged_budget_bytes,
            projected_total_bytes: budget.projected_total_bytes,
            projection_tolerance_bytes: runtime.map_or_else(
                || {
                    (budget.projected_total_bytes as f64
                        * fold_db::memory_budget::FOOTPRINT_PROJECTION_TOLERANCE_FRACTION)
                        as u64
                },
                |observation| observation.projection_tolerance_bytes,
            ),
            projection_diverged: runtime.is_some_and(|observation| observation.projection_diverged),
            footprint_over_limit: runtime
                .is_some_and(|observation| observation.footprint_over_limit),
            runtime_degraded: runtime.is_some_and(|observation| observation.runtime_degraded),
            phys_footprint_bytes,
            implied_footprint_multiplier: phys_footprint_bytes
                .filter(|_| charged_budget_bytes > 0)
                .map(|bytes| bytes as f64 / charged_budget_bytes as f64),
            footprint_net_bytes: governor.footprint_net_bytes,
            warm_bytes_freed: governor.warm_bytes_freed,
            footprint_sticky: governor.footprint_sticky,
            footprint_delta_per_evicted_byte: governor.footprint_delta_per_evicted_byte,
            governor_state: governor.governor_state.to_string(),
            governor_state_held_secs: governor_state_held_secs(
                governor.governor_state_since_epoch_secs,
                fold_db::clock::unix_secs(),
            ),
            host_pressure: governor.host_pressure.to_string(),
            swap_used_bytes: governor.swap_used_bytes,
            compressor_bytes: governor.compressor_bytes,
            purge_failed: governor.purge_failed,
            malloc_bytes_released_last_purge: governor.malloc_bytes_released_last_purge,
            malloc_bytes_in_use: attribution.allocator.bytes_in_use,
            malloc_bytes_held_free: attribution.allocator.bytes_held_free,
            allocator_name: attribution.allocator.name.to_string(),
            allocator_committed_bytes: attribution.allocator.committed_bytes,
            allocator_reserved_bytes: attribution.allocator.reserved_bytes,
            peak_request_bytes: attribution.peak_request_bytes,
            slowest_request_ms: attribution.slowest_request_ms,
            core_warm: CoreWarmBytesHealth::default(),
        }
    }
}
