use super::*;

/// Historical process projection input. Native pool charges are separate.
pub const HASH_GROUP_WARM_BOUNDS: &str =
    "conservative process reserve input; use native core pool counters";

/// Retired graph wire fields; the native core has its own current counters.
pub const RESIDENT_GRAPH_BOUNDS: &str = "retired graph fields; use core_model";

/// What bounds `resident_key_count`. The path-count cap is retired; the warm
/// set is limited by charged bytes (`memory_budget.core_warm.limit_bytes`).
pub const LOGICAL_KEY_MEMORY_LIMIT: &str =
    "no path-count limit; the warm set is limited by bytes (see Warm set bytes)";

pub(super) fn logical_key_memory_limit_for(_budget: Option<u64>) -> String {
    LOGICAL_KEY_MEMORY_LIMIT.to_string()
}

/// Cheap runtime view of the memory budget declared at boot.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MemoryBudgetHealth {
    pub warm_resident_bytes: u64,
    pub warm_budget_bytes: u64,
    pub warm_resident_groups: u64,
    pub resident_graph_bytes: u64,
    pub resident_graph_budget_bytes: u64,
    pub deferred_persist_bytes: u64,
    pub deferred_persist_cap_bytes: u64,
    #[serde(default)]
    pub deferred_lane_fair_share_bytes: u64,
    #[serde(default)]
    pub deferred_heaviest_lane: String,
    #[serde(default)]
    pub deferred_heaviest_lane_bytes: u64,
    #[serde(default)]
    pub deferred_lane_refuse_bytes: u64,
    #[serde(default)]
    pub deferred_lane_refuse_entries: u64,
    pub charged_budget_bytes: u64,
    #[serde(default)]
    pub projected_total_bytes: u64,
    #[serde(default)]
    pub projection_tolerance_bytes: u64,
    #[serde(default)]
    pub projection_diverged: bool,
    #[serde(default)]
    pub footprint_over_limit: bool,
    #[serde(default)]
    pub runtime_degraded: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phys_footprint_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub implied_footprint_multiplier: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub malloc_bytes_in_use: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub malloc_bytes_held_free: Option<u64>,
    #[serde(default)]
    pub footprint_net_bytes: u64,
    /// Lifetime warm bytes released by eviction steps.
    #[serde(default)]
    pub warm_bytes_freed: u64,
    #[serde(default)]
    pub footprint_sticky: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub footprint_delta_per_evicted_byte: Option<f64>,
    #[serde(default)]
    pub governor_state: String,
    /// Seconds `governor_state` has HELD its current value at sample time.
    ///
    /// `governor_state` alone cannot separate a one-tick blip from a latch,
    /// and `purge-failed` is the governor's highest-precedence label, so the
    /// reading most likely to be taken as news is the one that masks the
    /// rest while it holds. 0 means the governor has not stamped a
    /// transition yet (a cold process), not that the state just changed.
    #[serde(default)]
    pub governor_state_held_secs: u64,
    /// Latched host pressure: `high` or `clear`.
    #[serde(default)]
    pub host_pressure: String,
    #[serde(default)]
    pub swap_used_bytes: u64,
    #[serde(default)]
    pub compressor_bytes: u64,
    #[serde(default)]
    pub purge_failed: bool,
    /// Drop in allocator `committed` from the last `purge()`. Not the slack predicate.
    #[serde(default)]
    pub malloc_bytes_released_last_purge: u64,
    #[serde(default)]
    pub allocator_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allocator_committed_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allocator_reserved_bytes: Option<u64>,
    #[serde(default)]
    pub in_flight_cold_load_bytes: u64,
    #[serde(default)]
    pub in_flight_cold_load_count: u64,
    #[serde(default)]
    pub peak_request_bytes: u64,
    #[serde(default)]
    pub slowest_request_ms: u64,
    #[serde(default)]
    pub eviction_events: u64,
    #[serde(default)]
    pub effective_warm_budget_bytes: u64,
    /// Collections `warm_budget_bytes` bounds. Empty on an older daemon payload.
    #[serde(default)]
    pub hash_group_warm_bounds: String,
    /// What `resident_graph_budget_bytes` bounds. Empty on an older daemon payload.
    #[serde(default)]
    pub resident_graph_bounds: String,
    /// What `resident.resident_key_count` / `resident_key_budget` bound.
    /// Empty on an older daemon payload.
    #[serde(default)]
    pub logical_key_memory_limit: String,
    /// Store durability barrier counts (see `WarmSetAdmissionStats`).
    #[serde(flatten)]
    pub flush: StoreFlushHealth,
    /// Warm set byte charge and limit from the native core.
    #[serde(default)]
    pub core_warm: CoreWarmBytesHealth,
}

/// Durability barriers the store issued since process start, by kind.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreFlushHealth {
    /// Group data-file barriers.
    #[serde(default)]
    pub fsync_data_calls: u64,
    /// Total microseconds in data-file barriers.
    #[serde(default)]
    pub fsync_data_micros: u64,
    /// Key sidecar barriers (big groups only: a small group is not flushed).
    #[serde(default)]
    pub fsync_sidecar_calls: u64,
    /// Total microseconds in sidecar barriers.
    #[serde(default)]
    pub fsync_sidecar_micros: u64,
    /// Directory barriers.
    #[serde(default)]
    pub fsync_dir_calls: u64,
    /// Scoped flushes walked serially.
    #[serde(default)]
    pub scope_flushes_serial: u64,
    /// Scoped flushes synced with parallel workers.
    #[serde(default)]
    pub scope_flushes_parallel: u64,
    /// Whole-store flushes walked serially.
    #[serde(default)]
    pub whole_flushes_serial: u64,
    /// Whole-store flushes synced with parallel workers.
    #[serde(default)]
    pub whole_flushes_parallel: u64,
    /// Flush barriers since open.
    #[serde(default)]
    pub flush_barriers: u64,
    /// Groups synced by those barriers.
    #[serde(default)]
    pub groups_synced: u64,
}

impl StoreFlushHealth {
    fn from_admission(admission: Option<fold_db::storage::traits::WarmSetAdmissionStats>) -> Self {
        let Some(s) = admission else {
            return Self::default();
        };
        Self {
            fsync_data_calls: s.fsync_data_calls,
            fsync_data_micros: s.fsync_data_micros,
            fsync_sidecar_calls: s.fsync_sidecar_calls,
            fsync_sidecar_micros: s.fsync_sidecar_micros,
            fsync_dir_calls: s.fsync_dir_calls,
            scope_flushes_serial: s.scope_flushes_serial,
            scope_flushes_parallel: s.scope_flushes_parallel,
            whole_flushes_serial: s.whole_flushes_serial,
            whole_flushes_parallel: s.whole_flushes_parallel,
            flush_barriers: s.flush_barriers,
            groups_synced: s.groups_synced,
        }
    }
}

/// Charged warm set bytes next to the process footprint. The ratio of
/// `used_bytes` growth to footprint growth checks the charge estimate.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoreWarmBytesHealth {
    pub limit_bytes: u64,
    pub used_bytes: u64,
    pub peak_bytes: u64,
    pub held_bytes: u64,
    pub dirty_bytes: u64,
    pub dirty_limit_bytes: u64,
    pub tracked_atoms: u64,
    #[serde(default)]
    pub purged_bytes: u64,
    #[serde(default)]
    pub over_limit_admissions: u64,
}

impl CoreWarmBytesHealth {
    /// One `lastdb status` line. `None` from a daemon that predates the gauge.
    pub(super) fn status_line(&self) -> Option<String> {
        if self.limit_bytes == 0 {
            return None;
        }
        Some(format!(
            "Warm set bytes: used={} / limit={} peak={} held={} dirty={} / {} \
             purged={} over_limit_admissions={} atoms={}",
            format_bytes(self.used_bytes),
            format_bytes(self.limit_bytes),
            format_bytes(self.peak_bytes),
            format_bytes(self.held_bytes),
            format_bytes(self.dirty_bytes),
            format_bytes(self.dirty_limit_bytes),
            format_bytes(self.purged_bytes),
            self.over_limit_admissions,
            self.tracked_atoms,
        ))
    }
}

impl From<&fold_db::resident::CoreWarmByteStats> for CoreWarmBytesHealth {
    fn from(stats: &fold_db::resident::CoreWarmByteStats) -> Self {
        Self {
            limit_bytes: stats.limit_bytes as u64,
            used_bytes: stats.used_bytes as u64,
            peak_bytes: stats.peak_bytes as u64,
            held_bytes: stats.held_bytes as u64,
            dirty_bytes: stats.dirty_bytes as u64,
            dirty_limit_bytes: stats.dirty_limit_bytes as u64,
            tracked_atoms: stats.tracked_atoms as u64,
            purged_bytes: stats.purged_bytes,
            over_limit_admissions: stats.over_limit_admissions,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct MemoryBudgetAttribution {
    pub(super) admission: Option<fold_db::storage::traits::WarmSetAdmissionStats>,
    pub(super) allocator: crate::allocator::AllocatorMetrics,
    pub(super) peak_request_bytes: u64,
    pub(super) slowest_request_ms: u64,
}

impl MemoryBudgetHealth {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn measured(
        read_cost: Option<&ReadCostHealth>,
        resident: &ResidentHealth,
        deferred_persist_bytes: u64,
        deferred_persist_cap_bytes: u64,
        persist_lanes: &fold_db::resident::PersistLanePressure,
        phys_footprint_bytes: Option<u64>,
        runtime: Option<fold_db::memory_budget::RuntimeMemoryBudgetObservation>,
        attribution: MemoryBudgetAttribution,
    ) -> Self {
        let budget = fold_db::memory_budget::process_memory_budget();
        let charged_budget_bytes = budget.charged_bytes;
        let governor = crate::ops::footprint::governor_snapshot();
        Self {
            warm_resident_bytes: read_cost
                .and_then(|r| r.warm_resident_bytes.as_wire_u64())
                .unwrap_or(0),
            warm_budget_bytes: read_cost
                .and_then(|r| r.warm_budget_bytes.as_wire_u64())
                .unwrap_or(budget.warm_bytes),
            warm_resident_groups: read_cost
                .and_then(|r| r.warm_resident_groups.as_wire_u64())
                .unwrap_or(0),
            resident_graph_bytes: resident.resident_bytes,
            resident_graph_budget_bytes: resident.budget_bytes,
            deferred_persist_bytes,
            deferred_persist_cap_bytes,
            deferred_lane_fair_share_bytes: persist_lanes.fair_share_bytes,
            deferred_heaviest_lane: persist_lanes.heaviest_schema.clone(),
            deferred_heaviest_lane_bytes: persist_lanes.heaviest_bytes,
            deferred_lane_refuse_bytes: persist_lanes.refuse_bytes,
            deferred_lane_refuse_entries: persist_lanes.refuse_entries,
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
            in_flight_cold_load_bytes: attribution
                .admission
                .map_or(0, |s| s.in_flight_cold_load_bytes),
            in_flight_cold_load_count: attribution
                .admission
                .map_or(0, |s| s.in_flight_cold_load_count),
            peak_request_bytes: attribution.peak_request_bytes,
            slowest_request_ms: attribution.slowest_request_ms,
            eviction_events: attribution.admission.map_or(0, |s| s.eviction_events),
            effective_warm_budget_bytes: attribution
                .admission
                .map_or(budget.warm_bytes, |s| s.effective_warm_bytes),
            hash_group_warm_bounds: HASH_GROUP_WARM_BOUNDS.to_string(),
            resident_graph_bounds: RESIDENT_GRAPH_BOUNDS.to_string(),
            logical_key_memory_limit: logical_key_memory_limit_for(
                resident.resident_key_budget.as_wire_u64(),
            ),
            flush: StoreFlushHealth::from_admission(attribution.admission),
            core_warm: CoreWarmBytesHealth::default(),
        }
    }
}
