use super::*;

/// What `LASTDB_HASH_GROUP_WARM_BYTES` / `warm_budget_bytes` bounds.
///
/// Tips and atoms take the unpublished loader. This byte budget bounds the
/// hash-group warm set for every other collection.
pub const HASH_GROUP_WARM_BOUNDS: &str = "hash-group warm set for non-logical collections \
    (indexes, schema_index, atom_ref_edges_v2, keep_small, metadata, cas_blobs)";

/// What `LASTDB_RESIDENT_BYTES` / `resident.budget_bytes` bounds.
pub const RESIDENT_GRAPH_BOUNDS: &str = "ResidentGraph (LASTDB_RESIDENT_MODE=write)";

/// What `resident_key_count` / `resident_key_budget` bound.
///
/// The in-force used-record cap is `resident_key_budget`. Name
/// `RESIDENT_KEY_CAP=10000` only when that budget is the default;
/// `LASTDB_RESIDENT_KEY_CAP` can lower it.
pub const LOGICAL_KEY_MEMORY_LIMIT: &str = "memory limit for logical keys (tips and atoms)";

pub(super) fn logical_key_memory_limit_for(budget: Option<u64>) -> String {
    match budget {
        Some(n) if n == fold_db::resident::RESIDENT_KEY_CAP as u64 => {
            format!("{LOGICAL_KEY_MEMORY_LIMIT}; RESIDENT_KEY_CAP={n}")
        }
        _ => LOGICAL_KEY_MEMORY_LIMIT.to_string(),
    }
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
    pub deferred_write_through_bytes: u64,
    #[serde(default)]
    pub deferred_heaviest_lane: String,
    #[serde(default)]
    pub deferred_heaviest_lane_bytes: u64,
    #[serde(default)]
    pub deferred_lane_refuse_bytes: u64,
    #[serde(default)]
    pub deferred_lane_refuse_entries: u64,
    #[serde(default)]
    pub deferred_write_throughs: u64,
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
            deferred_write_through_bytes: persist_lanes.write_through_threshold_bytes,
            deferred_heaviest_lane: persist_lanes.heaviest_schema.clone(),
            deferred_heaviest_lane_bytes: persist_lanes.heaviest_bytes,
            deferred_lane_refuse_bytes: persist_lanes.refuse_bytes,
            deferred_lane_refuse_entries: persist_lanes.refuse_entries,
            deferred_write_throughs: persist_lanes.write_throughs,
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
        }
    }
}
