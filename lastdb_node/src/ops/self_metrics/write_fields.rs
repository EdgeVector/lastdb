//! Flat metric-field builders for one self-metrics snapshot record.

use super::*;

/// Process, memory budget and disk footprint fields.
pub(super) fn insert_process_and_memory_fields(
    fields: &mut HashMap<String, Value>,
    snapshot: &StatusSnapshot,
) {
    fields.insert("sampled_at".to_string(), json!(snapshot.sampled_at));
    fields.insert(
        "process_start_ts".to_string(),
        json!(snapshot.process_start_ts),
    );
    fields.insert("uptime_secs".to_string(), json!(snapshot.uptime_secs));
    fields.insert("rss_bytes".to_string(), json!(snapshot.rss_bytes));
    fields.insert(
        "phys_footprint_bytes".to_string(),
        json!(snapshot.phys_footprint_bytes),
    );
    fields.insert(
        "phys_footprint_peak_bytes".to_string(),
        json!(snapshot.phys_footprint_peak_bytes),
    );
    fields.insert(
        "memory_limit_bytes".to_string(),
        json!(snapshot.memory_limit_bytes),
    );
    fields.insert(
        "malloc_bytes_in_use".to_string(),
        json!(snapshot.memory_budget.malloc_bytes_in_use),
    );
    fields.insert(
        "malloc_bytes_held_free".to_string(),
        json!(snapshot.memory_budget.malloc_bytes_held_free),
    );
    fields.insert(
        "footprint_net_bytes".to_string(),
        json!(snapshot.memory_budget.footprint_net_bytes),
    );
    fields.insert(
        "footprint_sticky".to_string(),
        json!(snapshot.memory_budget.footprint_sticky),
    );
    fields.insert(
        "footprint_delta_per_evicted_byte".to_string(),
        json!(snapshot.memory_budget.footprint_delta_per_evicted_byte),
    );
    fields.insert(
        "governor_state".to_string(),
        json!(snapshot.memory_budget.governor_state),
    );
    fields.insert(
        "governor_state_held_secs".to_string(),
        json!(snapshot.memory_budget.governor_state_held_secs),
    );
    fields.insert(
        "host_pressure".to_string(),
        json!(snapshot.memory_budget.host_pressure),
    );
}

/// Host pressure, allocator, request peaks and disk footprint fields.
pub(super) fn insert_allocator_and_disk_fields(
    fields: &mut HashMap<String, Value>,
    snapshot: &StatusSnapshot,
) {
    fields.insert(
        "swap_used_bytes".to_string(),
        json!(snapshot.memory_budget.swap_used_bytes),
    );
    fields.insert(
        "compressor_bytes".to_string(),
        json!(snapshot.memory_budget.compressor_bytes),
    );
    fields.insert(
        "purge_failed".to_string(),
        json!(snapshot.memory_budget.purge_failed),
    );
    fields.insert(
        "malloc_bytes_released_last_purge".to_string(),
        json!(snapshot.memory_budget.malloc_bytes_released_last_purge),
    );
    fields.insert(
        "allocator_name".to_string(),
        json!(snapshot.memory_budget.allocator_name),
    );
    fields.insert(
        "allocator_committed_bytes".to_string(),
        json!(snapshot.memory_budget.allocator_committed_bytes),
    );
    fields.insert(
        "allocator_reserved_bytes".to_string(),
        json!(snapshot.memory_budget.allocator_reserved_bytes),
    );
    fields.insert(
        "peak_request_bytes".to_string(),
        json!(snapshot.memory_budget.peak_request_bytes),
    );
    fields.insert(
        "slowest_request_ms".to_string(),
        json!(snapshot.memory_budget.slowest_request_ms),
    );
    fields.insert(
        "effective_warm_budget_bytes".to_string(),
        json!(snapshot.memory_budget.effective_warm_budget_bytes),
    );
    fields.insert("cpu_percent".to_string(), json!(snapshot.cpu_percent));
    fields.insert("home_bytes".to_string(), json!(snapshot.home_bytes));
    fields.insert("data_dir_bytes".to_string(), json!(snapshot.data_dir_bytes));
}

/// Backup durability fields.
pub(super) fn insert_backup_durability_fields(
    fields: &mut HashMap<String, Value>,
    snapshot: &StatusSnapshot,
) {
    fields.insert(
        "backup_v2_observed".into(),
        json!(snapshot.durability.observed),
    );
    fields.insert(
        "backup_v2_accepted_backup_recorded".into(),
        json!(snapshot.durability.accepted_backup_recorded),
    );
    fields.insert(
        "backup_automatic_enabled".into(),
        json!(snapshot.durability.automatic_enabled),
    );
    fields.insert(
        "backup_durability_degraded".to_string(),
        json!(snapshot.durability.degraded),
    );
    fields.insert(
        "backup_durability_reasons".to_string(),
        json!(snapshot.durability.reasons),
    );
}

/// Sampler, QoS and UDS fields.
pub(super) fn insert_sampler_qos_uds_fields(
    fields: &mut HashMap<String, Value>,
    snapshot: &StatusSnapshot,
) {
    fields.insert(
        "sampler_last_error".to_string(),
        json!(snapshot.sampler.last_error),
    );
    fields.insert(
        "qos_total_permits".to_string(),
        json!(wire_u64(&snapshot.qos.total_permits)),
    );
    fields.insert(
        "qos_bulk_permits".to_string(),
        json!(wire_u64(&snapshot.qos.bulk_permits)),
    );
    fields.insert(
        "qos_total_in_use".to_string(),
        json!(wire_u64(&snapshot.qos.total_in_use)),
    );
    fields.insert(
        "qos_bulk_in_use".to_string(),
        json!(wire_u64(&snapshot.qos.bulk_in_use)),
    );
    fields.insert(
        "qos_interactive_sheds".to_string(),
        json!(wire_u64(&snapshot.qos.interactive_sheds)),
    );
    fields.insert(
        "qos_bulk_sheds".to_string(),
        json!(wire_u64(&snapshot.qos.bulk_sheds)),
    );
    fields.insert(
        "uds_workers".to_string(),
        json!(wire_u64(&snapshot.uds.workers)),
    );
    fields.insert(
        "uds_queue_capacity".to_string(),
        json!(wire_u64(&snapshot.uds.queue_capacity)),
    );
    fields.insert(
        "uds_in_flight".to_string(),
        json!(wire_u64(&snapshot.uds.in_flight)),
    );
    fields.insert(
        "uds_submitted".to_string(),
        json!(wire_u64(&snapshot.uds.submitted)),
    );
    fields.insert(
        "uds_queue_full_rejects".to_string(),
        json!(wire_u64(&snapshot.uds.queue_full_rejects)),
    );
}
