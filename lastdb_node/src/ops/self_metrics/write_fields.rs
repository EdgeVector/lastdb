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
        "in_flight_cold_load_bytes".to_string(),
        json!(snapshot.memory_budget.in_flight_cold_load_bytes),
    );
    fields.insert(
        "in_flight_cold_load_count".to_string(),
        json!(snapshot.memory_budget.in_flight_cold_load_count),
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
        "eviction_events".to_string(),
        json!(snapshot.memory_budget.eviction_events),
    );
    fields.insert(
        "effective_warm_budget_bytes".to_string(),
        json!(snapshot.memory_budget.effective_warm_budget_bytes),
    );
    fields.insert("cpu_percent".to_string(), json!(snapshot.cpu_percent));
    fields.insert("home_bytes".to_string(), json!(snapshot.home_bytes));
    fields.insert("data_dir_bytes".to_string(), json!(snapshot.data_dir_bytes));
}

/// Cloud sync status fields.
pub(super) fn insert_sync_status_fields(
    fields: &mut HashMap<String, Value>,
    snapshot: &StatusSnapshot,
) {
    fields.insert("sync_enabled".to_string(), json!(snapshot.sync.enabled));
    fields.insert("sync_state".to_string(), json!(snapshot.sync.state));
    fields.insert(
        "sync_last_success_ts".to_string(),
        json!(wire_u64(&snapshot.sync.last_success_ts)),
    );
    fields.insert(
        "sync_local_writable".to_string(),
        json!(snapshot.sync.local_writable),
    );
    fields.insert(
        "sync_degraded".to_string(),
        json!(snapshot.sync.sync_degraded),
    );
    fields.insert(
        "sync_last_error_at".to_string(),
        json!(wire_u64(&snapshot.sync.last_error_at)),
    );
    fields.insert(
        "sync_consecutive_failures".to_string(),
        json!(wire_u64(&snapshot.sync.consecutive_sync_failures)),
    );
    fields.insert(
        "sync_failing_since".to_string(),
        json!(wire_u64(&snapshot.sync.failing_since)),
    );
    fields.insert(
        "sync_degraded_reasons".to_string(),
        json!(snapshot.sync.degraded_reasons),
    );
}

/// Backup durability fields.
pub(super) fn insert_backup_durability_fields(
    fields: &mut HashMap<String, Value>,
    snapshot: &StatusSnapshot,
) {
    // Durability is emitted unconditionally so a time-series alarm can be
    // written against backup *age*. Every sync_* field above goes null when the
    // uploader is off, which is precisely when nobody notices.
    fields.insert(
        "backup_age_secs".to_string(),
        json!(wire_u64(&snapshot.durability.backup_age_secs)),
    );
    fields.insert(
        "backup_last_commit_ts".to_string(),
        json!(wire_u64(&snapshot.durability.last_backup_commit_ts)),
    );
    fields.insert(
        "backup_manifest_counter".to_string(),
        json!(wire_u64(&snapshot.durability.backup_manifest_counter)),
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

/// Sync queue, mutation-log and last-error fields.
pub(super) fn insert_sync_queue_and_log_fields(
    fields: &mut HashMap<String, Value>,
    snapshot: &StatusSnapshot,
) {
    fields.insert(
        "sync_staging_count".to_string(),
        json!(wire_u64(&snapshot.sync.durable_outbox_count)),
    );
    fields.insert(
        "sync_staging_max".to_string(),
        json!(wire_u64(&snapshot.sync.durable_outbox_max)),
    );
    fields.insert(
        "sync_pending_count".to_string(),
        json!(wire_u64(&snapshot.sync.pending_count)),
    );
    fields.insert(
        "sync_durable_outbox_count".to_string(),
        json!(wire_u64(&snapshot.sync.durable_outbox_count)),
    );
    fields.insert(
        "sync_durable_outbox_max".to_string(),
        json!(wire_u64(&snapshot.sync.durable_outbox_max)),
    );
    fields.insert(
        "sync_upload_queue_count".to_string(),
        json!(wire_u64(&snapshot.sync.upload_queue_count)),
    );
    fields.insert(
        "sync_upload_queue_max".to_string(),
        json!(wire_u64(&snapshot.sync.upload_queue_max)),
    );
    fields.insert(
        "sync_recording_local_changes".to_string(),
        json!(snapshot.sync.recording_local_changes),
    );
    fields.insert(
        "sync_off_grace_expired".to_string(),
        json!(snapshot.sync.sync_off_grace_expired),
    );
    fields.insert(
        "sync_reenable_strategy".to_string(),
        json!(snapshot.sync.reenable_strategy),
    );
    fields.insert(
        "sync_cloud_disabled_at".to_string(),
        json!(wire_u64(&snapshot.sync.cloud_sync_disabled_at)),
    );
    fields.insert(
        "sync_off_grace_secs".to_string(),
        json!(wire_u64(&snapshot.sync.sync_off_grace_secs)),
    );
}

/// Sync mutation-log and last-error fields.
pub(super) fn insert_sync_mutation_log_fields(
    fields: &mut HashMap<String, Value>,
    snapshot: &StatusSnapshot,
) {
    fields.insert(
        "sync_mutation_log_active".to_string(),
        json!(snapshot.sync.mutation_log_active),
    );
    fields.insert(
        "sync_mutation_log_writer_id".to_string(),
        json!(snapshot.sync.mutation_log_writer_id),
    );
    fields.insert(
        "sync_mutation_log_frontier_f".to_string(),
        json!(wire_u64(&snapshot.sync.mutation_log_frontier_f)),
    );
    fields.insert(
        "sync_mutation_log_published_through".to_string(),
        json!(wire_u64(&snapshot.sync.mutation_log_published_through)),
    );
    fields.insert(
        "sync_mutation_log_last_durable_frontier".to_string(),
        json!(wire_u64(&snapshot.sync.mutation_log_last_durable_frontier)),
    );
    fields.insert(
        "sync_mutation_log_recovery_point_age_secs".to_string(),
        json!(wire_u64(
            &snapshot.sync.mutation_log_recovery_point_age_secs
        )),
    );
    fields.insert(
        "sync_mutation_log_lag".to_string(),
        json!(wire_u64(&snapshot.sync.mutation_log_lag)),
    );
    fields.insert(
        "sync_mutation_log_segments_uploaded".to_string(),
        json!(wire_u64(&snapshot.sync.mutation_log_segments_uploaded)),
    );
    fields.insert(
        "sync_mutation_log_lag_degraded".to_string(),
        json!(snapshot.sync.mutation_log_lag_degraded),
    );
    fields.insert(
        "sync_mutation_log_capture_registered".to_string(),
        json!(snapshot.sync.mutation_log_capture_registered),
    );
    fields.insert(
        "sync_capture_reexport_pending_count_estimate".to_string(),
        json!(wire_u64(
            &snapshot.sync.capture_reexport_pending_count_estimate
        )),
    );
    fields.insert(
        "sync_capture_reexport_pending_known_nonempty".to_string(),
        json!(snapshot.sync.capture_reexport_pending_known_nonempty),
    );
    fields.insert(
        "sync_last_error".to_string(),
        json!(snapshot.sync.last_error),
    );
}

/// Last sync download fields.
pub(super) fn insert_sync_download_fields(
    fields: &mut HashMap<String, Value>,
    snapshot: &StatusSnapshot,
) {
    let (dl_bytes, dl_entries, dl_deferred, dl_health_deferred) = match &snapshot.sync.last_download
    {
        Some(v) => (
            v.get("bytes_downloaded").cloned().unwrap_or(json!(null)),
            v.get("entries_replayed").cloned().unwrap_or(json!(null)),
            v.get("entries_deferred").cloned().unwrap_or(json!(null)),
            v.get("entries_health_deferred")
                .cloned()
                .unwrap_or_else(|| v.get("entries_deferred").cloned().unwrap_or(json!(null))),
        ),
        None => (json!(null), json!(null), json!(null), json!(null)),
    };
    fields.insert("sync_download_bytes_last".to_string(), dl_bytes);
    fields.insert("sync_download_entries_last".to_string(), dl_entries);
    fields.insert("sync_download_deferred_last".to_string(), dl_deferred);
    fields.insert(
        "sync_download_health_deferred_last".to_string(),
        dl_health_deferred,
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
