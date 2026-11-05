pub(crate) const SELF_METRIC_FIELDS: &[&str] = &[
    "series",
    "sample_id",
    "sampled_at",
    "process_start_ts",
    "uptime_secs",
    "rss_bytes",
    "phys_footprint_bytes",
    "phys_footprint_peak_bytes",
    "memory_limit_bytes",
    "cpu_percent",
    "home_bytes",
    "data_dir_bytes",
    "backup_v2_observed",
    "backup_v2_accepted_backup_recorded",
    "backup_automatic_enabled",
    "backup_durability_degraded",
    "backup_durability_reasons",
    "sampler_last_error",
    "qos_total_permits",
    "qos_bulk_permits",
    "qos_total_in_use",
    "qos_bulk_in_use",
    "qos_interactive_sheds",
    "qos_bulk_sheds",
    "uds_workers",
    "uds_queue_capacity",
    "uds_in_flight",
    "uds_submitted",
    "uds_queue_full_rejects",
    "malloc_bytes_in_use",
    "malloc_bytes_held_free",
    "footprint_net_bytes",
    "footprint_sticky",
    "footprint_delta_per_evicted_byte",
    "governor_state",
    "governor_state_held_secs",
    "host_pressure",
    "swap_used_bytes",
    "compressor_bytes",
    "purge_failed",
    "malloc_bytes_released_last_purge",
    "allocator_name",
    "allocator_committed_bytes",
    "allocator_reserved_bytes",
    "peak_request_bytes",
    "slowest_request_ms",
];
/// Every field of one `RequestOpsRollup` record. The writer stores all of
/// them on every record, so a missing field is a defect, never a zero.
pub const REQUEST_OPS_ROLLUP_FIELDS: &[&str] = &[
    "series",
    "sample_id",
    "sampled_at_ms",
    "process_start_ts",
    "client",
    "kind",
    // "" when the op names no schema.
    "schema",
    // Record identity for a schema-less key; "" when the schema already
    // identifies the record.
    "route",
    "count",
    "sum_ms",
    "max_ms",
    "last_ts_ms",
    "error_count",
    // Interval SUM of request body bytes. Answers who sent the bytes after a
    // restart.
    "sum_body_bytes",
    // Process max of request body on the key (same semantics as `max_ms`:
    // not delta'd; persisted as the current max for the interval snapshot).
    "max_body_bytes",
    // One `sum_<phase>_us` interval SUM per `PhaseTimings::PHASE_NAMES` entry
    // (microseconds; sums stay honest under the delta writer, maxes would
    // not), plus `phase_count`: how many requests in the interval reported a
    // phase set at all (the denominator for per-request phase averages).
    "sum_queue_wait_us",
    "sum_admission_wait_us",
    "sum_parse_us",
    "sum_schema_resolve_us",
    "sum_validate_us",
    "sum_cas_precondition_us",
    "sum_count_us",
    "sum_hydrate_us",
    "sum_hydrate_records_us",
    "sum_hydrate_format_us",
    "sum_hydrate_sort_us",
    "sum_apply_us",
    "sum_grouping_us",
    "sum_index_wait_us",
    "sum_change_record_us",
    "sum_response_envelope_us",
    "sum_status_durability_us",
    "sum_status_data_dir_us",
    "sum_status_request_ops_us",
    "phase_count",
    // The wall clock matching `phase_count`'s population, so a rollup window
    // can compute the same unattributed residual the live snapshot renders
    // instead of subtracting `sum_ms` (which spans every request, phased or
    // not).
    "phased_sum_ms",
    // Work COUNTS for the resident commit path. A wall-clock phase moves with
    // node state, so only a durable count series can attribute a write-shape
    // change to a release. `sum_resident_operations / sum_resident_commits`
    // is 1 for the serial per-projection path and rises as batches carry
    // whole logical writes.
    "sum_resident_commits",
    "sum_resident_operations",
];
