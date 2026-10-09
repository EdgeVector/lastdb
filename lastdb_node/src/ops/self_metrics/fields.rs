pub(super) const SELF_METRIC_FIELDS: &[&str] = &[
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
    "sync_enabled",
    "sync_state",
    "sync_last_success_ts",
    "sync_local_writable",
    "sync_degraded",
    "sync_staging_count",
    "sync_staging_max",
    "sync_pending_count",
    "sync_durable_outbox_count",
    "sync_durable_outbox_max",
    "sync_upload_queue_count",
    "sync_upload_queue_max",
    "sync_last_error",
    "sync_last_error_at",
    "sync_consecutive_failures",
    "sync_failing_since",
    "sync_degraded_reasons",
    "sync_download_bytes_last",
    "sync_download_entries_last",
    "sync_download_deferred_last",
    "sync_download_health_deferred_last",
    "sync_recording_local_changes",
    "sync_off_grace_expired",
    "sync_reenable_strategy",
    "sync_cloud_disabled_at",
    "sync_off_grace_secs",
    "sync_mutation_log_active",
    "sync_mutation_log_writer_id",
    "sync_mutation_log_frontier_f",
    "sync_mutation_log_published_through",
    "sync_mutation_log_last_durable_frontier",
    "sync_mutation_log_recovery_point_age_secs",
    "sync_mutation_log_lag",
    "sync_mutation_log_segments_uploaded",
    "sync_mutation_log_lag_degraded",
    "sync_mutation_log_capture_registered",
    "backup_age_secs",
    "backup_last_commit_ts",
    "backup_manifest_counter",
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
    "in_flight_cold_load_bytes",
    "in_flight_cold_load_count",
    "peak_request_bytes",
    "slowest_request_ms",
    "eviction_events",
    "effective_warm_budget_bytes",
];
pub(super) const REQUEST_OPS_ROLLUP_FIELDS: &[&str] = &[
    "series",
    "sample_id",
    "sampled_at_ms",
    "process_start_ts",
    "client",
    "kind",
    "schema",
    // Row identity for a schema-less key. Added after the schema shipped;
    // `ensure_request_ops_rollup_schema` sees it as a missing field and
    // upgrades in place. Rows written before this field read back as absent,
    // which is correct — those rows genuinely carry no route attribution, and
    // `lastdb ops --since` renders them exactly as it did before.
    "route",
    "count",
    "sum_ms",
    "max_ms",
    "last_ts_ms",
    "error_count",
    // Added after the schema shipped; `ensure_request_ops_rollup_schema` sees
    // it as a missing field and upgrades in place. Rows written before this
    // read back as 0 via the reader's `unwrap_or(0)`, which is correct — those
    // samples genuinely carry no load attribution.
    "sum_cold_shard_loads",
    // Write-volume attribution — same additive-migration path as
    // `sum_cold_shard_loads`. Interval SUM of request body bytes so
    // `lastdb ops --since` can answer "who stuffed the node" after restarts.
    // Rows written before this field read back as 0.
    "sum_body_bytes",
    // Process max of request body on the key (same semantics as `max_ms`:
    // not delta'd; persisted as the current max for the interval snapshot).
    "max_body_bytes",
    // Mutation phase observability — same additive-migration path as
    // `sum_cold_shard_loads` above. One `sum_<phase>_us` interval SUM per
    // `PhaseTimings::PHASE_NAMES` entry (microseconds; sums stay honest under
    // the delta writer, maxes would not), plus `phase_count`: how many
    // requests in the interval reported a phase set at all (the denominator
    // for per-request phase averages). Rows written before these fields read
    // back as 0 — genuinely "no phases reported", not measured-as-zero.
    // `rollup_fields_cover_every_phase_name` pins this list to PHASE_NAMES.
    "sum_queue_wait_us",
    "sum_admission_wait_us",
    "sum_parse_us",
    "sum_schema_resolve_us",
    "sum_validate_us",
    "sum_lock_wait_us",
    "sum_purge_barrier_us",
    "sum_purge_plan_us",
    "sum_purge_commit_us",
    "sum_purge_materialize_us",
    "sum_purge_trace_us",
    "sum_purge_retention_guard_us",
    "sum_purge_delete_us",
    "sum_purge_finalize_us",
    "sum_molecule_gate_us",
    "sum_cas_precondition_us",
    "sum_count_us",
    "sum_hydrate_us",
    "sum_hydrate_atoms_us",
    "sum_hydrate_format_us",
    "sum_hydrate_sort_us",
    "sum_hydrate_filter_us",
    "sum_annotate_us",
    "sum_apply_us",
    "sum_apply_memory_us",
    "sum_schema_load_us",
    "sum_dedupe_scan_us",
    "sum_idempotency_check_us",
    "sum_grouping_us",
    "sum_restore_molecules_us",
    "sum_protein_sibling_fold_us",
    "sum_spawn_indexing_us",
    "sum_sync_uuids_us",
    "sum_schema_reload_us",
    "sum_persist_us",
    "sum_persist_molecules_us",
    "sum_persist_schema_us",
    "sum_persist_idempotency_us",
    "sum_flush_us",
    "sum_sync_capture_us",
    "sum_index_wait_us",
    "sum_change_record_us",
    "sum_change_record_lock_wait_us",
    "sum_change_record_write_us",
    "sum_response_envelope_us",
    "sum_status_sync_us",
    "sum_status_backup_us",
    "sum_status_durability_us",
    "sum_status_data_dir_us",
    "sum_status_request_ops_us",
    "phase_count",
    // The wall clock matching `phase_count`'s population, so `--since` can
    // compute the same unattributed residual the live snapshot renders
    // instead of subtracting `sum_ms` (which spans every request, phased or
    // not). Additive migration like the fields above; rows written before it
    // read back as 0, which suppresses the residual rather than inventing a
    // wrong one.
    "phased_sum_ms",
    // Work COUNTS for the molecule persist path — the denominator
    // `sum_persist_molecules_us` never had. Interval sums, additive migration
    // like every field above; rows written before them read back as 0, which
    // renders no ratio rather than a wrong one.
    //
    // These are why the pair is worth persisting rather than reading live: a
    // wall-clock phase moves with node state (measured 3.4x on the primary in
    // two hours with the code path unchanged), so only a durable count series
    // can attribute a write-path change to a release.
    "sum_molecules_persisted",
    "sum_molecule_store_commits",
    // Work COUNTS for the resident commit path, persisted for the same reason
    // as the molecule pair: the ratio between them is the shape of the write,
    // and only a durable count series can attribute a shape change to a
    // release. `sum_resident_operations / sum_resident_commits` is 1 for the
    // serial per-projection path and rises as batches carry whole logical
    // writes.
    "sum_resident_commits",
    "sum_resident_operations",
];
