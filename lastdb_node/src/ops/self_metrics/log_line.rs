use super::*;

/// Compact one-line vitals for the JSONL log (no request_ops ring — keep small).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct SelfMetricLogLine {
    pub(super) sampled_at: u64,
    pub(super) process_start_ts: u64,
    pub(super) uptime_secs: u64,
    pub(super) rss_bytes: Option<u64>,
    pub(super) phys_footprint_bytes: Option<u64>,
    pub(super) phys_footprint_peak_bytes: Option<u64>,
    pub(super) memory_limit_bytes: Option<u64>,
    pub(super) warm_resident_bytes: u64,
    pub(super) warm_budget_bytes: u64,
    pub(super) warm_resident_groups: u64,
    pub(super) resident_graph_bytes: u64,
    pub(super) resident_graph_budget_bytes: u64,
    pub(super) deferred_persist_bytes: u64,
    pub(super) deferred_persist_cap_bytes: u64,
    pub(super) charged_budget_bytes: u64,
    pub(super) projected_total_bytes: u64,
    pub(super) projection_tolerance_bytes: u64,
    pub(super) projection_diverged: bool,
    pub(super) footprint_over_limit: bool,
    pub(super) runtime_degraded: bool,
    pub(super) implied_footprint_multiplier: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) malloc_bytes_in_use: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) malloc_bytes_held_free: Option<u64>,
    #[serde(default)]
    pub(super) footprint_net_bytes: u64,
    #[serde(default)]
    pub(super) footprint_sticky: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) footprint_delta_per_evicted_byte: Option<f64>,
    #[serde(default)]
    pub(super) governor_state: String,
    #[serde(default)]
    pub(super) governor_state_held_secs: u64,
    #[serde(default)]
    pub(super) host_pressure: String,
    #[serde(default)]
    pub(super) swap_used_bytes: u64,
    #[serde(default)]
    pub(super) compressor_bytes: u64,
    #[serde(default)]
    pub(super) purge_failed: bool,
    #[serde(default)]
    pub(super) malloc_bytes_released_last_purge: u64,
    #[serde(default)]
    pub(super) allocator_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) allocator_committed_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) allocator_reserved_bytes: Option<u64>,
    #[serde(default)]
    pub(super) in_flight_cold_load_bytes: u64,
    #[serde(default)]
    pub(super) in_flight_cold_load_count: u64,
    #[serde(default)]
    pub(super) peak_request_bytes: u64,
    #[serde(default)]
    pub(super) slowest_request_ms: u64,
    #[serde(default)]
    pub(super) eviction_events: u64,
    #[serde(default)]
    pub(super) effective_warm_budget_bytes: u64,
    pub(super) cpu_percent: Option<f64>,
    pub(super) home_bytes: Option<u64>,
    pub(super) home_size_walks: u64,
    pub(super) data_dir_bytes: Option<u64>,
    pub(super) data_dir_size_walks: u64,
    pub(super) sync_enabled: bool,
    pub(super) sync_state: Option<String>,
    pub(super) sync_degraded: Option<bool>,
    pub(super) sync_local_writable: Option<bool>,
    pub(super) sync_last_success_ts: Option<u64>,
    pub(super) sync_pending_count: Option<usize>,
    pub(super) sync_durable_outbox_count: Option<usize>,
    pub(super) sync_upload_queue_count: Option<usize>,
    pub(super) sync_last_error: Option<String>,
    pub(super) uds_workers: usize,
    pub(super) uds_queue_capacity: usize,
    pub(super) uds_in_flight: usize,
    pub(super) uds_queue_full_rejects: u64,
    pub(super) qos_total_in_use: usize,
    pub(super) qos_interactive_sheds: u64,
    pub(super) qos_bulk_sheds: u64,
}

impl From<&StatusSnapshot> for SelfMetricLogLine {
    fn from(s: &StatusSnapshot) -> Self {
        Self {
            sampled_at: s.sampled_at,
            process_start_ts: s.process_start_ts,
            uptime_secs: s.uptime_secs,
            rss_bytes: s.rss_bytes,
            phys_footprint_bytes: s.phys_footprint_bytes,
            phys_footprint_peak_bytes: s.phys_footprint_peak_bytes,
            memory_limit_bytes: s.memory_limit_bytes,
            warm_resident_bytes: s.memory_budget.warm_resident_bytes,
            warm_budget_bytes: s.memory_budget.warm_budget_bytes,
            warm_resident_groups: s.memory_budget.warm_resident_groups,
            resident_graph_bytes: s.memory_budget.resident_graph_bytes,
            resident_graph_budget_bytes: s.memory_budget.resident_graph_budget_bytes,
            deferred_persist_bytes: s.memory_budget.deferred_persist_bytes,
            deferred_persist_cap_bytes: s.memory_budget.deferred_persist_cap_bytes,
            charged_budget_bytes: s.memory_budget.charged_budget_bytes,
            projected_total_bytes: s.memory_budget.projected_total_bytes,
            projection_tolerance_bytes: s.memory_budget.projection_tolerance_bytes,
            projection_diverged: s.memory_budget.projection_diverged,
            footprint_over_limit: s.memory_budget.footprint_over_limit,
            runtime_degraded: s.memory_budget.runtime_degraded,
            implied_footprint_multiplier: s.memory_budget.implied_footprint_multiplier,
            malloc_bytes_in_use: s.memory_budget.malloc_bytes_in_use,
            malloc_bytes_held_free: s.memory_budget.malloc_bytes_held_free,
            footprint_net_bytes: s.memory_budget.footprint_net_bytes,
            footprint_sticky: s.memory_budget.footprint_sticky,
            footprint_delta_per_evicted_byte: s.memory_budget.footprint_delta_per_evicted_byte,
            governor_state: s.memory_budget.governor_state.clone(),
            governor_state_held_secs: s.memory_budget.governor_state_held_secs,
            host_pressure: s.memory_budget.host_pressure.clone(),
            swap_used_bytes: s.memory_budget.swap_used_bytes,
            compressor_bytes: s.memory_budget.compressor_bytes,
            purge_failed: s.memory_budget.purge_failed,
            malloc_bytes_released_last_purge: s.memory_budget.malloc_bytes_released_last_purge,
            allocator_name: s.memory_budget.allocator_name.clone(),
            allocator_committed_bytes: s.memory_budget.allocator_committed_bytes,
            allocator_reserved_bytes: s.memory_budget.allocator_reserved_bytes,
            in_flight_cold_load_bytes: s.memory_budget.in_flight_cold_load_bytes,
            in_flight_cold_load_count: s.memory_budget.in_flight_cold_load_count,
            peak_request_bytes: s.memory_budget.peak_request_bytes,
            slowest_request_ms: s.memory_budget.slowest_request_ms,
            eviction_events: s.memory_budget.eviction_events,
            effective_warm_budget_bytes: s.memory_budget.effective_warm_budget_bytes,
            cpu_percent: s.cpu_percent,
            home_bytes: s.home_bytes,
            home_size_walks: s.home_size_walks,
            data_dir_bytes: s.data_dir_bytes,
            data_dir_size_walks: s.data_dir_size_walks,
            sync_enabled: s.sync.enabled,
            sync_state: s.sync.state.clone(),
            sync_degraded: s.sync.sync_degraded,
            sync_local_writable: s.sync.local_writable,
            sync_last_success_ts: wire_u64(&s.sync.last_success_ts),
            sync_pending_count: wire_u64(&s.sync.pending_count)
                .map(|n| usize::try_from(n).unwrap_or(usize::MAX)),
            sync_durable_outbox_count: wire_u64(&s.sync.durable_outbox_count)
                .map(|n| usize::try_from(n).unwrap_or(usize::MAX)),
            sync_upload_queue_count: wire_u64(&s.sync.upload_queue_count)
                .map(|n| usize::try_from(n).unwrap_or(usize::MAX)),
            sync_last_error: s.sync.last_error.clone(),
            uds_workers: gauge_or_zero_usize(&s.uds.workers),
            uds_queue_capacity: gauge_or_zero_usize(&s.uds.queue_capacity),
            uds_in_flight: gauge_or_zero_usize(&s.uds.in_flight),
            uds_queue_full_rejects: gauge_or_zero(&s.uds.queue_full_rejects),
            qos_total_in_use: gauge_or_zero_usize(&s.qos.total_in_use),
            qos_interactive_sheds: gauge_or_zero(&s.qos.interactive_sheds),
            qos_bulk_sheds: gauge_or_zero(&s.qos.bulk_sheds),
        }
    }
}

/// Append one vitals line to the self-metrics JSONL log. Creates parent dirs.
/// Rotates `path` → `path.1` when over size cap (keeps one previous file).
pub fn append_self_metrics_log_line(path: &Path, snapshot: &StatusSnapshot) -> Result<(), String> {
    use std::io::Write;

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("mkdir self-metrics log dir {}: {e}", parent.display()))?;
    }

    if let Ok(meta) = std::fs::metadata(path) {
        if meta.len() >= self_metrics_log_max_bytes() {
            let rotated = path.with_extension("jsonl.1");
            let _ = std::fs::remove_file(&rotated);
            std::fs::rename(path, &rotated).map_err(|e| {
                format!(
                    "rotate self-metrics log {} → {}: {e}",
                    path.display(),
                    rotated.display()
                )
            })?;
        }
    }

    let line = SelfMetricLogLine::from(snapshot);
    let mut json = serde_json::to_string(&line)
        .map_err(|e| format!("serialize self-metrics log line: {e}"))?;
    json.push('\n');

    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| format!("open self-metrics log {}: {e}", path.display()))?;
    file.write_all(json.as_bytes())
        .map_err(|e| format!("write self-metrics log {}: {e}", path.display()))?;
    Ok(())
}
