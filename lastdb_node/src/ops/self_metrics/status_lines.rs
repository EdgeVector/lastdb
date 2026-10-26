use super::*;

/// The one memory line, built to answer both questions an operator actually
/// has: *how much memory is this node using* and *how close is it to being
/// restarted*. Those have different answers and, on macOS, different units.
///
/// The footprint leads because it is the true number. RSS stays visible next to
/// it because the gap between them **is** the diagnostic — it is the compressed
/// and swapped working set. Reporting one number and calling it "memory" is
/// what let three operator runs read 1.39 GiB on a node holding 9.5 GB.
///
/// The ceiling clause attaches to whichever gauge
/// [`StatusSnapshot::memory_guard_metric`] says the guard samples, and names
/// it, because the restart distance is only readable against that one. It used
/// to attach to RSS and say so as a literal; after the guard moved to
/// phys_footprint that made the line understate the distance by the whole gap
/// between the gauges — 29% printed against a real 40%, and four routine runs
/// re-confirmed the node as healthy on it.
pub(super) fn memory_status_line(snapshot: &StatusSnapshot) -> String {
    let Some(rss) = snapshot.rss_bytes else {
        return "Memory RSS: unknown".to_string();
    };
    let metric = snapshot.memory_guard_metric;

    // How close to a guard restart, in the guard's own unit. `None` where that
    // gauge is not served on this platform: an unmeasurable distance is
    // reported as unmeasured, never as the other gauge's number.
    let enforced = match metric {
        MemoryGuardMetric::Rss => Some(rss),
        MemoryGuardMetric::PhysFootprint => snapshot.phys_footprint_bytes,
    };
    let against_limit =
        snapshot
            .memory_limit_bytes
            .map_or_else(String::new, |limit| match enforced {
                Some(enforced) => {
                    let pct = (enforced as f64 / limit as f64) * 100.0;
                    format!(" of {} guard ceiling ({pct:.0}%)", format_bytes(limit))
                }
                None => format!(
                    " of {} guard ceiling ({} unmeasured here)",
                    format_bytes(limit),
                    metric.label()
                ),
            });

    let Some(footprint) = snapshot.phys_footprint_bytes else {
        // No footprint accounting on this platform: name RSS for what it is
        // rather than implying it is the whole footprint.
        return format!("Memory RSS: {}{against_limit}", format_bytes(rss));
    };

    let peak = snapshot
        .phys_footprint_peak_bytes
        .map_or_else(String::new, |p| format!(" (peak {})", format_bytes(p)));
    // The clause rides on the gauge it measures. Both gauges are on the line,
    // so a percentage floating between them belongs to whichever it is printed
    // beside, and only one placement is true.
    let (footprint_limit, rss_limit) = match metric {
        MemoryGuardMetric::PhysFootprint => (against_limit.as_str(), ""),
        MemoryGuardMetric::Rss => ("", against_limit.as_str()),
    };
    format!(
        "Memory: footprint {}{peak}{footprint_limit} · RSS {}{rss_limit}, \
         and the guard measures {}",
        format_bytes(footprint),
        format_bytes(rss),
        metric.label(),
    )
}

/// Runtime occupancy against the budget declared at boot.
///
/// This is intentionally one line: operators can compare the measured
/// footprint multiplier with the configured projection without reaching for
/// `vmmap`, while the sampler pays only atomic/cached counter reads.
/// Size half of the `Node home:` / `Data dir:` lines.
///
/// Prints `measuring…` rather than `0 B` while no walk has completed. An
/// operator reads these lines to judge disk pressure, and `0 B` on a
/// multi-GiB store is not a smaller error than a missing number — it is a
/// different, confident, wrong answer.
///
/// The walk count rides along because it is the only place the single-flight
/// property is observable on a running node: after a restart this reads `1`
/// however many probes arrived at once, and `status_data_dir` — which measures
/// wait, not work — cannot tell you that.
pub(super) fn dir_size_display(bytes: Option<u64>, walks: u64) -> String {
    let walk_noun = if walks == 1 { "walk" } else { "walks" };
    match bytes {
        Some(bytes) => format!("{} · {walks} {walk_noun}", format_bytes(bytes)),
        None => format!("measuring… · {walks} {walk_noun}"),
    }
}

/// The slowest request's duration, and the largest per-request body in the ring.
///
/// Both halves are **measured**, and they are deliberately taken from different
/// samples: the slowest request and the biggest one are rarely the same request,
/// and a field named for bytes must not be selected by time.
///
/// This used to report `body_bytes + cold_shard_loads * (warm_bytes / warm_groups)`
/// for the slowest sample, which was wrong three ways at once, and wrong by enough
/// to be visible from the status line: on the live primary on 2026-09-06 it
/// published 20.17 GiB for one request while the whole process footprint was
/// 11.29 GiB, and 42.13 GiB earlier the same hour on a 36 GiB machine.
///
///  1. It charged **sequential** work as **simultaneous** residency. Cold loads
///     count groups admitted and evicted over the life of a request; the whole
///     warm set was 1039 groups, so a request logging thousands of loads plainly
///     never held them at once.
///  2. [`OpSample::cold_shard_loads`] says of itself that the underlying counter
///     is store-wide, so a sample is charged for loads other requests caused, and
///     that it is therefore "a bad forensic claim about one sample". Converting
///     it into a byte figure for one sample is precisely that claim.
///  3. The per-group price was the warm-set charge, which is dominated by each
///     group's open write buffer rather than by the bytes read.
///
/// So no byte figure is synthesised here any more. `body_bytes` is request-scoped
/// and exact; per-request cold-load cost is still published, as counts, by
/// `read_cost` and the `top_by_cold_shard_loads` table.
pub(super) fn peak_request_bytes_from_ops(
    ops: &crate::request_telemetry::RequestTelemetrySnapshot,
) -> (u64, u64) {
    let slowest_request_ms = ops
        .top_by_duration
        .first()
        .or_else(|| ops.recent.iter().max_by_key(|sample| sample.duration_ms))
        .map_or(0, |sample| sample.duration_ms);
    let peak_request_bytes = ops
        .top_by_duration
        .iter()
        .chain(ops.recent.iter())
        .map(|sample| sample.body_bytes)
        .max()
        .unwrap_or(0);
    (slowest_request_ms, peak_request_bytes)
}

/// One line for `lastdb status`: used-record cap for tips and atoms.
///
/// This is the memory limit for the logical resident set. The byte budgets
/// on the Memory budget line bound other collections.
pub(super) fn logical_keys_status_line(r: &ResidentHealth) -> String {
    let count = r.resident_key_count.as_wire_u64().map_or_else(
        || "unavailable from this daemon".to_string(),
        |n| n.to_string(),
    );
    let budget_n = r.resident_key_budget.as_wire_u64();
    let budget = budget_n.map_or_else(
        || "unavailable from this daemon".to_string(),
        |n| n.to_string(),
    );
    format!(
        "Logical keys: {count} / {budget} ({})",
        logical_key_memory_limit_for(budget_n)
    )
}

pub(super) fn memory_budget_status_line(memory: &MemoryBudgetHealth) -> Option<String> {
    if memory.charged_budget_bytes == 0 {
        return None;
    }
    let ratio = memory
        .implied_footprint_multiplier
        .map_or_else(|| "unavailable".to_string(), |ratio| format!("{ratio:.2}x"));
    let heaviest = if memory.deferred_heaviest_lane.is_empty() {
        "none".to_string()
    } else {
        format!(
            "{}:{}",
            memory.deferred_heaviest_lane,
            format_bytes(memory.deferred_heaviest_lane_bytes)
        )
    };
    Some(format!(
        "Memory admission: deferred={} / {} lane_fair_share={} heaviest_lane={} \
         refuse_bytes={} refuse_entries={} · reserve={} · projection={} ± {} diverged={} · \
         footprint={} over_limit={} runtime_degraded={} · implied_multiplier={ratio} · \
         allocator={} allocator_committed={} allocator_reserved={} \
         malloc_in_use={} malloc_held_free={} footprint_net={} footprint_sticky={} \
         footprint_delta_per_evicted_byte={} governor_state={} governor_state_for={} \
         host_pressure={} swap_used={} \
         compressor={} purge_failed={} malloc_released_last_purge={} \
         in_flight_loads={} in_flight_bytes={} \
         peak_request_bytes={} slowest_request_ms={} eviction_events={} effective_warm={}",
        format_bytes(memory.deferred_persist_bytes),
        format_bytes(memory.deferred_persist_cap_bytes),
        format_bytes(memory.deferred_lane_fair_share_bytes),
        heaviest,
        memory.deferred_lane_refuse_bytes,
        memory.deferred_lane_refuse_entries,
        format_bytes(memory.charged_budget_bytes),
        format_bytes(memory.projected_total_bytes),
        format_bytes(memory.projection_tolerance_bytes),
        memory.projection_diverged,
        memory
            .phys_footprint_bytes
            .map_or_else(|| "unavailable".to_string(), format_bytes),
        memory.footprint_over_limit,
        memory.runtime_degraded,
        if memory.allocator_name.is_empty() {
            "unavailable"
        } else {
            &memory.allocator_name
        },
        memory
            .allocator_committed_bytes
            .map_or_else(|| "unavailable".to_string(), format_bytes),
        memory
            .allocator_reserved_bytes
            .map_or_else(|| "unavailable".to_string(), format_bytes),
        memory
            .malloc_bytes_in_use
            .map_or_else(|| "unavailable".to_string(), format_bytes),
        memory
            .malloc_bytes_held_free
            .map_or_else(|| "unavailable".to_string(), format_bytes),
        format_bytes(memory.footprint_net_bytes),
        memory.footprint_sticky,
        memory
            .footprint_delta_per_evicted_byte
            .map_or_else(|| "unavailable".to_string(), |ratio| format!("{ratio:.3}"),),
        memory.governor_state,
        format_held_secs(memory.governor_state_held_secs),
        if memory.host_pressure.is_empty() {
            "clear"
        } else {
            memory.host_pressure.as_str()
        },
        format_bytes(memory.swap_used_bytes),
        format_bytes(memory.compressor_bytes),
        memory.purge_failed,
        format_bytes(memory.malloc_bytes_released_last_purge),
        memory.in_flight_cold_load_count,
        format_bytes(memory.in_flight_cold_load_bytes),
        format_bytes(memory.peak_request_bytes),
        memory.slowest_request_ms,
        memory.eviction_events,
        format_bytes(memory.effective_warm_budget_bytes),
    ))
}

fn node_home_line(snapshot: &StatusSnapshot) -> String {
    let size = dir_size_display(snapshot.home_bytes, snapshot.home_size_walks);
    match (snapshot.home_path_disclosed, snapshot.home.as_deref()) {
        (true, Some(path)) => format!("Node home: {path} ({size})"),
        (true, None) => format!("Node home: (path missing) ({size})"),
        (false, _) => format!("Node home: (path not disclosed) ({size})"),
    }
}

fn data_dir_line(snapshot: &StatusSnapshot) -> String {
    let size = dir_size_display(snapshot.data_dir_bytes, snapshot.data_dir_size_walks);
    match (
        snapshot.data_dir_path_disclosed,
        snapshot.data_dir.as_deref(),
    ) {
        (true, Some(path)) => format!("Data dir: {path} ({size})"),
        (true, None) => format!("Data dir: (path missing) ({size})"),
        (false, _) => format!("Data dir: (path not disclosed) ({size})"),
    }
}

fn sampler_line(snapshot: &StatusSnapshot) -> String {
    format!(
        "Sampler: last_sample={} retention_cap={}{}",
        snapshot
            .sampler
            .last_sample_at
            .map_or_else(|| "none".to_string(), |ts| ts.to_string()),
        wire_u64(&snapshot.sampler.retention_max_samples)
            .map_or_else(|| "unknown".to_string(), |n| n.to_string()),
        snapshot
            .sampler
            .last_error
            .as_ref()
            .map(|e| format!(" last_error={e}"))
            .unwrap_or_default()
    )
}

fn qos_line(snapshot: &StatusSnapshot) -> String {
    format!(
        "QoS: total={}/{} bulk={}/{} sheds_i={} sheds_b={}",
        gauge_or_zero(&snapshot.qos.total_in_use),
        gauge_or_zero(&snapshot.qos.total_permits),
        gauge_or_zero(&snapshot.qos.bulk_in_use),
        gauge_or_zero(&snapshot.qos.bulk_permits),
        gauge_or_zero(&snapshot.qos.interactive_sheds),
        gauge_or_zero(&snapshot.qos.bulk_sheds),
    )
}

fn uds_pool_line(snapshot: &StatusSnapshot) -> String {
    format!(
        "UDS pool: workers={} queue_cap={} in_flight={} rejects={}",
        gauge_or_zero(&snapshot.uds.workers),
        gauge_or_zero(&snapshot.uds.queue_capacity),
        gauge_or_zero(&snapshot.uds.in_flight),
        gauge_or_zero(&snapshot.uds.queue_full_rejects),
    )
}

/// Blocking long-poll waiters hold a worker and take no QoS permit, so
/// `in_flight` can be high while `QoS total` reads 0. This line is what
/// closes that gap: it names the share of the pool that is asleep.
fn watchers_line(snapshot: &StatusSnapshot) -> String {
    format!(
        "Watchers: {}/{} peak={} sheds={} (blocking long-polls; hold a UDS \
         worker, invisible to QoS)",
        gauge_or_zero(&snapshot.watchers.active),
        gauge_or_zero(&snapshot.watchers.max),
        gauge_or_zero(&snapshot.watchers.peak),
        gauge_or_zero(&snapshot.watchers.sheds),
    )
}

fn limits_line(snapshot: &StatusSnapshot) -> String {
    format!(
        "Limits: max_request_body={} B; max_atom_content={} B (default {} B; abs max {} B; env {})",
        gauge_or_zero(&snapshot.limits.max_request_body_bytes),
        gauge_or_zero(&snapshot.limits.max_atom_content_bytes),
        gauge_or_zero(&snapshot.limits.max_atom_content_bytes_default),
        gauge_or_zero(&snapshot.limits.max_atom_content_bytes_absolute_max),
        snapshot.limits.max_atom_content_bytes_env,
    )
}

/// Memory, CPU, paths, logs, and the sync/recoverability lines that open the report.
fn leading_status_lines(snapshot: &StatusSnapshot) -> Vec<String> {
    let mut lines = vec![
        memory_status_line(snapshot),
        format!(
            "CPU: {}",
            snapshot
                .cpu_percent
                .map_or_else(|| "warming up".to_string(), |pct| format!("{pct:.2}%"))
        ),
        node_home_line(snapshot),
        data_dir_line(snapshot),
        log_streams_line(&snapshot.logs),
        molecule_edge_status_line(&snapshot.molecule_edges),
        at_rest_compression_status_line(snapshot.at_rest_compression.as_ref()),
        "Automatic backup: disabled".to_string(),
        sync_status_line(snapshot.sampled_at, &snapshot.sync),
        upload_policy_status_line(&snapshot.sync),
    ];
    if let Some(line) = capture_status_line(&snapshot.sync) {
        lines.push(line);
    }
    lines.insert(1, logical_keys_status_line(&snapshot.resident));
    if let Some(line) = memory_budget_status_line(&snapshot.memory_budget) {
        lines.insert(2, line);
    }
    if let Some(line) = snapshot.memory_budget.core_warm.status_line() {
        lines.insert(3.min(lines.len()), line);
    }
    lines
}

fn backup_status_lines(snapshot: &StatusSnapshot) -> Vec<String> {
    let mut lines = Vec::new();
    if let Some(backup) = snapshot.backup.as_ref() {
        if let Some(line) = backup_progress_status_line(backup) {
            lines.push(line);
        }
    }
    if let Some(storage) = snapshot.backup_storage.as_ref() {
        lines.push(backup_storage_status_line(storage));
    }
    lines.push(durability_status_line(&snapshot.durability));
    lines
}

fn trailing_status_lines(snapshot: &StatusSnapshot) -> Vec<String> {
    let mut lines = Vec::new();
    if let Some(rc) = snapshot.read_cost.as_ref() {
        lines.push(read_cost_status_line(rc));
    }
    lines.push(resident_status_line(&snapshot.resident));
    if let Some(line) = integrity_status_line(&snapshot.integrity) {
        lines.push(line);
    }
    if let Some(line) = file_blob_status_line(&snapshot.file_blob) {
        lines.push(line);
    }
    lines.extend(purge_stats_lines(snapshot));
    lines.extend(local_retention_lines(snapshot));
    lines.extend(atom_counter_status_lines(snapshot));
    lines.extend(blob_counter_status_lines(snapshot));
    lines.extend(conflict_retention_status_lines(snapshot));
    lines
}

pub fn status_lines(snapshot: &StatusSnapshot) -> Vec<String> {
    let mut lines = leading_status_lines(snapshot);
    lines.extend(backup_status_lines(snapshot));
    lines.extend([
        sampler_line(snapshot),
        qos_line(snapshot),
        uds_pool_line(snapshot),
        watchers_line(snapshot),
        limits_line(snapshot),
    ]);
    lines.extend(trailing_status_lines(snapshot));
    lines
}

pub(super) fn molecule_edge_status_line(health: &MoleculeEdgeHealth) -> String {
    let bytes = |gauge: &Gauge| match gauge.value {
        Availability::Measured(value) => format_bytes(value),
        Availability::Unavailable(_) => "unavailable".to_string(),
    };
    format!("Molecule edges: bytes={}", bytes(&health.bytes))
}

/// At-rest compression effectiveness since this daemon process started.
///
/// This line is unconditional. In particular, zero counters are useful: they
/// mean the daemon supports the metric but has not sealed anything since its
/// last restart. An absent field is reserved for an older, unsupported daemon.
pub(super) fn at_rest_compression_status_line(stats: Option<&AtRestCompressionHealth>) -> String {
    let Some(stats) = stats else {
        return "At-rest compression: unsupported by daemon (no process-lifetime counters)"
            .to_string();
    };
    format!(
        "At-rest compression: {:.1}% saved ({} -> {}; {} saved) sealed={} compressed / {} plain; attempts={} cpu={}ns skips=below_min:{} above_ceiling:{} not_smaller:{} disabled:{}; enabled={}; unsealed_discarded={} reaped={} ({}); process-lifetime counters (reset on daemon restart)",
        stats.percent_saved(),
        format_bytes(stats.bytes_in),
        format_bytes(stats.bytes_out),
        format_bytes(stats.bytes_saved),
        stats.sealed_compressed,
        stats.sealed_plain,
        stats.compression_attempts,
        stats.compression_attempt_cpu_ns,
        stats.skipped_below_min_bytes,
        stats.skipped_above_inflate_ceiling,
        stats.skipped_output_not_smaller,
        stats.skipped_compression_disabled,
        stats.enabled,
        stats.unsealed_discarded,
        stats.unsealed_reaped_rows,
        format_bytes(stats.unsealed_reaped_bytes),
    )
}
