use super::snapshot_memory::observe_runtime_memory;
use super::*;

pub async fn status_snapshot(host: &Host) -> StatusSnapshot {
    build_snapshot(host, &mut CpuProbe::default()).await
}

pub(super) async fn build_snapshot(host: &Host, cpu: &mut CpuProbe) -> StatusSnapshot {
    let sampled_at = unix_secs();
    let cpu_percent = cpu.sample_percent();
    let qos: QosHealth = host.qos.snapshot().into();
    let request_ops_started = Instant::now();
    let request_ops = host.request_telemetry.snapshot();
    request_phases::add_phase(
        RequestPhase::StatusRequestOps,
        request_ops_started.elapsed(),
    );
    let durability = durability_health(host);
    let dirs = DirReport::measure(host);
    let footprint = current_phys_footprint();
    let runtime_memory = footprint.map(|footprint| observe_runtime_memory(host, footprint));
    // Pressure relief can purge clean records. Capture native status
    // afterward so its current counts describe the state after that relief.
    let native = host
        .db
        .db_ops()
        .namespaced_store()
        .disk_pack_runtime()
        .status_native();
    let resident = ResidentHealth::from_native(&native);
    let mut memory_budget = memory_budget_health(host, footprint, runtime_memory, &request_ops);
    memory_budget.core_warm = CoreWarmBytesHealth::from(&native.core_warm_bytes);
    StatusSnapshot {
        logs: daemon_log_streams(),
        sampled_at,
        process_start_ts: host.process_start_ts,
        uptime_secs: sampled_at.saturating_sub(host.process_start_ts),
        rss_bytes: current_rss_bytes(),
        phys_footprint_bytes: footprint.map(|f| f.current_bytes),
        phys_footprint_peak_bytes: footprint.map(|f| f.peak_bytes),
        memory_limit_bytes: memory_limit_bytes(),
        memory_guard_metric: memory_guard_metric(),
        memory_budget,
        cpu_percent,
        home_bytes: dirs.home_bytes,
        home_size_walks: dirs.home_size_walks,
        home: dirs.home,
        data_dir_bytes: dirs.data_dir_bytes,
        data_dir_size_walks: dirs.data_dir_size_walks,
        data_dir: dirs.data_dir,
        durability,
        sampler: host.self_metrics.snapshot(),
        qos,
        uds: host
            .uds_workers
            .get()
            .map(|p| p.snapshot().into())
            .unwrap_or_default(),
        watchers: host.watch_gate.snapshot().into(),
        request_ops,
        limits: LimitsHealth::default(),
        resident,
        build: BuildHealth::current(),
        local_retention: host
            .self_metrics
            .local_retention_health(crate::ttl_sweep::ttl_sweep_enabled()),
        verify_refs: host.db.verify_refs_summary(),
        blob_counters: host.db.blob_counter_report().await,
        conflict_retention: host.db.conflict_retention_report().await,
        at_rest_compression: AtRestCompressionHealth::current(),
    }
}

/// Node home and data dir size gauges plus their paths.
struct DirReport {
    home_bytes: Option<u64>,
    home_size_walks: u64,
    home: String,
    data_dir_bytes: Option<u64>,
    data_dir_size_walks: u64,
    data_dir: String,
}

impl DirReport {
    fn measure(host: &Host) -> Self {
        let started = Instant::now();
        // Node home (primary operator gauge for disk pressure) and data dir
        // (secondary). Both are cache reads: neither ever walks on this path, so
        // `status_data_dir` is now microseconds even on the first call after a
        // restart. `None` means no walk has completed yet — render it as
        // "measuring…", never as zero.
        let ttl = data_dir_size_ttl_from_env();
        let home_bytes = NODE_HOME_SIZE.get(&host.home, ttl);
        let data_dir_bytes = DATA_DIR_SIZE.get(&host.data_dir, ttl);
        let home_size_walks = NODE_HOME_SIZE.walks();
        let data_dir_size_walks = DATA_DIR_SIZE.walks();
        request_phases::add_phase(RequestPhase::StatusDataDir, started.elapsed());
        // HTTP clients (and non-socket targets that hit /api/status) resolve
        // node-relative durable paths from these.
        Self {
            home_bytes,
            home_size_walks,
            home: status_path(&host.home),
            data_dir_bytes,
            data_dir_size_walks,
            data_dir: status_path(&host.data_dir),
        }
    }
}

fn durability_health(host: &Host) -> DurabilityHealth {
    let started = Instant::now();
    let health = crate::byte_cloud_backup::status(host)
        .map(DurabilityHealth::from)
        .unwrap_or_default();
    request_phases::add_phase(RequestPhase::StatusDurability, started.elapsed());
    health
}

/// Memory budget, with the request peaks taken from `request_ops`.
fn memory_budget_health(
    host: &Host,
    footprint: Option<PhysFootprint>,
    runtime_memory: Option<fold_db::memory_budget::RuntimeMemoryBudgetObservation>,
    request_ops: &crate::request_telemetry::RequestTelemetrySnapshot,
) -> MemoryBudgetHealth {
    let (_, _, deferred_persist_bytes, deferred_persist_cap_bytes) =
        host.db.mutation_manager().defer_window_in_flight();
    let allocator = crate::allocator::metrics();
    let (slowest_request_ms, peak_request_bytes) = peak_request_bytes_from_ops(request_ops);
    MemoryBudgetHealth::measured(
        deferred_persist_bytes,
        deferred_persist_cap_bytes,
        footprint.map(|f| f.current_bytes),
        runtime_memory,
        MemoryBudgetAttribution {
            allocator,
            peak_request_bytes,
            slowest_request_ms,
            governor: crate::jobs::footprint::governor_snapshot(),
        },
    )
}
