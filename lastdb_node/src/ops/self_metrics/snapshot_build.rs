use super::process_probes::PhysFootprint;
use super::snapshot_memory::{observe_runtime_memory, persist_lane_rows};
use super::*;

pub async fn status_snapshot(host: &Host) -> StatusSnapshot {
    build_snapshot(host, &mut CpuProbe::default(), false).await
}

pub(super) async fn build_snapshot(
    host: &Host,
    cpu: &mut CpuProbe,
    refresh_sync: bool,
) -> StatusSnapshot {
    let sampled_at = unix_secs();
    let cpu_percent = cpu.sample_percent();
    let qos: QosHealth = host.qos.snapshot().into();
    let request_ops_started = Instant::now();
    let mut request_ops = host.request_telemetry.snapshot();
    request_phases::add_phase(
        RequestPhase::StatusRequestOps,
        request_ops_started.elapsed(),
    );
    publish_sync_foreground_pressure(host, cpu_percent, &qos, &request_ops);
    let sync = timed_sync_health(host, refresh_sync).await;
    let (backup, backup_storage) = backup_health(host);
    let durability = durability_health(host, sampled_at, backup.as_ref());
    let dirs = DirReport::measure(host);
    let footprint = current_phys_footprint();
    let runtime_memory = footprint.map(|footprint| observe_runtime_memory(host, footprint));
    // Pressure relief can purge clean logical records. Capture native status
    // afterward so its current counts describe the state after that relief.
    let namespaced_store = host.db.db_ops().namespaced_store();
    let native = namespaced_store
        .disk_pack_runtime()
        .expect("logical status requires native storage")
        .status_native();
    let resident =
        ResidentHealth::from_native(&native, host.db.db_ops().persist_metrics().snapshot());
    let (mut memory_budget, read_cost) = memory_budget_health(
        host,
        &namespaced_store,
        footprint,
        runtime_memory,
        &resident,
        &mut request_ops,
    );
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
        home_path_disclosed: dirs.home_path_disclosed,
        data_dir_bytes: dirs.data_dir_bytes,
        data_dir_size_walks: dirs.data_dir_size_walks,
        data_dir: dirs.data_dir,
        data_dir_path_disclosed: dirs.data_dir_path_disclosed,
        sync,
        backup,
        backup_storage,
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
        read_cost,
        resident,
        build: BuildHealth::current(),
        integrity: integrity_health(host),
        atom_ref_edges: host
            .self_metrics
            .atom_ref_edge_health()
            .unwrap_or_else(|| configured_atom_ref_edge_health(host)),
        file_blob: file_blob_health(host).await,
        purge_stats: host.db.mutation_manager().purge_stats_snapshot(),
        local_retention: host
            .self_metrics
            .local_retention_health(crate::ttl_sweep::ttl_sweep_enabled()),
        atom_counters: Some(host.db.atom_counter_report()),
        blob_counters: Some(host.db.blob_counter_report().await),
        conflict_retention: Some(host.db.conflict_retention_report().await),
        at_rest_compression: Some(AtRestCompressionHealth::current()),
        codec_policy: Some(CodecPolicyHealth::current(&host.home)),
    }
}

/// Node home and data dir size gauges plus their disclosed paths.
struct DirReport {
    home_bytes: Option<u64>,
    home_size_walks: u64,
    home: Option<String>,
    home_path_disclosed: bool,
    data_dir_bytes: Option<u64>,
    data_dir_size_walks: u64,
    data_dir: Option<String>,
    data_dir_path_disclosed: bool,
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
        // Always disclose on the current owner/status path so HTTP clients (and
        // non-socket targets that still hit /api/status) can resolve node-relative
        // durable paths. A future network bind policy can flip this without
        // reintroducing silent omission — see `data_dir_path_for_status`.
        let (home, home_path_disclosed) = data_dir_path_for_status(&host.home, true);
        let (data_dir, data_dir_path_disclosed) = data_dir_path_for_status(&host.data_dir, true);
        Self {
            home_bytes,
            home_size_walks,
            home,
            home_path_disclosed,
            data_dir_bytes,
            data_dir_size_walks,
            data_dir,
            data_dir_path_disclosed,
        }
    }
}

async fn timed_sync_health(host: &Host, refresh_sync: bool) -> SyncHealth {
    let sync_started = Instant::now();
    let sync = if refresh_sync {
        let health = sync_health(host).await;
        // A timed-out / unsampled probe must not replace a last-known snapshot
        // with empty gauges. Those empty gauges render as last_success=never
        // and "log plane not recording" — false durability-outage sentences.
        if health.snapshot_unavailable_reason.is_none() {
            host.self_metrics.cache_sync_health(health.clone());
        }
        health
    } else {
        cached_sync_health(host)
    };
    request_phases::add_phase(RequestPhase::StatusSync, sync_started.elapsed());
    sync
}

/// Backup progress and the cached storage footprint, with the phase timing.
fn backup_health(host: &Host) -> (Option<BackupProgressHealth>, Option<BackupStorageHealth>) {
    let backup_started = Instant::now();
    let backup = host
        .db
        .sync_engine()
        .map(|engine| BackupProgressHealth::from(engine.backup_progress_snapshot()));
    // Cheap cache read only — list_objects lives on GC / background refresh.
    let backup_storage = host
        .db
        .sync_engine()
        .and_then(|engine| engine.backup_storage_footprint_snapshot())
        .map(BackupStorageHealth::from);
    // Kick a single background list when the cache is empty and sync is up so
    // the next status poll can print the storage line without blocking this one.
    if backup_storage.is_none() {
        schedule_backup_storage_refresh(host);
    }
    request_phases::add_phase(RequestPhase::StatusBackup, backup_started.elapsed());
    (backup, backup_storage)
}

fn durability_health(
    host: &Host,
    sampled_at: u64,
    backup: Option<&BackupProgressHealth>,
) -> DurabilityHealth {
    // Read off the durable marker, not the engine: this must answer even when
    // `sync_engine()` is None, which is the case the alarm exists for.
    let durability_started = Instant::now();
    let mut durability_health = fold_db::backup_durability::evaluate_store(
        &host.data_dir,
        sampled_at,
        fold_db::backup_durability::max_backup_age_secs_from_env(),
    );
    // The age check above is deliberately engine-free (it must answer when
    // backup is switched off). Fold in the one terminal fact only the running
    // uploader knows, so a home that provably cannot publish says so at cut
    // time instead of waiting out the 24-hour threshold.
    if let Some(backup) = backup {
        fold_db::backup_durability::apply_cut_unbackable(
            &mut durability_health,
            wire_u64(&backup.chunks_unbackable_manifest).unwrap_or(0),
        );
    }
    let durability = DurabilityHealth::from(durability_health);
    request_phases::add_phase(RequestPhase::StatusDurability, durability_started.elapsed());
    durability
}

fn integrity_health(host: &Host) -> IntegrityHealth {
    let damage = host.db.db_ops().unresolved_atom_distinct();
    IntegrityHealth::measured(
        host.db.db_ops().unresolved_atom_skip_count(),
        damage.edges,
        Some(damage.rows),
        damage.capped,
    )
}

async fn file_blob_health(host: &Host) -> FileBlobHealth {
    match host.db.sync_engine() {
        // Read straight off the engine rather than through `sync_status()`:
        // the durability counters are independent of every other sync
        // field, and recomputing a whole status snapshot to reach four
        // atomics would put real work on the status path.
        //
        // No engine means no cloud plane, so there is nothing to have lost
        // — but "not served" is still the honest answer, not a measured
        // zero, and `FileBlobHealth::default()` is exactly that.
        Some(engine) => engine.file_blob_durability().await.into(),
        None => FileBlobHealth::default(),
    }
}

/// Memory budget plus the read-cost row it reads. Also stamps the persist-lane
/// rows onto `request_ops`.
fn memory_budget_health(
    host: &Host,
    namespaced_store: &std::sync::Arc<dyn fold_db::storage::NamespacedStore>,
    footprint: Option<PhysFootprint>,
    runtime_memory: Option<fold_db::memory_budget::RuntimeMemoryBudgetObservation>,
    resident: &ResidentHealth,
    request_ops: &mut crate::request_telemetry::RequestTelemetrySnapshot,
) -> (MemoryBudgetHealth, Option<ReadCostHealth>) {
    // Read after the pressure correction so this status response reports the
    // corrected metadata. The accessor only takes the warm-set mutex.
    let read_cost = host.db.db_ops().read_cost().map(ReadCostHealth::from);
    let (_, _, deferred_persist_bytes, deferred_persist_cap_bytes) =
        host.db.mutation_manager().defer_window_in_flight();
    let persist_lanes = host.db.mutation_manager().persist_lane_pressure();
    request_ops.persist_lanes = persist_lane_rows(&persist_lanes);
    let admission = namespaced_store.warm_set_admission_stats();
    let allocator = crate::allocator::metrics();
    let (slowest_request_ms, peak_request_bytes) = peak_request_bytes_from_ops(request_ops);
    let memory_budget = MemoryBudgetHealth::measured(
        read_cost.as_ref(),
        resident,
        deferred_persist_bytes,
        deferred_persist_cap_bytes,
        &persist_lanes,
        footprint.map(|f| f.current_bytes),
        runtime_memory,
        MemoryBudgetAttribution {
            admission,
            allocator,
            peak_request_bytes,
            slowest_request_ms,
        },
    );
    (memory_budget, read_cost)
}

pub(super) fn persist_lane_row_is_visible(
    row: &crate::request_telemetry::PersistLaneOpRow,
) -> bool {
    !row.schema.is_empty()
        || row.reserved_bytes > 0
        || row.refuse_bytes > 0
        || row.refuse_entries > 0
        || row.refuse_unhealthy > 0
        || row.unhealthy_lanes > 0
        || row.quarantined > 0
        || row.breaker_trips > 0
}

pub(super) fn publish_sync_foreground_pressure(
    host: &Host,
    cpu_percent: Option<f64>,
    qos: &QosHealth,
    request_ops: &crate::request_telemetry::RequestTelemetrySnapshot,
) {
    let Some(engine) = host.db.sync_engine() else {
        return;
    };
    let foreground_busy_ms = foreground_busy_p95_ms_from_env();
    let foreground_p95_ms = request_ops
        .app_verb
        .iter()
        .filter_map(|aggregate| aggregate.p95_ms)
        .max();
    engine.set_foreground_pressure_sample(fold_db::sync::ForegroundPressure {
        source: "lastdbd_status_sampler".to_string(),
        foreground_p95_ms,
        foreground_busy_ms,
        qos_total_in_use: gauge_or_zero_usize(&qos.total_in_use),
        qos_total_permits: gauge_or_zero_usize(&qos.total_permits),
        qos_interactive_shed_delta: qos_interactive_shed_delta(gauge_or_zero(
            &qos.interactive_sheds,
        )),
        cpu_percent,
    });
}

pub(super) fn foreground_busy_p95_ms_from_env() -> u64 {
    env_flag::var_or("LASTDB_SYNC_FOREGROUND_BUSY_P95_MS", 1_000).clamp(50, 60_000)
}

pub(super) fn qos_interactive_shed_delta(current: u64) -> u64 {
    static PREV_INTERACTIVE_SHEDS: OnceLock<Mutex<u64>> = OnceLock::new();
    let state = PREV_INTERACTIVE_SHEDS.get_or_init(|| Mutex::new(current));
    let Ok(mut previous) = state.lock() else {
        return 0;
    };
    let delta = current.saturating_sub(*previous);
    *previous = current;
    delta
}
