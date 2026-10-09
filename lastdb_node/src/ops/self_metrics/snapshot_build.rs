use super::*;

pub async fn status_snapshot(host: &Host) -> StatusSnapshot {
    build_snapshot(host, &mut CpuProbe::default(), false).await
}

pub(super) async fn build_snapshot(
    host: &Host,
    cpu: &mut CpuProbe,
    refresh_sync: bool,
) -> StatusSnapshot {
    // lint:fn-size-ok moved verbatim from self_metrics.rs; splitting this function is separate work.
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
    if let Some(backup) = backup.as_ref() {
        fold_db::backup_durability::apply_cut_unbackable(
            &mut durability_health,
            wire_u64(&backup.chunks_unbackable_manifest).unwrap_or(0),
        );
    }
    let durability = DurabilityHealth::from(durability_health);
    request_phases::add_phase(RequestPhase::StatusDurability, durability_started.elapsed());
    let data_dir_started = Instant::now();
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
    request_phases::add_phase(RequestPhase::StatusDataDir, data_dir_started.elapsed());
    // Always disclose on the current owner/status path so HTTP clients (and
    // non-socket targets that still hit /api/status) can resolve node-relative
    // durable paths. A future network bind policy can flip this without
    // reintroducing silent omission — see `data_dir_path_for_status`.
    let (home, home_path_disclosed) = data_dir_path_for_status(&host.home, true);
    let (data_dir, data_dir_path_disclosed) = data_dir_path_for_status(&host.data_dir, true);
    let footprint = current_phys_footprint();
    let namespaced_store = host.db.db_ops().namespaced_store();
    let atom_ref_edges = host
        .self_metrics
        .atom_ref_edge_health()
        .unwrap_or_else(|| configured_atom_ref_edge_health(host));
    let resident = ResidentHealth::from_graph(host.db.db_ops().resident());
    let runtime_memory = footprint.map(|footprint| {
        let observation = host
            .db
            .mutation_manager()
            .observe_memory_footprint(footprint.current_bytes);
        crate::ops::footprint::defend_measured_footprint(host, footprint.current_bytes);
        let budget = fold_db::memory_budget::process_memory_budget();
        let implied_multiplier = if budget.charged_bytes == 0 {
            0.0
        } else {
            footprint.current_bytes as f64 / budget.charged_bytes as f64
        };
        if observation.projection_warning_now {
            let warm_cache_reclaimed = match namespaced_store.trim_warm_cache_for_pressure() {
                Ok(Some(bytes)) => Some(bytes),
                Ok(None) => None,
                Err(error) => {
                    tracing::warn!(
                        target: "lastdbd::self_metrics",
                        error = %error,
                        "warm-cache pressure trim failed"
                    );
                    None
                }
            };
            tracing::warn!(
                target: "lastdbd::self_metrics",
                measured_footprint_mb = footprint.current_bytes / (1024 * 1024),
                projected_total_mb = observation.projected_total_bytes / (1024 * 1024),
                projection_tolerance_mb = observation.projection_tolerance_bytes / (1024 * 1024),
                implied_footprint_multiplier = implied_multiplier,
                warm_cache_reclaimed_bytes = warm_cache_reclaimed,
                "measured physical footprint diverges from the process memory projection; trimmed the warm cache"
            );
        }
        if observation.over_limit_alarm_now {
            // Only a hard latch actually closes the defer window. A near-limit
            // trip against a fitted boot budget keeps it open, so reporting
            // "deferred writes are disabled" there paged on-call for a
            // condition that had not happened (Sentry 7671016505).
            if observation.runtime_degraded {
                tracing::error!(
                    target: "lastdbd::self_metrics",
                    measured_footprint_mb = footprint.current_bytes / (1024 * 1024),
                    guard_limit_mb = budget.rss_limit_bytes / (1024 * 1024),
                    projected_total_mb = observation.projected_total_bytes / (1024 * 1024),
                    implied_footprint_multiplier = implied_multiplier,
                    effective_deferred_cap_bytes = observation.effective_deferred_cap_bytes,
                    "measured physical footprint exceeds the process guard; new deferred writes are disabled and persist inline"
                );
            } else {
                tracing::warn!(
                    target: "lastdbd::self_metrics",
                    measured_footprint_mb = footprint.current_bytes / (1024 * 1024),
                    guard_limit_mb = budget.rss_limit_bytes / (1024 * 1024),
                    projected_total_mb = observation.projected_total_bytes / (1024 * 1024),
                    implied_footprint_multiplier = implied_multiplier,
                    effective_deferred_cap_bytes = observation.effective_deferred_cap_bytes,
                    "measured physical footprint crossed the process guard; the derived defer window stays open because the boot budget still fits"
                );
            }
        }
        if observation.runtime_recovered_now {
            // The counterpart of the error above: the same episode closes here.
            // Without this line the Sentry record reads "disabled" with no end.
            tracing::info!(
                target: "lastdbd::self_metrics",
                measured_footprint_mb = footprint.current_bytes / (1024 * 1024),
                guard_limit_mb = budget.rss_limit_bytes / (1024 * 1024),
                projected_total_mb = observation.projected_total_bytes / (1024 * 1024),
                recovery_line_mb =
                    fold_db::memory_budget::footprint_latch_recovery_line(budget) / (1024 * 1024),
                hold_secs = fold_db::memory_budget::FOOTPRINT_LATCH_RECOVERY_HOLD_SECS,
                effective_deferred_cap_bytes = observation.effective_deferred_cap_bytes,
                "measured physical footprint held under the recovery line; the deferred-write window is open again"
            );
        }
        observation
    });
    // Read after the pressure correction so this status response reports the
    // corrected metadata. The accessor only takes the warm-set mutex.
    let read_cost = host.db.db_ops().read_cost().map(ReadCostHealth::from);
    let (_, _, deferred_persist_bytes, deferred_persist_cap_bytes) =
        host.db.mutation_manager().defer_window_in_flight();
    let persist_lanes = host.db.mutation_manager().persist_lane_pressure();
    let exclusive_hold_us = if persist_lanes.heaviest_schema.is_empty() {
        0
    } else {
        host.db
            .mutation_manager()
            .purge_stats_for_schema(&persist_lanes.heaviest_schema)
            .exclusive_hold_us
    };
    request_ops.persist_lanes = vec![crate::request_telemetry::PersistLaneOpRow {
        schema: persist_lanes.heaviest_schema.clone(),
        reserved_bytes: persist_lanes.heaviest_bytes,
        queued_entries: persist_lanes.queued_entries,
        reserved_entries: persist_lanes.reserved_entries,
        oldest_age_ms: persist_lanes.oldest_age_ms,
        exclusive_hold_us,
        refuse_bytes: persist_lanes.refuse_bytes,
        refuse_entries: persist_lanes.refuse_entries,
        refuse_unhealthy: persist_lanes.refuse_unhealthy,
        write_throughs: persist_lanes.write_throughs,
        unhealthy_lanes: persist_lanes.unhealthy_lanes,
        fair_share_bytes: persist_lanes.fair_share_bytes,
        quarantined: persist_lanes.quarantined,
        breaker_trips: persist_lanes.breaker_trips,
    }]
    .into_iter()
    .filter(persist_lane_row_is_visible)
    .collect();
    let admission = namespaced_store.warm_set_admission_stats();
    let allocator = crate::allocator::metrics();
    let (slowest_request_ms, peak_request_bytes) = peak_request_bytes_from_ops(&request_ops);
    let memory_budget = MemoryBudgetHealth::measured(
        read_cost.as_ref(),
        &resident,
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
        home_bytes,
        home_size_walks,
        home,
        home_path_disclosed,
        data_dir_bytes,
        data_dir_size_walks,
        data_dir,
        data_dir_path_disclosed,
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
        dual_read: fold_db::storage::dual_read_metrics_snapshot().into(),
        build: BuildHealth::current(),
        integrity: {
            let damage = host.db.db_ops().unresolved_atom_distinct();
            IntegrityHealth::measured(
                host.db.db_ops().unresolved_atom_skip_count(),
                damage.edges,
                Some(damage.rows),
                damage.capped,
            )
        },
        atom_ref_edges,
        file_blob: match host.db.sync_engine() {
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
        },
        locator_only: fold_db::db_operations::last_locator_only_probe().map(Into::into),
        purge_stats: host.db.mutation_manager().purge_stats_snapshot(),
        local_retention: host
            .self_metrics
            .local_retention_health(crate::ttl_sweep::ttl_sweep_enabled()),
        molecule_gate: {
            let hold = host.db.db_ops().molecule_gate_hold();
            MoleculeGateHealth::measured(hold.total_us, hold.count, hold.max_us)
        },
        at_rest_compression: Some(AtRestCompressionHealth::current()),
        codec_policy: Some(CodecPolicyHealth::current(&host.home)),
    }
}

pub(super) fn persist_lane_row_is_visible(
    row: &crate::request_telemetry::PersistLaneOpRow,
) -> bool {
    !row.schema.is_empty()
        || row.reserved_bytes > 0
        || row.refuse_bytes > 0
        || row.refuse_entries > 0
        || row.refuse_unhealthy > 0
        || row.write_throughs > 0
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
