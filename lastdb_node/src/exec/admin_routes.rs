use super::*;

pub(super) fn execute_status_route(ctx: &AccessContext, host: &Host) -> UdsResponse {
    let uptime_secs = fold_db::clock::unix_secs().saturating_sub(host.process_start_ts);
    let uds = host
        .uds_workers
        .get()
        .map(lastdb_uds::UdsWorkerPool::snapshot)
        .map(|s| {
            serde_json::json!({
                "workers": s.workers,
                "queue_capacity": s.queue_capacity,
                "in_flight": s.in_flight,
                "submitted": s.submitted,
                "queue_full_rejects": s.queue_full_rejects,
            })
        });
    let durability = match crate::byte_cloud_backup::status(host) {
        Ok(s) => {
            if let Some(shared) = s.shared_sync {
                serde_json::json!({
                    "observed": true,
                    "automatic_enabled": shared.enabled,
                    "running": shared.progress.running,
                    "degraded": shared.progress.blocked_reason.is_some(),
                })
            } else {
                let degraded = s.blocked_reason.is_some()
                    || s.automatic_backup
                        .as_ref()
                        .is_some_and(|a| a.paused.is_some());
                serde_json::json!({
                    "observed": true,
                    "automatic_enabled": s.automatic_backup_enabled,
                    "running": s.running,
                    "phase": s.phase,
                    "degraded": degraded,
                })
            }
        }
        Err(_) => serde_json::json!({
            "observed": false,
            "automatic_enabled": false,
            "running": false,
        }),
    };
    let search = fold_db::db_operations::search_index::queue_policy::status(
        &fold_db::db_operations::search_index::search_inbox_dir_for_home(&host.home),
    );
    let status = serde_json::json!({
        "build": {
            "version": crate::version::build_version(),
        },
        "process_start_ts": host.process_start_ts,
        "uptime_secs": uptime_secs,
        "uds": uds,
        "durability": durability,
        "search": search,
    });
    json_ok(&envelope(
        &serde_json::json!({ "status": status }),
        ctx.user_id.as_str(),
    ))
}

pub(super) async fn execute_admin_shed_route(ctx: &AccessContext, host: &Host) -> UdsResponse {
    let runtime = host.db.db_ops().native_runtime();
    let flush_err = runtime.flush_native().await.err().map(|e| e.to_string());
    let (target_met, records_purged, purge_err) = match runtime.purge_clean_memory(0) {
        Ok(report) => (
            flush_err.is_none() && report.target_met,
            report.records_purged,
            None,
        ),
        Err(e) => (false, 0, Some(e.to_string())),
    };
    let error = flush_err.or(purge_err);
    json_ok(&envelope(
        &serde_json::json!({
            "shed": {
                "target_met": target_met,
                "records_purged": records_purged,
                "error": error,
            }
        }),
        ctx.user_id.as_str(),
    ))
}

/// `GET /api/db/inventory` — live key-class and per-schema record byte
/// breakdown on the owner socket. Reads the open store.
pub(super) async fn execute_db_inventory_route(ctx: &AccessContext, host: &Host) -> UdsResponse {
    match host.db.db_inventory().await {
        Ok(inventory) => json_ok(&envelope(
            &serde_json::json!({ "inventory": inventory }),
            ctx.user_id.as_str(),
        )),
        Err(e) => error_response(500, &format!("db inventory failed: {e}"), ctx),
    }
}

/// `GET /api/db/schemas` — record-only per-schema logical storage (owner socket).
pub(super) async fn execute_db_schemas_route(ctx: &AccessContext, host: &Host) -> UdsResponse {
    match host.db.db_schema_storage().await {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({ "schema_storage": report }),
            ctx.user_id.as_str(),
        )),
        Err(e) => error_response(500, &format!("db schemas failed: {e}"), ctx),
    }
}
