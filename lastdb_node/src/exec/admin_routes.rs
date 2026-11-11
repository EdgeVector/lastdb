//! Status, identity, shed, schema-storage, and native flush/cleanup/status routes.

use fold_db::access::AccessContext;
use serde::Deserialize;

use super::reply::{envelope, error_response, json_ok, mapped_error_response};
use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};

pub(in crate::exec) fn execute_status_route(ctx: &AccessContext, host: &Host) -> UdsResponse {
    let uptime_secs = fold_db::clock::unix_secs().saturating_sub(host.process_start_ts);
    let uds = host
        .uds_workers
        .get()
        .map(crate::lastdb_uds::UdsWorkerPool::snapshot)
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

pub(in crate::exec) fn execute_auto_identity_route(host: &Host) -> UdsResponse {
    let public_key = host.public_key();
    json_ok(&serde_json::json!({
        "user_id": public_key,
        "user_hash": host.user_hash,
        "public_key": public_key,
    }))
}

pub(in crate::exec) fn execute_boot_identity_route(host: &Host) -> UdsResponse {
    json_ok(&serde_json::json!({
        "pid": std::process::id(),
        "process_start_ts": host.process_start_ts,
        "build": crate::version::build_version(),
        "restart_cause": "clean",
    }))
}

pub(in crate::exec) async fn execute_admin_shed_route(
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
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

/// `GET /api/db/schemas` — record-only per-schema logical storage (owner socket).
pub(in crate::exec) async fn execute_db_schemas_route(
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    match host.db.db_schema_storage().await {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({ "schema_storage": report }),
            ctx.user_id.as_str(),
        )),
        Err(e) => error_response(500, &format!("db schemas failed: {e}"), ctx),
    }
}

/// Explicit owner cleanup and read-only completion receipt.
pub(in crate::exec) async fn execute_db_cleanup_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    if !ctx.is_owner {
        return error_response(403, "native cleanup is owner-only", ctx);
    }
    let runtime = host.db.db_ops().native_runtime();
    if req.method == "GET" {
        return json_ok(&envelope(
            &serde_json::json!({"cleanup": runtime.status_cleanup()}),
            ctx.user_id.as_str(),
        ));
    }
    #[derive(Default, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Body {
        request_id: Option<uuid::Uuid>,
    }
    let body = if req.body.is_empty() {
        Body::default()
    } else {
        match serde_json::from_slice::<Body>(&req.body) {
            Ok(body) => body,
            Err(error) => {
                return error_response(400, &format!("invalid cleanup request: {error}"), ctx)
            }
        }
    };
    match runtime.request_native_cleanup(body.request_id).await {
        Ok(status) => json_ok(&envelope(
            &serde_json::json!({"cleanup": status}),
            ctx.user_id.as_str(),
        )),
        Err(error) => mapped_error_response("native cleanup request failed", error.into(), ctx),
    }
}

/// Wait for all earlier accepted local writes to reach the durable boundary.
pub(in crate::exec) async fn execute_db_flush_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    if !ctx.is_owner {
        return error_response(403, "the local durability barrier is owner-only", ctx);
    }
    if !req.body.is_empty() {
        match serde_json::from_slice::<serde_json::Map<String, serde_json::Value>>(&req.body) {
            Ok(fields) if fields.is_empty() => {}
            _ => return error_response(400, "local flush body must be an empty object", ctx),
        }
    }
    let result = host
        .db
        .db_ops()
        .native_runtime()
        .flush_native()
        .await
        .map(|receipt| serde_json::json!({"durable": true, "native_flush": receipt}));
    match result {
        Ok(report) => json_ok(&envelope(&report, ctx.user_id.as_str())),
        Err(error) => error_response(503, &format!("local flush failed: {error}"), ctx),
    }
}

/// Read the native durable boundary without advancing its normal flush schedule.
pub(in crate::exec) async fn execute_db_native_status_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    if !ctx.is_owner {
        return error_response(403, "the native status route is owner-only", ctx);
    }
    let runtime = host.db.db_ops().native_runtime();
    let mut status = serde_json::json!({
        "native_disk": runtime.status_native(),
        "search": fold_db::db_operations::search_index::queue_policy::status(
            &fold_db::db_operations::search_index::search_inbox_dir_for_home(&host.home)
        ),
    });
    if laststore::diagnostics_enabled() {
        let inventory = match inventory_requested(req) {
            Ok(inventory) => inventory,
            Err(message) => return error_response(400, message, ctx),
        };
        match runtime.diagnostics_native(inventory).await {
            Ok(Some(report)) => status["storage_diagnostics"] = serde_json::json!(report),
            Ok(None) => {}
            Err(error) => {
                return mapped_error_response("storage diagnostic failed", error.into(), ctx)
            }
        }
    }
    json_ok(&envelope(&status, ctx.user_id.as_str()))
}

fn inventory_requested(req: &UdsRequest) -> Result<bool, &'static str> {
    #[derive(Deserialize, Default)]
    struct Options {
        #[serde(default)]
        inventory: bool,
    }
    if req.body.is_empty() {
        return Ok(false);
    }
    serde_json::from_slice::<Options>(&req.body)
        .map(|options| options.inventory)
        .map_err(|_| "invalid native-status diagnostic options")
}
