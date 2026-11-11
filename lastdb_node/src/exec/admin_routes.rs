//! Status, identity, shed, schema-storage, native flush/cleanup, delete-ledger, and retention routes.

use fold_db::access::AccessContext;
use fold_db::schema::SchemaRetentionPolicy;
use serde::Deserialize;
use serde_json::json;

use super::reply::{envelope, error_response, json_ok, mapped_error_response};
use super::wire::query_value;
use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};

pub(in crate::exec) fn execute_status_route(ctx: &AccessContext, host: &Host) -> UdsResponse {
    let uptime_secs = fold_db::clock::unix_secs().saturating_sub(host.process_start_ts);
    let uds = host
        .uds_workers
        .get()
        .map(crate::lastdb_uds::UdsWorkerPool::snapshot)
        .map(|s| {
            json!({
                "workers": s.workers, "queue_capacity": s.queue_capacity,
                "in_flight": s.in_flight, "submitted": s.submitted,
                "queue_full_rejects": s.queue_full_rejects,
            })
        });
    let durability = match crate::byte_cloud_backup::status(host) {
        Ok(s) => {
            let degraded = s.blocked_reason.is_some()
                || s.automatic_backup
                    .as_ref()
                    .is_some_and(|a| a.paused.is_some());
            json!({
                "observed": true, "automatic_enabled": s.automatic_backup_enabled,
                "running": s.running, "phase": s.phase, "degraded": degraded,
            })
        }
        Err(_) => json!({ "observed": false, "automatic_enabled": false, "running": false }),
    };
    let search = fold_db::db_operations::search_index::queue_policy::status(
        &fold_db::db_operations::search_index::search_inbox_dir_for_home(&host.home),
    );
    let status = json!({
        "build": { "version": crate::version::build_version() },
        "process_start_ts": host.process_start_ts, "uptime_secs": uptime_secs,
        "uds": uds, "durability": durability, "search": search,
    });
    json_ok(&envelope(
        &json!({ "status": status }),
        ctx.user_id.as_str(),
    ))
}

pub(in crate::exec) fn execute_auto_identity_route(host: &Host) -> UdsResponse {
    let public_key = host.public_key();
    json_ok(&json!({
        "user_id": public_key, "user_hash": host.user_hash, "public_key": public_key,
    }))
}

pub(in crate::exec) fn execute_boot_identity_route(host: &Host) -> UdsResponse {
    json_ok(&json!({
        "pid": std::process::id(), "process_start_ts": host.process_start_ts,
        "build": crate::version::build_version(), "restart_cause": "clean",
    }))
}

pub(in crate::exec) async fn execute_admin_shed_route(
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let runtime = host.db.db_ops().native_runtime();
    let flush_err = runtime.flush_native().await.err().map(|e| e.to_string());
    let (target_met, records_purged, purge_err) = match runtime.purge_clean_memory(0) {
        Ok(r) => (flush_err.is_none() && r.target_met, r.records_purged, None),
        Err(e) => (false, 0, Some(e.to_string())),
    };
    json_ok(&envelope(
        &json!({ "shed": { "target_met": target_met, "records_purged": records_purged, "error": flush_err.or(purge_err) } }),
        ctx.user_id.as_str(),
    ))
}

pub(in crate::exec) async fn execute_db_schemas_route(
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    match host.db.db_schema_storage().await {
        Ok(report) => json_ok(&envelope(
            &json!({ "schema_storage": report }),
            ctx.user_id.as_str(),
        )),
        Err(e) => error_response(500, &format!("db schemas failed: {e}"), ctx),
    }
}

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
            &json!({"cleanup": runtime.status_cleanup()}),
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
            Err(e) => return error_response(400, &format!("invalid cleanup request: {e}"), ctx),
        }
    };
    match runtime.request_native_cleanup(body.request_id).await {
        Ok(status) => json_ok(&envelope(&json!({"cleanup": status}), ctx.user_id.as_str())),
        Err(error) => mapped_error_response("native cleanup request failed", error.into(), ctx),
    }
}

pub(in crate::exec) async fn execute_db_flush_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    if !ctx.is_owner {
        return error_response(403, "the local durability barrier is owner-only", ctx);
    }
    if !req.body.is_empty()
        && !serde_json::from_slice::<serde_json::Map<String, serde_json::Value>>(&req.body)
            .is_ok_and(|fields| fields.is_empty())
    {
        return error_response(400, "local flush body must be an empty object", ctx);
    }
    match host.db.db_ops().native_runtime().flush_native().await {
        Ok(receipt) => json_ok(&envelope(
            &json!({"durable": true, "native_flush": receipt}),
            ctx.user_id.as_str(),
        )),
        Err(error) => error_response(503, &format!("local flush failed: {error}"), ctx),
    }
}

pub(in crate::exec) async fn execute_db_native_status_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    if !ctx.is_owner {
        return error_response(403, "the native status route is owner-only", ctx);
    }
    let runtime = host.db.db_ops().native_runtime();
    let mut status = json!({
        "native_disk": runtime.status_native(),
        "search": fold_db::db_operations::search_index::queue_policy::status(
            &fold_db::db_operations::search_index::search_inbox_dir_for_home(&host.home)
        ),
    });
    if laststore::diagnostics_enabled() {
        #[derive(Deserialize, Default)]
        struct Options {
            #[serde(default)]
            inventory: bool,
        }
        let inventory = if req.body.is_empty() {
            false
        } else {
            match serde_json::from_slice::<Options>(&req.body) {
                Ok(opts) => opts.inventory,
                Err(_) => {
                    return error_response(400, "invalid native-status diagnostic options", ctx)
                }
            }
        };
        match runtime.diagnostics_native(inventory).await {
            Ok(Some(report)) => status["storage_diagnostics"] = json!(report),
            Ok(None) => {}
            Err(error) => {
                return mapped_error_response("storage diagnostic failed", error.into(), ctx)
            }
        }
    }
    json_ok(&envelope(&status, ctx.user_id.as_str()))
}

pub(in crate::exec) async fn execute_db_delete_ledger_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let limit = query_value(&req.target, "limit")
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0) as usize;
    match host.db.list_record_delete_ledger(limit).await {
        Ok(entries) => json_ok(&envelope(
            &json!({ "delete_ledger": { "count": entries.len(), "entries": entries } }),
            ctx.user_id.as_str(),
        )),
        Err(e) => error_response(500, &format!("delete-ledger failed: {e}"), ctx),
    }
}

#[derive(Deserialize)]
struct RetentionBody {
    action: String,
    schema: String,
    #[serde(default)]
    ttl_seconds: Option<u64>,
    #[serde(default)]
    hash_partitions: Vec<String>,
    #[serde(default)]
    confirm_expire_existing: bool,
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

fn clear_may_expire(
    body: &RetentionBody,
    previous: &Option<SchemaRetentionPolicy>,
) -> Result<bool, String> {
    if body.action != "clear" {
        return Ok(false);
    }
    let (Some(operator), Some(restored)) = (
        previous.as_ref(),
        SchemaRetentionPolicy::after_operator_clear(previous.as_ref()),
    ) else {
        return Ok(false);
    };
    if restored.ttl_seconds >= operator.ttl_seconds {
        return Ok(false);
    }
    if body.confirm_expire_existing {
        return Ok(true);
    }
    Err(format!(
        "clearing the operator policy of {}s puts the manifest TTL of {}s in force. existing_records_may_expire: every record older than {}s is deleted for good, a Range schema's at the next sweep. Review it, then run `lastdb admin retention clear --schema {} --confirm-expire-existing`",
        operator.ttl_seconds, restored.ttl_seconds, restored.ttl_seconds, body.schema
    ))
}

async fn run_retention_backfill_or_partitions(
    body: &RetentionBody,
    ctx: &AccessContext,
    host: &Host,
) -> Option<UdsResponse> {
    if body.action == "backfill" {
        if !body.confirm_expire_existing {
            return Some(error_response(
                409,
                "retention backfill can expire existing records; confirm_expire_existing is required",
                ctx,
            ));
        }
        return Some(
            match host
                .db
                .backfill_schema_retention_page(
                    &body.schema,
                    body.cursor.as_deref(),
                    body.limit.unwrap_or(16),
                )
                .await
            {
                Ok(value) => json_ok(&envelope(&value, ctx.user_id.as_str())),
                Err(e) => error_response(400, &format!("retention backfill failed: {e}"), ctx),
            },
        );
    }
    if body.action == "partitions" {
        return Some(
            match host
                .db
                .db_ops()
                .schemas()
                .schema_retention_hash_partitions_page(
                    &body.schema,
                    body.cursor.as_deref(),
                    body.limit.unwrap_or(256),
                )
                .await
            {
                Ok((partitions, next_cursor)) => json_ok(&envelope(
                    &json!({
                        "schema": body.schema,
                        "observed_hash_partitions": partitions,
                        "next_cursor": next_cursor,
                    }),
                    ctx.user_id.as_str(),
                )),
                Err(e) => error_response(400, &format!("schema retention failed: {e}"), ctx),
            },
        );
    }
    None
}

pub(in crate::exec) async fn execute_db_schema_retention_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    if !ctx.is_owner {
        return error_response(403, "schema retention is owner-only", ctx);
    }
    let body: RetentionBody = match serde_json::from_slice(&req.body) {
        Ok(body) => body,
        Err(e) => {
            return error_response(
                400,
                &format!("invalid /api/db/schema-retention body: {e}"),
                ctx,
            )
        }
    };
    if body.schema.is_empty() {
        return error_response(400, "schema is required", ctx);
    }
    let store = host.db.db_ops().schemas();
    let previous = if body.action == "backfill" {
        None
    } else {
        match store.get_schema_retention_policy(&body.schema).await {
            Ok(prev) => prev,
            Err(e) => return error_response(400, &format!("schema retention failed: {e}"), ctx),
        }
    };
    if let Some(resp) = run_retention_backfill_or_partitions(&body, ctx, host).await {
        return resp;
    }
    let may_expire = match clear_may_expire(&body, &previous) {
        Ok(v) => v,
        Err(msg) => return error_response(409, &msg, ctx),
    };
    let result = match body.action.as_str() {
        "get" => Ok(previous),
        "set" => {
            let Some(ttl) = body.ttl_seconds else {
                return error_response(400, "ttl_seconds is required for set", ctx);
            };
            let policy = SchemaRetentionPolicy::operator(ttl, body.hash_partitions.clone());
            match store
                .set_schema_retention_policy(&body.schema, policy)
                .await
            {
                Ok(()) => store.get_schema_retention_policy(&body.schema).await,
                Err(e) => Err(e),
            }
        }
        "clear" => match store.clear_schema_retention_policy(&body.schema).await {
            Ok(()) => store.get_schema_retention_policy(&body.schema).await,
            Err(e) => Err(e),
        },
        _ => return error_response(400, "action must be get, set, or clear", ctx),
    };
    match result {
        Ok(policy) => json_ok(&envelope(
            &json!({
                "schema": body.schema, "retention_policy": policy,
                "anchor": body.schema, "existing_records_may_expire": may_expire,
            }),
            ctx.user_id.as_str(),
        )),
        Err(e) => error_response(400, &format!("schema retention failed: {e}"), ctx),
    }
}
