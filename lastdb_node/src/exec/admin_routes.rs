//! Status, identity, shed, schema-storage, native flush/cleanup, delete-ledger, and retention routes.

use fold_db::access::AccessContext;
use fold_db::schema::SchemaRetentionPolicy;
use serde::Deserialize;
use serde_json::{json, Value};

use super::reply::{envelope, json_ok, map_host_err, render, HostError};
use super::wire::query_value;
use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};

fn search_queue_status(host: &Host) -> Value {
    fold_db::db_operations::search_index::queue_policy::status(
        &fold_db::db_operations::search_index::search_inbox_dir_for_home(&host.home),
    )
}

fn require_owner(ctx: &AccessContext, msg: &'static str) -> Result<(), HostError> {
    ctx.is_owner
        .then_some(())
        .ok_or_else(|| HostError::new(403, msg))
}

pub(in crate::exec) fn status_route(ctx: &AccessContext, host: &Host) -> UdsResponse {
    let uptime_secs = fold_db::clock::unix_secs().saturating_sub(host.process_start_ts);
    let uds = host
        .uds_workers
        .get()
        .map(crate::lastdb_uds::UdsWorkerPool::snapshot);
    let status = json!({
        "build": { "version": crate::version::build_version() },
        "process_start_ts": host.process_start_ts, "uptime_secs": uptime_secs,
        "uds": uds, "search": search_queue_status(host),
    });
    json_ok(&envelope(&json!({ "status": status }), &ctx.user_id))
}

pub(in crate::exec) fn auto_identity(host: &Host) -> UdsResponse {
    let pk = host.public_key();
    json_ok(&json!({ "user_id": pk, "user_hash": host.user_hash, "public_key": pk }))
}

pub(in crate::exec) fn boot_identity(host: &Host) -> UdsResponse {
    json_ok(&json!({
        "pid": std::process::id(), "process_start_ts": host.process_start_ts,
        "build": crate::version::build_version(), "restart_cause": "clean",
    }))
}

pub(in crate::exec) async fn admin_shed(ctx: &AccessContext, host: &Host) -> UdsResponse {
    let rt = host.db.db_ops().native_runtime();
    let flush_err = rt.flush_native().await.err().map(|e| e.to_string());
    let (met, purged, purge_err) = match rt.purge_clean_memory(0) {
        Ok(r) => (flush_err.is_none() && r.target_met, r.records_purged, None),
        Err(e) => (false, 0, Some(e.to_string())),
    };
    json_ok(&envelope(
        &json!({ "shed": { "target_met": met, "records_purged": purged, "error": flush_err.or(purge_err) } }),
        &ctx.user_id,
    ))
}

pub(in crate::exec) async fn db_schemas(ctx: &AccessContext, host: &Host) -> UdsResponse {
    render(
        host.db
            .db_ops()
            .schema_storage_report()
            .await
            .map(|r| json!({ "schema_storage": r }))
            .map_err(|e| HostError::new(500, format!("db schemas failed: {e}"))),
        ctx,
    )
}

pub(in crate::exec) async fn db_cleanup(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Default, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Body {
        request_id: Option<uuid::Uuid>,
    }
    let res = async {
        require_owner(ctx, "native cleanup is owner-only")?;
        let rt = host.db.db_ops().native_runtime();
        if req.method == "GET" {
            return Ok(json!({ "cleanup": rt.status_cleanup() }));
        }
        let body = if req.body.is_empty() {
            Body::default()
        } else {
            serde_json::from_slice(&req.body)
                .map_err(|e| HostError::new(400, format!("invalid cleanup request: {e}")))?
        };
        let status = rt
            .request_native_cleanup(body.request_id)
            .await
            .map_err(|e| map_host_err("native cleanup request failed", e.into()))?;
        Ok(json!({ "cleanup": status }))
    }
    .await;
    render(res, ctx)
}

pub(in crate::exec) async fn db_flush(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let res = async {
        require_owner(ctx, "the local durability barrier is owner-only")?;
        if !req.body.is_empty()
            && !serde_json::from_slice::<serde_json::Map<String, Value>>(&req.body)
                .is_ok_and(|f| f.is_empty())
        {
            return Err(HostError::new(
                400,
                "local flush body must be an empty object",
            ));
        }
        let r = host
            .db
            .db_ops()
            .native_runtime()
            .flush_native()
            .await
            .map_err(|e| HostError::new(503, format!("local flush failed: {e}")))?;
        Ok(json!({ "durable": true, "native_flush": r }))
    }
    .await;
    render(res, ctx)
}

pub(in crate::exec) async fn db_native_status(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    struct Options {
        #[serde(default)]
        inventory: bool,
    }
    let res = async {
        require_owner(ctx, "the native status route is owner-only")?;
        let rt = host.db.db_ops().native_runtime();
        let mut status =
            json!({ "native_disk": rt.status_native(), "search": search_queue_status(host) });
        if laststore::diagnostics_enabled() {
            let inv = !req.body.is_empty()
                && serde_json::from_slice::<Options>(&req.body)
                    .map_err(|_| HostError::new(400, "invalid native-status diagnostic options"))?
                    .inventory;
            if let Some(r) = rt
                .diagnostics_native(inv)
                .await
                .map_err(|e| map_host_err("storage diagnostic failed", e.into()))?
            {
                status["storage_diagnostics"] = json!(r);
            }
        }
        Ok(status)
    }
    .await;
    render(res, ctx)
}

pub(in crate::exec) async fn db_delete_ledger(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let limit = query_value(&req.target, "limit")
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0) as usize;
    render(
        host.db
            .db_ops()
            .records()
            .list_record_delete_ledger(limit)
            .await
            .map(|e| json!({ "delete_ledger": { "count": e.len(), "entries": e } }))
            .map_err(|e| HostError::new(500, format!("delete-ledger failed: {e}"))),
        ctx,
    )
}

#[derive(Deserialize)]
struct RetentionBody {
    action: String,
    schema: String,
    ttl_seconds: Option<u64>,
    #[serde(default)]
    hash_partitions: Vec<String>,
    #[serde(default)]
    confirm_expire_existing: bool,
    cursor: Option<String>,
    limit: Option<usize>,
}

pub(in crate::exec) async fn db_retention(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let res = async {
        require_owner(ctx, "schema retention is owner-only")?;
        let b: RetentionBody = serde_json::from_slice(&req.body).map_err(|e| {
            HostError::new(400, format!("invalid /api/db/schema-retention body: {e}"))
        })?;
        if b.schema.is_empty() {
            return Err(HostError::new(400, "schema is required"));
        }
        let store = host.db.db_ops().schemas();
        let ret_err = |e| HostError::new(400, format!("schema retention failed: {e}"));
        if b.action == "partitions" {
            let (p, next) = store
                .schema_retention_hash_partitions_page(
                    &b.schema,
                    b.cursor.as_deref(),
                    b.limit.unwrap_or(256),
                )
                .await
                .map_err(ret_err)?;
            return Ok(json!({ "schema": b.schema, "observed_hash_partitions": p, "next_cursor": next }));
        }
        let prev = store.get_schema_retention_policy(&b.schema).await.map_err(ret_err)?;
        let mut may_expire = false;
        if b.action == "clear" {
            if let (Some(op), Some(res)) = (prev.as_ref(), SchemaRetentionPolicy::after_operator_clear(prev.as_ref())) {
                if res.ttl_seconds < op.ttl_seconds {
                    if !b.confirm_expire_existing {
                        return Err(HostError::new(
                            409,
                            format!(
                                "clearing the operator policy of {}s puts the manifest TTL of {}s in force. existing_records_may_expire: every record older than {}s is deleted for good, a Range schema's at the next sweep.",
                                op.ttl_seconds, res.ttl_seconds, res.ttl_seconds
                            ),
                        ));
                    }
                    may_expire = true;
                }
            }
        }
        let policy = if b.action == "get" {
            prev
        } else {
            match b.action.as_str() {
                "set" => store
                    .set_schema_retention_policy(
                        &b.schema,
                        SchemaRetentionPolicy::operator(
                            b.ttl_seconds.ok_or_else(|| {
                                HostError::new(400, "ttl_seconds is required for set")
                            })?,
                            b.hash_partitions,
                        ),
                    )
                    .await
                    .map_err(ret_err)?,
                "clear" => store
                    .clear_schema_retention_policy(&b.schema)
                    .await
                    .map_err(ret_err)?,
                _ => return Err(HostError::new(400, "action must be get, set, or clear")),
            }
            store
                .get_schema_retention_policy(&b.schema)
                .await
                .map_err(ret_err)?
        };
        Ok(json!({
            "schema": b.schema, "retention_policy": policy,
            "anchor": b.schema, "existing_records_may_expire": may_expire,
        }))
    }
    .await;
    render(res, ctx)
}
