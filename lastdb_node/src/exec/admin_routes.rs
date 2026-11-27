//! Owner-only admin, storage maintenance, schema-retention, and isolated system-seed routes.

use fold_db::access::AccessContext;
use fold_db::schema::types::DeclarativeSchemaDefinition;
use fold_db::schema::SchemaRetentionPolicy;
use serde::Deserialize;
use serde_json::{json, Value};

use super::reply::{envelope, json_ok, map_host_err, render, HostError};
use super::wire;
use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};

fn require_owner(ctx: &AccessContext, msg: &'static str) -> Result<(), HostError> {
    ctx.is_owner
        .then_some(())
        .ok_or_else(|| HostError::new(403, msg))
}

fn require_empty_json_body(body: &[u8], msg: &'static str) -> Result<(), HostError> {
    if body.is_empty()
        || serde_json::from_slice::<serde_json::Map<String, Value>>(body)
            .is_ok_and(|f| f.is_empty())
    {
        Ok(())
    } else {
        Err(HostError::new(400, msg))
    }
}

pub(super) async fn admin_shed(ctx: &AccessContext, host: &Host) -> UdsResponse {
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

pub(super) async fn db_schemas(ctx: &AccessContext, host: &Host) -> UdsResponse {
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

pub(super) async fn db_cleanup(req: &UdsRequest, ctx: &AccessContext, host: &Host) -> UdsResponse {
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
        let b = if req.body.is_empty() {
            Body::default()
        } else {
            serde_json::from_slice(&req.body)
                .map_err(|e| HostError::new(400, format!("invalid cleanup request: {e}")))?
        };
        let st = rt
            .request_native_cleanup(b.request_id)
            .await
            .map_err(|e| map_host_err("native cleanup request failed", e.into()))?;
        Ok(json!({ "cleanup": st }))
    }
    .await;
    render(res, ctx)
}

pub(super) async fn db_flush(req: &UdsRequest, ctx: &AccessContext, host: &Host) -> UdsResponse {
    let res = async {
        require_owner(ctx, "the local durability barrier is owner-only")?;
        require_empty_json_body(&req.body, "local flush body must be an empty object")?;
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

pub(super) async fn db_native_status(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    struct Opts {
        #[serde(default)]
        inventory: bool,
    }
    let res = async {
        require_owner(ctx, "the native status route is owner-only")?;
        let rt = host.db.db_ops().native_runtime();
        let mut status = json!({ "native_disk": rt.status_native() });
        if std::env::var("LASTDB_SNAPSHOT_TIMING")
            .is_ok_and(|v| matches!(v.trim(), "1" | "true" | "on"))
        {
            let inv = !req.body.is_empty()
                && serde_json::from_slice::<Opts>(&req.body)
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

pub(super) async fn db_delete_ledger(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let limit = wire::query_value(&req.target, "limit")
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

pub(super) async fn db_retention(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    struct Body {
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
    let res = async {
        require_owner(ctx, "schema retention is owner-only")?;
        let b: Body = serde_json::from_slice(&req.body).map_err(|e| {
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
        let prev = store
            .get_schema_retention_policy(&b.schema)
            .await
            .map_err(ret_err)?;
        let mut may_expire = false;
        if b.action == "clear" {
            if let (Some(op), Some(res)) = (
                prev.as_ref(),
                SchemaRetentionPolicy::after_operator_clear(prev.as_ref()),
            ) {
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

pub(super) async fn seed_system(req: &UdsRequest, ctx: &AccessContext, host: &Host) -> UdsResponse {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Req {
        schema: DeclarativeSchemaDefinition,
        #[serde(default)]
        isolated_copy: bool,
    }
    let res = async {
        let r: Req = serde_json::from_slice(&req.body)
            .map_err(|e| HostError::new(400, format!("invalid system seed setup request: {e}")))?;
        if !r.isolated_copy || !fold_db::env_flag::var_truthy("LASTDB_ISOLATED_COPY") {
            return Err(HostError::new(
                409,
                "system seed setup requires isolated_copy=true and LASTDB_ISOLATED_COPY=1",
            ));
        }
        if r.schema
            .owner_app_id
            .as_deref()
            .is_some_and(|o| !o.trim().is_empty())
        {
            return Err(HostError::new(
                400,
                "system seed setup requires a schema without owner_app_id",
            ));
        }
        fold_db::schema::require_logical_layout(&r.schema.schema_type)
            .map_err(|e| HostError::new(400, e.to_string()))?;
        r.schema
            .validate()
            .map_err(|e| map_host_err("system seed setup failed", e.into()))?;
        let name = r.schema.name.clone();
        host.records()
            .schema_manager()
            .load_schema_internal(r.schema)
            .await
            .map_err(|e| map_host_err("system seed setup failed", e.into()))?;
        Ok(json!({ "system_seed_schema": name, "source": "system_seed" }))
    }
    .await;
    render(res, ctx)
}

/// `POST /api/backup/snapshot {"database": "personal"}` asks for a snapshot of the
/// personal database now and returns the nonce of the request. `GET` with that
/// nonce reports whether it is done. With `"reclaim": true` the request is for a
/// sweep instead (`lastdb backup reclaim`): the relay drops every stored file that
/// the newest snapshot and the entries after it do not name. `GET` with
/// `reclaim=<nonce>` reports on that request.
pub(super) async fn backup_snapshot(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Body {
        #[serde(default)]
        database: Option<String>,
        #[serde(default)]
        reclaim: bool,
    }
    let res = async {
        require_owner(ctx, "backup snapshot is owner-only")?;
        if req.method == "GET" {
            if let Some(nonce) = wire::query_value(&req.target, "reclaim") {
                if nonce.is_empty() {
                    return Err(HostError::new(
                        400,
                        "the reclaim query parameter is required",
                    ));
                }
                return crate::log_sync::reclaim_status(host, &nonce)
                    .map_err(|e| HostError::new(503, e));
            }
            let nonce = wire::query_value(&req.target, "nonce").unwrap_or_default();
            if nonce.is_empty() {
                return Err(HostError::new(400, "the nonce query parameter is required"));
            }
            return crate::log_sync::snapshot_status(host, &nonce)
                .map_err(|e| HostError::new(503, e));
        }
        let body: Body = serde_json::from_slice(&req.body)
            .map_err(|e| HostError::new(400, format!("invalid /api/backup/snapshot body: {e}")))?;
        let asked = if body.reclaim {
            crate::log_sync::request_reclaim(host, body.database.as_deref())
        } else {
            crate::log_sync::request_snapshot(host, body.database.as_deref())
        };
        let (id, nonce) = asked.map_err(|e| {
            let status = if e == "database is not owned by this node" {
                404
            } else {
                503
            };
            HostError::new(status, e)
        })?;
        crate::log_sync::wake_loop();
        host.cloud_sync.wake();
        Ok(json!({ "database_id": id, "nonce": nonce }))
    }
    .await;
    render(res, ctx)
}

pub(super) fn cloud_wake(req: &UdsRequest, ctx: &AccessContext, host: &Host) -> UdsResponse {
    let res = (|| {
        require_owner(ctx, "cloud wake is owner-only")?;
        require_empty_json_body(
            &req.body,
            "invalid /api/cloud/wake body: expected an empty JSON object",
        )?;
        crate::log_sync::wake_loop();
        host.cloud_sync.wake();
        Ok(json!({}))
    })();
    render(res, ctx)
}

/// The body of `POST /api/system/prepare-stop`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PrepareStopRequest {
    #[serde(default = "default_upload")]
    upload: bool,
    #[serde(default = "default_deadline_ms")]
    deadline_ms: u64,
    #[serde(default = "default_hold_ms")]
    hold_ms: u64,
}

const fn default_upload() -> bool {
    true
}
const fn default_deadline_ms() -> u64 {
    120_000
}
const fn default_hold_ms() -> u64 {
    300_000
}

/// The state a prepare-stop answer reports: the spool head and the acknowledged position of the
/// personal database, and what the cloud holds.
fn prepare_stop_state(host: &Host, detail: &str) -> UdsResponse {
    let status = host.db.db_ops().native_runtime().accepted_status();
    let cloud = if crate::cloud::relay_base_url(&host.home).is_none() {
        "off"
    } else if status.acked >= status.head {
        "current"
    } else {
        "behind"
    };
    json_ok(&json!({
        "head": status.head, "acked": status.acked, "cloud": cloud, "detail": detail,
    }))
}

/// `POST /api/system/prepare-stop`: refuse new writes, then send the spool to the cloud.
pub(super) async fn prepare_stop(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    use std::time::{Duration, Instant};

    use crate::prepare_stop as hold;

    let parsed: Result<PrepareStopRequest, HostError> = (|| {
        require_owner(ctx, "prepare-stop is owner-only")?;
        let request: PrepareStopRequest = if req.body.is_empty() {
            PrepareStopRequest {
                upload: default_upload(),
                deadline_ms: default_deadline_ms(),
                hold_ms: default_hold_ms(),
            }
        } else {
            serde_json::from_slice(&req.body).map_err(|e| {
                HostError::new(
                    400,
                    format!(
                        "invalid /api/system/prepare-stop body: expected {{upload, deadline_ms, hold_ms}}: {e}"
                    ),
                )
            })?
        };
        if request.hold_ms == 0 || request.hold_ms > hold::MAX_HOLD_MS {
            return Err(HostError::new(
                400,
                format!("hold_ms must be from 1 to {}", hold::MAX_HOLD_MS),
            ));
        }
        if request.deadline_ms > hold::MAX_DEADLINE_MS {
            return Err(HostError::new(
                400,
                format!("deadline_ms must be at most {}", hold::MAX_DEADLINE_MS),
            ));
        }
        Ok(request)
    })();
    let request = match parsed {
        Ok(request) => request,
        Err(error) => return render(Err(error), ctx),
    };
    let began = Instant::now();
    let deadline = Duration::from_millis(request.deadline_ms);
    if hold::begin(Duration::from_millis(request.hold_ms)) {
        // A hold is set: answer with the state now. The first call keeps its own upload.
        return prepare_stop_state(host, &hold::detail());
    }
    tracing::info!(
        target: "lastdbd::prepare_stop",
        upload = request.upload,
        deadline_ms = request.deadline_ms,
        hold_ms = request.hold_ms,
        "prepare-stop: new writes are refused"
    );
    let in_flight = hold::wait_idle(deadline).await;
    let detail = if in_flight > 0 {
        format!("{in_flight} writes were still running when the deadline passed")
    } else if request.upload {
        let left = deadline.saturating_sub(began.elapsed());
        match tokio::time::timeout(left, crate::log_sync::send_spool(host)).await {
            Ok(Ok(())) => "uploaded the spool".to_owned(),
            Ok(Err(error)) => error,
            Err(_) => "the deadline passed before the upload finished".to_owned(),
        }
    } else {
        "upload was not asked for".to_owned()
    };
    hold::set_detail(&detail);
    prepare_stop_state(host, &detail)
}

/// `POST /api/system/prepare-stop/cancel`: accept writes again.
pub(super) fn prepare_stop_cancel(req: &UdsRequest, ctx: &AccessContext) -> UdsResponse {
    let res: Result<(), HostError> = (|| {
        require_owner(ctx, "prepare-stop is owner-only")?;
        require_empty_json_body(
            &req.body,
            "invalid /api/system/prepare-stop/cancel body: expected an empty JSON object",
        )?;
        Ok(())
    })();
    if let Err(error) = res {
        return render(Err(error), ctx);
    }
    let was = crate::prepare_stop::cancel();
    tracing::info!(target: "lastdbd::prepare_stop", was_holding = was, "prepare-stop cancelled");
    json_ok(&json!({ "holding": false, "was_holding": was }))
}
