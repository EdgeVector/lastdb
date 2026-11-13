//! Data-route executor, admin/status routes, and wall-clock timeout budgets for `lastdbd`.

use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;

use base64::Engine as _;
use fold_db::access::AccessContext;
use fold_db::schema::types::Schema;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};
use crate::lastdb_uds::uds_router::DataRoute;

type ErasedRoute<'a> = std::pin::Pin<Box<dyn Future<Output = UdsResponse> + Send + 'a>>;

macro_rules! heap_route {
    ($f:path, $req:ident, $ctx:ident, $host:ident) => {{
        #[inline(never)]
        fn start<'a>(
            req: &'a UdsRequest,
            ctx: &'a AccessContext,
            host: &'a Host,
        ) -> ErasedRoute<'a> {
            Box::pin($f(req, ctx, host))
        }
        start($req, $ctx, $host)
    }};
    ($f:path, $ctx:ident, $host:ident) => {{
        #[inline(never)]
        fn start<'a>(ctx: &'a AccessContext, host: &'a Host) -> ErasedRoute<'a> {
            Box::pin($f(ctx, host))
        }
        start($ctx, $host)
    }};
}

mod database_mutation;
mod database_owned;
mod database_read;
mod database_routes;
mod database_session;
mod database_share;
mod database_signed;
mod file_blob;
mod mutation;
mod query;
mod reply;
mod schema_declare;
mod schema_routes;
mod wire;

mod list_keys_route {
    pub(in crate::exec) use super::reply::{
        alternate_list_report, decode_alternate_cursor, ListRecordKeysRequest,
    };
}

mod schema_resolve {
    pub(in crate::exec) use super::schema_routes::{
        resolve_captured_schema_alias, resolve_schema_name,
    };
}

use reply::{content_free, envelope, error_response, json_ok, map_host_err, render, HostError};
use wire::{body_object, parse_query_body, query_flag, schema_name_from_target, QueryRequest};

pub use database_routes::{
    execute_database_request, execute_deferred_database_request, is_deferred_database_request,
};
pub use database_session::admit as admit_database_session;
pub(crate) use database_session::DatabaseSessions;
pub use mutation::execute_deferred_mutation;
pub use schema_declare::{execute_apps_declare_schema_route, normalize_app_manifest_schema};
pub use wire::{parse_mutation_body, ParsedMutation, Reject};

pub const DEFAULT_CLI_UDS_TIMEOUT_SECS: u64 = 30;

fn secs_override(var: &str) -> Option<Duration> {
    fold_db::env_flag::var_parsed::<u64>(var)
        .filter(|&n| n > 0)
        .map(Duration::from_secs)
}

#[must_use]
pub fn handler_timeout() -> Duration {
    secs_override("LASTDB_UDS_HANDLER_TIMEOUT_SECS").unwrap_or(Duration::from_secs(90))
}

#[must_use]
pub fn admin_handler_timeout_override() -> Option<Duration> {
    secs_override("LASTDB_UDS_ADMIN_TIMEOUT_SECS")
}

#[must_use]
pub fn admin_handler_timeout() -> Duration {
    admin_handler_timeout_override().unwrap_or(Duration::from_secs(600))
}

pub(in crate::exec) async fn with_handler_timeout_budget<F: Future<Output = UdsResponse>>(
    ctx: &AccessContext,
    budget: Duration,
    fut: F,
) -> UdsResponse {
    tokio::time::timeout(budget, fut).await.unwrap_or_else(|_| {
        tracing::warn!(
            timeout_secs = budget.as_secs(),
            "uds handler deadline exceeded; returning 503"
        );
        error_response(
            503,
            "node is busy: handler deadline exceeded; retry after 1s",
            ctx,
        )
    })
}

pub fn block_on_route<Fut: Future<Output = UdsResponse>>(
    handle: &tokio::runtime::Handle,
    ctx: &AccessContext,
    admin: bool,
    fut: Fut,
) -> UdsResponse {
    let budget = if admin {
        admin_handler_timeout()
    } else {
        handler_timeout()
    };
    handle.block_on(with_handler_timeout_budget(ctx, budget, fut))
}

pub fn block_on_data_route(
    handle: &tokio::runtime::Handle,
    route: DataRoute,
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    handle.block_on(execute_data_route_with_timeout(route, req, ctx, host))
}

pub(in crate::exec) async fn execute_data_route_with_timeout(
    route: DataRoute,
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    with_handler_timeout_budget(ctx, data_route_budget(route), async {
        let fut: ErasedRoute<'_> = match route {
            DataRoute::ListSchemas => return schema_routes::list_schemas(req, ctx, host),
            DataRoute::GetSchema => return schema_routes::get_schema(req, ctx, host),
            DataRoute::Status => {
                let up = fold_db::clock::unix_secs().saturating_sub(host.process_start_ts);
                let uds = host
                    .uds_workers
                    .get()
                    .map(crate::lastdb_uds::UdsWorkerPool::snapshot);
                let st = json!({
                    "build": { "version": crate::version::build_version() },
                    "process_start_ts": host.process_start_ts, "uptime_secs": up,
                    "uds": uds, "search": search_queue_status(host),
                });
                return json_ok(&envelope(&json!({ "status": st }), &ctx.user_id));
            }
            DataRoute::AutoIdentity => {
                let pk = host.public_key();
                return json_ok(
                    &json!({ "user_id": pk, "user_hash": host.user_hash, "public_key": pk }),
                );
            }
            DataRoute::BootIdentity => {
                return json_ok(&json!({
                    "pid": std::process::id(), "process_start_ts": host.process_start_ts,
                    "build": crate::version::build_version(), "restart_cause": "clean",
                }));
            }
            DataRoute::Query => heap_route!(query::query_route, req, ctx, host),
            DataRoute::Mutation => heap_route!(mutation::mutation_route, req, ctx, host),
            DataRoute::MutationBatch => heap_route!(mutation::batch_route, req, ctx, host),
            DataRoute::QueryBatch => heap_route!(query::query_batch_route, req, ctx, host),
            DataRoute::ListRecordKeys => heap_route!(reply::list_keys, req, ctx, host),
            DataRoute::DeclareSchema => heap_route!(schema_declare::declare_schema, req, ctx, host),
            DataRoute::SeedSystemSchema => heap_route!(mutation::seed_system, req, ctx, host),
            DataRoute::DropSchema => heap_route!(schema_routes::drop_schema, req, ctx, host),
            DataRoute::AdminShed => heap_route!(admin_shed, ctx, host),
            DataRoute::DbSchemas => heap_route!(db_schemas, ctx, host),
            DataRoute::DbFlush => heap_route!(db_flush, req, ctx, host),
            DataRoute::DbCleanup => heap_route!(db_cleanup, req, ctx, host),
            DataRoute::DbNativeStatus => heap_route!(db_native_status, req, ctx, host),
            DataRoute::DbDeleteLedger => heap_route!(db_delete_ledger, req, ctx, host),
            DataRoute::DbSchemaRetention => heap_route!(mutation::db_retention, req, ctx, host),
            DataRoute::DbPutBlob => heap_route!(file_blob::db_put_blob, req, ctx, host),
            DataRoute::DbFetchBlob => heap_route!(file_blob::db_fetch_blob, req, ctx, host),
            DataRoute::DbDeleteBlob => heap_route!(file_blob::db_delete_blob, req, ctx, host),
        };
        fut.await
    })
    .await
}

#[must_use]
pub(in crate::exec) fn data_route_budget(route: DataRoute) -> Duration {
    match route {
        DataRoute::DbSchemas
        | DataRoute::DbDeleteLedger
        | DataRoute::DeclareSchema
        | DataRoute::DropSchema => admin_handler_timeout(),
        DataRoute::Status => Duration::from_secs(2),
        DataRoute::AdminShed => Duration::from_secs(60),
        DataRoute::BootIdentity => Duration::from_secs(1),
        _ => handler_timeout(),
    }
}

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

async fn admin_shed(ctx: &AccessContext, host: &Host) -> UdsResponse {
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

async fn db_schemas(ctx: &AccessContext, host: &Host) -> UdsResponse {
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

async fn db_cleanup(req: &UdsRequest, ctx: &AccessContext, host: &Host) -> UdsResponse {
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

async fn db_flush(req: &UdsRequest, ctx: &AccessContext, host: &Host) -> UdsResponse {
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

async fn db_native_status(req: &UdsRequest, ctx: &AccessContext, host: &Host) -> UdsResponse {
    #[derive(Deserialize)]
    struct Opts {
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

async fn db_delete_ledger(req: &UdsRequest, ctx: &AccessContext, host: &Host) -> UdsResponse {
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
