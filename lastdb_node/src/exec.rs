//! Data-route dispatch table and wall-clock timeout wrapper for `lastdbd`.

use std::future::Future;
use std::time::Duration;

use base64::Engine as _;
use fold_db::access::AccessContext;
use serde::Deserialize;
use serde_json::{json, Map, Value};

use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};
use crate::lastdb_uds::uds_router::DataRoute;
use crate::lastdb_uds::{admin_handler_timeout, handler_timeout};

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

mod admin_routes;
mod app_api;
mod changes;
mod database_owned;
mod database_routes;
mod database_session;
mod database_share;
mod file_blob;
mod list_keys;
mod mutation;
mod query;
mod reply;
mod schema_declare;
mod schema_routes;
mod uds_server;
mod wire;

use reply::{content_free, envelope, error_response, json_ok, render, HostError};

pub use schema_declare::normalize_app_manifest_schema;
pub use uds_server::UdsServer;
pub use wire::{parse_mutation_body, ParsedMutation, Reject};

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

pub(in crate::exec) fn block_on_route<Fut: Future<Output = UdsResponse>>(
    handle: &tokio::runtime::Handle,
    ctx: &AccessContext,
    fut: Fut,
) -> UdsResponse {
    handle.block_on(with_handler_timeout_budget(ctx, handler_timeout(), fut))
}

pub(in crate::exec) fn block_on_data_route(
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
                    "uds": uds, "search": admin_routes::search_queue_status(host),
                    "cloud_sync": host.cloud_sync.snapshot(),
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
            DataRoute::ListRecordKeys => heap_route!(list_keys::list_keys, req, ctx, host),
            DataRoute::Changes => heap_route!(changes::changes, req, ctx, host),
            DataRoute::DeclareSchema => heap_route!(schema_declare::declare_schema, req, ctx, host),
            DataRoute::AppsDeclareSchema => {
                heap_route!(
                    schema_declare::execute_apps_declare_schema_route,
                    req,
                    ctx,
                    host
                )
            }
            DataRoute::SeedSystemSchema => heap_route!(admin_routes::seed_system, req, ctx, host),
            DataRoute::DropSchema => heap_route!(schema_routes::drop_schema, req, ctx, host),
            DataRoute::AdminShed => heap_route!(admin_routes::admin_shed, ctx, host),
            DataRoute::DbSchemas => heap_route!(admin_routes::db_schemas, ctx, host),
            DataRoute::DbFlush => heap_route!(admin_routes::db_flush, req, ctx, host),
            DataRoute::DbCleanup => heap_route!(admin_routes::db_cleanup, req, ctx, host),
            DataRoute::DbNativeStatus => {
                heap_route!(admin_routes::db_native_status, req, ctx, host)
            }
            DataRoute::DbDeleteLedger => {
                heap_route!(admin_routes::db_delete_ledger, req, ctx, host)
            }
            DataRoute::DbSchemaRetention => heap_route!(admin_routes::db_retention, req, ctx, host),
            DataRoute::DbPutBlob => heap_route!(file_blob::db_put_blob, req, ctx, host),
            DataRoute::DbFetchBlob => heap_route!(file_blob::db_fetch_blob, req, ctx, host),
            DataRoute::DbDeleteBlob => heap_route!(file_blob::db_delete_blob, req, ctx, host),
            DataRoute::BackupSnapshot => heap_route!(backup_snapshot, req, ctx, host),
            DataRoute::CloudWake => return cloud_wake(req, ctx, host),
            DataRoute::V1Read => heap_route!(app_api::read_route, req, ctx, host),
            DataRoute::V1Write => heap_route!(app_api::write_route, req, ctx, host),
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
        | DataRoute::AppsDeclareSchema
        | DataRoute::DropSchema
        | DataRoute::BackupSnapshot
        | DataRoute::CloudWake => admin_handler_timeout(),
        DataRoute::Status => Duration::from_secs(2),
        DataRoute::AdminShed => Duration::from_secs(60),
        DataRoute::BootIdentity => Duration::from_secs(1),
        _ => handler_timeout(),
    }
}

async fn backup_snapshot(req: &UdsRequest, ctx: &AccessContext, host: &Host) -> UdsResponse {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Body {
        database: String,
    }
    let res = async {
        if !ctx.is_owner {
            return Err(HostError::new(403, "backup snapshot is owner-only"));
        }
        if req.method == "GET" {
            let db = wire::query_value(&req.target, "database").unwrap_or_default();
            let nonce = wire::query_value(&req.target, "nonce").unwrap_or_default();
            if db.is_empty() || nonce.is_empty() {
                return Err(HostError::new(
                    400,
                    "database and nonce query parameters are required",
                ));
            }
            cloud_backup_protocol::Id::try_from(db.clone())
                .map_err(|e| HostError::new(400, e.to_string()))?;
            if let Some((through, entries)) =
                crate::log_sync::snapshot_completed(&host.home, &db, &nonce)
            {
                return Ok(json!({
                    "ready": true,
                    "snapshot_through": through,
                    "log_head": through,
                    "log_entries": entries,
                }));
            }
            return Ok(match crate::log_sync::snapshot_error(&host.home, &db) {
                Some(error) => json!({ "ready": false, "error": error }),
                None => json!({ "ready": false }),
            });
        }
        let b: Body = serde_json::from_slice(&req.body)
            .map_err(|e| HostError::new(400, format!("invalid /api/backup/snapshot body: {e}")))?;
        let (id, nonce) =
            crate::log_sync::request_snapshot(&host.home, &b.database).map_err(|e| {
                let status = if e == "database is not owned by this node" {
                    404
                } else if e.starts_with("cannot ") {
                    503
                } else {
                    400
                };
                HostError::new(status, e)
            })?;
        host.cloud_sync.wake();
        Ok(json!({ "database_id": id, "nonce": nonce }))
    }
    .await;
    render(res, ctx)
}

fn cloud_wake(req: &UdsRequest, ctx: &AccessContext, host: &Host) -> UdsResponse {
    let res = (|| {
        if !ctx.is_owner {
            return Err(HostError::new(403, "cloud wake is owner-only"));
        }
        if !req.body.is_empty()
            && !serde_json::from_slice::<Map<String, Value>>(&req.body).is_ok_and(|f| f.is_empty())
        {
            return Err(HostError::new(
                400,
                "invalid /api/cloud/wake body: expected an empty JSON object",
            ));
        }
        crate::log_sync::ensure_wake(&host.home);
        host.cloud_sync.wake();
        Ok(json!({}))
    })();
    render(res, ctx)
}
