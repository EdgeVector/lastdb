//! Data-route dispatch table and wall-clock timeout wrapper for `lastdbd`.

use std::future::Future;
use std::time::Duration;

use fold_db::access::AccessContext;
use serde::Deserialize;
use serde_json::json;

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
mod app_data;
mod calling_app;
mod database_owned;
mod database_received;
mod database_routes;
mod database_share;
mod mutation;
mod prepare_stop_routes;
mod query;
mod reply;
mod schema_declare;
mod schema_routes;
mod uds_server;
mod wire;

use reply::{content_free, envelope, error_response, json_ok, render, HostError};

pub use schema_declare::normalize_app_manifest_schema;
pub use uds_server::UdsServer;

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
        render(
            Err(HostError::new(
                503,
                "node is busy: handler deadline exceeded; retry after 1s",
            )
            .with_retry_after(1)),
            ctx,
        )
    })
}

async fn with_data_route_timeout_budget<F: Future<Output = UdsResponse>>(
    route: DataRoute,
    ctx: &AccessContext,
    fut: F,
) -> UdsResponse {
    let budget = data_route_budget(route);
    let is_v1 = matches!(
        route,
        DataRoute::V1Read | DataRoute::V1Write | DataRoute::V1Changes | DataRoute::V1Databases
    );
    if !is_v1 {
        return with_handler_timeout_budget(ctx, budget, fut).await;
    }
    tokio::time::timeout(budget, fut).await.unwrap_or_else(|_| {
        tracing::warn!(
            timeout_secs = budget.as_secs(),
            "uds v1 handler deadline exceeded; returning 503"
        );
        app_api::fail(
            &HostError::new(
                503,
                "node is busy: handler deadline exceeded; retry after 1s",
            )
            .with_retry_after(1),
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
    with_data_route_timeout_budget(route, ctx, async {
        let fut: ErasedRoute<'_> = match route {
            DataRoute::ListSchemas => return schema_routes::list_schemas(req, ctx, host),
            DataRoute::GetSchema => return schema_routes::get_schema(req, ctx, host),
            DataRoute::Status => {
                let up = fold_db::clock::unix_secs().saturating_sub(host.process_start_ts);
                let uds = host
                    .uds_workers
                    .get()
                    .map(crate::lastdb_uds::UdsWorkerPool::snapshot);
                let app_socket = host
                    .app_uds_workers
                    .get()
                    .map(crate::lastdb_uds::UdsWorkerPool::snapshot);
                let runtime = host.db.db_ops().native_runtime();
                let mut cloud_sync = host.cloud_sync.snapshot();
                crate::log_sync::usage::fill(
                    &host.home,
                    runtime.accepted_status().acked,
                    &mut cloud_sync,
                );
                host.cloud_sync.attach_personal_maintenance(&mut cloud_sync);
                let st = json!({
                    "build": { "version": crate::version::build_version() },
                    "process_start_ts": host.process_start_ts, "uptime_secs": up,
                    "uds": uds,
                    "app_socket": app_socket,
                    "cloud_sync": cloud_sync,
                    "spool": runtime.accepted_report(),
                    "commit_log": runtime.commit_log_stats(),
                });
                return json_ok(&envelope(&json!({ "status": st }), &ctx.user_id));
            }
            DataRoute::Ops => {
                let up = fold_db::clock::unix_secs().saturating_sub(host.process_start_ts);
                return json_ok(&envelope(&host.ops.snapshot(up), &ctx.user_id));
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
            DataRoute::BackupSnapshot => heap_route!(admin_routes::backup_snapshot, req, ctx, host),
            DataRoute::CloudWake => return admin_routes::cloud_wake(req, ctx, host),
            DataRoute::PrepareStop => {
                heap_route!(prepare_stop_routes::prepare_stop, req, ctx, host)
            }
            DataRoute::PrepareStopCancel => {
                return prepare_stop_routes::prepare_stop_cancel(req, ctx);
            }
            DataRoute::V1Read => heap_route!(app_api::read_route, req, ctx, host),
            DataRoute::V1Write => heap_route!(app_api::write_route, req, ctx, host),
            DataRoute::V1Changes => heap_route!(app_api::changes_route, req, ctx, host),
            DataRoute::V1Databases => heap_route!(app_api::databases_route, req, ctx, host),
            DataRoute::AppRegister => return calling_app::register_route(req, ctx, host),
            DataRoute::AppUnregister => return calling_app::unregister_route(req, ctx, host),
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
        | DataRoute::AppsDeclareSchema
        | DataRoute::DropSchema
        | DataRoute::BackupSnapshot
        | DataRoute::CloudWake => admin_handler_timeout(),
        DataRoute::Status | DataRoute::Ops => Duration::from_secs(2),
        DataRoute::AdminShed => Duration::from_secs(60),
        DataRoute::BootIdentity => Duration::from_secs(1),
        // The handler applies its own deadline (`deadline_ms`, at most
        // `MAX_DEADLINE_MS`); this budget only keeps the reply a JSON answer.
        DataRoute::PrepareStop => Duration::from_millis(crate::prepare_stop::MAX_DEADLINE_MS)
            .saturating_add(admin_handler_timeout()),
        // The handler applies the real deadline and maps a timeout into the app
        // API error shape. This budget only keeps that reply from becoming plain text.
        DataRoute::V1Databases => admin_handler_timeout().saturating_add(handler_timeout()),
        _ => handler_timeout(),
    }
}
