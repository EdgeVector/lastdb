//! Data-route dispatch table and wall-clock timeout wrapper for `lastdbd`.

use std::future::Future;
use std::time::Duration;

use base64::Engine as _;
use fold_db::access::AccessContext;
use serde::Deserialize;
use serde_json::{json, Value};

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
mod database_owned;
mod database_routes;
mod database_session;
mod database_share;
mod database_signed;
mod file_blob;
mod list_keys;
mod mutation;
mod query;
mod reply;
mod schema_declare;
mod schema_routes;
mod wire;

use reply::{content_free, envelope, error_response, json_ok, render, HostError};

pub use database_routes::{
    execute_database_request, execute_deferred_database_request, is_deferred_database_request,
};
pub use database_session::admit as admit_database_session;
pub use mutation::execute_deferred_mutation;
pub use schema_declare::{execute_apps_declare_schema_route, normalize_app_manifest_schema};
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
                    "uds": uds, "search": admin_routes::search_queue_status(host),
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
            DataRoute::DeclareSchema => heap_route!(schema_declare::declare_schema, req, ctx, host),
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
