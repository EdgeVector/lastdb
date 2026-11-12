//! Data-route executor and wall-clock timeout budgets for the `lastdbd` owner control socket.

use std::collections::HashMap;
use std::time::Duration;

use base64::Engine as _;
use fold_db::access::AccessContext;
use fold_db::schema::types::field::HashRangeFilter;
use fold_db::schema::types::{Query, Schema};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};
use crate::lastdb_uds::uds_router::DataRoute;

type ErasedRoute<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = UdsResponse> + Send + 'a>>;

macro_rules! heap_route {
    ($f:path, $req:ident, $ctx:ident, $host:ident) => {{
        #[inline(never)]
        fn start<'a>(
            req: &'a UdsRequest,
            ctx: &'a AccessContext,
            host: &'a Host,
        ) -> ErasedRoute<'a> {
            Box::pin(async move { $f(req, ctx, host).await })
        }
        start($req, $ctx, $host)
    }};
}

macro_rules! heap_route_ctx_host {
    ($f:path, $ctx:ident, $host:ident) => {{
        #[inline(never)]
        fn start<'a>(ctx: &'a AccessContext, host: &'a Host) -> ErasedRoute<'a> {
            Box::pin(async move { $f(ctx, host).await })
        }
        start($ctx, $host)
    }};
}

mod admin_routes;
mod database_mutation;
mod database_owned;
mod database_read;
mod database_routes;
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

use reply::{
    content_free, envelope, error_response, json_ok, mapped_error_response, render, HostError,
};
use wire::{body_object, parse_query_body, query_flag, schema_name_from_target, QueryRequest};

pub use database_routes::{
    execute_database_request, execute_deferred_database_mutation, is_deferred_database_mutation,
};
pub use mutation::execute_deferred_mutation;
pub use schema_declare::{execute_apps_declare_schema_route, normalize_app_manifest_schema};
pub use wire::{parse_mutation_body, ParsedMutation, Reject};

const DEFAULT_HANDLER_TIMEOUT_SECS: u64 = 90;
const DEFAULT_ADMIN_HANDLER_TIMEOUT_SECS: u64 = 600;
pub const DEFAULT_CLI_UDS_TIMEOUT_SECS: u64 = 30;

fn secs_override(var: &str) -> Option<Duration> {
    fold_db::env_flag::var_parsed::<u64>(var)
        .filter(|&n| n > 0)
        .map(Duration::from_secs)
}

#[must_use]
pub fn handler_timeout() -> Duration {
    secs_override("LASTDB_UDS_HANDLER_TIMEOUT_SECS")
        .unwrap_or(Duration::from_secs(DEFAULT_HANDLER_TIMEOUT_SECS))
}

#[must_use]
pub fn admin_handler_timeout_override() -> Option<Duration> {
    secs_override("LASTDB_UDS_ADMIN_TIMEOUT_SECS")
}

#[must_use]
pub fn admin_handler_timeout() -> Duration {
    admin_handler_timeout_override()
        .unwrap_or(Duration::from_secs(DEFAULT_ADMIN_HANDLER_TIMEOUT_SECS))
}

pub(in crate::exec) async fn with_handler_timeout_budget<F>(
    ctx: &AccessContext,
    budget: Duration,
    fut: F,
) -> UdsResponse
where
    F: std::future::Future<Output = UdsResponse>,
{
    if let Ok(resp) = tokio::time::timeout(budget, fut).await {
        resp
    } else {
        tracing::warn!(
            timeout_secs = budget.as_secs(),
            "uds handler deadline exceeded; returning 503"
        );
        error_response(
            503,
            "node is busy: handler deadline exceeded; retry after 1s",
            ctx,
        )
    }
}

pub fn block_on_route<Fut>(
    handle: &tokio::runtime::Handle,
    ctx: &AccessContext,
    fut: Fut,
) -> UdsResponse
where
    Fut: std::future::Future<Output = UdsResponse>,
{
    handle.block_on(with_handler_timeout_budget(ctx, handler_timeout(), fut))
}

pub fn block_on_admin_route<Fut>(
    handle: &tokio::runtime::Handle,
    ctx: &AccessContext,
    fut: Fut,
) -> UdsResponse
where
    Fut: std::future::Future<Output = UdsResponse>,
{
    handle.block_on(with_handler_timeout_budget(
        ctx,
        admin_handler_timeout(),
        fut,
    ))
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
    with_handler_timeout_budget(
        ctx,
        data_route_budget(route),
        execute_data_route(route, req, ctx, host),
    )
    .await
}

#[must_use]
pub(in crate::exec) fn data_route_budget(route: DataRoute) -> Duration {
    match route {
        DataRoute::DbSchemas
        | DataRoute::DbDeleteLedger
        | DataRoute::CloudConnection
        | DataRoute::DeclareSchema
        | DataRoute::DropSchema => admin_handler_timeout(),
        DataRoute::Status => Duration::from_secs(2),
        DataRoute::AdminShed => Duration::from_secs(60),
        DataRoute::BootIdentity => Duration::from_secs(1),
        _ => handler_timeout(),
    }
}

pub(in crate::exec) async fn execute_data_route(
    route: DataRoute,
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    match route {
        DataRoute::Query => heap_route!(query::execute_query_route, req, ctx, host).await,
        DataRoute::Mutation => heap_route!(mutation::execute_mutation_route, req, ctx, host).await,
        DataRoute::MutationBatch => {
            heap_route!(mutation::execute_mutations_batch_route, req, ctx, host).await
        }
        DataRoute::QueryBatch => {
            heap_route!(query::execute_query_batch_route, req, ctx, host).await
        }
        DataRoute::ListSchemas => schema_routes::execute_list_schemas_route(req, ctx, host),
        DataRoute::ListRecordKeys => {
            heap_route!(execute_list_record_keys_route, req, ctx, host).await
        }
        DataRoute::GetSchema => schema_routes::execute_get_schema_route(req, ctx, host),
        DataRoute::DeclareSchema => {
            heap_route!(schema_declare::execute_declare_schema_route, req, ctx, host).await
        }
        DataRoute::SeedSystemSchema => {
            heap_route!(
                schema_routes::execute_seed_system_schema_route,
                req,
                ctx,
                host
            )
            .await
        }
        DataRoute::DropSchema => {
            heap_route!(schema_routes::execute_drop_schema_route, req, ctx, host).await
        }
        DataRoute::Status => admin_routes::execute_status_route(ctx, host),
        DataRoute::AdminShed => {
            heap_route_ctx_host!(admin_routes::execute_admin_shed_route, ctx, host).await
        }
        DataRoute::AutoIdentity => admin_routes::execute_auto_identity_route(host),
        DataRoute::BootIdentity => admin_routes::execute_boot_identity_route(host),
        DataRoute::CloudConnection => admin_routes::connect_byte_backup(req, ctx, host).await,
        DataRoute::DbSchemas => {
            heap_route_ctx_host!(admin_routes::execute_db_schemas_route, ctx, host).await
        }
        DataRoute::DbFlush => {
            heap_route!(admin_routes::execute_db_flush_route, req, ctx, host).await
        }
        DataRoute::DbCleanup => {
            heap_route!(admin_routes::execute_db_cleanup_route, req, ctx, host).await
        }
        DataRoute::DbNativeStatus => {
            heap_route!(admin_routes::execute_db_native_status_route, req, ctx, host).await
        }
        DataRoute::DbDeleteLedger => {
            heap_route!(admin_routes::execute_db_delete_ledger_route, req, ctx, host).await
        }
        DataRoute::DbSchemaRetention => {
            heap_route!(
                admin_routes::execute_db_schema_retention_route,
                req,
                ctx,
                host
            )
            .await
        }
        DataRoute::DbPutBlob => {
            heap_route!(file_blob::execute_db_put_blob_route, req, ctx, host).await
        }
        DataRoute::DbFetchBlob => {
            heap_route!(file_blob::execute_db_fetch_blob_route, req, ctx, host).await
        }
        DataRoute::DbDeleteBlob => {
            heap_route!(mutation::execute_db_delete_blob_route, req, ctx, host).await
        }
    }
}

async fn execute_list_record_keys_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let request = match list_keys_route::ListRecordKeysRequest::parse(&req.target) {
        Ok(r) => r,
        Err(msg) => return error_response(400, msg, ctx),
    };
    let canonical = match schema_resolve::resolve_schema_name(host, &request.requested) {
        Ok(Some(c)) => c,
        Ok(None) => {
            return error_response(
                404,
                &format!("schema not found: {}", request.requested),
                ctx,
            )
        }
        Err(e) => return render(Err(e), ctx),
    };
    if let Some(name) = request.key_name.as_deref() {
        let page_fut = async {
            let hash = request
                .hash_filter
                .as_deref()
                .ok_or_else(|| HostError::new(400, "alternate key lists require hash="))?;
            let schema = host
                .records()
                .schema_manager()
                .get_schema_metadata(&canonical)
                .map_err(HostError::from)?
                .ok_or_else(|| HostError::new(404, "schema is absent"))?;
            let mut q = Query::new_with_filter(
                canonical.clone(),
                Some(HashRangeFilter::HashKey(hash.to_owned())),
            );
            q.key_name = Some(name.to_owned());
            let cursor = list_keys_route::decode_alternate_cursor(request.cursor.as_deref())?;
            let page = query::execute_query(
                host,
                q,
                Some(request.limit),
                None,
                cursor,
                query::QueryResponseFormat::Full,
            )
            .await?;
            list_keys_route::alternate_list_report(&request, &schema, &page)
        };
        let result = match host.records().db_ops().read_scope().await {
            Ok(scope) => scope.run_result(Box::pin(page_fut)).await,
            Err(e) => Err(HostError::from(e)),
        };
        return render(result.map(|report| json!({ "list": report })), ctx);
    }
    match host
        .records()
        .list_schema_record_keys(
            &request.requested,
            &canonical,
            request.limit,
            request.cursor.as_deref(),
            request.hash_filter.as_deref(),
        )
        .await
    {
        Ok(report) => json_ok(&envelope(&json!({ "list": report }), ctx.user_id.as_str())),
        Err(e) => mapped_error_response("list failed", e, ctx),
    }
}
