//! Data-route executor and wall-clock timeout budgets for the `lastdbd` owner control socket.

use std::collections::HashMap;
use std::future::Future;
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
mod database_mutation;
mod database_owned;
mod database_read;
mod database_routes;
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
            DataRoute::Status => return admin_routes::status_route(ctx, host),
            DataRoute::AutoIdentity => return admin_routes::auto_identity(host),
            DataRoute::BootIdentity => return admin_routes::boot_identity(host),
            DataRoute::Query => heap_route!(query::query_route, req, ctx, host),
            DataRoute::Mutation => heap_route!(mutation::mutation_route, req, ctx, host),
            DataRoute::MutationBatch => heap_route!(mutation::batch_route, req, ctx, host),
            DataRoute::QueryBatch => heap_route!(query::query_batch_route, req, ctx, host),
            DataRoute::ListRecordKeys => heap_route!(list_keys, req, ctx, host),
            DataRoute::DeclareSchema => heap_route!(schema_declare::declare_schema, req, ctx, host),
            DataRoute::SeedSystemSchema => heap_route!(schema_routes::seed_system, req, ctx, host),
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
            DataRoute::DbDeleteBlob => heap_route!(mutation::db_delete_blob, req, ctx, host),
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

async fn list_keys(req: &UdsRequest, ctx: &AccessContext, host: &Host) -> UdsResponse {
    let result = async {
        let r = list_keys_route::ListRecordKeysRequest::parse(&req.target)
            .map_err(|m| HostError::new(400, m))?;
        let canonical = schema_resolve::resolve_schema_name(host, &r.requested)?
            .ok_or_else(|| HostError::new(404, format!("schema not found: {}", r.requested)))?;
        if let Some(name) = r.key_name.as_deref() {
            let scope = host.records().db_ops().read_scope().await?;
            let report = scope
                .run_result(Box::pin(async {
                    let hash = r
                        .hash_filter
                        .as_deref()
                        .ok_or_else(|| HostError::new(400, "alternate key lists require hash="))?;
                    let schema = host
                        .records()
                        .schema_manager()
                        .get_schema_metadata(&canonical)?
                        .ok_or_else(|| HostError::new(404, "schema is absent"))?;
                    let mut q = Query::new_with_filter(
                        canonical.clone(),
                        Some(HashRangeFilter::HashKey(hash.to_owned())),
                    );
                    q.key_name = Some(name.to_owned());
                    let qreq = QueryRequest {
                        query: q,
                        limit: Some(r.limit),
                        offset: None,
                        cursor: list_keys_route::decode_alternate_cursor(r.cursor.as_deref())?,
                        response_format: query::QueryResponseFormat::Full,
                    };
                    let page = query::execute_query(host, qreq).await?;
                    list_keys_route::alternate_list_report(&r, &schema, &page)
                }))
                .await?;
            return Ok(json!({ "list": report }));
        }
        let report = host
            .records()
            .list_schema_record_keys(
                &r.requested,
                &canonical,
                r.limit,
                r.cursor.as_deref(),
                r.hash_filter.as_deref(),
            )
            .await
            .map_err(|e| map_host_err("list failed", e))?;
        Ok(json!({ "list": report }))
    }
    .await;
    render(result, ctx)
}
