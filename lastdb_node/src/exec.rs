//! Data-route executor for the `lastdbd` owner control socket.

use std::collections::HashMap;
use std::time::Duration;

use base64::Engine as _;
use fold_db::access::AccessContext;
use fold_db::schema::types::Schema;
use serde::Deserialize;
use serde_json::Value;

use crate::byte_cloud_backup::{self, ByteBackupRequest};
use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};
use crate::lastdb_uds::uds_router::DataRoute;

/// Heap future for one data route so the dispatch poll frame stays small.
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
mod database_read;
mod database_routes;
mod file_blob;
mod file_blob_put;
mod list_keys_route;
mod mutation;
mod query;
mod reject;
mod reply;
mod retention_routes;
mod schema_declare;
mod schema_resolve;
mod schema_routes;
mod timeouts;
mod wire;

use file_blob::is_media_type;
use reply::{content_free, envelope, error_response, json_ok, render, HostError};
use timeouts::{data_route_budget, execute_data_route_with_timeout, with_handler_timeout_budget};
use wire::{body_object, parse_query_body, query_flag, schema_name_from_target, QueryRequest};

pub use crate::shared_snapshot::sequence::SharedSnapshotRequest;
pub use database_routes::{
    execute_database_request, execute_deferred_database_mutation, is_deferred_database_mutation,
};
pub use mutation::execute_deferred_mutation;
pub use reject::Reject;
pub use schema_declare::{execute_apps_declare_schema_route, normalize_app_manifest_schema};
pub use timeouts::{
    admin_handler_timeout, admin_handler_timeout_override, block_on_admin_route,
    block_on_data_route, block_on_route, handler_timeout, DEFAULT_CLI_UDS_TIMEOUT_SECS,
};
pub use wire::{parse_mutation_body, ParsedMutation};

/// Execute a recognized control-socket data route against the host.
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
            heap_route!(
                list_keys_route::execute_list_record_keys_route,
                req,
                ctx,
                host
            )
            .await
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
        DataRoute::SyncByteBackupV2 => execute_byte_backup(req, ctx, host).await,
        DataRoute::SyncByteBackupV2Connect => connect_byte_backup(req, ctx, host).await,
        DataRoute::SyncSharedSnapshot => heap_route!(shared_snapshot, req, ctx, host).await,
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
            heap_route!(
                retention_routes::execute_db_delete_ledger_route,
                req,
                ctx,
                host
            )
            .await
        }
        DataRoute::DbSchemaRetention => {
            heap_route!(
                retention_routes::execute_db_schema_retention_route,
                req,
                ctx,
                host
            )
            .await
        }
        DataRoute::DbPutBlob => {
            heap_route!(file_blob_put::execute_db_put_blob_route, req, ctx, host).await
        }
        DataRoute::DbFetchBlob => {
            heap_route!(file_blob::execute_db_fetch_blob_route, req, ctx, host).await
        }
        DataRoute::DbDeleteBlob => {
            heap_route!(file_blob::execute_db_delete_blob_route, req, ctx, host).await
        }
    }
}

async fn execute_byte_backup(req: &UdsRequest, ctx: &AccessContext, host: &Host) -> UdsResponse {
    if !ctx.is_owner {
        return error_response(403, "byte backup is owner-only", ctx);
    }
    if req.body.len() as u64 > cloud_backup_protocol::v2::limits::CONTROL_JSON_MAX_BYTES {
        return error_response(413, "byte backup request exceeds its byte limit", ctx);
    }
    let request: ByteBackupRequest = match serde_json::from_slice(&req.body) {
        Ok(value) => value,
        Err(_) => return error_response(400, "invalid byte backup request", ctx),
    };
    match byte_cloud_backup::request(host, request).await {
        Ok(status) => json_ok(&envelope(&status, ctx.user_id.as_str())),
        Err(error) => error_response(409, &error.to_string(), ctx),
    }
}

async fn connect_byte_backup(req: &UdsRequest, ctx: &AccessContext, host: &Host) -> UdsResponse {
    if !ctx.is_owner {
        return error_response(403, "byte backup connection is owner-only", ctx);
    }
    if req.body.len() > 8192 {
        return error_response(413, "byte backup connection exceeds its byte limit", ctx);
    }
    let request: byte_cloud_backup::SensitiveConnectionRequest =
        match serde_json::from_slice(&req.body) {
            Ok(value) => value,
            Err(_) => return error_response(400, "invalid byte backup connection", ctx),
        };
    match byte_cloud_backup::connect_existing(host, request).await {
        Ok(status) => json_ok(&envelope(&status, ctx.user_id.as_str())),
        Err(error) => error_response(409, &error.to_string(), ctx),
    }
}

async fn shared_snapshot(req: &UdsRequest, ctx: &AccessContext, host: &Host) -> UdsResponse {
    if !ctx.is_owner {
        return error_response(403, "shared snapshot is owner-only", ctx);
    }
    if req.body.len() > 8192 {
        return error_response(413, "shared snapshot request exceeds its byte limit", ctx);
    }
    let Ok(request) = serde_json::from_slice(&req.body) else {
        return error_response(400, "invalid shared snapshot request", ctx);
    };
    let budget = admin_handler_timeout().saturating_sub(Duration::from_secs(1));
    match crate::shared_snapshot::sequence::run_shared_snapshot(host, &request, budget).await {
        Ok(result) => json_ok(&envelope(&result, ctx.user_id.as_str())),
        Err(error) => error_response(409, &error.to_string(), ctx),
    }
}
