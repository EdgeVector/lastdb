//! Data-route executor for the `lastdbd` owner control socket.

use std::collections::HashMap;
use std::time::Duration;

use base64::Engine as _;
use fold_db::access::AccessContext;
use fold_db::schema::types::Schema;
use serde::Deserialize;
use serde_json::Value;

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
mod byte_backup_routes;
mod database_mutation;
mod database_read;
mod database_routes;
mod dispatch;
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
use wire::{body_object, parse_query_body, query_flag, schema_name_from_target, QueryRequest};

pub use crate::shared_snapshot::sequence::SharedSnapshotRequest;
pub use database_routes::{
    execute_database_request, execute_deferred_database_mutation, is_deferred_database_mutation,
};
pub use dispatch::execute_data_route;
pub use mutation::execute_deferred_mutation;
pub use reject::Reject;
pub use schema_declare::{execute_apps_declare_schema_route, normalize_app_manifest_schema};
pub use timeouts::{
    admin_handler_timeout, admin_handler_timeout_override, block_on_admin_route,
    block_on_data_route, block_on_route, data_route_budget, execute_data_route_with_timeout,
    handler_timeout, with_handler_timeout, CANARY_BOOT_IDENTITY_HANDLER_TIMEOUT_SECS,
    CANARY_STATUS_HANDLER_TIMEOUT_SECS, DEFAULT_ADMIN_HANDLER_TIMEOUT_SECS,
    DEFAULT_CLI_UDS_TIMEOUT_SECS, DEFAULT_HANDLER_TIMEOUT_SECS,
};
pub use wire::{parse_mutation_body, ParsedMutation};
