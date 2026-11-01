//! Data-route executor for the minimal daemon's owner control socket.
//!
//! `lastdbd` serves the SAME wire surface fbrain/fkanban speak to the full
//! node's socket — the [`lastdb_uds::uds_router::DataRoute`] allowlist with
//! identical request/response JSON shapes. The query / mutation /
//! native-index-search HANDLER BODIES now live in the shared
//! [`lastdb_host::handlers`] module, driven off the [`HostNode`]
//! trait, so this executor and the full node's (`fold_db_node::server::uds_exec`)
//! are literally ONE implementation below the app-identity axis — no drift.
//!
//! What stays local here is only the wire glue and the routes the minimal
//! daemon serves in its own way:
//! - Request/response wire parsing (shared `lastdb_host::wire`) + the final
//!   envelope/error render (shared `lastdb_host::handlers::render`).
//! - `POST /api/schemas/load` — a SETUP verb served only on the full-surface
//!   socket (first-time schema loads for fkanban init / fbrain bootstrap).
//! - `GET /api/schemas` / `GET /api/schema/{name}` / `POST /api/schemas/declare`
//!   — the catalog listing (with record counts), the single-schema fetch, and
//!   the owner direct-declare, which resolve schema names via the shared
//!   [`lastdb_host::handlers::resolve_schema_name`] but otherwise assemble
//!   host-local bodies.
//! - App-blob routes are structurally absent (404): no upload-storage backend.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use base64::Engine as _;
use fold_db::access::{parse_db_locator, storage_prefix_for, AccessContext, DbLocator};
use fold_db::db_operations::{DbCatalogEntry, DbCatalogKeySelection};
use fold_db::schema::schema_types::SchemaWithState;
use fold_db::schema::types::declarative_schemas::SchemaSource;
use fold_db::schema::types::{DeclarativeSchemaDefinition, Schema, SchemaType};
use lastdb_uds::uds_http::{UdsRequest, UdsResponse};
use lastdb_uds::uds_router::DataRoute;
use serde::{Deserialize, Serialize};
use serde_json::Value;

// Framework-agnostic wire-shape contract + the shared handler bodies, consumed
// verbatim by the full node's socket executor too so the two cannot drift.
use lastdb_host::envelope::{content_free, envelope, json_ok};
use lastdb_host::handlers::{self, render, AppSearchParams, NativeSearchParams};
use lastdb_host::reject::{Reject, WireKey};
use lastdb_host::wire::{
    body_object, parse_batch_mutation_body, parse_min_score, parse_mutation_body, parse_query_body,
    path_tail, query_flag, query_value, schema_name_from_target, QueryRequest,
};
use lastdb_host::HostError;

use crate::host::Host;
use crate::schema_sync_audit::{self, SchemaSyncAuditEvent};

/// Render an error to the owner-socket wire, delegating to the shared I4 mapping.
fn error_response(status: u16, message: &str, ctx: &AccessContext) -> UdsResponse {
    render(Err(HostError::new(status, message)), ctx)
}

/// Render a failed owner-socket operation through the canonical
/// [`FoldDbError`](fold_db::error::FoldDbError) -> [`HostError`] mapping.
///
/// **Use this instead of `Err(e) => error_response(500, …)` wherever the error
/// being caught can carry a caller fault.** A handler that ends its error match
/// with a bare 500 reports a *caller* fault as a server bug: name a `field` the
/// schema does not have and the node answered 500. [`render`] logs every 5xx at
/// ERROR, and the observability ERROR layer promotes each of those into its own
/// Sentry issue — so one bad field name became 210 events with zero users
/// affected (Sentry `7620011366`), and one bad `uds` accept became 492 more
/// (`7641868650`). The mutation-path bug behind the first burst was fixed in
/// `70e120f27`; the misclassification that turned it into a storm outlived it,
/// and did the same for the next caller fault.
///
/// Only a genuine 500 takes the operation prefix, mirroring the mutation path
/// in `lastdb_host::handlers`: a 4xx keeps the status the mapping assigned and
/// any structured body it carries (`cas_conflict`, `catalog_membership_denied`,
/// …), which re-wrapping the message would drop.
///
/// This is only half the contract. The mapping can only classify what reaches
/// it, so the core wrapper must not have flattened the fault into
/// `FoldDbError::Database` first — see `admin_op_error` in
/// `fold_db_core::fold_db::admin_db`.
fn mapped_error_response(
    operation: &str,
    err: fold_db::error::FoldDbError,
    ctx: &AccessContext,
) -> UdsResponse {
    let mapped = HostError::from(err);
    if mapped.status == 500 {
        return render(
            Err(HostError::new(
                500,
                format!("{operation}: {}", mapped.message),
            )),
            ctx,
        );
    }
    render(Err(mapped), ctx)
}

mod byte_backup_routes;
mod conflict_metadata;
mod db_catalog;
mod db_maintenance;
mod file_blob;
mod file_blob_local;
mod logical_sync_routes;
mod query_batch;
mod schema_declare;
use self::conflict_metadata::*;
use self::db_catalog::*;
use self::db_maintenance::*;
use self::file_blob::*;
use self::file_blob_local::*;
use self::query_batch::*;
pub use self::schema_declare::execute_apps_declare_schema_route;
use self::schema_declare::*;

/// Heap future for one data route.
///
/// A dev build of `dispatch_data_route` used to keep every route future in one
/// poll frame (about 2.5 MiB). The request future copies that frame again at
/// each caller, which overflowed the 8 MiB UDS worker and the libtest thread.
/// Each awaited route is built inside `#[inline(never)]` and returned as a
/// pointer, so the dispatch frame stays small in dev and in release.
///
/// Two routes stay inline. Their futures are `Send` for this borrow and are
/// not `Send` for `dyn Future + Send` (`Send` is not general enough).
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
            // A fresh async block is `Send` when the route future is `Send`.
            // Casting the route future itself trips a higher-ranked `Send`
            // check on the references that future already holds.
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
mod app_routes;
mod batch_mutation_routes;
mod db_compact_routes;
mod deferred_mutation;
mod dispatch;
mod field_history;
mod introspection_routes;
mod liveness_routes;
mod mutation_notifications;
mod query_filters;
mod query_mutation_routes;
mod query_read_receipt;
mod schema_list_rows;
mod schema_routes;
mod setup_routes;
mod socket_queue;
mod storage_routes;
mod timeouts;
pub use socket_queue::{current_socket_queue_wait, with_socket_queue_wait};

use admin_routes::*;
use app_routes::*;
use batch_mutation_routes::*;
use db_compact_routes::*;
pub use deferred_mutation::execute_deferred_mutation;
pub use dispatch::*;
use field_history::*;
use introspection_routes::*;
use liveness_routes::*;
use query_filters::*;
use query_mutation_routes::*;
use schema_routes::*;
pub use setup_routes::*;
use storage_routes::*;
pub use timeouts::*;
