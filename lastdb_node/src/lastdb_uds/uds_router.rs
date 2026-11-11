//! Request router for the LastDB Mini Unix-domain socket.

use fold_db::access::AccessContext;
use std::sync::OnceLock;

use super::uds_http::{UdsRequest, UdsResponse};

const API_VERSION_PATH: &str = "/api/version";

/// The request-grammar version this node speaks (`GET /api/version`).
pub const API_VERSION: u32 = 34;

static BUILD_VERSION: OnceLock<&'static str> = OnceLock::new();

/// Install the baked build string once at startup.
pub fn set_build_version(build: &'static str) {
    let _ = BUILD_VERSION.set(build);
}

/// The build string the version route reports (`"unknown"` until installed).
#[must_use]
pub fn build_version() -> &'static str {
    BUILD_VERSION.get().copied().unwrap_or("unknown")
}

/// A data route the control socket exposes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataRoute {
    /// `POST /api/query` — read query against the node's data.
    Query,
    /// `POST /api/mutation` — write mutation against the node's data.
    Mutation,
    /// `POST /api/mutations/batch` — write multiple mutations in one request.
    MutationBatch,
    /// `POST /api/queries/batch` — several independent `/api/query` bodies in one request.
    QueryBatch,
    /// `GET /api/schemas` — list the node's available schemas.
    ListSchemas,
    /// `GET /api/list?schema=` — keys-only page of live record identities.
    ListRecordKeys,
    /// `GET /api/schema/{name}` — fetch one schema by canonical name/hash or descriptive name.
    GetSchema,
    /// `POST /api/schemas/declare` — owner direct namespaced schema declaration.
    DeclareSchema,
    /// `POST /api/schemas/seed-system` — install one `system_seed` schema on an isolated daemon.
    SeedSystemSchema,
    /// `POST /api/schemas/drop` — remove one installed schema identity or an app's schemas.
    DropSchema,
    /// `GET /api/status` — daemon status summary.
    Status,
    /// `POST /api/admin/shed` — flush pending writes and purge clean warm memory.
    AdminShed,
    /// `GET /api/system/auto-identity` — node public key and derived user hash.
    AutoIdentity,
    /// `GET /api/system/boot-identity` — current process pid, start timestamp, and build.
    BootIdentity,
    /// `POST /api/sync/byte-backup-v2` — explicit owner byte backup work or status.
    SyncByteBackupV2,
    /// `POST /api/sync/byte-backup-v2/connect` — import owner credentials for manual backup.
    SyncByteBackupV2Connect,
    /// `POST /api/sync/shared-snapshot` — publish and finish shared writer upload.
    SyncSharedSnapshot,
    /// `GET /api/db/schemas` — record-only per-schema logical storage.
    DbSchemas,
    /// `POST /api/db/flush` — wait for accepted local writes to reach disk.
    DbFlush,
    /// `POST /api/db/cleanup` / `GET /api/db/cleanup` — resumable cleanup request and receipt.
    DbCleanup,
    /// `GET /api/db/native-status` — read native completion and writer state.
    DbNativeStatus,
    /// `GET /api/db/delete-ledger` — read the durable record hard-delete audit trail.
    DbDeleteLedger,
    /// `POST /api/db/schema-retention` — read/set/clear time-based schema retention policy.
    DbSchemaRetention,
    /// `POST /api/db/put-blob` — store the file of one record with its marker and fields.
    DbPutBlob,
    /// `POST /api/db/fetch-blob` — read the file of one record by schema and key.
    DbFetchBlob,
    /// `POST /api/db/delete-blob` — remove the file and marker of one record.
    DbDeleteBlob,
}

struct Row {
    path: &'static str,
    method: &'static str,
    route: DataRoute,
}

const fn row(path: &'static str, method: &'static str, route: DataRoute) -> Row {
    Row {
        path,
        method,
        route,
    }
}

#[rustfmt::skip]
static EXACT_ROUTES: &[Row] = &[
    row("/api/schemas", "GET", DataRoute::ListSchemas),
    row("/api/list", "GET", DataRoute::ListRecordKeys),
    row("/api/schemas/declare", "POST", DataRoute::DeclareSchema),
    row("/api/schemas/seed-system", "POST", DataRoute::SeedSystemSchema),
    row("/api/schemas/drop", "POST", DataRoute::DropSchema),
    row("/api/status", "GET", DataRoute::Status),
    row("/api/admin/shed", "POST", DataRoute::AdminShed),
    row("/api/db/schemas", "GET", DataRoute::DbSchemas),
    row("/api/db/flush", "POST", DataRoute::DbFlush),
    row("/api/db/delete-ledger", "GET", DataRoute::DbDeleteLedger),
    row("/api/db/schema-retention", "POST", DataRoute::DbSchemaRetention),
    row("/api/db/put-blob", "POST", DataRoute::DbPutBlob),
    row("/api/db/fetch-blob", "POST", DataRoute::DbFetchBlob),
    row("/api/db/delete-blob", "POST", DataRoute::DbDeleteBlob),
    row("/api/sync/byte-backup-v2", "POST", DataRoute::SyncByteBackupV2),
    row("/api/sync/byte-backup-v2/connect", "POST", DataRoute::SyncByteBackupV2Connect),
    row("/api/sync/shared-snapshot", "POST", DataRoute::SyncSharedSnapshot),
    row("/api/query", "POST", DataRoute::Query),
    row("/api/mutation", "POST", DataRoute::Mutation),
    row("/api/mutations/batch", "POST", DataRoute::MutationBatch),
    row("/api/queries/batch", "POST", DataRoute::QueryBatch),
    row("/api/system/auto-identity", "GET", DataRoute::AutoIdentity),
    row("/api/system/boot-identity", "GET", DataRoute::BootIdentity),
];

/// Classification of a control-socket request before any handler runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlRoute {
    /// Liveness probe (`GET /health`) — answerable without node state.
    Health,
    /// Version handshake (`GET /api/version`).
    Version,
    /// A recognized data route, executed against node state by the dispatcher.
    Data(DataRoute),
    /// The target path is recognized, but not for this method.
    MethodNotAllowed {
        /// Methods the path supports, sent as the `Allow` header.
        allow: &'static str,
    },
    /// No recognized control-socket route matches the target path.
    NotFound,
}

fn path_only(target: &str) -> &str {
    let end = target.find(['?', '#']).unwrap_or(target.len());
    &target[..end]
}

/// Classify a parsed control-socket request into a [`ControlRoute`].
pub fn route(req: &UdsRequest) -> ControlRoute {
    let path = path_only(&req.target);
    let method = req.method.as_str();

    if let Some(route) = route_special(path, method) {
        return route;
    }
    if let Some(row) = EXACT_ROUTES.iter().find(|row| row.path == path) {
        return single_method(method, row.method, ControlRoute::Data(row.route));
    }
    if path
        .strip_prefix("/api/schema/")
        .is_some_and(|rest| !rest.is_empty() && !rest.contains('/'))
    {
        return single_method(method, "GET", ControlRoute::Data(DataRoute::GetSchema));
    }
    ControlRoute::NotFound
}

fn single_method(method: &str, allow: &'static str, route: ControlRoute) -> ControlRoute {
    if method == allow {
        route
    } else {
        ControlRoute::MethodNotAllowed { allow }
    }
}

fn route_special(path: &str, method: &str) -> Option<ControlRoute> {
    let route = match path {
        "/health" => single_method(method, "GET", ControlRoute::Health),
        API_VERSION_PATH => single_method(method, "GET", ControlRoute::Version),
        "/api/db/native-status" => match method {
            "GET" | "POST" => ControlRoute::Data(DataRoute::DbNativeStatus),
            _ => ControlRoute::MethodNotAllowed { allow: "GET, POST" },
        },
        "/api/db/cleanup" => match method {
            "GET" | "POST" => ControlRoute::Data(DataRoute::DbCleanup),
            _ => ControlRoute::MethodNotAllowed { allow: "GET, POST" },
        },
        _ => return None,
    };
    Some(route)
}

const CAPABILITIES_JSON: &str = r#"{"owned_app_schemas_v1":true,"atomic_owned_mutations_v1":true,"compound_alternate_keys_v1":true,"independent_hash_range_v1":true}"#;

fn instance_id() -> &'static str {
    static INSTANCE_ID: OnceLock<String> = OnceLock::new();
    INSTANCE_ID.get_or_init(|| {
        let started_at_nanos = fold_db::clock::unix_nanos_wide();
        format!("{}-{started_at_nanos}", std::process::id())
    })
}

/// Cheap liveness response for the control socket.
pub fn health_response() -> UdsResponse {
    let instance_id = instance_id();
    UdsResponse::new(
        200,
        "OK",
        format!(
            "{{\"status\":\"ok\",\"api_version\":{API_VERSION},\"capabilities\":{CAPABILITIES_JSON},\"instance_id\":\"{instance_id}\"}}"
        )
        .into_bytes(),
    )
    .with_header("Content-Type", "application/json")
}

/// The version handshake for the control socket.
pub fn version_response(
    min_supported_api_version: u32,
    current_outbox_format_version: u32,
    min_supported_outbox_format_version: u32,
) -> UdsResponse {
    let build = serde_json::Value::String(build_version().to_string());
    let instance_id = instance_id();
    UdsResponse::new(
        200,
        "OK",
        format!(
            "{{\"ok\":true,\"api_version\":{API_VERSION},\"build\":{build},\"capabilities\":{CAPABILITIES_JSON},\"instance_id\":\"{instance_id}\",\"min_supported_api_version\":{min_supported_api_version},\"current_outbox_format_version\":{current_outbox_format_version},\"min_supported_outbox_format_version\":{min_supported_outbox_format_version}}}"
        )
        .into_bytes(),
    )
    .with_header("Content-Type", "application/json")
}

/// Content-free `404 Not Found` for an unrecognized control-socket route (**I4**).
pub fn not_found_response() -> UdsResponse {
    UdsResponse::new(404, "Not Found", b"Not Found".to_vec())
}

/// Content-free `405 Method Not Allowed`, advertising the supported methods (**I4**).
pub fn method_not_allowed_response(allow: &str) -> UdsResponse {
    UdsResponse::new(405, "Method Not Allowed", b"Method Not Allowed".to_vec())
        .with_header("Allow", allow.to_string())
}

/// Route a parsed request and answer it, delegating recognized data routes to `execute_data`.
pub fn dispatch<F>(
    req: &UdsRequest,
    ctx: &AccessContext,
    execute_data: F,
    version_params: (u32, u32, u32),
) -> UdsResponse
where
    F: FnOnce(DataRoute, &UdsRequest, &AccessContext) -> UdsResponse,
{
    match route(req) {
        ControlRoute::Health => health_response(),
        ControlRoute::Version => {
            version_response(version_params.0, version_params.1, version_params.2)
        }
        ControlRoute::Data(data) => execute_data(data, req, ctx),
        ControlRoute::NotFound => not_found_response(),
        ControlRoute::MethodNotAllowed { allow } => method_not_allowed_response(allow),
    }
}
