//! Request router for the LastDB Mini Unix-domain socket.

use std::sync::OnceLock;

use fold_db::access::AccessContext;

use super::uds_http::{UdsRequest, UdsResponse};

/// The request-grammar version this node speaks (`GET /api/version`).
pub const API_VERSION: u32 = 34;

static BUILD_VERSION: OnceLock<&'static str> = OnceLock::new();
const CAPABILITIES_JSON: &str = r#"{"owned_app_schemas_v1":true,"atomic_owned_mutations_v1":true,"compound_alternate_keys_v1":true,"independent_hash_range_v1":true}"#;

/// Install the baked build string once at startup.
pub fn set_build_version(build: &'static str) {
    let _ = BUILD_VERSION.set(build);
}

/// A data route the control socket exposes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataRoute {
    /// `POST /api/query`
    Query,
    /// `POST /api/mutation`
    Mutation,
    /// `POST /api/mutations/batch`
    MutationBatch,
    /// `POST /api/queries/batch`
    QueryBatch,
    /// `GET /api/schemas`
    ListSchemas,
    /// `GET /api/list?schema=`
    ListRecordKeys,
    /// `GET /api/schema/{name}`
    GetSchema,
    /// `POST /api/schemas/declare`
    DeclareSchema,
    /// `POST /api/schemas/seed-system`
    SeedSystemSchema,
    /// `POST /api/schemas/drop`
    DropSchema,
    /// `GET /api/status`
    Status,
    /// `POST /api/admin/shed`
    AdminShed,
    /// `GET /api/system/auto-identity`
    AutoIdentity,
    /// `GET /api/system/boot-identity`
    BootIdentity,
    /// `POST /api/cloud/connection`
    CloudConnection,
    /// `GET /api/db/schemas`
    DbSchemas,
    /// `POST /api/db/flush`
    DbFlush,
    /// `POST /api/db/cleanup` or `GET /api/db/cleanup`
    DbCleanup,
    /// `GET /api/db/native-status` or `POST /api/db/native-status`
    DbNativeStatus,
    /// `GET /api/db/delete-ledger`
    DbDeleteLedger,
    /// `POST /api/db/schema-retention`
    DbSchemaRetention,
    /// `POST /api/db/put-blob`
    DbPutBlob,
    /// `POST /api/db/fetch-blob`
    DbFetchBlob,
    /// `POST /api/db/delete-blob`
    DbDeleteBlob,
}

#[rustfmt::skip]
static EXACT_ROUTES: &[(&str, &str, DataRoute)] = &[
    ("/api/schemas", "GET", DataRoute::ListSchemas),
    ("/api/list", "GET", DataRoute::ListRecordKeys),
    ("/api/schemas/declare", "POST", DataRoute::DeclareSchema),
    ("/api/schemas/seed-system", "POST", DataRoute::SeedSystemSchema),
    ("/api/schemas/drop", "POST", DataRoute::DropSchema),
    ("/api/status", "GET", DataRoute::Status),
    ("/api/admin/shed", "POST", DataRoute::AdminShed),
    ("/api/db/schemas", "GET", DataRoute::DbSchemas),
    ("/api/db/flush", "POST", DataRoute::DbFlush),
    ("/api/db/delete-ledger", "GET", DataRoute::DbDeleteLedger),
    ("/api/db/schema-retention", "POST", DataRoute::DbSchemaRetention),
    ("/api/db/put-blob", "POST", DataRoute::DbPutBlob),
    ("/api/db/fetch-blob", "POST", DataRoute::DbFetchBlob),
    ("/api/db/delete-blob", "POST", DataRoute::DbDeleteBlob),
    ("/api/cloud/connection", "POST", DataRoute::CloudConnection),
    ("/api/query", "POST", DataRoute::Query),
    ("/api/mutation", "POST", DataRoute::Mutation),
    ("/api/mutations/batch", "POST", DataRoute::MutationBatch),
    ("/api/queries/batch", "POST", DataRoute::QueryBatch),
    ("/api/system/auto-identity", "GET", DataRoute::AutoIdentity),
    ("/api/system/boot-identity", "GET", DataRoute::BootIdentity),
];

/// Classification of a control-socket request before any handler runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlRoute {
    /// Liveness probe (`GET /health`).
    Health,
    /// Version handshake (`GET /api/version`).
    Version,
    /// Recognized data route.
    Data(DataRoute),
    /// Target path is recognized, but not for this HTTP method.
    MethodNotAllowed {
        /// Supported methods sent as the `Allow` header.
        allow: &'static str,
    },
    /// Unrecognized control-socket path.
    NotFound,
}

fn allow_only(method: &str, allow: &'static str, route: ControlRoute) -> ControlRoute {
    if method == allow {
        route
    } else {
        ControlRoute::MethodNotAllowed { allow }
    }
}

/// Classify a parsed control-socket request into a [`ControlRoute`].
pub fn route(req: &UdsRequest) -> ControlRoute {
    let path = &req.target[..req.target.find(['?', '#']).unwrap_or(req.target.len())];
    let method = req.method.as_str();
    match path {
        "/health" => return allow_only(method, "GET", ControlRoute::Health),
        "/api/version" => return allow_only(method, "GET", ControlRoute::Version),
        "/api/db/native-status" | "/api/db/cleanup" => {
            let data = if path == "/api/db/cleanup" {
                DataRoute::DbCleanup
            } else {
                DataRoute::DbNativeStatus
            };
            return if matches!(method, "GET" | "POST") {
                ControlRoute::Data(data)
            } else {
                ControlRoute::MethodNotAllowed { allow: "GET, POST" }
            };
        }
        _ => {}
    }
    if let Some(&(_, allow, data)) = EXACT_ROUTES.iter().find(|&&(p, _, _)| p == path) {
        return allow_only(method, allow, ControlRoute::Data(data));
    }
    if path
        .strip_prefix("/api/schema/")
        .is_some_and(|r| !r.is_empty() && !r.contains('/'))
    {
        return allow_only(method, "GET", ControlRoute::Data(DataRoute::GetSchema));
    }
    ControlRoute::NotFound
}

fn instance_id() -> &'static str {
    static INSTANCE_ID: OnceLock<String> = OnceLock::new();
    INSTANCE_ID.get_or_init(|| {
        format!(
            "{}-{}",
            std::process::id(),
            fold_db::clock::unix_nanos_wide()
        )
    })
}

/// Cheap liveness response for the control socket.
pub fn health_response() -> UdsResponse {
    let id = instance_id();
    let body = format!(
        "{{\"status\":\"ok\",\"api_version\":{API_VERSION},\"capabilities\":{CAPABILITIES_JSON},\"instance_id\":\"{id}\"}}"
    );
    UdsResponse::new(200, "OK", body.into_bytes()).with_header("Content-Type", "application/json")
}

/// Build a 405 Method Not Allowed response with an `Allow` header.
pub fn method_not_allowed_response(allow: &str) -> UdsResponse {
    UdsResponse::new(405, "Method Not Allowed", b"Method Not Allowed".to_vec())
        .with_header("Allow", allow.to_string())
}

/// Route a parsed request and answer it, delegating recognized data routes to `execute_data`.
pub fn dispatch<F: FnOnce(DataRoute, &UdsRequest, &AccessContext) -> UdsResponse>(
    req: &UdsRequest,
    ctx: &AccessContext,
    execute_data: F,
    (min_api, cur_outbox, min_outbox): (u32, u32, u32),
) -> UdsResponse {
    match route(req) {
        ControlRoute::Health => health_response(),
        ControlRoute::Version => {
            let build =
                serde_json::Value::String(BUILD_VERSION.get().copied().unwrap_or("unknown").into());
            let id = instance_id();
            let body = format!(
                "{{\"ok\":true,\"api_version\":{API_VERSION},\"build\":{build},\"capabilities\":{CAPABILITIES_JSON},\"instance_id\":\"{id}\",\"min_supported_api_version\":{min_api},\"current_outbox_format_version\":{cur_outbox},\"min_supported_outbox_format_version\":{min_outbox}}}"
            );
            UdsResponse::new(200, "OK", body.into_bytes())
                .with_header("Content-Type", "application/json")
        }
        ControlRoute::Data(data) => execute_data(data, req, ctx),
        ControlRoute::NotFound => UdsResponse::new(404, "Not Found", b"Not Found".to_vec()),
        ControlRoute::MethodNotAllowed { allow } => method_not_allowed_response(allow),
    }
}
