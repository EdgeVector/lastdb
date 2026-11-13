//! Request router for the LastDB Mini Unix-domain socket.

use std::sync::OnceLock;

use fold_db::access::AccessContext;

use super::uds_http::{UdsRequest, UdsResponse};

pub const API_VERSION: u32 = 34;

const CAPABILITIES_JSON: &str = r#"{"owned_app_schemas_v1":true,"atomic_owned_mutations_v1":true,"compound_alternate_keys_v1":true,"independent_hash_range_v1":true}"#;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataRoute {
    Query,
    Mutation,
    MutationBatch,
    QueryBatch,
    ListSchemas,
    ListRecordKeys,
    GetSchema,
    DeclareSchema,
    SeedSystemSchema,
    DropSchema,
    Status,
    AdminShed,
    AutoIdentity,
    BootIdentity,
    DbSchemas,
    DbFlush,
    DbCleanup,
    DbNativeStatus,
    DbDeleteLedger,
    DbSchemaRetention,
    DbPutBlob,
    DbFetchBlob,
    DbDeleteBlob,
}

static EXACT_ROUTES: &[(&str, &str, DataRoute)] = &[
    ("/api/schemas", "GET", DataRoute::ListSchemas),
    ("/api/list", "GET", DataRoute::ListRecordKeys),
    ("/api/schemas/declare", "POST", DataRoute::DeclareSchema),
    (
        "/api/schemas/seed-system",
        "POST",
        DataRoute::SeedSystemSchema,
    ),
    ("/api/schemas/drop", "POST", DataRoute::DropSchema),
    ("/api/status", "GET", DataRoute::Status),
    ("/api/admin/shed", "POST", DataRoute::AdminShed),
    ("/api/db/schemas", "GET", DataRoute::DbSchemas),
    ("/api/db/flush", "POST", DataRoute::DbFlush),
    ("/api/db/delete-ledger", "GET", DataRoute::DbDeleteLedger),
    (
        "/api/db/schema-retention",
        "POST",
        DataRoute::DbSchemaRetention,
    ),
    ("/api/db/put-blob", "POST", DataRoute::DbPutBlob),
    ("/api/db/fetch-blob", "POST", DataRoute::DbFetchBlob),
    ("/api/db/delete-blob", "POST", DataRoute::DbDeleteBlob),
    ("/api/query", "POST", DataRoute::Query),
    ("/api/mutation", "POST", DataRoute::Mutation),
    ("/api/mutations/batch", "POST", DataRoute::MutationBatch),
    ("/api/queries/batch", "POST", DataRoute::QueryBatch),
    ("/api/system/auto-identity", "GET", DataRoute::AutoIdentity),
    ("/api/system/boot-identity", "GET", DataRoute::BootIdentity),
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlRoute {
    Health,
    Version,
    Data(DataRoute),
    MethodNotAllowed { allow: &'static str },
    NotFound,
}

fn allow_only(method: &str, allow: &'static str, route: ControlRoute) -> ControlRoute {
    if method == allow {
        route
    } else {
        ControlRoute::MethodNotAllowed { allow }
    }
}

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

pub fn health_response() -> UdsResponse {
    let id = instance_id();
    let body = format!(
        "{{\"status\":\"ok\",\"api_version\":{API_VERSION},\"capabilities\":{CAPABILITIES_JSON},\"instance_id\":\"{id}\"}}"
    );
    UdsResponse::new(200, "OK", body.into_bytes()).with_header("Content-Type", "application/json")
}

pub fn method_not_allowed_response(allow: &str) -> UdsResponse {
    UdsResponse::new(405, "Method Not Allowed", b"Method Not Allowed".to_vec())
        .with_header("Allow", allow.to_string())
}

pub fn dispatch<F: FnOnce(DataRoute, &UdsRequest, &AccessContext) -> UdsResponse>(
    req: &UdsRequest,
    ctx: &AccessContext,
    execute_data: F,
) -> UdsResponse {
    match route(req) {
        ControlRoute::Health => health_response(),
        ControlRoute::Version => {
            let build = serde_json::Value::String(crate::version::build_version().into());
            let id = instance_id();
            let body = format!(
                "{{\"ok\":true,\"api_version\":{API_VERSION},\"build\":{build},\"capabilities\":{CAPABILITIES_JSON},\"instance_id\":\"{id}\",\"min_supported_api_version\":{},\"current_outbox_format_version\":{},\"min_supported_outbox_format_version\":{}}}",
                crate::version::MIN_SUPPORTED_API_VERSION,
                crate::version::CURRENT_OUTBOX_FORMAT_VERSION,
                crate::version::MIN_SUPPORTED_OUTBOX_FORMAT_VERSION,
            );
            UdsResponse::new(200, "OK", body.into_bytes())
                .with_header("Content-Type", "application/json")
        }
        ControlRoute::Data(data) => execute_data(data, req, ctx),
        ControlRoute::NotFound => UdsResponse::new(404, "Not Found", b"Not Found".to_vec()),
        ControlRoute::MethodNotAllowed { allow } => method_not_allowed_response(allow),
    }
}
