//! Fixed control-socket responses and dispatch.

use super::*;

/// Cheap liveness response for the control socket.
///
/// No database state and no caller bytes. The process-lifetime `instance_id`
/// lets a client detect a daemon restart between two health probes.
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

/// The capability flags both `/health` and `/api/version` advertise. One
/// literal so the two routes cannot disagree; a flag is a fixed `&'static str`
/// (never a caller byte).
pub(super) const CAPABILITIES_JSON: &str = r#"{"owned_app_schemas_v1":true,"atomic_owned_mutations_v1":true,"compound_alternate_keys_v1":true,"independent_hash_range_v1":true}"#;

/// The process-lifetime instance id shared by `/health` and `/api/version`, so
/// a client can pair a version answer with the liveness probe it followed.
pub(super) fn instance_id() -> &'static str {
    static INSTANCE_ID: OnceLock<String> = OnceLock::new();
    INSTANCE_ID.get_or_init(|| {
        let started_at_nanos = fold_db::clock::unix_nanos_wide();
        format!("{}-{started_at_nanos}", std::process::id())
    })
}

/// The version handshake for the control socket.
///
/// ```json
/// {"ok":true,"api_version":34,"build":"0.23.3-2068-g51fca83b2",
///  "capabilities":{"owned_app_schemas_v1":true,"atomic_owned_mutations_v1":true,"compound_alternate_keys_v1":true,"independent_hash_range_v1":true},"instance_id":"<pid-nanos>",
///  "min_supported_api_version":29,"current_outbox_format_version":1,
///  "min_supported_outbox_format_version":1}
/// ```
///
/// No database state and no caller bytes: `api_version` and `capabilities` are
/// compile-time constants, `build` is the string the binary installed at
/// startup, `instance_id` is process-lifetime. A client compares the
/// `api_version` it was built against with this one and prints one line that
/// names the fix (`brew upgrade lastdb`) instead of failing on its first write.
///
/// The response includes contract version ranges for both surfaces (HTTP API
/// and outbox format), allowing apps to check compatibility before install.
pub fn version_response(
    min_supported_api_version: u32,
    current_outbox_format_version: u32,
    min_supported_outbox_format_version: u32,
) -> UdsResponse {
    let build = build_version();
    let instance_id = instance_id();
    // `build` is a git-describe string (`[0-9A-Za-z.-]`) or the literal
    // `unknown`; neither needs JSON escaping. Route through serde anyway so a
    // future build string with a quote cannot produce a malformed body.
    let build = serde_json::Value::String(build.to_string());
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

/// Content-free `404 Not Found` for an unrecognized control-socket route.
///
/// **I4:** the body is the fixed status phrase; the caller's target path is not
/// echoed, so answering a probe for an unknown path leaks nothing.
pub fn not_found_response() -> UdsResponse {
    UdsResponse::new(404, "Not Found", b"Not Found".to_vec())
}

/// Content-free `405 Method Not Allowed`, advertising the supported methods.
///
/// `allow` is the router's own fixed method set for the matched path (never a
/// caller byte); the body is the fixed status phrase (**I4**).
pub fn method_not_allowed_response(allow: &str) -> UdsResponse {
    UdsResponse::new(405, "Method Not Allowed", b"Method Not Allowed".to_vec())
        .with_header("Allow", allow.to_string())
}

/// Route a parsed request and answer it, delegating the recognized *data* routes
/// to `execute_data`.
///
/// The stock routes — `Health`, `NotFound`, `MethodNotAllowed` — are answered
/// here from fixed, content-free responses. Only a [`ControlRoute::Data`] route
/// invokes `execute_data`, which receives the matched [`DataRoute`], the request,
/// and the connection's [`AccessContext`]. `lastdb_node` supplies
/// `execute_data` with access to its live state.
///
/// `socket` scopes the route table to the connection's [`SocketKind`].
/// Owner-only paths are [`ControlRoute::NotFound`] on [`SocketKind::App`].
///
/// `version_params` provides the contract version ranges for the version
/// handshake response (min_supported_api_version, current_outbox_format_version,
/// min_supported_outbox_format_version).
pub fn dispatch<F>(
    req: &UdsRequest,
    ctx: &AccessContext,
    socket: SocketKind,
    execute_data: F,
    version_params: (u32, u32, u32),
) -> UdsResponse
where
    F: FnOnce(DataRoute, &UdsRequest, &AccessContext) -> UdsResponse,
{
    match route(req, socket) {
        ControlRoute::Health => health_response(),
        ControlRoute::Version => {
            version_response(version_params.0, version_params.1, version_params.2)
        }
        ControlRoute::Data(data) => execute_data(data, req, ctx),
        ControlRoute::NotFound => not_found_response(),
        ControlRoute::MethodNotAllowed { allow } => method_not_allowed_response(allow),
    }
}
