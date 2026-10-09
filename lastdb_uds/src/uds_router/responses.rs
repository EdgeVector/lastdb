//! Fixed control-socket responses, dispatch and the browser-pairing mint verb.

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
pub(super) const CAPABILITIES_JSON: &str = r#"{"atomic_aggregate_set_v1":true}"#;

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
/// {"ok":true,"api_version":1,"build":"0.23.3-2068-g51fca83b2",
///  "capabilities":{"atomic_aggregate_set_v1":true},"instance_id":"<pid-nanos>"}
/// ```
///
/// No database state and no caller bytes: `api_version` and `capabilities` are
/// compile-time constants, `build` is the string the binary installed at
/// startup, `instance_id` is process-lifetime. A client compares the
/// `api_version` it was built against with this one and prints one line that
/// names the fix (`brew upgrade lastdb`) instead of failing on its first write.
pub fn version_response() -> UdsResponse {
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
            "{{\"ok\":true,\"api_version\":{API_VERSION},\"build\":{build},\"capabilities\":{CAPABILITIES_JSON},\"instance_id\":\"{instance_id}\"}}"
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
/// `socket` scopes the route table to the connection's [`SocketKind`]. On
/// [`SocketKind::App`] the mint verb is never reachable (it classifies as
/// [`ControlRoute::NotFound`]), so `mint_browser_pairing_code_response` cannot
/// run for a jailed app — the structural no-mint guarantee (Option B).
///
/// `mint_pairing` supplies a browser-pairing mint callback. `lastdbd` passes
/// [`no_pairing_mint`], so the mint verb answers `404`.
pub fn dispatch<F, M>(
    req: &UdsRequest,
    ctx: &AccessContext,
    socket: SocketKind,
    execute_data: F,
    mint_pairing: M,
) -> UdsResponse
where
    F: FnOnce(DataRoute, &UdsRequest, &AccessContext) -> UdsResponse,
    M: FnOnce() -> Option<PairingMint>,
{
    match route(req, socket) {
        ControlRoute::Health => health_response(),
        ControlRoute::Version => version_response(),
        ControlRoute::Data(data) => execute_data(data, req, ctx),
        ControlRoute::MintBrowserPairingCode => {
            mint_browser_pairing_code_response(ctx, mint_pairing)
        }
        ControlRoute::NotFound => not_found_response(),
        ControlRoute::MethodNotAllowed { allow } => method_not_allowed_response(allow),
    }
}

/// A freshly minted browser-pairing code, produced by the binary's pairing
/// registry and rendered by the router's mint verb.
pub struct PairingMint {
    /// The single-use pairing code handed to the socket peer (never logged).
    pub code: String,
    /// Seconds until the code expires.
    pub expires_in_seconds: u64,
}

/// Stock `mint_pairing` callback for binaries with no browser-pairing surface:
/// the mint verb answers a content-free `404` instead of minting.
pub fn no_pairing_mint() -> Option<PairingMint> {
    None
}

/// Answer the owner-channel mint verb (`browser_owner_attestation.md`).
///
/// The transport already attests a same-user caller (UDS peer creds), but a
/// **code-signature-verified app** on this socket is precisely NOT the owner —
/// it must not be able to mint a code, pair a browser it controls, and walk
/// away with an owner-bypass session. Unverified same-user callers (the
/// `folddb` CLI, launcher scripts) run with owner posture on this channel and
/// may mint. The code goes only to the socket peer; it is never logged.
pub(super) fn mint_browser_pairing_code_response<M>(
    ctx: &AccessContext,
    mint_pairing: M,
) -> UdsResponse
where
    M: FnOnce() -> Option<PairingMint>,
{
    if ctx.is_verified() {
        tracing::warn!(
            target: "fold_node::uds",
            "browser-pairing mint refused: code-signature-verified app attempted to mint"
        );
        return UdsResponse::new(
            403,
            "Forbidden",
            br#"{"ok":false,"error":"app_cannot_mint_pairing_code"}"#.to_vec(),
        )
        .with_header("Content-Type", "application/json");
    }
    let Some(mint) = mint_pairing() else {
        // This binary has no pairing surface — same content-free shape as an
        // unrecognized route (I4: nothing about the surface is leaked).
        return not_found_response();
    };
    let body = serde_json::json!({
        "ok": true,
        "pairing_code": mint.code,
        "expires_in_seconds": mint.expires_in_seconds,
    });
    UdsResponse::new(200, "OK", body.to_string().into_bytes())
        .with_header("Content-Type", "application/json")
}
