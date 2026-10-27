//! Request router for the LastDB Mini Unix-domain sockets.
//!
//! [`super::uds_http::serve_connection`] reads a request off an accepted
//! stream. This module classifies its `(method, target)` into a [`ControlRoute`].
//! It answers state-free routes and passes data routes to the daemon's executor.
//!
//! The explicit route table scopes each route to the owner or app socket.
//! Unknown paths receive a content-free 404 or 405 response; the response does
//! not echo the caller's path. Health receives a state-free response.
//!
//! [`dispatch`] calls `execute_data` for data routes. `lastdb_node` supplies
//! that callback with access to its live state.

use fold_db::access::AccessContext;
use std::sync::OnceLock;

use super::uds_http::{UdsRequest, UdsResponse};

const API_LOG_FILTER_PATH: &str = "/api/system/log-filter";
const API_VERSION_PATH: &str = "/api/version";

/// The request-grammar version this node speaks. **The client↔node
/// compatibility handshake.**
///
/// Bump it when the socket grammar changes in a way a client can observe: a new
/// key a route accepts, a key a route stops accepting, a removed route, or a
/// changed value grammar. The `contract-diff` CI check reads this number
/// (through `lastdb_node::version::CURRENT_API_VERSION`) and fails a breaking
/// route change that does not raise it.
///
/// A client that needs a key this node does not know gets a typed
/// `unknown_key` rejection (`lastdb_host::reject`) instead of the
/// bare `Bad Request` that hid the 2026-09-03 brain 0.8.0 `durability`
/// incident for hours; comparing its required version against `GET
/// /api/version` is how it tells "wrong request" from "node too old".
///
/// `1` names the grammar that shipped as LastDB Mini `v0.23.4` (2026-09-18):
/// the first build with the per-mutation `durability` key and this route.
///
/// `2` (2026-10-26) removes three retired owner-socket routes:
/// `POST /api/db/stamp-purged-atom-retirements`,
/// `POST /api/db/reclaim-keep-small-snapshot` and `POST /api/db/pin-log-audit`.
/// No app-socket route and no request key changed, so a client that requires
/// `1` still works; `MIN_SUPPORTED_API_VERSION` stays `1`.
///
/// `3` (2026-10-26) removes the `cloud_publication` mutation key and the
/// `local_committed`, `cloud_capture` and `cloud_publication` reply fields of
/// `POST /api/mutation` and `POST /api/mutations/batch`. They carried a fixed
/// failed cloud receipt, because no cloud capture engine exists. A request
/// that sends `cloud_publication` now gets the typed `unknown_key` rejection.
/// No app in the EdgeVector repos sends the key or reads the fields, so a
/// client that requires `1` or `2` still works; `MIN_SUPPORTED_API_VERSION`
/// stays `1`.
///
/// `4` (2026-10-26) removes the two disk format v1 owner-socket routes
/// `POST /api/db/compact` and `POST /api/db/native-backup`. No app-socket
/// route and no request key changed; `MIN_SUPPORTED_API_VERSION` stays `1`.
pub const API_VERSION: u32 = 4;

/// The build string `GET /api/version` reports, installed once by the binary.
///
/// The baked build version is stamped by `lastdb_node`'s `build.rs`, which this
/// transport crate cannot see, so the binary hands it in at startup through
/// [`set_build_version`]. An unset slot reports `"unknown"` rather than failing
/// the route: the version handshake must answer even from a harness that never
/// installed a build string.
static BUILD_VERSION: OnceLock<&'static str> = OnceLock::new();

/// Install the baked build string once at startup. A second call is a no-op:
/// the first binary identity wins, and a test double cannot re-label a process.
pub fn set_build_version(build: &'static str) {
    let _ = BUILD_VERSION.set(build);
}

/// The build string the version route reports (`"unknown"` until installed).
#[must_use]
pub fn build_version() -> &'static str {
    BUILD_VERSION.get().copied().unwrap_or("unknown")
}

mod data_route;
pub use data_route::{DataRoute, SocketKind};

mod responses;
pub use responses::*;

mod route_table;

/// Classification of a control-socket request before any handler runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlRoute {
    /// Liveness probe (`GET /health`) — answerable without node state.
    Health,
    /// Version handshake (`GET /api/version`) — the node's request-grammar
    /// [`API_VERSION`], baked build string, and capability flags. Answerable
    /// without node state, on both sockets: an app must be able to ask "are we
    /// compatible?" before it sends its first data request.
    Version,
    /// A recognized app-facing data route, to be executed against node state by
    /// the dispatcher. Inert in this slice — no executor is wired yet.
    Data(DataRoute),
    /// Mint a one-time browser pairing code (`browser_owner_attestation.md`).
    /// Exists ONLY on this owner-attested channel — deliberately absent from
    /// the TCP router, so a co-resident app on the loopback port cannot reach
    /// it at all. Answered in [`dispatch`] (needs the connection's
    /// verification posture, but no node state).
    MintBrowserPairingCode,
    /// The target path is a recognized control-socket path, but not for this
    /// method. `allow` is the fixed set of methods the path supports, sent back
    /// as the `Allow` header (the node's own data, never a caller byte).
    MethodNotAllowed { allow: &'static str },
    /// No recognized control-socket route matches the target path.
    NotFound,
}

/// The path portion of a request target, dropping any `?query` or `#fragment`.
///
/// `/api/query?foo=bar` and `/api/query` route identically; the query string is
/// the handler's concern, not the router's.
fn path_only(target: &str) -> &str {
    let end = target.find(['?', '#']).unwrap_or(target.len());
    &target[..end]
}

/// Classify a parsed control-socket request into a [`ControlRoute`], scoped to
/// the [`SocketKind`] it arrived on.
///
/// Pure: matches `(method, path)` against the fixed allowlist. A recognized path
/// with the wrong method is [`ControlRoute::MethodNotAllowed`]; an unrecognized
/// path is [`ControlRoute::NotFound`]. No node state, no side effects, no caller
/// bytes retained.
///
/// **The no-mint guarantee (Option B).** The owner-only mint verb
/// (`/control/browser-pairing-code`) is classified ONLY on [`SocketKind::Owner`].
/// On [`SocketKind::App`] — the socket a jailed app reaches — that exact path
/// falls through to [`ControlRoute::NotFound`]: the route is not in the table, so
/// a confined app physically cannot mint an owner-bypass pairing code regardless
/// of its method or any header it sends. The app-facing data plane and health
/// route on both sockets; owner data routes remain absent from the app socket.
pub fn route(req: &UdsRequest, socket: SocketKind) -> ControlRoute {
    let path = path_only(&req.target);
    let method = req.method.as_str();
    let owner = socket == SocketKind::Owner;

    if let Some(route) = route_special(path, method, owner) {
        return route;
    }
    if let Some(route) = route_exact(path, method, owner) {
        return route;
    }
    route_prefixed(path, method, owner).unwrap_or(ControlRoute::NotFound)
}

/// Pick the response for a path that supports exactly one method.
fn single_method(method: &str, allow: &'static str, route: ControlRoute) -> ControlRoute {
    if method == allow {
        route
    } else {
        ControlRoute::MethodNotAllowed { allow }
    }
}

/// Paths with state-free answers, several methods, or an owner-only mint verb.
fn route_special(path: &str, method: &str, owner: bool) -> Option<ControlRoute> {
    let route = match path {
        "/health" => single_method(method, "GET", ControlRoute::Health),
        API_VERSION_PATH => single_method(method, "GET", ControlRoute::Version),
        API_LOG_FILTER_PATH if owner => match method {
            "GET" => ControlRoute::Data(DataRoute::LogFilterGet),
            "POST" => ControlRoute::Data(DataRoute::LogFilterSet),
            _ => ControlRoute::MethodNotAllowed { allow: "GET, POST" },
        },
        "/api/db/native-status" if owner => match method {
            "GET" | "POST" => ControlRoute::Data(DataRoute::DbNativeStatus),
            _ => ControlRoute::MethodNotAllowed { allow: "GET, POST" },
        },
        "/api/db/cleanup" if owner => match method {
            "GET" | "POST" => ControlRoute::Data(DataRoute::DbCleanup),
            _ => ControlRoute::MethodNotAllowed { allow: "GET, POST" },
        },
        // The counter verifier: GET reads the last result, POST runs one step.
        "/api/db/verify-refs" if owner => match method {
            "GET" => ControlRoute::Data(DataRoute::DbVerifyRefsLast),
            "POST" => ControlRoute::Data(DataRoute::DbVerifyRefs),
            _ => ControlRoute::MethodNotAllowed { allow: "GET, POST" },
        },
        "/api/db/catalog" if owner => match method {
            "GET" => ControlRoute::Data(DataRoute::DbCatalogGet),
            "POST" | "PUT" => ControlRoute::Data(DataRoute::DbCatalogPut),
            "DELETE" => ControlRoute::Data(DataRoute::DbCatalogDelete),
            _ => ControlRoute::MethodNotAllowed {
                allow: "GET, POST, PUT, DELETE",
            },
        },
        // Owner control plane: present ONLY on the owner socket. On the app
        // socket the path is unrecognized → NotFound (the structural no-mint
        // guarantee — the route simply does not exist for a jailed app).
        "/control/browser-pairing-code" if owner => {
            single_method(method, "POST", ControlRoute::MintBrowserPairingCode)
        }
        // Retired app-facing protein writes. Proteins are bound by the node on
        // schema load and folded on the mutation path, so these have no caller
        // left inside the engine and must not have one outside it either.
        // Answered as NotFound rather than MethodNotAllowed on purpose: a
        // pre-cutover client probes for the routes and treats their absence as
        // "this node has no protein API", which is its cue to fall back to its
        // own writes instead of failing the user's mutation.
        "/api/protein" | "/api/protein/member" | "/api/protein/write" | "/api/protein/fold" => {
            ControlRoute::NotFound
        }
        _ => return None,
    };
    Some(route)
}

/// Exact-path, single-method routes from [`route_table::EXACT_ROUTES`].
///
/// The owner-only verbs include node identity (a jailed app must never learn
/// it) and every history/disclosure read; on the app socket they are absent.
fn route_exact(path: &str, method: &str, owner: bool) -> Option<ControlRoute> {
    let row = route_table::EXACT_ROUTES
        .iter()
        .find(|row| row.path == path && (owner || row.scope == route_table::Scope::Both))?;
    Some(single_method(
        method,
        row.method,
        ControlRoute::Data(row.route),
    ))
}

/// The segment after `prefix` when it is one non-empty segment with no `/`.
fn one_segment<'a>(path: &'a str, prefix: &str) -> Option<&'a str> {
    path.strip_prefix(prefix)
        .filter(|rest| !rest.is_empty() && !rest.contains('/'))
}

/// Routes whose path carries an id segment.
fn route_prefixed(path: &str, method: &str, owner: bool) -> Option<ControlRoute> {
    if owner {
        if let Some(route) = route_owner_prefixed(path, method) {
            return Some(route);
        }
    }
    // Before the `{uuid}` arm below: a two-segment tail would fail that
    // arm's one-segment guard and fall through to NotFound.
    if one_segment(path, "/api/protein/of-molecule/").is_some() {
        return Some(single_method(
            method,
            "GET",
            ControlRoute::Data(DataRoute::ProteinOfMolecule),
        ));
    }
    if one_segment(path, "/api/protein/").is_some() {
        return Some(single_method(
            method,
            "GET",
            ControlRoute::Data(DataRoute::ProteinGet),
        ));
    }
    if one_segment(path, "/api/schema/").is_some() {
        return Some(single_method(
            method,
            "GET",
            ControlRoute::Data(DataRoute::GetSchema),
        ));
    }
    if one_segment(path, "/api/app/blob/cas/sha256/").is_some() {
        return Some(blob_route(
            method,
            DataRoute::AppBlobPut,
            DataRoute::AppBlobGet,
        ));
    }
    let org_blob = path
        .strip_prefix("/api/app/org/")
        .and_then(|rest| rest.split_once("/blob/cas/sha256/"))
        .is_some_and(|(org_hash, hash)| {
            !org_hash.is_empty()
                && !org_hash.contains('/')
                && !hash.is_empty()
                && !hash.contains('/')
        });
    if org_blob {
        return Some(blob_route(
            method,
            DataRoute::AppOrgBlobPut,
            DataRoute::AppOrgBlobGet,
        ));
    }
    None
}

fn blob_route(method: &str, put: DataRoute, get: DataRoute) -> ControlRoute {
    match method {
        "PUT" => ControlRoute::Data(put),
        "GET" => ControlRoute::Data(get),
        _ => ControlRoute::MethodNotAllowed { allow: "GET, PUT" },
    }
}

/// Owner-socket-only routes with an id segment.
///
/// Molecule history + atom content: OWNER socket only. History spans every
/// record of the owning (schema, field) and atom-by-uuid is the terminal
/// disclosure in that chain, so neither may exist for a jailed app. This is
/// the "write everything, resolve in history" read surface: concurrent-write
/// branches, conflict winners AND losers, become readable instead of orphaned.
fn route_owner_prefixed(path: &str, method: &str) -> Option<ControlRoute> {
    if one_segment(path, "/api/history/").is_some() {
        return Some(single_method(
            method,
            "GET",
            ControlRoute::Data(DataRoute::MoleculeHistory),
        ));
    }
    if one_segment(path, "/api/atom/").is_some() {
        return Some(single_method(
            method,
            "GET",
            ControlRoute::Data(DataRoute::AtomContent),
        ));
    }
    None
}
