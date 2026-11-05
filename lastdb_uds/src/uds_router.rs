//! Request router for the LastDB Mini Unix-domain sockets.
//!
//! [`super::uds_http::receive_connection`] reads a request off an accepted
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
/// Bump it when an app-socket route breaks: a removed route, a required request
/// or response field changing type or meaning, or tighter validation. Optional
/// additions, new routes, bug fixes, new builds, and owner-only changes do not
/// raise it. The `contract-diff` CI check reads this number
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
/// `POST /api/db/reclaim-keep-small-snapshot`, `POST /api/db/pin-log-audit`,
/// and one retired purge route.
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
///
/// `5` (2026-10-27) removes the legacy Cloud Sync owner-socket route
/// `POST /api/sync/cloud-off`. No app-socket route and no request key
/// changed; `MIN_SUPPORTED_API_VERSION` stays `1`.
///
/// `6` (2026-10-27) removes the two staged-delivery owner-socket routes
/// `GET /api/sharing/deliveries` and `POST /api/sharing/deliveries/{id}/reject`.
/// No app-socket route and no request key changed; `MIN_SUPPORTED_API_VERSION`
/// stays `1`.
///
/// `7` (2026-10-28) removes the two at-rest migration owner-socket routes
/// `POST /api/db/reseal-at-rest` and `POST /api/db/reap-unsealed`. No
/// app-socket route and no request key changed; `MIN_SUPPORTED_API_VERSION`
/// stays `1`.
///
/// `8` (2026-10-28) adds the owner-only `POST /api/sync/logical-v1` route.
/// Existing requests remain compatible; `MIN_SUPPORTED_API_VERSION` stays `1`.
///
/// `9` (2026-10-28) adds local snapshot prepare, inspect, resume and release
/// actions to the owner-only logical sync route. The minimum stays `1`.
///
/// `10` (2026-10-29) adds read-only snapshot adoption plans on the owner route.
/// Existing actions and the minimum API version remain unchanged.
/// 11 adds named alternate-key query pages in the unmerged spike.
/// 12 adds owner-directed alternate changes, named key lists/counts, and signed declaration revisions.
/// `14` (2026-10-31) removes one owner-only schema map repair route.
/// Other requests are unchanged; `MIN_SUPPORTED_API_VERSION` stays `1`.
/// `15` (2026-10-31) removes the owner-only routes that no current home
/// serves: the drain, the unresolved-count read, the key repair,
/// `POST /api/db/alternate-keys`, `POST /api/db/repair-hashrange-key-fields`,
/// `POST /api/db/clear-history`, `POST /api/storage/schema`,
/// `GET /api/storage/schemas`, `POST /api/aggregate/repair`,
/// `POST /api/aggregate/finalize`, the history read and the record read.
/// `POST /api/mutation` no longer accepts the `aggregate_set` key (the home
/// already refused it), and the `atomic_aggregate_set_v1` and
/// `alternate_key_changes_v1` capabilities are gone. No other app-socket
/// request changed; `MIN_SUPPORTED_API_VERSION` stays `1`.
/// `16` (2026-11-01) removes the `independent_fields` key from the schema
/// list, the schema detail and the declare reply. Installed apps accept a
/// missing key from API 13 and later. No request changed;
/// `MIN_SUPPORTED_API_VERSION` stays `1`.
/// `17` (2026-11-01) removes the always-zero count keys from the owner-only
/// storage reports `GET /api/db/schemas` and `GET /api/storage/app`.
/// No request changed; `MIN_SUPPORTED_API_VERSION` stays `1`.
/// `18` (2026-11-01) removes `unattributable_plane_bytes` from the owner-only
/// `GET /api/storage/app` reply. It counted bookkeeping that the home does
/// not have, so it was always zero. No request changed;
/// `MIN_SUPPORTED_API_VERSION` stays `1`.
/// `19` (2026-11-01) removes reply keys that the home always left
/// empty: the empty value id on query field metadata, `integrity` on
/// `/api/status`, the warm count on native disk status, the value byte count
/// and the tracked count on warm byte stats, and the empty value id on a
/// field history entry. No request changed; `MIN_SUPPORTED_API_VERSION`
/// stays `1`.
/// `20` (2026-11-01) renames the field history route to
/// `POST /api/field-history`. Status, inventory, restore, and share replies
/// use the field-value, verify-ref, and record-count names.
/// `MIN_SUPPORTED_API_VERSION` stays `1`.
/// `21` (2026-11-01) gives a query record one `metadata` block,
/// `{source_file_name, metadata?}` or null, in place of the per-field
/// metadata map. A query record carries `has_conflicts` and
/// `conflicted_fields`; `author_pub_key`, `field_key_id`, and `writer_pubkey`
/// are removed. Query pages and counts remove `unresolved_rows` and
/// `tombstoned_rows`. Mutation replies remove `touched_group_ids` and the
/// `gate_wait` and `publish` stages. Share slices remove `field_key_id`,
/// `writer_pubkey`, and `signature`. Restore replies rename `verified_rows` to
/// `verified_records`, and the record proof stage is `records`. The local
/// watch reply names its sequence `last_seq`. The delete ledger removes its
/// always-zero counters, `version`, and the always-true `committed`, and
/// renames `records_converged` to `records_erased`. `GET /api/db/inventory`
/// removes `attribution` and writes nothing. `GET /api/storage/home` removes
/// `inclusive_app_attribution`, which summed field key counters that no write
/// fills. Native disk status removes `warm_record_limit`, `core_warm_paths`, the
/// partial-record path count, `core_warm_field_keys`, `core_warm_dirty`, and
/// the always-zero dirty and current byte keys of `core_model`. `/api/status`
/// removes the partial-record key count, `resident_dirty_keys`, the unserved
/// `resident_key_budget`, the constant notes `resident_graph_bounds` and the
/// key memory limit, and the purge stat `purge_records_deleted`; its
/// `memory_budget.core_warm` removes `held_bytes`, `dirty_bytes`, and
/// `dirty_limit_bytes`. `POST /api/admin/shed` removes `groups_evicted`,
/// `effective_warm_bytes`, and `target_warm_bytes`, and names its purge count
/// `records_purged`. Its `native_memory` block reports `records` and
/// `held_records` in place of the previous record counts, without the
/// always-zero unflushed count and `complete_key_bytes`; `native_memory.warm`
/// removes `path_bytes`, `slot_bytes`, `link_bytes`, `held_bytes`,
/// `dirty_bytes`, and `dirty_limit_bytes`. A read that cannot resolve a
/// selected record reports the `MissingSelectedRecord` error identity.
/// The owner-only routes `GET /api/storage/app` and
/// `POST /api/storage/app/reconcile` are removed: no write filled their
/// per-schema meters, so the report was always empty.
/// No other request changed; `MIN_SUPPORTED_API_VERSION` stays `1`.
/// `22` (2026-11-02) removes the declare refusal of a non-empty
/// `field_mappers` schema key. The node ignores the key, the same as any other
/// unknown schema key; no EdgeVector app sends it. No other request changed;
/// `MIN_SUPPORTED_API_VERSION` stays `1`.
/// `23` (2026-11-02) gives a record at most one file, addressed by schema and
/// key (design `design-lastdb-no-shared-file-blobs`). The owner-only routes
/// `POST /api/db/put-blob`, `POST /api/db/fetch-blob` and
/// `POST /api/db/delete-blob` replace `put-blob-local`, `fetch-file-blob`
/// and the `$lastdb_file` pointer value: no field value names a file. A
/// query record's `metadata` block carries the record's `file` marker
/// `{name?, media_type?, size_bytes, sha256}`. A share slice has no `blobs`.
/// `POST /api/storage/liveness/explain` and the `blob_counters` block of
/// `/api/status` are removed with the blob counter. `verify-refs` reports
/// only the retention queue. The delete ledger names `file_records_erased`.
/// `24` (2026-11-03) removes the app blob routes
/// `PUT|GET /api/app/blob/cas/sha256/{hash}`,
/// `PUT|GET /api/app/org/{org_hash}/blob/cas/sha256/{hash}` and
/// `GET /api/app/blob/footprint`. They had no backend and answered a
/// content-free 404, which an unknown path still answers. No other request
/// changed; `MIN_SUPPORTED_API_VERSION` stays `1`.
/// `25` (2026-11-03) removes the kept conflict rows. It removes
/// `POST /api/conflicts/keys`, `GET|POST /api/db/verify-refs` and
/// `GET /api/db/verify-refs/scopes`. A query record has no `has_conflicts`
/// or `conflicted_fields`, and a query page has no `conflict_flags`.
/// `/api/status` removes the `verify_refs` and `conflict_retention` blocks
/// and the `annotate` request phase. `lastdb db recount` only raises the
/// replay cutoff. No other request changed; `MIN_SUPPORTED_API_VERSION`
/// stays `1`.
/// `26` (2026-11-03) removes the org database catalog: the owner-only routes
/// `GET|POST|PUT|DELETE /api/db/catalog` and `POST /api/db/catalog/share`, the
/// `catalog_membership_denied` error, and the `database` key of
/// `POST /api/field-history`. The node no longer reads the `X-LastDB-Db`
/// header. It also removes the
/// owner-only routes `GET /api/storage/home` and
/// `POST /api/storage/home/reconcile`. No other request changed;
/// `MIN_SUPPORTED_API_VERSION` stays `1`.
/// API 27 requires complete record mutations, external keys and record revisions.
/// `28` is the previous request grammar.
/// `29` (2026-11-04) removes `fields` and `secondary_concurrency` from
/// `POST /api/query`. A query returns every stored field of each live record.
/// A request that still sends `fields` is rejected.
/// `MIN_SUPPORTED_API_VERSION` moves to `29`.
/// `30` (2026-11-05) removes the app SDK org-database locator. The node
/// stopped reading `X-LastDB-Db` at API 26, when the org catalog routes
/// left. The SDK no longer sends that header or exports the locator. No
/// request key changed; `MIN_SUPPORTED_API_VERSION` stays `29`.
pub const API_VERSION: u32 = 30;

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
    /// method.
    MethodNotAllowed {
        /// Methods the path supports, sent as the `Allow` header. This is the
        /// node's own data, never a caller byte.
        allow: &'static str,
    },
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
        // Owner control plane: present ONLY on the owner socket. On the app
        // socket the path is unrecognized → NotFound (the structural no-mint
        // guarantee — the route simply does not exist for a jailed app).
        "/control/browser-pairing-code" if owner => {
            single_method(method, "POST", ControlRoute::MintBrowserPairingCode)
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
    if one_segment(path, "/api/schema/").is_some() {
        return Some(single_method(
            method,
            "GET",
            ControlRoute::Data(DataRoute::GetSchema),
        ));
    }
    None
}

/// Owner-socket-only history route.
///
/// Field history spans every record of the owning (schema, field), so it may
/// not exist for a jailed app.
fn route_owner_prefixed(path: &str, method: &str) -> Option<ControlRoute> {
    if path == "/api/field-history" {
        return Some(single_method(
            method,
            "POST",
            ControlRoute::Data(DataRoute::FieldHistory),
        ));
    }
    None
}
