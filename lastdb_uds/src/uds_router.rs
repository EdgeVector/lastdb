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

const API_QUERY_PATH: &str = "/api/query";
const API_QUERY_BATCH_PATH: &str = "/api/queries/batch";
const API_MUTATION_PATH: &str = "/api/mutation";
const API_AGGREGATE_REPAIR_PATH: &str = "/api/aggregate/repair";
const API_AGGREGATE_FINALIZE_PATH: &str = "/api/aggregate/finalize";
const API_SCHEMAS_PATH: &str = "/api/schemas";
const API_AUTO_IDENTITY_PATH: &str = "/api/system/auto-identity";
const API_BOOT_IDENTITY_PATH: &str = "/api/system/boot-identity";
const API_BOOT_LEDGER_PATH: &str = "/api/system/boot-ledger";
const API_LOG_FILTER_PATH: &str = "/api/system/log-filter";
const API_APP_SEARCH_PATH: &str = "/api/app/search";
const API_APP_CHANGES_PATH: &str = "/api/app/changes";
const API_VERSION_PATH: &str = "/api/version";

/// The request-grammar version this node speaks. **The client↔node
/// compatibility handshake.**
///
/// Bump it when the socket grammar changes in a way a client can observe: a new
/// key a route accepts, a key a route stops accepting, or a changed value
/// grammar. A client that needs a key this node does not know gets a
/// typed `unknown_key` rejection (`lastdb_host::reject`) instead of the
/// bare `Bad Request` that hid the 2026-09-03 brain 0.8.0 `durability`
/// incident for hours; comparing its required version against `GET
/// /api/version` is how it tells "wrong request" from "node too old".
///
/// `1` names the grammar that shipped as LastDB Mini `v0.23.4` (2026-09-18):
/// the first build with the per-mutation `durability` key and this route.
pub const API_VERSION: u32 = 1;

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
    match path {
        "/health" => match req.method.as_str() {
            "GET" => ControlRoute::Health,
            _ => ControlRoute::MethodNotAllowed { allow: "GET" },
        },
        API_VERSION_PATH => match req.method.as_str() {
            "GET" => ControlRoute::Version,
            _ => ControlRoute::MethodNotAllowed { allow: "GET" },
        },
        API_SCHEMAS_PATH => match req.method.as_str() {
            "GET" => ControlRoute::Data(DataRoute::ListSchemas),
            _ => ControlRoute::MethodNotAllowed { allow: "GET" },
        },
        "/api/list" | "/api/db/list" => match req.method.as_str() {
            "GET" => ControlRoute::Data(DataRoute::ListRecordKeys),
            _ => ControlRoute::MethodNotAllowed { allow: "GET" },
        },
        "/api/schemas/declare" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::DeclareSchema),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/schemas/seed-system" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::SeedSystemSchema),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/schemas/retire-name-claim" if socket == SocketKind::Owner => {
            match req.method.as_str() {
                "POST" => ControlRoute::Data(DataRoute::RetireSchemaNameClaim),
                _ => ControlRoute::MethodNotAllowed { allow: "POST" },
            }
        }
        "/api/schemas/drop" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::DropSchema),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/status" if socket == SocketKind::Owner => match req.method.as_str() {
            "GET" => ControlRoute::Data(DataRoute::Status),
            _ => ControlRoute::MethodNotAllowed { allow: "GET" },
        },
        "/api/admin/shed" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::AdminShed),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        API_LOG_FILTER_PATH if socket == SocketKind::Owner => match req.method.as_str() {
            "GET" => ControlRoute::Data(DataRoute::LogFilterGet),
            "POST" => ControlRoute::Data(DataRoute::LogFilterSet),
            _ => ControlRoute::MethodNotAllowed { allow: "GET, POST" },
        },
        "/api/db/inventory" if socket == SocketKind::Owner => match req.method.as_str() {
            "GET" => ControlRoute::Data(DataRoute::DbInventory),
            _ => ControlRoute::MethodNotAllowed { allow: "GET" },
        },
        "/api/db/schemas" if socket == SocketKind::Owner => match req.method.as_str() {
            "GET" => ControlRoute::Data(DataRoute::DbSchemas),
            _ => ControlRoute::MethodNotAllowed { allow: "GET" },
        },
        "/api/storage/schema" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::SchemaStorage),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/storage/schemas" if socket == SocketKind::Owner => match req.method.as_str() {
            "GET" => ControlRoute::Data(DataRoute::SchemaStorageReport),
            _ => ControlRoute::MethodNotAllowed { allow: "GET" },
        },
        "/api/storage/liveness/explain" if socket == SocketKind::Owner => {
            match req.method.as_str() {
                "POST" => ControlRoute::Data(DataRoute::LivenessExplain),
                _ => ControlRoute::MethodNotAllowed { allow: "POST" },
            }
        }
        "/api/storage/liveness/bootstrap" if socket == SocketKind::Owner => {
            match req.method.as_str() {
                "POST" => ControlRoute::Data(DataRoute::LivenessBootstrap),
                _ => ControlRoute::MethodNotAllowed { allow: "POST" },
            }
        }
        "/api/db/clear-history" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::DbClearHistory),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/db/compact" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::DbCompact),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/db/stamp-purged-atom-retirements" if socket == SocketKind::Owner => {
            match req.method.as_str() {
                "POST" => ControlRoute::Data(DataRoute::DbStampPurgedAtomRetirements),
                _ => ControlRoute::MethodNotAllowed { allow: "POST" },
            }
        }
        "/api/db/compact-record" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::CompactRecord),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/db/purge-schemaidx" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::DbPurgeSchemaIdx),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/db/gc-atoms" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::DbGcAtoms),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/db/reap-dropped-schema" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::DbReapDroppedSchema),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/db/gc-file-blobs" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::DbGcFileBlobs),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/db/gc-proteins" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::DbGcProteins),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/db/purge-ref-blobs" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::DbPurgeRefBlobs),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/db/reclaim-keep-small-legacy" if socket == SocketKind::Owner => {
            match req.method.as_str() {
                "POST" => ControlRoute::Data(DataRoute::DbReclaimKeepSmallLegacy),
                _ => ControlRoute::MethodNotAllowed { allow: "POST" },
            }
        }
        "/api/db/reclaim-keep-small-snapshot" if socket == SocketKind::Owner => {
            match req.method.as_str() {
                "POST" => ControlRoute::Data(DataRoute::DbReclaimKeepSmallSnapshot),
                _ => ControlRoute::MethodNotAllowed { allow: "POST" },
            }
        }
        "/api/db/repair-dangling-tips" if socket == SocketKind::Owner => {
            match req.method.as_str() {
                "POST" => ControlRoute::Data(DataRoute::DbRepairDanglingTips),
                _ => ControlRoute::MethodNotAllowed { allow: "POST" },
            }
        }
        "/api/db/unresolved-atoms" if socket == SocketKind::Owner => match req.method.as_str() {
            "GET" => ControlRoute::Data(DataRoute::DbUnresolvedAtoms),
            _ => ControlRoute::MethodNotAllowed { allow: "GET" },
        },
        "/api/db/drain-tip-history" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::DbDrainTipHistory),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/db/retain-superseded-versions" if socket == SocketKind::Owner => {
            match req.method.as_str() {
                "POST" => ControlRoute::Data(DataRoute::DbRetainSupersededVersions),
                _ => ControlRoute::MethodNotAllowed { allow: "POST" },
            }
        }
        "/api/db/probe-locator-only" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::DbProbeLocatorOnly),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/db/delete-ledger" if socket == SocketKind::Owner => match req.method.as_str() {
            "GET" => ControlRoute::Data(DataRoute::DbDeleteLedger),
            _ => ControlRoute::MethodNotAllowed { allow: "GET" },
        },
        "/api/db/migrate-photo-blobs" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::DbMigratePhotoBlobs),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/db/migrate-thin-tips" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::DbMigrateThinTips),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/db/rekey-atom-partition-prefix" if socket == SocketKind::Owner => {
            match req.method.as_str() {
                "POST" => ControlRoute::Data(DataRoute::DbRekeyAtomPartitionPrefix),
                _ => ControlRoute::MethodNotAllowed { allow: "POST" },
            }
        }
        "/api/db/reseal-at-rest" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::DbResealAtRest),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/db/reap-unsealed" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::DbReapUnsealed),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/db/tombstone-flag-audit" if socket == SocketKind::Owner => {
            match req.method.as_str() {
                "POST" => ControlRoute::Data(DataRoute::DbTombstoneFlagAudit),
                _ => ControlRoute::MethodNotAllowed { allow: "POST" },
            }
        }
        "/api/db/drain-legacy-tombstones" if socket == SocketKind::Owner => {
            match req.method.as_str() {
                "POST" => ControlRoute::Data(DataRoute::DbDrainLegacyTombstones),
                _ => ControlRoute::MethodNotAllowed { allow: "POST" },
            }
        }
        "/api/db/legacy-key-fork-audit" if socket == SocketKind::Owner => {
            match req.method.as_str() {
                "POST" => ControlRoute::Data(DataRoute::DbLegacyKeyForkAudit),
                _ => ControlRoute::MethodNotAllowed { allow: "POST" },
            }
        }
        "/api/db/order-log-audit" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::DbOrderLogAudit),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/db/pin-log-audit" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::DbPinLogAudit),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/db/order-log-bloat-audit" if socket == SocketKind::Owner => match req.method.as_str()
        {
            "POST" => ControlRoute::Data(DataRoute::DbOrderLogBloatAudit),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/db/compact-order-log" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::DbCompactOrderLog),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/db/repair-order-log-shortfall" if socket == SocketKind::Owner => {
            match req.method.as_str() {
                "POST" => ControlRoute::Data(DataRoute::DbRepairOrderLogShortfall),
                _ => ControlRoute::MethodNotAllowed { allow: "POST" },
            }
        }
        "/api/db/molecule-keys" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::DbMoleculeKeys),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/db/schema-retention" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::DbSchemaRetention),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/db/repair-schema-molecule-map" if socket == SocketKind::Owner => {
            match req.method.as_str() {
                "POST" => ControlRoute::Data(DataRoute::DbRepairSchemaMoleculeMap),
                _ => ControlRoute::MethodNotAllowed { allow: "POST" },
            }
        }
        "/api/db/repair-hashrange-key-fields" if socket == SocketKind::Owner => {
            match req.method.as_str() {
                "POST" => ControlRoute::Data(DataRoute::DbRepairHashRangeKeyFields),
                _ => ControlRoute::MethodNotAllowed { allow: "POST" },
            }
        }
        "/api/db/drain-plane-residue" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::DbDrainPlaneResidue),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/db/fetch-file-blob" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::DbFetchFileBlob),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/db/fork-file-blob" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::DbForkFileBlob),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/db/file-blob" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::DbPutFileBlob),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/db/put-blob-local" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::DbPutBlobLocal),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/db/catalog" if socket == SocketKind::Owner => match req.method.as_str() {
            "GET" => ControlRoute::Data(DataRoute::DbCatalogGet),
            "POST" | "PUT" => ControlRoute::Data(DataRoute::DbCatalogPut),
            "DELETE" => ControlRoute::Data(DataRoute::DbCatalogDelete),
            _ => ControlRoute::MethodNotAllowed {
                allow: "GET, POST, PUT, DELETE",
            },
        },
        "/api/db/catalog/share" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::DbCatalogShare),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/db/catalog/reclaim" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::DbCatalogReclaim),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/org/sync/register" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::OrgSyncRegister),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/org/sync/targets" if socket == SocketKind::Owner => match req.method.as_str() {
            "GET" => ControlRoute::Data(DataRoute::OrgSyncTargets),
            _ => ControlRoute::MethodNotAllowed { allow: "GET" },
        },
        "/api/org/sync/deactivate" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::OrgSyncDeactivate),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/org/sync/grant-member" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::OrgSyncGrantMember),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/org/sync/revoke-member" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::OrgSyncRevokeMember),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/sync/heal-staging" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::SyncHealStaging),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/native-index/search" if socket == SocketKind::Owner => match req.method.as_str() {
            "GET" => ControlRoute::Data(DataRoute::NativeIndexSearch),
            _ => ControlRoute::MethodNotAllowed { allow: "GET" },
        },
        "/api/sync/laststore-snapshot" if socket == SocketKind::Owner => {
            match req.method.as_str() {
                "POST" => ControlRoute::Data(DataRoute::SyncLastStoreSnapshot),
                _ => ControlRoute::MethodNotAllowed { allow: "POST" },
            }
        }
        "/api/sync/backup-gc" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::SyncBackupGc),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/sync/prefix-inventory" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::SyncPrefixInventory),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/sync/cloud-off" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::SyncCloudOff),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/sync/cloud-on" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::SyncCloudOn),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/sync/cloud-resume-primary" if socket == SocketKind::Owner => {
            match req.method.as_str() {
                "POST" => ControlRoute::Data(DataRoute::SyncCloudResumePrimary),
                _ => ControlRoute::MethodNotAllowed { allow: "POST" },
            }
        }
        "/api/sync/quarantine-replay-blocker" if socket == SocketKind::Owner => {
            match req.method.as_str() {
                "POST" => ControlRoute::Data(DataRoute::SyncQuarantineReplayBlocker),
                _ => ControlRoute::MethodNotAllowed { allow: "POST" },
            }
        }
        "/api/sync/backup-concurrency" if socket == SocketKind::Owner => {
            match req.method.as_str() {
                "POST" => ControlRoute::Data(DataRoute::SyncBackupConcurrency),
                _ => ControlRoute::MethodNotAllowed { allow: "POST" },
            }
        }
        "/api/sharing/deliver" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::DeliverStage),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/sharing/snapshot" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::DeliverSnapshot),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/sharing/deliveries" if socket == SocketKind::Owner => match req.method.as_str() {
            "GET" => ControlRoute::Data(DataRoute::DeliverList),
            _ => ControlRoute::MethodNotAllowed { allow: "GET" },
        },
        path if socket == SocketKind::Owner
            && path
                .strip_prefix("/api/sharing/deliveries/")
                .is_some_and(|rest| {
                    let mut parts = rest.split('/');
                    matches!(
                        (parts.next(), parts.next(), parts.next()),
                        (Some(id), Some("approve"), None) if !id.is_empty()
                    )
                }) =>
        {
            match req.method.as_str() {
                "POST" => ControlRoute::Data(DataRoute::DeliverApprove),
                _ => ControlRoute::MethodNotAllowed { allow: "POST" },
            }
        }
        path if socket == SocketKind::Owner
            && path
                .strip_prefix("/api/sharing/deliveries/")
                .is_some_and(|rest| {
                    let mut parts = rest.split('/');
                    matches!(
                        (parts.next(), parts.next(), parts.next()),
                        (Some(id), Some("reject"), None) if !id.is_empty()
                    )
                }) =>
        {
            match req.method.as_str() {
                "POST" => ControlRoute::Data(DataRoute::DeliverReject),
                _ => ControlRoute::MethodNotAllowed { allow: "POST" },
            }
        }
        path if path
            .strip_prefix("/api/schema/")
            .is_some_and(|name| !name.is_empty() && !name.contains('/')) =>
        {
            match req.method.as_str() {
                "GET" => ControlRoute::Data(DataRoute::GetSchema),
                _ => ControlRoute::MethodNotAllowed { allow: "GET" },
            }
        }
        API_QUERY_PATH => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::Query),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        API_MUTATION_PATH => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::Mutation),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        API_AGGREGATE_REPAIR_PATH if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::AggregateRepair),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        API_AGGREGATE_FINALIZE_PATH if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::AggregateFinalize),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/mutations/batch" => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::MutationBatch),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        API_QUERY_BATCH_PATH => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::QueryBatch),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        // Local mutation doorbell — same sockets as query/mutation (owner + app).
        "/api/local-watch" => match req.method.as_str() {
            "GET" => ControlRoute::Data(DataRoute::LocalWatch),
            _ => ControlRoute::MethodNotAllowed { allow: "GET" },
        },
        API_APP_CHANGES_PATH => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::AppChanges),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        API_APP_SEARCH_PATH => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::AppSearch),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        path if path
            .strip_prefix("/api/app/blob/cas/sha256/")
            .is_some_and(|hash| !hash.is_empty() && !hash.contains('/')) =>
        {
            match req.method.as_str() {
                "PUT" => ControlRoute::Data(DataRoute::AppBlobPut),
                "GET" => ControlRoute::Data(DataRoute::AppBlobGet),
                _ => ControlRoute::MethodNotAllowed { allow: "GET, PUT" },
            }
        }
        path if path
            .strip_prefix("/api/app/org/")
            .and_then(|rest| rest.split_once("/blob/cas/sha256/"))
            .is_some_and(|(org_hash, hash)| {
                !org_hash.is_empty()
                    && !org_hash.contains('/')
                    && !hash.is_empty()
                    && !hash.contains('/')
            }) =>
        {
            match req.method.as_str() {
                "PUT" => ControlRoute::Data(DataRoute::AppOrgBlobPut),
                "GET" => ControlRoute::Data(DataRoute::AppOrgBlobGet),
                _ => ControlRoute::MethodNotAllowed { allow: "GET, PUT" },
            }
        }
        "/api/app/blob/footprint" => match req.method.as_str() {
            "GET" => ControlRoute::Data(DataRoute::AppBlobFootprint),
            _ => ControlRoute::MethodNotAllowed { allow: "GET" },
        },
        "/api/storage/app" if socket == SocketKind::Owner => match req.method.as_str() {
            "GET" => ControlRoute::Data(DataRoute::AppStorage),
            _ => ControlRoute::MethodNotAllowed { allow: "GET" },
        },
        "/api/storage/app/reconcile" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::AppStorageReconcile),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/storage/home" if socket == SocketKind::Owner => match req.method.as_str() {
            "GET" => ControlRoute::Data(DataRoute::HomeStorage),
            _ => ControlRoute::MethodNotAllowed { allow: "GET" },
        },
        "/api/storage/home/reconcile" if socket == SocketKind::Owner => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::HomeStorageReconcile),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        // Owner control plane: present ONLY on the owner socket. On the app
        // socket the path is unrecognized → NotFound (the structural no-mint
        // guarantee — the route simply does not exist for a jailed app).
        "/control/browser-pairing-code" if socket == SocketKind::Owner => match req.method.as_str()
        {
            "POST" => ControlRoute::MintBrowserPairingCode,
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        // Node-identity probe: OWNER socket only. The node's public key is an
        // owner-only read — a jailed app must never learn the node identity, so
        // on the app socket this path is unrecognized → NotFound (same
        // structural deny as the mint verb). Serves the CLI startup preflight
        // over the socket.
        API_AUTO_IDENTITY_PATH if socket == SocketKind::Owner => match req.method.as_str() {
            "GET" => ControlRoute::Data(DataRoute::AutoIdentity),
            _ => ControlRoute::MethodNotAllowed { allow: "GET" },
        },
        API_BOOT_IDENTITY_PATH if socket == SocketKind::Owner => match req.method.as_str() {
            "GET" => ControlRoute::Data(DataRoute::BootIdentity),
            _ => ControlRoute::MethodNotAllowed { allow: "GET" },
        },
        API_BOOT_LEDGER_PATH if socket == SocketKind::Owner => match req.method.as_str() {
            "GET" => ControlRoute::Data(DataRoute::BootLedger),
            _ => ControlRoute::MethodNotAllowed { allow: "GET" },
        },
        // App-supplied vector index: available on BOTH sockets, like
        // query/mutation. The put is a write into one named schema (the
        // handler enforces write-own-namespace for verified apps); the k-NN
        // read requires an explicit schema scope.
        "/api/native-index/embeddings" => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::NativeIndexAppVectorPut),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        "/api/native-index/knn" => match req.method.as_str() {
            "POST" => ControlRoute::Data(DataRoute::NativeIndexKnn),
            _ => ControlRoute::MethodNotAllowed { allow: "POST" },
        },
        // First-party Search app text query: route is available on both
        // sockets, while the executor enforces verified app identity and
        // Search-owned schema scope.
        "/api/search/query" => match req.method.as_str() {
            "GET" => ControlRoute::Data(DataRoute::SearchAppQuery),
            _ => ControlRoute::MethodNotAllowed { allow: "GET" },
        },
        // Molecule history + atom content: OWNER socket only, exactly like
        // auto-identity — history spans every record of
        // the owning (schema, field) and atom-by-uuid is the terminal
        // disclosure in that chain, so neither may exist for a jailed app.
        // This is the "write everything, resolve in history" read surface:
        // concurrent-write branches, conflict winners AND losers, become
        // readable instead of orphaned.
        path if socket == SocketKind::Owner
            && path
                .strip_prefix("/api/history/")
                .is_some_and(|uuid| !uuid.is_empty() && !uuid.contains('/')) =>
        {
            match req.method.as_str() {
                "GET" => ControlRoute::Data(DataRoute::MoleculeHistory),
                _ => ControlRoute::MethodNotAllowed { allow: "GET" },
            }
        }
        path if socket == SocketKind::Owner
            && path
                .strip_prefix("/api/atom/")
                .is_some_and(|uuid| !uuid.is_empty() && !uuid.contains('/')) =>
        {
            match req.method.as_str() {
                "GET" => ControlRoute::Data(DataRoute::AtomContent),
                _ => ControlRoute::MethodNotAllowed { allow: "GET" },
            }
        }
        // Retired app-facing protein writes. Proteins are bound by the node on
        // schema load and folded on the mutation path, so these have no caller
        // left inside the engine and must not have one outside it either.
        // Answered as NotFound rather than MethodNotAllowed on purpose: a
        // pre-cutover client probes for the routes and treats their absence as
        // "this node has no protein API", which is its cue to fall back to its
        // own writes instead of failing the user's mutation.
        //
        // Listed ahead of the `{uuid}` arm below, which would otherwise read
        // `/api/protein/member` as a request for a protein named "member".
        "/api/protein" | "/api/protein/member" | "/api/protein/write" | "/api/protein/fold" => {
            ControlRoute::NotFound
        }
        // Before the `{uuid}` arm below: a two-segment tail would fail that
        // arm's `!uuid.contains('/')` guard and fall through to NotFound.
        path if path
            .strip_prefix("/api/protein/of-molecule/")
            .is_some_and(|mol| !mol.is_empty() && !mol.contains('/')) =>
        {
            match req.method.as_str() {
                "GET" => ControlRoute::Data(DataRoute::ProteinOfMolecule),
                _ => ControlRoute::MethodNotAllowed { allow: "GET" },
            }
        }
        path if path
            .strip_prefix("/api/protein/")
            .is_some_and(|uuid| !uuid.is_empty() && !uuid.contains('/')) =>
        {
            match req.method.as_str() {
                "GET" => ControlRoute::Data(DataRoute::ProteinGet),
                _ => ControlRoute::MethodNotAllowed { allow: "GET" },
            }
        }
        _ => ControlRoute::NotFound,
    }
}
