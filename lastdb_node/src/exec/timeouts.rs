use super::*;

/// Default wall-clock budget for one UDS handler's async work
/// (`LASTDB_UDS_HANDLER_TIMEOUT_SECS` overrides).
///
/// A worker is occupied for the full handler. Socket read/write has its own
/// 10s budget, but **handler** work (query/mutate/`block_on`) used to be
/// unbounded. Hung futures occupied all workers and blocked new requests.
/// This budget forces a 503 and releases the worker. It must be ≥ the QoS bulk
/// queue budget (~60s) so healthy bulk work does not time out.
pub const DEFAULT_HANDLER_TIMEOUT_SECS: u64 = 90;

/// Scheduling and response headroom around the exact cloud-publication wait.
pub const MUTATION_CLOUD_PUBLICATION_HANDLER_MARGIN_SECS: u64 = 10;

/// Longer budget for owner DB admin scans (`/api/db/inventory`,
/// `/api/db/clear-history`) — full-prefix walks over multi-GiB stores.
/// Override with `LASTDB_UDS_ADMIN_TIMEOUT_SECS`.
pub const DEFAULT_ADMIN_HANDLER_TIMEOUT_SECS: u64 = 600;

/// Default client deadline for ordinary CLI Unix-socket reads (list, get,
/// app-storage, app-publish, and the `lastdb status` health default).
///
/// Distinct from [`DEFAULT_HANDLER_TIMEOUT_SECS`] (server handler 90s) and
/// [`DEFAULT_ADMIN_HANDLER_TIMEOUT_SECS`] (admin-scan 600s). Declare-schema
/// keeps the 600s admin budget via `LASTDB_UDS_ADMIN_TIMEOUT_SECS`.
pub const DEFAULT_CLI_UDS_TIMEOUT_SECS: u64 = 30;

/// Canary Pipeline v2 treats a status response past two seconds as build
/// evidence, not as an absent signal. The status route must therefore release
/// its worker at this bound.
pub const CANARY_STATUS_HANDLER_TIMEOUT_SECS: u64 = 2;

/// The boot identity route is one durable-row read. It has a tighter budget
/// because callers use it to decide whether a fresh process is the candidate.
pub const CANARY_BOOT_IDENTITY_HANDLER_TIMEOUT_SECS: u64 = 1;

/// Effective handler deadline from env or [`DEFAULT_HANDLER_TIMEOUT_SECS`].
#[must_use]
pub fn handler_timeout() -> Duration {
    env_flag::var_parsed::<u64>("LASTDB_UDS_HANDLER_TIMEOUT_SECS")
        .filter(|&n| n > 0)
        .map_or(
            Duration::from_secs(DEFAULT_HANDLER_TIMEOUT_SECS),
            Duration::from_secs,
        )
}

/// Full server budget for one exact cloud-publication mutation request.
///
/// The publication wait starts after local mutation work completes. Derive
/// this value from the effective handler timeout so a server override cannot
/// leave exact clients with a stale fixed deadline.
#[must_use]
pub fn exact_mutation_route_budget() -> Duration {
    handler_timeout()
        .saturating_add(handlers::MUTATION_CLOUD_PUBLICATION_TIMEOUT)
        .saturating_add(Duration::from_secs(
            MUTATION_CLOUD_PUBLICATION_HANDLER_MARGIN_SECS,
        ))
}

/// The operator's explicit `LASTDB_UDS_ADMIN_TIMEOUT_SECS` override, when the
/// value is usable (a positive integer). `None` means unset or unusable, so a
/// caller with its own tighter default (the `status` health probe) keeps that
/// default instead of inheriting the 600s admin-scan budget.
#[must_use]
pub fn admin_handler_timeout_override() -> Option<Duration> {
    env_flag::var_parsed::<u64>("LASTDB_UDS_ADMIN_TIMEOUT_SECS")
        .filter(|&n| n > 0)
        .map(Duration::from_secs)
}

/// Effective admin-scan deadline from env or [`DEFAULT_ADMIN_HANDLER_TIMEOUT_SECS`].
#[must_use]
pub fn admin_handler_timeout() -> Duration {
    admin_handler_timeout_override()
        .unwrap_or(Duration::from_secs(DEFAULT_ADMIN_HANDLER_TIMEOUT_SECS))
}

/// Run `fut` under the handler wall-clock budget; on expiry return a 503 so the
/// UDS worker can accept another connection when the handler returns.
pub async fn with_handler_timeout<F>(ctx: &AccessContext, fut: F) -> UdsResponse
where
    F: std::future::Future<Output = UdsResponse>,
{
    with_handler_timeout_budget(ctx, handler_timeout(), fut).await
}

pub(super) async fn with_handler_timeout_budget<F>(
    ctx: &AccessContext,
    budget: Duration,
    fut: F,
) -> UdsResponse
where
    F: std::future::Future<Output = UdsResponse>,
{
    match tokio::time::timeout(budget, fut).await {
        Ok(resp) => resp,
        Err(_elapsed) => {
            tracing::warn!(
                timeout_secs = budget.as_secs(),
                "uds handler deadline exceeded; returning 503"
            );
            error_response(
                503,
                "node is busy: handler deadline exceeded; retry after 1s",
                ctx,
            )
        }
    }
}

/// Worker-pool entry: `block_on` + [`with_handler_timeout`] for Mini control-socket
/// handlers so a hung future cannot pin a worker forever (pairs with
/// [`lastdb_uds::UdsWorkerPool`]).
pub fn block_on_route<Fut>(
    handle: &tokio::runtime::Handle,
    ctx: &AccessContext,
    fut: Fut,
) -> UdsResponse
where
    Fut: std::future::Future<Output = UdsResponse>,
{
    handle.block_on(with_handler_timeout(ctx, fut))
}

/// Like [`block_on_route`] but with the longer admin-scan budget (inventory /
/// history clear over large stores).
pub fn block_on_admin_route<Fut>(
    handle: &tokio::runtime::Handle,
    ctx: &AccessContext,
    fut: Fut,
) -> UdsResponse
where
    Fut: std::future::Future<Output = UdsResponse>,
{
    handle.block_on(with_handler_timeout_budget(
        ctx,
        admin_handler_timeout(),
        fut,
    ))
}

/// Dispatch a data route with the right wall-clock budget (admin scans and
/// schema-mutation declare get the longer inventory/clear-history timeout).
///
/// `POST /api/schemas/declare` falls back to Schema Service register when the
/// live resolver is cold; that path solves node-key proof-of-work (diff 18 can
/// take ~80s+) plus network round-trips. The default 90s handler budget races
/// that work and returns 503 with an empty body (backup-restore probe 2026-08-07).
/// Route it with the admin budget so fresh-node declare can complete under the
/// product default (`LASTDB_UDS_HANDLER_TIMEOUT_SECS=90` still applies to other
/// routes; override admin class with `LASTDB_UDS_ADMIN_TIMEOUT_SECS`).
pub fn block_on_data_route(
    handle: &tokio::runtime::Handle,
    route: DataRoute,
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let fut = execute_data_route(route, req, ctx, host);
    handle.block_on(with_handler_timeout_budget(
        ctx,
        data_route_budget_for_request(route, req),
        fut,
    ))
}

/// Resolve the route budget plus any explicit request-owned remote wait.
///
/// A durable Delete with `cloud_publication: "wait"` starts its fixed cloud
/// wait only after the local commit completes. Give that opt-in request the
/// normal local-work budget, the full publication interval, and bounded
/// scheduling headroom. All default mutation requests keep the normal budget.
#[must_use]
pub fn data_route_budget_for_request(route: DataRoute, req: &UdsRequest) -> Duration {
    let budget = data_route_budget(route);
    if route != DataRoute::Mutation || !mutation_requests_cloud_publication_wait(req) {
        return budget;
    }

    exact_mutation_route_budget()
}

pub(super) fn mutation_requests_cloud_publication_wait(req: &UdsRequest) -> bool {
    serde_json::from_slice::<Value>(&req.body)
        .ok()
        .is_some_and(|body| body.get("cloud_publication").and_then(Value::as_str) == Some("wait"))
}

/// Wall-clock budget for one data route.
///
/// **This match is deliberately exhaustive — do not add a `_` arm.** Every
/// route added since this list was written that walks the whole keyspace
/// (`drain-legacy-tombstones`, `tombstone-flag-audit`, `order-log-audit`, `order-log-bloat-audit`,
/// `drain-plane-residue`) silently inherited the 90s default because the old
/// catch-all classified anything unlisted as short work. On a real store that
/// is not "slow" — it is fatal: `drain-legacy-tombstones` must walk ~1.9M keys
/// to find the tombstones it is asked to drain, so it 503'd at 90s **before
/// returning a resume cursor**, which meant no progress ever persisted and
/// retrying started from zero (measured 2026-08-08 on the primary: 830,474
/// tombstones, unreachable at both `--max-keys 5000` and `--max-keys 500`).
///
/// Forcing a compile error on a new variant is the point. A route author must
/// decide "is this bounded per-request work, or does it scan the store?" —
/// getting it wrong by omission is what shipped the bug.
#[must_use]
// lint:fn-size-ok moved verbatim from exec.rs; splitting this function is separate work.
pub fn data_route_budget(route: DataRoute) -> Duration {
    match route {
        // Owner admin work that walks the store, or blocks on a remote service.
        // Full-prefix scans over a multi-GiB home; minutes, not seconds.
        DataRoute::DbInventory
        | DataRoute::DbSchemas
        | DataRoute::DbClearHistory
        | DataRoute::DbCompact
        | DataRoute::DbStampPurgedAtomRetirements
        | DataRoute::CompactRecord
        | DataRoute::DbPurgeSchemaIdx
        | DataRoute::DbGcAtoms
        | DataRoute::DbReapDroppedSchema
        | DataRoute::DbGcFileBlobs
        | DataRoute::DbGcProteins
        | DataRoute::DbPurgeRefBlobs
        | DataRoute::DbReclaimKeepSmallLegacy
        | DataRoute::DbReclaimKeepSmallSnapshot
        | DataRoute::DbRepairDanglingTips
        | DataRoute::DbDrainTipHistory
        | DataRoute::DbRetainSupersededVersions
        | DataRoute::DbProbeLocatorOnly
        | DataRoute::DbMigratePhotoBlobs
        | DataRoute::DbMigrateThinTips
        | DataRoute::DbRekeyAtomPartitionPrefix
        | DataRoute::DbResealAtRest
        | DataRoute::DbReapUnsealed
        | DataRoute::DbDeleteLedger
        | DataRoute::DbTombstoneFlagAudit
        | DataRoute::DbDrainLegacyTombstones
        | DataRoute::DbLegacyKeyForkAudit
        | DataRoute::DbOrderLogAudit
        | DataRoute::DbPinLogAudit
        | DataRoute::DbOrderLogBloatAudit
        | DataRoute::DbCompactOrderLog
        | DataRoute::DbRepairOrderLogShortfall
        | DataRoute::DbRepairHashRangeKeyFields
        | DataRoute::DbDrainPlaneResidue
        | DataRoute::SyncHealStaging
        | DataRoute::SyncLastStoreSnapshot
        | DataRoute::SyncBackupGc
        | DataRoute::SyncPrefixInventory
        | DataRoute::SyncCloudOff
        | DataRoute::SyncCloudOn
        | DataRoute::SyncCloudResumePrimary
        | DataRoute::SyncQuarantineReplayBlocker
        | DataRoute::AggregateRepair
        | DataRoute::DeclareSchema
        | DataRoute::DbCatalogReclaim
        | DataRoute::LivenessBootstrap => admin_handler_timeout(),

        DataRoute::Status => Duration::from_secs(CANARY_STATUS_HANDLER_TIMEOUT_SECS),
        DataRoute::AdminShed => Duration::from_secs(60),
        // Parse one directive and swap an `Arc` — no IO, no storage, no lock
        // held across an await. Grouped with the bounded routes below in cost;
        // named here so a reader does not look for it among the storage verbs.
        DataRoute::LogFilterGet | DataRoute::LogFilterSet => Duration::from_secs(5),
        DataRoute::BootIdentity | DataRoute::BootLedger => {
            Duration::from_secs(CANARY_BOOT_IDENTITY_HANDLER_TIMEOUT_SECS)
        }

        // Bounded per-request work: one key, one page, one blob, one watch.
        DataRoute::Query
        | DataRoute::Mutation
        | DataRoute::AggregateFinalize
        | DataRoute::MutationBatch
        | DataRoute::QueryBatch
        | DataRoute::ListSchemas
        | DataRoute::ListRecordKeys
        | DataRoute::GetSchema
        | DataRoute::SyncBackupConcurrency
        | DataRoute::AutoIdentity
        | DataRoute::NativeIndexSearch
        | DataRoute::SearchAppQuery
        | DataRoute::AppSearch
        | DataRoute::NativeIndexAppVectorPut
        | DataRoute::NativeIndexKnn
        | DataRoute::MoleculeHistory
        | DataRoute::AtomContent
        // One in-memory catalog list plus at most one durable row per
        // colliding claimant. No store walk.
        | DataRoute::RetireSchemaNameClaim
        | DataRoute::DropSchema
        | DataRoute::SeedSystemSchema
        | DataRoute::AppBlobPut
        | DataRoute::AppBlobGet
        | DataRoute::AppOrgBlobPut
        | DataRoute::AppOrgBlobGet
        | DataRoute::AppBlobFootprint
        | DataRoute::AppStorage
        | DataRoute::AppStorageReconcile
        | DataRoute::HomeStorage
        | DataRoute::HomeStorageReconcile
        | DataRoute::SchemaStorage
        | DataRoute::SchemaStorageReport
        | DataRoute::LivenessExplain
        | DataRoute::OrgSyncRegister
        | DataRoute::OrgSyncTargets
        | DataRoute::OrgSyncDeactivate
        | DataRoute::OrgSyncGrantMember
        | DataRoute::OrgSyncRevokeMember
        | DataRoute::DbCatalogGet
        | DataRoute::DbCatalogPut
        | DataRoute::DbCatalogDelete
        | DataRoute::DbCatalogShare
        | DataRoute::DbMoleculeKeys
        | DataRoute::DbUnresolvedAtoms
        | DataRoute::DbSchemaRetention
        | DataRoute::DbRepairSchemaMoleculeMap
        | DataRoute::DbFetchFileBlob
        | DataRoute::DbForkFileBlob
        | DataRoute::DbPutFileBlob
        | DataRoute::DbPutBlobLocal
        | DataRoute::DeliverStage
        | DataRoute::DeliverSnapshot
        | DataRoute::DeliverList
        | DataRoute::DeliverApprove
        | DataRoute::DeliverReject
        | DataRoute::LocalWatch
        | DataRoute::AppChanges
        | DataRoute::ProteinGet
        | DataRoute::ProteinOfMolecule => handler_timeout(),
    }
}
