use super::*;

const DEFAULT_HANDLER_TIMEOUT_SECS: u64 = 90;
const DEFAULT_ADMIN_HANDLER_TIMEOUT_SECS: u64 = 600;

/// Default client deadline for ordinary CLI Unix-socket reads (list, get,
/// app-storage, app-publish, and the `lastdb status` health default).
pub const DEFAULT_CLI_UDS_TIMEOUT_SECS: u64 = 30;

const CANARY_STATUS_HANDLER_TIMEOUT_SECS: u64 = 2;
const CANARY_BOOT_IDENTITY_HANDLER_TIMEOUT_SECS: u64 = 1;

/// Effective handler deadline from env or [`DEFAULT_HANDLER_TIMEOUT_SECS`].
#[must_use]
pub fn handler_timeout() -> Duration {
    fold_db::env_flag::var_parsed::<u64>("LASTDB_UDS_HANDLER_TIMEOUT_SECS")
        .filter(|&n| n > 0)
        .map_or(
            Duration::from_secs(DEFAULT_HANDLER_TIMEOUT_SECS),
            Duration::from_secs,
        )
}

/// The operator's explicit `LASTDB_UDS_ADMIN_TIMEOUT_SECS` override, when the
/// value is usable (a positive integer). `None` means unset or unusable, so a
/// caller with its own tighter default (the `status` health probe) keeps that
/// default instead of inheriting the 600s admin-scan budget.
#[must_use]
pub fn admin_handler_timeout_override() -> Option<Duration> {
    fold_db::env_flag::var_parsed::<u64>("LASTDB_UDS_ADMIN_TIMEOUT_SECS")
        .filter(|&n| n > 0)
        .map(Duration::from_secs)
}

/// Effective admin-scan deadline from env or [`DEFAULT_ADMIN_HANDLER_TIMEOUT_SECS`].
#[must_use]
pub fn admin_handler_timeout() -> Duration {
    admin_handler_timeout_override()
        .unwrap_or(Duration::from_secs(DEFAULT_ADMIN_HANDLER_TIMEOUT_SECS))
}

async fn with_handler_timeout<F>(ctx: &AccessContext, fut: F) -> UdsResponse
where
    F: std::future::Future<Output = UdsResponse>,
{
    with_handler_timeout_budget(ctx, handler_timeout(), fut).await
}

pub(in crate::exec) async fn with_handler_timeout_budget<F>(
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
/// [`crate::lastdb_uds::UdsWorkerPool`]).
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

/// Like [`block_on_route`] but with the longer budget for storage inventory.
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
/// schema-mutation declare get the longer inventory timeout).
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
    handle.block_on(execute_data_route_with_timeout(route, req, ctx, host))
}

/// The same deadline applies when a mutation awaits outside its socket worker.
pub(in crate::exec) async fn execute_data_route_with_timeout(
    route: DataRoute,
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    with_handler_timeout_budget(
        ctx,
        data_route_budget(route),
        execute_data_route(route, req, ctx, host),
    )
    .await
}

/// Wall-clock budget for one data route.
#[must_use]
// lint:fn-size-ok one exhaustive match with no `_` arm, so a new DataRoute forces a budget decision; the length is the variant list.
pub(in crate::exec) fn data_route_budget(route: DataRoute) -> Duration {
    match route {
        // Owner admin work that walks the store, or blocks on a remote service.
        // Full-prefix scans over a multi-GiB home; minutes, not seconds.
        DataRoute::DbSchemas
        | DataRoute::DbDeleteLedger
        | DataRoute::SyncByteBackupV2
        | DataRoute::SyncByteBackupV2Connect
        | DataRoute::SyncSharedSnapshot
        | DataRoute::DeclareSchema
        // The drop releases the schema's records and dependent entries
        // in bounded batches before it removes the schema.
        | DataRoute::DropSchema => admin_handler_timeout(),

        DataRoute::Status => Duration::from_secs(CANARY_STATUS_HANDLER_TIMEOUT_SECS),
        DataRoute::AdminShed => Duration::from_secs(60),
        DataRoute::BootIdentity => Duration::from_secs(CANARY_BOOT_IDENTITY_HANDLER_TIMEOUT_SECS),

        // Bounded per-request work: one key, one page, one blob.
        DataRoute::Query
        | DataRoute::DbFlush
        | DataRoute::DbCleanup
        | DataRoute::DbNativeStatus
        | DataRoute::Mutation
        | DataRoute::MutationBatch
        | DataRoute::QueryBatch
        | DataRoute::ListSchemas
        | DataRoute::ListRecordKeys
        | DataRoute::GetSchema
        | DataRoute::AutoIdentity
        | DataRoute::SeedSystemSchema
        | DataRoute::DbSchemaRetention
        | DataRoute::DbPutBlob
        | DataRoute::DbFetchBlob
        | DataRoute::DbDeleteBlob => handler_timeout(),
    }
}
