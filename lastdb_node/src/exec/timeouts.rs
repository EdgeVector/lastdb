//! Wall-clock route timeout budgets and blocking runtime entrypoints.

use super::*;
use std::time::Duration;

const DEFAULT_HANDLER_TIMEOUT_SECS: u64 = 90;
const DEFAULT_ADMIN_HANDLER_TIMEOUT_SECS: u64 = 600;
pub const DEFAULT_CLI_UDS_TIMEOUT_SECS: u64 = 30;

fn secs_override(var: &str) -> Option<Duration> {
    fold_db::env_flag::var_parsed::<u64>(var)
        .filter(|&n| n > 0)
        .map(Duration::from_secs)
}

#[must_use]
pub fn handler_timeout() -> Duration {
    secs_override("LASTDB_UDS_HANDLER_TIMEOUT_SECS")
        .unwrap_or(Duration::from_secs(DEFAULT_HANDLER_TIMEOUT_SECS))
}

#[must_use]
pub fn admin_handler_timeout_override() -> Option<Duration> {
    secs_override("LASTDB_UDS_ADMIN_TIMEOUT_SECS")
}

#[must_use]
pub fn admin_handler_timeout() -> Duration {
    admin_handler_timeout_override()
        .unwrap_or(Duration::from_secs(DEFAULT_ADMIN_HANDLER_TIMEOUT_SECS))
}

pub(in crate::exec) async fn with_handler_timeout_budget<F>(
    ctx: &AccessContext,
    budget: Duration,
    fut: F,
) -> UdsResponse
where
    F: std::future::Future<Output = UdsResponse>,
{
    if let Ok(resp) = tokio::time::timeout(budget, fut).await {
        resp
    } else {
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

pub fn block_on_route<Fut>(
    handle: &tokio::runtime::Handle,
    ctx: &AccessContext,
    fut: Fut,
) -> UdsResponse
where
    Fut: std::future::Future<Output = UdsResponse>,
{
    handle.block_on(with_handler_timeout_budget(ctx, handler_timeout(), fut))
}

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

pub fn block_on_data_route(
    handle: &tokio::runtime::Handle,
    route: DataRoute,
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    handle.block_on(execute_data_route_with_timeout(route, req, ctx, host))
}

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

#[must_use]
pub(in crate::exec) fn data_route_budget(route: DataRoute) -> Duration {
    match route {
        DataRoute::DbSchemas
        | DataRoute::DbDeleteLedger
        | DataRoute::SyncByteBackupV2
        | DataRoute::SyncByteBackupV2Connect
        | DataRoute::DeclareSchema
        | DataRoute::DropSchema => admin_handler_timeout(),
        DataRoute::Status => Duration::from_secs(2),
        DataRoute::AdminShed => Duration::from_secs(60),
        DataRoute::BootIdentity => Duration::from_secs(1),
        _ => handler_timeout(),
    }
}
