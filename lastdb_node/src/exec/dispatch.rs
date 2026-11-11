use super::*;

mod routes_core;
mod routes_maintenance;
mod routes_sync_org;

use routes_core::*;
use routes_maintenance::*;
use routes_sync_org::*;

/// Execute a recognized control-socket data route against the minimal host,
/// with the caller's user id bound as the task-local user context.
pub async fn execute_data_route(
    route: DataRoute,
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    execute_observed_route(ctx, dispatch_data_route(route, req, ctx, host)).await
}

/// Bind the caller's user id as the task-local user context for synchronous
/// dispatch and the narrow `Send` mutation executor.
pub(super) async fn execute_observed_route<F>(ctx: &AccessContext, work: F) -> UdsResponse
where
    F: std::future::Future<Output = UdsResponse>,
{
    fold_db::user_context::run_with_user(&ctx.user_id, work).await
}

pub(super) async fn dispatch_data_route(
    route: DataRoute,
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    if let Some(response) = dispatch_core(route, req, ctx, host).await {
        return response;
    }
    if let Some(response) = dispatch_search_app(route, req, ctx, host).await {
        return response;
    }
    if let Some(response) = dispatch_maintenance_gc(route, req, ctx, host).await {
        return response;
    }
    if let Some(response) = dispatch_maintenance_repair(route, req, ctx, host).await {
        return response;
    }
    if let Some(response) = dispatch_blob_identity(route, req, ctx, host).await {
        return response;
    }
    if let Some(response) = dispatch_sync_deliver(route, req, ctx, host).await {
        return response;
    }
    content_free(404, "Not Found")
}
