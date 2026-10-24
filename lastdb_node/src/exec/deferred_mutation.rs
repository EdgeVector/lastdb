//! Only the two mutation futures cross from socket workers to runtime tasks.
use super::*;

/// Preserve route scopes, handlers and budgets without capturing unrelated
/// data routes whose higher-ranked borrows cannot cross runtime threads.
pub fn execute_deferred_mutation<'a>(
    route: DataRoute,
    req: &'a UdsRequest,
    ctx: &'a AccessContext,
    host: &'a Host,
) -> ErasedRoute<'a> {
    Box::pin(async move {
        let work = match route {
            DataRoute::Mutation => heap_route!(execute_mutation_route, req, ctx, host),
            DataRoute::MutationBatch => heap_route!(execute_mutations_batch_route, req, ctx, host),
            _ => return content_free(404, "Not Found"),
        };
        with_handler_timeout_budget(
            ctx,
            data_route_budget_for_request(route, req),
            dispatch::execute_observed_route(route, req, ctx, host, work),
        )
        .await
    })
}
