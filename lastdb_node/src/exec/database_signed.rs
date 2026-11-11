//! Ordinary reads of a signed-log share. The store is the managed cache entry.
use super::*;
use crate::registered_share::{self, RegisteredShare};

pub(super) async fn read(
    route: DataRoute,
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
    share: &RegisteredShare,
) -> UdsResponse {
    let store = match registered_share::receiver_store(host, &share.id).await {
        Ok(store) => store,
        Err(_) => return error_response(503, "database store is unavailable", ctx),
    };
    host.with_records(
        store,
        super::execute_data_route_with_timeout(route, req, ctx, host),
    )
    .await
}
