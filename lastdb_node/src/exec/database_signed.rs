//! Ordinary reads of a signed-log share. The store is the managed cache entry.
//!
//! A read is refused until snapshot install has flushed and saved a complete
//! prefix. Opening the store is not that signal.
use super::*;
use crate::registered_share::{self, RegisteredShare};

pub(super) async fn read(
    route: DataRoute,
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
    share: &RegisteredShare,
) -> UdsResponse {
    if !replay_durable(host, share) {
        return error_response(503, "database snapshot is not durable", ctx);
    }
    let store = match registered_share::receiver_store(host, &share.id).await {
        Ok(store) => store,
        Err(_) => return error_response(503, "database store is unavailable", ctx),
    };
    let response = host
        .with_records(
            store,
            super::execute_data_route_with_timeout(route, req, ctx, host),
        )
        .await;
    if !replay_durable(host, share) {
        return error_response(503, "database snapshot is not durable", ctx);
    }
    response
}

fn replay_durable(host: &Host, share: &RegisteredShare) -> bool {
    crate::named_snapshot_restore::prefix_durable(&host.home, share.id.as_str())
}
