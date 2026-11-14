//! Ordinary reads of a signed-log share. The store is the managed cache entry.
//!
//! A read is refused until snapshot install has flushed and saved a complete
//! prefix. Opening the store is not that signal.
use super::*;
use crate::registered_share::{self, RegisteredShare};
use cloud_backup_protocol::Id;

pub(super) async fn submit(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
    share: &RegisteredShare,
) -> UdsResponse {
    use crate::log_sync::ContributionError;
    if !share.whole_database() {
        return error_response(403, "this share is read-only", ctx);
    }
    if !replay_durable(host, share) {
        return error_response(503, "database snapshot is not durable", ctx);
    }
    let Some(id) = req
        .header("Idempotency-Key")
        .and_then(|value| Id::try_from(value.to_owned()).ok())
    else {
        return error_response(400, "a shared write requires a UUID Idempotency-Key", ctx);
    };
    match crate::log_sync::submit_share_write(host, share, &id, &req.body).await {
        Ok(sequence) => {
            let mut response = json_ok(&json!({
                "ok": true, "status": "submitted", "mutation_id": id,
                "submission_sequence": sequence,
            }));
            if response.status == 200 {
                response.status = 202;
                response.reason = "Accepted";
            }
            response
        }
        Err(ContributionError::Invalid) => {
            error_response(400, "invalid shared mutation or schema definition", ctx)
        }
        Err(ContributionError::Forbidden) => {
            error_response(403, "this member cannot submit this shared mutation", ctx)
        }
        Err(ContributionError::Unavailable) => {
            error_response(503, "shared mutation was not confirmed", ctx)
        }
    }
}

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
    let Ok(store) = registered_share::receiver_store(host, &share.id).await else {
        return error_response(503, "database store is unavailable", ctx);
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
    crate::database_replay::prefix_durable(&host.home, share.id.as_str())
}
