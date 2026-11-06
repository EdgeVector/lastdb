use super::super::*;

/// Explicit byte backup and local slice export operations.
pub(super) async fn dispatch_sync_deliver(
    route: DataRoute,
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> Option<UdsResponse> {
    Some(match route {
        DataRoute::SyncByteBackupV2 => {
            super::super::byte_backup_routes::execute(req, ctx, host).await
        }
        DataRoute::SyncByteBackupV2Connect => {
            super::super::byte_backup_routes::connect(req, ctx, host).await
        }
        DataRoute::SyncSharedSnapshot => heap_route!(shared_snapshot, req, ctx, host).await,
        DataRoute::SyncLogicalV1 => {
            heap_route!(super::super::logical_sync_routes::execute, req, ctx, host).await
        }
        DataRoute::DeliverSnapshot => {
            heap_route!(crate::deliver::execute_publish_snapshot, req, ctx, host).await
        }
        _ => return None,
    })
}

async fn shared_snapshot(req: &UdsRequest, ctx: &AccessContext, host: &Host) -> UdsResponse {
    if !ctx.is_owner {
        return error_response(403, "shared snapshot is owner-only", ctx);
    }
    if req.body.len() > 8192 {
        return error_response(413, "shared snapshot request exceeds its byte limit", ctx);
    }
    let Ok(request) = serde_json::from_slice(&req.body) else {
        return error_response(400, "invalid shared snapshot request", ctx);
    };
    let budget =
        super::super::admin_handler_timeout().saturating_sub(std::time::Duration::from_secs(1));
    match crate::exec::shared_snapshot_sequence::run_shared_snapshot(host, &request, budget).await {
        Ok(result) => json_ok(&envelope(&result, ctx.user_id.as_str())),
        Err(error) => error_response(409, &error.to_string(), ctx),
    }
}
