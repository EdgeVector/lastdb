use super::*;

mod rules;
use rules::{target_ids, ExplainRequest};

/// `POST /api/storage/liveness/explain` — the counter of each named file blob.
pub(super) async fn execute_liveness_explain_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let request = match ExplainRequest::parse(&req.body) {
        Ok(request) => request,
        Err(message) => return error_response(400, &message, ctx),
    };
    // A named handle can map each schema to a different storage instance.
    // This target-only diagnostic has no schema with which to resolve it.
    if ctx.db_locator.is_some() {
        return error_response(
            400,
            "liveness explain requires the default owner scope; named database schema mappings are unsupported",
            ctx,
        );
    }
    explain_blobs(host, ctx, request.id.trim(), request.ids).await
}

/// The counter of each named blob. A blob counts the live records that name
/// it; it has no list of referrers. A blob at zero is in its dwell until
/// `zero_since` plus the dwell has passed.
async fn explain_blobs(
    host: &Host,
    ctx: &AccessContext,
    id: &str,
    ids: Vec<String>,
) -> UdsResponse {
    let ids = target_ids(id, ids);
    match host.db.db_ops().records().blob_counter_views(&ids).await {
        Ok(views) => {
            let dwell_secs = host.db.db_ops().records().blob_counter_report().dwell_secs;
            json_ok(&envelope(
                &rules::blob_reply(&views, dwell_secs),
                ctx.user_id.as_str(),
            ))
        }
        Err(error) => mapped_error_response(
            "liveness explain failed",
            fold_db::error::FoldDbError::Schema(error),
            ctx,
        ),
    }
}
