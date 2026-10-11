//! Wire parser for the owner-only selected-key conflict metadata batch.

use super::*;

pub(super) async fn execute_conflict_metadata_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let request = match serde_json::from_slice::<handlers::SelectedConflictRequest>(&req.body) {
        Ok(request) => request,
        Err(error) => return Reject::from_serde_error(&error, None).response(),
    };
    render(
        handlers::selected_key_conflict_metadata(host, request, ctx).await,
        ctx,
    )
}
