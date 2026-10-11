//! Accept a durable owner job; source retention and cloud work use the worker.

use super::*;
mod admission;
pub(super) use admission::reject_local_write;

pub(super) async fn execute(req: &UdsRequest, ctx: &AccessContext, host: &Host) -> UdsResponse {
    if !ctx.is_owner {
        return error_response(403, "cloud purge requires the node owner", ctx);
    }
    let Ok(request) = serde_json::from_slice::<crate::cloud_purge_runtime::Request>(&req.body)
    else {
        return error_response(400, "invalid cloud purge request", ctx);
    };
    match crate::cloud_purge_runtime::handle(host, request).await {
        Ok(result) => json_ok(&envelope(&result, &ctx.user_id)),
        Err(error) => error_response(409, &error, ctx),
    }
}
