//! Owner-only declaration changes with durable resumable backfill pages.

use super::*;

pub(in crate::exec) async fn execute_db_alternate_keys_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    if ctx.db_locator.is_some() || ctx.storage_prefix.is_some() {
        return error_response(400, "alternate changes require the personal database", ctx);
    }
    let request = match serde_json::from_slice::<fold_db::fold_db_core::fold_db::AlternateKeyChange>(
        &req.body,
    ) {
        Ok(request) => request,
        Err(_) => return error_response(400, "invalid alternate-key change request", ctx),
    };
    match host.db.change_alternate_keys(request).await {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({ "alternate_key_change": report }),
            ctx.user_id.as_str(),
        )),
        Err(error) => mapped_error_response("alternate-key change failed", error.into(), ctx),
    }
}
