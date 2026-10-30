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
    let action = serde_json::from_slice::<Value>(&req.body)
        .ok()
        .and_then(|body| {
            body.get("action")
                .and_then(Value::as_str)
                .map(str::to_owned)
        });
    if action
        .as_deref()
        .is_some_and(|action| action.starts_with("app_"))
    {
        let Ok(mut request) = serde_json::from_slice::<
            fold_db::fold_db_core::fold_db::AppSchemaConversion,
        >(&req.body) else {
            return error_response(400, "invalid app schema conversion request", ctx);
        };
        if let Err(error) = super::app_conversion_definitions::resolve(&mut request).await {
            return error_response(409, &error, ctx);
        }
        return match host.db.convert_app_schemas(request).await {
            Ok(report) => json_ok(&envelope(
                &serde_json::json!({ "app_schema_conversion": report }),
                ctx.user_id.as_str(),
            )),
            Err(error) => mapped_error_response("app schema conversion failed", error.into(), ctx),
        };
    }
    if host.db.app_schema_conversion_active() {
        return error_response(
            423,
            "finish or abort the app schema conversion before another declaration change",
            ctx,
        );
    }
    let Ok(request) =
        serde_json::from_slice::<fold_db::fold_db_core::fold_db::AlternateKeyChange>(&req.body)
    else {
        return error_response(400, "invalid alternate-key change request", ctx);
    };
    match host.db.change_alternate_keys(request).await {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({ "alternate_key_change": report }),
            ctx.user_id.as_str(),
        )),
        Err(error) => mapped_error_response("alternate-key change failed", error.into(), ctx),
    }
}
