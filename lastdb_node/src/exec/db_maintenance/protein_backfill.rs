//! Owner-only explicit source-key migration into existing protein members.

use super::*;

pub(in crate::exec) async fn execute_db_backfill_protein_keys_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Body {
        source_schema: String,
        target_schema: String,
        keys: Vec<fold_db::schema::types::KeyValue>,
    }
    let body: Body = match serde_json::from_slice(&req.body) {
        Ok(body) => body,
        Err(_) => return error_response(400, "invalid explicit protein backfill request", ctx),
    };
    if ctx.db_locator.is_some() || ctx.storage_prefix.is_some() {
        return error_response(
            400,
            "protein backfill currently supports personal schemas only",
            ctx,
        );
    }
    match host
        .db
        .backfill_protein_keys(&body.source_schema, &body.target_schema, &body.keys)
        .await
    {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({ "protein_backfill": report }),
            ctx.user_id.as_str(),
        )),
        Err(error) => mapped_error_response("explicit protein backfill failed", error.into(), ctx),
    }
}
