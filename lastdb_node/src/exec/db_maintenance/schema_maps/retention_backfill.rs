//! Owner-only, explicitly confirmed retention backfill, one durable page per call.

use super::*;

pub(super) async fn execute(ctx: &AccessContext, host: &Host, body: &RetentionBody) -> UdsResponse {
    if !body.confirm_expire_existing {
        return error_response(
            409,
            "retention backfill can expire existing records; confirm_expire_existing is required",
            ctx,
        );
    }
    let result = host
        .db
        .backfill_schema_retention_page(
            &body.schema,
            body.cursor.as_deref(),
            body.limit.unwrap_or(16),
        )
        .await;
    match result {
        Ok(value) => json_ok(&envelope(&value, ctx.user_id.as_str())),
        Err(error) => error_response(400, &format!("retention backfill failed: {error}"), ctx),
    }
}
