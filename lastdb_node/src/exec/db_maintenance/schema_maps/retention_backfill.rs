//! Owner-only, explicitly confirmed retention backfill, one durable page per call.

use super::*;

pub(super) async fn execute(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
    body: &RetentionBody,
) -> UdsResponse {
    if !body.confirm_expire_existing {
        return error_response(
            409,
            "retention backfill can expire existing records; confirm_expire_existing is required",
            ctx,
        );
    }
    if ctx.storage_prefix.is_some() || ctx.db_locator.is_some() {
        return error_response(
            400,
            "retention backfill currently requires the personal scope",
            ctx,
        );
    }
    let mut audit = crate::retention_audit::RetentionAuditEvent::new(
        "db_schema_retention",
        &crate::request_telemetry::client_from_request(req),
        ctx.user_id.as_str(),
        "backfill",
        &body.schema,
        &body.schema,
    );
    audit.confirmed_expire_existing = true;
    if let Err(error) = crate::retention_audit::append(&host.home, &audit) {
        return error_response(503, &format!("backfill audit failed: {error}"), ctx);
    }
    let result = host
        .db
        .backfill_schema_retention_page(
            &body.schema,
            body.cursor.as_deref(),
            body.limit.unwrap_or(16),
        )
        .await;
    let error = result.as_ref().err().map(ToString::to_string);
    let closed = audit.closed(error.as_deref().map_or(Ok(()), Err));
    if let Err(error) = crate::retention_audit::append(&host.home, &closed) {
        return error_response(
            503,
            &format!(
                "backfill audit close failed: {error}; this page can be retried",
                ctx
            ),
        );
    }
    match result {
        Ok(value) => json_ok(&envelope(&value, ctx.user_id.as_str())),
        Err(error) => error_response(400, &format!("retention backfill failed: {error}"), ctx),
    }
}
