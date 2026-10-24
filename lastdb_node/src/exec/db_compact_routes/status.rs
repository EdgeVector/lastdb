use super::*;

/// Read the native durable boundary without advancing its normal flush schedule.
pub(in crate::exec) async fn execute(
    _req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    if !ctx.is_owner {
        return error_response(403, "the native status route is owner-only", ctx);
    }
    let Some(runtime) = host.db.db_ops().namespaced_store().disk_pack_runtime() else {
        return error_response(
            400,
            "native disk status is unavailable on a legacy home",
            ctx,
        );
    };
    let retry_cache = host
        .db
        .db_ops()
        .metadata()
        .retry_cache_status()
        .await
        .unwrap_or_else(|error| serde_json::json!({"error": error.to_string()}));
    json_ok(&envelope(
        &serde_json::json!({"native_disk":runtime.status_native(), "retry_cache": retry_cache}),
        ctx.user_id.as_str(),
    ))
}
