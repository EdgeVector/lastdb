use super::*;

/// Wait for all earlier accepted local writes to reach the durable boundary.
pub(in crate::exec) async fn execute(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    if !ctx.is_owner {
        return error_response(403, "the local durability barrier is owner-only", ctx);
    }
    if !req.body.is_empty() {
        match serde_json::from_slice::<serde_json::Map<String, serde_json::Value>>(&req.body) {
            Ok(fields) if fields.is_empty() => {}
            _ => return error_response(400, "local flush body must be an empty object", ctx),
        }
    }
    let runtime = host.db.db_ops().namespaced_store().disk_pack_runtime();
    let result = if let Some(runtime) = runtime {
        runtime
            .flush_native()
            .await
            .map(|receipt| serde_json::json!({"durable":true,"native_flush":receipt}))
    } else {
        host.db
            .flush_accepted_mutations(Duration::from_secs(30))
            .await
            .map(|()| serde_json::json!({"durable":true}))
    };
    match result {
        Ok(report) => json_ok(&envelope(&report, ctx.user_id.as_str())),
        Err(error) => error_response(503, &format!("local flush failed: {error}"), ctx),
    }
}
