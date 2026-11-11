//! Resolution payloads and the sync/check executors.

use super::*;

// The request holds this lock through the policy write.
static DECLARE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub(crate) async fn execute_catalog_schema_sync(
    ctx: &AccessContext,
    host: &Host,
    declare: DirectDeclareProposal,
) -> UdsResponse {
    if let Some(response) = super::logical_admission::sync_admission_error(ctx, &declare) {
        return response;
    }

    let _declare_guard = DECLARE_LOCK.lock().await;

    let resolution = match super::owned::load(host, &declare).await {
        Ok(resolution) => resolution,
        Err(e) => return error_response(409, &e, ctx),
    };

    let mut payload = match declare_resolution_payload(&declare, &resolution) {
        Ok(payload) => payload,
        Err(error) => return error_response(409, &error, ctx),
    };
    payload["bind_eligible"] = serde_json::json!(true);

    // Install the policy before the declaration permits its first write.
    let retention = finish_declare_retention(host, &declare, &resolution).await;

    match retention {
        Ok(Some(retention)) => payload["retention"] = retention,
        Ok(None) => {}
        Err(e) => {
            return error_response(
                409,
                &format!("schema declared and bound, but its retention step failed: {e}"),
                ctx,
            )
        }
    }

    json_ok(&envelope(&payload, ctx.user_id.as_str()))
}

/// Read-only declaration preview. It checks the exact definition and retention
/// policy without registration or schema writes.
pub(crate) async fn execute_catalog_schema_check(
    ctx: &AccessContext,
    host: &Host,
    declare: DirectDeclareProposal,
) -> UdsResponse {
    if let Some(response) = logical_layout_error(ctx, &declare.catalog_schema) {
        return response;
    }
    let plan = match super::owned::plan(host, &declare).await {
        Ok(plan) => plan,
        Err(e) => return error_response(409, &e, ctx),
    };
    let mut payload = match declare_resolution_payload(&declare, &plan.resolution) {
        Ok(payload) => payload,
        Err(error) => return error_response(409, &error, ctx),
    };
    payload["intent"] = serde_json::json!("check");
    payload["dry_run"] = serde_json::json!(true);
    payload["bind_eligible"] = serde_json::json!(false);
    if let Some(reason) = plan.register_reason {
        payload["register_reason"] = serde_json::json!(reason);
    }
    match preview_declare_retention(host, &declare, &plan.resolution).await {
        Ok(Some(retention)) => payload["retention"] = retention,
        Ok(None) => {}
        Err(e) => return error_response(409, &e, ctx),
    }
    json_ok(&envelope(&payload, ctx.user_id.as_str()))
}
