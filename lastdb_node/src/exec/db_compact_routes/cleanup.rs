//! Explicit owner cleanup and a read-only completion receipt.

use super::*;

pub(in crate::exec) async fn execute(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    if !ctx.is_owner {
        return error_response(403, "native cleanup is owner-only", ctx);
    }
    let Some(runtime) = host.db.db_ops().namespaced_store().disk_pack_runtime() else {
        return error_response(400, "native cleanup requires a v2 home", ctx);
    };
    if req.method == "GET" {
        return json_ok(&envelope(
            &serde_json::json!({"cleanup": runtime.status_cleanup()}),
            ctx.user_id.as_str(),
        ));
    }
    #[derive(Default, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Body {
        request_id: Option<uuid::Uuid>,
    }
    let body = if req.body.is_empty() {
        Body::default()
    } else {
        match serde_json::from_slice::<Body>(&req.body) {
            Ok(body) => body,
            Err(error) => {
                return error_response(400, &format!("invalid cleanup request: {error}"), ctx)
            }
        }
    };
    match runtime.request_native_cleanup(body.request_id).await {
        Ok(status) => json_ok(&envelope(
            &serde_json::json!({"cleanup": status}),
            ctx.user_id.as_str(),
        )),
        Err(error) => mapped_error_response("native cleanup request failed", error.into(), ctx),
    }
}
