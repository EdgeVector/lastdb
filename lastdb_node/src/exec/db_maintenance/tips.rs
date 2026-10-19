//! Owner-socket routes that migrate tips (dry run only) and the photo blobs.

use super::*;

/// `POST /api/db/migrate-thin-tips` — count the fat mk: tips (dry run only).
pub(in crate::exec) async fn execute_db_migrate_thin_tips_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize, Default)]
    struct Body {
        #[serde(default)]
        dry_run: Option<bool>,
        /// Cap on `mk:` records decided by this call. Keeps a whole-store pass
        /// inside the control socket's read deadline.
        #[serde(default)]
        max_keys: Option<usize>,
        /// Resume cursor from a previous call's `next_after_key`.
        #[serde(default)]
        after_key: Option<String>,
    }
    let body: Body = if req.body.is_empty() {
        Body::default()
    } else {
        match serde_json::from_slice(&req.body) {
            Ok(b) => b,
            Err(e) => {
                return error_response(
                    400,
                    &format!("invalid /api/db/migrate-thin-tips body: {e}"),
                    ctx,
                )
            }
        }
    };
    let dry_run = body.dry_run.unwrap_or(true);
    if !dry_run {
        return error_response(
            400,
            "migrate-thin-tips execute was removed: only the atom counter planner writes tip records",
            ctx,
        );
    }
    match host
        .db
        .migrate_thin_tips(dry_run, body.max_keys, body.after_key.as_deref())
        .await
    {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({ "migrate_thin_tips": report }),
            ctx.user_id.as_str(),
        )),
        Err(e) => mapped_error_response("migrate-thin-tips failed", e, ctx),
    }
}
