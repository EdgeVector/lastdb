use super::*;
mod flush;
mod status;

pub(super) use flush::execute as execute_db_flush_route;
pub(super) use status::execute as execute_db_native_status_route;

/// `POST /api/db/clear-history` — trim or purge mutation-history rows.
///
/// Body (all optional):
/// ```json
/// { "schema": "<name>", "keep_last": 1, "dry_run": true }
/// ```
/// Defaults: all schemas, keep newest 1 event per field key, dry_run=true.
/// Pass `"keep_last": 0` for a full history purge (latest-only: tips remain).
pub(super) async fn execute_db_clear_history_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    struct Body {
        #[serde(default)]
        schema: Option<String>,
        #[serde(default)]
        keep_last: Option<usize>,
        /// When omitted, default true (preview only). Pass `false` to delete.
        #[serde(default)]
        dry_run: Option<bool>,
    }
    let body: Body = if req.body.is_empty() {
        Body {
            schema: None,
            keep_last: None,
            dry_run: None,
        }
    } else {
        match serde_json::from_slice(&req.body) {
            Ok(b) => b,
            Err(e) => {
                return error_response(
                    400,
                    &format!("invalid /api/db/clear-history body: {e}"),
                    ctx,
                )
            }
        }
    };
    // Default 1 (trim extras). Explicit 0 = purge all history (latest tip only).
    let keep_last = body.keep_last.unwrap_or(1);
    let dry_run = body.dry_run.unwrap_or(true);
    let schema = body
        .schema
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());

    match host
        .db
        .clear_mutation_history(schema, keep_last, dry_run)
        .await
    {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({ "clear_history": report }),
            ctx.user_id.as_str(),
        )),
        Err(e) => mapped_error_response("clear-history failed", e, ctx),
    }
}
