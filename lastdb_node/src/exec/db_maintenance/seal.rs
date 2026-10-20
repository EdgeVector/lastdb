//! Owner-socket at-rest encryption maintenance routes.

use super::*;

/// `POST /api/db/reseal-at-rest` — rewrite sealed values in one plane to an
/// explicit ENB target. Default dry-run. OWNER only.
pub(in crate::exec) async fn execute_db_reseal_at_rest_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    struct Body {
        collection: String,
        #[serde(default)]
        target: Option<fold_db::storage::ResealAtRestTarget>,
        #[serde(default)]
        dry_run: Option<bool>,
        #[serde(default)]
        max_rows: Option<usize>,
        #[serde(default)]
        max_secs: Option<u64>,
        #[serde(default)]
        progress_only: Option<bool>,
        #[serde(default)]
        restart: Option<bool>,
    }
    let body: Body = if req.body.is_empty() {
        return error_response(400, "reseal-at-rest: collection is required", ctx);
    } else {
        match serde_json::from_slice(&req.body) {
            Ok(b) => b,
            Err(e) => {
                return error_response(
                    400,
                    &format!("invalid /api/db/reseal-at-rest body: {e}"),
                    ctx,
                )
            }
        }
    };
    if body.collection.trim().is_empty() {
        return error_response(400, "reseal-at-rest: collection is required", ctx);
    }
    let options = fold_db::storage::ResealAtRestOptions {
        collection: body.collection,
        target: body.target.unwrap_or_default(),
        dry_run: body.dry_run.unwrap_or(true),
        max_rows: body.max_rows,
        max_secs: body.max_secs,
        progress_only: body.progress_only.unwrap_or(false),
        restart: body.restart.unwrap_or(false),
    };
    match host.db.reseal_at_rest(options).await {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({ "reseal_at_rest": report }),
            ctx.user_id.as_str(),
        )),
        Err(e) => mapped_error_response("reseal-at-rest failed", e, ctx),
    }
}

/// `POST /api/db/reap-unsealed` — remove un-enveloped rows from one encrypted
/// plane. They already read as absent; this returns their bytes. Default
/// dry-run. OWNER only. Plaintext-by-policy namespaces are refused (400).
pub(in crate::exec) async fn execute_db_reap_unsealed_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    struct Body {
        collection: String,
        #[serde(default)]
        dry_run: Option<bool>,
        #[serde(default)]
        max_rows: Option<usize>,
        #[serde(default)]
        max_secs: Option<u64>,
        #[serde(default)]
        progress_only: Option<bool>,
        #[serde(default)]
        restart: Option<bool>,
    }
    let body: Body = if req.body.is_empty() {
        return error_response(400, "reap-unsealed: collection is required", ctx);
    } else {
        match serde_json::from_slice(&req.body) {
            Ok(b) => b,
            Err(e) => {
                return error_response(
                    400,
                    &format!("invalid /api/db/reap-unsealed body: {e}"),
                    ctx,
                )
            }
        }
    };
    if body.collection.trim().is_empty() {
        return error_response(400, "reap-unsealed: collection is required", ctx);
    }
    let options = fold_db::storage::ReapUnsealedOptions {
        collection: body.collection,
        dry_run: body.dry_run.unwrap_or(true),
        max_rows: body.max_rows,
        max_secs: body.max_secs,
        progress_only: body.progress_only.unwrap_or(false),
        restart: body.restart.unwrap_or(false),
    };
    match host.db.reap_unsealed(options).await {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({ "reap_unsealed": report }),
            ctx.user_id.as_str(),
        )),
        // A refusal is the caller's mistake, not the node's: answer 400 so a
        // script that points this at `schemas` cannot read a 500 as "retry".
        Err(e) if e.to_string().contains("reap-unsealed: refused:") => {
            error_response(400, &e.to_string(), ctx)
        }
        Err(e) => mapped_error_response("reap-unsealed failed", e, ctx),
    }
}
