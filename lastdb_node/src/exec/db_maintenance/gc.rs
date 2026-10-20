//! Owner-socket atom, file-blob, protein and dropped-schema GC.

use super::*;

/// `POST /api/db/gc-file-blobs` — delete local file-blob rows no live atom
/// references (the reclaim path for sealed bytes a purge orphans).
pub(in crate::exec) async fn execute_db_gc_file_blobs_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    struct Body {
        #[serde(default)]
        dry_run: Option<bool>,
    }
    let body: Body = if req.body.is_empty() {
        Body { dry_run: None }
    } else {
        match serde_json::from_slice(&req.body) {
            Ok(b) => b,
            Err(e) => {
                return error_response(
                    400,
                    &format!("invalid /api/db/gc-file-blobs body: {e}"),
                    ctx,
                )
            }
        }
    };
    let dry_run = body.dry_run.unwrap_or(true);
    {
        match host.db.gc_orphan_file_blobs(dry_run).await {
            Ok(report) => json_ok(&envelope(
                &serde_json::json!({ "gc_file_blobs": report }),
                ctx.user_id.as_str(),
            )),
            Err(e) => error_response(500, &format!("gc-file-blobs failed: {e}"), ctx),
        }
    }
}

/// `POST /api/db/gc-proteins` — delete empty, unbound protein rows.
pub(in crate::exec) async fn execute_db_gc_proteins_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    struct Body {
        #[serde(default)]
        dry_run: Option<bool>,
    }
    let body: Body = if req.body.is_empty() {
        Body { dry_run: None }
    } else {
        match serde_json::from_slice(&req.body) {
            Ok(b) => b,
            Err(e) => {
                return error_response(400, &format!("invalid /api/db/gc-proteins body: {e}"), ctx)
            }
        }
    };
    let dry_run = body.dry_run.unwrap_or(true);
    match host.db.gc_orphan_proteins(dry_run).await {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({ "gc_proteins": report }),
            ctx.user_id.as_str(),
        )),
        Err(e) => error_response(500, &format!("gc-proteins failed: {e}"), ctx),
    }
}
