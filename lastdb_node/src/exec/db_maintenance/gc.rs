//! Owner-socket routes that purge, collect and reap dead storage: schema index, atoms, file blobs, proteins, ref blobs, dropped schemas and plane residue.

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

/// `POST /api/db/drain-plane-residue` — copy one bounded page of rows sitting
/// outside their canonical plane collection into the canonical home, deleting
/// the source copy per key once the target holds it. Dry run by default.
pub(in crate::exec) async fn execute_db_drain_plane_residue_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    use fold_db::storage::{PlaneResidueDrainOptions, PlaneResidueFamily};

    #[derive(Deserialize)]
    struct Body {
        family: PlaneResidueFamily,
        source_collection: String,
        target_collection: String,
        /// Optional id prefix (required for protein/index drains from tips).
        #[serde(default)]
        key_prefix: Option<String>,
        #[serde(default)]
        after: Option<String>,
        /// Rows one call decides. Bounds the page so a full-collection drain
        /// is a resumable loop of short calls, not one long socket request.
        #[serde(default)]
        limit: Option<usize>,
        #[serde(default)]
        execute: bool,
        #[serde(default)]
        drop_empty_source: bool,
    }
    let body: Body = match serde_json::from_slice(&req.body) {
        Ok(b) => b,
        Err(e) => {
            return error_response(
                400,
                &format!("invalid /api/db/drain-plane-residue body: {e}"),
                ctx,
            )
        }
    };
    match host
        .db
        .drain_plane_residue(PlaneResidueDrainOptions {
            family: body.family,
            source_collection: body.source_collection,
            target_collection: body.target_collection,
            key_prefix: body.key_prefix,
            after: body.after,
            limit: body.limit.unwrap_or(1000),
            execute: body.execute,
            drop_empty_source: body.drop_empty_source,
        })
        .await
    {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({ "plane_residue_drain": report }),
            ctx.user_id.as_str(),
        )),
        Err(e) => mapped_error_response("drain-plane-residue failed", e, ctx),
    }
}
