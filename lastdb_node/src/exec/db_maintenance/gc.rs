//! Owner-socket routes that purge, collect and reap dead storage: schema index, atoms, file blobs, proteins, ref blobs, dropped schemas and plane residue.

use super::*;

/// `POST /api/db/purge-schemaidx` — delete retired full-atom-copy secondary index.
pub(in crate::exec) async fn execute_db_purge_schemaidx_route(
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    match host.db.purge_schemaidx().await {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({ "purge_schemaidx": report }),
            ctx.user_id.as_str(),
        )),
        Err(e) => error_response(500, &format!("purge-schemaidx failed: {e}"), ctx),
    }
}

/// `POST /api/db/gc-atoms` — delete unreferenced atom: rows.
pub(in crate::exec) async fn execute_db_gc_atoms_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    struct Body {
        #[serde(default)]
        dry_run: Option<bool>,
        /// When true, also prune tip-version history on **live** tips (not only
        /// tombstoned). Drops `as_of` history; frees append-heavy body atoms.
        #[serde(default)]
        prune_live_history: Option<bool>,
        #[serde(default)]
        schema: Option<String>,
    }
    let body: Body = if req.body.is_empty() {
        Body {
            dry_run: None,
            prune_live_history: None,
            schema: None,
        }
    } else {
        match serde_json::from_slice(&req.body) {
            Ok(b) => b,
            Err(e) => {
                return error_response(400, &format!("invalid /api/db/gc-atoms body: {e}"), ctx)
            }
        }
    };
    let dry_run = body.dry_run.unwrap_or(true);
    let prune_live_history = body.prune_live_history.unwrap_or(false);
    let result = if let Some(schema) = body.schema.as_deref() {
        host.db
            .gc_orphan_atoms_for_schema(schema, dry_run, prune_live_history)
            .await
    } else {
        host.db
            .gc_orphan_atoms_with(dry_run, prune_live_history)
            .await
    };
    match result {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({ "gc_atoms": report }),
            ctx.user_id.as_str(),
        )),
        Err(e) => error_response(500, &format!("gc-atoms failed: {e}"), ctx),
    }
}

pub(in crate::exec) fn checked_reaper_max_ops(value: Option<u64>) -> Result<usize, &'static str> {
    let value = value.unwrap_or(fold_db::db_operations::MAX_DROPPED_SCHEMA_REAP_OPS as u64);
    if value == 0 || value > fold_db::db_operations::MAX_DROPPED_SCHEMA_REAP_OPS as u64 {
        return Err("max_ops must be 1..=4096; tip proof needs at least 2");
    }
    Ok(value as usize)
}

/// `POST /api/db/reap-dropped-schema` — reap live tips of one dropped identity.
pub(in crate::exec) async fn execute_db_reap_dropped_schema_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    struct Body {
        schema: String,
        #[serde(default)]
        dry_run: Option<bool>,
        #[serde(default)]
        max_ops: Option<u64>,
        #[serde(default)]
        fields: Vec<String>,
        #[serde(default)]
        cursor: Option<fold_db::db_operations::DroppedSchemaReapCursor>,
        #[serde(default)]
        probe: Option<Probe>,
    }
    #[derive(Deserialize)]
    struct Probe {
        molecule_uuid: String,
        key_hash: String,
        #[serde(default)]
        key_range: String,
        #[serde(default)]
        expected_key_fingerprint: Option<String>,
    }
    let body: Body = match serde_json::from_slice(&req.body) {
        Ok(b) => b,
        Err(e) => {
            return error_response(
                400,
                &format!("invalid /api/db/reap-dropped-schema body: {e}"),
                ctx,
            )
        }
    };
    let schema = body.schema.trim();
    if schema.is_empty() {
        return error_response(400, "schema is required", ctx);
    }
    if let Some(probe) = body.probe {
        if !body.dry_run.unwrap_or(true) || body.cursor.is_some() {
            return error_response(400, "probe requires dry_run and no cursor", ctx);
        }
        return match host
            .db
            .probe_dropped_schema_tip(
                schema,
                &probe.molecule_uuid,
                &probe.key_hash,
                &probe.key_range,
                probe.expected_key_fingerprint.as_deref(),
            )
            .await
        {
            Ok(report) => json_ok(&envelope(
                &serde_json::json!({ "probe_dropped_tip": report }),
                ctx.user_id.as_str(),
            )),
            Err(error) => mapped_error_response("probe-dropped-tip failed", error, ctx),
        };
    }
    let dry_run = body.dry_run.unwrap_or(true);
    let max_ops = match checked_reaper_max_ops(body.max_ops) {
        Ok(value) => value,
        Err(message) => return error_response(400, message, ctx),
    };
    match host
        .db
        .reap_dropped_schema(schema, &body.fields, dry_run, max_ops, body.cursor)
        .await
    {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({ "reap_dropped_schema": report }),
            ctx.user_id.as_str(),
        )),
        Err(e) => mapped_error_response("reap-dropped-schema failed", e, ctx),
    }
}

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
    #[cfg(feature = "sharing")]
    {
        match host.db.gc_orphan_file_blobs(dry_run).await {
            Ok(report) => json_ok(&envelope(
                &serde_json::json!({ "gc_file_blobs": report }),
                ctx.user_id.as_str(),
            )),
            Err(e) => error_response(500, &format!("gc-file-blobs failed: {e}"), ctx),
        }
    }
    #[cfg(not(feature = "sharing"))]
    {
        let _ = dry_run;
        error_response(
            501,
            "gc-file-blobs requires a daemon built with the `sharing` feature",
            ctx,
        )
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

/// `POST /api/db/purge-ref-blobs` — measure/delete legacy `ref:` whole-molecule blobs.
pub(in crate::exec) async fn execute_db_purge_ref_blobs_route(
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
                    &format!("invalid /api/db/purge-ref-blobs body: {e}"),
                    ctx,
                )
            }
        }
    };
    let dry_run = body.dry_run.unwrap_or(true);
    match host.db.purge_ref_blobs(dry_run).await {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({ "purge_ref_blobs": report }),
            ctx.user_id.as_str(),
        )),
        Err(e) => error_response(500, &format!("purge-ref-blobs failed: {e}"), ctx),
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
