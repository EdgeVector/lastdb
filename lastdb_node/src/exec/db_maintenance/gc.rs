//! Owner-socket atom, file-blob, protein and dropped-schema GC.

use super::*;

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
        #[serde(default)]
        schema: Option<String>,
        /// Accept that the atoms of a lost multi-device write can be deleted.
        /// Without it a delete refuses on a node that has applied a peer
        /// mutation or cannot prove it never did.
        #[serde(default)]
        allow_after_peer_apply: Option<bool>,
    }
    // The route table keeps this verb off the app socket. This check also keeps
    // out a code-signed app on the owner socket: it is not the owner principal,
    // and the override below accepts that atoms can be deleted.
    if !ctx.is_owner {
        return error_response(403, "gc-atoms is owner-only", ctx);
    }
    let body: Body = if req.body.is_empty() {
        Body {
            dry_run: None,
            schema: None,
            allow_after_peer_apply: None,
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
    let allow_after_peer_apply = body.allow_after_peer_apply.unwrap_or(false);
    let result = if let Some(schema) = body.schema.as_deref() {
        host.db
            .gc_orphan_atoms_for_schema(schema, dry_run, allow_after_peer_apply)
            .await
    } else {
        host.db
            .gc_orphan_atoms_with(dry_run, allow_after_peer_apply)
            .await
    };
    match result {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({ "gc_atoms": report }),
            ctx.user_id.as_str(),
        )),
        // The peer-apply interlock is a precondition, not a server fault.
        Err(e) if e.to_string().contains("gc-atoms refused:") => {
            error_response(409, &format!("gc-atoms failed: {e}"), ctx)
        }
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
