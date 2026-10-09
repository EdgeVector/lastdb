//! Owner-socket routes that rekey, reseal and reap unsealed at-rest rows.

use super::*;

/// `POST /api/db/rekey-atom-partition-prefix` — dual-write flat atom bodies onto
/// partition-prefixed keys (+ locators). Default dry-run.
pub(in crate::exec) async fn execute_db_rekey_atom_partition_prefix_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    struct Body {
        #[serde(default)]
        dry_run: Option<bool>,
        #[serde(default)]
        remove_flat: Option<bool>,
        #[serde(default)]
        max_ops: Option<usize>,
        /// Tips per range page. Absent = `ATOM_PARTITION_REKEY_TIP_PAGE`.
        ///
        /// The pass batches per page, so its store round trips per page do not
        /// scale with page size — this is the knob that trades the page's
        /// resident bytes (its `get_many` over the bodies needing a copy)
        /// against wall clock on a multi-hour run. Operators tuning a long
        /// migration need it reachable without a rebuild; before it was
        /// plumbed, the only page size a live node would ever use was the
        /// compiled-in default.
        #[serde(default)]
        tip_page: Option<usize>,
        /// Classify every tip that resolves to no atom body, returning up to
        /// this many detail rows. Absent = count only (pre-audit behaviour).
        #[serde(default)]
        audit_unresolved: Option<usize>,
        /// Report the durable checkpoint only — no scan, no cursor write.
        #[serde(default)]
        progress_only: Option<bool>,
    }
    let body: Body = if req.body.is_empty() {
        Body {
            dry_run: None,
            remove_flat: None,
            max_ops: None,
            tip_page: None,
            audit_unresolved: None,
            progress_only: None,
        }
    } else {
        match serde_json::from_slice(&req.body) {
            Ok(b) => b,
            Err(e) => {
                return error_response(
                    400,
                    &format!("invalid /api/db/rekey-atom-partition-prefix body: {e}"),
                    ctx,
                )
            }
        }
    };
    let options = fold_db::db_operations::AtomPartitionRekeyOptions {
        dry_run: body.dry_run.unwrap_or(true),
        remove_flat: body.remove_flat.unwrap_or(false),
        max_ops: body.max_ops,
        audit_unresolved: body.audit_unresolved,
        tip_page: body.tip_page,
        progress_only: body.progress_only.unwrap_or(false),
        storage_prefix: None,
    };
    match host.db.rekey_atoms_to_partition_prefix(options).await {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({ "rekey_atom_partition_prefix": report }),
            ctx.user_id.as_str(),
        )),
        Err(e) => error_response(
            500,
            &format!("rekey-atom-partition-prefix failed: {e}"),
            ctx,
        ),
    }
}

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
