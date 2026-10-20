//! Owner-socket routes that repair, drain and retain tip history.

use super::*;

/// `POST /api/db/repair-dangling-tips` — remove unreachable `mk:` tips.
pub(in crate::exec) async fn execute_db_repair_dangling_tips_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    struct Body {
        #[serde(default)]
        dry_run: Option<bool>,
        #[serde(default)]
        max_ops: Option<usize>,
        #[serde(default)]
        tip_page: Option<usize>,
        #[serde(default)]
        audit_unresolved: Option<usize>,
        /// Walk only this schema's field molecules (name, descriptive name,
        /// or identity hash). A prefix range per molecule, not a store scan.
        #[serde(default)]
        schema: Option<String>,
        /// With `schema`: walk only this API HashKey (partition) inside it.
        #[serde(default)]
        hash_key: Option<String>,
    }
    let body: Body = if req.body.is_empty() {
        Body {
            dry_run: None,
            max_ops: None,
            tip_page: None,
            audit_unresolved: None,
            schema: None,
            hash_key: None,
        }
    } else {
        match serde_json::from_slice(&req.body) {
            Ok(b) => b,
            Err(e) => {
                return error_response(
                    400,
                    &format!("invalid /api/db/repair-dangling-tips body: {e}"),
                    ctx,
                )
            }
        }
    };
    let options = fold_db::db_operations::DanglingTipRepairOptions {
        dry_run: body.dry_run.unwrap_or(true),
        max_ops: body.max_ops,
        tip_page: body.tip_page,
        audit_unresolved: body.audit_unresolved,
        storage_prefix: None,
        // Deliberately not exposed over HTTP: a windowed `--execute` would
        // repair one slice of the store and still report a completed walk.
        key_window: None,
        // Set by `repair_dangling_tips_for_schema` from `body.schema`. Unlike
        // `key_window`, a scoped report carries `scope`, so its `completed`
        // cannot be read as a whole-store claim.
        scope: None,
    };
    let result = match (body.schema.as_deref(), body.hash_key) {
        (Some(schema), hash_key) => {
            host.db
                .repair_dangling_tips_for_schema(options, schema, hash_key)
                .await
        }
        (None, Some(_)) => {
            return error_response(
                400,
                "invalid /api/db/repair-dangling-tips body: hash_key requires schema",
                ctx,
            )
        }
        (None, None) => host.db.repair_dangling_tips(options).await,
    };
    match result {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({ "repair_dangling_tips": report }),
            ctx.user_id.as_str(),
        )),
        Err(e) => mapped_error_response("repair-dangling-tips failed", e, ctx),
    }
}

/// `POST /api/db/probe-locator-only` — bounded locator-only population sample.
pub(in crate::exec) async fn execute_db_probe_locator_only_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    struct Body {
        #[serde(default)]
        max_tips: Option<usize>,
        #[serde(default)]
        tip_page: Option<usize>,
    }
    let body: Body = if req.body.is_empty() {
        Body {
            max_tips: None,
            tip_page: None,
        }
    } else {
        match serde_json::from_slice(&req.body) {
            Ok(b) => b,
            Err(e) => {
                return error_response(
                    400,
                    &format!("invalid /api/db/probe-locator-only body: {e}"),
                    ctx,
                )
            }
        }
    };
    let options = fold_db::db_operations::LocatorOnlyProbeOptions {
        max_tips: body.max_tips,
        tip_page: body.tip_page,
        storage_prefix: None,
    };
    match host.db.probe_locator_only_population(options).await {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({ "probe_locator_only": report }),
            ctx.user_id.as_str(),
        )),
        Err(e) => mapped_error_response("probe-locator-only failed", e, ctx),
    }
}
