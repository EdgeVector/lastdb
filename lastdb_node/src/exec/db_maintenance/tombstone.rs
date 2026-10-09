//! Owner-socket routes that audit and drain tombstones and audit legacy key forks.

use super::*;

/// `POST /api/db/tombstone-flag-audit` — measure (and optionally repair) `mk:`
/// records whose `meta.tombstoned` disagrees with their atom content.
pub(in crate::exec) async fn execute_db_tombstone_flag_audit_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize, Default)]
    struct Body {
        #[serde(default)]
        schema: Option<String>,
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
                    &format!("invalid /api/db/tombstone-flag-audit body: {e}"),
                    ctx,
                )
            }
        }
    };
    let dry_run = body.dry_run.unwrap_or(true);
    match host
        .db
        .audit_tombstone_flags(
            body.schema.as_deref(),
            !dry_run,
            body.max_keys,
            body.after_key.as_deref(),
        )
        .await
    {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({ "tombstone_flag_audit": report }),
            ctx.user_id.as_str(),
        )),
        Err(e) => mapped_error_response("tombstone-flag-audit failed", e, ctx),
    }
}

/// `POST /api/db/drain-legacy-tombstones` — discover and optionally hard-erase
/// one bounded page of tombstone-content storage slots.
pub(in crate::exec) async fn execute_db_drain_legacy_tombstones_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize, Default)]
    struct Body {
        #[serde(default)]
        schema: Option<String>,
        #[serde(default)]
        dry_run: Option<bool>,
        #[serde(default)]
        max_keys: Option<usize>,
        #[serde(default)]
        after_key: Option<String>,
    }
    let body: Body = if req.body.is_empty() {
        Body::default()
    } else {
        match serde_json::from_slice(&req.body) {
            Ok(body) => body,
            Err(error) => {
                return error_response(
                    400,
                    &format!("invalid /api/db/drain-legacy-tombstones body: {error}"),
                    ctx,
                )
            }
        }
    };
    match host
        .db
        .drain_legacy_tombstones(
            body.schema.as_deref(),
            body.dry_run.unwrap_or(true),
            body.max_keys.unwrap_or(50_000),
            body.after_key.as_deref(),
        )
        .await
    {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({ "legacy_tombstone_drain": report }),
            ctx.user_id.as_str(),
        )),
        Err(error) => mapped_error_response("legacy-tombstone drain failed", error, ctx),
    }
}

/// `POST /api/db/legacy-key-fork-audit` — audit or drain one bounded page of
/// legacy/plain HashKey-encoding tips.
pub(in crate::exec) async fn execute_db_legacy_key_fork_audit_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize, Default)]
    struct Body {
        #[serde(default)]
        dry_run: Option<bool>,
        #[serde(default)]
        max_keys: Option<usize>,
        #[serde(default)]
        after_key: Option<String>,
    }
    let body: Body = if req.body.is_empty() {
        Body::default()
    } else {
        match serde_json::from_slice(&req.body) {
            Ok(body) => body,
            Err(error) => {
                return error_response(
                    400,
                    &format!("invalid /api/db/legacy-key-fork-audit body: {error}"),
                    ctx,
                )
            }
        }
    };
    match host
        .db
        .audit_legacy_key_forks(
            body.dry_run.unwrap_or(true),
            body.max_keys.unwrap_or(50_000),
            body.after_key.as_deref(),
        )
        .await
    {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({ "legacy_key_fork_audit": report }),
            ctx.user_id.as_str(),
        )),
        Err(error) => mapped_error_response("legacy-key-fork audit failed", error, ctx),
    }
}
