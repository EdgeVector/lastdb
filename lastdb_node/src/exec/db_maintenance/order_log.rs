//! Owner-socket routes that audit, compact and repair the order log and audit the pin log.

use super::*;

/// `POST /api/db/order-log-audit` — compare every `moc:{M}` order-log count
/// against its molecule's live `mk:` record count. Read-only.
pub(in crate::exec) async fn execute_db_order_log_audit_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize, Default)]
    struct Body {
        /// Soft cap on `mk:` records walked before this call stops at the next
        /// molecule boundary and returns a resume cursor.
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
                    &format!("invalid /api/db/order-log-audit body: {e}"),
                    ctx,
                )
            }
        }
    };
    match host
        .db
        .audit_order_log_counts(body.max_keys, body.after_key.as_deref())
        .await
    {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({ "order_log_audit": report }),
            ctx.user_id.as_str(),
        )),
        Err(e) => mapped_error_response("order-log-audit failed", e, ctx),
    }
}

/// `POST /api/db/pin-log-audit` — bounded, resumable, read-only pin-log plane
/// audit. Classifies entry rows via durable published-F; never deletes.
pub(in crate::exec) async fn execute_db_pin_log_audit_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize, Default)]
    struct Body {
        /// Cap on pin-log keys this call walks before returning a resume cursor.
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
                    &format!("invalid /api/db/pin-log-audit body: {e}"),
                    ctx,
                )
            }
        }
    };

    #[cfg(not(feature = "cloud-sync"))]
    {
        let _ = (body, host);
        return error_response(
            501,
            "cloud-sync support is not compiled into this daemon",
            ctx,
        );
    }

    #[cfg(feature = "cloud-sync")]
    {
        match host
            .db
            .audit_pin_log(body.max_keys, body.after_key.as_deref())
            .await
        {
            Ok(report) => json_ok(&envelope(
                &serde_json::json!({ "pin_log_audit": report }),
                ctx.user_id.as_str(),
            )),
            // A malformed resume cursor arrives as
            // `SchemaError::InvalidCursor` and maps to 400 on its own; this
            // used to sniff the message text for "after_key must be", which
            // classified one message and missed every other caller fault the
            // route can raise.
            Err(e) => mapped_error_response("pin-log-audit failed", e, ctx),
        }
    }
}

/// `POST /api/db/order-log-bloat-audit` — measure append-only order-log excess
/// (stale entries + zero-live residue) with exact stored bytes. Read-only.
pub(in crate::exec) async fn execute_db_order_log_bloat_audit_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize, Default)]
    struct Body {
        #[serde(default)]
        max_keys: Option<usize>,
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
                    &format!("invalid /api/db/order-log-bloat-audit body: {e}"),
                    ctx,
                )
            }
        }
    };
    match host
        .db
        .audit_order_log_bloat(body.max_keys, body.after_key.as_deref())
        .await
    {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({ "order_log_bloat_audit": report }),
            ctx.user_id.as_str(),
        )),
        Err(e) => mapped_error_response("order-log-bloat-audit failed", e, ctx),
    }
}

/// `POST /api/db/compact-order-log` — plan or delete the order log of a
/// zero-live molecule, a bloated molecule, and a clean molecule. Does not
/// write a new log. Dry-run writes nothing. `retention_seconds` does not keep
/// rows. Execute skips while a backup cut is held.
pub(in crate::exec) async fn execute_db_compact_order_log_route(
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
        #[serde(default)]
        retention_seconds: Option<u64>,
    }
    let body: Body = if req.body.is_empty() {
        Body::default()
    } else {
        match serde_json::from_slice(&req.body) {
            Ok(body) => body,
            Err(error) => {
                return error_response(
                    400,
                    &format!("invalid /api/db/compact-order-log body: {error}"),
                    ctx,
                )
            }
        }
    };
    let dry_run = body.dry_run.unwrap_or(true);
    if !dry_run && host.db.backup_publish_target_is_held().await {
        let skipped = fold_db::db_operations::OrderLogZeroLiveCompactionReport {
            dry_run: false,
            skipped_backup_cut: true,
            retention_seconds: body
                .retention_seconds
                .unwrap_or(fold_db::atom::molecule_key_codec::ORDER_LOG_RETENTION_SECS),
            ..Default::default()
        };
        return json_ok(&envelope(
            &serde_json::json!({ "order_log_compaction": skipped }),
            ctx.user_id.as_str(),
        ));
    }
    match host
        .db
        .compact_order_log_zero_live(
            dry_run,
            body.max_keys,
            body.after_key.as_deref(),
            body.retention_seconds,
        )
        .await
    {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({ "order_log_compaction": report }),
            ctx.user_id.as_str(),
        )),
        Err(error) => mapped_error_response("compact-order-log failed", error, ctx),
    }
}

/// `POST /api/db/repair-order-log-shortfall` — the verb writes nothing.
// `heap_route!` awaits this function. The body has no await.
#[allow(clippy::unused_async)]
pub(in crate::exec) async fn execute_db_repair_order_log_shortfall_route(
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
                    &format!("invalid /api/db/repair-order-log-shortfall body: {error}"),
                    ctx,
                )
            }
        }
    };
    match host.db.repair_order_log_shortfall(
        body.dry_run.unwrap_or(true),
        body.max_keys,
        body.after_key.as_deref(),
    ) {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({ "order_log_repair": report }),
            ctx.user_id.as_str(),
        )),
        Err(error) => error_response(
            500,
            &format!("repair-order-log-shortfall failed: {error}"),
            ctx,
        ),
    }
}
