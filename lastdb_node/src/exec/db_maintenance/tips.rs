//! Owner-socket routes that drain, retain and migrate tip history.

use super::*;

/// `POST /api/db/drain-tip-history` — one bounded tip-version chain drain pass.
pub(in crate::exec) async fn execute_db_drain_tip_history_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    struct Body {
        #[serde(default)]
        dry_run: Option<bool>,
        #[serde(default)]
        max_keys: Option<usize>,
        #[serde(default)]
        max_prunes: Option<usize>,
        #[serde(default)]
        after_key: Option<String>,
        /// When true, resume from and advance the durable automatic-drain
        /// checkpoint for this explicit maintenance request.
        #[serde(default)]
        from_checkpoint: Option<bool>,
    }
    let body: Body = if req.body.is_empty() {
        Body {
            dry_run: None,
            max_keys: None,
            max_prunes: None,
            after_key: None,
            from_checkpoint: None,
        }
    } else {
        match serde_json::from_slice(&req.body) {
            Ok(b) => b,
            Err(e) => {
                return error_response(
                    400,
                    &format!("invalid /api/db/drain-tip-history body: {e}"),
                    ctx,
                )
            }
        }
    };
    let options = fold_db::db_operations::TipHistoryDrainOptions {
        dry_run: body.dry_run.unwrap_or(true),
        max_keys: body
            .max_keys
            .unwrap_or(fold_db::db_operations::TipHistoryDrainOptions::DEFAULT_MAX_KEYS),
        max_prunes: body.max_prunes,
        after_key: body.after_key,
        storage_prefix: None,
    };
    if body.from_checkpoint.unwrap_or(false) {
        match host
            .db
            .drain_tip_history_chains_from_checkpoint(options)
            .await
        {
            Ok((report, checkpoint)) => json_ok(&envelope(
                &serde_json::json!({
                    "drain_tip_history": report,
                    "checkpoint": checkpoint,
                    "from_checkpoint": true,
                }),
                ctx.user_id.as_str(),
            )),
            Err(e) => mapped_error_response("drain-tip-history failed", e, ctx),
        }
    } else {
        match host.db.drain_tip_history_chains(options).await {
            Ok(report) => json_ok(&envelope(
                &serde_json::json!({
                    "drain_tip_history": report,
                    "from_checkpoint": false,
                }),
                ctx.user_id.as_str(),
            )),
            Err(e) => mapped_error_response("drain-tip-history failed", e, ctx),
        }
    }
}

/// `POST /api/db/retain-superseded-versions` — drop expired live-record `tv:`
/// nodes (7-day window). Tombstoned heads are skipped. Execute skips while a
/// backup cut is held.
pub(in crate::exec) async fn execute_db_retain_superseded_versions_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    struct Body {
        #[serde(default)]
        dry_run: Option<bool>,
        #[serde(default)]
        max_keys: Option<usize>,
        #[serde(default)]
        max_prunes: Option<usize>,
        #[serde(default)]
        after_key: Option<String>,
        #[serde(default)]
        from_checkpoint: Option<bool>,
        #[serde(default)]
        retention_seconds: Option<u64>,
    }
    let body: Body = if req.body.is_empty() {
        Body {
            dry_run: None,
            max_keys: None,
            max_prunes: None,
            after_key: None,
            from_checkpoint: None,
            retention_seconds: None,
        }
    } else {
        match serde_json::from_slice(&req.body) {
            Ok(b) => b,
            Err(e) => {
                return error_response(
                    400,
                    &format!("invalid /api/db/retain-superseded-versions body: {e}"),
                    ctx,
                )
            }
        }
    };
    let dry_run = body.dry_run.unwrap_or(true);
    if !dry_run && host.db.backup_publish_target_is_held().await {
        let skipped = fold_db::db_operations::SupersededVersionRetentionReport {
            dry_run: false,
            retention_seconds: body
                .retention_seconds
                .unwrap_or(fold_db::atom::molecule_key_codec::SUPERSEDED_VERSION_RETENTION_SECS),
            skipped_backup_cut: true,
            settled: false,
            ..Default::default()
        };
        return json_ok(&envelope(
            &serde_json::json!({ "retain_superseded_versions": skipped }),
            ctx.user_id.as_str(),
        ));
    }
    let options = fold_db::db_operations::SupersededVersionRetentionOptions {
        dry_run,
        max_keys: body.max_keys.unwrap_or(256),
        max_prunes: body.max_prunes,
        after_key: body.after_key,
        storage_prefix: None,
        retention_seconds: body.retention_seconds,
    };
    if body.from_checkpoint.unwrap_or(false) {
        match host
            .db
            .retain_superseded_versions_from_checkpoint(options)
            .await
        {
            Ok((report, checkpoint)) => json_ok(&envelope(
                &serde_json::json!({
                    "retain_superseded_versions": report,
                    "checkpoint": checkpoint,
                    "from_checkpoint": true,
                }),
                ctx.user_id.as_str(),
            )),
            Err(e) => mapped_error_response("retain-superseded-versions failed", e, ctx),
        }
    } else {
        match host.db.retain_superseded_versions(options).await {
            Ok(report) => json_ok(&envelope(
                &serde_json::json!({
                    "retain_superseded_versions": report,
                    "from_checkpoint": false,
                }),
                ctx.user_id.as_str(),
            )),
            Err(e) => mapped_error_response("retain-superseded-versions failed", e, ctx),
        }
    }
}

/// `POST /api/db/migrate-thin-tips` — rewrite fat mk: tips to thin.
pub(in crate::exec) async fn execute_db_migrate_thin_tips_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize, Default)]
    struct Body {
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
                    &format!("invalid /api/db/migrate-thin-tips body: {e}"),
                    ctx,
                )
            }
        }
    };
    let dry_run = body.dry_run.unwrap_or(true);
    match host
        .db
        .migrate_thin_tips(dry_run, body.max_keys, body.after_key.as_deref())
        .await
    {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({ "migrate_thin_tips": report }),
            ctx.user_id.as_str(),
        )),
        Err(e) => mapped_error_response("migrate-thin-tips failed", e, ctx),
    }
}
