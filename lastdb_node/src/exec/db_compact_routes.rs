use super::*;
mod backup;
mod flush;
mod native;
mod status;

pub(super) use backup::execute as execute_db_native_backup_route;
pub(super) use flush::execute as execute_db_flush_route;
pub(super) use status::execute as execute_db_native_status_route;

/// `POST /api/db/clear-history` — trim or purge mutation-history rows.
///
/// Body (all optional):
/// ```json
/// { "schema": "<name>", "keep_last": 1, "dry_run": true }
/// ```
/// Defaults: all schemas, keep newest 1 event per field key, dry_run=true.
/// Pass `"keep_last": 0` for a full history purge (latest-only: tips remain).
pub(super) async fn execute_db_clear_history_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    struct Body {
        #[serde(default)]
        schema: Option<String>,
        #[serde(default)]
        keep_last: Option<usize>,
        /// When omitted, default true (preview only). Pass `false` to delete.
        #[serde(default)]
        dry_run: Option<bool>,
    }
    let body: Body = if req.body.is_empty() {
        Body {
            schema: None,
            keep_last: None,
            dry_run: None,
        }
    } else {
        match serde_json::from_slice(&req.body) {
            Ok(b) => b,
            Err(e) => {
                return error_response(
                    400,
                    &format!("invalid /api/db/clear-history body: {e}"),
                    ctx,
                )
            }
        }
    };
    // Default 1 (trim extras). Explicit 0 = purge all history (latest tip only).
    let keep_last = body.keep_last.unwrap_or(1);
    let dry_run = body.dry_run.unwrap_or(true);
    let schema = body
        .schema
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());

    match host
        .db
        .clear_mutation_history(schema, keep_last, dry_run)
        .await
    {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({ "clear_history": report }),
            ctx.user_id.as_str(),
        )),
        Err(e) => mapped_error_response("clear-history failed", e, ctx),
    }
}

/// `POST /api/db/compact` — compact one allowlisted LastStore collection,
/// or every allowlisted collection when `all` is true / `collection` is omitted.
/// The all-walk skips `COMPACT_NAMED_ONLY` planes (`cas_blobs`); name one to run it.
///
/// Body:
/// ```json
/// { "collection": "schemas", "dry_run": true }
/// { "all": true, "dry_run": true }
/// ```
/// Defaults: dry_run=true. Native storage retains its normal compaction guards.
pub(super) async fn execute_db_compact_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    if let Some(runtime) = host.db.db_ops().namespaced_store().disk_pack_runtime() {
        return native::execute(req, ctx, runtime).await;
    }
    #[derive(Deserialize)]
    struct Body {
        #[serde(default)]
        collection: Option<String>,
        #[serde(default)]
        all: Option<bool>,
        #[serde(default)]
        dry_run: Option<bool>,
    }
    let body: Body = if req.body.is_empty() {
        Body {
            collection: None,
            all: Some(true),
            dry_run: Some(true),
        }
    } else {
        match serde_json::from_slice(&req.body) {
            Ok(b) => b,
            Err(e) => {
                return error_response(400, &format!("invalid /api/db/compact body: {e}"), ctx)
            }
        }
    };
    let dry_run = body.dry_run.unwrap_or(true);
    let want_all = body.all.unwrap_or(false);
    let named = body
        .collection
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    if want_all && named.is_some() {
        return error_response(400, "compact: pass collection or all, not both", ctx);
    }
    let collections: Vec<String> = if let Some(name) = named {
        vec![name.to_string()]
    } else {
        // A plane the operator must name (`cas_blobs`) is not part of the walk.
        fold_db::storage::laststore::COMPACT_ALLOWLIST
            .iter()
            .filter(|name| !fold_db::storage::laststore::COMPACT_NAMED_ONLY.contains(name))
            .map(|s| (*s).to_string())
            .collect()
    };
    if collections.is_empty() {
        return error_response(400, "compact: collection must be non-empty", ctx);
    }
    let walk_all = named.is_none();
    if dry_run {
        return execute_db_compact_walk(host, ctx, collections, walk_all, true).await;
    }
    if host.db.backup_publish_target_is_held().await {
        return skipped_backup_cut_compact_response(&collections, walk_all, ctx, None);
    }
    execute_db_compact_walk(host, ctx, collections, walk_all, false).await
}

/// `{ "dry_run": true }`
///
/// Default dry_run=true. Execute stamps successor-history SHAs into pending
/// purged atom retirements. Skips while a backup cut is held.
pub(super) async fn execute_db_stamp_purged_atom_retirements_route(
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
        Body {
            dry_run: Some(true),
        }
    } else {
        match serde_json::from_slice(&req.body) {
            Ok(b) => b,
            Err(e) => {
                return error_response(
                    400,
                    &format!("invalid /api/db/stamp-purged-atom-retirements body: {e}"),
                    ctx,
                )
            }
        }
    };
    let dry_run = body.dry_run.unwrap_or(true);
    if !dry_run && host.db.backup_publish_target_is_held().await {
        return error_response(
            409,
            "stamp-purged-atom-retirements skipped: backup cut is held",
            ctx,
        );
    }
    let store = host.db.db_ops().namespaced_store();
    let result = if dry_run {
        store.report_committed_successor_history()
    } else {
        store.stamp_pending_committed_successor_history()
    };
    match result {
        Ok(report) => {
            let payload = serde_json::json!({
                "stamp_purged_atom_retirements": {
                    "dry_run": dry_run,
                    "scope_ok": report.scope_ok,
                    "retired_sha_count": report.retired_shas.len(),
                    "retired_bytes": report.retired_bytes,
                    "groups_without_local_file": report.groups_without_local_file,
                    "unstamped_successor_history_refs": report.unstamped_successor_history_refs,
                    "local_shas": report.local_shas,
                    "pending_after": report.pending_after,
                }
            });
            json_ok(&envelope(&payload, ctx.user_id.as_str()))
        }
        Err(e) => error_response(500, &format!("stamp-purged-atom-retirements: {e}"), ctx),
    }
}

pub(super) fn skipped_backup_cut_compact_response(
    collections: &[String],
    walk_all: bool,
    ctx: &AccessContext,
    cloud: Option<serde_json::Value>,
) -> UdsResponse {
    let reports: Vec<serde_json::Value> = collections
        .iter()
        .map(|collection| {
            serde_json::json!({
                "collection": collection,
                "dry_run": false,
                "live_keys": 0,
                "bytes_before": 0,
                "bytes_after": null,
                "never_compact": false,
                // A held backup cut is a genuine refusal: this call compacted
                // nothing and the caller must not read the plane as reclaimable
                // right now.
                "compactable_here": false,
                "executed": false,
                "skipped_reason": "backup cut is held",
                "skipped_backup_cut": true,
            })
        })
        .collect();
    let cloud = cloud.unwrap_or_else(|| {
        serde_json::json!({
            "isolation_needed": false,
            "was_enabled": false,
            "paused": false,
            "restored": false,
            "file_state_before": null,
            "file_state_after": null,
            "restore_error": null,
            "skipped_backup_cut": true,
        })
    });
    let compact_json = if walk_all {
        serde_json::json!({
            "all": true,
            "reports": reports,
            "skipped_backup_cut": true,
            "cloud": cloud,
            "named_only_not_walked": fold_db::storage::laststore::COMPACT_NAMED_ONLY,
        })
    } else if let Some(mut compact_json) = reports.into_iter().next() {
        if let Some(obj) = compact_json.as_object_mut() {
            obj.insert("cloud".to_string(), cloud);
        }
        compact_json
    } else {
        serde_json::json!({ "skipped_backup_cut": true, "cloud": cloud })
    };
    json_ok(&envelope(
        &serde_json::json!({ "compact": compact_json }),
        ctx.user_id.as_str(),
    ))
}

pub(super) async fn execute_db_compact_walk(
    host: &Host,
    ctx: &AccessContext,
    collections: Vec<String>,
    walk_all: bool,
    dry_run: bool,
) -> UdsResponse {
    // Local compaction never changes protected cloud credentials or resumes an engine.
    let compact = || compact_collections(host, &collections, dry_run);
    let result = if dry_run {
        Some(compact().await)
    } else {
        host.db.run_unless_backup_publish_target_held(compact).await
    };
    let file_state = crate::cloud::cloud_sync_file_state(&host.home);
    let cloud = serde_json::json!({
        "isolation_needed": false,
        "was_enabled": false,
        "paused": false,
        "restored": false,
        "file_state_before": file_state,
        "file_state_after": file_state,
        "restore_error": null,
        "legacy_cloud_retired": true,
    });

    let Some((mut reports, first_error)) = result else {
        return skipped_backup_cut_compact_response(&collections, walk_all, ctx, Some(cloud));
    };

    if let Some(e) = first_error {
        let extra = cloud
            .get("restore_error")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| format!("; {s}"))
            .unwrap_or_default();
        return error_response(500, &format!("{e}{extra}"), ctx);
    }

    let compact_json = if walk_all {
        serde_json::json!({
            "all": true,
            "reports": reports,
            "cloud": cloud,
            "named_only_not_walked": fold_db::storage::laststore::COMPACT_NAMED_ONLY,
        })
    } else if let Some(mut compact_json) = reports.pop() {
        if let Some(obj) = compact_json.as_object_mut() {
            obj.insert("cloud".to_string(), cloud);
        }
        compact_json
    } else {
        serde_json::json!({ "cloud": cloud })
    };
    json_ok(&envelope(
        &serde_json::json!({ "compact": compact_json }),
        ctx.user_id.as_str(),
    ))
}

/// Compact each collection in order. Stops at the first failure and returns
/// the reports gathered so far with that error.
async fn compact_collections(
    host: &Host,
    collections: &[String],
    dry_run: bool,
) -> (Vec<serde_json::Value>, Option<String>) {
    let mut reports: Vec<serde_json::Value> = Vec::new();
    for collection in collections {
        match host.db.compact_collection(collection, dry_run).await {
            Ok(report) => match serde_json::to_value(&report) {
                Ok(v) => reports.push(v),
                Err(e) => return (reports, Some(format!("serialize compact report: {e}"))),
            },
            Err(e) => return (reports, Some(format!("compact {collection} failed: {e}"))),
        }
    }
    (reports, None)
}
