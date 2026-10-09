use super::*;

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

/// Policy bit surfaced on every compact JSON report, including dry-run.
/// True means `--execute` will pause Cloud Sync when it is on. Distinct from
/// `paused`, which is what this invocation actually did.
pub(super) fn compact_json_isolation_needed(collection: &str) -> bool {
    fold_db::storage::laststore::compact_requires_cloud_isolation(collection)
}

/// `POST /api/db/compact-record` — zip one HashRange key onto record molecule R.
pub(super) async fn execute_compact_record_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    struct Body {
        schema: String,
        hash: String,
        range: String,
    }
    let body: Body = match serde_json::from_slice(&req.body) {
        Ok(b) => b,
        Err(e) => {
            return error_response(
                400,
                &format!("invalid /api/db/compact-record body: {e}"),
                ctx,
            )
        }
    };
    if body.schema.trim().is_empty() || body.hash.trim().is_empty() || body.range.trim().is_empty()
    {
        return error_response(400, "schema, hash, and range are required", ctx);
    }
    let resolved = match handlers::resolve_schema_name(host, body.schema.trim()) {
        Ok(v) => v,
        Err(e) => return render(Err(e), ctx),
    };
    let Some(canonical) = resolved else {
        return error_response(404, &format!("schema not found: {}", body.schema), ctx);
    };
    match host
        .db
        .compact_record_molecule_key(&canonical, body.hash.trim(), body.range.trim())
        .await
    {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({
                "schema": report.schema_name,
                "hash": report.hash,
                "range": body.range.trim(),
                "keys_compacted": report.keys_compacted,
                "keys_skipped": report.keys_skipped,
                "record_molecule_uuid": report.record_molecule_uuid,
            }),
            ctx.user_id.as_str(),
        )),
        Err(e) => mapped_error_response(
            "compact-record failed",
            fold_db::FoldDbError::Schema(e),
            ctx,
        ),
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
/// Defaults: dry_run=true. Atom execute records the retired chunk shas so the
/// next manifest cut can cover the keep-set shrink with a signed receipt.
/// Execute skips while a backup cut is held (photograph packing lock).
pub(super) async fn execute_db_compact_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
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

// lint:fn-size-ok moved verbatim from exec.rs; splitting this function is separate work.
pub(super) async fn execute_db_compact_walk(
    host: &Host,
    ctx: &AccessContext,
    collections: Vec<String>,
    walk_all: bool,
    dry_run: bool,
) -> UdsResponse {
    // Captured user-state planes rewrite live rows. If Cloud Sync is on,
    // pause it once for the whole walk and restore afterwards. Dry-run and
    // capture-free-only walks never touch cloud. A crash mid-compact leaves
    // sync paused (safer than capturing the rewrite).
    let isolation_needed = collections
        .iter()
        .any(|collection| compact_json_isolation_needed(collection));
    let isolate = !dry_run && isolation_needed;
    let file_state_before = crate::cloud::cloud_sync_file_state(&host.home);
    let mut cloud_paused = false;
    if isolate && file_state_before == "on" {
        if let Err(e) = host.db.set_cloud_sync_disabled_live(true).await {
            tracing::warn!(
                error = %e,
                "compact: live engine pause failed; continuing with durable file pause"
            );
        }
        if let Err(e) = crate::cloud::pause_cloud_sync_file(&host.home) {
            return error_response(500, &format!("compact: cloud pause failed: {e}"), ctx);
        }
        cloud_paused = true;
    }

    // Cloud Off retires the backup target and takes this same packing lock.
    // Keep pause/resume outside the lock; protect the entire physical walk so
    // no new cut can start between collections. Recheck after the pause to
    // cover a target acquired since the route's early refusal check.
    let compact = || async {
        let mut reports: Vec<serde_json::Value> = Vec::new();
        let mut first_error: Option<String> = None;
        for collection in &collections {
            match host.db.compact_collection(collection, dry_run).await {
                Ok(report) => match serde_json::to_value(&report) {
                    Ok(v) => reports.push(v),
                    Err(e) => {
                        first_error = Some(format!("serialize compact report: {e}"));
                        break;
                    }
                },
                Err(e) => {
                    first_error = Some(format!("compact {collection} failed: {e}"));
                    break;
                }
            }
        }
        (reports, first_error)
    };
    let result = if dry_run {
        Some(compact().await)
    } else {
        host.db.run_unless_backup_publish_target_held(compact).await
    };

    let mut cloud_restored = false;
    let mut cloud_restore_error: Option<String> = None;
    if cloud_paused {
        match crate::cloud::resume_cloud_sync_file(&host.home) {
            Ok(_) => {
                cloud_restored = true;
                if let Err(e) = host.db.reenable_cloud_sync_live().await {
                    cloud_restore_error = Some(format!(
                        "file restored but live engine not re-enabled ({e}); \
                         restart lastdbd or run `lastdb cloud on`"
                    ));
                }
            }
            Err(e) => {
                cloud_restore_error = Some(format!(
                    "cloud left paused ({e}). Run `lastdb cloud on` to restore."
                ));
            }
        }
    }

    let cloud = serde_json::json!({
        "isolation_needed": isolation_needed,
        "was_enabled": file_state_before == "on",
        "paused": cloud_paused,
        "restored": cloud_restored,
        "file_state_before": file_state_before,
        "file_state_after": crate::cloud::cloud_sync_file_state(&host.home),
        "restore_error": cloud_restore_error,
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
// lint:file-size-ok moved verbatim from exec.rs; cohesive unit, split further in a later pass
