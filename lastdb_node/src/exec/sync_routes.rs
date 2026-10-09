use super::*;

/// `POST /api/sync/cloud-off` — intentional Cloud Sync pause.
///
/// 1. Stamp grace clock on the live engine (`set_cloud_sync_disabled(true)`).
/// 2. Rename `cloud_sync.json` → `cloud_sync.json.paused` so reboots stay off.
///
/// Local R/W never blocked. Prefer this over hand-editing files.
pub(super) async fn execute_sync_cloud_off_route(ctx: &AccessContext, host: &Host) -> UdsResponse {
    let (engine_ok, engine_note) = match host.db.set_cloud_sync_disabled_live(true).await {
        Ok(()) => (true, "live engine paused (grace clock stamped)".to_string()),
        Err(e) => (false, format!("live engine not updated: {e}")),
    };
    let file_renamed = match crate::cloud::pause_cloud_sync_file(&host.home) {
        Ok(renamed) => renamed,
        Err(e) => {
            return error_response(500, &format!("cloud-off durable pause failed: {e}"), ctx);
        }
    };
    if let Err(error) = crate::cloud::clear_cloud_resume_requested(&host.home)
        .and_then(|()| crate::cloud::clear_cloud_resume_ready(&host.home))
    {
        return error_response(500, &format!("cloud-off marker clear failed: {error}"), ctx);
    }
    let status = host.db.sync_status().await;
    json_ok(&envelope(
        &serde_json::json!({
            "ok": true,
            "intent": "off",
            "file_state": crate::cloud::cloud_sync_file_state(&host.home),
            "file_renamed": file_renamed,
            "engine_paused": engine_ok,
            "engine_note": engine_note,
            "recording_local_changes": status.as_ref().map(|s| s.recording_local_changes),
            "sync_off_grace_expired": status.as_ref().map(|s| s.sync_off_grace_expired),
            "reenable_strategy": status.as_ref().and_then(|s| s.reenable_strategy.clone()),
            "sync_off_grace_secs": status.as_ref().map(|s| s.sync_off_grace_secs),
            "note": "Use `lastdb cloud on` only while this daemon has a live sync engine. A paused boot requires a separate recovery plan.",
        }),
        ctx.user_id.as_str(),
    ))
}

/// `POST /api/sync/cloud-on` — restore durable intent + re-enable live engine.
pub(super) async fn execute_sync_cloud_on_route(ctx: &AccessContext, host: &Host) -> UdsResponse {
    if crate::cloud::cloud_sync_file_state(&host.home) == "unset" {
        return error_response(400, "cloud sync is not configured", ctx);
    }
    if crate::cloud::cloud_resume_required_path(&host.home).exists() {
        return error_response(
            409,
            "Cloud Sync remains Off: the paused-home backup does not reconcile older peer writes with local Deletes or update existing peers",
            ctx,
        );
    }
    let resume_pending = host.db.sync_engine().is_none();
    if resume_pending {
        if let Err(error) = crate::cloud::mark_cloud_resume_required(&host.home) {
            return error_response(500, &format!("cloud-on resume marker failed: {error}"), ctx);
        }
        return error_response(
            409,
            "Cloud Sync remains Off: this daemon has no sync engine. The paused-home backup does not reconcile peer writes or update existing peers.",
            ctx,
        );
    }
    let file_restored = match crate::cloud::resume_cloud_sync_file(&host.home) {
        Ok(restored) => restored,
        Err(e) => {
            return error_response(400, &format!("cloud-on durable resume failed: {e}"), ctx);
        }
    };
    let (reenable, engine_note) = match host.db.reenable_cloud_sync_live().await {
        Ok(outcome) => {
            let note = format!(
                "live reenable strategy={} recording={}",
                outcome.strategy, outcome.now_recording
            );
            let value = serde_json::to_value(&outcome).unwrap_or(serde_json::Value::Null);
            (value, note)
        }
        Err(e) => (
            serde_json::Value::Null,
            format!(
                "file restored but live engine not running ({e}) — restart lastdbd to boot sync"
            ),
        ),
    };
    let status = host.db.sync_status().await;
    json_ok(&envelope(
        &serde_json::json!({
            "ok": true,
            "intent": "on",
            "file_state": crate::cloud::cloud_sync_file_state(&host.home),
            "file_restored": file_restored,
            "resume_pending": resume_pending,
            "engine_note": engine_note,
            "reenable": reenable,
            "recording_local_changes": status.as_ref().map(|s| s.recording_local_changes),
            "note": "This daemon resumed its live sync engine. Check its sync status before you depend on cloud sync.",
        }),
        ctx.user_id.as_str(),
    ))
}

/// `POST /api/sync/cloud-resume-primary` accepts or reads one durable job.
/// The long inventory and upload run after the owner route returns.
pub(super) fn execute_sync_cloud_resume_primary_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize, Default)]
    #[serde(default, deny_unknown_fields)]
    struct Body {
        status: bool,
        finish: bool,
        fresh_from_local: bool,
        accept_local_damage: bool,
        restore_manifest_sha256: Option<String>,
        restore_home: Option<String>,
    }
    if !ctx.is_owner {
        return error_response(403, "primary resume is owner-only", ctx);
    }
    let body: Body = match serde_json::from_slice(&req.body) {
        Ok(body) => body,
        Err(error) => {
            return error_response(
                400,
                &format!("invalid primary resume request: {error}"),
                ctx,
            )
        }
    };
    if (body.status && body.finish) || (body.fresh_from_local && (body.status || body.finish)) {
        return error_response(400, "primary resume action is ambiguous", ctx);
    }
    if body.accept_local_damage && !body.fresh_from_local {
        return error_response(400, "local damage acceptance requires fresh mode", ctx);
    }
    if !body.finish && body.restore_home.is_some() {
        return error_response(400, "restore home requires finish", ctx);
    }
    let result = if body.status {
        crate::primary_resume_job::status(host)
    } else if body.finish {
        match body.restore_manifest_sha256.as_deref() {
            Some(sha) => crate::primary_resume_job::finish(host, sha, body.restore_home.as_deref()),
            None => Err("finish requires the restored normal manifest SHA-256".into()),
        }
    } else {
        if body.restore_manifest_sha256.is_some() {
            return error_response(400, "restore manifest SHA-256 requires finish", ctx);
        }
        crate::primary_resume_job::start(host, body.fresh_from_local, body.accept_local_damage)
    };
    match result {
        Ok(job) => json_ok(&envelope(&job, ctx.user_id.as_str())),
        Err(error) => error_response(409, &error, ctx),
    }
}

/// `POST /api/sync/quarantine-replay-blocker` — clear one active replay pin.
///
/// Body (required):
/// ```json
/// { "target": "<label from status.replay_blocker>", "seq": <u64> }
/// ```
///
/// Refuses wildcards: target+seq must match the live blocker. Mode is chosen
/// by the blocker code (delete for corrupt; skip_local for apply_failed).
pub(super) async fn execute_sync_quarantine_replay_blocker_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(serde::Deserialize)]
    struct Body {
        target: String,
        seq: u64,
    }
    let body: Body = if req.body.is_empty() {
        return error_response(
            400,
            "quarantine-replay-blocker requires JSON body {\"target\",\"seq\"}",
            ctx,
        );
    } else {
        match serde_json::from_slice(&req.body) {
            Ok(b) => b,
            Err(e) => {
                return error_response(
                    400,
                    &format!("invalid /api/sync/quarantine-replay-blocker body: {e}"),
                    ctx,
                );
            }
        }
    };
    let Some(engine) = host.db.sync_engine() else {
        return error_response(409, "cloud sync is not configured on this node", ctx);
    };
    match engine
        .quarantine_current_replay_blocker(&body.target, body.seq)
        .await
    {
        Ok(outcome) => json_ok(&envelope(
            &serde_json::json!({
                "ok": true,
                "target": outcome.target,
                "seq": outcome.seq,
                "mode": outcome.mode,
                "deleted_log_objects": outcome.deleted_log_objects,
                "note": if outcome.mode == "skip_local" {
                    "local skip only — cloud object left for peers; retry sync / wait for next cycle"
                } else {
                    "cloud object deleted; retry sync so the cursor advances past the 404+tombstone"
                },
            }),
            ctx.user_id.as_str(),
        )),
        Err(e) => {
            let msg = e.to_string();
            let code = if msg.contains("no replay blocker")
                || msg.contains("mismatch")
                || msg.contains("unsupported replay blocker")
            {
                400
            } else if msg.contains("while sync is running") {
                409
            } else {
                500
            };
            error_response(
                code,
                &format!("quarantine-replay-blocker failed: {msg}"),
                ctx,
            )
        }
    }
}

/// `POST /api/sync/backup-concurrency` — get, set, or clear the live
/// sealed-home backup PUT concurrency override.
pub(super) async fn execute_sync_backup_concurrency_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    struct Body {
        action: String,
        #[serde(default)]
        value: Option<usize>,
    }

    if !ctx.is_owner {
        return error_response(403, "backup concurrency is owner-only", ctx);
    }
    let body: Body = match serde_json::from_slice(&req.body) {
        Ok(body) => body,
        Err(error) => {
            return error_response(
                400,
                &format!("invalid /api/sync/backup-concurrency body: {error}"),
                ctx,
            )
        }
    };
    let Some(engine) = host.db.sync_engine() else {
        return error_response(409, "cloud sync is not configured on this node", ctx);
    };
    let status = match body.action.as_str() {
        "get" => engine.backup_upload_concurrency_status().await,
        "set" => {
            let Some(value) = body.value else {
                return error_response(400, "value is required for set", ctx);
            };
            match engine
                .set_backup_upload_concurrency_override(Some(value))
                .await
            {
                Ok(status) => status,
                Err(error) => return error_response(400, &error, ctx),
            }
        }
        "clear" => match engine.set_backup_upload_concurrency_override(None).await {
            Ok(status) => status,
            Err(error) => return error_response(400, &error, ctx),
        },
        _ => return error_response(400, "action must be get, set, or clear", ctx),
    };
    json_ok(&envelope(
        &serde_json::json!({ "backup_upload_concurrency": status }),
        ctx.user_id.as_str(),
    ))
}

/// `POST /api/sync/heal-staging` — live cloud snapshot + clear upload staging.
///
/// Never stops Mini. Uses the running sync engine's open store handle so local
/// R/W continues. Staging is cleared only after snapshot upload succeeds.
pub(super) async fn execute_sync_heal_staging_route(
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    match host.db.heal_cloud_staging().await {
        Ok(report) => json_ok(&envelope(
            &serde_json::json!({
                "ok": true,
                "snapshot_seq": report.snapshot_seq,
                "staging_before": report.staging_before,
                "staging_cleared": report.staging_cleared,
                "staging_after": report.staging_after,
                "note": "live heal: concurrent local R/W were not stopped",
            }),
            ctx.user_id.as_str(),
        )),
        Err(e) => error_response(500, &format!("sync heal-staging failed: {e}"), ctx),
    }
}

/// `POST /api/sync/backup-gc`: accept durable work or attach to a receipt.
/// Acceptance does no manifest IO or cloud work. `{status:true}` reads the
/// latest job, `{job_id:UUID}` an exact job. Object receipts page by sequence.
pub(super) fn execute_sync_backup_gc_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    if crate::cloud::cloud_resume_required_path(&host.home).exists() {
        return error_response(
            409,
            "backup cleanup is blocked while Cloud Sync is Off",
            ctx,
        );
    }
    #[derive(Default, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Body {
        /// When omitted, default true (preview only). Pass `false` to delete.
        #[serde(default)]
        dry_run: Option<bool>,
        #[serde(default)]
        status: bool,
        job_id: Option<String>,
        request_id: Option<String>,
        #[serde(default)]
        after: u64,
        limit: Option<usize>,
    }
    let body: Body = if req.body.is_empty() {
        Body::default()
    } else {
        match serde_json::from_slice(&req.body) {
            Ok(b) => b,
            Err(e) => {
                return error_response(400, &format!("invalid /api/sync/backup-gc body: {e}"), ctx)
            }
        }
    };
    let engine = host.db.sync_engine();
    if body.status || body.job_id.is_some() {
        if body.request_id.is_some() || body.dry_run.is_some() {
            return error_response(400, "status cannot accept execution parameters", ctx);
        }
        if engine.is_none() {
            return match fold_db::sync::engine::gc_jobs::backup_gc_receipts_without_engine(
                host.home.clone(),
                body.job_id.as_deref(),
                body.after,
                body.limit.unwrap_or(0),
            ) {
                Ok((job, objects)) => json_ok(&envelope(
                    &serde_json::json!({"ok":true,"job":job,"objects":objects}),
                    ctx.user_id.as_str(),
                )),
                Err(error) => error_response(400, &error.to_string(), ctx),
            };
        }
        let engine = engine.as_ref().expect("checked engine");
        let job = match engine.backup_gc_job(body.job_id.as_deref()) {
            Ok(job) => job,
            Err(error) => return error_response(400, &error.to_string(), ctx),
        };
        let objects = match job.as_ref() {
            Some(job) => {
                match engine.backup_gc_objects(&job.job_id, body.after, body.limit.unwrap_or(0)) {
                    Ok(objects) => objects,
                    Err(error) => return error_response(500, &error.to_string(), ctx),
                }
            }
            None => Vec::new(),
        };
        return json_ok(&envelope(
            &serde_json::json!({"ok":true, "job":job, "objects":objects}),
            ctx.user_id.as_str(),
        ));
    }
    let Some(engine) = engine else {
        return error_response(
            400,
            "cloud sync engine unavailable; no GC work accepted",
            ctx,
        );
    };
    match engine.start_backup_gc(body.dry_run.unwrap_or(true), body.request_id.as_deref()) {
        Ok(job) => {
            let mut response = json_ok(&envelope(
                &serde_json::json!({"ok":true, "job_id":job.job_id, "job":job}),
                ctx.user_id.as_str(),
            ));
            response.status = 202;
            response
        }
        Err(error) => error_response(400, &error.to_string(), ctx),
    }
}

/// `POST /api/sync/prefix-inventory` — read-only R2 prefix/category size
/// breakdown for the connected cloud account.
///
/// No body. List-only: never deletes, never fetches an object body, never
/// touches the DELETE presign path. Uses the admin UDS deadline (same
/// rationale as `SyncBackupGc`) because listing a large scope must not die on
/// the short generic POST timeout.
pub(super) async fn execute_sync_prefix_inventory_route(
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    // Caller/config precondition (cloud sync never enabled on this home) is a
    // 400, not a 500 — mirrors `execute_sync_backup_gc_route`'s missing-cache
    // check, and keeps a genuine listing/transport failure on 500.
    if host.db.sync_engine().is_none() {
        return error_response(
            400,
            "cloud sync is not configured (no cloud_sync.json / sync engine)",
            ctx,
        );
    }
    let report = match host.db.prefix_inventory().await {
        Ok(report) => report,
        Err(e) => return error_response(500, &format!("prefix inventory failed: {e}"), ctx),
    };

    json_ok(&envelope(
        &serde_json::json!({
            "ok": true,
            "report": report,
        }),
        ctx.user_id.as_str(),
    ))
}

/// `POST /api/sync/laststore-snapshot` — live LastStore cloud backup snapshot.
pub(super) async fn execute_sync_laststore_snapshot_route(
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let cache_path = crate::host::backup_manifest_cache_path(&host.home);
    let previous_manifest = match std::fs::read(&cache_path) {
        Ok(bytes) => {
            match serde_json::from_slice::<fold_db::storage::laststore::BackupManifest>(&bytes) {
                Ok(manifest) => Some(manifest),
                Err(e) => {
                    return error_response(
                        500,
                        &format!("parse {} failed: {e}", cache_path.display()),
                        ctx,
                    )
                }
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => {
            return error_response(
                500,
                &format!("read {} failed: {e}", cache_path.display()),
                ctx,
            )
        }
    };

    let (manifest, report) = match host
        .db
        .laststore_cloud_snapshot(previous_manifest.as_ref())
        .await
    {
        Ok(result) => result,
        Err(e) => return mapped_error_response("laststore cloud snapshot failed", e, ctx),
    };

    let encoded = match serde_json::to_vec_pretty(&manifest) {
        Ok(bytes) => bytes,
        Err(e) => return error_response(500, &format!("encode backup manifest failed: {e}"), ctx),
    };
    // The engine already mirrored this manifest through temp file + rename.
    // A concurrent `backup-gc` reads this path outside the publish turn, so a
    // second, in-place write of an 11 MiB body here would expose a torn keep
    // set. Place it atomically too.
    let tmp_path = cache_path.with_extension("json.route.tmp");
    let placed =
        std::fs::write(&tmp_path, encoded).and_then(|()| std::fs::rename(&tmp_path, &cache_path));
    if let Err(e) = placed {
        let _ = std::fs::remove_file(&tmp_path);
        return error_response(
            500,
            &format!(
                "write {} failed after latest CAS: {e}",
                cache_path.display()
            ),
            ctx,
        );
    }

    json_ok(&envelope(
        &serde_json::json!({
            "ok": true,
            "report": report,
            "manifest_cache": cache_path,
        }),
        ctx.user_id.as_str(),
    ))
}
// lint:file-size-ok moved verbatim from exec.rs; cohesive unit, split further in a later pass
