//! Org sync routes: register, grant and revoke member, deactivate and list targets.

use super::*;

/// `POST /api/org/sync/register` — arm cloud sync for an org database.
///
/// Body: `{ "org_hash": "<64 hex>", "e2e_key_b64": "<base64 32 bytes>", "slug"?: "..." }`.
/// The target database is taken from `X-LastDB-Db`; personal context is rejected.
/// Stores the target and reconfigures the live SyncEngine when cloud sync is on.
pub(super) async fn execute_org_sync_register_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    // Check before the registry upsert and before the cloud owner claim.
    match crate::host::org_sync_registration_allowed(&host.home) {
        Ok(true) => {}
        Ok(false) => {
            return error_response(
                403,
                "org sync registration is disabled on this node; DEV must opt in with org_sync_registration.json",
                ctx,
            );
        }
        Err(e) => {
            tracing::warn!(error = %e, "org sync registration policy denied request");
            return error_response(403, "org sync registration policy is invalid", ctx);
        }
    }
    #[derive(Deserialize)]
    struct Body {
        org_hash: String,
        e2e_key_b64: String,
        #[serde(default)]
        slug: String,
    }
    let body: Body = match serde_json::from_slice(&req.body) {
        Ok(b) => b,
        Err(e) => {
            return error_response(
                400,
                &format!("invalid /api/org/sync/register body: {e}"),
                ctx,
            )
        }
    };
    let Some(storage_prefix) = ctx.storage_prefix.as_deref() else {
        return error_response(
            400,
            "org sync registration requires an org database in X-LastDB-Db",
            ctx,
        );
    };
    match host
        .db
        .register_org_sync_target_for_storage_prefix(
            &body.org_hash,
            storage_prefix,
            &body.e2e_key_b64,
            &body.slug,
        )
        .await
    {
        Ok(target) => {
            let sync_enabled = host.db.is_sync_enabled();
            let prefixes = if let Some(engine) = host.db.sync_engine() {
                engine.target_prefixes().await
            } else {
                vec![]
            };
            // Principal membership on the cloud head: claim ownership so
            // org_hash-scoped presigns are allowed for this user.
            let mut registry_note: Option<String> = None;
            let mut bootstrap_note: Option<String> = None;
            if let Some(engine) = host.db.sync_engine() {
                match engine.register_cloud_head_owner(&body.org_hash).await {
                    Ok(reg) => {
                        registry_note = Some(format!(
                            "cloud registry: principal {} role={}",
                            reg.principal_hash, reg.role
                        ));
                        if sync_enabled {
                            match engine.bootstrap_target_by_prefix(&body.org_hash).await {
                                Ok(Some(outcome)) => {
                                    bootstrap_note = Some(format!(
                                        "org photograph bootstrap complete: last_seq={} entries_replayed={}",
                                        outcome.last_seq, outcome.entries_replayed
                                    ));
                                }
                                Ok(None) => {
                                    bootstrap_note = Some(
                                        "org photograph bootstrap skipped: target is not active in the sync engine"
                                            .to_string(),
                                    );
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        error = %e,
                                        org_hash = %body.org_hash,
                                        "org sync armed and owner claimed, but photograph bootstrap failed"
                                    );
                                    bootstrap_note = Some(format!(
                                        "org photograph bootstrap failed; sync will retry: {e}"
                                    ));
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            org_hash = %body.org_hash,
                            "org sync armed locally but cloud head owner claim failed"
                        );
                        registry_note = Some(format!(
                            "cloud registry owner claim failed (kick/grant unavailable until this succeeds): {e}"
                        ));
                    }
                }
            }
            json_ok(&envelope(
                &serde_json::json!({
                    "ok": true,
                    "org_hash": target.org_hash,
                    "storage_prefixes": target.storage_prefixes,
                    "unprefixed_schema_names": target.unprefixed_schema_names,
                    "slug": target.slug,
                    "active": target.active,
                    "sync_enabled": sync_enabled,
                    "target_prefixes": prefixes,
                    "registry_note": registry_note,
                    "bootstrap_note": bootstrap_note,
                    "note": if sync_enabled {
                        "org cloud-sync target armed; writes under the registered database prefix append to the org log"
                    } else {
                        "target stored; cloud_sync.json not configured — enable cloud sync to upload/download"
                    },
                }),
                ctx.user_id.as_str(),
            ))
        }
        Err(e) => error_response(400, &format!("register org sync failed: {e}"), ctx),
    }
}

/// `POST /api/org/sync/grant-member` — owner grants live cloud access.
/// Body: `{ "org_hash": "<64 hex>", "target_user_hash": "<principal>", "role"?: "writer"|"reader" }`
pub(super) async fn execute_org_sync_grant_member_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    struct Body {
        org_hash: String,
        target_user_hash: String,
        #[serde(default)]
        role: Option<String>,
    }
    let body: Body = match serde_json::from_slice(&req.body) {
        Ok(b) => b,
        Err(e) => {
            return error_response(
                400,
                &format!("invalid /api/org/sync/grant-member body: {e}"),
                ctx,
            )
        }
    };
    let role = body.role.as_deref().unwrap_or("writer");
    let Some(engine) = host.db.sync_engine() else {
        return error_response(
            400,
            "cloud sync is not enabled; configure cloud_sync.json first",
            ctx,
        );
    };
    match engine
        .grant_cloud_head_member(&body.org_hash, &body.target_user_hash, role)
        .await
    {
        Ok(reg) => json_ok(&envelope(
            &serde_json::json!({
                "ok": true,
                "db_hash": reg.db_hash,
                "principal_hash": reg.principal_hash,
                "role": reg.role,
            }),
            ctx.user_id.as_str(),
        )),
        Err(e) => error_response(400, &format!("grant member failed: {e}"), ctx),
    }
}

/// `POST /api/org/sync/revoke-member` — kick or leave.
/// Body: `{ "org_hash": "<64 hex>", "target_user_hash"?: "<principal>" }`
/// Omit target to leave as self.
pub(super) async fn execute_org_sync_revoke_member_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    struct Body {
        org_hash: String,
        #[serde(default)]
        target_user_hash: Option<String>,
    }
    let body: Body = match serde_json::from_slice(&req.body) {
        Ok(b) => b,
        Err(e) => {
            return error_response(
                400,
                &format!("invalid /api/org/sync/revoke-member body: {e}"),
                ctx,
            )
        }
    };
    let target = body.target_user_hash.unwrap_or_else(|| ctx.user_id.clone());
    let Some(engine) = host.db.sync_engine() else {
        return error_response(
            400,
            "cloud sync is not enabled; configure cloud_sync.json first",
            ctx,
        );
    };
    match engine
        .revoke_cloud_head_member(&body.org_hash, &target)
        .await
    {
        Ok(()) => json_ok(&envelope(
            &serde_json::json!({
                "ok": true,
                "org_hash": body.org_hash,
                "principal_hash": target,
                "revoked": true,
            }),
            ctx.user_id.as_str(),
        )),
        Err(e) => error_response(400, &format!("revoke member failed: {e}"), ctx),
    }
}

/// `GET /api/org/sync/targets` — list registered org cloud-sync targets.
/// `POST /api/org/sync/deactivate` — disarm org cloud-sync targets.
///
/// Body: `{"org_hash"?: "<64 hex>", "slug"?: "<slug>", "dry_run"?: bool}`.
/// One selector is required. The rows stay in the registry (inactive) with
/// their key, so `POST /api/org/sync/register` for the same org re-arms it.
/// No cloud call: this never claims or releases an Exemem registry row.
pub(super) async fn execute_org_sync_deactivate_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Body {
        #[serde(default)]
        org_hash: Option<String>,
        #[serde(default)]
        slug: Option<String>,
        #[serde(default)]
        dry_run: bool,
    }
    let body: Body = match serde_json::from_slice(&req.body) {
        Ok(b) => b,
        Err(e) => {
            return error_response(
                400,
                &format!("invalid /api/org/sync/deactivate body: {e}"),
                ctx,
            )
        }
    };
    match host
        .db
        .deactivate_org_sync_targets(body.org_hash.as_deref(), body.slug.as_deref(), body.dry_run)
        .await
    {
        Ok(changed) => {
            let prefixes = if let Some(engine) = host.db.sync_engine() {
                engine.target_prefixes().await
            } else {
                vec![]
            };
            let hashes: Vec<&str> = changed.iter().map(|t| t.org_hash.as_str()).collect();
            json_ok(&envelope(
                &serde_json::json!({
                    "dry_run": body.dry_run,
                    "matched": changed.len(),
                    "deactivated": if body.dry_run { 0 } else { changed.len() },
                    "org_hashes": hashes,
                    "target_prefix_count": prefixes.len(),
                }),
                ctx.user_id.as_str(),
            ))
        }
        Err(fold_db::error::FoldDbError::Config(msg)) => error_response(400, &msg, ctx),
        Err(e) => error_response(500, &format!("org sync deactivate failed: {e}"), ctx),
    }
}

pub(super) async fn execute_org_sync_targets_route(
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    match host.db.list_org_sync_targets().await {
        Ok(targets) => {
            let sync_enabled = host.db.is_sync_enabled();
            let prefixes = if let Some(engine) = host.db.sync_engine() {
                engine.target_prefixes().await
            } else {
                vec![]
            };
            let list: Vec<serde_json::Value> = targets
                .iter()
                .map(|t| {
                    serde_json::json!({
                        "org_hash": t.org_hash,
                        "storage_prefixes": t.storage_prefixes,
                        "unprefixed_schema_names": t.unprefixed_schema_names,
                        "slug": t.slug,
                        "active": t.active,
                        "registered_at": t.registered_at,
                        // Never return the raw e2e key in status.
                    })
                })
                .collect();
            json_ok(&envelope(
                &serde_json::json!({
                    "targets": list,
                    "sync_enabled": sync_enabled,
                    "target_prefixes": prefixes,
                }),
                ctx.user_id.as_str(),
            ))
        }
        Err(e) => error_response(500, &format!("list org sync targets failed: {e}"), ctx),
    }
}
