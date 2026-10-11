//! The owner socket relays signed Org authority without enabling cloud sync.

use super::*;
use fold_db::sync::auth::ops::org_authority::{
    validate_account, OrgAuthorityPublishRequest, ORG_AUTHORITY_BODY_LIMIT,
    ORG_AUTHORITY_EPOCH_LIMIT, ORG_AUTHORITY_TERMINAL_JSON_LIMIT,
};
use fold_db::sync::auth::AuthClient;
use fold_db::sync::cloud_purge::protocol::CloudAccount;
use fold_db::sync::SyncError;
use std::sync::Arc;

pub(super) async fn execute_org_authority_publish_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    if let Some(response) = reject_request(req, ctx) {
        return response;
    }
    let request: OrgAuthorityPublishRequest = match serde_json::from_slice(&req.body) {
        Ok(request) => request,
        Err(_) => return error_response(400, "invalid signed Org authority request", ctx),
    };
    if host
        .db
        .flush_accepted_mutations(Duration::from_secs(30))
        .await
        .is_err()
    {
        return error_response(503, "the signed local Org epoch is not yet durable", ctx);
    }
    let Ok(auth) = signed_auth(host).await else {
        return error_response(503, "signed cloud account credentials are unavailable", ctx);
    };
    match auth.cloud_org_authority_publish(&request).await {
        Ok(receipt) => json_ok(&envelope(
            &serde_json::json!({"ok":true,"account":receipt.account,
                "epoch_hash":receipt.epoch_hash,"epoch_no":receipt.epoch_no}),
            &host.user_hash,
        )),
        Err(error) => cloud_error(&error, ctx),
    }
}

pub(super) async fn execute_org_authority_status_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    if let Some(response) = reject_request(req, ctx) {
        return response;
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Request {
        account: CloudAccount,
    }
    let request: Request = match serde_json::from_slice(&req.body) {
        Ok(request) => request,
        Err(_) => return error_response(400, "invalid Org authority status request", ctx),
    };
    if validate_account(&request.account).is_err() {
        return error_response(400, "an exact Org account is required", ctx);
    }
    if configuration_absent(&host.home) {
        return json_ok(&envelope(
            &serde_json::json!({"ok":true,"configured":false,"account":request.account,
                "absent":true,"epoch_hash":null,"epoch_no":null,"limits":limits()}),
            &host.user_hash,
        ));
    }
    let Ok(auth) = signed_auth(host).await else {
        return error_response(503, "signed cloud account credentials are unavailable", ctx);
    };
    match auth.cloud_org_authority_get(&request.account).await {
        Ok(tip) => json_ok(&envelope(
            &serde_json::json!({"ok":true,"configured":true,"account":tip.account,
                "absent":tip.absent,"epoch_hash":tip.epoch_hash,"epoch_no":tip.epoch_no,"limits":limits()}),
            &host.user_hash,
        )),
        Err(error) => cloud_error(&error, ctx),
    }
}

pub(super) fn reject_request(req: &UdsRequest, ctx: &AccessContext) -> Option<UdsResponse> {
    if !ctx.is_owner {
        return Some(error_response(403, "Org authority is owner-only", ctx));
    }
    if req.body.len() > ORG_AUTHORITY_BODY_LIMIT {
        return Some(error_response(
            413,
            "the signed Org authority body exceeds 1 MiB",
            ctx,
        ));
    }
    None
}

fn configuration_absent(home: &std::path::Path) -> bool {
    let (active, paused) = crate::cloud::cloud_sync_paths(home);
    [active, paused].iter().all(|path| {
        std::fs::symlink_metadata(path)
            .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
    })
}

fn limits() -> Value {
    serde_json::json!({"body_bytes":ORG_AUTHORITY_BODY_LIMIT,
        "epochs":ORG_AUTHORITY_EPOCH_LIMIT,"authority_json_bytes":ORG_AUTHORITY_TERMINAL_JSON_LIMIT})
}

pub(super) async fn signed_auth(host: &Host) -> Result<AuthClient, ()> {
    // Serialize refresh at this bridge. The new session
    // updates the existing engine's shared credential cell; Cloud Off stays off.
    static REFRESH: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    let _refresh = REFRESH.lock().await;
    let principal =
        crate::host::user_hash_from_pubkey(&host.keypair.public_key_base64()).map_err(|_| ())?;
    if principal != host.user_hash {
        return Err(());
    }
    let config = crate::cloud_purge_job::read_cloud_config(&host.home).map_err(|_| ())?;
    let engine = crate::cloud_purge_job::source_engine(host)
        .await
        .map_err(|_| ())?;
    let auth = engine.cloud_purge_auth();
    if !auth.cloud_org_authority_endpoint_matches(&config.api_url) {
        return Err(());
    }
    let refresh = crate::cloud::auth_refresh_callback(
        host.home.clone(),
        config.api_url,
        Arc::clone(&host.keypair),
    );
    // Registration verifies the returned principal against the exact public
    // key before these credentials reach the shared cache or either API call.
    let credential = tokio::time::timeout(Duration::from_secs(60), refresh())
        .await
        .map_err(|_| ())?
        .map_err(|_| ())?;
    let auth = auth.with_auth_refresh_callback(refresh);
    auth.update_auth(credential).await;
    Ok(auth)
}

pub(super) fn cloud_error(error: &SyncError, ctx: &AccessContext) -> UdsResponse {
    if matches!(error, SyncError::Storage(value) if value.contains("cloud_org_authority_busy")) {
        let code = if matches!(error, SyncError::Storage(value) if value.contains("cloud_org_authority_busy_or_changed"))
        {
            "cloud_org_authority_busy_or_changed"
        } else {
            "cloud_org_authority_busy"
        };
        return lastdb_host::envelope::owner_json_or_content_free(
            503,
            &serde_json::json!({"ok":false,"code":code,
                "message":"tracked Org work must finish; read the cloud tip and retry the same signed chain"}),
            ctx,
        );
    }
    let (status, message) = match error {
        SyncError::Storage(value) if value.contains("cloud_org_authority_item_too_large") => (
            413,
            "the terminal Org authority exceeds the 350 KiB cloud item budget",
        ),
        SyncError::Storage(value) if value.starts_with("org_authority_invalid:") => {
            (400, "the signed Org authority or cloud receipt is invalid")
        }
        SyncError::Storage(value) if value.contains("cloud_org_epoch_conflict") => (
            409,
            "the cloud Org epoch differs; read the current tip before another update",
        ),
        SyncError::Storage(value) if value.contains("cloud_authority_revoked") => {
            (403, "the cloud requires current active Org authority")
        }
        SyncError::Storage(value) if value.contains("cloud_org_authority_migration_required") => {
            (409, "the original Org owner enrollment proof is required")
        }
        SyncError::Auth(_) | SyncError::Banned(_) => {
            (403, "the cloud refused the signed account credentials")
        }
        _ => (
            502,
            "the cloud Org authority request failed; its result remains unconfirmed",
        ),
    };
    error_response(status, message, ctx)
}
