//! `/api/databases` catalog listing and signed-log invitation acceptance.

use std::path::Path;

use base64::Engine as _;
use cloud_backup_protocol::Digest;
use fold_db::access::AccessContext;
use serde::Deserialize;
use serde_json::{json, Value};

use super::owned_response;
use crate::exec::database_owned;
use crate::exec::reply::{envelope, error_response, json_ok};
use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};
use crate::registered_share::{self, RegisteredShare};
use crate::share_invitation::MAX_SEALED_INVITATION_BYTES;

pub(super) const ACCEPT_PATH: &str = "/api/databases/accept";

pub(super) fn list(ctx: &AccessContext, host: &Host) -> UdsResponse {
    let owner = match host.owned.owner() {
        Ok(entry) => entry,
        Err(error) => return owned_response(error, ctx),
    };
    let owned = match host.owned.owned_entries() {
        Ok(entries) => entries,
        Err(error) => return owned_response(error, ctx),
    };
    let Ok(shares) = registered_share::list(&host.home) else {
        return error_response(503, "database registry is unavailable", ctx);
    };
    let mut databases = vec![database_owned::row(&owner)];
    databases.extend(owned.iter().map(database_owned::row));
    databases.extend(shares.iter().map(|share| database(&host.home, share)));
    json_ok(&envelope(&json!({ "databases": databases }), &ctx.user_id))
}

fn database(home: &Path, share: &RegisteredShare) -> Value {
    let mut value = json!(share);
    let ready = share.signed_log && crate::database_replay::prefix_durable(home, share.id.as_str());
    value["state"] = json!(if ready { "ready" } else { "restoring" });
    value["access"] = json!("reader");
    if share.signed_log {
        value["selfAccess"] = json!(crate::log_sync::received_access::view(
            home,
            share.id.as_str()
        ));
    }
    value["kind"] = json!("received");
    value
}

pub(super) async fn accept(req: &UdsRequest, ctx: &AccessContext, host: &Host) -> UdsResponse {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Request {
        invitation: String,
        publisher: Digest,
        name: Option<String>,
        #[serde(default)]
        database_id: Option<String>,
    }
    let Ok(body) = decode::<Request>(req, MAX_SEALED_INVITATION_BYTES) else {
        return error_response(400, "invalid database invitation request", ctx);
    };
    let Ok(sealed) = decode_sealed(&body.invitation, MAX_SEALED_INVITATION_BYTES) else {
        return error_response(400, "invalid encoded invitation", ctx);
    };
    let result = registered_share::accept_signed_log(
        &host.home,
        &sealed,
        &body.publisher,
        body.name.as_deref(),
        body.database_id.as_deref(),
    );
    if let Ok(share) = &result {
        if registered_share::receiver_store(host, &share.id)
            .await
            .is_err()
        {
            return error_response(503, "database store is unavailable", ctx);
        }
    }
    registration_reply(result, ctx, &host.home)
}

fn decode<T: serde::de::DeserializeOwned>(req: &UdsRequest, limit: usize) -> Result<T, ()> {
    if req.body.len() > limit.div_ceil(3) * 4 + 512 {
        return Err(());
    }
    serde_json::from_slice(&req.body).map_err(|_| ())
}

fn decode_sealed(text: &str, limit: usize) -> Result<Vec<u8>, ()> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(text)
        .map_err(|_| ())?;
    if bytes.is_empty() || bytes.len() > limit {
        return Err(());
    }
    Ok(bytes)
}

fn registration_reply(
    result: Result<RegisteredShare, crate::share_cache::ShareCacheError>,
    ctx: &AccessContext,
    home: &Path,
) -> UdsResponse {
    use crate::share_cache::ShareCacheError;
    match result {
        Ok(share) => json_ok(&envelope(
            &json!({ "database": database(home, &share) }),
            &ctx.user_id,
        )),
        Err(ShareCacheError::Invalid("database selector")) => {
            error_response(400, "invalid database selector", ctx)
        }
        Err(ShareCacheError::Invalid("database id")) => {
            error_response(400, "database id does not match the invitation", ctx)
        }
        Err(error @ (ShareCacheError::Invalid(_) | ShareCacheError::Invitation(_))) => {
            error_response(400, &error.to_string(), ctx)
        }
        Err(ShareCacheError::Unavailable) => error_response(404, "database is not registered", ctx),
        Err(_) => error_response(503, "database registration is unavailable", ctx),
    }
}
