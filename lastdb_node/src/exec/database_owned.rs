//! Owned database creation, spec binding, catalog listing, and invitation acceptance.

use std::path::Path;
use std::sync::Arc;

use cloud_backup_protocol::Digest;
use serde_json::{json, Value};

use super::*;
use crate::host::owned::{self, Entry, Kind, OwnedError};
use crate::profile::PublicationSpec;
use crate::registered_share::{self, RegisteredShare};
use crate::share_invitation::MAX_SEALED_INVITATION_BYTES;

pub(super) const SPEC_PATH: &str = "/api/databases/spec";
pub(super) const ACCEPT_PATH: &str = "/api/databases/accept";
const BODY_LIMIT: usize = 4 * 1024;

pub(super) async fn create(req: &UdsRequest, ctx: &AccessContext, host: &Host) -> UdsResponse {
    let Some(body) = decode::<CreateBody>(req, BODY_LIMIT) else {
        return error_response(400, create_decode_message(req), ctx);
    };
    let kind = match body.kind.as_deref() {
        None | Some("shareable") => Kind::Shareable,
        Some("personal") => Kind::Personal,
        Some(_) => return error_response(400, "invalid database access policy", ctx),
    };
    let spec = match body.sharing_mode.as_deref() {
        Some("whole_database") if kind == Kind::Shareable && body.spec.is_none() => {
            let binding = database_policy::SpecBinding::whole_database();
            Some(PublicationSpec {
                name: binding.name().to_owned(),
                version: binding.version(),
            })
        }
        None if !body.spec.as_ref().is_some_and(reserved_binding) => body.spec,
        _ => return error_response(400, "invalid database sharing mode", ctx),
    };
    if let Err(error) = owned::validate_create(&body.name, spec.as_ref()) {
        return fail(error, ctx);
    }
    let requested_id = match req.header("Idempotency-Key") {
        None => None,
        Some(key) => match uuid::Uuid::parse_str(key) {
            Ok(id)
                if id.get_version_num() == 4
                    && body.sharing_mode.as_deref() == Some("whole_database")
                    && id.to_string() == key =>
            {
                Some(id)
            }
            _ => {
                return error_response(
                    400,
                    "whole-database creation uses a v4 UUID idempotency key",
                    ctx,
                )
            }
        },
    };
    if let Some(response) = share_conflict(host, &body.name, requested_id, ctx) {
        return response;
    }
    match host
        .owned
        .create(
            &host.home,
            Arc::clone(&host.keypair),
            &body.name,
            kind,
            spec,
            requested_id,
        )
        .await
    {
        Ok(entry) => json_ok(&envelope(&json!({ "database": row(&entry) }), &ctx.user_id)),
        Err(error) => fail(error, ctx),
    }
}

pub(super) fn bind(req: &UdsRequest, ctx: &AccessContext, host: &Host) -> UdsResponse {
    let Some(body) = decode::<BindBody>(req, BODY_LIMIT) else {
        return error_response(400, "invalid spec binding", ctx);
    };
    if body.spec.as_ref().is_some_and(reserved_binding) {
        return error_response(400, "whole-database sharing requires a new database", ctx);
    }
    match host.owned.bind_spec(&host.home, &body.id, body.spec) {
        Ok(entry) => json_ok(&envelope(&json!({ "database": row(&entry) }), &ctx.user_id)),
        Err(error) => fail(error, ctx),
    }
}

pub(super) fn list(ctx: &AccessContext, host: &Host) -> UdsResponse {
    let result = (|| {
        let owner = host.owned.owner()?;
        let owned = host.owned.owned_entries()?;
        let shares = registered_share::list(&host.home)
            .map_err(|_| HostError::new(503, "database registry is unavailable"))?;
        let mut databases = vec![row(&owner)];
        databases.extend(owned.iter().map(row));
        databases.extend(
            shares
                .iter()
                .map(|share| shared_database(&host.home, share)),
        );
        Ok(json!({ "databases": databases }))
    })();
    render(result, ctx)
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
    let Some(body) = decode::<Request>(req, MAX_SEALED_INVITATION_BYTES.div_ceil(3) * 4 + 512)
    else {
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

fn row(entry: &Entry) -> Value {
    json!({
        "id": entry.id,
        "name": entry.name,
        "kind": entry.kind.as_str(),
        "spec": entry.spec,
        "access": "owner",
        "state": "ready",
        "sharingMode": entry.spec.as_ref().filter(|spec| {
            database_policy::SpecBinding::is_whole_database_name(&spec.name) && spec.version == 1
        }).map(|_| "whole_database"),
    })
}

fn shared_database(home: &Path, share: &RegisteredShare) -> Value {
    let mut value = json!(share);
    let ready = share.signed_log && crate::database_replay::prefix_durable(home, share.id.as_str());
    value["state"] = json!(if ready { "ready" } else { "restoring" });
    value["access"] = json!("reader");
    if share.signed_log {
        value["selfAccess"] = json!(crate::log_sync::received::view(home, share.id.as_str()));
    }
    value["kind"] = json!("received");
    if share.whole_database() {
        value["sharingMode"] = json!("whole_database");
    }
    value
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
            &json!({ "database": shared_database(home, &share) }),
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

fn share_conflict(
    host: &Host,
    name: &str,
    requested_id: Option<uuid::Uuid>,
    ctx: &AccessContext,
) -> Option<UdsResponse> {
    let id = requested_id.map(|id| id.to_string());
    match registered_share::list(&host.home) {
        Ok(shares)
            if shares.iter().any(|share| {
                share.name.as_deref() == Some(name) || id.as_deref() == Some(share.id.as_str())
            }) =>
        {
            Some(error_response(
                409,
                "database name or ID is already registered",
                ctx,
            ))
        }
        Ok(_) => None,
        Err(_) => Some(error_response(503, "database registry is unavailable", ctx)),
    }
}

fn fail(error: OwnedError, ctx: &AccessContext) -> UdsResponse {
    render(Err(error.into()), ctx)
}

fn reserved_binding(spec: &PublicationSpec) -> bool {
    database_policy::SpecBinding::is_whole_database_name(&spec.name)
}

fn decode<T: serde::de::DeserializeOwned>(req: &UdsRequest, limit: usize) -> Option<T> {
    if req.body.len() > limit {
        return None;
    }
    serde_json::from_slice(&req.body).ok()
}

fn create_decode_message(req: &UdsRequest) -> &'static str {
    let Ok(value) = serde_json::from_slice::<Value>(&req.body) else {
        return "invalid database name";
    };
    if value.get("spec").is_some_and(|spec| !spec.is_null()) {
        "invalid spec binding"
    } else {
        "invalid database name"
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateBody {
    name: String,
    #[serde(default)]
    spec: Option<PublicationSpec>,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default, rename = "sharingMode")]
    sharing_mode: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BindBody {
    id: String,
    spec: Option<PublicationSpec>,
}
