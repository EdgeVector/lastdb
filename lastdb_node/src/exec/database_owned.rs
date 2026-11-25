//! Owned database creation, spec binding, catalog listing, and open-link accept.

use std::sync::Arc;

use serde_json::{json, Value};

use super::*;
use crate::host::owned::{self, Entry, Kind, OwnedError};
use crate::profile::PublicationSpec;
use crate::registered_share::{self, RegisteredShare};

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
    // Shareable databases are whole-database only. A filtered or app catalog
    // `spec` on create is refused.
    if body.spec.is_some() {
        return error_response(400, "invalid database sharing mode", ctx);
    }
    let spec = match body.sharing_mode.as_deref() {
        Some("whole_database") if kind == Kind::Shareable => {
            let binding = database_policy::SpecBinding::whole_database();
            Some(PublicationSpec {
                name: binding.name().to_owned(),
                version: binding.version(),
            })
        }
        None => None,
        _ => return error_response(400, "invalid database sharing mode", ctx),
    };
    if let Err(error) = owned::validate_create(&body.name, spec.as_ref()) {
        return fail(error, ctx);
    }
    let requested_id = match req.header("Idempotency-Key") {
        None => None,
        Some(key) if body.sharing_mode.as_deref() == Some("whole_database") => {
            if mutation_log::DatabaseId::from_hex(key).is_ok() {
                Some(key.to_owned())
            } else {
                return error_response(
                    400,
                    "whole-database creation uses a 64-hex database id idempotency key",
                    ctx,
                );
            }
        }
        Some(_) => {
            return error_response(
                400,
                "whole-database creation uses a 64-hex database id idempotency key",
                ctx,
            );
        }
    };
    let creating = host.owned.lock_creating().await;
    let Ok(_share_registry) = crate::share_link::lock_existing_registry(&host.home) else {
        return error_response(503, "database registry is unavailable", ctx);
    };
    let reserved_ids = match share_conflict(host, &body.name, requested_id.as_deref(), ctx) {
        Ok(ids) => ids,
        Err(response) => return response,
    };
    match host
        .owned
        .create(
            &creating,
            &host.home,
            Arc::clone(&host.keypair),
            owned::CreateArgs {
                name: &body.name,
                kind,
                spec,
                requested_id,
                reserved_ids: &reserved_ids,
            },
            |id, seed| {
                crate::log_sync::birth::mint(&host.home, id, host.keypair.public_key_bytes(), seed)
            },
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
    // Clearing a binding is still allowed. Attaching a filtered or app catalog
    // is not: create with `sharingMode: whole_database` instead.
    if body.spec.is_some() {
        return error_response(400, "filtered share specs are not supported", ctx);
    }
    match host.owned.bind_spec(&host.home, &body.id, body.spec) {
        Ok(entry) => json_ok(&envelope(&json!({ "database": row(&entry) }), &ctx.user_id)),
        Err(error) => fail(error, ctx),
    }
}

pub(super) fn list(ctx: &AccessContext, host: &Host) -> UdsResponse {
    render(listed(host), ctx)
}

fn listed(host: &Host) -> Result<Value, HostError> {
    let owner = host.owned.owner()?;
    let owned = host.owned.owned_entries()?;
    let shares = registered_share::list(&host.home)
        .map_err(|_| HostError::new(503, "database registry is unavailable"))?;
    let mut databases = vec![row(&owner)];
    databases.extend(owned.iter().map(row));
    for share in &shares {
        databases.push(shared_database(host, share));
    }
    Ok(json!({ "databases": databases }))
}

pub(super) async fn accept(req: &UdsRequest, ctx: &AccessContext, host: &Host) -> UdsResponse {
    if req.body.len() > 4096 {
        return error_response(400, "invalid link", ctx);
    }
    let Ok(body) = serde_json::from_slice::<LinkAccept>(&req.body) else {
        return error_response(400, "invalid link", ctx);
    };
    let Ok(opened) = crate::share_link::open(&body.link) else {
        return error_response(400, "invalid link", ctx);
    };
    let _creating = host.owned.lock_creating().await;
    let registered = match host
        .owned
        .claims(opened.database_id(), body.name.as_deref())
    {
        Ok(true) => Err(crate::share_cache::ShareCacheError::Invalid(
            "database selector already in use",
        )),
        Ok(false) => crate::share_link::register(&host.home, &opened, body.name.as_deref()),
        Err(_) => Err(crate::share_cache::ShareCacheError::Io),
    };
    if registered.is_ok() {
        crate::log_sync::request_wake(&host.home);
    }
    registration_reply(registered, ctx, host)
}

fn row(entry: &Entry) -> Value {
    let mut value = json!({
        "id": entry.id,
        "name": entry.name,
        "kind": entry.kind.as_str(),
        "spec": entry.spec,
        "access": "owner",
        "state": "ready",
        "sharingMode": entry.spec.as_ref().filter(|spec| {
            database_policy::SpecBinding::is_whole_database_name(&spec.name) && spec.version == 1
        }).map(|_| "whole_database"),
    });
    if let Some(length) = entry.lease_length_ms {
        value["leaseLengthMs"] = json!(length);
    }
    value
}

fn shared_database(host: &Host, share: &RegisteredShare) -> Value {
    let mut value = json!(share);
    // Ready once signed history holds birth (materialize runs on pull).
    let ready = share.signed_log
        && crate::log_sync::histories_for(host)
            .peek_with(share.id.as_str(), |history| history.owner().is_some())
            .unwrap_or(false);
    value["state"] = json!(if ready { "ready" } else { "restoring" });
    value["access"] = json!("reader");
    if let Some(access) = share
        .signed_log
        .then(|| crate::log_sync::received::view(host, share.id.as_str()))
        .flatten()
    {
        value["selfAccess"] = access;
    }
    value["kind"] = json!("received");
    if share.whole_database() {
        value["sharingMode"] = json!("whole_database");
    }
    value["publish"] = crate::log_sync::publish_row(share.id.as_str(), share.signed_log);
    value
}

fn registration_reply(
    result: Result<RegisteredShare, crate::share_cache::ShareCacheError>,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    use crate::share_cache::ShareCacheError;
    match result {
        Ok(share) => {
            let database = shared_database(host, &share);
            json_ok(&envelope(&json!({ "database": database }), &ctx.user_id))
        }
        Err(ShareCacheError::Invalid("database selector")) => {
            error_response(400, "invalid database selector", ctx)
        }
        Err(error @ ShareCacheError::Invalid(_)) => error_response(400, &error.to_string(), ctx),
        Err(ShareCacheError::Unavailable) => error_response(404, "database is not registered", ctx),
        Err(_) => error_response(503, "database registration is unavailable", ctx),
    }
}

fn share_conflict(
    host: &Host,
    name: &str,
    id: Option<&str>,
    ctx: &AccessContext,
) -> Result<Vec<String>, UdsResponse> {
    let Ok(shares) = registered_share::list(&host.home) else {
        return Err(error_response(503, "database registry is unavailable", ctx));
    };
    if shares
        .iter()
        .any(|share| share.name.as_deref() == Some(name) || id == Some(share.id.as_str()))
    {
        return Err(error_response(
            409,
            "database name or ID is already registered",
            ctx,
        ));
    }
    Ok(shares
        .into_iter()
        .map(|share| share.id.as_str().to_owned())
        .collect())
}

fn fail(error: OwnedError, ctx: &AccessContext) -> UdsResponse {
    render(Err(error.into()), ctx)
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
        "invalid database sharing mode"
    } else {
        "invalid database name"
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateBody {
    name: String,
    /// Present only so a filtered create is refused with a clear message.
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LinkAccept {
    link: String,
    name: Option<String>,
}
