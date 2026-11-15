//! Create an owned database and bind its spec on the owner socket.
use super::*;
use crate::host::owned::{self, Entry, Kind, OwnedError, SpecBinding};
use crate::registered_share;
use serde_json::{json, Value};
use std::sync::Arc;

pub(super) const SPEC_PATH: &str = "/api/databases/spec";
const BODY_LIMIT: usize = 4 * 1024;

pub(super) async fn create(req: &UdsRequest, ctx: &AccessContext, host: &Host) -> UdsResponse {
    let Some(body) = decode::<CreateBody>(req) else {
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
            Some(SpecBinding {
                name: binding.name().to_owned(),
                version: binding.version(),
            })
        }
        None if !body.spec.as_ref().is_some_and(reserved_binding) => body.spec.map(spec_binding),
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
    let Some(body) = decode::<BindBody>(req) else {
        return error_response(400, "invalid spec binding", ctx);
    };
    if body.spec.as_ref().is_some_and(reserved_binding) {
        return error_response(400, "whole-database sharing requires a new database", ctx);
    }
    let spec = body.spec.map(spec_binding);
    match host.owned.bind_spec(&host.home, &body.id, spec) {
        Ok(entry) => json_ok(&envelope(&json!({ "database": row(&entry) }), &ctx.user_id)),
        Err(error) => fail(error, ctx),
    }
}

pub(super) fn row(entry: &Entry) -> Value {
    json!({
        "id": entry.id,
        "name": entry.name,
        "kind": match entry.kind {
            Kind::Personal => "personal",
            Kind::Shareable => "shareable",
        },
        "spec": entry.spec.as_ref().map(|spec| json!({
            "name": spec.name,
            "version": spec.version,
        })),
        "access": "owner",
        "state": "ready",
        "sharingMode": entry.spec.as_ref().filter(|spec| {
            database_policy::SpecBinding::is_whole_database_name(&spec.name) && spec.version == 1
        }).map(|_| "whole_database"),
    })
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
    let (status, message) = error.status();
    error_response(status, message, ctx)
}

fn spec_binding(spec: SpecBody) -> SpecBinding {
    SpecBinding {
        name: spec.name,
        version: spec.version,
    }
}

fn reserved_binding(spec: &SpecBody) -> bool {
    database_policy::SpecBinding::is_whole_database_name(&spec.name)
}

fn decode<T: serde::de::DeserializeOwned>(req: &UdsRequest) -> Option<T> {
    if req.body.len() > BODY_LIMIT {
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
    spec: Option<SpecBody>,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default, rename = "sharingMode")]
    sharing_mode: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BindBody {
    id: String,
    spec: Option<SpecBody>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SpecBody {
    name: String,
    version: u32,
}
