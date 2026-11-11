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
    if let Some(response) = reject_kind(body.kind.as_deref(), ctx) {
        return response;
    }
    let spec = body.spec.map(spec_binding);
    if let Err(error) = owned::validate_create(&body.name, spec.as_ref()) {
        return fail(error, ctx);
    }
    if let Some(response) = share_conflict(host, &body.name, ctx) {
        return response;
    }
    match host
        .owned
        .create(&host.home, Arc::clone(&host.keypair), &body.name, spec)
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
    })
}

fn share_conflict(host: &Host, name: &str, ctx: &AccessContext) -> Option<UdsResponse> {
    if let Some(response) = registered_name(host, name, ctx) {
        return Some(response);
    }
    match crate::named_grant::received_name_taken(&host.home, name) {
        Ok(false) => None,
        Ok(true) => Some(error_response(
            409,
            "database name is already registered",
            ctx,
        )),
        Err(()) => Some(error_response(503, "database registry is unavailable", ctx)),
    }
}

fn registered_name(host: &Host, name: &str, ctx: &AccessContext) -> Option<UdsResponse> {
    match registered_share::list(&host.home) {
        Ok(shares)
            if shares
                .iter()
                .any(|share| share.name.as_deref() == Some(name)) =>
        {
            Some(error_response(
                409,
                "database name is already registered",
                ctx,
            ))
        }
        Ok(_) => None,
        Err(_) => Some(error_response(503, "database registry is unavailable", ctx)),
    }
}

fn reject_kind(kind: Option<&str>, ctx: &AccessContext) -> Option<UdsResponse> {
    match kind {
        None | Some("shareable") => None,
        Some("personal") => Some(fail(OwnedError::ReservedUnprovisioned, ctx)),
        Some(_) => Some(fail(OwnedError::InvalidName, ctx)),
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
