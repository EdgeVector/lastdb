//! Local open-link mint for People consent-code hand-off.
//!
//! Membership is the signed grant chain (`grant` / `revoke` / `grants`). There
//! is no ExeMem share/members or sealed-invitation route.
use super::*;
use crate::host::owned;
use crate::lastdb_uds::uds_router::method_not_allowed_response;
use crate::log_sync::CloudState;
use crate::named_grant::{cloud_failure, ConfigError};
use serde_json::Value;

const NO_ACCOUNT: &str = "this node has no cloud account";
const ENROLLING: &str = "the database is still enrolling with the cloud; retry";
pub(super) const RETRY_AFTER_SECS: u64 = 2;

pub(super) enum Route {
    Link,
    Lease,
}

impl Route {
    pub(super) fn parse(path: &str) -> Option<Self> {
        match path {
            "share/link" => Some(Self::Link),
            "lease" => Some(Self::Lease),
            _ => None,
        }
    }
}

pub(super) fn execute(
    route: Route,
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
    selector: &str,
) -> UdsResponse {
    if matches!(route, Route::Lease) {
        return lease(req, ctx, host, selector);
    }
    let entry = match owned::find_shareable(&host.home, selector) {
        Ok(entry) => entry,
        Err(message) => {
            let status = match message.as_str() {
                "database registry is unavailable" => 503,
                "database is not registered on this node" => 404,
                _ => 403,
            };
            return error_response(status, &message, ctx);
        }
    };
    match (route, req.method.as_str()) {
        (Route::Link, "POST") => link(req, ctx, host, &entry),
        (Route::Link, _) => method_not_allowed_response("POST"),
        (Route::Lease, _) => unreachable!("the lease route is answered above"),
    }
}

/// `POST /api/databases/{id}/lease`. An empty body or `{}` reads the lease
/// this node holds (and, for an owned database, the length it chose).
/// `{"length_ms": N}` sets the lease length of an owned database and
/// `{"length_ms": null}` clears it.
fn lease(req: &UdsRequest, ctx: &AccessContext, host: &Host, selector: &str) -> UdsResponse {
    if req.method != "POST" {
        return method_not_allowed_response("POST");
    }
    let body = if req.body.is_empty() {
        Some(serde_json::Map::new())
    } else if req.body.len() > 256 {
        None
    } else {
        match serde_json::from_slice::<Value>(&req.body) {
            Ok(Value::Object(map)) if map.keys().all(|key| key == "length_ms") => Some(map),
            _ => None,
        }
    };
    let Some(body) = body else {
        return error_response(400, "invalid lease request", ctx);
    };
    let result = match body.get("length_ms") {
        None => crate::log_sync::lease_status::status(host, selector),
        Some(Value::Null) => crate::log_sync::lease_status::set_length(host, selector, None),
        Some(Value::Number(number)) => match number.as_u64() {
            Some(length) => crate::log_sync::lease_status::set_length(host, selector, Some(length)),
            None => return error_response(400, "length_ms must be a whole number", ctx),
        },
        Some(_) => return error_response(400, "length_ms must be a whole number or null", ctx),
    };
    match result {
        Ok(value) => render(Ok(value), ctx),
        Err(error) => {
            let reply = error.reply();
            error_response(reply.status, reply.message, ctx)
        }
    }
}

fn link(req: &UdsRequest, ctx: &AccessContext, host: &Host, entry: &owned::Entry) -> UdsResponse {
    if !req.body.is_empty() {
        return error_response(400, "invalid sharing request", ctx);
    }
    if let Err(error) = ensure_enrolled(host, &entry.id) {
        return render(Err(host_error(error)), ctx);
    }
    match crate::share_link::issue(host, entry) {
        Ok(token) => render(
            Ok(json!({
                "databaseId": entry.id,
                "link": token,
            })),
            ctx,
        ),
        Err(error) => link_error(host, &entry.id, &error, ctx),
    }
}

fn ensure_enrolled(host: &Host, database_id: &str) -> Result<(), ConfigError> {
    if payment_blocked(host, database_id) {
        return Err(ConfigError::PaymentRequired);
    }
    match crate::log_sync::wait_for_enrollment(host, database_id) {
        CloudState::Enrolled => Ok(()),
        CloudState::NoAccount => Err(ConfigError::NoAccount),
        CloudState::Enrolling if payment_blocked(host, database_id) => {
            Err(ConfigError::PaymentRequired)
        }
        CloudState::Enrolling => Err(ConfigError::Enrolling),
        CloudState::Unverified => Err(ConfigError::Unconfirmed),
    }
}

fn payment_blocked(host: &Host, database_id: &str) -> bool {
    host.cloud_sync.snapshot().payment_hold
        && crate::log_sync::database_cloud_state(&host.home, database_id) == CloudState::Enrolling
}

fn link_error(
    host: &Host,
    database_id: &str,
    error: &crate::share_link::IssueError,
    ctx: &AccessContext,
) -> UdsResponse {
    let (status, message) = error.status();
    if error.is_cloud_connection() {
        let failure = cloud_failure(&host.home, database_id, message);
        return render(Err(host_error(failure)), ctx);
    }
    error_response(status, message, ctx)
}

fn host_error(error: ConfigError) -> HostError {
    let retry = matches!(error, ConfigError::Enrolling).then_some(RETRY_AFTER_SECS);
    let (status, code, body) = match error {
        ConfigError::PaymentRequired => (
            402,
            "payment_required".to_owned(),
            json!({
                "reason": crate::cloud::PAYMENT_REQUIRED_MESSAGE,
                "message": crate::cloud::PAYMENT_REQUIRED_MESSAGE,
            }),
        ),
        ConfigError::NoAccount => (
            409,
            "no_cloud_account".to_owned(),
            json!({ "message": NO_ACCOUNT }),
        ),
        ConfigError::Enrolling => (
            503,
            "database_enrolling".to_owned(),
            json!({ "message": ENROLLING }),
        ),
        ConfigError::Unconfirmed => (
            503,
            "share_configuration_unconfirmed".to_owned(),
            json!({ "message": "share configuration is not confirmed" }),
        ),
    };
    let mut body = body;
    body["ok"] = json!(false);
    body["error"] = json!(code);
    let built = HostError::structured(status, code, body);
    match retry {
        Some(seconds) => built.with_retry_after(seconds),
        None => built,
    }
}
