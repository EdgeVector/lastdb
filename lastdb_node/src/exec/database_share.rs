//! Local open-material mint for one shareable database.
//! Decided state: docs/designs/cloud-api.md (signed history; grants in-chain).
//! Membership is the signed grant chain. People wraps the open token as a
//! consent code. This route does not call ExeMem members or blob/bytes.
use super::*;
use crate::host::owned;
use crate::lastdb_uds::uds_router::method_not_allowed_response;
use crate::log_sync::{self, CloudState};

const NO_ACCOUNT: &str = "this node has no cloud account";
const ENROLLING: &str = "the database is still enrolling with the cloud; retry";
pub(super) const RETRY_AFTER_SECS: u64 = 2;

pub(super) enum Route {
    Link,
}

impl Route {
    pub(super) fn parse(path: &str) -> Option<Self> {
        match path {
            "share/link" => Some(Self::Link),
            _ => None,
        }
    }
}

pub(super) async fn execute(
    route: Route,
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
    selector: &str,
) -> UdsResponse {
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
    }
}

fn link(req: &UdsRequest, ctx: &AccessContext, host: &Host, entry: &owned::Entry) -> UdsResponse {
    if !req.body.is_empty() {
        return error_response(400, "invalid sharing request", ctx);
    }
    if let Err(error) = ensure_credentials(host, &entry.id) {
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

enum ShareError {
    PaymentRequired,
    Unconfirmed,
    NoAccount,
    Enrolling,
}

fn ensure_credentials(host: &Host, database_id: &str) -> Result<(), ShareError> {
    if payment_blocked(host, database_id) {
        return Err(ShareError::PaymentRequired);
    }
    match log_sync::wait_for_enrollment(host, database_id) {
        CloudState::Enrolled => Ok(()),
        CloudState::NoAccount => Err(ShareError::NoAccount),
        CloudState::Enrolling if payment_blocked(host, database_id) => {
            Err(ShareError::PaymentRequired)
        }
        CloudState::Enrolling => Err(ShareError::Enrolling),
        CloudState::Unverified => Err(ShareError::Unconfirmed),
    }
}

fn payment_blocked(host: &Host, database_id: &str) -> bool {
    host.cloud_sync.snapshot().payment_hold
        && log_sync::database_cloud_state(&host.home, database_id) == CloudState::Enrolling
}

fn link_error(
    host: &Host,
    database_id: &str,
    error: &crate::share_link::IssueError,
    ctx: &AccessContext,
) -> UdsResponse {
    let (status, message) = error.status();
    if error.is_cloud_connection() {
        let failure = cloud_failure(host, database_id, message);
        return render(Err(host_error(failure)), ctx);
    }
    error_response(status, message, ctx)
}

fn cloud_failure(host: &Host, database_id: &str, error: &str) -> ShareError {
    if error == crate::cloud::PAYMENT_REQUIRED_MESSAGE {
        return ShareError::PaymentRequired;
    }
    match log_sync::database_cloud_state(&host.home, database_id) {
        CloudState::NoAccount => ShareError::NoAccount,
        CloudState::Enrolling => ShareError::Enrolling,
        CloudState::Unverified | CloudState::Enrolled => ShareError::Unconfirmed,
    }
}

fn host_error(error: ShareError) -> HostError {
    let retry = matches!(error, ShareError::Enrolling).then_some(RETRY_AFTER_SECS);
    let (status, code, body) = match error {
        ShareError::PaymentRequired => (
            402,
            "payment_required".to_owned(),
            json!({
                "reason": crate::cloud::PAYMENT_REQUIRED_MESSAGE,
                "message": crate::cloud::PAYMENT_REQUIRED_MESSAGE,
            }),
        ),
        ShareError::NoAccount => (
            409,
            "no_cloud_account".to_owned(),
            json!({ "message": NO_ACCOUNT }),
        ),
        ShareError::Enrolling => (
            503,
            "database_enrolling".to_owned(),
            json!({ "message": ENROLLING }),
        ),
        ShareError::Unconfirmed => (
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
