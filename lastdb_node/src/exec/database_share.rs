//! Publisher-only sharing configuration, member grant/revoke, and invitation routes.
//! Decided state: docs/designs/cloud-api.md (signed history; grants in-chain).
use super::*;
use crate::host::owned;
use crate::lastdb_uds::uds_router::method_not_allowed_response;
use crate::named_grant::{self, ConfigError, MemberIntent};
use crate::share_invitation::ShareContact;
use cloud_backup_protocol::Digest;

const NO_ACCOUNT: &str = "this node has no cloud account";
const ENROLLING: &str = "the database is still enrolling with the cloud; retry";
pub(super) const RETRY_AFTER_SECS: u64 = 2;

pub(super) enum Route {
    Members,
    Invitation,
    Link,
}

impl Route {
    pub(super) fn parse(path: &str) -> Option<Self> {
        match path {
            "share/members" => Some(Self::Members),
            "share/link" => Some(Self::Link),
            "invitations" => Some(Self::Invitation),
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
    let result = match (route, req.method.as_str()) {
        (Route::Members, "POST") => return members(req, ctx, host, &entry.id).await,
        (Route::Invitation, "POST") => invite(req, host, &entry.id).await,
        (Route::Link, "POST") => return link(req, ctx, host, &entry).await,
        (Route::Members | Route::Invitation | Route::Link, _) => {
            return method_not_allowed_response("POST")
        }
    };
    render(result.map_err(host_error), ctx)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MemberRequest {
    action: String,
    contact_code: Option<String>,
    recipient_key: Digest,
    #[serde(default)]
    regrant: bool,
    expires_in_hours: Option<u32>,
}

async fn members(req: &UdsRequest, ctx: &AccessContext, host: &Host, id: &str) -> UdsResponse {
    let res: Result<Value, HostError> = async {
        if req.body.len() > 4096 {
            return Err(HostError::new(
                400,
                "sharing request exceeds its size limit",
            ));
        }
        let b: MemberRequest = serde_json::from_slice(&req.body)
            .map_err(|_| HostError::new(400, "invalid sharing request"))?;
        let (intent, seal_target) = match b.action.as_str() {
            "read" | "submit" => {
                let code = b
                    .contact_code
                    .as_deref()
                    .ok_or_else(|| HostError::new(400, "contactCode is required"))?;
                let contact = ShareContact::parse(code.trim())
                    .map_err(|e| HostError::new(400, e.to_string()))?;
                if &b.recipient_key != contact.identity_public_key() {
                    return Err(HostError::new(
                        400,
                        "contact does not match the trusted recipient",
                    ));
                }
                let intent = if b.action == "read" {
                    MemberIntent::Read { regrant: b.regrant }
                } else {
                    MemberIntent::Submit { regrant: b.regrant }
                };
                (intent, Some((contact, b.expires_in_hours.unwrap_or(24))))
            }
            "revoke" => (MemberIntent::Revoke, None),
            _ => return Err(HostError::new(400, "invalid member action")),
        };
        ensure_enrolled(host, id).map_err(host_error)?;
        let ack = named_grant::update_member(&host.home, id, &b.recipient_key, intent)
            .await
            .map_err(|error| member_cloud_error(host, id, error))?;
        let mut out = json!({
            "database_id": ack.database_id,
            "revision": ack.revision.to_string(),
            "unchanged": ack.unchanged,
        });
        if let Some((contact, hours)) = seal_target {
            let sealed =
                named_grant::seal_named_invitation(&host.home, &ack.database_id, &contact, hours)
                    .await
                    .map_err(|error| {
                        if error == crate::cloud::PAYMENT_REQUIRED_MESSAGE {
                            host_error(ConfigError::PaymentRequired)
                        } else {
                            HostError::new(
                                500,
                                format!(
                                    "member list committed at revision {}; {error}",
                                    ack.revision
                                ),
                            )
                        }
                    })?;
            out["sealed"] = json!(base64::engine::general_purpose::STANDARD.encode(sealed));
        }
        Ok(out)
    }
    .await;
    render(res, ctx)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Invitation {
    contact_code: String,
    expected_node_key: Digest,
    expires_in_hours: Option<u32>,
}

async fn link(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
    entry: &owned::Entry,
) -> UdsResponse {
    if !req.body.is_empty() {
        return error_response(400, "invalid sharing request", ctx);
    }
    if let Err(error) = ensure_enrolled(host, &entry.id) {
        return render(Err(host_error(error)), ctx);
    }
    match crate::share_link::issue(host, entry).await {
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

async fn invite(req: &UdsRequest, host: &Host, id: &str) -> Result<Value, ConfigError> {
    let invitation: Invitation = decode(req, 4096)?;
    ensure_enrolled(host, id)?;
    let sealed = named_grant::issue_invitation(
        host,
        id,
        &invitation.contact_code,
        &invitation.expected_node_key,
        invitation.expires_in_hours.unwrap_or(24),
    )
    .await?;
    Ok(json!({
        "databaseId": id,
        "invitation": base64::engine::general_purpose::STANDARD.encode(sealed),
    }))
}

fn decode<T: serde::de::DeserializeOwned>(req: &UdsRequest, limit: u64) -> Result<T, ConfigError> {
    if req.body.len() as u64 > limit {
        return Err(ConfigError::Invalid(
            "sharing request exceeds its size limit".into(),
        ));
    }
    serde_json::from_slice(&req.body)
        .map_err(|_| ConfigError::Invalid("invalid sharing request".into()))
}

fn ensure_enrolled(host: &Host, database_id: &str) -> Result<(), ConfigError> {
    if payment_blocked(host, database_id) {
        return Err(ConfigError::PaymentRequired);
    }
    match crate::log_sync::wait_for_enrollment(host, database_id) {
        crate::log_sync::CloudState::Enrolled => Ok(()),
        crate::log_sync::CloudState::NoAccount => Err(ConfigError::NoAccount),
        crate::log_sync::CloudState::Enrolling if payment_blocked(host, database_id) => {
            Err(ConfigError::PaymentRequired)
        }
        crate::log_sync::CloudState::Enrolling => Err(ConfigError::Enrolling),
        crate::log_sync::CloudState::Unverified => Err(ConfigError::Unconfirmed),
    }
}

fn payment_blocked(host: &Host, database_id: &str) -> bool {
    host.cloud_sync.snapshot().payment_hold
        && crate::log_sync::database_cloud_state(&host.home, database_id)
            == crate::log_sync::CloudState::Enrolling
}

fn member_cloud_error(host: &Host, database_id: &str, error: String) -> HostError {
    if error == crate::cloud::PAYMENT_REQUIRED_MESSAGE {
        return host_error(ConfigError::PaymentRequired);
    }
    match crate::log_sync::database_cloud_state(&host.home, database_id) {
        crate::log_sync::CloudState::NoAccount => host_error(ConfigError::NoAccount),
        crate::log_sync::CloudState::Enrolling => host_error(ConfigError::Enrolling),
        crate::log_sync::CloudState::Unverified => host_error(ConfigError::Unconfirmed),
        crate::log_sync::CloudState::Enrolled => HostError::new(400, error),
    }
}

fn link_error(
    host: &Host,
    database_id: &str,
    error: &crate::share_link::IssueError,
    ctx: &AccessContext,
) -> UdsResponse {
    let (status, message) = error.status();
    if error.is_cloud_connection() {
        let failure = named_grant::cloud_failure(&host.home, database_id, message);
        return render(Err(host_error(failure)), ctx);
    }
    error_response(status, message, ctx)
}

fn host_error(error: ConfigError) -> HostError {
    let retry = matches!(error, ConfigError::Enrolling).then_some(RETRY_AFTER_SECS);
    let (status, code, body) = match error {
        ConfigError::Invalid(reason) => (
            400,
            "invalid_share_configuration".to_owned(),
            json!({ "reason": reason }),
        ),
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
