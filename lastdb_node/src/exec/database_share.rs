//! Publisher-only sharing configuration, member grant/revoke, and invitation routes.
use super::*;
use crate::host::owned;
use crate::lastdb_uds::uds_router::method_not_allowed_response;
use crate::named_grant::{self, ConfigError, ConfigUpdate, MemberIntent};
use crate::share_invitation::ShareContact;
use cloud_backup_protocol::v2::limits::CONTROL_JSON_MAX_BYTES;
use cloud_backup_protocol::Digest;

pub(super) enum Route {
    Config,
    Members,
    Invitation,
}

impl Route {
    pub(super) fn parse(path: &str) -> Option<Self> {
        match path {
            "share/config" => Some(Self::Config),
            "share/members" => Some(Self::Members),
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
        (Route::Config, "GET") => named_grant::read_config(&host.home, &entry.id)
            .await
            .and_then(|config| serde_json::to_value(config).map_err(|_| ConfigError::Unconfirmed)),
        (Route::Config, "PUT") => set(req, host, &entry).await,
        (Route::Members, "POST") => return members(req, ctx, host, &entry.id).await,
        (Route::Invitation, "POST") => invite(req, host, &entry.id).await,
        (Route::Config, _) => return method_not_allowed_response("GET, PUT"),
        (Route::Members | Route::Invitation, _) => return method_not_allowed_response("POST"),
    };
    render(result.map_err(host_error), ctx)
}

async fn set(req: &UdsRequest, host: &Host, entry: &owned::Entry) -> Result<Value, ConfigError> {
    let update: ConfigUpdate = decode(req, CONTROL_JSON_MAX_BYTES)?;
    crate::log_sync::validate_write_scopes(host, entry, &update)?;
    let id = &entry.id;
    let request_id = update.request_id.clone();
    let revision = named_grant::set_config(&host.home, id, update).await?;
    Ok(json!({
        "databaseId": id,
        "requestId": request_id,
        "revision": revision,
        "committed": true,
    }))
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
        let ack = named_grant::update_member(&host.home, id, &b.recipient_key, intent)
            .await
            .map_err(|e| {
                if e == crate::cloud::PAYMENT_REQUIRED_MESSAGE {
                    host_error(ConfigError::PaymentRequired)
                } else {
                    HostError::new(400, e)
                }
            })?;
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

async fn invite(req: &UdsRequest, host: &Host, id: &str) -> Result<Value, ConfigError> {
    let invitation: Invitation = decode(req, 4096)?;
    let sealed = named_grant::issue_invitation(
        &host.home,
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

fn host_error(error: ConfigError) -> HostError {
    let (status, code, body) = match error {
        ConfigError::Invalid(reason) => (
            400,
            "invalid_share_configuration",
            json!({ "reason": reason }),
        ),
        ConfigError::Conflict(revision) => (
            409,
            "share_revision_conflict",
            json!({ "revision": revision }),
        ),
        ConfigError::PaymentRequired => (
            402,
            "payment_required",
            json!({
                "reason": crate::cloud::PAYMENT_REQUIRED_MESSAGE,
                "message": crate::cloud::PAYMENT_REQUIRED_MESSAGE,
            }),
        ),
        ConfigError::Unconfirmed => (503, "share_configuration_unconfirmed", json!({})),
    };
    let mut body = body;
    body["ok"] = json!(false);
    body["error"] = json!(code);
    HostError::structured(status, code, body)
}
