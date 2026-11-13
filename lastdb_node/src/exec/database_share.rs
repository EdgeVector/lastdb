//! Publisher-only sharing configuration and invitation routes.
use super::*;
use crate::host::owned;
use crate::lastdb_uds::uds_router::method_not_allowed_response;
use crate::named_grant::{self, ConfigError, ConfigUpdate};
use cloud_backup_protocol::v2::limits::CONTROL_JSON_MAX_BYTES;
use cloud_backup_protocol::Digest;

pub(super) enum Route {
    Config,
    Invitation,
}

impl Route {
    pub(super) fn parse(path: &str) -> Option<Self> {
        match path {
            "share/config" => Some(Self::Config),
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
        (Route::Config, "PUT") => set(req, host, &entry.id).await,
        (Route::Invitation, "POST") => invite(req, host, &entry.id).await,
        (Route::Config, _) => return method_not_allowed_response("GET, PUT"),
        (Route::Invitation, _) => return method_not_allowed_response("POST"),
    };
    render(result.map_err(host_error), ctx)
}

async fn set(req: &UdsRequest, host: &Host, id: &str) -> Result<Value, ConfigError> {
    let update: ConfigUpdate = decode(req, CONTROL_JSON_MAX_BYTES)?;
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
        ConfigError::Pending => (409, "share_update_pending", json!({})),
        ConfigError::Unconfirmed => (503, "share_configuration_unconfirmed", json!({})),
    };
    let mut body = body;
    body["ok"] = json!(false);
    body["error"] = json!(code);
    HostError::structured(status, code, body)
}
