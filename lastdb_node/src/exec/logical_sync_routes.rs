//! Owner snapshot actions on the logical sync route.

use super::*;
use crate::byte_cloud_backup::LocalSnapshotAction;
use serde_json::Value;

const CONTROL_BYTES: usize = 8192;
const HTTP_BYTES: usize = 1024 * 1024 + CONTROL_BYTES;

#[derive(Clone, Copy, Deserialize)]
enum Action {
    #[serde(rename = "snapshot_prepare")]
    Prepare,
    #[serde(rename = "snapshot_inspect")]
    Inspect,
    #[serde(rename = "snapshot_resume")]
    Resume,
    #[serde(rename = "snapshot_release")]
    Release,
}

impl From<Action> for LocalSnapshotAction {
    fn from(action: Action) -> Self {
        match action {
            Action::Prepare => Self::Prepare,
            Action::Inspect => Self::Inspect,
            Action::Resume => Self::Resume,
            Action::Release => Self::Release,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireRequest {
    action: Action,
    request_id: cloud_backup_protocol::Id,
}

pub(super) async fn execute(req: &UdsRequest, ctx: &AccessContext, host: &Host) -> UdsResponse {
    if !ctx.is_owner {
        return error_response(403, "logical sync is owner-only", ctx);
    }
    if req.body.len() > CONTROL_BYTES {
        return error_response(413, "logical sync control exceeds its byte limit", ctx);
    }
    let Ok(request) = serde_json::from_slice::<WireRequest>(&req.body) else {
        return error_response(400, "invalid logical sync request", ctx);
    };
    match perform(host, request).await {
        Ok(value) => {
            let response = json_ok(&envelope(&value, ctx.user_id.as_str()));
            if response.body.len() > HTTP_BYTES {
                return error_response(413, "logical sync response exceeds its byte limit", ctx);
            }
            response
        }
        Err(error) => error_response(409, &error, ctx),
    }
}

async fn perform(host: &Host, request: WireRequest) -> Result<Value, String> {
    let report =
        crate::byte_cloud_backup::local_snapshot(host, request.action.into(), request.request_id)
            .await
            .map_err(|error| error.to_string())?;
    serde_json::to_value(report).map_err(|_| "cannot encode logical sync response".into())
}
