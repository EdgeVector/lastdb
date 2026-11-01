//! Owner snapshot actions on the logical sync route.

use super::*;
use fold_db::storage::disk_pack::byte_backup::SnapshotAdoptionCandidate;
use serde_json::Value;

const CONTROL_BYTES: usize = 8192;
const PLAN_BYTES: usize = 32 * 1024 + CONTROL_BYTES;
const HTTP_BYTES: usize = 1024 * 1024 + CONTROL_BYTES;

enum Request {
    Snapshot {
        action: crate::byte_cloud_backup::LocalSnapshotAction,
        request_id: cloud_backup_protocol::Id,
    },
    SnapshotPlan {
        request_id: cloud_backup_protocol::Id,
        candidate: Box<SnapshotAdoptionCandidate>,
    },
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Action {
    SnapshotPrepare,
    SnapshotInspect,
    SnapshotResume,
    SnapshotRelease,
    SnapshotPlan,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireRequest {
    action: Action,
    request_id: Option<cloud_backup_protocol::Id>,
    candidate: Option<Box<SnapshotAdoptionCandidate>>,
}

impl TryFrom<WireRequest> for Request {
    type Error = ();

    fn try_from(wire: WireRequest) -> Result<Self, Self::Error> {
        match (wire.action, wire.request_id, wire.candidate) {
            (action, Some(request_id), None) => {
                use crate::byte_cloud_backup::LocalSnapshotAction;
                let action = match action {
                    Action::SnapshotPrepare => LocalSnapshotAction::Prepare,
                    Action::SnapshotInspect => LocalSnapshotAction::Inspect,
                    Action::SnapshotResume => LocalSnapshotAction::Resume,
                    Action::SnapshotRelease => LocalSnapshotAction::Release,
                    Action::SnapshotPlan => return Err(()),
                };
                Ok(Self::Snapshot { action, request_id })
            }
            (Action::SnapshotPlan, Some(request_id), Some(candidate)) => Ok(Self::SnapshotPlan {
                request_id,
                candidate,
            }),
            _ => Err(()),
        }
    }
}

pub(super) async fn execute(req: &UdsRequest, ctx: &AccessContext, host: &Host) -> UdsResponse {
    if !ctx.is_owner {
        return error_response(403, "logical sync is owner-only", ctx);
    }
    if req.body.len() > HTTP_BYTES {
        return error_response(413, "logical sync request exceeds its byte limit", ctx);
    }
    let request = match serde_json::from_slice::<WireRequest>(&req.body) {
        Ok(value) => match Request::try_from(value) {
            Ok(request) => request,
            Err(()) => return error_response(400, "invalid logical sync fields", ctx),
        },
        Err(_) => return error_response(400, "invalid logical sync request", ctx),
    };
    let control_bytes = if matches!(&request, Request::SnapshotPlan { .. }) {
        PLAN_BYTES
    } else {
        CONTROL_BYTES
    };
    if req.body.len() > control_bytes {
        return error_response(413, "logical sync control exceeds its byte limit", ctx);
    }
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

async fn perform(host: &Host, request: Request) -> Result<Value, String> {
    match request {
        Request::Snapshot { action, request_id } => encoded(
            crate::byte_cloud_backup::local_snapshot(host, action, request_id)
                .await
                .map_err(|error| error.to_string())?,
        ),
        Request::SnapshotPlan {
            request_id,
            candidate,
        } => encoded(
            crate::byte_cloud_backup::local_snapshot_plan(host, request_id, *candidate)
                .await
                .map_err(|error| error.to_string())?,
        ),
    }
}

fn encoded(value: impl Serialize) -> Result<Value, String> {
    serde_json::to_value(value).map_err(|_| "cannot encode logical sync response".into())
}
