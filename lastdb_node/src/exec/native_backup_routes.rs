//! Explicit owner backup requests. No legacy cloud engine activation occurs.

use super::*;
use crate::native_cloud_backup::{self, AutoRequest, NativeBackupRequest};

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum OwnerRequest {
    Manual(NativeBackupRequest),
    Automatic(AutoRequest),
}

pub(super) async fn execute(req: &UdsRequest, ctx: &AccessContext, host: &Host) -> UdsResponse {
    if !ctx.is_owner {
        return error_response(403, "native backup is owner-only", ctx);
    }
    if req.body.len() as u64 > cloud_backup_protocol::limits::REQUEST_JSON_MAX_BYTES {
        return error_response(413, "native backup request exceeds its byte limit", ctx);
    }
    let request: OwnerRequest = match serde_json::from_slice(&req.body) {
        Ok(request) => request,
        Err(_) => return error_response(400, "invalid native backup request", ctx),
    };
    match request {
        OwnerRequest::Manual(request) => match native_cloud_backup::request(host, request).await {
            Ok(status) => json_ok(&envelope(&status, ctx.user_id.as_str())),
            Err(error) => error_response(409, &error.to_string(), ctx),
        },
        OwnerRequest::Automatic(request) => {
            match native_cloud_backup::request_auto(host, request).await {
                Ok(status) => json_ok(&envelope(&status, ctx.user_id.as_str())),
                Err(error) => error_response(409, &error.to_string(), ctx),
            }
        }
    }
}
