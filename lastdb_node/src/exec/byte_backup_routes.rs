//! Owner-only admission for explicit V2 work. Legacy sync remains separate.
use super::*;
use crate::byte_cloud_backup::{self, ByteBackupRequest};

pub(super) async fn execute(req: &UdsRequest, ctx: &AccessContext, host: &Host) -> UdsResponse {
    if !ctx.is_owner {
        return error_response(403, "byte backup is owner-only", ctx);
    }
    if req.body.len() as u64 > cloud_backup_protocol::v2::limits::CONTROL_JSON_MAX_BYTES {
        return error_response(413, "byte backup request exceeds its byte limit", ctx);
    }
    let request: ByteBackupRequest = match serde_json::from_slice(&req.body) {
        Ok(value) => value,
        Err(_) => return error_response(400, "invalid byte backup request", ctx),
    };
    match byte_cloud_backup::request(host, request).await {
        Ok(status) => json_ok(&envelope(&status, ctx.user_id.as_str())),
        Err(error) => error_response(409, &error.to_string(), ctx),
    }
}
