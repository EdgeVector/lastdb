use super::*;

/// Pause the local legacy credential file. Explicit V2 jobs remain independent.
pub(super) async fn execute_sync_cloud_off_route(ctx: &AccessContext, host: &Host) -> UdsResponse {
    let file_renamed = match crate::cloud::pause_cloud_sync_file(&host.home) {
        Ok(renamed) => renamed,
        Err(error) => {
            return error_response(500, &format!("credential pause failed: {error}"), ctx)
        }
    };
    // Keep the existing asynchronous owner-handler signature.
    std::future::ready(json_ok(&envelope(
        &serde_json::json!({
            "ok": true,
            "intent": "off",
            "file_state": crate::cloud::cloud_sync_file_state(&host.home),
            "file_renamed": file_renamed,
            "explicit_backup_cancelled": false,
            "note": "Local credentials stay paused. Explicit V2 backups use backup-v2 close.",
        }),
        ctx.user_id.as_str(),
    )))
    .await
}
