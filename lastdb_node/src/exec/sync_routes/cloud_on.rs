//! Owner Cloud On checks the account generation before runtime activation.

use super::*;

/// `POST /api/sync/cloud-on` refuses retired repair generations.
pub(in crate::exec) async fn execute_sync_cloud_on_route(
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    if let Err(error) = host.db.require_cloud_sync_supported() {
        return error_response(409, &error, ctx);
    }
    if crate::cloud::cloud_sync_file_state(&host.home) == "unset" {
        return error_response(400, "cloud sync is not configured", ctx);
    }
    if !ctx.is_owner {
        return error_response(403, "Cloud On is owner-only", ctx);
    }
    if let Err(error) = crate::cloud_activation::require_legacy_generation(host).await {
        return error_response(409, &error, ctx);
    }
    if host.db.sync_engine().is_none()
        || crate::cloud::cloud_resume_required_path(&host.home).exists()
    {
        return old_control_pending_restart(ctx, host);
    }
    let file_restored = match crate::cloud::resume_cloud_sync_file(&host.home) {
        Ok(restored) => restored,
        Err(e) => {
            return error_response(400, &format!("cloud-on durable resume failed: {e}"), ctx);
        }
    };
    let (reenable, engine_note) = match host.db.reenable_cloud_sync_live().await {
        Ok(outcome) => {
            let note = format!(
                "live reenable strategy={} recording={}",
                outcome.strategy, outcome.now_recording
            );
            let value = serde_json::to_value(&outcome).unwrap_or(serde_json::Value::Null);
            (value, note)
        }
        Err(e) => (
            serde_json::Value::Null,
            format!(
                "file restored but live engine not running ({e}) — restart lastdbd to boot sync"
            ),
        ),
    };
    let status = host.db.sync_status().await;
    json_ok(&envelope(
        &serde_json::json!({
            "ok": true,
            "intent": "on",
            "file_state": crate::cloud::cloud_sync_file_state(&host.home),
            "file_restored": file_restored,
            "resume_pending": false,
            "engine_note": engine_note,
            "reenable": reenable,
            "recording_local_changes": status.as_ref().map(|s| s.recording_local_changes),
            "note": "This daemon resumed its live sync engine. Check its sync status before you depend on cloud sync.",
        }),
        ctx.user_id.as_str(),
    ))
}

/// Old account control: marker, then rename the pause file. Workers stay stopped.
fn old_control_pending_restart(ctx: &AccessContext, host: &Host) -> UdsResponse {
    let file_restored = match crate::cloud::prepare_primary_resume_file(&host.home) {
        Ok(restored) => restored,
        Err(error) => {
            return error_response(400, &format!("cloud-on resume file failed: {error}"), ctx);
        }
    };
    json_ok(&envelope(
        &serde_json::json!({
            "ok": true,
            "intent": "on",
            "state": "pending_restart",
            "file_state": crate::cloud::cloud_sync_file_state(&host.home),
            "file_restored": file_restored,
            "resume_required": crate::cloud::cloud_resume_required_path(&host.home).exists(),
            "workers_started": false,
            "note": "The resume file is active. This daemon does not start workers.",
        }),
        ctx.user_id.as_str(),
    ))
}
