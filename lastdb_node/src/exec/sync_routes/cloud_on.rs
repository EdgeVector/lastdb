//! Owner Cloud On requires an exact generation receipt before runtime activation.

use super::*;

/// `POST /api/sync/cloud-on` — restore durable intent + re-enable live engine.
pub(in crate::exec) async fn execute_sync_cloud_on_route(
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    if crate::cloud::cloud_sync_file_state(&host.home) == "unset" {
        return error_response(400, "cloud sync is not configured", ctx);
    }
    if !ctx.is_owner {
        return error_response(403, "Cloud On is owner-only", ctx);
    }
    if host.db.sync_engine().is_none()
        || crate::cloud::cloud_resume_required_path(&host.home).exists()
    {
        match crate::cloud_activation::activate_adopted_generation(host).await {
            Ok(Some(report)) => return json_ok(&envelope(&report, ctx.user_id.as_str())),
            Ok(None) => {}
            Err(error) => return error_response(409, &error, ctx),
        }
    }
    if crate::cloud::cloud_resume_required_path(&host.home).exists() {
        return error_response(
            409,
            "Cloud Sync remains Off: the paused-home backup does not reconcile older peer writes with local Deletes or update existing peers",
            ctx,
        );
    }
    let resume_pending = host.db.sync_engine().is_none();
    if resume_pending {
        if let Err(error) = crate::cloud::mark_cloud_resume_required(&host.home) {
            return error_response(500, &format!("cloud-on resume marker failed: {error}"), ctx);
        }
        return error_response(
            409,
            "Cloud Sync remains Off: this daemon has no sync engine. The paused-home backup does not reconcile peer writes or update existing peers.",
            ctx,
        );
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
            "resume_pending": resume_pending,
            "engine_note": engine_note,
            "reenable": reenable,
            "recording_local_changes": status.as_ref().map(|s| s.recording_local_changes),
            "note": "This daemon resumed its live sync engine. Check its sync status before you depend on cloud sync.",
        }),
        ctx.user_id.as_str(),
    ))
}
