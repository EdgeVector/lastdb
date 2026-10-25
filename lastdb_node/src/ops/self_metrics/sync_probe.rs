//! Passive legacy cloud fields plus factual local compaction status.
//! `local_writable` reports only that cloud policy does not block local writes.
//! It does not measure native writer, persist, maintenance or disk health.
use super::*;

pub(super) fn cached_sync_health(host: &Host) -> SyncHealth {
    if let Some((mut health, age)) = host.self_metrics.cached_sync_health() {
        health.enabled = false;
        let max_age = sample_interval_from_env()
            .checked_mul(2)
            .unwrap_or(Duration::MAX);
        if age > max_age {
            health.snapshot_unavailable_reason = Some("sync_snapshot_stale".to_string());
        }
        return health;
    }
    SyncHealth {
        enabled: false,
        local_writable: Some(true),
        sync_degraded: Some(false),
        ..SyncHealth::default()
    }
}

/// Local compaction remains observable without a cloud engine.
pub(super) async fn local_only_capture_status(host: &Host) -> Option<Value> {
    let planes = host.db.automatic_compaction_status().await?;
    if planes.is_empty() {
        return None;
    }
    serde_json::to_value(planes)
        .ok()
        .map(|planes| json!({ "automatic_compactions": planes }))
}

pub(super) fn sync_snapshot_complete(sync: &SyncHealth) -> bool {
    sync.snapshot_unavailable_reason.is_none()
}

pub(super) async fn sync_health(host: &Host) -> SyncHealth {
    SyncHealth {
        enabled: false,
        local_writable: Some(true),
        sync_degraded: Some(false),
        capture: local_only_capture_status(host).await,
        ..SyncHealth::default()
    }
}
