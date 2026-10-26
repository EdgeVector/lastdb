//! Passive legacy cloud fields.
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

pub(super) fn sync_snapshot_complete(sync: &SyncHealth) -> bool {
    sync.snapshot_unavailable_reason.is_none()
}

pub(super) fn sync_health() -> SyncHealth {
    SyncHealth {
        enabled: false,
        local_writable: Some(true),
        sync_degraded: Some(false),
        ..SyncHealth::default()
    }
}
