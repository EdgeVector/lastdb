//! Wire shape for deserializing `SyncHealth`.

use super::*;

#[derive(Deserialize)]
pub(super) struct SyncHealthRaw {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    snapshot_unavailable_reason: Option<String>,
    #[serde(default)]
    local_writable: Option<bool>,
    #[serde(default)]
    sync_degraded: Option<bool>,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    last_success_ts: Option<u64>,
    #[serde(default)]
    pending_count: Option<u64>,
    #[serde(default)]
    durable_outbox_count: Option<u64>,
    #[serde(default)]
    upload_queue_count: Option<u64>,
    #[serde(default)]
    upload_queue_max: Option<u64>,
    #[serde(default)]
    durable_outbox_max: Option<u64>,
    #[serde(default)]
    last_error: Option<String>,
    #[serde(default)]
    last_error_at: Option<u64>,
    #[serde(default)]
    consecutive_sync_failures: Option<u64>,
    #[serde(default)]
    failing_since: Option<u64>,
    #[serde(default)]
    degraded_reasons: Option<Vec<String>>,
    #[serde(default)]
    replay_blocker: Option<Value>,
    #[serde(default)]
    last_download: Option<Value>,
    #[serde(default)]
    upload_policy: Option<Value>,
    #[serde(default)]
    backup_upload_concurrency: Option<Value>,
    #[serde(default)]
    recording_local_changes: Option<bool>,
    #[serde(default)]
    sync_off_grace_expired: Option<bool>,
    #[serde(default)]
    reenable_strategy: Option<String>,
    #[serde(default)]
    cloud_sync_disabled_at: Option<u64>,
    #[serde(default)]
    sync_off_grace_secs: Option<u64>,
    #[serde(default)]
    mutation_log_active: Option<bool>,
    #[serde(default)]
    mutation_log_writer_id: Option<String>,
    #[serde(default)]
    mutation_log_frontier_f: Option<u64>,
    #[serde(default)]
    mutation_log_published_through: Option<u64>,
    #[serde(default)]
    mutation_log_last_durable_frontier: Option<u64>,
    #[serde(default)]
    mutation_log_recovery_point_age_secs: Option<u64>,
    #[serde(default)]
    mutation_log_lag: Option<u64>,
    #[serde(default)]
    mutation_log_segments_uploaded: Option<u64>,
    #[serde(default)]
    mutation_log_lag_degraded: Option<bool>,
    #[serde(default)]
    mutation_log_capture_registered: Option<bool>,
    #[serde(default)]
    capture_reexport_pending_count_estimate: Option<u64>,
    #[serde(default)]
    capture_reexport_pending_known_nonempty: Option<bool>,
    #[serde(default)]
    mutation_log_peer_segments_applied: Option<u64>,
    #[serde(default)]
    mutation_log_peer_records_applied: Option<u64>,
    #[serde(default)]
    mutation_log_records_quarantined: Option<u64>,
    #[serde(default)]
    mutation_log_last_quarantine_reason: Option<String>,
    #[serde(default)]
    capture: Option<Value>,
}

impl From<SyncHealthRaw> for SyncHealth {
    fn from(raw: SyncHealthRaw) -> Self {
        Self {
            enabled: raw.enabled,
            snapshot_unavailable_reason: raw.snapshot_unavailable_reason,
            local_writable: raw.local_writable,
            sync_degraded: raw.sync_degraded,
            state: raw.state,
            last_success_ts: g_instant_wire(raw.last_success_ts, Unit::Named("unix_s")),
            pending_count: g_instant_wire(raw.pending_count, Unit::Named("entry(ies)")),
            durable_outbox_count: g_instant_wire(
                raw.durable_outbox_count,
                Unit::Named("entry(ies)"),
            ),
            upload_queue_count: g_instant_wire(raw.upload_queue_count, Unit::Named("entry(ies)")),
            upload_queue_max: g_instant_wire(raw.upload_queue_max, Unit::Named("entry(ies)")),
            durable_outbox_max: g_instant_wire(raw.durable_outbox_max, Unit::Named("entry(ies)")),
            last_error: raw.last_error,
            last_error_at: g_instant_wire(raw.last_error_at, Unit::Named("unix_s")),
            consecutive_sync_failures: g_lifetime_wire(raw.consecutive_sync_failures, Unit::Events),
            failing_since: g_instant_wire(raw.failing_since, Unit::Named("unix_s")),
            degraded_reasons: raw.degraded_reasons,
            replay_blocker: raw.replay_blocker,
            last_download: raw.last_download,
            upload_policy: raw.upload_policy,
            backup_upload_concurrency: raw.backup_upload_concurrency,
            recording_local_changes: raw.recording_local_changes,
            sync_off_grace_expired: raw.sync_off_grace_expired,
            reenable_strategy: raw.reenable_strategy,
            cloud_sync_disabled_at: g_instant_wire(
                raw.cloud_sync_disabled_at,
                Unit::Named("unix_s"),
            ),
            sync_off_grace_secs: g_instant_wire(raw.sync_off_grace_secs, Unit::Named("sec(s)")),
            mutation_log_active: raw.mutation_log_active,
            mutation_log_writer_id: raw.mutation_log_writer_id,
            mutation_log_frontier_f: g_instant_wire(
                raw.mutation_log_frontier_f,
                Unit::Named("seq"),
            ),
            mutation_log_published_through: g_instant_wire(
                raw.mutation_log_published_through,
                Unit::Named("seq"),
            ),
            mutation_log_last_durable_frontier: g_instant_wire(
                raw.mutation_log_last_durable_frontier,
                Unit::Named("seq"),
            ),
            mutation_log_recovery_point_age_secs: g_instant_wire(
                raw.mutation_log_recovery_point_age_secs,
                Unit::Named("sec(s)"),
            ),
            mutation_log_lag: g_instant_wire(raw.mutation_log_lag, Unit::Named("seq")),
            mutation_log_segments_uploaded: g_lifetime_wire(
                raw.mutation_log_segments_uploaded,
                Unit::Named("segment(s)"),
            ),
            mutation_log_lag_degraded: raw.mutation_log_lag_degraded,
            mutation_log_capture_registered: raw.mutation_log_capture_registered,
            capture_reexport_pending_count_estimate: g_instant_wire(
                raw.capture_reexport_pending_count_estimate,
                Unit::Named("entry(ies)"),
            ),
            capture_reexport_pending_known_nonempty: raw.capture_reexport_pending_known_nonempty,
            mutation_log_peer_segments_applied: raw.mutation_log_peer_segments_applied,
            mutation_log_peer_records_applied: raw.mutation_log_peer_records_applied,
            mutation_log_records_quarantined: raw.mutation_log_records_quarantined,
            mutation_log_last_quarantine_reason: raw.mutation_log_last_quarantine_reason,
            capture: raw.capture,
        }
    }
}
