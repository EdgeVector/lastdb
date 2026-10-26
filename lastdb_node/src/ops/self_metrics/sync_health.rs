use super::sync_health_wire::SyncHealthRaw;
use super::*;

/// Cloud Sync operator vitals. Numeric counts/timestamps/lag are wire-transparent
/// gauges (bare u64 / null); bools/strings/JSON blobs stay bare.
#[derive(Debug, Clone, Serialize)]
pub struct SyncHealth {
    pub enabled: bool,
    /// Why the sync section is unavailable. `None` means the section came from
    /// a complete engine snapshot or sync is not configured.
    pub snapshot_unavailable_reason: Option<String>,
    pub local_writable: Option<bool>,
    pub sync_degraded: Option<bool>,
    pub state: Option<String>,
    #[serde(serialize_with = "serialize_gauge_option")]
    pub last_success_ts: Gauge,
    #[serde(serialize_with = "serialize_gauge_option")]
    pub pending_count: Gauge,
    #[serde(serialize_with = "serialize_gauge_option")]
    pub durable_outbox_count: Gauge,
    #[serde(serialize_with = "serialize_gauge_option")]
    pub upload_queue_count: Gauge,
    #[serde(serialize_with = "serialize_gauge_option")]
    pub upload_queue_max: Gauge,
    #[serde(serialize_with = "serialize_gauge_option")]
    pub durable_outbox_max: Gauge,
    pub last_error: Option<String>,
    /// Unix timestamp (seconds) when `last_error` was recorded. Read with
    /// `last_error`: an undated message hides how stale it is.
    #[serde(serialize_with = "serialize_gauge_option")]
    pub last_error_at: Gauge,
    /// Consecutive failed sync cycles (0 when the last cycle succeeded).
    #[serde(serialize_with = "serialize_gauge_option")]
    pub consecutive_sync_failures: Gauge,
    /// Unix timestamp (seconds) of the first failure in the current streak.
    #[serde(serialize_with = "serialize_gauge_option")]
    pub failing_since: Gauge,
    /// Machine-readable reasons `sync_degraded` is set.
    pub degraded_reasons: Option<Vec<String>>,
    pub replay_blocker: Option<Value>,
    /// Last download-cycle observability (bytes, deferred backlog, caps).
    pub last_download: Option<Value>,
    /// Adaptive upload policy snapshot (budget_bytes, concurrency, mode).
    pub upload_policy: Option<Value>,
    /// Effective sealed-home backup PUT override and its precedence source.
    pub backup_upload_concurrency: Option<Value>,
    /// Whether local mutations are still staged for cloud while intentional
    /// sync-off is within `sync_off_grace_secs` (or sync is on).
    pub recording_local_changes: Option<bool>,
    /// True when intentional Cloud Sync off has exceeded the grace window.
    pub sync_off_grace_expired: Option<bool>,
    /// `incremental` | `snapshot_reconcile` while intentionally off; null when on.
    pub reenable_strategy: Option<String>,
    /// Unix timestamp (seconds) when Cloud Sync was intentionally disabled.
    #[serde(serialize_with = "serialize_gauge_option")]
    pub cloud_sync_disabled_at: Gauge,
    /// Configured temporary-off grace window in seconds (default 3600).
    #[serde(serialize_with = "serialize_gauge_option")]
    pub sync_off_grace_secs: Gauge,
    /// Retained continuous mutation-log plane activity flag.
    pub mutation_log_active: Option<bool>,
    /// Writer/device id for the local continuous mutation-log stream.
    pub mutation_log_writer_id: Option<String>,
    /// Published frontier F for continuous mutation-log upload.
    #[serde(serialize_with = "serialize_gauge_option")]
    pub mutation_log_frontier_f: Gauge,
    /// Cloud-confirmed published frontier. NOT an alias for F: `frontier_f` is
    /// the max across writers after the plane-vector merge, which includes
    /// frontiers sealed locally by other writers and never cloud-confirmed.
    /// `frontier_f - published_through` is the publish gap.
    #[serde(serialize_with = "serialize_gauge_option")]
    pub mutation_log_published_through: Gauge,
    /// Highest frontier group-committed to the durable local mutation log.
    /// Emitted so `mutation_log_lag` is verifiable from this document.
    #[serde(serialize_with = "serialize_gauge_option")]
    pub mutation_log_last_durable_frontier: Gauge,
    /// Seconds since the oldest writer's cloud-confirmed recovery point.
    #[serde(serialize_with = "serialize_gauge_option")]
    pub mutation_log_recovery_point_age_secs: Gauge,
    /// Log lag: durable frontier minus published F.
    #[serde(serialize_with = "serialize_gauge_option")]
    pub mutation_log_lag: Gauge,
    /// Segments sealed+uploaded this process under continuous log plane.
    #[serde(serialize_with = "serialize_gauge_option")]
    pub mutation_log_segments_uploaded: Gauge,
    /// True when log lag is at/above the configured degraded threshold.
    pub mutation_log_lag_degraded: Option<bool>,
    /// Whether the active mutation-log plane has a registered capture runtime.
    pub mutation_log_capture_registered: Option<bool>,
    /// A process estimate. Zero alone does not prove an empty marker plane.
    #[serde(serialize_with = "serialize_gauge_option")]
    pub capture_reexport_pending_count_estimate: Gauge,
    /// `null` means unverified after restart; false requires an empty physical lap.
    /// This does not prove historical Async writes reached cloud after a crash.
    pub capture_reexport_pending_known_nonempty: Option<bool>,
    /// MutationIntent rows skipped this process because they cannot be sealed.
    /// Peer writer-scoped mutation-log segments applied this process by
    /// the regular `do_sync` cycle. The download-side twin of
    /// `mutation_log_segments_uploaded`; restore does not book here.
    pub mutation_log_peer_segments_applied: Option<u64>,
    /// Records applied out of `mutation_log_peer_segments_applied`.
    pub mutation_log_peer_records_applied: Option<u64>,
    pub mutation_log_records_quarantined: Option<u64>,
    /// Last unsealable-record reason (includes the missing atom id).
    pub mutation_log_last_quarantine_reason: Option<String>,
    /// Scan-free capture-plane counters and directory-stat byte gauges.
    pub capture: Option<Value>,
}

impl Default for SyncHealth {
    fn default() -> Self {
        let un = |unit: Unit| Gauge::field_not_served(unit, GAUGE_INSTANT);
        let un_life = |unit: Unit| Gauge::field_not_served(unit, GAUGE_PROCESS_LIFETIME);
        Self {
            enabled: false,
            snapshot_unavailable_reason: None,
            local_writable: None,
            sync_degraded: None,
            state: None,
            last_success_ts: un(Unit::Named("unix_s")),
            pending_count: un(Unit::Named("entry(ies)")),
            durable_outbox_count: un(Unit::Named("entry(ies)")),
            upload_queue_count: un(Unit::Named("entry(ies)")),
            upload_queue_max: un(Unit::Named("entry(ies)")),
            durable_outbox_max: un(Unit::Named("entry(ies)")),
            last_error: None,
            last_error_at: un(Unit::Named("unix_s")),
            consecutive_sync_failures: un_life(Unit::Events),
            failing_since: un(Unit::Named("unix_s")),
            degraded_reasons: None,
            replay_blocker: None,
            last_download: None,
            upload_policy: None,
            backup_upload_concurrency: None,
            recording_local_changes: None,
            sync_off_grace_expired: None,
            reenable_strategy: None,
            cloud_sync_disabled_at: un(Unit::Named("unix_s")),
            sync_off_grace_secs: un(Unit::Named("sec(s)")),
            mutation_log_active: None,
            mutation_log_writer_id: None,
            mutation_log_frontier_f: un(Unit::Named("seq")),
            mutation_log_published_through: un(Unit::Named("seq")),
            mutation_log_last_durable_frontier: un(Unit::Named("seq")),
            mutation_log_recovery_point_age_secs: un(Unit::Named("sec(s)")),
            mutation_log_lag: un(Unit::Named("seq")),
            mutation_log_segments_uploaded: un_life(Unit::Named("segment(s)")),
            mutation_log_lag_degraded: None,
            mutation_log_capture_registered: None,
            capture_reexport_pending_count_estimate: un(Unit::Named("entry(ies)")),
            capture_reexport_pending_known_nonempty: None,
            mutation_log_peer_segments_applied: None,
            mutation_log_peer_records_applied: None,
            mutation_log_records_quarantined: None,
            mutation_log_last_quarantine_reason: None,
            capture: None,
        }
    }
}

impl<'de> Deserialize<'de> for SyncHealth {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        SyncHealthRaw::deserialize(deserializer).map(Self::from)
    }
}
