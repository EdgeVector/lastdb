use super::*;

/// Cloud Sync operator vitals. Numeric counts/timestamps/lag are wire-transparent
/// gauges (bare u64 / null); bools/strings/JSON blobs stay bare.
#[derive(Debug, Clone)]
pub struct SyncHealth {
    pub enabled: bool,
    /// Why the sync section is unavailable. `None` means the section came from
    /// a complete engine snapshot or sync is not configured.
    pub snapshot_unavailable_reason: Option<String>,
    pub local_writable: Option<bool>,
    pub sync_degraded: Option<bool>,
    pub state: Option<String>,
    pub last_success_ts: Gauge,
    pub pending_count: Gauge,
    pub durable_outbox_count: Gauge,
    pub upload_queue_count: Gauge,
    pub upload_queue_max: Gauge,
    pub durable_outbox_max: Gauge,
    pub last_error: Option<String>,
    /// Unix timestamp (seconds) when `last_error` was recorded. Read with
    /// `last_error`: an undated message hides how stale it is.
    pub last_error_at: Gauge,
    /// Consecutive failed sync cycles (0 when the last cycle succeeded).
    pub consecutive_sync_failures: Gauge,
    /// Unix timestamp (seconds) of the first failure in the current streak.
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
    pub cloud_sync_disabled_at: Gauge,
    /// Configured temporary-off grace window in seconds (default 3600).
    pub sync_off_grace_secs: Gauge,
    /// Continuous mutation-log plane active (CaptureMode::MutationLog).
    pub mutation_log_active: Option<bool>,
    /// Writer/device id for the local continuous mutation-log stream.
    pub mutation_log_writer_id: Option<String>,
    /// Published frontier F for continuous mutation-log upload.
    pub mutation_log_frontier_f: Gauge,
    /// Cloud-confirmed published frontier. NOT an alias for F: `frontier_f` is
    /// the max across writers after the plane-vector merge, which includes
    /// frontiers sealed locally by other writers and never cloud-confirmed.
    /// `frontier_f - published_through` is the publish gap.
    pub mutation_log_published_through: Gauge,
    /// Highest frontier group-committed to the durable local mutation log.
    /// Emitted so `mutation_log_lag` is verifiable from this document.
    pub mutation_log_last_durable_frontier: Gauge,
    /// Seconds since the oldest writer's cloud-confirmed recovery point.
    pub mutation_log_recovery_point_age_secs: Gauge,
    /// Log lag: durable frontier minus published F.
    pub mutation_log_lag: Gauge,
    /// Segments sealed+uploaded this process under continuous log plane.
    pub mutation_log_segments_uploaded: Gauge,
    /// True when log lag is at/above the configured degraded threshold.
    pub mutation_log_lag_degraded: Option<bool>,
    /// Whether the active mutation-log plane has a registered capture runtime.
    pub mutation_log_capture_registered: Option<bool>,
    /// A process estimate. Zero alone does not prove an empty marker plane.
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

impl Serialize for SyncHealth {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("SyncHealth", 41)?;
        s.serialize_field("enabled", &self.enabled)?;
        s.serialize_field(
            "snapshot_unavailable_reason",
            &self.snapshot_unavailable_reason,
        )?;
        s.serialize_field("local_writable", &self.local_writable)?;
        s.serialize_field("sync_degraded", &self.sync_degraded)?;
        s.serialize_field("state", &self.state)?;
        s.serialize_field("last_success_ts", &wire_u64(&self.last_success_ts))?;
        s.serialize_field("pending_count", &wire_u64(&self.pending_count))?;
        s.serialize_field(
            "durable_outbox_count",
            &wire_u64(&self.durable_outbox_count),
        )?;
        s.serialize_field("upload_queue_count", &wire_u64(&self.upload_queue_count))?;
        s.serialize_field("upload_queue_max", &wire_u64(&self.upload_queue_max))?;
        s.serialize_field("durable_outbox_max", &wire_u64(&self.durable_outbox_max))?;
        s.serialize_field("last_error", &self.last_error)?;
        s.serialize_field("last_error_at", &wire_u64(&self.last_error_at))?;
        s.serialize_field(
            "consecutive_sync_failures",
            &wire_u64(&self.consecutive_sync_failures),
        )?;
        s.serialize_field("failing_since", &wire_u64(&self.failing_since))?;
        s.serialize_field("degraded_reasons", &self.degraded_reasons)?;
        s.serialize_field("replay_blocker", &self.replay_blocker)?;
        s.serialize_field("last_download", &self.last_download)?;
        s.serialize_field("upload_policy", &self.upload_policy)?;
        s.serialize_field("backup_upload_concurrency", &self.backup_upload_concurrency)?;
        s.serialize_field("recording_local_changes", &self.recording_local_changes)?;
        s.serialize_field("sync_off_grace_expired", &self.sync_off_grace_expired)?;
        s.serialize_field("reenable_strategy", &self.reenable_strategy)?;
        s.serialize_field(
            "cloud_sync_disabled_at",
            &wire_u64(&self.cloud_sync_disabled_at),
        )?;
        s.serialize_field("sync_off_grace_secs", &wire_u64(&self.sync_off_grace_secs))?;
        s.serialize_field("mutation_log_active", &self.mutation_log_active)?;
        s.serialize_field("mutation_log_writer_id", &self.mutation_log_writer_id)?;
        s.serialize_field(
            "mutation_log_frontier_f",
            &wire_u64(&self.mutation_log_frontier_f),
        )?;
        s.serialize_field(
            "mutation_log_published_through",
            &wire_u64(&self.mutation_log_published_through),
        )?;
        s.serialize_field(
            "mutation_log_last_durable_frontier",
            &wire_u64(&self.mutation_log_last_durable_frontier),
        )?;
        s.serialize_field(
            "mutation_log_recovery_point_age_secs",
            &wire_u64(&self.mutation_log_recovery_point_age_secs),
        )?;
        s.serialize_field("mutation_log_lag", &wire_u64(&self.mutation_log_lag))?;
        s.serialize_field(
            "mutation_log_segments_uploaded",
            &wire_u64(&self.mutation_log_segments_uploaded),
        )?;
        s.serialize_field("mutation_log_lag_degraded", &self.mutation_log_lag_degraded)?;
        s.serialize_field(
            "mutation_log_capture_registered",
            &self.mutation_log_capture_registered,
        )?;
        s.serialize_field(
            "capture_reexport_pending_count_estimate",
            &wire_u64(&self.capture_reexport_pending_count_estimate),
        )?;
        s.serialize_field(
            "capture_reexport_pending_known_nonempty",
            &self.capture_reexport_pending_known_nonempty,
        )?;
        s.serialize_field(
            "mutation_log_peer_segments_applied",
            &self.mutation_log_peer_segments_applied,
        )?;
        s.serialize_field(
            "mutation_log_peer_records_applied",
            &self.mutation_log_peer_records_applied,
        )?;
        s.serialize_field(
            "mutation_log_records_quarantined",
            &self.mutation_log_records_quarantined,
        )?;
        s.serialize_field(
            "mutation_log_last_quarantine_reason",
            &self.mutation_log_last_quarantine_reason,
        )?;
        s.serialize_field("capture", &self.capture)?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for SyncHealth {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // lint:fn-size-ok moved verbatim from self_metrics.rs; splitting this function is separate work.
        #[derive(Deserialize)]
        struct Raw {
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
        let raw = Raw::deserialize(deserializer)?;
        Ok(Self {
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
        })
    }
}
