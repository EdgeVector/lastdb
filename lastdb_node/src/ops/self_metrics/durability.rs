use super::*;

/// Redacted V2 job observation. A receipt is not a proof of current coverage.
/// Standalone coordinators remain separate and cannot be inferred from a missing local job.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DurabilityHealth {
    pub observed: bool,
    pub automatic_enabled: bool,
    pub running: bool,
    pub phase: Option<crate::byte_cloud_backup::Phase>,
    pub accepted_backup_recorded: bool,
    pub degraded: Option<bool>,
    pub reasons: Vec<String>,
}

impl Default for DurabilityHealth {
    fn default() -> Self {
        Self {
            observed: false,
            automatic_enabled: false,
            running: false,
            phase: None,
            accepted_backup_recorded: false,
            degraded: None,
            reasons: vec!["v2_status_unavailable".into()],
        }
    }
}

impl From<crate::byte_cloud_backup::ByteBackupStatus> for DurabilityHealth {
    fn from(status: crate::byte_cloud_backup::ByteBackupStatus) -> Self {
        let blocked = status.blocked_reason.is_some();
        Self {
            observed: true,
            automatic_enabled: false,
            running: status.running,
            phase: status.phase,
            accepted_backup_recorded: status.last_accepted_backup.is_some(),
            // No capture time or current-source equivalence is available here.
            // Never turn an accepted historical receipt into a fresh-coverage claim.
            degraded: blocked.then_some(true),
            reasons: vec![if blocked {
                "v2_job_blocked".into()
            } else {
                "v2_freshness_unmeasured".into()
            }],
        }
    }
}

impl DurabilityHealth {
    pub(super) fn status_line(&self) -> String {
        let state = if !self.observed {
            "status unavailable; use cloud backup-v2 status"
        } else if self.degraded == Some(true) {
            "job blocked; use cloud backup-v2 status"
        } else if self.running {
            "job active; use cloud backup-v2 status"
        } else if self.accepted_backup_recorded {
            "accepted backup recorded; current coverage is not measured"
        } else {
            "no receipt in this node; check any external coordinator separately"
        };
        format!("Manual V2 backup: {state}")
    }
}

gauge_health! {
    /// Operator-facing cloud backup storage honesty (referenced vs billed).
    ///
    /// Wire-transparent Instant byte/chunk gauges.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct BackupStorageHealth {
        /// Cloud object bytes for digests in the live tip keep-set.
        pub referenced_bytes: (Unit::Bytes, GAUGE_INSTANT),
        /// Total listed `backup/chunks/` object bytes (billable inventory).
        pub billed_bytes: (Unit::Bytes, GAUGE_INSTANT),
        /// `billed − referenced` when keep-set known; 0 when fail-closed.
        pub reclaimable_bytes: (Unit::Bytes, GAUGE_INSTANT),
        pub referenced_chunks: (Unit::Chunks, GAUGE_INSTANT),
        pub billed_chunks: (Unit::Chunks, GAUGE_INSTANT),
        pub reclaimable_chunks: (Unit::Chunks, GAUGE_INSTANT),
    }
}

/// Operator-facing sealed-chunk backup progress (from continuous publisher).
///
/// Chunk/byte/cycle counters are wire-transparent gauges (bare u64 / null).
/// f64 EWMA fields and bools stay bare.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct BackupProgressHealth {
    pub enabled: bool,
    pub complete: bool,
    pub show_progress: bool,
    pub percent: Option<f64>,
    #[serde(serialize_with = "serialize_gauge_option")]
    pub chunks_total: Gauge,
    #[serde(serialize_with = "serialize_gauge_option")]
    pub chunks_present: Gauge,
    #[serde(serialize_with = "serialize_gauge_option")]
    pub chunks_remaining: Gauge,
    #[serde(serialize_with = "serialize_gauge_option")]
    pub bytes_remaining: Gauge,
    #[serde(serialize_with = "serialize_gauge_option")]
    pub elapsed_secs: Gauge,
    #[serde(serialize_with = "serialize_gauge_option")]
    pub eta_secs: Gauge,
    pub ewma_upload_bps: Option<f64>,
    /// True link rate (bytes per second of transfer). `eta_secs` charges
    /// remaining bytes at this; `ewma_upload_bps` is per whole cycle and is
    /// dominated by the walk, so the two differ by an order of magnitude on a
    /// large home.
    pub ewma_link_bps: Option<f64>,
    /// Per-cycle walk overhead in seconds, charged once per remaining cycle.
    pub ewma_overhead_secs: Option<f64>,
    #[serde(serialize_with = "serialize_gauge_option")]
    pub last_cycle_uploaded: Gauge,
    #[serde(serialize_with = "serialize_gauge_option")]
    pub last_cycle_already_present: Gauge,
    #[serde(serialize_with = "serialize_gauge_option")]
    pub last_cycle_bytes_uploaded: Gauge,
    /// Chunks whose presign/upload/confirm failed in the last drain cycle.
    #[serde(serialize_with = "serialize_gauge_option")]
    pub last_cycle_failed: Gauge,
    /// Chunks of the current cut with no local sealed file, so the cut can
    /// never publish.
    #[serde(serialize_with = "serialize_gauge_option")]
    pub chunks_source_missing: Gauge,
    /// Chunks NAMED BY THE CUT'S MANIFEST with no local candidate to upload
    /// them from. Disjoint from `chunks_source_missing` and equally terminal —
    /// see `fold_db::backup_progress::BackupProgressSnapshot`.
    #[serde(serialize_with = "serialize_gauge_option")]
    pub chunks_unbackable_manifest: Gauge,
    #[serde(serialize_with = "serialize_gauge_option")]
    pub cas_counter: Gauge,
    pub phase: String,
    /// Unix seconds of the last publish cycle that completed without error.
    #[serde(serialize_with = "serialize_gauge_option")]
    pub last_success_unix: Gauge,
    /// Age of that success. Absent when none has happened.
    #[serde(serialize_with = "serialize_gauge_option")]
    pub last_success_age_secs: Gauge,
    /// Consecutive failed publish cycles since the last success.
    #[serde(serialize_with = "serialize_gauge_option")]
    pub consecutive_failures: Gauge,
    /// Redacted text of the most recent cycle failure.
    pub last_error: Option<String>,
    /// Chunks that entered the cloud-present set since the current cut started.
    #[serde(serialize_with = "serialize_gauge_option")]
    pub chunks_gained: Gauge,
    /// Chunks that LEFT it — uploaded bytes invalidated by sealed-chunk reseal.
    #[serde(serialize_with = "serialize_gauge_option")]
    pub chunks_erased: Gauge,
    /// Whether the current cut is net-progressing.
    pub net_progressing: bool,
    /// Chunks gained over the recent window that decides `net_progressing`.
    #[serde(serialize_with = "serialize_gauge_option")]
    pub recent_gained: Gauge,
    /// Chunks erased over that same window.
    #[serde(serialize_with = "serialize_gauge_option")]
    pub recent_erased: Gauge,
    /// How many cycles that window spans.
    #[serde(serialize_with = "serialize_gauge_option")]
    pub net_progress_window_cycles: Gauge,
    /// Cycles since one last gained ground. Zero while gaining.
    #[serde(serialize_with = "serialize_gauge_option")]
    pub cycles_since_net_gain: Gauge,
    /// Manifest counter of the cut currently being drained.
    #[serde(serialize_with = "serialize_gauge_option")]
    pub target_generation: Gauge,
    /// Unix seconds of the last successful CAS — the last time this home
    /// actually became restorable.
    #[serde(serialize_with = "serialize_gauge_option")]
    pub last_publish_unix: Gauge,
    /// Age of that publish. Absent when nothing has ever landed.
    #[serde(serialize_with = "serialize_gauge_option")]
    pub last_publish_age_secs: Gauge,
}

impl From<fold_db::backup_progress::BackupProgressSnapshot> for BackupProgressHealth {
    fn from(s: fold_db::backup_progress::BackupProgressSnapshot) -> Self {
        Self {
            enabled: s.enabled,
            complete: s.complete,
            show_progress: s.show_progress,
            percent: s.percent,
            chunks_total: g_instant(s.chunks_total, Unit::Chunks),
            chunks_present: g_instant(s.chunks_present, Unit::Chunks),
            chunks_remaining: g_instant(s.chunks_remaining, Unit::Chunks),
            bytes_remaining: g_instant_wire(s.bytes_remaining, Unit::Bytes),
            elapsed_secs: g_instant_wire(s.elapsed_secs, Unit::Named("sec(s)")),
            eta_secs: g_instant_wire(s.eta_secs, Unit::Named("sec(s)")),
            ewma_upload_bps: s.ewma_upload_bps,
            ewma_link_bps: s.ewma_link_bps,
            ewma_overhead_secs: s.ewma_overhead_secs,
            last_cycle_uploaded: g_instant(s.last_cycle_uploaded, Unit::Chunks),
            last_cycle_already_present: g_instant(s.last_cycle_already_present, Unit::Chunks),
            last_cycle_bytes_uploaded: g_instant(s.last_cycle_bytes_uploaded, Unit::Bytes),
            last_cycle_failed: g_instant(s.last_cycle_failed, Unit::Chunks),
            chunks_source_missing: g_instant(s.chunks_source_missing, Unit::Chunks),
            chunks_unbackable_manifest: g_instant(s.chunks_unbackable_manifest, Unit::Chunks),
            cas_counter: g_instant_wire(s.cas_counter, Unit::Named("manifest")),
            phase: s.phase,
            last_success_unix: g_instant_wire(s.last_success_unix, Unit::Named("unix_s")),
            last_success_age_secs: g_instant_wire(s.last_success_age_secs, Unit::Named("sec(s)")),
            consecutive_failures: g_lifetime(s.consecutive_failures as u64, Unit::Events),
            last_error: s.last_error,
            chunks_gained: g_instant(s.chunks_gained, Unit::Chunks),
            chunks_erased: g_instant(s.chunks_erased, Unit::Chunks),
            net_progressing: s.net_progressing,
            recent_gained: g_instant(s.recent_gained, Unit::Chunks),
            recent_erased: g_instant(s.recent_erased, Unit::Chunks),
            net_progress_window_cycles: g_instant(
                s.net_progress_window_cycles,
                Unit::Named("cycle(s)"),
            ),
            cycles_since_net_gain: g_instant(s.cycles_since_net_gain, Unit::Named("cycle(s)")),
            target_generation: g_instant_wire(s.target_generation, Unit::Named("manifest")),
            last_publish_unix: g_instant_wire(s.last_publish_unix, Unit::Named("unix_s")),
            last_publish_age_secs: g_instant_wire(s.last_publish_age_secs, Unit::Named("sec(s)")),
        }
    }
}

#[derive(Deserialize)]
struct BackupProgressHealthRaw {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    complete: bool,
    #[serde(default)]
    show_progress: bool,
    #[serde(default)]
    percent: Option<f64>,
    #[serde(default)]
    chunks_total: Option<u64>,
    #[serde(default)]
    chunks_present: Option<u64>,
    #[serde(default)]
    chunks_remaining: Option<u64>,
    #[serde(default)]
    bytes_remaining: Option<u64>,
    #[serde(default)]
    elapsed_secs: Option<u64>,
    #[serde(default)]
    eta_secs: Option<u64>,
    #[serde(default)]
    ewma_upload_bps: Option<f64>,
    #[serde(default)]
    ewma_link_bps: Option<f64>,
    #[serde(default)]
    ewma_overhead_secs: Option<f64>,
    #[serde(default)]
    last_cycle_uploaded: Option<u64>,
    #[serde(default)]
    last_cycle_already_present: Option<u64>,
    #[serde(default)]
    last_cycle_bytes_uploaded: Option<u64>,
    #[serde(default)]
    last_cycle_failed: Option<u64>,
    #[serde(default)]
    chunks_source_missing: Option<u64>,
    #[serde(default)]
    chunks_unbackable_manifest: Option<u64>,
    #[serde(default)]
    cas_counter: Option<u64>,
    #[serde(default)]
    phase: Option<String>,
    #[serde(default)]
    last_success_unix: Option<u64>,
    #[serde(default)]
    last_success_age_secs: Option<u64>,
    #[serde(default)]
    consecutive_failures: Option<u64>,
    #[serde(default)]
    last_error: Option<String>,
    #[serde(default)]
    chunks_gained: Option<u64>,
    #[serde(default)]
    chunks_erased: Option<u64>,
    #[serde(default)]
    net_progressing: bool,
    #[serde(default)]
    recent_gained: Option<u64>,
    #[serde(default)]
    recent_erased: Option<u64>,
    #[serde(default)]
    net_progress_window_cycles: Option<u64>,
    #[serde(default)]
    cycles_since_net_gain: Option<u64>,
    #[serde(default)]
    target_generation: Option<u64>,
    #[serde(default)]
    last_publish_unix: Option<u64>,
    #[serde(default)]
    last_publish_age_secs: Option<u64>,
}

impl From<BackupProgressHealthRaw> for BackupProgressHealth {
    fn from(raw: BackupProgressHealthRaw) -> Self {
        Self {
            enabled: raw.enabled,
            complete: raw.complete,
            show_progress: raw.show_progress,
            percent: raw.percent,
            chunks_total: g_instant_wire(raw.chunks_total, Unit::Chunks),
            chunks_present: g_instant_wire(raw.chunks_present, Unit::Chunks),
            chunks_remaining: g_instant_wire(raw.chunks_remaining, Unit::Chunks),
            bytes_remaining: g_instant_wire(raw.bytes_remaining, Unit::Bytes),
            elapsed_secs: g_instant_wire(raw.elapsed_secs, Unit::Named("sec(s)")),
            eta_secs: g_instant_wire(raw.eta_secs, Unit::Named("sec(s)")),
            ewma_upload_bps: raw.ewma_upload_bps,
            ewma_link_bps: raw.ewma_link_bps,
            ewma_overhead_secs: raw.ewma_overhead_secs,
            last_cycle_uploaded: g_instant_wire(raw.last_cycle_uploaded, Unit::Chunks),
            last_cycle_already_present: g_instant_wire(
                raw.last_cycle_already_present,
                Unit::Chunks,
            ),
            last_cycle_bytes_uploaded: g_instant_wire(raw.last_cycle_bytes_uploaded, Unit::Bytes),
            last_cycle_failed: g_instant_wire(raw.last_cycle_failed, Unit::Chunks),
            chunks_source_missing: g_instant_wire(raw.chunks_source_missing, Unit::Chunks),
            chunks_unbackable_manifest: g_instant_wire(
                raw.chunks_unbackable_manifest,
                Unit::Chunks,
            ),
            cas_counter: g_instant_wire(raw.cas_counter, Unit::Named("manifest")),
            phase: raw.phase.unwrap_or_default(),
            last_success_unix: g_instant_wire(raw.last_success_unix, Unit::Named("unix_s")),
            last_success_age_secs: g_instant_wire(raw.last_success_age_secs, Unit::Named("sec(s)")),
            consecutive_failures: g_lifetime_wire(raw.consecutive_failures, Unit::Events),
            last_error: raw.last_error,
            chunks_gained: g_instant_wire(raw.chunks_gained, Unit::Chunks),
            chunks_erased: g_instant_wire(raw.chunks_erased, Unit::Chunks),
            net_progressing: raw.net_progressing,
            recent_gained: g_instant_wire(raw.recent_gained, Unit::Chunks),
            recent_erased: g_instant_wire(raw.recent_erased, Unit::Chunks),
            net_progress_window_cycles: g_instant_wire(
                raw.net_progress_window_cycles,
                Unit::Named("cycle(s)"),
            ),
            cycles_since_net_gain: g_instant_wire(
                raw.cycles_since_net_gain,
                Unit::Named("cycle(s)"),
            ),
            target_generation: g_instant_wire(raw.target_generation, Unit::Named("manifest")),
            last_publish_unix: g_instant_wire(raw.last_publish_unix, Unit::Named("unix_s")),
            last_publish_age_secs: g_instant_wire(raw.last_publish_age_secs, Unit::Named("sec(s)")),
        }
    }
}

impl<'de> Deserialize<'de> for BackupProgressHealth {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        BackupProgressHealthRaw::deserialize(deserializer).map(Self::from)
    }
}

// lint:file-size-ok moved verbatim from self_metrics.rs; cohesive unit, split further in a later pass
