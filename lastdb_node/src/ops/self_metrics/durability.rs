use super::*;

/// Age of the last committed backup manifest, from the durable on-disk marker.
///
/// Engine-independent by design: `BackupProgressHealth` and every
/// `SyncHealth` failure counter go `null` when the uploader is switched off, so
/// a monitor keyed on them cannot distinguish "backed up minutes ago" from
/// "nothing has left this machine in nine days". This can.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurabilityHealth {
    /// Highest committed backup manifest counter, when the marker was readable.
    pub backup_manifest_counter: Gauge,
    /// Unix seconds of the last committed manifest, when known.
    pub last_backup_commit_ts: Gauge,
    /// Age of that commit in seconds, when known.
    pub backup_age_secs: Gauge,
    /// Threshold in force (`LASTDB_BACKUP_MAX_AGE_SECS`); `0` disables the age
    /// check.
    pub max_age_secs: Gauge,
    /// Whether durability needs attention. True for stale, never, and unknown.
    pub degraded: bool,
    /// Machine-readable reason codes, empty when healthy.
    pub reasons: Vec<String>,
}

impl Default for DurabilityHealth {
    /// Un-evaluated durability is **degraded**, not healthy.
    ///
    /// This is the `#[serde(default)]` value for a snapshot written before the
    /// field existed, and the value a caller gets by forgetting to evaluate.
    /// Either way we cannot show the data is covered, and a durability signal
    /// whose default reads green is the exact failure being fixed here.
    fn default() -> Self {
        Self {
            backup_manifest_counter: Gauge::field_not_served(
                Unit::Named("manifest"),
                GAUGE_INSTANT,
            ),
            last_backup_commit_ts: Gauge::field_not_served(Unit::Named("unix_s"), GAUGE_INSTANT),
            backup_age_secs: Gauge::field_not_served(Unit::Named("sec(s)"), GAUGE_INSTANT),
            max_age_secs: g_instant(
                fold_db::backup_durability::DEFAULT_MAX_BACKUP_AGE_SECS,
                Unit::Named("sec(s)"),
            ),
            degraded: true,
            reasons: vec![fold_db::backup_durability::reason::NO_MARKER.to_string()],
        }
    }
}

impl From<fold_db::backup_durability::BackupDurabilityHealth> for DurabilityHealth {
    fn from(h: fold_db::backup_durability::BackupDurabilityHealth) -> Self {
        Self {
            backup_manifest_counter: g_instant_wire(
                h.backup_manifest_counter,
                Unit::Named("manifest"),
            ),
            last_backup_commit_ts: g_instant_wire(h.last_backup_commit_ts, Unit::Named("unix_s")),
            backup_age_secs: g_instant_wire(h.age_secs, Unit::Named("sec(s)")),
            max_age_secs: g_instant(h.max_age_secs, Unit::Named("sec(s)")),
            degraded: h.degraded,
            reasons: h.reasons,
        }
    }
}

impl Serialize for DurabilityHealth {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("DurabilityHealth", 6)?;
        s.serialize_field(
            "backup_manifest_counter",
            &wire_u64(&self.backup_manifest_counter),
        )?;
        s.serialize_field(
            "last_backup_commit_ts",
            &wire_u64(&self.last_backup_commit_ts),
        )?;
        s.serialize_field("backup_age_secs", &wire_u64(&self.backup_age_secs))?;
        s.serialize_field("max_age_secs", &wire_u64(&self.max_age_secs))?;
        s.serialize_field("degraded", &self.degraded)?;
        s.serialize_field("reasons", &self.reasons)?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for DurabilityHealth {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Raw {
            #[serde(default)]
            backup_manifest_counter: Option<u64>,
            #[serde(default)]
            last_backup_commit_ts: Option<u64>,
            #[serde(default)]
            backup_age_secs: Option<u64>,
            #[serde(default)]
            max_age_secs: Option<u64>,
            #[serde(default)]
            degraded: Option<bool>,
            #[serde(default)]
            reasons: Option<Vec<String>>,
        }
        let raw = Raw::deserialize(deserializer)?;
        // Missing degraded/reasons on older wire → treat as unevaluated degraded.
        let degraded = raw.degraded.unwrap_or(true);
        let reasons = raw.reasons.unwrap_or_else(|| {
            if degraded {
                vec![fold_db::backup_durability::reason::NO_MARKER.to_string()]
            } else {
                Vec::new()
            }
        });
        Ok(Self {
            backup_manifest_counter: g_instant_wire(
                raw.backup_manifest_counter,
                Unit::Named("manifest"),
            ),
            last_backup_commit_ts: g_instant_wire(raw.last_backup_commit_ts, Unit::Named("unix_s")),
            backup_age_secs: g_instant_wire(raw.backup_age_secs, Unit::Named("sec(s)")),
            max_age_secs: match raw.max_age_secs {
                Some(n) => g_instant(n, Unit::Named("sec(s)")),
                None => g_instant(
                    fold_db::backup_durability::DEFAULT_MAX_BACKUP_AGE_SECS,
                    Unit::Named("sec(s)"),
                ),
            },
            degraded,
            reasons,
        })
    }
}

/// Operator-facing cloud backup storage honesty (referenced vs billed).
///
/// Wire-transparent Instant byte/chunk gauges.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackupStorageHealth {
    /// Cloud object bytes for digests in the live tip keep-set.
    pub referenced_bytes: Gauge,
    /// Total listed `backup/chunks/` object bytes (billable inventory).
    pub billed_bytes: Gauge,
    /// `billed − referenced` when keep-set known; 0 when fail-closed.
    pub reclaimable_bytes: Gauge,
    pub referenced_chunks: Gauge,
    pub billed_chunks: Gauge,
    pub reclaimable_chunks: Gauge,
}

impl Default for BackupStorageHealth {
    fn default() -> Self {
        Self {
            referenced_bytes: Gauge::field_not_served(Unit::Bytes, GAUGE_INSTANT),
            billed_bytes: Gauge::field_not_served(Unit::Bytes, GAUGE_INSTANT),
            reclaimable_bytes: Gauge::field_not_served(Unit::Bytes, GAUGE_INSTANT),
            referenced_chunks: Gauge::field_not_served(Unit::Chunks, GAUGE_INSTANT),
            billed_chunks: Gauge::field_not_served(Unit::Chunks, GAUGE_INSTANT),
            reclaimable_chunks: Gauge::field_not_served(Unit::Chunks, GAUGE_INSTANT),
        }
    }
}

impl From<fold_db::storage::laststore::BackupStorageFootprint> for BackupStorageHealth {
    fn from(fp: fold_db::storage::laststore::BackupStorageFootprint) -> Self {
        Self {
            referenced_bytes: g_instant(fp.referenced_bytes, Unit::Bytes),
            billed_bytes: g_instant(fp.billed_bytes, Unit::Bytes),
            reclaimable_bytes: g_instant(fp.reclaimable_bytes, Unit::Bytes),
            referenced_chunks: g_instant(fp.referenced_chunks, Unit::Chunks),
            billed_chunks: g_instant(fp.billed_chunks, Unit::Chunks),
            reclaimable_chunks: g_instant(fp.reclaimable_chunks, Unit::Chunks),
        }
    }
}

impl Serialize for BackupStorageHealth {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("BackupStorageHealth", 6)?;
        s.serialize_field("referenced_bytes", &wire_u64(&self.referenced_bytes))?;
        s.serialize_field("billed_bytes", &wire_u64(&self.billed_bytes))?;
        s.serialize_field("reclaimable_bytes", &wire_u64(&self.reclaimable_bytes))?;
        s.serialize_field("referenced_chunks", &wire_u64(&self.referenced_chunks))?;
        s.serialize_field("billed_chunks", &wire_u64(&self.billed_chunks))?;
        s.serialize_field("reclaimable_chunks", &wire_u64(&self.reclaimable_chunks))?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for BackupStorageHealth {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Raw {
            #[serde(default)]
            referenced_bytes: Option<u64>,
            #[serde(default)]
            billed_bytes: Option<u64>,
            #[serde(default)]
            reclaimable_bytes: Option<u64>,
            #[serde(default)]
            referenced_chunks: Option<u64>,
            #[serde(default)]
            billed_chunks: Option<u64>,
            #[serde(default)]
            reclaimable_chunks: Option<u64>,
        }
        let raw = Raw::deserialize(deserializer)?;
        Ok(Self {
            referenced_bytes: g_instant_wire(raw.referenced_bytes, Unit::Bytes),
            billed_bytes: g_instant_wire(raw.billed_bytes, Unit::Bytes),
            reclaimable_bytes: g_instant_wire(raw.reclaimable_bytes, Unit::Bytes),
            referenced_chunks: g_instant_wire(raw.referenced_chunks, Unit::Chunks),
            billed_chunks: g_instant_wire(raw.billed_chunks, Unit::Chunks),
            reclaimable_chunks: g_instant_wire(raw.reclaimable_chunks, Unit::Chunks),
        })
    }
}

/// Operator-facing sealed-chunk backup progress (from continuous publisher).
///
/// Chunk/byte/cycle counters are wire-transparent gauges (bare u64 / null).
/// f64 EWMA fields and bools stay bare.
#[derive(Debug, Clone, PartialEq)]
pub struct BackupProgressHealth {
    pub enabled: bool,
    pub complete: bool,
    pub show_progress: bool,
    pub percent: Option<f64>,
    pub chunks_total: Gauge,
    pub chunks_present: Gauge,
    pub chunks_remaining: Gauge,
    pub bytes_remaining: Gauge,
    pub elapsed_secs: Gauge,
    pub eta_secs: Gauge,
    pub ewma_upload_bps: Option<f64>,
    /// True link rate (bytes per second of transfer). `eta_secs` charges
    /// remaining bytes at this; `ewma_upload_bps` is per whole cycle and is
    /// dominated by the walk, so the two differ by an order of magnitude on a
    /// large home.
    pub ewma_link_bps: Option<f64>,
    /// Per-cycle walk overhead in seconds, charged once per remaining cycle.
    pub ewma_overhead_secs: Option<f64>,
    pub last_cycle_uploaded: Gauge,
    pub last_cycle_already_present: Gauge,
    pub last_cycle_bytes_uploaded: Gauge,
    /// Chunks whose presign/upload/confirm failed in the last drain cycle.
    pub last_cycle_failed: Gauge,
    /// Chunks of the current cut with no local sealed file, so the cut can
    /// never publish.
    pub chunks_source_missing: Gauge,
    /// Chunks NAMED BY THE CUT'S MANIFEST with no local candidate to upload
    /// them from. Disjoint from `chunks_source_missing` and equally terminal —
    /// see `fold_db::backup_progress::BackupProgressSnapshot`.
    pub chunks_unbackable_manifest: Gauge,
    pub cas_counter: Gauge,
    pub phase: String,
    /// Unix seconds of the last publish cycle that completed without error.
    pub last_success_unix: Gauge,
    /// Age of that success. Absent when none has happened.
    pub last_success_age_secs: Gauge,
    /// Consecutive failed publish cycles since the last success.
    pub consecutive_failures: Gauge,
    /// Redacted text of the most recent cycle failure.
    pub last_error: Option<String>,
    /// Chunks that entered the cloud-present set since the current cut started.
    pub chunks_gained: Gauge,
    /// Chunks that LEFT it — uploaded bytes invalidated by sealed-chunk reseal.
    pub chunks_erased: Gauge,
    /// Whether the current cut is net-progressing.
    pub net_progressing: bool,
    /// Chunks gained over the recent window that decides `net_progressing`.
    pub recent_gained: Gauge,
    /// Chunks erased over that same window.
    pub recent_erased: Gauge,
    /// How many cycles that window spans.
    pub net_progress_window_cycles: Gauge,
    /// Cycles since one last gained ground. Zero while gaining.
    pub cycles_since_net_gain: Gauge,
    /// Manifest counter of the cut currently being drained.
    pub target_generation: Gauge,
    /// Unix seconds of the last successful CAS — the last time this home
    /// actually became restorable.
    pub last_publish_unix: Gauge,
    /// Age of that publish. Absent when nothing has ever landed.
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

impl Serialize for BackupProgressHealth {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("BackupProgressHealth", 33)?;
        s.serialize_field("enabled", &self.enabled)?;
        s.serialize_field("complete", &self.complete)?;
        s.serialize_field("show_progress", &self.show_progress)?;
        s.serialize_field("percent", &self.percent)?;
        s.serialize_field("chunks_total", &wire_u64(&self.chunks_total))?;
        s.serialize_field("chunks_present", &wire_u64(&self.chunks_present))?;
        s.serialize_field("chunks_remaining", &wire_u64(&self.chunks_remaining))?;
        s.serialize_field("bytes_remaining", &wire_u64(&self.bytes_remaining))?;
        s.serialize_field("elapsed_secs", &wire_u64(&self.elapsed_secs))?;
        s.serialize_field("eta_secs", &wire_u64(&self.eta_secs))?;
        s.serialize_field("ewma_upload_bps", &self.ewma_upload_bps)?;
        s.serialize_field("ewma_link_bps", &self.ewma_link_bps)?;
        s.serialize_field("ewma_overhead_secs", &self.ewma_overhead_secs)?;
        s.serialize_field("last_cycle_uploaded", &wire_u64(&self.last_cycle_uploaded))?;
        s.serialize_field(
            "last_cycle_already_present",
            &wire_u64(&self.last_cycle_already_present),
        )?;
        s.serialize_field(
            "last_cycle_bytes_uploaded",
            &wire_u64(&self.last_cycle_bytes_uploaded),
        )?;
        s.serialize_field("last_cycle_failed", &wire_u64(&self.last_cycle_failed))?;
        s.serialize_field(
            "chunks_source_missing",
            &wire_u64(&self.chunks_source_missing),
        )?;
        s.serialize_field(
            "chunks_unbackable_manifest",
            &wire_u64(&self.chunks_unbackable_manifest),
        )?;
        s.serialize_field("cas_counter", &wire_u64(&self.cas_counter))?;
        s.serialize_field("phase", &self.phase)?;
        s.serialize_field("last_success_unix", &wire_u64(&self.last_success_unix))?;
        s.serialize_field(
            "last_success_age_secs",
            &wire_u64(&self.last_success_age_secs),
        )?;
        s.serialize_field(
            "consecutive_failures",
            &wire_u64(&self.consecutive_failures),
        )?;
        s.serialize_field("last_error", &self.last_error)?;
        s.serialize_field("chunks_gained", &wire_u64(&self.chunks_gained))?;
        s.serialize_field("chunks_erased", &wire_u64(&self.chunks_erased))?;
        s.serialize_field("net_progressing", &self.net_progressing)?;
        s.serialize_field("recent_gained", &wire_u64(&self.recent_gained))?;
        s.serialize_field("recent_erased", &wire_u64(&self.recent_erased))?;
        s.serialize_field(
            "net_progress_window_cycles",
            &wire_u64(&self.net_progress_window_cycles),
        )?;
        s.serialize_field(
            "cycles_since_net_gain",
            &wire_u64(&self.cycles_since_net_gain),
        )?;
        s.serialize_field("target_generation", &wire_u64(&self.target_generation))?;
        s.serialize_field("last_publish_unix", &wire_u64(&self.last_publish_unix))?;
        s.serialize_field(
            "last_publish_age_secs",
            &wire_u64(&self.last_publish_age_secs),
        )?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for BackupProgressHealth {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // lint:fn-size-ok moved verbatim from self_metrics.rs; splitting this function is separate work.
        #[derive(Deserialize)]
        struct Raw {
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
        let raw = Raw::deserialize(deserializer)?;
        Ok(Self {
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
        })
    }
}

// lint:file-size-ok moved verbatim from self_metrics.rs; cohesive unit, split further in a later pass
