use serde::{Deserialize, Serialize};

/// Cheap, scan-free status for the local mutation capture planes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct CapturePlaneStatus {
    /// Live durable pin-log keys known by the registered target runtimes.
    pub pin_log_live_keys: u64,
    /// Allocated bytes under the `sync_pin_log` collection directory.
    pub pin_log_disk_bytes: Option<u64>,
    /// Trigger for the most recent successful pin-log compaction.
    pub pin_log_last_compact_trigger: Option<String>,
    /// Live capture envelope/op kind (`mutation_intent` on the current plane).
    pub capture_envelope_version: Option<String>,
    /// Physical digest records emitted by non-catalog fallback paths.
    pub capture_physical_fallback_records: u64,
    /// Physical digest records emitted by catalog paths.
    pub capture_physical_catalog_records: u64,
    /// Capture jobs accepted since this process started.
    #[serde(default)]
    pub queue_jobs: u64,
    /// Writes refused because the capture queue stayed full for the whole
    /// admission window, since this process started.
    ///
    /// The refusal is transient backpressure, so it is logged at WARN and
    /// answered with a retryable `503` rather than one ERROR per write
    /// (Sentry issue `7699865707`). This counter is where its volume shows up
    /// instead: a rising rate here is the signal that the single capture
    /// worker is not draining fast enough for the offered load.
    #[serde(default)]
    pub queue_rejections: u64,
    /// Total request time spent on bounded queue admission.
    #[serde(default)]
    pub queue_admission_us: u64,
    /// Total time jobs spent in the capture queue before worker receipt.
    #[serde(default)]
    pub queue_delay_us: u64,
    /// Total worker time spent on crash-safe marker stage operations.
    #[serde(default)]
    pub stage_us: u64,
    /// Total worker time spent on mutation-log record operations.
    #[serde(default)]
    pub record_us: u64,
    /// Total worker time spent on marker cleanup operations.
    #[serde(default)]
    pub cleanup_us: u64,
    /// Panics caught while processing a capture job, since this process
    /// started. The queue worker keeps draining after a panic instead of
    /// dying silently, so a healthy process always reads 0 here; any nonzero
    /// value means a capture job's write never reached the mutation log.
    #[serde(default)]
    pub queue_worker_panics: u64,
    /// Allocated bytes under the crash-safe re-export marker directory.
    pub reexport_disk_bytes: Option<u64>,
    /// Undecodable re-export markers dropped from the drain plane since start.
    ///
    /// Nonzero means the drain queue contained markers no code could parse, so
    /// the tick dropped them to keep draining. The dirty keys they named were
    /// never re-exported and are not recoverable from the marker.
    #[serde(default)]
    pub reexport_poison_dropped: u64,
    /// Automatic physical compaction state per armed plane.
    ///
    /// The map always names each armed plane, including one that has not fired
    /// in this process. Live overhang is a TTL cache snapshot (never a
    /// request-path `read_dir`); last-compact fields come from the in-process
    /// report. Compaction workers still stat the collection directory when
    /// they probe a trigger.
    #[serde(default)]
    pub automatic_compactions: std::collections::BTreeMap<String, AutomaticCompactionStatus>,
}

/// Last successful automatic physical compaction for one captured plane.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct AutomaticCompactionStatus {
    /// Unix seconds when the last automatic compaction completed.
    pub last_compacted_at_unix_s: Option<u64>,
    /// Trigger that armed the rewrite (`overhang` after D2; `max_bytes` is retired).
    pub last_trigger: Option<String>,
    /// Status-only absolute budget alarm. Does not gate compaction.
    pub configured_max_bytes: u64,
    /// Bytes before the last successful rewrite.
    pub last_bytes_before: Option<u64>,
    /// Bytes after the last successful rewrite.
    pub last_bytes_after: Option<u64>,
    /// Last measured allocated bytes for this plane.
    #[serde(default)]
    pub allocated_bytes: Option<u64>,
    /// Last measured apparent (live) bytes for this plane.
    #[serde(default)]
    pub apparent_bytes: Option<u64>,
    /// allocated − apparent. The reclaim a compact would return.
    #[serde(default)]
    pub overhang_bytes: Option<u64>,
    /// Measured overhang in basis points of allocation.
    #[serde(default)]
    pub overhang_bps: Option<u64>,
    /// Configured ratio trigger in basis points.
    #[serde(default)]
    pub trigger_overhang_bps: u64,
    /// Configured absolute overhang floor in bytes.
    #[serde(default)]
    pub trigger_overhang_bytes: u64,
    /// True when the current measurement is above both ratio and floor.
    #[serde(default)]
    pub above_trigger: bool,
    /// True when allocated bytes exceed the status-only budget alarm.
    #[serde(default)]
    pub cap_alarm: bool,
    /// Record bytes live ids still address (store residue counters), over
    /// the groups the probe could measure.
    #[serde(default)]
    pub live_bytes: Option<u64>,
    /// Dead record bytes: superseded puts, deleted puts, delete markers.
    /// This is what a delete leaves behind; only a rewrite returns it.
    #[serde(default)]
    pub dead_bytes: Option<u64>,
    /// `dead_bytes` as basis points of measured record bytes.
    #[serde(default)]
    pub dead_bps: Option<u64>,
    /// On-disk bytes whose residue the probe could not classify.
    #[serde(default)]
    pub residue_unknown_bytes: Option<u64>,
    /// Bytes a rewrite is expected to return: filesystem overhang plus the
    /// dead-record share of the measured plane. The trigger compares this,
    /// not `overhang_bytes` alone, against the floor and ratio.
    #[serde(default)]
    pub reclaimable_estimate_bytes: Option<u64>,
    /// `reclaimable_estimate_bytes` as basis points of allocation.
    #[serde(default)]
    pub reclaimable_bps: Option<u64>,
}

/// Operator-facing continuous mutation-log plane status (log lag + F).
///
/// Surfaced on [`SyncStatus`] so agents and `lastdb status` can read log lag
/// and published frontier without treating sealed-chunk % as primary health.
///
/// Multi-writer (design-lastdb-cloud-sync-mutation-log-first Phase B): published
/// head is vector F (`frontier_f_by_writer`: writer_id → through_seq). Scalar
/// `frontier_f` remains the max across writers for backward-compatible single-
/// stream consumers. Single-writer scaffolds are a one-entry map.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct MutationLogPlaneStatus {
    /// True when CaptureMode::MutationLog is the active continuous plane.
    pub active: bool,
    /// Device/writer id for the local stream (this process).
    pub writer_id: String,
    /// Published frontier F as scalar max across writers (compat).
    /// Prefer [`Self::frontier_f_by_writer`] for multi-device continuous sync.
    pub frontier_f: u64,
    /// Cloud-confirmed published frontier, named for operators rather than the
    /// internal F notation. Strictly the published watermark: unlike
    /// [`Self::frontier_f`] it is NOT raised by the plane-vector merge of
    /// frontiers other writers sealed locally, because a local seal is not a
    /// cloud confirmation. `frontier_f - published_through` is the publish gap.
    #[serde(default)]
    pub published_through: u64,
    /// Per-writer published through_seq (vector / HWM map F).
    /// Empty only before any segment is uploaded; single-writer is one key.
    #[serde(default)]
    pub frontier_f_by_writer: std::collections::BTreeMap<String, u64>,
    /// Highest frontier group-committed to the durable local mutation log.
    pub last_durable_frontier: u64,
    /// Upload lag: `last_durable_frontier - published_through`, i.e. work that
    /// is group-committed to the durable local log but not yet cloud-published.
    /// NOT `last_durable_frontier - frontier_f`: `frontier_f` is the max across
    /// writers after the plane-vector merge and does not reproduce this value.
    ///
    /// This measures the upload queue only. Writes not yet group-committed are
    /// in neither operand, so between local durable flushes both sides sit still
    /// and this reads 0 while cloud exposure keeps growing; at each flush it
    /// steps to the whole accumulated interval. Use `recovery_point_age_secs`
    /// for continuous exposure, not this.
    pub log_lag: u64,
    /// Seconds since the oldest writer's cloud-confirmed recovery point.
    /// `None` until a cloud upload has been confirmed in this process.
    #[serde(default)]
    pub recovery_point_age_secs: Option<u64>,
    /// Segments sealed+uploaded this process under the continuous log plane.
    pub segments_uploaded: u64,
    /// Peer writer-scoped segments this process applied on the regular
    /// `do_sync` cycle. Read it as the download-side twin of
    /// [`Self::segments_uploaded`]: without it, "this node published nothing to
    /// apply" and "this node never ran the apply path" are the same reading.
    ///
    /// Restore (`lastdb restore --into`) does not book here — this counts only
    /// the live-Mini cycle.
    #[serde(default)]
    pub peer_segments_applied: u64,
    /// Records applied out of [`Self::peer_segments_applied`].
    #[serde(default)]
    pub peer_records_applied: u64,
    /// True when [`Self::recovery_point_age_secs`] is at or above the
    /// configured `mutation_log_lag_degraded_threshold_secs`.
    ///
    /// Derived from the recovery point age in **seconds**, not from
    /// [`Self::log_lag`]: `log_lag` is a nanosecond frontier delta that reads 0
    /// between durable flushes and steps to the whole interval at each flush,
    /// so it cannot express continuous exposure (see its own doc). Until
    /// 2026-08-23 this flag compared `log_lag` against a seconds-scale
    /// threshold and was therefore `true` at every nonzero lag.
    ///
    /// `false` while `recovery_point_age_secs` is `None` — a node with no
    /// cloud-confirmed upload yet is fresh, not degraded.
    ///
    /// Also `false` while [`Self::log_lag`] is `0`: the publish event age only
    /// resets when a publish runs, and a publish only runs when something is
    /// unpublished, so on a caught-up node the age grows without bound by
    /// construction. From 2026-09-05 to 2026-09-13 that read as
    /// `mutation_log_lag` on every idle status sample. A stalled uploader
    /// still trips as soon as the next local write seals, because the backlog
    /// is then nonzero while the age keeps growing.
    pub lag_degraded: bool,
    /// True when at least one pin-log capture runtime is registered for this
    /// plane. This is a factual presence signal, not degradation by itself:
    /// a fresh node has no runtime until its first capturable local write.
    #[serde(default)]
    pub capture_registered: bool,
    /// MutationIntent rows skipped this process because they cannot be sealed
    /// (missing atom). One bad row must not fail-close later uploads.
    #[serde(default)]
    pub records_quarantined: u64,
    /// Last unsealable-record reason, including the missing atom id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_quarantine_reason: Option<String>,
}

/// Outcome of the retired engine.
///
/// Past-grace re-enable pulls cloud logs first, then snapshots local state,
/// then resumes recording. Network/snapshot failures are reported here without
/// blocking local R/W; recording still resumes so the node is not stuck off.
#[derive(Debug, Clone, Serialize)]
pub struct CloudSyncReenableOutcome {
    /// `incremental` (within grace), `snapshot_reconcile` (past grace), or
    /// `already_on` when Cloud Sync was not intentionally disabled.
    pub strategy: String,
    /// Whether a peer/self cloud log download was attempted.
    pub pull_attempted: bool,
    /// Whether the pull completed without error.
    pub pull_ok: bool,
    /// Whether a force snapshot publish was attempted.
    pub snapshot_attempted: bool,
    /// Whether the snapshot publish succeeded.
    pub snapshot_ok: bool,
    /// Snapshot sequence when `snapshot_ok`.
    pub snapshot_seq: Option<u64>,
    /// Durable outbox entries cleared as residual dead staging.
    pub staging_cleared: usize,
    /// Whether the engine is recording local cloud mutations after re-enable.
    pub now_recording: bool,
    /// Non-fatal error messages from pull/snapshot steps.
    pub errors: Vec<String>,
}

/// Operator-visible detail for an oversize durable-outbox entry that was
/// permanently dropped so smaller entries behind it can sync.
#[derive(Debug, Clone, Serialize)]
pub struct OversizeOutboxDropStatus {
    /// Durable outbox sequence number that was dropped.
    pub seq: u64,
    /// Serialized/raw entry size observed at the drop site.
    pub size_bytes: usize,
    /// Active per-cycle upload byte cap that the entry exceeded.
    pub max_bytes: usize,
    /// Drop site (`record_op`, `upload_queue`, `durable_outbox_raw`, or
    /// `durable_outbox_decoded`).
    pub source: String,
    /// Unix timestamp (seconds) when the drop was recorded.
    pub dropped_at: u64,
}
