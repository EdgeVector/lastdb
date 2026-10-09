use crate::sync::engine::PinLogTargetStatus;
use crate::sync::snapshot::SnapshotScrubReport;
use serde::{Deserialize, Serialize};

/// File-blob durability accounting for this process: fetches the remote CAS
/// proved absent, split by whether this node had claimed to have uploaded them.
///
/// ## Why the split is the whole point
///
/// Both classes answer the caller with the same `404`, so the request-layer
/// error count (`lastdb ops`, verb `file_blob`) cannot separate them. That
/// leaves the one number an operator can reach meaning either of two opposite
/// things:
///
/// - **`absent_with_memo`** — this node recorded these bytes as uploaded, and
///   the remote proves they are not there. That is a write the node accepted,
///   returned a well-formed pointer for, and cannot serve back. It is a
///   durability defect, and it is silent: the pointer still reads back fine
///   from the row.
/// - **`absent_without_memo`** — an ordinary miss for bytes this node never
///   claimed to hold. Expected, benign, and by far the more common of the two.
///
/// The engine already distinguishes them: `forget_missing_file_blob` reads the
/// memo before clearing it, precisely so a proven-absent object drops the claim
/// that it exists. It used to spend that answer on a log line and discard it,
/// which is why measuring the durability class needed one CAS fetch per pointer
/// from outside the node (356 fetches across the fleet, 2026-08-17).
///
/// ## What the event/distinct pair actually means here
///
/// A proven-absent fetch *clears* the memo, so the second fetch of the same
/// lost blob books under `absent_without_memo` instead. `absent_with_memo`
/// therefore counts **loss discoveries**, not retries: one lost blob a client
/// re-fetches all afternoon still books once.
///
/// So `absent_with_memo > distinct_with_memo` does not mean "a chatty client".
/// It means a blob was re-uploaded, memoized again, and lost again — recurrent
/// loss, which is a worse condition than the same count spread over distinct
/// blobs. Read the two together or neither.
///
/// The cost of that same clearing: `absent_without_memo` conflates "bytes we
/// never had" with "bytes we already reported lost". It is a denominator, not
/// a diagnosis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct FileBlobDurability {
    /// Proven-absent fetch events where this node held an upload memo.
    pub absent_with_memo: u64,
    /// Proven-absent fetch events with no memo held (ordinary miss).
    pub absent_without_memo: u64,
    /// Distinct `file_hash`es behind `absent_with_memo`. A floor when
    /// `distinct_capped` is set.
    pub distinct_with_memo: u64,
    /// True once the identity set hit its cap, so `distinct_with_memo` is a
    /// floor rather than a total.
    pub distinct_capped: bool,
}

/// Sync engine state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncState {
    /// No unsynced changes.
    Idle,
    /// Local changes not yet uploaded.
    Dirty,
    /// Upload in progress.
    Syncing,
    /// Network unavailable, will retry.
    Offline,
}

/// Effective explicit backup-upload concurrency override for the live engine.
///
/// The process environment remains authoritative for compatibility. A runtime
/// owner override is retained even when shadowed so status explains why an
/// owner action did not change the effective value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BackupUploadConcurrencyStatus {
    /// Explicit concurrency currently used by the backup uploader. `None`
    /// means the adaptive policy / catch-up floor decides each cycle.
    pub effective_override: Option<usize>,
    /// `environment`, `runtime`, or `adaptive`.
    pub effective_source: &'static str,
    /// Process-local owner override, including when an environment override
    /// shadows it.
    pub runtime_override: Option<usize>,
}

/// Snapshot of sync engine status for external consumers.
#[derive(Debug, Clone, Serialize)]
pub struct SyncStatus {
    /// Current state of the sync engine.
    pub state: SyncState,
    /// Whether local storage writes are currently allowed.
    ///
    /// Cloud sync degradation must never make Mini read-only; this stays true
    /// while the local store is available, even if the durable outbox is past
    /// its target capacity.
    pub local_writable: bool,
    /// Whether sync needs operator/user attention but local reads/writes remain
    /// available.
    pub sync_degraded: bool,
    /// Number of durable upload-staging entries waiting for cloud backup.
    ///
    /// Kept as the legacy field name for clients that already render
    /// `pending_count`; new clients should prefer `durable_outbox_count`.
    pub pending_count: usize,
    /// Number of durable upload-staging entries waiting for cloud backup.
    pub durable_outbox_count: usize,
    /// Number of entries currently admitted to the bounded upload worker queue.
    pub upload_queue_count: usize,
    /// Maximum entries admitted to the upload worker queue. `0` means unlimited.
    pub upload_queue_max: usize,
    /// Target durable upload-staging entry count before sync is reported
    /// degraded. This is a health threshold, not a local write admission cap.
    /// `0` means unlimited.
    pub durable_outbox_max: usize,
    /// Unix timestamp (seconds) of last successful sync, if any.
    pub last_sync_at: Option<u64>,
    /// Last sync error message, if the most recent sync failed.
    pub last_error: Option<String>,
    /// Unix timestamp (seconds) when `last_error` was recorded.
    ///
    /// Always read this alongside `last_error`. The message is one field shared
    /// by every sync subsystem, so an hours-stale transient failure otherwise
    /// reads as the live explanation of health.
    pub last_error_at: Option<u64>,
    /// Consecutive failed sync cycles, zero when the last cycle succeeded.
    pub consecutive_sync_failures: u64,
    /// Unix timestamp (seconds) of the first failure in the current streak.
    pub failing_since: Option<u64>,
    /// Machine-readable reasons `sync_degraded` is set, empty when healthy.
    ///
    /// Lets an operator or dashboard see *why* sync is unhealthy without
    /// re-deriving it from the other fields: `sync_failing`,
    /// `outbox_over_target`, `oversize_outbox_drops`,
    /// `capture_reexport_pending` (only when pending markers remain **and**
    /// `last_capture_reexport_error` is set — pure residual drain is not RED),
    /// and `capture_reexport_poison_dropped` for process-local capture loss.
    pub degraded_reasons: Vec<String>,
    /// Structured replay blocker when sync is stopped on an untrusted cloud log
    /// entry that must not be skipped automatically.
    pub replay_blocker: Option<SyncReplayBlocker>,
    /// Structured backup blocker when the pre-upload decryptability proof is
    /// deterministically unsatisfiable, so no cycle can ever upload.
    ///
    /// `degraded_reasons` gains `backup_bootstrap_blocked` at the same time.
    /// The two together are the difference between "backup is failing" (retry
    /// might fix it) and "backup cannot start" (only an operator can).
    pub backup_blocker: Option<SyncBackupBlocker>,
    /// Number of undecryptable at-rest rows omitted from the last successful
    /// explicit snapshot backup.
    pub undecryptable_unsynced_count: usize,
    /// Whether the last successful explicit snapshot backup included every
    /// at-rest row it scanned.
    pub last_snapshot_complete: bool,
    /// Namespaces that contained undecryptable rows in the last successful
    /// explicit snapshot backup.
    pub last_snapshot_undecryptable_namespaces: Vec<String>,
    /// Stats from the most recent steady-state download cycle, if any.
    ///
    /// Used to diagnose catch-up memory pressure (`bytes_downloaded`,
    /// deferred backlog) without a kernel panic first.
    pub last_download: Option<DownloadCycleStats>,
    /// Stats from the most recent steady-state upload selection, if any.
    pub last_upload: Option<UploadCycleStats>,
    /// Adaptive (or fixed) upload caps for the current / last cycle.
    ///
    /// Operators should look here instead of assuming hard-coded entry counts:
    /// `budget_bytes` and `concurrency` are the real resource controls.
    pub upload_policy: Option<crate::sync::engine::UploadPolicySnapshot>,
    /// Live backup-upload concurrency override and its precedence source.
    pub backup_upload_concurrency: BackupUploadConcurrencyStatus,
    /// Trigger reason (`"outbox_overflow_depth"` / `"outbox_overflow_age"`) of
    /// the most recent durable-outbox overflow snapshot attempt this process,
    /// if any. Lets operators see *why* a snapshot fired without grepping logs.
    pub last_outbox_overflow_reason: Option<String>,
    /// Number of oversize durable-outbox entries permanently dropped this
    /// process so upload catch-up could continue.
    pub oversize_outbox_drop_count: u64,
    /// Details for the most recent oversize durable-outbox drop, if any.
    pub last_oversize_outbox_drop: Option<OversizeOutboxDropStatus>,
    /// Whether local mutations are still being recorded into the cloud staging
    /// plane. False when Cloud Sync is intentionally off past
    /// `sync_off_grace_secs` (stop-staging). Always true while sync is on or
    /// within the temporary-off grace window.
    pub recording_local_changes: bool,
    /// True when Cloud Sync has been intentionally off longer than
    /// `sync_off_grace_secs`. False when sync is on or still within grace.
    pub sync_off_grace_expired: bool,
    /// How re-enable will reconverge while intentionally off:
    /// `"incremental"` (within grace) or `"snapshot_reconcile"` (past grace).
    /// `None` when Cloud Sync is on.
    pub reenable_strategy: Option<String>,
    /// Unix timestamp (seconds) when Cloud Sync was intentionally disabled, if
    /// currently off. `None` means sync is on / not intentionally paused.
    pub cloud_sync_disabled_at: Option<u64>,
    /// Configured grace window (seconds) for temporary off. Surfaced so status
    /// consumers do not need a second config read.
    pub sync_off_grace_secs: u64,
    /// Per-target pin-mode durable log counters for active or recently active
    /// pins in this process.
    pub pin_logs: Vec<PinLogTargetStatus>,
    /// Continuous mutation-log plane health when CaptureMode::MutationLog is on.
    ///
    /// Primary continuous-sync health story (log lag + published frontier F +
    /// writer_id). Sealed-chunk backup progress is secondary / bootstrap-only
    /// under this plane (design-lastdb-cloud-sync-mutation-log-first).
    pub mutation_log: Option<MutationLogPlaneStatus>,
    /// Process estimate for dirty-key intents not yet recovered into the log.
    /// It can be high or low after a restart or a concurrent stage and drain.
    /// Use [`Self::capture_reexport_pending_known_nonempty`] for presence.
    ///
    /// This is an observe-only drain signal: count > 0 alone does **not** set
    /// [`Self::sync_degraded`]. Health RED for reexport requires pending work
    /// *and* an unrecovered [`Self::last_capture_reexport_error`].
    pub capture_reexport_pending_count: u64,
    /// `None` means marker presence has not been proved after restart.
    /// `Some(true)` means a durable marker was staged or observed. It can stay
    /// true after a clear until the next complete empty physical scan.
    /// `Some(false)` requires that scan with no concurrent stage.
    /// This does not prove cloud completeness for past Async acknowledgments.
    pub capture_reexport_pending_known_nonempty: Option<bool>,
    /// Capture/re-export failures observed since this engine started.
    pub capture_reexport_failure_count: u64,
    /// Most recent capture/re-export error, distinct from ordinary upload lag.
    ///
    /// When this is set and [`Self::capture_reexport_pending_count`] > 0,
    /// `degraded_reasons` includes `capture_reexport_pending`. Cleared when a
    /// capture tick drains a marker and leaves no marker skipped in that tick.
    pub last_capture_reexport_error: Option<String>,
    /// Operator-facing capture-plane accounting. Every field is either an
    /// in-process counter or a collection-directory stat; status never opens
    /// the store index or runs a compact dry-run to populate this block.
    pub capture: CapturePlaneStatus,
}

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

/// Outcome of [`crate::sync::SyncEngine::reenable_cloud_sync`].
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

/// Actionable description of a cloud replay blocker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncReplayBlocker {
    /// Stable machine-readable code for consumers.
    pub code: String,
    /// Sync target label whose replay is blocked.
    pub target: String,
    /// Cloud log sequence that could not be safely replayed.
    pub seq: u64,
    /// Redacted failure reason.
    pub reason: String,
    /// Human-actionable next step.
    pub action: String,
}

/// Actionable description of a cloud **backup** blocker — the upload side of
/// [`SyncReplayBlocker`].
///
/// Raised when the pre-upload decryptability proof is unsatisfiable in a way
/// retrying cannot change, so every further cycle is doomed. Carrying it as
/// structured status (rather than only as the cycle's last error string) is
/// what lets `lastdb status` say "backup cannot start, an operator must act"
/// instead of a generic `sync_failing` that reads as transient.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncBackupBlocker {
    /// Stable machine-readable code for consumers.
    pub code: String,
    /// Sync target label whose backup is blocked.
    pub target: String,
    /// Cloud log head sequence that could not be used as a proof, when one was
    /// found. `None` when the prefix listed no log object at all.
    pub head_seq: Option<u64>,
    /// Redacted failure reason.
    pub reason: String,
    /// Human-actionable next step.
    pub action: String,
    /// Unix timestamp (seconds) when the block was first latched.
    pub blocked_since: u64,
}

/// Outcome of explicitly quarantining one blocked cloud log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayBlockerQuarantineOutcome {
    /// Sync target label whose blocked entry was cleared.
    pub target: String,
    /// Cloud log sequence that was cleared for this device.
    pub seq: u64,
    /// Number of cloud log objects deleted (`1` for corrupt-entry delete path,
    /// `0` for apply-failed local skip).
    pub deleted_log_objects: usize,
    /// `"delete"` destroys the cloud object; `"skip_local"` only advances this
    /// device and leaves the object for peers / later builds.
    pub mode: String,
}

/// User-visible completeness summary for the last successful explicit snapshot
/// backup. Clean by default so status stays honest before any poison row has
/// been observed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SnapshotCompletionStatus {
    pub undecryptable_unsynced_count: usize,
    pub last_snapshot_complete: bool,
    pub undecryptable_namespaces: Vec<String>,
}

impl Default for SnapshotCompletionStatus {
    fn default() -> Self {
        Self {
            undecryptable_unsynced_count: 0,
            last_snapshot_complete: true,
            undecryptable_namespaces: Vec::new(),
        }
    }
}

impl From<&SnapshotScrubReport> for SnapshotCompletionStatus {
    fn from(report: &SnapshotScrubReport) -> Self {
        let mut namespaces: Vec<String> = report
            .undecryptable
            .iter()
            .map(|row| row.namespace.clone())
            .collect();
        namespaces.sort();
        namespaces.dedup();

        Self {
            undecryptable_unsynced_count: report.skipped(),
            last_snapshot_complete: report.is_clean(),
            undecryptable_namespaces: namespaces,
        }
    }
}

/// Outcome of `SyncEngine::purge_personal_log`. Counts of cloud-side
/// objects deleted, returned to the caller for logging and progress
/// reporting.
#[derive(Debug, Default, Clone, Serialize)]
pub struct PurgeOutcome {
    /// Number of `{user_hash}/log/{seq}.enc` objects deleted.
    pub deleted_log_objects: usize,
    /// Number of `{user_hash}/snapshots/*.enc` objects deleted.
    pub deleted_snapshots: usize,
}

/// Observability snapshot from the most recent steady-state download cycle.
///
/// Surfaced on [`SyncStatus`] and in `lastdb status` / self-metrics so operators
/// can see catch-up pressure without attaching Instruments after a thrash.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DownloadCycleStats {
    /// Sync target label (e.g. `"personal"`).
    pub target: String,
    /// Seqs listed after the cursor before per-cycle truncation.
    pub entries_listed: usize,
    /// Seqs this cycle actually attempted to fetch.
    pub entries_attempted: usize,
    /// Entries successfully unsealed and replayed.
    pub entries_replayed: u64,
    /// Entries produced by this same device that the steady-state downloader
    /// skipped. Restore/bootstrap intentionally replays self-authored entries.
    pub entries_skipped_self: u64,
    /// Ciphertext bytes pulled from object storage this cycle.
    pub bytes_downloaded: u64,
    /// Entries skipped because they exceeded `max_download_entry_bytes`.
    pub entries_skipped_oversize: u64,
    /// Seqs left for a later cycle because of entry/byte caps.
    pub entries_deferred: usize,
    /// Health-facing deferred replay work. Unlike `entries_deferred`, this can
    /// exclude a personal-log backlog that currently appears to be only local
    /// echoes, so status output does not report self-drain as peer replay work.
    pub entries_health_deferred: usize,
    /// Whether the entry-count cap truncated the listed backlog.
    pub truncated_by_entry_cap: bool,
    /// Whether the byte budget stopped the cycle early.
    pub truncated_by_byte_cap: bool,
}

/// Observability snapshot from the most recent steady-state upload selection.
///
/// Surfaced on [`SyncStatus`] so operators can see upload catch-up pressure
/// (re-enable thrash with 0 downloads + large pending).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UploadCycleStats {
    /// Entries sitting in the in-memory upload queue before this cycle's cap.
    pub entries_queued: usize,
    /// Entries selected for seal/upload this cycle.
    pub entries_selected: usize,
    /// Serialized plaintext bytes of the selected entries.
    pub bytes_selected: u64,
    /// Entries left for a later cycle because of entry/byte caps.
    pub entries_deferred: usize,
    /// Whether the entry-count cap truncated the queue.
    pub truncated_by_entry_cap: bool,
    /// Whether the byte budget stopped selection early.
    pub truncated_by_byte_cap: bool,
}

/// Summary of a single `bootstrap_target` invocation.
///
/// Consumed by `bootstrap_all` to decide whether schema/embedding reloaders
/// need to fire after a multi-target restore, and by fold_db_node's
/// `bootstrap_from_cloud` flow to report what was restored.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BootstrapOutcome {
    /// Highest sequence number restored (from snapshot + log replay).
    /// Zero if the target had no prior data.
    pub last_seq: u64,
    /// Count of log entries replayed after the snapshot.
    pub entries_replayed: usize,
    /// True if at least one replayed entry wrote to the `schemas` namespace.
    /// Used to decide whether the schema reloader should fire.
    pub schemas_replayed: bool,
    /// True if at least one replayed entry wrote to the `native_index`
    /// namespace. Used to decide whether the embedding reloader should fire.
    pub embeddings_replayed: bool,
}

#[derive(Debug, Default)]
pub(crate) struct UploadOutcome {
    pub(crate) entries_uploaded: usize,
    pub(crate) max_seq_uploaded: Option<u64>,
    pub(crate) partial_error: Option<crate::sync::error::SyncError>,
    pub(crate) transfer_bytes_uploaded: u64,
    pub(crate) transfer_elapsed_secs: f64,
}

/// Outcome of a pre-upload snapshot decrypt-proof (see
/// [`SyncEngine::try_decrypt_latest_snapshot`](crate::sync::engine::SyncEngine::try_decrypt_latest_snapshot)).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SnapshotDecryptProof {
    /// `latest.enc` exists and opened with the current key.
    Decrypted,
    /// `latest.enc` exists but could not be opened as a proof object.
    Undecryptable { reason: String },
    /// No `latest.enc` snapshot present.
    Absent,
    /// Snapshot body exceeded `SyncConfig::max_download_entry_bytes` — refuse
    /// to decrypt in-process for proof (memory-safety).
    Oversized { bytes: usize, max: usize },
}
