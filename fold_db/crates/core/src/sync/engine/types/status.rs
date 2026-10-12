use super::{
    CapturePlaneStatus, DownloadCycleStats, MutationLogPlaneStatus, OversizeOutboxDropStatus,
    SyncBackupBlocker, SyncReplayBlocker, UploadCycleStats,
};
use crate::sync::engine::PinLogTargetStatus;
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
