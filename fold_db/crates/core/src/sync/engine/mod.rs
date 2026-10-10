//! Sync engine: local store ↔ S3 replication via encrypted log entries.
//!
//! Split across submodules for navigability:
//! - [`types`] — public status/config types and private bookkeeping structs
//! - [`helpers`] — pure helpers (key parsing, compaction policy, ordered fetch)
//! - [`wiring`] — construction, callbacks, status, capacity, cursor codec
//! - [`cycle`] — `sync` / `do_sync` orchestration, outcome recording, backlog
//! - [`outbox`] — durable outbox + local op recording
//! - [`transfer`] — upload/download, partition, personal log index
//! - [`compact`] — compaction policy + snapshot/delete-old-logs
//! - [`bootstrap`] — cloud bootstrap (snapshot + ordered log replay)
//! - [`backup`] — explicit snapshot backup + scrub
//! - [`lock`] — single-device write lock
//! - [`purge`] — personal cloud log/snapshot purge
//! - [`prefix_inventory`] — read-only R2 prefix/category size breakdown
//! - [`configure`] — share/org target configuration
//! - [`replay`] — convergent replay / merge paths
//!
//! This file keeps the `SyncEngine` struct definition and Drop scrub only.

mod backup;
pub mod backup_atom_rewrite;
pub(crate) mod backup_keys;
mod backup_restore;
pub(crate) mod backup_uploader;
mod bootstrap;
mod compact;
mod configure;
mod cycle;
mod file_blob;
pub mod gc_jobs;
mod helpers;
mod lock;
mod outbox;
pub(crate) use outbox::CAPTURE_MARKER_BATCH_MAX_ENVELOPE_BYTES;
mod pin_log;
mod prefix_inventory;
mod primary_resume;
mod purge;
mod recovery_descriptor;
mod replay;
mod restore_chunk_cache;
pub use recovery_descriptor::{RecoveryDescriptorV1, RecoveryLayoutV1};
pub use restore_chunk_cache::RestoreChunkCache;
mod restore_progress;
pub use restore_progress::{RestorePhase, RestoreProgress, RestoreProgressSnapshot};
mod status_disk_cache;
mod thumb_pack;
mod transfer;
mod types;
mod upload_policy;
mod wiring;

tokio::task_local! {
    /// Only the explicit backup of a paused home may bypass its upload interlock.
    static PAUSED_HOME_BACKUP_UPLOAD: ();
}

// Store-level capture methods live in `crate::sync::capture` and attach via
// `impl SyncEngine` there; re-export tick stats for callers/tests.
pub use crate::sync::capture::{CaptureTickStats, HealStagingReport};
pub use backup_restore::{
    laststore_published_backup_cut, laststore_published_backup_cut_detailed,
    restore_laststore_cloud_backup, restore_laststore_cloud_backup_detailed,
    restore_laststore_cloud_backup_from_latest_pointer,
    restore_laststore_cloud_backup_from_rescue_with_cache,
    restore_laststore_cloud_backup_with_cache, restore_laststore_cloud_backup_with_progress,
    LastStoreCloudRestoreReport, S0RestoreBoundary, S0RestoreFailure,
};
pub use backup_uploader::{BackupOrphanGcReport, LastStoreCloudSnapshotReport};
pub use prefix_inventory::{PrefixInventoryEntry, PrefixInventoryReport};
pub use primary_resume::{
    inspect_primary_resume_plan, PrimaryResumeCut, PrimaryResumeCutIdentity,
    PrimaryResumeLogInventory,
};
pub(crate) use status_disk_cache::{StatusDiskUsageCache, DEFAULT_TTL as STATUS_DISK_USAGE_TTL};
pub use types::CaptureMode;

pub use crate::SyncConflict;
pub use file_blob::{FileBlobRef, FileThumbnailUpload};
pub use pin_log::{
    audit_pin_log_plane, prepare_offline_s0_restore_marker, replay_mutation_log_segments,
    require_offline_s0_restore_marker, restore_mutation_log_after_s0,
    restore_mutation_log_after_s0_from_plane, seal_mutation_log_segment, BackupRestoreMode,
    MutationLogLocalCloud, MutationLogReplayReport, MutationLogSegment, MutationLogUploadReport,
    PinLogPlaneReport, PinLogRecord, PinLogTargetStatus, PinLogWriterStat, PinModeLocalCloud,
    PinModePublishDescriptor, PinModeRestoreReport, SealedBaseMember, PIN_LOG_NAMESPACE,
    PIN_LOG_OPERATOR_KEYS_PER_CALL,
};
pub(crate) use pin_log::{MutationLogAppendReceipt, MutationPublicationWait};
pub use types::{
    AutomaticCompactionStatus, BackupUploadConcurrencyStatus, BootstrapOutcome,
    CloudSyncReenableOutcome, DownloadCycleStats, EmbeddingReloadCallback, FileBlobDurability,
    MutationIntentApplier, MutationLogPlaneStatus, OversizeOutboxDropStatus, PhotographCutBarrier,
    PurgeOutcome, ReloadCallback, ReplayBlockerQuarantineOutcome, SchemaReloadCallback,
    SnapshotCompletionStatus, SyncBackupBlocker, SyncConfig, SyncReplayBlocker, SyncState,
    SyncStatus,
};
pub use upload_policy::{
    ForegroundPressure, UploadPolicyMode, UploadPolicySnapshot, UploadThrottleSource,
};

// Struct-field types used in this file.
use super::auth::{AuthClient, AuthRefreshCallback};
use super::org_sync::SyncTarget;
use super::s3::S3Client;
use crate::crypto::CryptoProvider;
use crate::security::Ed25519KeyPair;
use crate::storage::traits::NamespacedStore;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;

/// Cap on distinct `file_hash`es retained behind the proven-absent file-blob
/// counter, so a badly damaged CAS cannot grow the set without bound.
///
/// Past the cap the count is reported as a floor rather than a total. Sized
/// like the read-path integrity identity cap: large enough that a real repair
/// can be scoped from it, small enough to be irrelevant to RSS.
pub(crate) const FILE_BLOB_ABSENT_IDENTITY_CAP: usize = 1024;

// Re-exports for child modules that `use super::*` (transfer, bootstrap, …).
// These used to live as plain `use` imports while impl methods sat in this
// file; after the wiring/cycle split they must stay visible to descendants.
// Every name here is referenced by a child module; the compiler flags any that stop being used.
pub(crate) use super::error::{redact_sync_error_text, SyncError, SyncResult};
pub(crate) use super::log::{LogEntry, LogOp};
pub(crate) use super::org_sync::{SyncDestination, SyncPartitioner};
pub(crate) use super::snapshot::{snapshot_should_skip_namespace, Snapshot};
pub(crate) use futures::stream::StreamExt;
pub(crate) use helpers::*;
pub(crate) use types::*;

/// The sync engine manages replication of a local Sled database to S3.
///
/// Architecture:
/// ```text
/// fold_db (local) ──▶ SyncEngine ──▶ Auth Lambda ──▶ S3 (encrypted blobs)
///
/// State machine:
///   IDLE ──mutation──▶ DIRTY ──timer──▶ SYNCING ──success──▶ IDLE
///                       ▲                  │
///                       └──── failure ─────┘
/// ```
///
/// The engine:
/// 1. Records KvStore operations as encrypted log entries
/// 2. Uploads log entries to S3 via presigned URLs
/// 3. Periodically compacts logs into snapshots
/// 4. Manages single-device write lock
/// 5. Supports bootstrap (download snapshot + replay logs) for new devices
pub struct SyncEngine {
    state: Arc<Mutex<SyncState>>,
    /// Pending log entries not yet uploaded (in-memory upload queue).
    pub(crate) pending: Arc<Mutex<Vec<LogEntry>>>,
    /// Current sequence number.
    seq: Arc<Mutex<u64>>,
    /// Lazily-seeded, incrementally-maintained durable-outbox bookkeeping
    /// (entry count + highest seq). See [`OutboxMeta`]. Keeps the synced-write
    /// hot path and `status()` `O(1)` in outbox depth instead of scanning the
    /// whole outbox to count.
    outbox_meta: Arc<Mutex<OutboxMeta>>,
    /// Device identifier (unique per device).
    device_id: String,
    /// Encryption provider for sealing log entries and snapshots.
    crypto: Arc<dyn CryptoProvider>,
    /// S3 client for uploads/downloads.
    s3: S3Client,
    /// Auth client for presigned URLs and lock management.
    auth: AuthClient,
    /// The local namespaced store (for snapshot creation + store-level capture).
    pub(crate) store: Arc<dyn NamespacedStore>,
    /// Store used only for sync cursor bookkeeping.
    ///
    /// Production sync/replay uses the encrypted store view for user data, but
    /// cursor values already carry their own E2E `ENC:` envelope. Keeping this
    /// on the raw store preserves compatibility with existing cursor rows and
    /// avoids confusing those self-sealed values with the local at-rest seam.
    cursor_store: Arc<dyn NamespacedStore>,
    /// Configuration.
    pub(crate) config: SyncConfig,
    /// A paused home may publish one explicit full backup. It cannot run
    /// normal cloud upload, peer replay, or mutation capture.
    backup_only_mode: std::sync::atomic::AtomicBool,
    /// One cut's local writer frontier for a primary-authoritative resume.
    /// The cut stays in backup-only mode for strict cloud-copy verification.
    primary_resume_frontier: Mutex<Option<u64>>,
    /// Optional callback for status changes.
    /// Unix timestamp (seconds) of last successful sync.
    last_sync_at: Arc<Mutex<Option<u64>>>,
    /// Last sync error message (cleared on success).
    last_error: Arc<Mutex<Option<String>>>,
    /// Unix timestamp (seconds) when `last_error` was recorded.
    ///
    /// `last_error` is one field shared by every sync subsystem, so without a
    /// timestamp a stale transient failure reads as the current explanation of
    /// health. That is exactly how a 36 h continuous backup stall hid behind an
    /// hours-old download error (papercut
    /// `lastdb-cloud-backup-silent-stall-green-status`).
    last_error_at: Arc<Mutex<Option<u64>>>,
    /// Consecutive failed sync cycles; reset to zero by a successful sync.
    ///
    /// This is the liveness signal behind [`SyncStatus::sync_degraded`]. Depth
    /// alone cannot see a stall that keeps the outbox empty, and
    /// `last_sync_at` alone cannot distinguish "never needed to sync" from
    /// "never managed to" — only a failure count can.
    consecutive_sync_failures: AtomicU64,
    /// Unix timestamp (seconds) of the first failure in the current run of
    /// consecutive failures, so operators can see *how long* sync has been
    /// failing rather than only that it failed once.
    failing_since: Arc<Mutex<Option<u64>>>,
    /// Structured replay blocker for the last corrupt cloud log failure.
    replay_blocker: Arc<Mutex<Option<SyncReplayBlocker>>>,
    /// Latched, deterministic pre-upload proof failure (see
    /// [`crate::sync::error::SyncError::BackupBootstrapBlocked`]).
    ///
    /// Held in memory on purpose: a daemon restart re-evaluates, so an upgrade
    /// that understands the envelope version — or a `max_download_entry_bytes`
    /// change — clears it without an operator step.
    pub(crate) backup_blocker: Arc<Mutex<Option<SyncBackupBlocker>>>,
    /// Completeness of the last successful explicit snapshot backup.
    last_snapshot_completion: Arc<Mutex<SnapshotCompletionStatus>>,
    /// Aggregates transfer failures until pending depth crosses an alert threshold.
    backlog_alerts: Arc<Mutex<CloudSyncBacklogAlertState>>,
    /// Partitioner for classifying pending entries by key prefix.
    partitioner: Arc<Mutex<Option<SyncPartitioner>>>,
    /// All sync targets. Index 0 is always the personal target.
    /// Share targets are appended via `configure_targets`.
    targets: Arc<Mutex<Vec<SyncTarget>>>,
    /// Local storage prefixes represented by each remote target prefix.
    ///
    /// A named org database has a local hash that differs from the org cloud
    /// head hash. Keep both identities so photographs select the local rows
    /// that the target owns.
    target_restore_scopes: Arc<Mutex<HashMap<String, Vec<String>>>>,
    /// Serializes snapshots and updates of the partitioner + target list pair.
    target_config_lock: Arc<Mutex<()>>,
    /// Monotonic generation for the target/partitioner pair. Entry allocation
    /// binds its durable HWM seed to this value, and append rejects a stale
    /// generation before it can reuse a newly configured target's frontier.
    target_config_generation: AtomicU64,
    /// Rotates scoped mutation-log uploads without delaying every personal cycle.
    scoped_upload_turn: AtomicU64,
    /// Per-prefix download cursor: maps prefix -> last_seq_downloaded.
    download_cursors: Arc<Mutex<std::collections::HashMap<String, u64>>>,
    /// Counts personal-index reads so the cheap steady-state index remains a
    /// cache, not an authority. Every bounded interval the downloader does a
    /// full object-list reconciliation, recovering objects uploaded by a peer
    /// that crashed before it could merge them into `log_index.enc`.
    personal_index_reads_since_reconcile: AtomicU64,
    /// Unseal failures already emitted at error level in this engine process.
    ///
    /// Wrong-key / corrupt ciphertext failures are intentionally not
    /// quarantined: the cursor must stop before undecryptable data. They can
    /// still recur on every sync tick, so this keeps the first failure loud and
    /// suppresses duplicate error-level events for the same target/seq/reason.
    unseal_failure_log_cache: Arc<Mutex<HashSet<UnsealFailureLogKey>>>,
    /// Count of org-scoped storage keys skipped during replay/bootstrap this
    /// process (incident-lastdbd-0226: consented drop of org-shared rows after
    /// the org-crypto map was removed). Loudly reported; never silent.
    org_scoped_replay_skips: AtomicU64,
    /// File-blob fetches the remote CAS proved absent **while this node held an
    /// "already uploaded" memo** for those bytes — see [`FileBlobDurability`].
    ///
    /// This is the durability class: a write this node accepted, returned a
    /// well-formed pointer for, and cannot serve back.
    file_blob_absent_with_memo: AtomicU64,
    /// File-blob fetches the remote CAS proved absent with **no** memo held —
    /// an ordinary miss for bytes this node never claimed to have uploaded.
    ///
    /// Counted separately for one reason: without it the durability class above
    /// is unreadable. Both land on the same `404` at the request layer, so
    /// `lastdb ops`' `file_blob` error count cannot tell them apart and a
    /// reader is left with a number that means either "nothing is wrong" or
    /// "we are losing user bytes".
    file_blob_absent_without_memo: AtomicU64,
    /// Distinct `file_hash`es behind [`Self::file_blob_absent_with_memo`],
    /// bounded by [`FILE_BLOB_ABSENT_IDENTITY_CAP`].
    ///
    /// A proven-absent fetch clears the memo, so the event counter already
    /// books each loss once rather than once per retry. The distinct set is
    /// what separates "N different blobs are gone" from "one blob keeps being
    /// re-uploaded and lost again" — the second is the accelerating condition.
    file_blob_absent_identities: Arc<Mutex<HashSet<String>>>,
    /// Set once the identity set hits the cap, so the distinct count can be
    /// labelled a floor rather than a total.
    file_blob_absent_identities_capped: AtomicBool,
    /// Optional callback invoked after sync replay writes schemas to Sled.
    /// This lets the SchemaCore cache refresh without a hard dependency.
    schema_reloader: Arc<Mutex<Option<SchemaReloadCallback>>>,
    /// Apply [`crate::sync::log::LogOp::MutationIntent`] on the serving
    /// mutation path with capture suppressed.
    mutation_intent_applier: Arc<Mutex<Option<crate::sync::engine::types::MutationIntentApplier>>>,
    mutation_intent_materializer:
        Arc<Mutex<Option<crate::sync::engine::types::MutationIntentMaterializer>>>,
    /// Serving atom store used only to protect legacy reference-only pin rows
    /// while an automatic GC generation is active.
    pub(crate) automatic_gc_atom_store: Arc<Mutex<Option<crate::db_operations::AtomStore>>>,
    /// Serializes the short transition from marker clear to an active probe.
    pub(crate) automatic_gc_pin_log_barrier: Arc<Mutex<()>>,
    /// True while pin appends must finish before the probe activates markers.
    pub(crate) automatic_gc_pin_log_activation_pending: AtomicBool,
    /// Flush acknowledged resident writes before the compactor enumerates S.
    photograph_cut_barrier: Arc<Mutex<Option<PhotographCutBarrier>>>,
    /// Refresh store addressing after S installs durable layout markers.
    photograph_restore_barrier: Arc<Mutex<Option<PhotographCutBarrier>>>,
    /// Optional callback invoked after sync replay writes native_index entries to Sled.
    /// This lets the EmbeddingIndex refresh without a hard dependency.
    embedding_reloader: Arc<Mutex<Option<EmbeddingReloadCallback>>>,
    /// Optional callback to refresh authentication credentials on 401.
    /// When set, the sync engine will call this on `SyncError::Auth`, update
    /// the `AuthClient`, and retry the sync cycle once before giving up.
    auth_refresh: Option<AuthRefreshCallback>,
    /// Wake handle notified by `record_op` whenever a new local write appends
    /// to the pending queue. The background sync coordinator races its next
    /// sleep against this notification so a write can trigger a near-immediate
    /// flush instead of waiting the full `sync_interval_ms`. A `Notify` holds
    /// at most one pending notification across multiple writes — concurrent
    /// writes coalesce into the next sync cycle naturally.
    pub(crate) wake: Arc<tokio::sync::Notify>,
    /// Node signing keypair, shared with `MutationManager`. Used to sign
    /// merge results during sync replay so a peer can attribute the
    /// merged write to this node's identity instead of an ephemeral keypair.
    node_signer: Arc<Ed25519KeyPair>,
    /// Optional 32-byte E2E content key for download cursor payloads.
    ///
    /// Production wires the engine to the encrypted store view, so cursor rows
    /// also pass through the local at-rest seam. This key adds the portable
    /// E2E envelope used before that local seam, preserving dual-read support
    /// for older cursor rows. `None` (keyless tests) stores raw cursor bytes.
    enc_key: Option<[u8; 32]>,
    /// Accumulated serialized byte size of personal log entries uploaded since
    /// the last successful compaction. Drives the SIZE-based compaction trigger
    /// ([`SyncConfig::compaction_log_ratio`]): once this reaches
    /// `ratio * last_snapshot_bytes` we re-snapshot. Reset to 0 after a
    /// successful `compact()`.
    bytes_since_snapshot: Arc<Mutex<u64>>,
    /// Count of personal log entries uploaded since the last successful
    /// compaction. Feeds the final entry-count backstop in [`should_compact`]
    /// (and is the ONLY trigger available before this process's first snapshot,
    /// when there is no snapshot size to evaluate the ratio against). Reset to 0
    /// after a successful `compact()`.
    entries_since_snapshot: Arc<Mutex<u64>>,
    /// Byte size of the most recent snapshot (≈ the DB size at that point), the
    /// denominator of the size trigger. `0` until the first compaction this
    /// process performs — while `0` the size trigger can't fire (there's no
    /// snapshot to compare against), so a brand-new node falls back to the
    /// entry-count backstop / time bounds for its very first snapshot.
    last_snapshot_bytes: Arc<Mutex<u64>>,
    /// Unix timestamp (seconds) of the last successful compaction, for the
    /// min/max interval bounds. `None` until this process compacts once.
    last_snapshot_at: Arc<Mutex<Option<u64>>>,
    /// Retry gate for the automatic personal compaction paths in `cycle`.
    /// See [`CompactionFailureBackoff`].
    personal_compaction_backoff: Arc<Mutex<CompactionFailureBackoff>>,
    /// Observability for the most recent steady-state download cycle.
    last_download_stats: Arc<Mutex<Option<DownloadCycleStats>>>,
    /// Observability for the most recent steady-state upload selection.
    last_upload_stats: Arc<Mutex<Option<UploadCycleStats>>>,
    /// Adaptive upload policy runtime (EWMA + last snapshot). Caps for the
    /// current cycle live in `cycle_upload_caps`.
    upload_policy: Arc<upload_policy::UploadPolicyRuntime>,
    /// Latest foreground pressure sample supplied by the embedding node.
    ///
    /// Core stays dependency-free: the node translates its QoS/request telemetry
    /// into this small value and refresh_upload_policy consumes it opportunistically.
    foreground_pressure: Arc<std::sync::Mutex<Option<upload_policy::ForegroundPressure>>>,
    /// Where to mirror the keep set (the manifest a publish committed) so an
    /// operator verb can read it without being present when the cut landed.
    ///
    /// `lastdb cloud backup-gc` refuses to select orphans without a keep set,
    /// and the only writer used to be the operator snapshot route — a caller
    /// that has to run inside the window right after a drain finishes. A cut
    /// takes hours on a large home, nothing schedules that verb, and the
    /// continuous publisher lands cuts on its own, so the file simply never
    /// appeared and the cloud namespace accumulated orphans forever.
    ///
    /// Core stays filesystem-policy-free: the node owns the location and
    /// installs it with [`SyncEngine::set_backup_manifest_cache_path`].
    backup_manifest_cache_path: Arc<std::sync::Mutex<Option<std::path::PathBuf>>>,
    /// Caps active for the in-flight (or last prepared) sync cycle.
    cycle_upload_caps: Arc<Mutex<UploadPolicySnapshot>>,
    /// Process-local owner override for sealed-home backup PUT concurrency.
    /// The environment override remains authoritative when present.
    backup_upload_concurrency_override: Arc<Mutex<Option<usize>>>,
    /// LastStore source used by the manifest/chunk backup uploader. Present
    /// only for LastStore-backed cloud homes.
    laststore_backup_source: Option<Arc<crate::storage::LastStoreNamespacedStore>>,
    /// Ensures a serving engine starts at most one uploader thread.
    laststore_backup_uploader_started: AtomicBool,
    /// Set by boot-error cleanup and Drop so the std thread exits its loop.
    laststore_backup_uploader_stop: AtomicBool,
    /// Sha256 digests already confirmed present in cloud backup this process.
    /// Lets the continuous drain skip re-presigning a saturated head and walk
    /// to still-missing sealed segs without re-probing the same units every cycle.
    backup_known_present: Arc<Mutex<HashSet<String>>>,
    /// Ensures the durable CAS presence sidecar is read at most once per engine.
    backup_known_present_loaded: AtomicBool,
    /// Last successful `backup/chunks/` listing used to seed presence.
    ///
    /// Catch-up re-lists when remaining ≥ 64 so an out-of-band fill can collapse
    /// a stale-negative cache. The operator snapshot path and the continuous
    /// publisher both call that reseed, so without a cooldown a progressing
    /// drain lists the whole prefix twice a cycle (measured 2026-08-20: 8–30s
    /// of listing before each 256-PUT catch-up cycle; 7.3 GiB still missed
    /// 10 minutes after the PUT budget itself was fixed).
    backup_presence_listed_at: Arc<Mutex<Option<Instant>>>,
    /// Chunks of the held cut whose local sealed file is gone, so no retry can
    /// upload them. Deliberately NOT persisted: it is a judgement about one
    /// frozen cut, and a restart re-derives it in a cycle.
    backup_unresolvable: Arc<Mutex<backup_uploader::BackupUnresolvableChunks>>,
    /// The cut this publisher is currently trying to land, held across attempts.
    ///
    /// A busy home reseals sealed chunks in place, so a cut re-derived from the
    /// live store every attempt is a moving target: uploaded bytes stop counting
    /// as soon as their sha rotates, and `chunks_present` random-walks instead of
    /// converging. Cutting **once** and holding a packing lock (compaction/reseal
    /// of sealed files off) makes the denominator immutable, so the drain is a
    /// countdown.
    backup_publish_target: Arc<Mutex<Option<backup_uploader::BackupPublishTarget>>>,
    /// Serializes the complete backup publish lifecycle across operator and
    /// continuous callers.
    ///
    /// The target mutex protects only the in-memory slot. A publish turn spans
    /// `ensure -> drain -> CAS -> retire`, including network waits. Without
    /// this outer lock, one caller can retire the shared target while another
    /// caller verifies it for CAS.
    backup_publish_turn: Arc<Mutex<()>>,
    /// Exact identity of the latest manifest this process committed locally.
    ///
    /// Publishers update it after successful cloud CAS plus local commit and
    /// before they release `backup_publish_turn`. It starts unknown after each
    /// process start; orphan GC never reconstructs it from a caller manifest or
    /// the counter-only durable high-water marker.
    backup_published_tip_identity: Arc<Mutex<Option<backup_uploader::BackupTipIdentity>>>,
    /// Set when Cloud Sync Off revokes GC identity. Stays set after On until
    /// this process CASes and commits a new tip. Restart leaves it false, so
    /// admin GC can use a verified published body after a clean process start.
    backup_gc_identity_revoked: AtomicBool,
    /// How many consecutive publish targets died to reseal (`source_missing > 0`)
    /// without a successful CAS between them. Reset on land. Bounds automatic
    /// re-cut so a home that cannot outrun reseal says so rather than spinning
    /// (see `MAX_CONSECUTIVE_RESEAL_KILLED_CUTS`).
    backup_consecutive_reseal_kills: AtomicU64,
    /// Manifest-only chunks of the most recently assessed cut that have no
    /// local candidate — the count computed at cut time and re-computed after
    /// every unbackable retirement.
    ///
    /// Mirrored OFF the held `BackupPublishTarget` rather than read from it at
    /// report time, because the cycle that matters is the one that ABANDONS the
    /// cut: `retire_backup_publish_target()` runs before the cycle ends, so a
    /// reader that locks the target sees `None` and reports 0 on exactly the
    /// cycle whose count is the whole story. Semantics are "the last cut we
    /// assessed named N"; a fresh cut overwrites it, a successful land clears
    /// it to 0.
    backup_unbackable_manifest_chunks: AtomicU64,
    /// Incomplete sealed-chunk backup progress (percent / ETA) for status.
    /// Updated by the continuous publisher; never blocks local R/W.
    backup_progress: Arc<std::sync::Mutex<crate::backup_progress::BackupProgressTracker>>,
    /// Latest tip manifest waiting for post-CAS auto orphan GC, paired with the
    /// generation that enqueued it.
    ///
    /// Replaced (not appended) on each successful `backup/latest` CAS so default
    /// retention is **latest tip only**. The continuous uploader drains this
    /// on a detached task so local Mini R/W and the drain loop stay unblocked.
    ///
    /// The generation is also mirrored in [`Self::post_cas_backup_gc_generation`]
    /// so an in-flight GC that already `take()`d tip N can observe that tip N+1
    /// landed and abort before DELETEing chunks exclusive to the newer tip.
    post_cas_backup_gc_tip: Arc<Mutex<Option<backup_uploader::PendingBackupGc>>>,
    /// Monotonic post-CAS GC generation. Bumped on every enqueue; an in-flight
    /// GC aborts when `load() > my_generation` before it issues any DELETE.
    post_cas_backup_gc_generation: AtomicU64,
    /// Single-flight lock for backup orphan GC (quota recovery, post-CAS auto,
    /// and manual admin). Prevents concurrent DELETE sweeps that each built a
    /// different keep-set snapshot (e.g. empty-published quota path racing a
    /// post-CAS keep for tip N).
    backup_orphan_gc_mutex: Arc<Mutex<()>>,
    backup_gc_jobs: gc_jobs::GcJobManager,
    /// Last computed cloud backup footprint (referenced / billed / reclaimable).
    ///
    /// Filled by orphan GC and explicit refresh so `/api/status` never pays for
    /// a full `list_objects` on the request path. `std::sync::Mutex` matches
    /// `backup_progress` so status can read without awaiting.
    backup_storage_footprint:
        Arc<std::sync::Mutex<Option<crate::storage::laststore::BackupStorageFootprint>>>,
    /// Staged-but-unacked capture keys (design C11). Never gates local writes.
    pub(crate) capture_pending: Arc<Mutex<crate::sync::capture::pending::PendingMap>>,
    /// Durable dirty-key markers awaiting bounded point-read re-export after a
    /// post-commit mutation-log capture failure.
    pub(crate) capture_reexport_pending_count: AtomicU64,
    /// Versioned marker presence: unknown at boot, nonempty after an observed
    /// row, empty only after a full physical lap with no intervening stage.
    pub(crate) capture_reexport_presence: AtomicU64,
    /// One marker drain at a time. A cloud cycle and a local drain worker may
    /// run concurrently, but they must not append the same marker twice.
    pub(crate) capture_reexport_drain_lock: Mutex<()>,
    /// Physical page position. Advancing past a failed marker lets later
    /// markers progress; a completed lap wraps and retries failures.
    pub(crate) capture_reexport_scan_cursor:
        Mutex<Option<crate::storage::traits::PhysicalScanCursor>>,
    pub(crate) capture_reexport_scan_lap_version: Mutex<u64>,
    pub(crate) capture_reexport_scan_lap_saw_rows: Mutex<bool>,
    /// Capture/re-export failures observed in this engine process.
    pub(crate) capture_reexport_failure_count: AtomicU64,
    /// Undecodable re-export markers dropped from the drain plane.
    ///
    /// A marker whose JSON body — or whose inner encoded key list — cannot be
    /// decoded is *deterministically* poison: the same bytes fail the same way
    /// on every tick, and no consumer can ever read the namespace/keys it was
    /// supposed to name. Leaving it in place pinned a slot in the fixed-size
    /// drain page forever, so enough of them wedged the whole queue. The tick
    /// drops such a marker and counts it here. A nonzero value is durable
    /// evidence that some dirty keys were never re-exported.
    pub(crate) capture_reexport_poison_dropped_count: AtomicU64,
    /// Successful leftover physical capture records, split so catalog traffic
    /// cannot masquerade as product-path fallback.
    pub(crate) capture_physical_fallback_records: AtomicU64,
    pub(crate) capture_physical_catalog_records: AtomicU64,
    /// Process-lifetime capture queue phase totals. These counters keep the
    /// worker cost visible after the request task-local phase ends.
    pub(crate) capture_queue_jobs: AtomicU64,
    /// Writes refused because the queue stayed full for the whole
    /// admission window. This is the rate an operator needs: the
    /// refusal is transient and no longer raises one ERROR per write,
    /// so a counter is the only place the volume is visible.
    pub(crate) capture_queue_rejections: AtomicU64,
    pub(crate) capture_queue_admission_us: AtomicU64,
    pub(crate) capture_queue_delay_us: AtomicU64,
    /// Panics caught while processing a capture job. The queue worker keeps
    /// draining after a panic (see `run_capture_queue`), so this counter is
    /// the only durable signal that a capture job's write never actually
    /// reached the mutation log — a healthy process should always read 0.
    pub(crate) capture_queue_worker_panics: AtomicU64,
    pub(crate) capture_stage_us: AtomicU64,
    pub(crate) capture_record_us: AtomicU64,
    pub(crate) capture_cleanup_us: AtomicU64,
    /// Peer writer-scoped mutation-log segments this process applied on the
    /// regular `do_sync` cycle (never `lastdb restore --into`).
    ///
    /// Uploads already have `segments_uploaded`. Without the download-side
    /// counterpart, a live Mini that downloads nothing and a live Mini whose
    /// peer-apply cycle never ran read identically on `lastdb status`: both
    /// show a moving F and no evidence either way. This counter is the
    /// difference, so an operator can prove the live-Mini apply path ran
    /// without reading logs or restoring into a second home.
    pub(crate) mutation_log_peer_segments_applied: AtomicU64,
    /// Records applied out of [`Self::mutation_log_peer_segments_applied`].
    /// Segments answer "did the path run"; records answer "did it carry work".
    pub(crate) mutation_log_peer_records_applied: AtomicU64,
    /// Unix milliseconds of the last `do_sync` peer-apply attempt this process.
    /// `0` means this process has not run peer apply yet, so the drain skip
    /// must not starve the first attempt. Auth errors leave this at the prior
    /// value so `sync`'s refresh retry still runs peer apply with the new token.
    pub(crate) mutation_log_last_peer_apply_ms: AtomicU64,
    /// Unix milliseconds of the last successful mutation-log append in this
    /// process. `0` means this process has not appended, so a restart drain
    /// uploads immediately instead of opening a coalesce hold.
    pub(crate) mutation_log_last_append_ms: AtomicU64,
    /// Unix milliseconds when the current coalesce hold started. `0` means
    /// no hold is open. Cleared when the held records upload.
    pub(crate) mutation_log_hold_since_ms: AtomicU64,
    /// Milliseconds the background sync loop should wait before the next
    /// cycle when the upload was held to group changes. `0` means use the
    /// normal sync interval. A write wake still aborts the wait.
    pub(crate) mutation_log_coalesce_retry_ms: AtomicU64,
    /// Every automatic plane self-compaction trigger, probe timestamp, and
    /// last-fire report. Held behind an `Arc` because the identical cadence
    /// runs on a node with no engine at all — see
    /// [`crate::sync::capture::PlaneCompactor`]. It shares this engine's
    /// `backup_publish_target` and `cloud_sync_disabled_at`, so a rewrite still
    /// serializes against a backup cut and still stamps a cloud pause where a
    /// plane requires isolation.
    pub(crate) compaction: Arc<crate::sync::capture::PlaneCompactor>,
    /// Per-collection disk-usage cache for [`SyncEngine::status`].
    ///
    /// Status never walks collection directories on the request path. A cold
    /// read returns `None` and kicks one background `dir_size_bytes`; a warm
    /// read is a mutex snapshot. Compaction workers still call
    /// `collection_disk_usage` directly when they probe a trigger.
    pub(crate) status_disk_usage_cache: Arc<StatusDiskUsageCache>,
    /// Most recent capture/re-export failure, distinct from ordinary log lag.
    pub(crate) last_capture_reexport_error: Arc<Mutex<Option<String>>>,
    /// Unix timestamp (seconds) when Cloud Sync was intentionally disabled /
    /// paused. `None` means sync is on. Used with
    /// [`SyncConfig::sync_off_grace_secs`] for status honesty and (sibling
    /// cards) stop-staging / re-enable strategy. Never blocks local R/W.
    pub(crate) cloud_sync_disabled_at: Arc<Mutex<Option<u64>>>,
    /// Per-sync-target pin-mode durable append log state. This is intentionally
    /// target-keyed so a personal pin cannot absorb org mutations, and an org
    /// pin cannot spill into personal cloud durability.
    pub(crate) pin_log: pin_log::PinLog,
    /// Process-local continuous mutation-log plane (`log/{writer_id}/{seq}` + F).
    /// Production will map puts onto the account prefix; unit/CoW harnesses
    /// and do_sync share this plane so published F is never advanced without a
    /// corresponding sealed segment object.
    pub(crate) mutation_log_plane: Arc<Mutex<pin_log::MutationLogLocalCloud>>,
    /// Current process-plane frontier, readable while cloud upload holds the
    /// plane mutex. Shares the canonical map, not a periodically refreshed copy.
    pub(crate) mutation_log_frontier: Arc<pin_log::MutationLogFrontierSnapshot>,
    /// Unix timestamp (seconds) of the last durable-outbox overflow snapshot
    /// attempt (success or failure). Gates
    /// `maybe_force_snapshot_for_outbox_overflow`'s retry cooldown so a
    /// persistent cloud failure cannot turn every sync cycle into a full-DB
    /// snapshot attempt. `None` until the first attempt this process.
    pub(crate) outbox_overflow_last_attempt_at: Arc<Mutex<Option<u64>>>,
    /// Trigger reason (`"outbox_overflow_depth"` / `"outbox_overflow_age"`) of
    /// the most recent durable-outbox overflow snapshot attempt, surfaced on
    /// [`SyncStatus`] for operator visibility. `None` until the first attempt.
    pub(crate) last_outbox_overflow_reason: Arc<Mutex<Option<String>>>,
    /// Count of oversize durable-outbox entries this process permanently
    /// dropped to keep upload catch-up moving.
    oversize_outbox_drop_count: AtomicU64,
    /// Lowest durable-outbox seq deferred by the **adaptive** cycle budget.
    ///
    /// `0` means nothing is deferred. While this is set, no seq at or above it
    /// may join the in-memory upload queue. Personal upload keys are the
    /// client-assigned `entry.seq`, so uploading a later row while an older row
    /// is still durable-only lets a peer that lists `seq > cursor` advance past
    /// the deferred head; the older row would then never be listed again.
    /// `schedule_outbox_entries` recomputes this every cycle, so a cycle with a
    /// larger adaptive budget clears it.
    adaptive_deferred_head_seq: AtomicU64,
    /// Most recent oversize durable-outbox drop, surfaced on [`SyncStatus`] so
    /// operators do not have to recover permanent drops from logs.
    last_oversize_outbox_drop: Arc<Mutex<Option<OversizeOutboxDropStatus>>>,
}

impl Drop for SyncEngine {
    fn drop(&mut self) {
        // Scrub the cached at-rest key from memory on drop (Gap G3).
        use zeroize::Zeroize as _;
        if let Some(key) = self.enc_key.as_mut() {
            key.zeroize();
        }
    }
}
