use serde::{Deserialize, Serialize};

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
