//! Retained sealed-chunk backup status models and operator formatting.
//!
//! The retired uploader's mutable ETA/progress tracker has no consumers.
//! These passive snapshots remain for status and CLI compatibility; active
//! V2 byte-backup progress is reported separately by the node.

mod format;
pub use format::format_backup_progress_line;
use serde::Serialize;

/// Snapshot exposed to status consumers (JSON + CLI).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct BackupProgressSnapshot {
    /// True when a LastStore backup uploader is active for this home.
    pub enabled: bool,
    /// True when sealed backup-eligible work is fully present in cloud.
    pub complete: bool,
    /// Percent of sealed backup-eligible chunks already in cloud (0–100).
    /// `None` when total is unknown / zero (no candidates yet).
    pub percent: Option<f64>,
    pub chunks_total: u64,
    pub chunks_present: u64,
    pub chunks_remaining: u64,
    /// Estimated remaining payload bytes (from candidate sizes when known).
    pub bytes_remaining: Option<u64>,
    /// Seconds since the current incomplete session started.
    pub elapsed_secs: Option<u64>,
    /// Estimated seconds to finish at EWMA rate.
    pub eta_secs: Option<u64>,
    /// EWMA of recent upload throughput (bytes per second of *whole cycle*).
    ///
    /// Retained for continuity, but note this is not the link rate: on a large
    /// home most of a cycle is the walk over already-present candidates, so
    /// this number is dominated by walk time and by how big the recently
    /// drained chunks happened to be. Use `ewma_link_bps` to reason about the
    /// link; this one is only meaningful as "bytes per cycle-second".
    pub ewma_upload_bps: Option<f64>,
    /// EWMA of the true link rate: bytes per second *of transfer*. This is the
    /// rate `eta_secs` charges remaining bytes at.
    pub ewma_link_bps: Option<f64>,
    /// EWMA of per-cycle overhead in seconds (cycle wall time minus transfer
    /// wall time) — the walk. `eta_secs` charges this once per remaining cycle.
    pub ewma_overhead_secs: Option<f64>,
    pub last_cycle_uploaded: u64,
    pub last_cycle_already_present: u64,
    pub last_cycle_bytes_uploaded: u64,
    /// Chunks whose presign/upload/confirm failed in the last drain cycle.
    /// Distinct from `consecutive_failures` (whole-cycle publish failures):
    /// this is partial PUT loss that used to leave zero operator signal.
    #[serde(default)]
    pub last_cycle_failed: u64,
    /// Chunks of the current cut that have no local sealed file, so the cut can
    /// never reach 100% present and can never publish.
    ///
    /// Non-zero is terminal for this generation: it means the home is not
    /// getting more restorable no matter how long the drain runs, and the cut
    /// has to be replaced. Distinguishes "the backup is slow" from "the backup
    /// is over", which `chunks_remaining` alone cannot.
    #[serde(default)]
    pub chunks_source_missing: u64,
    /// Chunks NAMED BY THE CUT'S MANIFEST that have no local candidate to
    /// upload them from — carried forward from an earlier manifest and since
    /// resealed away. Terminal for this generation exactly like
    /// `chunks_source_missing`, and reached by the opposite route.
    ///
    /// The two populations are disjoint by construction: `cut_backup_manifest`
    /// walks the store UNION the previous manifest's `atom_chunks`, while
    /// candidate enumeration walks the store alone, so the manifest universe is
    /// strictly larger. A candidate whose frozen file vanished lands in
    /// `chunks_source_missing`; a manifest-only ref lands here. Both make the
    /// cut unable to reach 100% present, and only the first used to have a
    /// number — the second was reachable only by parsing English out of
    /// `last_error`, while `complete`, `percent` and `chunks_remaining` all
    /// argued the cut had finished.
    #[serde(default)]
    pub chunks_unbackable_manifest: u64,
    pub cas_counter: Option<u64>,
    pub phase: String,
    /// Whether the human CLI should print a progress line (incomplete only).
    pub show_progress: bool,
    /// Unix seconds of the last cycle that completed without error, if any in
    /// this process. `None` means the uploader has never gotten a cycle
    /// through — which is NOT the same as "nothing to do".
    pub last_success_unix: Option<u64>,
    /// Seconds since `last_success_unix`. `None` when no cycle has succeeded.
    pub last_success_age_secs: Option<u64>,
    /// Consecutive failed publish cycles since the last success. Non-zero means
    /// backup is actively failing even though local reads/writes are fine.
    pub consecutive_failures: u32,
    /// Redacted text of the most recent cycle failure, if any.
    pub last_error: Option<String>,
    /// Chunks that entered the present set since this session started.
    ///
    /// Reported next to `chunks_erased` because `chunks_present` alone cannot
    /// distinguish "uploading nothing" from "uploading hard and losing it to
    /// churn". On the 2026-07-31 primary those were 234 gained against 253
    /// erased in 14 minutes, while the bar sat at ~56% quoting "ETA ~56m".
    pub chunks_gained: u64,
    /// Chunks that LEFT the present set since this session started (sealed-chunk
    /// reseal rotating a sha out of the candidate set). Non-zero means uploaded
    /// bytes are being invalidated; a run with `chunks_erased >= chunks_gained`
    /// is not converging, whatever the percentage says.
    pub chunks_erased: u64,
    /// False when the backup is not currently gaining ground, judged over the
    /// last `net_progress_window_cycles` cycles rather than the whole session.
    /// An ETA is only meaningful when this is true, so `eta_secs` is `None`
    /// otherwise.
    ///
    /// Deliberately NOT `chunks_gained > chunks_erased`: those are session
    /// totals, and a session that banked a large early gain keeps satisfying
    /// that comparison through hours of pure loss. The window length is carried
    /// by `net_progress_window_cycles`.
    pub net_progressing: bool,
    /// Chunks gained within the recent window that decides `net_progressing`.
    #[serde(default)]
    pub recent_gained: u64,
    /// Chunks erased within the recent window that decides `net_progressing`.
    /// `recent_erased > recent_gained` is the live "losing ground" condition;
    /// the session totals cannot express it.
    #[serde(default)]
    pub recent_erased: u64,
    /// How many cycles that window spans, so a reader can size the two above.
    #[serde(default)]
    pub net_progress_window_cycles: u64,
    /// Cycles observed since the last one that gained ground. Zero while
    /// gaining. This is the plainest form of the question an operator asks —
    /// "how long has it been going backwards" — and unlike the window totals it
    /// keeps counting past the window's edge.
    #[serde(default)]
    pub cycles_since_net_gain: u64,
    /// `manifest.counter` of the cut currently being drained, when there is one.
    pub target_generation: Option<u64>,
    /// Unix seconds of the last successful CAS — the last time a backup actually
    /// became restorable. Distinct from `last_success_unix`, which only means a
    /// drain cycle round-tripped; conflating the two is what let a 10-day-old
    /// backup read green.
    pub last_publish_unix: Option<u64>,
    /// Seconds since `last_publish_unix`. `None` when nothing ever landed.
    pub last_publish_age_secs: Option<u64>,
}
