use super::*;

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct LastStoreBackupUploadStats {
    pub candidates: usize,
    pub selected: usize,
    pub uploaded: usize,
    pub already_present: usize,
    pub bytes_uploaded: u64,
    /// After the cycle: how many candidates are known-present in cloud.
    #[serde(default)]
    pub chunks_present: usize,
    /// Sum of bytes for candidates still not present (estimate of remaining work).
    #[serde(default)]
    pub bytes_remaining: u64,
    /// Candidates whose presign/upload/confirm failed this cycle. A single
    /// failed chunk no longer aborts the whole drain (2026-07-30: a 10%
    /// server-side presign poison aborted ~80% of 16-put cycles).
    #[serde(default)]
    pub failed: usize,
    /// Candidates of this cut whose local sealed file is gone, accumulated over
    /// the life of the cut rather than the cycle.
    ///
    /// Non-zero after cloud-presence heal means the cut **cannot** publish:
    /// CAS requires every manifest digest in the object store, and these digests
    /// are absent both locally (no sealed file to upload) and in cloud. Digests
    /// that already landed in a prior generation are **not** counted here once
    /// `resolve_source_missing_via_cloud_presence` promotes them into
    /// `backup_known_present`. Residual non-zero is the signal to abandon (and
    /// re-cut when allowed) rather than spin.
    /// Counted separately from `failed` because a failed PUT is worth retrying
    /// and an absent source is not. Named for the cause rather than reusing
    /// "unresolvable", which the verify path already spends on a different
    /// condition (a chunk present but unverifiable).
    #[serde(default)]
    pub source_missing: usize,
    /// First error text from this cycle's PUT fan-out, if any candidate failed.
    ///
    /// Recorded even when the cycle returns `Ok`: not aborting on the first
    /// error is deliberate, but it must not also mean the error text is thrown
    /// away. This is what lets `last_error` name the cause of a drain that lost
    /// every unit of its work.
    #[serde(default)]
    pub first_error: Option<String>,
    /// At least one failed PUT carried the typed cloud quota rejection.
    /// Drives the self-recovery orphan sweep without parsing error strings.
    #[serde(default)]
    pub quota_exceeded: bool,
    /// Wall time this cycle spent actually moving bytes (the parallel PUT fan-out
    /// only), as opposed to the walk over already-present candidates that
    /// surrounds it.
    ///
    /// The two are wildly different on a large home: a cycle that walks ~15k
    /// present chunks to find 16 to upload spends ~55s walking and ~5s
    /// transferring. Dividing `bytes_uploaded` by the *whole* cycle therefore
    /// does not measure the link — it measures the walk. Keeping the transfer
    /// window separate is what lets the ETA charge bytes at the link rate and
    /// per-cycle overhead at the walk rate, instead of conflating them.
    #[serde(default)]
    pub transfer_secs: f64,
}

impl LastStoreBackupUploadStats {
    /// Chunks this cut is *locally known* to be missing from cloud, if any.
    ///
    /// `chunks_present` is counted over the whole candidate list against the
    /// known-present set, so a shortfall is proof the cut cannot publish — no
    /// network round trip can turn it into a complete snapshot this cycle.
    /// `None` means "nothing locally rules it out", which is where the real
    /// per-chunk verify earns its cost.
    #[must_use]
    pub fn known_missing_chunks(&self) -> Option<usize> {
        let missing = self.candidates.checked_sub(self.chunks_present)?;
        (missing > 0).then_some(missing)
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct LastStoreCloudSnapshotReport {
    pub manifest_sha256: String,
    pub manifest_key: String,
    pub latest_key: String,
    pub counter: u64,
    pub cut_csn: u64,
    pub chunks_referenced: usize,
    pub chunks_uploaded: usize,
    pub chunks_already_present: usize,
    pub bytes_uploaded: u64,
    /// Explicit frontier F from the snapshot+log object model (v1 = cut_csn).
    #[serde(default)]
    pub frontier_through: u64,
    /// Object-model CAS payload counter (same as `counter` for backup cuts).
    #[serde(default)]
    pub cas_counter: u64,
    /// Mutation-log segments fully ≤ F after successful CAS (GC-eligible after grace).
    #[serde(default)]
    pub gc_eligible_log_segments: usize,
}

/// Outcome of one candidate's presign/upload/confirm attempt.
pub(super) enum UploadOneOutcome {
    AlreadyPresent,
    Uploaded,
    /// The cycle's wall-clock budget expired before this candidate started.
    /// Not a failure: the cut is held, so the next cycle picks it up again.
    DeadlineReached,
    /// The candidate's local sealed file is gone, so no retry of THIS cut can
    /// upload it. The cut is a packing lock (see `BackupPublishTarget`):
    /// compaction/reseal of sealed files is off while it is held, so a missing
    /// live path is a hole, not concurrent rewrite.
    ///
    /// Distinct from `failed` because the two need opposite handling: a failed
    /// PUT is worth retrying next cycle, and an absent source is not. Retrying
    /// it is what wedges the drain — see the skip in `drain_backup_candidates`.
    SourceMissing,
}

/// One continuous publisher cycle result (chunks + optional snapshot CAS).
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct SnapshotLogPublishCycleReport {
    pub chunks: LastStoreBackupUploadStats,
    pub snapshot_published: bool,
    pub frontier_through: Option<u64>,
    pub cas_counter: Option<u64>,
    pub snapshot_id: Option<String>,
    pub gc_eligible_log_segments: usize,
    pub publish_phase: String,
    /// `manifest.counter` of the sticky target this cycle drained, when there
    /// was one. Lets progress consumers tell "the denominator changed because a
    /// new cut was taken" from "the denominator changed because chunks churned".
    #[serde(default)]
    pub target_generation: Option<u64>,
    /// Full manifest that just CAS-landed, when `snapshot_published` is true.
    /// The continuous uploader keeps this across cycles so the next cut threads
    /// a real previous-manifest into `cut_backup_manifest` / chain validation
    /// (atom-superset / rollback-attack checks) instead of always cutting with
    /// `previous=None`.
    #[serde(skip)]
    pub published_manifest: Option<BackupManifest>,
}

pub(super) fn missing_backup_publish_target_for_cas() -> SyncError {
    SyncError::Storage(
        "backup publish target retired before CAS; retry the snapshot request".to_string(),
    )
}

/// Result of one [`SyncEngine::resolve_source_missing_via_cloud_presence`]
/// pass. `probe_error` distinguishes "the server confirmed these digests are
/// absent" from "at least one presence check could not be completed" — a
/// transport/auth blip must not be treated as the former.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct CloudPresenceHeal {
    pub(super) remaining: usize,
    pub(super) probe_error: bool,
}

/// Result of one [`SyncEngine::probe_missing_manifest_chunk_shas`] pass,
/// split by how "missing" was determined.
///
/// `confirmed_missing` names digests the server affirmatively reported
/// absent (`already_present == false`) — the only set retirement / named-hole
/// logic may act on. `unconfirmed` names digests whose presence could not be
/// checked this pass (transport/auth failure): CAS must still treat them as
/// not-yet-verified-present (so a flaky HEAD cannot wave through a publish),
/// but a flaky HEAD is not evidence the object is gone, so this set must
/// never be retired or turned into a named hole.
#[derive(Debug, Default, Clone)]
pub(super) struct ManifestPresenceProbe {
    pub(super) confirmed_missing: BTreeSet<String>,
    pub(super) unconfirmed: BTreeSet<String>,
}

impl ManifestPresenceProbe {
    pub(super) fn is_empty(&self) -> bool {
        self.confirmed_missing.is_empty() && self.unconfirmed.is_empty()
    }

    pub(super) fn len(&self) -> usize {
        self.confirmed_missing.len() + self.unconfirmed.len()
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = &String> {
        self.confirmed_missing.iter().chain(self.unconfirmed.iter())
    }
}
