use super::*;

/// Chunks of the held cut whose local sealed file is gone.
///
/// Scoped to the generation they were observed against: a fresh cut re-plans
/// against the current sealed set, so its shas must start unjudged. Keeping
/// the generation here rather than clearing at the cut site means a stale
/// entry can never survive into a cut that did not produce it.
#[derive(Debug, Default)]
pub struct BackupUnresolvableChunks {
    pub(super) generation: Option<u64>,
    pub(super) shas: HashSet<String>,
}

impl BackupUnresolvableChunks {
    /// Drop everything remembered about a previous cut when the generation
    /// moves. Returns the set scoped to `generation`.
    pub(super) fn for_generation(&mut self, generation: Option<u64>) -> &mut HashSet<String> {
        if self.generation != generation {
            self.generation = generation;
            self.shas.clear();
        }
        &mut self.shas
    }
}

/// Max consecutive cuts that may die to reseal before automatic re-cut stops.
///
/// Three is enough to prove the home cannot outrun reseal without recreating
/// the 2026-07-31 livelock (23/23 incomplete attempts over 5h53m). The defect
/// then is the reseal rate, not the cut policy — see
/// `papercut-lastdb-primary-reseals-72-sealed-chunks-per-minute-on-a-quiet-node`.
pub(crate) const MAX_CONSECUTIVE_RESEAL_KILLED_CUTS: u32 = 3;

/// Operator-facing reason a demoted home has no publishable sealed base.
///
/// Shared with the restart path: the abandon happens once, in one process, and
/// the durable marker is all a later process has to re-derive it from. Both
/// spellings of the state have to name the same required action or the two
/// reads of `/api/status` disagree about the same home.
pub(crate) const SEALED_BASE_ABANDONED_STATUS_ERROR: &str =
    "backup cut abandoned: sealed-home base is unpublishable (chunks have no local sealed file \
     and are not in cloud); continuous sealed-home is demoted so automatic re-cut is disabled; \
     mutation-log plane is active durability; operator re-cut / bootstrap required for a new \
     sealed-home base";

/// Decision for a drain that observed proven unpublishability (`source_missing > 0`).
///
/// Settled 2026-08-06: abandon only on proven unpublishability (not age /
/// staleness / failed-attempt count), require the replacement to name the
/// post-reseal sealed set, and bound consecutive reseal-killed cuts so
/// re-cut cannot become the re-planning livelock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UnpublishableCutAction {
    /// Still under the bound: abandon the pinned cut so the next ensure re-cuts.
    AbandonAndRecut { consecutive_after: u32 },
    /// Bound hit: do not re-cut; surface reseal rate as the defect.
    StopRecutting { consecutive: u32 },
}

/// Decide whether a proven-unpublishable cut may be abandoned for a replacement.
///
/// `consecutive_so_far` is how many cuts have already died to reseal *before*
/// this one (0 on a healthy publisher). Call only when `source_missing > 0`.
#[must_use]
pub(crate) fn decide_unpublishable_cut_action(
    consecutive_so_far: u32,
    max_consecutive: u32,
) -> UnpublishableCutAction {
    let consecutive_after = consecutive_so_far.saturating_add(1);
    let max = max_consecutive.max(1);
    if consecutive_after >= max {
        UnpublishableCutAction::StopRecutting {
            consecutive: consecutive_after,
        }
    } else {
        UnpublishableCutAction::AbandonAndRecut { consecutive_after }
    }
}

/// The cut this publisher is currently trying to land, held across attempts.
///
/// Cutting a fresh manifest every publish attempt is what makes a busy home
/// non-convergent: LastStore reseals sealed chunks in place, so each new cut
/// names shas that did not exist at the previous cut, and every byte already
/// uploaded for the rotated-out sha stops counting toward the commit condition
/// (publish CASes only at 100% of manifest chunks present). Measured on the
/// 2026-07-31 primary: 23/23 attempts `backup snapshot incomplete`, `missing`
/// oscillating 7,114-7,818 with no trend over 5h53m.
///
/// A target is cut **once** and then kept until it either lands or is
/// explicitly abandoned. While it is held, sealed-file rewrite is off (the
/// packing lock), so candidate shas cannot rotate. `chunks_present` against a
/// target is monotonically non-decreasing and the deficit is a countdown
/// rather than a random walk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BackupTipIdentity {
    pub(super) store_uuid: String,
    pub(super) epoch: u64,
    pub(super) counter: u64,
    pub(super) manifest_sha256: String,
}

impl BackupTipIdentity {
    pub(super) fn from_manifest(manifest: &BackupManifest) -> SyncResult<Self> {
        let manifest_sha256 = manifest_sha256_hex(manifest)
            .map_err(|e| SyncError::Storage(format!("hash backup manifest identity: {e}")))?;
        Ok(Self {
            store_uuid: manifest.store_uuid.clone(),
            epoch: manifest.epoch,
            counter: manifest.counter,
            manifest_sha256,
        })
    }
}

/// Exact chunk reachability of one held publish target.
///
/// The manifest counter is not enough: retirement and named-hole repair can
/// replace a target in place without changing its counter. The shared set is
/// rebuilt only when a target changes, so each orphan DELETE can compare an
/// exact snapshot without rewalking a large manifest.
#[derive(Debug, Clone)]
pub(super) struct BackupPublishTargetReachabilityIdentity {
    pub(super) store_uuid: String,
    pub(super) epoch: u64,
    pub(super) counter: u64,
    pub(super) referenced_shas: Arc<BTreeSet<String>>,
}

impl PartialEq for BackupPublishTargetReachabilityIdentity {
    fn eq(&self, other: &Self) -> bool {
        self.store_uuid == other.store_uuid
            && self.epoch == other.epoch
            && self.counter == other.counter
            && (Arc::ptr_eq(&self.referenced_shas, &other.referenced_shas)
                || self.referenced_shas == other.referenced_shas)
    }
}

impl Eq for BackupPublishTargetReachabilityIdentity {}

pub(crate) struct BackupPublishTarget {
    pub(super) manifest: BackupManifest,
    /// Live sealed-file paths under the packing lock. Digests are immutable
    /// because compaction/reseal of sealed files is skipped while this target
    /// is held. Paths are the store's own files, not a freeze-dir clone.
    pub(super) candidates: Vec<BackupChunkUploadCandidate>,
    /// Exact union of manifest references and candidate digests.
    pub(super) reachability_identity: BackupPublishTargetReachabilityIdentity,
    /// `manifest.counter` — identifies this target in progress samples so the
    /// tracker knows a denominator change is a legitimate new cut, not churn.
    pub(super) generation: u64,
    /// Manifest chunk digests with no candidate to upload them from.
    ///
    /// `cut_backup_manifest` unions the store walk with
    /// `previous_manifest.atom_chunks`, which it carries forward unconditionally
    /// so a restore never narrows. The candidate enumeration is the walk ALONE.
    /// A carried-forward atom chunk that reseal/compaction has since removed
    /// from disk is therefore named by the manifest and absent from the
    /// candidate list — and the walk *is* the complete on-disk enumeration, so
    /// there is no file to upload it from either.
    ///
    /// While such a digest is still in the bucket from an earlier generation the
    /// cut publishes fine. Once one goes missing in cloud, CAS verify reports a
    /// shortfall the drain can never close: it uploads only candidates, and this
    /// digest is not one. The result is a cut that drains to 100 % and then
    /// re-fails CAS with a fixed count forever (2026-08-01: 871; 2026-08-06:
    /// 156, holding backup durability DEGRADED past 24 h).
    ///
    /// Measured on the primary's cached manifest: 1024 of 1026 carried-forward
    /// atom refs were outside the candidate set. Recording the set at cut time
    /// is what turns that silent livelock into a named condition.
    pub(super) unbackable_manifest_chunks: usize,
}

impl BackupPublishTarget {
    pub(super) fn new(
        manifest: BackupManifest,
        candidates: Vec<BackupChunkUploadCandidate>,
        unbackable_manifest_chunks: usize,
    ) -> Self {
        let generation = manifest.counter;
        let reachability_identity = Self::reachability_identity(&manifest, &candidates);
        Self {
            manifest,
            candidates,
            reachability_identity,
            generation,
            unbackable_manifest_chunks,
        }
    }

    pub(super) fn reachability_identity(
        manifest: &BackupManifest,
        candidates: &[BackupChunkUploadCandidate],
    ) -> BackupPublishTargetReachabilityIdentity {
        let mut referenced_shas = manifest_referenced_chunk_shas(manifest);
        referenced_shas.extend(
            candidates
                .iter()
                .map(|candidate| candidate.chunk.sha256.clone()),
        );
        BackupPublishTargetReachabilityIdentity {
            store_uuid: manifest.store_uuid.clone(),
            epoch: manifest.epoch,
            counter: manifest.counter,
            referenced_shas: Arc::new(referenced_shas),
        }
    }

    pub(super) fn refresh_reachability_identity(&mut self) {
        self.reachability_identity = Self::reachability_identity(&self.manifest, &self.candidates);
    }

    pub(super) fn total(&self) -> usize {
        self.candidates.len()
    }
}
