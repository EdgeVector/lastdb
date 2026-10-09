use super::*;

pub(super) fn is_store_uuid_mismatch_error(err: &SyncError) -> bool {
    let text = err.to_string();
    text.contains("store_uuid_mismatch")
}

/// Server `backup_latest_transition_conflict` reason when the candidate is not
/// strictly ahead of the current pointer under the same store_uuid.
pub(super) fn is_stale_counter_error(err: &SyncError) -> bool {
    err.to_string().contains("stale_counter")
}

/// True when cloud `latest` is exactly the cut we still hold locally.
///
/// Used to break the livelock where CAS already advanced cloud, local
/// `commit_backup_manifest` failed, and the sticky target re-attempts the same
/// (now non-increasing) counter.
/// Raise local high-water to `cloud_counter` before cutting when cloud
/// `latest` is already ahead. Next `reserve_backup_manifest` is then
/// strictly greater, so CAS is not `stale_counter` against that tip.
pub(super) fn cloud_counter_to_observe_before_cut(
    local_high_water: u64,
    cloud_counter: u64,
) -> Option<u64> {
    (cloud_counter > local_high_water).then_some(cloud_counter)
}

/// After `stale_counter`, observe cloud.counter only when the cloud tip is
/// **not** the held cut (heal-as-landed is the other branch) and cloud is
/// at least the held counter (do not rewind).
pub(super) fn cloud_counter_to_observe_after_stale_cas(
    held_counter: u64,
    cloud_counter: u64,
    cloud_matches_held: bool,
) -> Option<u64> {
    if cloud_matches_held {
        return None;
    }
    (cloud_counter >= held_counter).then_some(cloud_counter)
}

pub(super) fn backup_cloud_latest_matches_held_cut(
    cloud_store_uuid: &str,
    cloud_epoch: u64,
    cloud_counter: u64,
    cloud_manifest_sha256: &str,
    manifest: &BackupManifest,
    manifest_sha256: &str,
) -> bool {
    cloud_store_uuid == manifest.store_uuid
        && cloud_epoch == manifest.epoch
        && cloud_counter == manifest.counter
        && cloud_manifest_sha256.eq_ignore_ascii_case(manifest_sha256)
}

/// Held-cut comparison against a decoded cloud `latest` pointer.
///
/// A pointer whose `format_version` is not 1 is refused by name
/// ([`SyncError::UnsupportedBackupFormat`]) before any identity field is
/// compared: this publisher writes v1 cuts only, so its held cut can never be
/// the tip of a newer-format chain, and the heal path must not read one as
/// "not ours, recut" or as a silent chain-walk miss.
pub(super) fn backup_cloud_latest_held_cut_match(
    cloud: &BackupLatestPointer,
    manifest: &BackupManifest,
    manifest_sha256: &str,
) -> SyncResult<bool> {
    cloud.require_v1_format()?;
    Ok(backup_cloud_latest_matches_held_cut(
        &cloud.store_uuid,
        cloud.epoch,
        cloud.counter,
        &cloud.manifest_sha256,
        manifest,
        manifest_sha256,
    ))
}

/// Report from one backup orphan GC sweep.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupOrphanGcReport {
    pub cloud_chunks_listed: usize,
    /// Union keep-set size (published tips + in-flight cut when held).
    pub live_referenced: usize,
    /// Digests kept because they appear in published tip manifests.
    #[serde(default)]
    pub published_referenced: usize,
    /// Digests kept because they appear on the in-flight publish target
    /// (manifest refs and/or live candidate paths). Zero when no cut is held.
    #[serde(default)]
    pub in_flight_referenced: usize,
    pub orphans_selected: usize,
    pub deleted: usize,
    pub failed: usize,
    /// A local publish, a target change, or a failed fresh-state proof stopped
    /// this sweep before its next DELETE. Counts before the stop remain valid.
    #[serde(default)]
    pub superseded: bool,
    /// Cloud keep-set bytes (listed sizes for digests in the full keep-set).
    #[serde(default)]
    pub referenced_bytes: u64,
    /// Total listed `backup/chunks/` object bytes (billable inventory).
    #[serde(default)]
    pub billed_bytes: u64,
    /// `billed − referenced` when keep-set known; else 0.
    #[serde(default)]
    pub reclaimable_bytes: u64,
}

/// Partition of the orphan-GC keep-set: published tips vs the held cut.
///
/// The union is what DELETE uses. The partition is what operators see so a
/// large "referenced" count during drain is not mistaken for a retention bug.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct BackupOrphanGcKeepParts {
    pub published: std::collections::BTreeSet<String>,
    pub in_flight: std::collections::BTreeSet<String>,
}

/// How orphan GC proves the published half of its keep-set.
///
/// Post-CAS GC requires an exact tip this process CASed and committed in this
/// lifetime. Quota recovery is the pre-CAS hatch: after restart the process
/// identity is `None` even when a published tip is live in cloud and on the
/// durable local counter. That hatch accepts a keep-set already resolved from
/// last-committed or downloaded `backup/latest`.
///
/// Admin `gc_orphan_backup_chunks` uses exact identity when this process has
/// one, and the same verified-published-body hatch after restart (identity
/// `None`, durable counter > 0, a named published body that matches live
/// `backup/latest`, local commit stamp, cloud plane On). A lagging sidecar
/// is refuse. Cloud Sync Off still refuses: Off can retire a held cut whose
/// chunks may already be the cloud pointer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BackupGcKeepProof {
    ExactProcessIdentity,
    VerifiedPublishedBody,
}

/// Local publication state that must remain unchanged from selection through
/// each remote DELETE.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct BackupGcPublicationState {
    pub(super) post_cas_generation: u64,
    pub(super) published_tip: Option<BackupTipIdentity>,
    pub(super) in_flight: Option<BackupPublishTargetReachabilityIdentity>,
    /// Live `backup/latest` pointer when keep-proof is a verified published
    /// body after restart. `None` on the exact-identity path.
    pub(super) cloud_latest: Option<BackupTipIdentity>,
}

pub(crate) struct PendingBackupGc {
    pub(super) manifest: BackupManifest,
    pub(super) generation: u64,
    pub(super) job_id: String,
}

impl BackupOrphanGcKeepParts {
    /// Digests referenced by published tip manifests only.
    pub(crate) fn from_published_manifests(live_manifests: &[BackupManifest]) -> Self {
        let mut published = std::collections::BTreeSet::new();
        for m in live_manifests {
            published.extend(manifest_referenced_chunk_shas(m));
        }
        Self {
            published,
            in_flight: std::collections::BTreeSet::new(),
        }
    }

    /// Full keep-set union used for orphan selection and footprint.
    pub(crate) fn keep_union(&self) -> std::collections::BTreeSet<String> {
        let mut keep = self.published.clone();
        keep.extend(self.in_flight.iter().cloned());
        keep
    }
}
