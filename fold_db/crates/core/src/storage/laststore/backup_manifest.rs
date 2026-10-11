use super::{LastStoreKvStore, LastStoreNamespacedStore};
use crate::hex::{hex_lower, sha256_hex};
use crate::storage::error::{StorageError, StorageResult};
use laststore::SealedChunkMeta;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write as _;
use std::path::PathBuf;
pub const MANIFEST_VERSION: u32 = 1;
pub const PACKED_MANIFEST_VERSION: u32 = 2;
/// Version of the v2 descriptor page and receipt formats. The v2 reader in
/// [`super::backup_descriptor`] accepts this value only; the v1 reader above
/// keeps accepting [`MANIFEST_VERSION`] only.
pub const DESCRIPTOR_VERSION: u32 = 2;
const ATOMS_COLLECTION: &str = "atoms";
/// System-authenticated receipt: retire carried-forward atom chunk refs that
/// exist neither on local disk nor in the object store. Distinct from a
/// user-authorized retention-floor move — agents may mint this type only when
/// both absences are proven (see [`CloudChunkPresence`]).
const UNBACKABLE_ATOM_RETIREMENT_RECEIPT_TYPE: &str = "unbackable_atom_chunk_retirement_receipt";
/// Reason code for unbackable atom retirement (stable, machine-readable).
pub const UNBACKABLE_RETIREMENT_REASON_ABSENT_LOCAL_AND_CLOUD: &str = "absent_local_and_cloud";
const PURGED_ATOM_RETIREMENT_RECEIPT_TYPE: &str = "purged_atom_chunk_retirement_receipt";
pub const PURGED_ATOM_RETIREMENT_REASON: &str = "purged";
/// System-authenticated exclusion: a leftover digest that exists neither on
/// disk under the packing lock nor in the last finished cloud photograph.
/// Distinct from unbackable-*atom* retirement: this covers any leftover name
/// (mutable planes included) so the stamp can publish instead of waiting.
const NAMED_HOLE_EXCLUSION_RECEIPT_TYPE: &str = "named_hole_exclusion_receipt";
pub const NAMED_HOLE_REASON_ABSENT_LOCAL_AND_CLOUD: &str =
    UNBACKABLE_RETIREMENT_REASON_ABSENT_LOCAL_AND_CLOUD;
pub(super) const PENDING_PURGED_ATOM_RETIREMENTS_FILE: &str =
    "laststore_pending_purged_atom_retirements.json";
/// One current committed keep-set, never a history of manifests. Oversized
/// keep-sets disable prefix inference rather than blocking cloud publication.
const MAX_COMMITTED_ATOM_PREFIXES: usize = 65_536;
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct PendingPurgedAtomRetirements {
    #[serde(default)]
    pub pending_shas: BTreeSet<String>,
    /// Pre-compaction inventory. Non-empty means an owner compact dreplacements.iter().map(ToString::to_string).collect() not
    /// finish recording its retired chunks, so manifest cuts must fail closed.
    #[serde(default)]
    pub compaction_in_progress_shas: BTreeSet<String>,
    /// Exact local chunk digest -> authenticated shorter prefix digests.
    /// These are not pending retirements until that compaction returns success.
    #[serde(default)]
    pub compaction_in_progress_prefixes: BTreeMap<String, BTreeSet<String>>,
    /// Only the confirmed commit path supplies these refs; a speculative cut
    /// or failed CAS must never replace the authenticated provenance source.
    #[serde(default)]
    pub committed_atom_prefixes: Option<CommittedAtomPrefixes>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct CommittedAtomPrefixes {
    pub store_uuid: String,
    pub epoch: u64,
    pub counter: u64,
    pub manifest_sha256: String,
    pub atom_chunks: Vec<BackupChunkRef>,
}
impl CommittedAtomPrefixes {
    fn from_manifest(manifest: &BackupManifest) -> StorageResult<Self> {
        Ok(Self {
            store_uuid: manifest.store_uuid.clone(),
            epoch: manifest.epoch,
            counter: manifest.counter,
            manifest_sha256: manifest_sha256_hex(manifest)?,
            // Preserve commit identity even when prefix inference is disabled.
            atom_chunks: if manifest.atom_chunks.len() > MAX_COMMITTED_ATOM_PREFIXES {
                Vec::new()
            } else {
                manifest.atom_chunks.clone()
            },
        })
    }
}
/// Role a sealed chunk plays in the cloud-backup manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackupManifestRole {
    /// Immutable atom chunks. Manifests carry this as a monotonic superset.
    Atom,
    /// Mutable collections such as schemas, tips, and the CSN log.
    Mutable,
}
/// One content-addressed sealed LastStore chunk referenced by a backup manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupChunkRef {
    pub collection: String,
    pub shard: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_id: Option<u32>,
    pub chunk_uuid: String,
    pub role: BackupManifestRole,
    /// SHA-256 over the stored bytes (`stored_bytes_sha256` in the v2 vocabulary).
    /// Frame-AEAD chunks and sorted units store ciphertext; plain-packaging
    /// segments and declared cleartext catalogs store the bytes verbatim, so
    /// this is never a ciphertext digest by definition.
    pub sha256: String,
    pub bytes: u64,
    pub end_csn: u64,
    /// Server-minted physical instance id of the stored object under the v2
    /// `backup/v2/` namespace. `None` on every v1 manifest; a v2 descriptor
    /// requires `Some`. Skipped when absent so v1 manifest bytes and their
    /// digests are unchanged by this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<String>,
    /// Location in a byte-for-byte pack. Absent on legacy direct objects.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pack: Option<BackupPackLocation>,
}
/// Hash-chained manifest for a local LastStore snapshot cut.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupManifest {
    pub version: u32,
    pub store_uuid: String,
    pub epoch: u64,
    pub counter: u64,
    pub previous_manifest_sha256: Option<String>,
    pub cut_csn: u64,
    pub created_at_unix_secs: u64,
    pub mutable_chunks: Vec<BackupChunkRef>,
    pub atom_chunks: Vec<BackupChunkRef>,
    /// Reserved for existing B2 CAS blob references. The first local cut keeps
    /// the field authenticated and round-trippable, but upload wiring lands in
    /// a later PR.
    pub b2_cas_blob_refs: Vec<String>,
    /// Authenticated receipts for future manual history deletion. A receipt
    /// makes a retention-floor move distinguishable from rollback/truncation.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deletion_receipts: Vec<BackupDeletionReceipt>,
    /// Leftover names that existed nowhere at stamp time (not on disk under
    /// the packing lock, not in the last finished cloud photograph). Recorded
    /// on the stamp so the gap is neither a wait-forever nor a silent drop.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub named_holes: Vec<BackupNamedHole>,
}
/// One leftover digest recorded on a photograph stamp.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupNamedHole {
    pub sha256: String,
    pub collection: String,
    pub role: BackupManifestRole,
}
/// Select cloud backup chunk digests that are safe to reclaim: present in the
/// cloud listing, but referenced by none of the keep-set manifests (current +
/// recent retained cuts). Live-manifest digests are never returned.
///
/// An empty keep set means the live set is unknown, not that nothing is live.
/// Return no orphans in that case so callers fail closed if they forget to
/// validate the manifest-cache precondition first.
#[must_use]
pub fn select_orphan_backup_chunk_shas<'a>(
    cloud_chunk_shas: impl IntoIterator<Item = &'a str>,
    live_manifest_chunk_shas: impl IntoIterator<Item = &'a str>,
) -> Vec<String> {
    let keep: BTreeSet<&str> = live_manifest_chunk_shas.into_iter().collect();
    if keep.is_empty() {
        return Vec::new();
    }
    let mut orphans = BTreeSet::new();
    for sha in cloud_chunk_shas {
        if sha.len() == 64 && sha.bytes().all(|b| b.is_ascii_hexdigit()) && !keep.contains(sha) {
            orphans.insert(sha.to_string());
        }
    }
    orphans.into_iter().collect()
}
/// Collect every chunk sha referenced by a manifest (atoms + mutable).
#[must_use]
pub fn manifest_referenced_chunk_shas(manifest: &BackupManifest) -> BTreeSet<String> {
    manifest
        .atom_chunks
        .iter()
        .chain(manifest.mutable_chunks.iter())
        .map(|c| c.object_sha256().to_string())
        .collect()
}
/// Operator-facing cloud backup footprint: referenced keep-set vs billed list.
///
/// All three byte totals come from the same cloud listing + keep-set so the
/// identity `reclaimable = billed − referenced` holds when the keep-set is
/// known. An empty keep-set is fail-closed: billed still reflects listed
/// objects, but reclaimable stays 0 (unknown live set ≠ free to delete).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupStorageFootprint {
    /// Cloud object bytes whose digests appear in the live tip keep-set.
    pub referenced_bytes: u64,
    /// Total listed object bytes under `backup/chunks/` (billable inventory).
    pub billed_bytes: u64,
    /// `billed − referenced` when keep-set non-empty; else 0 (fail closed).
    pub reclaimable_bytes: u64,
    pub referenced_chunks: u64,
    pub billed_chunks: u64,
    pub reclaimable_chunks: u64,
}
/// Compute referenced / billed / reclaimable from a cloud listing + keep-set.
///
/// `listed` yields `(chunk_sha256, object_size_bytes)` pairs. Non-hex / wrong-
/// length keys are ignored (same filter as orphan selection).
#[must_use]
pub fn compute_backup_storage_footprint<'a>(
    listed: impl IntoIterator<Item = (&'a str, u64)>,
    keep_shas: &BTreeSet<String>,
) -> BackupStorageFootprint {
    let keep_known = !keep_shas.is_empty();
    let mut billed_bytes = 0u64;
    let mut billed_chunks = 0u64;
    let mut referenced_bytes = 0u64;
    let mut referenced_chunks = 0u64;
    let mut reclaimable_bytes = 0u64;
    let mut reclaimable_chunks = 0u64;

    for (sha, size) in listed {
        if sha.len() != 64 || !sha.bytes().all(|b| b.is_ascii_hexdigit()) {
            continue;
        }
        billed_bytes = billed_bytes.saturating_add(size);
        billed_chunks = billed_chunks.saturating_add(1);
        if keep_shas.contains(sha) {
            referenced_bytes = referenced_bytes.saturating_add(size);
            referenced_chunks = referenced_chunks.saturating_add(1);
        } else if keep_known {
            reclaimable_bytes = reclaimable_bytes.saturating_add(size);
            reclaimable_chunks = reclaimable_chunks.saturating_add(1);
        }
    }

    BackupStorageFootprint {
        referenced_bytes,
        billed_bytes,
        reclaimable_bytes,
        referenced_chunks,
        billed_chunks,
        reclaimable_chunks,
    }
}
mod chunk_sha_memo;
mod deletion_receipt;
mod exclusions;
mod scan;
pub use deletion_receipt::*;
use exclusions::*;
pub use scan::*;
mod local_recovery;
mod packs;
pub use packs::*;
mod retirement_receipts;
pub(crate) use chunk_sha_memo::*;
use retirement_receipts::*;
mod atom_copy;
mod atom_lineage;
mod chain;
mod cut;
mod retirements;
pub use atom_copy::*;
pub use atom_lineage::*;
pub use chain::*;
pub use cut::*;
pub use retirements::*;
