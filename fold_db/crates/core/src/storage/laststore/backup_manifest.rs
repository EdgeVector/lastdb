use super::backup_descriptor::{DescriptorRootView, RetirementReceiptV2};
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
const DELETION_RECEIPT_RECORD_TYPE: &str = "retention_floor_deletion_receipt";
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

/// Collections that must never enter cloud backup (local-only / rebuildable /
/// aside archives / at-rest-exempt). Everything else on disk with sealed segs
/// is backed up so second-device restore is a usable copy of the primary
/// (schemas, atoms, cas_blobs/file blobs, range indexes, etc.).
///
/// **Coupled to at-rest encryption policy (won't-undo, 2026-08-03):** the
/// chunk-backup plane below has no encryption of its own — it ships on-disk
/// `.seg` bytes verbatim (live-path file-backed PUT), so a namespace
/// exempt from at-rest encryption
/// (`encrypting_namespaced_store::LASTSTORE_PLAINTEXT_NAMESPACES`) that is
/// *not* excluded here goes to the object store in cleartext.
///
/// The first form of this coupling required *exclusion*, on the premise that
/// cleartext catalogs in the object store are a confidentiality defect. The
/// Trinity-only encryption standard (Tom, 2026-08-03 — brain
/// `preference-lastdb-encryption-standard-trinity-only`) retires that premise:
/// published schema definitions are not a secret, and "treating cleartext
/// published schema JSON on disk or in cloud backup as a confidentiality
/// incident" is explicitly out of standard. Excluding `schemas` would also make
/// a restored device an unusable copy of the primary — it is a source-of-truth
/// collection (`mini_cutover::plane_roles::SOT_NAMED_COLLECTIONS`).
///
/// So the coupling now demands a **declaration** rather than exclusion: every
/// at-rest-plaintext namespace must be either listed here or named in
/// `BACKUP_CLEARTEXT_CATALOG_NAMESPACES` below. That keeps the guard load-bearing —
/// adding a namespace to the plaintext allowlist still fails the build until
/// someone states which of the two it is — while allowing the catalogs the
/// standard says may travel in the clear. Enforced by
/// `backup_role_matches_at_rest_encryption_exemption` below.
///
/// **Coupled to source-of-truth role (2026-08-04):** the confidentiality
/// coupling above says nothing about durability, and that gap shipped —
/// `schema_states` and `schema_superseded_by` are SOT
/// (`mini_cutover::plane_roles::SOT_COLLECTIONS`) and were excluded here purely
/// as fallout of the retired secrecy premise, so a restored device silently
/// lost every non-default schema state. Both are now backed up, and
/// `sot_collections_are_backed_up_or_declared_exception` below fails the build
/// if a future SOT collection is excluded without a stated reason.
///
/// The remaining entries that are only here because of the original exclusion
/// coupling (`schema_index`, `public_keys`, `idempotency`) are deliberately left
/// excluded: none is source of truth, so a restored device rebuilds or
/// re-earns them. Their exclusion is a restore-completeness choice, not a
/// secrecy one. (History: post-WASM ghosts `views` / `view_states` /
/// `transform_field_overrides` / `process_results` dropped from this list.)
const BACKUP_EXCLUDED_EXACT: &[&str] = &[
    // Sync machinery / ephemeral queues — not product state.
    "sync_capture",
    // Crash-safe dirty-key intent markers for in-flight local writes. Added
    // 2026-08-17: every `sync_*` ephemeral queue beside it was already
    // excluded, but this one was being uploaded — 3.14 GiB of segment bytes on
    // Tom's primary whose live set was zero. The markers name keys in *this*
    // node's serving store at one instant, and the drain re-reads current
    // truth, so a restored device gains nothing from them and would drain
    // intents belonging to a machine it is not.
    "sync_capture_reexport",
    "sync_outbox",
    "sync_conflicts",
    "sync_cursors",
    "sync_file_blob_known",
    "share_delivery_outbox",
    "org_sync_targets",
    // Rebuildable atom reverse-reference index. Restore reconstructs it from
    // authoritative tips and tip-version rows.
    "atom_ref_edges",
    "atom_ref_edges_v2",
    "molecule_ref_edges",
    "blob_ref_edges",
    // Owner-only copy-migration proof. A restored node rebuilds it against its
    // own catalog snapshot; it has no cross-node meaning.
    "attribution_ledger",
    // The keep-small meter snapshot (its own plane since 2026-09-21). A
    // node-local gauge that normal writes and the liveness bootstrap rebuild;
    // hydrate treats a missing row as a fresh home. Nothing a restored device
    // gains, and every put is a whole-map rewrite, so keep it out of the
    // photograph like the attribution ledger.
    "keep_small",
    // Local app-consent cache; re-enrolled on device.
    "app_identity:consent_requests",
    // Marker namespace, not user data.
    "__at_rest_strict_markers",
    // `native_index` removed 2026-08-05 — retired product collection is no
    // longer a first-class backup-exclusion catalog entry. Residual cold-home
    // keys (if any) follow the default mutable backup path.
    // Remaining entries are at-rest plaintext and excluded from the uploader.
    // Kept excluded by the Trinity-only pass (see doc comment above): their
    // exclusion is now a restore-completeness choice, not a secrecy one. None
    // of them is source of truth — `schema_states` and `schema_superseded_by`
    // were, and left this list on 2026-08-04.
    "schema_index",
    "public_keys",
    "idempotency",
];

/// Prefixes excluded from backup (legacy asides, temp renames).
const BACKUP_EXCLUDED_PREFIXES: &[&str] = &["sync_outbox.aside", "__at_rest"];

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

/// One local sealed chunk selected for cloud backup upload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupChunkUploadCandidate {
    pub chunk: BackupChunkRef,
    pub path: PathBuf,
    /// A synthetic pack names its source files here. Its path is the local
    /// directory for the short-lived pack file built only during upload.
    pub pack_members: Option<Vec<BackupChunkUploadCandidate>>,
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

/// Authenticated manifest-chain record that sanctions narrowing atom_chunks.
///
/// Two record types:
/// - `retention_floor_deletion_receipt` — user-authorized history delete
///   (floor move). `user_authorized` must be true.
/// - `unbackable_atom_chunk_retirement_receipt` — system-authorized retirement
///   of carried-forward atom refs proven absent from both local disk and the
///   object store. Lists every retired digest in `retired_atom_chunk_shas`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupDeletionReceipt {
    /// Type tag for forward-compatible manifest-chain parsing.
    pub record_type: String,
    /// Previous authenticated retention floor / predecessor counter.
    pub floor_from_manifest_counter: u64,
    /// New authenticated retention floor / current counter.
    pub floor_to_manifest_counter: u64,
    /// User floor-moves must be true. Unbackable-atom retirements are
    /// system-authorized (`false`) — they only fire when both local and cloud
    /// absence are proven, so they are not a human history-delete.
    pub user_authorized: bool,
    /// Authorization / retirement timestamp, seconds since the Unix epoch.
    pub authorized_at_unix_secs: u64,
    /// Digests retired by an unbackable-atom receipt (empty for floor moves).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub retired_atom_chunk_shas: Vec<String>,
    /// Machine-readable reason for unbackable retirement (e.g.
    /// [`UNBACKABLE_RETIREMENT_REASON_ABSENT_LOCAL_AND_CLOUD`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl BackupDeletionReceipt {
    /// Exact owner-authorized purge coverage for one predecessor chunk.
    #[cfg(feature = "cloud-sync")]
    pub(crate) fn covers_purged_atom(&self, from: u64, to: u64, sha: &str) -> bool {
        self.record_type == PURGED_ATOM_RETIREMENT_RECEIPT_TYPE
            && self.reason.as_deref() == Some(PURGED_ATOM_RETIREMENT_REASON)
            && self.user_authorized
            && self.authorized_at_unix_secs > 0
            && self.floor_from_manifest_counter == from
            && self.floor_to_manifest_counter == to
            && to > from
            && self
                .retired_atom_chunk_shas
                .iter()
                .any(|retired| retired == sha)
    }

    pub fn new_floor_move(
        floor_from_manifest_counter: u64,
        floor_to_manifest_counter: u64,
        authorized_at_unix_secs: u64,
    ) -> Self {
        Self {
            record_type: DELETION_RECEIPT_RECORD_TYPE.to_string(),
            floor_from_manifest_counter,
            floor_to_manifest_counter,
            user_authorized: true,
            authorized_at_unix_secs,
            retired_atom_chunk_shas: Vec::new(),
            reason: None,
        }
    }

    /// System receipt: retire carried-forward atom digests that are usable for
    /// neither upload nor restore (absent locally and in cloud).
    pub fn new_unbackable_atom_retirement(
        from_counter: u64,
        to_counter: u64,
        retired_atom_chunk_shas: Vec<String>,
        authorized_at_unix_secs: u64,
    ) -> Self {
        let mut retired_atom_chunk_shas = retired_atom_chunk_shas;
        retired_atom_chunk_shas.sort();
        retired_atom_chunk_shas.dedup();
        Self {
            record_type: UNBACKABLE_ATOM_RETIREMENT_RECEIPT_TYPE.to_string(),
            floor_from_manifest_counter: from_counter,
            floor_to_manifest_counter: to_counter,
            user_authorized: false,
            authorized_at_unix_secs,
            retired_atom_chunk_shas,
            reason: Some(UNBACKABLE_RETIREMENT_REASON_ABSENT_LOCAL_AND_CLOUD.to_string()),
        }
    }

    /// Owner-authorized receipt for atom chunks deliberately retired by the
    /// explicit compaction path.
    pub fn new_purged_atom_retirement(
        from_counter: u64,
        to_counter: u64,
        retired_atom_chunk_shas: Vec<String>,
        authorized_at_unix_secs: u64,
    ) -> Self {
        let mut retired_atom_chunk_shas = retired_atom_chunk_shas;
        retired_atom_chunk_shas.sort();
        retired_atom_chunk_shas.dedup();
        Self {
            record_type: PURGED_ATOM_RETIREMENT_RECEIPT_TYPE.to_string(),
            floor_from_manifest_counter: from_counter,
            floor_to_manifest_counter: to_counter,
            user_authorized: true,
            authorized_at_unix_secs,
            retired_atom_chunk_shas,
            reason: Some(PURGED_ATOM_RETIREMENT_REASON.to_string()),
        }
    }

    /// System receipt: leftover names that exist nowhere become named holes
    /// on this stamp. `retired_atom_chunk_shas` is the covering set for chain
    /// validation (any role; the field name is historical).
    pub fn new_named_hole_exclusion(
        from_counter: u64,
        to_counter: u64,
        named_hole_shas: Vec<String>,
        authorized_at_unix_secs: u64,
    ) -> Self {
        let mut named_hole_shas = named_hole_shas;
        named_hole_shas.sort();
        named_hole_shas.dedup();
        Self {
            record_type: NAMED_HOLE_EXCLUSION_RECEIPT_TYPE.to_string(),
            floor_from_manifest_counter: from_counter,
            floor_to_manifest_counter: to_counter,
            user_authorized: false,
            authorized_at_unix_secs,
            retired_atom_chunk_shas: named_hole_shas,
            reason: Some(NAMED_HOLE_REASON_ABSENT_LOCAL_AND_CLOUD.to_string()),
        }
    }
}

/// Complete object-store presence view for carried-forward atom reconciliation.
///
/// Absence from `present_shas` is only treated as proven-absent when
/// `listing_complete` is true (full `backup/chunks/` listing succeeded). A
/// positive-only cache without a complete listing must set
/// `listing_complete = false` so the cut never retires a ref that might still
/// be the only cloud copy.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CloudChunkPresence {
    pub present_shas: BTreeSet<String>,
    pub listing_complete: bool,
}

impl CloudChunkPresence {
    pub fn from_complete_listing(present_shas: impl IntoIterator<Item = String>) -> Self {
        Self {
            present_shas: present_shas.into_iter().collect(),
            listing_complete: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackupManifestChainStep {
    OrdinaryAppend,
    SanctionedRetentionFloorMove,
    /// Carried-forward atom refs retired because they exist neither locally
    /// nor in the object store (see unbackable retirement receipt).
    SanctionedUnbackableAtomRetirement,
    /// Leftover names (any role) recorded as named holes on the stamp.
    SanctionedNamedHoleExclusion,
    /// Owner-authorized atom compaction named every retired chunk digest.
    SanctionedPurgedAtomRetirement,
}

/// One chunk that `enumerate_chunks` listed but that could not be verified at
/// the location the listing reported.
///
/// Carried rather than thrown so a single bad chunk cannot cost every other
/// chunk in the store its backup — see [`walk_backup_chunks`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnresolvableChunk {
    pub collection: String,
    pub chunk_uuid: String,
    pub path: PathBuf,
    pub reason: String,
}

/// Outcome of one enumerate+verify walk of the store's backup-eligible chunks.
#[derive(Debug, Clone, Default)]
pub struct BackupChunkScan {
    /// Verified chunks not already present in the previous manifest.
    pub candidates: Vec<BackupChunkUploadCandidate>,
    /// Chunks that were listed but failed verification. Never silently dropped:
    /// the uploader reports them, and a manifest cut refuses while any remain.
    pub unresolvable: Vec<UnresolvableChunk>,
}

/// A verified chunk plus the backup role its collection plays.
struct VerifiedChunk {
    meta: SealedChunkMeta,
    role: BackupManifestRole,
}

/// Walk every backup-eligible collection, verifying each chunk where
/// `enumerate_chunks` said it is.
///
/// A chunk that fails verification is **collected**, not propagated. Both
/// callers used to `?` on the first verify error, so one unresolvable uuid
/// abandoned the walk across every collection and the whole cloud publish cycle
/// failed. On Tom's primary that cost 9,832 consecutive publish cycles over
/// 2.5+ days: one chunk out of ~19,435 blocked backup of the other ~19,434,
/// while the log still read "local R/W unaffected" and nothing surfaced the
/// stalled cloud copy.
///
/// Structural failures — listing collections, listing one collection's chunks —
/// stay fatal. Those are not per-chunk faults and silently continuing past them
/// could under-report the store.
fn walk_backup_chunks(
    store: &LastStoreNamespacedStore,
) -> StorageResult<(Vec<VerifiedChunk>, Vec<UnresolvableChunk>)> {
    let collections = store
        .store
        .collections_on_disk()
        .map_err(LastStoreKvStore::map_error)?;
    let mut verified = Vec::new();
    let mut unresolvable = Vec::new();

    for collection in collections {
        let Some(role) = backup_role_for_collection(&collection) else {
            continue;
        };
        for listed in store
            .store
            .enumerate_chunks(&collection)
            .map_err(LastStoreKvStore::map_error)?
        {
            // `verify_chunk_at`, not `verify_chunk`: `enumerate_chunks` just
            // gave us this chunk's location, and re-finding it by uuid walks
            // the entire store per chunk.
            match store.store.verify_chunk_at(&listed) {
                Ok(meta) => verified.push(VerifiedChunk { meta, role }),
                Err(e) => unresolvable.push(UnresolvableChunk {
                    collection: collection.clone(),
                    chunk_uuid: listed.chunk_uuid.to_string(),
                    path: listed.path.clone(),
                    reason: e.to_string(),
                }),
            }
        }
    }

    Ok((verified, unresolvable))
}

/// Memoized sha resolution + post-walk memo flush for one full chunk walk.
fn chunk_refs_from_walk(
    store: &LastStoreNamespacedStore,
    verified: Vec<VerifiedChunk>,
) -> StorageResult<Vec<(BackupChunkRef, std::path::PathBuf)>> {
    let memo = &store.chunk_sha_memo;
    let mut refs = Vec::with_capacity(verified.len());
    for VerifiedChunk { meta, role } in verified {
        let path = meta.path.clone();
        let chunk = chunk_ref_from_meta(meta, role, memo)?;
        refs.push((chunk, path));
    }
    memo.flush_after_walk();
    Ok(refs)
}

/// Enumerate upload candidates, reporting rather than throwing on chunks that
/// cannot be verified.
///
/// Skipping an unresolvable chunk here is strictly more progress, not less: the
/// healthy chunks still upload. Publish correctness is unaffected, because a
/// manifest is only CASed once every chunk it names is confirmed present in the
/// cloud, and [`cut_backup_manifest`] refuses outright while any chunk is
/// unresolvable.
pub fn scan_backup_chunks(
    store: &LastStoreNamespacedStore,
    previous_manifest: Option<&BackupManifest>,
) -> StorageResult<BackupChunkScan> {
    let previous_refs: BTreeSet<_> = previous_manifest
        .into_iter()
        .flat_map(|manifest| {
            manifest
                .mutable_chunks
                .iter()
                .chain(manifest.atom_chunks.iter())
        })
        .map(chunk_key)
        .collect();

    let (verified, unresolvable) = walk_backup_chunks(store)?;
    let mut candidates = Vec::new();
    for (chunk, path) in chunk_refs_from_walk(store, verified)? {
        if previous_refs.contains(&chunk_key(&chunk)) {
            continue;
        }
        candidates.push(BackupChunkUploadCandidate {
            chunk,
            path,
            pack_members: None,
        });
    }
    candidates.sort_by_key(|candidate| chunk_key(&candidate.chunk));
    Ok(BackupChunkScan {
        candidates,
        unresolvable,
    })
}

pub fn enumerate_backup_chunk_candidates(
    store: &LastStoreNamespacedStore,
    previous_manifest: Option<&BackupManifest>,
) -> StorageResult<Vec<BackupChunkUploadCandidate>> {
    scan_backup_chunks(store, previous_manifest).map(|scan| scan.candidates)
}

pub fn enumerate_backup_publish_target_candidates(
    store: &LastStoreNamespacedStore,
) -> StorageResult<Vec<BackupChunkUploadCandidate>> {
    scan_backup_chunks(store, None).map(|scan| scan.candidates)
}

fn backup_role_for_collection(collection: &str) -> Option<BackupManifestRole> {
    if collection == ATOMS_COLLECTION {
        return Some(BackupManifestRole::Atom);
    }
    if is_backup_excluded_collection(collection) {
        return None;
    }
    // All remaining finalized/CAS-eligible collections (schemas, cas_blobs,
    // field indexes, tips, proteins, log, blobs, …) are mutable product
    // state. "Finalized" here is lifecycle, not crypto — see
    // `concepts-lastdb-cloud-upload-seal-is-lifecycle-not-crypto`; whether the
    // bytes are actually encrypted is a separate question answered by
    // `LASTSTORE_PLAINTEXT_NAMESPACES` and enforced against this exclusion
    // list by `backup_role_matches_at_rest_encryption_exemption` below.
    Some(BackupManifestRole::Mutable)
}

fn is_backup_excluded_collection(collection: &str) -> bool {
    if BACKUP_EXCLUDED_EXACT.contains(&collection) {
        return true;
    }
    BACKUP_EXCLUDED_PREFIXES
        .iter()
        .any(|prefix| collection.starts_with(prefix))
}

fn chunk_ref_from_meta(
    meta: SealedChunkMeta,
    role: BackupManifestRole,
    memo: &ChunkShaMemo,
) -> StorageResult<BackupChunkRef> {
    // Stream the sealed unit — multi-MB segs must not be fully buffered or a
    // full-home enumerate (1000+) spikes Mini RSS into the memory-guard kill zone.
    //
    // Sealed chunks are immutable, so (mtime, len) identifies the bytes: the
    // memo turns the walk's hash cost from O(store bytes) per cycle into
    // O(changed bytes). Without it, a 2.9 GiB home re-hashed every uploader
    // cycle under IOPOL throttle — the measured cause of the 2026-07-30
    // first-CAS grind.
    let (sha256, bytes) = memo.sha256_for(&meta.path)?;
    Ok(BackupChunkRef {
        collection: meta.collection,
        shard: meta.shard,
        group_id: meta.group_id,
        chunk_uuid: meta.chunk_uuid.to_string(),
        role,
        sha256,
        bytes,
        end_csn: meta.end_csn,
        instance: None,
        pack: None,
    })
}

fn dedupe_and_sort_chunks(chunks: Vec<BackupChunkRef>) -> Vec<BackupChunkRef> {
    let mut by_key = BTreeMap::new();
    for chunk in chunks {
        by_key.insert(chunk_key(&chunk), chunk);
    }
    by_key.into_values().collect()
}

fn chunk_key(chunk: &BackupChunkRef) -> (String, u16, Option<u32>, String) {
    (
        chunk.collection.clone(),
        chunk.shard,
        chunk.group_id,
        chunk.chunk_uuid.clone(),
    )
}

mod chunk_sha_memo;
mod packs;
pub use packs::*;
mod retirement_receipts;
pub(crate) use chunk_sha_memo::*;
use retirement_receipts::*;

mod atom_lineage;
mod chain;
mod cut;
mod retirements;
pub use atom_lineage::*;
pub use chain::*;
pub use cut::*;
pub use retirements::*;
