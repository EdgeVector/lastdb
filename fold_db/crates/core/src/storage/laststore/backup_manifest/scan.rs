use super::*;

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
pub(super) struct VerifiedChunk {
    pub(super) meta: SealedChunkMeta,
    pub(super) role: BackupManifestRole,
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
pub(super) fn walk_backup_chunks(
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
pub(super) fn chunk_refs_from_walk(
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

pub(super) fn chunk_ref_from_meta(
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

pub(super) fn dedupe_and_sort_chunks(chunks: Vec<BackupChunkRef>) -> Vec<BackupChunkRef> {
    let mut by_key = BTreeMap::new();
    for chunk in chunks {
        by_key.insert(chunk_key(&chunk), chunk);
    }
    by_key.into_values().collect()
}

pub(super) fn chunk_key(chunk: &BackupChunkRef) -> (String, u16, Option<u32>, String) {
    (
        chunk.collection.clone(),
        chunk.shard,
        chunk.group_id,
        chunk.chunk_uuid.clone(),
    )
}
