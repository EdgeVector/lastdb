//! A private recovery photograph includes every collection, including queues.

use super::*;

pub(super) fn cut_local_recovery(
    store: &LastStoreNamespacedStore,
) -> StorageResult<(BackupManifest, Vec<BackupChunkUploadCandidate>)> {
    let _retirement_guard = store
        .atom_retirement_lock
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let high_water = store.high_water.as_ref().ok_or_else(|| {
        StorageError::BackendError("local recovery requires a high-water marker".into())
    })?;
    let snapshot = store
        .store
        .snapshot()
        .map_err(LastStoreKvStore::map_error)?;
    if snapshot.skipped_capped_groups != 0 {
        return Err(StorageError::BackendError("local recovery cut skipped unsealed groups; retain the write fence and retry after compaction".into()));
    }
    let state = high_water.reserve_backup_manifest(snapshot.max_csn)?;
    let mut verified = Vec::new();
    for collection in store
        .store
        .collections_on_disk()
        .map_err(LastStoreKvStore::map_error)?
    {
        let role = if collection == ATOMS_COLLECTION {
            BackupManifestRole::Atom
        } else {
            BackupManifestRole::Mutable
        };
        for listed in store
            .store
            .enumerate_chunks(&collection)
            .map_err(LastStoreKvStore::map_error)?
        {
            let meta = store
                .store
                .verify_chunk_at(&listed)
                .map_err(LastStoreKvStore::map_error)?;
            verified.push(VerifiedChunk { meta, role });
        }
    }
    let mut candidates = Vec::new();
    let mut atoms = Vec::new();
    let mut mutable = Vec::new();
    for (chunk, path) in chunk_refs_from_walk(store, verified)? {
        match chunk.role {
            BackupManifestRole::Atom => atoms.push(chunk.clone()),
            BackupManifestRole::Mutable => mutable.push(chunk.clone()),
        }
        candidates.push(BackupChunkUploadCandidate {
            chunk,
            path,
            pack_members: None,
        });
    }
    let manifest = BackupManifest {
        version: MANIFEST_VERSION,
        store_uuid: state.store_uuid,
        epoch: state.backup_epoch,
        counter: state.backup_manifest_counter,
        previous_manifest_sha256: None,
        cut_csn: snapshot.max_csn,
        created_at_unix_secs: state.updated_at_unix_secs,
        mutable_chunks: dedupe_and_sort_chunks(mutable),
        atom_chunks: dedupe_and_sort_chunks(atoms),
        b2_cas_blob_refs: Vec::new(),
        deletion_receipts: Vec::new(),
        named_holes: Vec::new(),
    };
    validate_manifest_chain(None, &manifest)?;
    Ok((manifest, candidates))
}

impl LastStoreNamespacedStore {
    /// Local-only. The caller holds serving admission and the packing lock
    /// through the independent file copy; this does not publish cloud objects.
    pub fn cut_local_recovery_manifest(
        &self,
    ) -> StorageResult<(BackupManifest, Vec<BackupChunkUploadCandidate>)> {
        cut_local_recovery(self)
    }
}
