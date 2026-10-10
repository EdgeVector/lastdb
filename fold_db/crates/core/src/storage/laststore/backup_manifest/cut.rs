//! Cutting and committing a backup manifest generation.

use super::*;

/// Render unresolvable chunks into one actionable message.
///
/// Names every offender up to a bound, so a home with several bad chunks takes
/// one repair pass instead of one per chunk. Paths are deliberately omitted —
/// collection plus uuid is enough to locate a chunk without putting absolute
/// filesystem paths into an error string.
pub(super) fn describe_unresolvable(prefix: &str, unresolvable: &[UnresolvableChunk]) -> String {
    const MAX_LISTED: usize = 10;
    let listed = unresolvable
        .iter()
        .take(MAX_LISTED)
        .map(|c| format!("{}/{} ({})", c.collection, c.chunk_uuid, c.reason))
        .collect::<Vec<_>>()
        .join("; ");
    let more = unresolvable.len().saturating_sub(MAX_LISTED);
    let total = unresolvable.len();
    if more > 0 {
        format!("{prefix}: {total} unresolvable chunk(s): {listed}; and {more} more")
    } else {
        format!("{prefix}: {total} unresolvable chunk(s): {listed}")
    }
}

pub fn cut_backup_manifest(
    store: &LastStoreNamespacedStore,
    previous_manifest: Option<&BackupManifest>,
) -> StorageResult<BackupManifest> {
    cut_backup_manifest_with_cloud_presence(store, previous_manifest, None)
}

/// Cut a backup manifest, optionally reconciling carried-forward atom refs
/// against a complete object-store listing.
///
/// When `cloud` is `Some` with `listing_complete = true`, a carried-forward
/// atom chunk that is **not** in the local walk and **not** in
/// `cloud.present_shas` is retired with an authenticated unbackable-atom
/// deletion receipt. Refs still present in cloud are **kept** even when
/// missing locally — they may be the only restore copy.
///
/// When `cloud` is `None` or incomplete, behaviour matches the historical cut
/// (unconditional carry-forward).
pub fn cut_backup_manifest_with_cloud_presence(
    store: &LastStoreNamespacedStore,
    previous_manifest: Option<&BackupManifest>,
    cloud: Option<&CloudChunkPresence>,
) -> StorageResult<BackupManifest> {
    cut_backup_manifest_with_cloud_presence_inner(store, previous_manifest, cloud, false)
}

/// Refuse a backup-only cut if the snapshot left an unsealed capped group out.
pub fn cut_backup_manifest_with_cloud_presence_strict(
    store: &LastStoreNamespacedStore,
    previous_manifest: Option<&BackupManifest>,
    cloud: Option<&CloudChunkPresence>,
) -> StorageResult<BackupManifest> {
    cut_backup_manifest_with_cloud_presence_inner(store, previous_manifest, cloud, true)
}

pub(super) fn cut_backup_manifest_with_cloud_presence_inner(
    store: &LastStoreNamespacedStore,
    previous_manifest: Option<&BackupManifest>,
    cloud: Option<&CloudChunkPresence>,
    require_complete_seal: bool,
    // lint:fn-size-ok verbatim move from backup_manifest.rs; splitting this function is separate work
) -> StorageResult<BackupManifest> {
    let _retirement_guard = store
        .atom_retirement_lock
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let high_water = store.high_water.as_ref().ok_or_else(|| {
        StorageError::BackendError(
            "laststore backup manifest cut requires a high-water marker".to_string(),
        )
    })?;

    // Snapshot first: it seals pending writes, and the walk below must see the
    // chunks that sealing produces or the manifest silently omits them.
    let snapshot = store
        .store
        .snapshot()
        .map_err(LastStoreKvStore::map_error)?;
    if require_complete_seal && snapshot.skipped_capped_groups > 0 {
        return Err(StorageError::BackendError(format!(
            "backup snapshot skipped {} capped group(s) with unsealed data; reclaim or compact the groups, then retry",
            snapshot.skipped_capped_groups
        )));
    }
    let state = high_water.reserve_backup_manifest(snapshot.max_csn)?;

    let (verified, unresolvable) = walk_backup_chunks(store)?;
    if !unresolvable.is_empty() {
        // A manifest is the durable record a restore is rebuilt from. Dropping
        // an unreadable chunk from it would silently narrow what a restore can
        // recover, so the cut refuses — but it names every offender at once.
        return Err(StorageError::BackendError(describe_unresolvable(
            "cannot cut a backup manifest",
            &unresolvable,
        )));
    }

    let mut atom_chunks = previous_manifest
        .map(|manifest| manifest.atom_chunks.clone())
        .unwrap_or_default();
    let mut mutable_chunks = Vec::new();
    let mut local_atom_keys = BTreeSet::new();
    let mut local_atom_shas = BTreeSet::new();
    let prior_by_key: BTreeMap<_, _> = previous_manifest
        .into_iter()
        .flat_map(|manifest| manifest.atom_chunks.iter().chain(&manifest.mutable_chunks))
        .map(|chunk| (chunk_key(chunk), chunk))
        .collect();

    let selected = chunk_refs_from_walk(store, verified)?;
    let pending_purged = load_pending_purged_atom_retirements(store)?;
    verify_selected_atom_replacements(previous_manifest, &selected, &pending_purged)?;
    for (mut chunk, _path) in selected {
        if let Some(prior) = prior_by_key.get(&chunk_key(&chunk)) {
            if prior.sha256 == chunk.sha256 && prior.bytes == chunk.bytes {
                chunk.pack.clone_from(&prior.pack);
            }
        }
        match chunk.role {
            BackupManifestRole::Atom => {
                local_atom_keys.insert(chunk_key(&chunk));
                local_atom_shas.insert(chunk.sha256.clone());
                atom_chunks.push(chunk);
            }
            BackupManifestRole::Mutable => mutable_chunks.push(chunk),
        }
    }

    atom_chunks = dedupe_and_sort_chunks(atom_chunks);
    mutable_chunks = dedupe_and_sort_chunks(mutable_chunks);

    let mut deletion_receipts = Vec::new();
    if !pending_purged.compaction_in_progress_shas.is_empty()
        || !pending_purged.compaction_in_progress_prefixes.is_empty()
    {
        return Err(StorageError::BackendError(
            "atom compaction retirement record is incomplete; refusing backup manifest cut"
                .to_string(),
        ));
    }
    let retired_purged_shas: BTreeSet<_> = pending_purged
        .pending_shas
        .difference(&local_atom_shas)
        .cloned()
        .collect();
    if !retired_purged_shas.is_empty() {
        atom_chunks.retain(|chunk| !retired_purged_shas.contains(&chunk.sha256));
        deletion_receipts.push(BackupDeletionReceipt::new_purged_atom_retirement(
            previous_manifest.map_or(0, |manifest| manifest.counter),
            state.backup_manifest_counter,
            retired_purged_shas.into_iter().collect(),
            state.updated_at_unix_secs.max(1),
        ));
    }
    if let (Some(previous), Some(cloud)) = (previous_manifest, cloud) {
        if cloud.listing_complete {
            let (kept, retired_shas) = partition_unbackable_carried_forward_atoms(
                &atom_chunks,
                &local_atom_keys,
                &cloud.present_shas,
            );
            if !retired_shas.is_empty() {
                deletion_receipts.push(BackupDeletionReceipt::new_unbackable_atom_retirement(
                    previous.counter,
                    state.backup_manifest_counter,
                    retired_shas,
                    state.updated_at_unix_secs.max(1),
                ));
                atom_chunks = kept;
            }
        }
    }

    let previous_manifest_sha256 = previous_manifest.map(manifest_sha256_hex).transpose()?;

    Ok(BackupManifest {
        version: if previous_manifest.is_some_and(|prior| prior.version == PACKED_MANIFEST_VERSION)
            || std::env::var("LASTDB_BACKUP_FILE_PACKS").is_ok_and(|value| value == "1")
        {
            PACKED_MANIFEST_VERSION
        } else {
            MANIFEST_VERSION
        },
        store_uuid: state.store_uuid,
        epoch: state.backup_epoch,
        counter: state.backup_manifest_counter,
        previous_manifest_sha256,
        cut_csn: snapshot.max_csn,
        created_at_unix_secs: state.updated_at_unix_secs,
        mutable_chunks,
        atom_chunks,
        b2_cas_blob_refs: Vec::new(),
        deletion_receipts,
        named_holes: Vec::new(),
    })
}

pub fn commit_backup_manifest(
    store: &LastStoreNamespacedStore,
    manifest: &BackupManifest,
) -> StorageResult<()> {
    let _retirement_guard = store
        .atom_retirement_lock
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let high_water = store.high_water.as_ref().ok_or_else(|| {
        StorageError::BackendError(
            "laststore backup manifest commit requires a high-water marker".to_string(),
        )
    })?;
    let scope = high_water.load_or_init()?;
    if scope.store_uuid != manifest.store_uuid || scope.backup_epoch != manifest.epoch {
        return Err(StorageError::BackendError(
            "committed atom provenance source scope mismatch".into(),
        ));
    }
    let mut state = load_pending_purged_atom_retirements(store)?;
    if let Some(previous) = &state.committed_atom_prefixes {
        if previous.store_uuid == manifest.store_uuid && previous.epoch == manifest.epoch {
            if manifest.counter < previous.counter {
                return Err(StorageError::BackendError(
                    "committed atom provenance counter regression".into(),
                ));
            }
            if manifest.counter == previous.counter {
                if manifest_sha256_hex(manifest)? != previous.manifest_sha256 {
                    return Err(StorageError::BackendError(
                        "committed atom provenance digest conflict".into(),
                    ));
                }
                // This cache and the receipt cleanup share one atomic sidecar
                // write. A repeated commit must not clear retirements created
                // by a later compaction after that write already completed.
                return Ok(());
            }
        }
    }
    high_water.record_backup_manifest_commit(manifest.counter, manifest.cut_csn)?;
    let committed_purged_shas: BTreeSet<_> = manifest
        .deletion_receipts
        .iter()
        .filter(|receipt| receipt.record_type == PURGED_ATOM_RETIREMENT_RECEIPT_TYPE)
        .flat_map(|receipt| receipt.retired_atom_chunk_shas.iter().cloned())
        .collect();
    if !committed_purged_shas.is_empty() {
        state.pending_shas = state
            .pending_shas
            .difference(&committed_purged_shas)
            .cloned()
            .collect();
    }
    state.committed_atom_prefixes = Some(CommittedAtomPrefixes::from_manifest(manifest)?);
    write_pending_purged_atom_retirements(store, &state)?;
    Ok(())
}
