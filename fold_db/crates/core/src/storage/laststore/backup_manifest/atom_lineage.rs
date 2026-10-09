//! Local atom chunk enumeration and the verified committed-prefix lineage that
//! authorizes historical successor stamps.

use super::*;

pub(in super::super) fn local_atom_chunk_shas(
    store: &LastStoreNamespacedStore,
) -> StorageResult<BTreeSet<String>> {
    Ok(local_atom_chunk_refs(store)?
        .into_iter()
        .map(|(chunk, _)| chunk.sha256)
        .collect())
}

pub(in super::super) fn local_atom_chunk_refs(
    store: &LastStoreNamespacedStore,
) -> StorageResult<Vec<(BackupChunkRef, PathBuf)>> {
    let mut refs = Vec::new();
    for listed in store
        .store
        .enumerate_chunks(ATOMS_COLLECTION)
        .map_err(LastStoreKvStore::map_error)?
    {
        let meta = store
            .store
            .verify_chunk_at(&listed)
            .map_err(LastStoreKvStore::map_error)?;
        let path = meta.path.clone();
        let chunk = chunk_ref_from_meta(meta, BackupManifestRole::Atom, &store.chunk_sha_memo)?;
        refs.push((chunk, path));
    }
    Ok(refs)
}

/// Bind only authenticated refs whose exact bytes still form a local prefix.
/// Work is bounded by the committed atom refs and the existing local chunk
/// inventory. No cloud listing or cross-namespace search supplies provenance.
pub(in super::super) fn verified_atom_prefix_lineage(
    store: &LastStoreNamespacedStore,
    state: &PendingPurgedAtomRetirements,
    local: &[(BackupChunkRef, PathBuf)],
) -> StorageResult<BTreeMap<String, BTreeSet<String>>> {
    let mut lineage = BTreeMap::<String, BTreeSet<String>>::new();
    let (Some(committed), Some(high_water)) = (&state.committed_atom_prefixes, &store.high_water)
    else {
        return Ok(lineage);
    };
    let scope = high_water.load_or_init()?;
    if committed.store_uuid != scope.store_uuid
        || committed.epoch != scope.backup_epoch
        || committed.counter == 0
        || committed.counter > scope.backup_manifest_counter
        || committed.atom_chunks.len() > MAX_COMMITTED_ATOM_PREFIXES
    {
        return Ok(lineage);
    }
    // Duplicate addresses are ambiguous. Do not pick an arbitrary ref as
    // retirement authority, even if one of its digests matches local bytes.
    let mut by_address = BTreeMap::new();
    for chunk in &committed.atom_chunks {
        if chunk.role != BackupManifestRole::Atom || chunk.collection != ATOMS_COLLECTION {
            continue;
        }
        by_address
            .entry(chunk_key(chunk))
            .and_modify(|entry| *entry = None)
            .or_insert(Some(chunk));
    }
    for (current, path) in local {
        let Some(Some(prefix)) = by_address.get(&chunk_key(current)) else {
            continue;
        };
        if prefix.bytes == 0 || prefix.bytes >= current.bytes || prefix.sha256 == current.sha256 {
            continue;
        }
        if file_has_verified_prefix(path, prefix, current)? {
            lineage
                .entry(current.sha256.clone())
                .or_default()
                .insert(prefix.sha256.clone());
        }
    }
    Ok(lineage)
}

pub(super) fn group_addr(chunk: &BackupChunkRef) -> (u16, Option<u32>) {
    (chunk.shard, chunk.group_id)
}

/// True when committed_atom_prefixes may authorize historical stamps.
/// Same guards as [`verified_atom_prefix_lineage`].
pub(in super::super) fn committed_atom_prefixes_in_scope(
    store: &LastStoreNamespacedStore,
    state: &PendingPurgedAtomRetirements,
) -> bool {
    let (Some(committed), Some(high_water)) = (&state.committed_atom_prefixes, &store.high_water)
    else {
        return false;
    };
    let Ok(scope) = high_water.load_or_init() else {
        return false;
    };
    committed.store_uuid == scope.store_uuid
        && committed.epoch == scope.backup_epoch
        && committed.counter > 0
        && committed.counter <= scope.backup_manifest_counter
        && committed.atom_chunks.len() <= MAX_COMMITTED_ATOM_PREFIXES
}

/// Committed keep-set SHAs whose group still has a verified local atom chunk.
/// Does not apply scope guards (used for stamp-miss counts). Skips groups with
/// no verified local successor. Does not include those local successor SHAs.
pub(super) fn committed_atom_extras_with_local_successor(
    state: &PendingPurgedAtomRetirements,
    local: &[(BackupChunkRef, PathBuf)],
) -> BTreeSet<String> {
    let Some(committed) = &state.committed_atom_prefixes else {
        return BTreeSet::new();
    };
    let successor_groups: BTreeSet<_> = local
        .iter()
        .filter(|(c, _)| c.role == BackupManifestRole::Atom && c.collection == ATOMS_COLLECTION)
        .map(|(c, _)| group_addr(c))
        .collect();
    let local_shas: BTreeSet<_> = local.iter().map(|(c, _)| c.sha256.clone()).collect();
    let mut by_address = BTreeMap::new();
    for chunk in &committed.atom_chunks {
        by_address
            .entry(chunk_key(chunk))
            .and_modify(|entry| *entry = None)
            .or_insert(Some(chunk));
    }
    let mut shas = BTreeSet::new();
    for chunk in by_address.into_values().flatten() {
        if chunk.role != BackupManifestRole::Atom || chunk.collection != ATOMS_COLLECTION {
            continue;
        }
        if !successor_groups.contains(&group_addr(chunk)) {
            continue;
        }
        if local_shas.contains(&chunk.sha256) {
            continue;
        }
        shas.insert(chunk.sha256.clone());
    }
    shas
}

/// Committed keep-set SHAs whose group still has a verified local atom chunk.
/// Empty on scope mismatch (fail closed: local-only).
pub(in super::super) fn committed_successor_history_shas(
    store: &LastStoreNamespacedStore,
    state: &PendingPurgedAtomRetirements,
    local: &[(BackupChunkRef, PathBuf)],
) -> BTreeSet<String> {
    if !committed_atom_prefixes_in_scope(store, state) {
        return BTreeSet::new();
    }
    committed_atom_extras_with_local_successor(state, local)
}

/// Report from stamp / dry-run of committed successor-history retirement.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct StampCommittedSuccessorHistoryReport {
    pub scope_ok: bool,
    pub retired_shas: BTreeSet<String>,
    pub retired_bytes: u64,
    pub groups_without_local_file: u64,
    pub unstamped_successor_history_refs: u64,
    pub local_shas: usize,
    pub pending_after: usize,
}

pub(super) fn successor_history_report(
    store: &LastStoreNamespacedStore,
    state: &PendingPurgedAtomRetirements,
    local: &[(BackupChunkRef, PathBuf)],
    pending_after: usize,
) -> StampCommittedSuccessorHistoryReport {
    let scope_ok = committed_atom_prefixes_in_scope(store, state);
    let retired_shas = committed_successor_history_shas(store, state, local);
    let successor_groups: BTreeSet<_> = local
        .iter()
        .filter(|(c, _)| c.role == BackupManifestRole::Atom && c.collection == ATOMS_COLLECTION)
        .map(|(c, _)| group_addr(c))
        .collect();
    let mut groups_without_local_file = BTreeSet::new();
    let mut retired_bytes = 0u64;
    if let Some(committed) = &state.committed_atom_prefixes {
        let mut seen_bytes = BTreeSet::new();
        for chunk in &committed.atom_chunks {
            if chunk.role != BackupManifestRole::Atom || chunk.collection != ATOMS_COLLECTION {
                continue;
            }
            if !successor_groups.contains(&group_addr(chunk)) {
                groups_without_local_file.insert(group_addr(chunk));
            }
            if retired_shas.contains(&chunk.sha256) && seen_bytes.insert(chunk.sha256.clone()) {
                retired_bytes = retired_bytes.saturating_add(chunk.bytes);
            }
        }
    }
    let pending_union: BTreeSet<_> = state.pending_shas.union(&retired_shas).cloned().collect();
    let unstamped = committed_atom_extras_with_local_successor(state, local)
        .difference(&pending_union)
        .count() as u64;
    StampCommittedSuccessorHistoryReport {
        scope_ok,
        retired_shas,
        retired_bytes,
        groups_without_local_file: groups_without_local_file.len() as u64,
        unstamped_successor_history_refs: unstamped,
        local_shas: local.len(),
        pending_after,
    }
}

/// Stamp committed successor history. Takes `atom_retirement_lock`.
/// Refuses if compaction_in_progress_shas or prefixes are nonempty.
pub fn stamp_pending_committed_successor_history(
    store: &LastStoreNamespacedStore,
) -> StorageResult<StampCommittedSuccessorHistoryReport> {
    let _retirement_guard = store
        .atom_retirement_lock
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    stamp_pending_committed_successor_history_locked(store)
}

/// Report-only. Takes `atom_retirement_lock` to read the sidecar. Does not write.
pub fn report_committed_successor_history(
    store: &LastStoreNamespacedStore,
) -> StorageResult<StampCommittedSuccessorHistoryReport> {
    let _retirement_guard = store
        .atom_retirement_lock
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    report_committed_successor_history_locked(store)
}

/// Verify every committed atom group with no local file against the exact
/// predecessor and a complete cloud list. This permits a backup-only cut to
/// retain a cloud copy without hiding a missing group from the new manifest.
pub(in super::super) fn missing_committed_atom_groups_have_cloud_copies(
    store: &LastStoreNamespacedStore,
    previous: Option<&BackupManifest>,
    current: &BackupManifest,
    cloud: Option<&CloudChunkPresence>,
    expected_missing_groups: u64,
) -> StorageResult<bool> {
    let (Some(previous), Some(cloud)) = (previous, cloud) else {
        return Ok(false);
    };
    if !cloud.listing_complete {
        return Ok(false);
    }
    let state = load_pending_purged_atom_retirements(store)?;
    let Some(committed) = state.committed_atom_prefixes.as_ref() else {
        return Ok(false);
    };
    if !committed_atom_prefixes_in_scope(store, &state)
        || committed.store_uuid != previous.store_uuid
        || committed.epoch != previous.epoch
        || committed.counter != previous.counter
        || committed.manifest_sha256 != manifest_sha256_hex(previous)?
        || committed.atom_chunks != previous.atom_chunks
    {
        return Ok(false);
    }
    let local_groups: BTreeSet<_> = local_atom_chunk_refs(store)?
        .iter()
        .filter(|(chunk, _)| {
            chunk.role == BackupManifestRole::Atom && chunk.collection == ATOMS_COLLECTION
        })
        .map(|(chunk, _)| group_addr(chunk))
        .collect();
    let missing_groups: BTreeSet<_> = committed
        .atom_chunks
        .iter()
        .filter(|chunk| {
            chunk.role == BackupManifestRole::Atom && chunk.collection == ATOMS_COLLECTION
        })
        .map(group_addr)
        .filter(|group| !local_groups.contains(group))
        .collect();
    if missing_groups.is_empty() || missing_groups.len() as u64 != expected_missing_groups {
        return Ok(false);
    }
    Ok(committed
        .atom_chunks
        .iter()
        .filter(|chunk| {
            chunk.role == BackupManifestRole::Atom
                && chunk.collection == ATOMS_COLLECTION
                && missing_groups.contains(&group_addr(chunk))
        })
        .all(|chunk| {
            current.atom_chunks.contains(chunk) && cloud.present_shas.contains(&chunk.sha256)
        }))
}

/// Caller already holds `atom_retirement_lock`.
pub(in super::super) fn stamp_pending_committed_successor_history_locked(
    store: &LastStoreNamespacedStore,
) -> StorageResult<StampCommittedSuccessorHistoryReport> {
    let mut state = load_pending_purged_atom_retirements(store)?;
    if !state.compaction_in_progress_shas.is_empty()
        || !state.compaction_in_progress_prefixes.is_empty()
    {
        return Err(StorageError::BackendError(
            "atom compaction retirement record is incomplete; refusing successor-history stamp"
                .to_string(),
        ));
    }
    let local = local_atom_chunk_refs(store)?;
    let retired = committed_successor_history_shas(store, &state, &local);
    state.pending_shas.extend(retired.iter().cloned());
    let pending_after = state.pending_shas.len();
    write_pending_purged_atom_retirements(store, &state)?;
    Ok(successor_history_report(
        store,
        &state,
        &local,
        pending_after,
    ))
}

/// Same compute as the write path. Does not extend pending. Does not write.
pub(in super::super) fn report_committed_successor_history_locked(
    store: &LastStoreNamespacedStore,
) -> StorageResult<StampCommittedSuccessorHistoryReport> {
    let state = load_pending_purged_atom_retirements(store)?;
    let local = local_atom_chunk_refs(store)?;
    let retired = committed_successor_history_shas(store, &state, &local);
    let pending_after = state.pending_shas.union(&retired).count();
    Ok(successor_history_report(
        store,
        &state,
        &local,
        pending_after,
    ))
}

/// Verify the prefix and its full local successor from the same open file.
/// Exact full-digest/length checks reject concurrent append or stale memo
/// metadata. An I/O failure stops compaction before any local file is removed.
pub(super) fn file_has_verified_prefix(
    path: &std::path::Path,
    prefix: &BackupChunkRef,
    current: &BackupChunkRef,
) -> StorageResult<bool> {
    use std::io::Read;
    if !fs::symlink_metadata(path)
        .map_err(StorageError::IoError)?
        .file_type()
        .is_file()
    {
        return Ok(false);
    }
    let mut file = fs::File::open(path).map_err(StorageError::IoError)?;
    let mut full = Sha256::new();
    let mut head = Sha256::new();
    let mut total = 0u64;
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf).map_err(StorageError::IoError)?;
        if n == 0 {
            break;
        }
        let head_bytes = prefix.bytes.saturating_sub(total).min(n as u64) as usize;
        head.update(&buf[..head_bytes]);
        full.update(&buf[..n]);
        total += n as u64;
        if total > current.bytes {
            return Ok(false);
        }
    }
    let full_sha = hex_lower(full.finalize());
    let head_sha = hex_lower(head.finalize());
    Ok(total == current.bytes && full_sha == current.sha256 && head_sha == prefix.sha256)
}
