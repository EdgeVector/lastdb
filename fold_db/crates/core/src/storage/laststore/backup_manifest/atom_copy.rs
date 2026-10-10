//! Exact manifest-prefix proof under the backup packing lock.

use super::*;

/// Closed counts only: no paths, chunk names, keys, or record bytes.
#[derive(Debug, Default, Clone, Copy)]
pub struct AtomPhotographCopyReport {
    pub manifest_chunks: usize,
    pub local_chunks: usize,
    pub verified_local_prefixes: usize,
    pub verified_cloud_copies: usize,
    pub missing_local_chunks: usize,
    pub mismatched_local_prefixes: usize,
    pub unexpected_local_chunks: usize,
    pub invalid_manifest_refs: usize,
}

impl AtomPhotographCopyReport {
    pub fn is_complete(&self) -> bool {
        self.invalid_manifest_refs == 0
            && self.mismatched_local_prefixes == 0
            && self.unexpected_local_chunks == 0
            && self.verified_local_prefixes + self.verified_cloud_copies == self.manifest_chunks
    }
}

/// Bind by physical address, then verify exactly the manifest length and SHA.
/// Plain SegmentLog files may append after the cut. The packing lock prevents
/// rewrite, so growth cannot invalidate a verified manifest prefix. New local
/// addresses, missing files, duplicate refs, and changed prefixes still refuse.
/// Backup-only passes `false`: a stopped S0 source must not grow after its cut.
pub(in super::super) fn atom_photograph_copy_report(
    store: &LastStoreNamespacedStore,
    manifest: &BackupManifest,
    cloud: Option<&CloudChunkPresence>,
    allow_append_growth: bool,
) -> StorageResult<AtomPhotographCopyReport> {
    let mut report = AtomPhotographCopyReport {
        manifest_chunks: manifest.atom_chunks.len(),
        ..Default::default()
    };
    let mut by_address = BTreeMap::new();
    for chunk in &manifest.atom_chunks {
        if chunk.role != BackupManifestRole::Atom || chunk.collection != ATOMS_COLLECTION {
            report.invalid_manifest_refs += 1;
            continue;
        }
        if by_address.insert(chunk_key(chunk), chunk).is_some() {
            report.invalid_manifest_refs += 1;
        }
    }
    let mut seen = BTreeSet::new();
    for listed in store
        .store
        .enumerate_chunks(ATOMS_COLLECTION)
        .map_err(LastStoreKvStore::map_error)?
    {
        // Keep format/integrity verification for every local file, including
        // unexpected addresses. A corrupt file cannot become a cloud-only ref.
        let meta = store
            .store
            .verify_chunk_at(&listed)
            .map_err(LastStoreKvStore::map_error)?;
        report.local_chunks += 1;
        let address = (
            meta.collection,
            meta.shard,
            meta.group_id,
            meta.chunk_uuid.to_string(),
        );
        if !seen.insert(address.clone()) {
            report.unexpected_local_chunks += 1;
            continue;
        }
        let Some(chunk) = by_address.get(&address) else {
            report.unexpected_local_chunks += 1;
            continue;
        };
        if file_has_manifest_prefix(&meta.path, chunk, allow_append_growth)? {
            report.verified_local_prefixes += 1;
        } else {
            report.mismatched_local_prefixes += 1;
        }
    }
    for (address, chunk) in by_address {
        if seen.contains(&address) {
            continue;
        }
        report.missing_local_chunks += 1;
        if cloud
            .is_some_and(|view| view.listing_complete && view.present_shas.contains(&chunk.sha256))
        {
            report.verified_cloud_copies += 1;
        }
    }
    Ok(report)
}

/// Replacement authority remains the existing pending-retirement sidecar.
/// A changed prior ref without that authority stops the cut. Even with the
/// authority, the selected successor must contain the exact prior prefix.
pub(in super::super) fn verify_selected_atom_replacements(
    previous: Option<&BackupManifest>,
    selected: &[(BackupChunkRef, PathBuf)],
    pending: &PendingPurgedAtomRetirements,
) -> StorageResult<()> {
    let Some(previous) = previous else {
        return Ok(());
    };
    let mut prior_by_address = BTreeMap::new();
    for chunk in &previous.atom_chunks {
        if chunk.role != BackupManifestRole::Atom
            || chunk.collection != ATOMS_COLLECTION
            || prior_by_address.insert(chunk_key(chunk), chunk).is_some()
        {
            return Err(StorageError::BackendError(
                "atom predecessor contains an invalid or duplicate physical address".into(),
            ));
        }
    }
    for (current, path) in selected {
        if current.role != BackupManifestRole::Atom {
            continue;
        }
        let Some(prior) = prior_by_address.get(&chunk_key(current)) else {
            continue;
        };
        if current.sha256 == prior.sha256 && current.bytes == prior.bytes {
            continue;
        }
        if !pending.pending_shas.contains(&prior.sha256) {
            return Err(StorageError::BackendError(
                "atom selected replacement lacks existing retirement evidence; retry the fenced cut".into(),
            ));
        }
        if prior.bytes >= current.bytes
            || !file_has_manifest_prefix(path, current, true)?
            || !file_has_manifest_prefix(path, prior, true)?
        {
            return Err(StorageError::BackendError(
                "atom selected replacement does not contain the exact predecessor prefix".into(),
            ));
        }
    }
    Ok(())
}

fn file_has_manifest_prefix(
    path: &std::path::Path,
    chunk: &BackupChunkRef,
    allow_append_growth: bool,
) -> StorageResult<bool> {
    use std::io::Read;
    if !fs::symlink_metadata(path)
        .map_err(StorageError::IoError)?
        .file_type()
        .is_file()
    {
        return Ok(false);
    }
    let file = fs::File::open(path).map_err(StorageError::IoError)?;
    let current_bytes = file.metadata().map_err(StorageError::IoError)?.len();
    if current_bytes < chunk.bytes || (!allow_append_growth && current_bytes != chunk.bytes) {
        return Ok(false);
    }
    let mut prefix = file.take(chunk.bytes);
    let mut hasher = Sha256::new();
    let mut total = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let count = prefix.read(&mut buffer).map_err(StorageError::IoError)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
        total += count as u64;
    }
    let final_bytes = prefix
        .into_inner()
        .metadata()
        .map_err(StorageError::IoError)?
        .len();
    Ok(total == chunk.bytes
        && hex_lower(hasher.finalize()) == chunk.sha256
        && (allow_append_growth || final_bytes == chunk.bytes))
}
