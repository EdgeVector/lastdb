//! Preserve complete surviving members when a cloud pack needs replacement.

use super::*;
use std::collections::BTreeSet;

pub(super) fn finish_pack_candidates(
    manifest: &mut BackupManifest,
    directory: &Path,
    retained: BTreeMap<String, Vec<BackupChunkUploadCandidate>>,
    locations: &BTreeMap<FileKey, (String, BackupPackLocation)>,
    ready: &mut Vec<BackupChunkUploadCandidate>,
) {
    for members in retained.into_values() {
        if let Some(candidate) = retained_pack_candidate(directory, members) {
            ready.push(candidate);
        }
    }
    for chunk in manifest
        .atom_chunks
        .iter_mut()
        .chain(&mut manifest.mutable_chunks)
    {
        if let Some((sha, location)) = locations.get(&file_key(chunk)) {
            if &chunk.sha256 == sha {
                chunk.pack = Some(location.clone());
            }
        }
    }
}

/// A pack must be replaced as a unit when a member changes, leaves, or its
/// cloud object disappears. Otherwise an old pack can retain deleted bytes.
pub(super) fn repack_dirty_survivors(
    manifest: &mut BackupManifest,
    candidates: &mut [BackupChunkUploadCandidate],
    previous: Option<&BackupManifest>,
    cloud_presence: Option<&CloudChunkPresence>,
) -> SyncResult<()> {
    let dirty = {
        let current_by_key: BTreeMap<_, _> = manifest
            .atom_chunks
            .iter()
            .chain(&manifest.mutable_chunks)
            .map(|chunk| (file_key(chunk), chunk))
            .collect();
        let mut dirty = BTreeSet::new();
        for prior in previous
            .into_iter()
            .flat_map(|prior| prior.atom_chunks.iter().chain(&prior.mutable_chunks))
        {
            let Some(pack) = &prior.pack else { continue };
            let same = current_by_key.get(&file_key(prior)).is_some_and(|current| {
                current.sha256 == prior.sha256
                    && current.bytes == prior.bytes
                    && current.pack.as_ref() == Some(pack)
            });
            let cloud_has_pack = cloud_presence.is_none_or(|presence| {
                !presence.listing_complete || presence.present_shas.contains(&pack.sha256)
            });
            if !same || !cloud_has_pack {
                dirty.insert(pack.sha256.as_str());
            }
        }
        dirty
    };
    if dirty.is_empty() {
        return Ok(());
    }
    let local_by_key: BTreeMap<_, _> = candidates
        .iter()
        .map(|candidate| {
            (
                file_key(&candidate.chunk),
                (candidate.chunk.sha256.clone(), candidate.chunk.bytes),
            )
        })
        .collect();
    for chunk in manifest
        .atom_chunks
        .iter_mut()
        .chain(&mut manifest.mutable_chunks)
    {
        if !chunk
            .pack
            .as_ref()
            .is_some_and(|pack| dirty.contains(pack.sha256.as_str()))
        {
            continue;
        }
        let local = local_by_key.get(&file_key(chunk));
        if !local.is_some_and(|(sha, bytes)| sha == &chunk.sha256 && *bytes == chunk.bytes) {
            return Err(SyncError::Storage(format!(
                "cannot replace backup pack: surviving file {}/{} is absent locally",
                chunk.collection, chunk.chunk_uuid
            )));
        }
        chunk.pack = None;
    }
    for candidate in candidates {
        if candidate
            .chunk
            .pack
            .as_ref()
            .is_some_and(|pack| dirty.contains(pack.sha256.as_str()))
        {
            candidate.chunk.pack = None;
        }
    }
    Ok(())
}
