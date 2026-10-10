//! Byte-for-byte packs for small completed LastStore files.

use super::*;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::path::Path;

const MAX_PACK_BYTES: u64 = 8 * 1024 * 1024;
// The DEV proof stages at most this much data before CAS. Larger cuts keep
// later files direct until a bounded prepare/upload/release path exists.
const MAX_STAGED_PACK_BYTES: u64 = 64 * 1024 * 1024;
const MAX_PACK_FILES: usize = 8;
type FileKey = (String, u16, Option<u32>, String);

impl SyncEngine {
    pub(super) async fn require_backup_file_pack_capability(
        &self,
        previous: Option<&BackupManifest>,
        fresh_root_proven: bool,
    ) -> SyncResult<()> {
        if previous.is_none()
            && !fresh_root_proven
            && self
                .auth
                .backup_latest_get_optional()
                .await?
                .is_some_and(|tip| tip.latest.format_version() == PACKED_MANIFEST_VERSION)
        {
            return Err(SyncError::Storage(
                "cloud has a packed backup tip but the local predecessor manifest is unavailable"
                    .into(),
            ));
        }
        if previous.is_some_and(|prior| prior.version == PACKED_MANIFEST_VERSION)
            || std::env::var("LASTDB_BACKUP_FILE_PACKS").is_ok_and(|value| value == "1")
        {
            self.auth
                .require_backup_format_version_2_capability()
                .await?;
        }
        Ok(())
    }

    pub(super) fn prepare_backup_file_packs(
        &self,
        store: &crate::storage::laststore::LastStoreNamespacedStore,
        manifest: &mut BackupManifest,
        candidates: Vec<BackupChunkUploadCandidate>,
        previous: Option<&BackupManifest>,
        cloud_presence: Option<&CloudChunkPresence>,
        fresh_proof: Option<&FreshCloudProof>,
        primary_resume_root: bool,
    ) -> SyncResult<Vec<BackupChunkUploadCandidate>> {
        self.sweep_backup_freeze_dirs(None);
        let ready = pack_backup_publish_candidates(
            store,
            manifest,
            candidates,
            previous,
            cloud_presence,
            fresh_proof,
        )?;
        let chain_previous = if primary_resume_root { None } else { previous };
        validate_manifest_chain(chain_previous, manifest)
            .map_err(|error| SyncError::Storage(format!("packed manifest invalid: {error}")))?;
        Ok(ready)
    }
}

fn file_key(chunk: &crate::storage::laststore::BackupChunkRef) -> FileKey {
    (
        chunk.collection.clone(),
        chunk.shard,
        chunk.group_id,
        chunk.chunk_uuid.clone(),
    )
}

/// A pack must be replaced as a unit when a member changes, leaves, or its
/// cloud object disappears. Otherwise an old pack can retain deleted bytes.
fn repack_dirty_survivors(
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

fn write_pack(
    directory: &Path,
    generation: u64,
    index: usize,
    group: &[BackupChunkUploadCandidate],
) -> SyncResult<(
    BackupChunkUploadCandidate,
    Vec<(FileKey, String, BackupPackLocation)>,
)> {
    std::fs::create_dir_all(directory)?;
    let temporary = directory.join(format!("pack-{generation}-{index}.tmp"));
    let result = (|| {
        let mut output = std::fs::File::create(&temporary)?;
        let mut pack_hash = Sha256::new();
        let mut locations = Vec::with_capacity(group.len());
        let mut offset = 0u64;
        for candidate in group {
            let mut input = open_verified_backup_candidate(candidate)?.ok_or_else(|| {
                SyncError::Storage("backup pack source file disappeared after cut".into())
            })?;
            let mut remaining = candidate.chunk.bytes;
            let mut buffer = [0u8; 64 * 1024];
            while remaining > 0 {
                let to_read = remaining.min(buffer.len() as u64) as usize;
                let count = input.read(&mut buffer[..to_read])?;
                if count == 0 {
                    return Err(SyncError::Storage("backup pack source ended early".into()));
                }
                output.write_all(&buffer[..count])?;
                pack_hash.update(&buffer[..count]);
                remaining -= count as u64;
            }
            locations.push((
                file_key(&candidate.chunk),
                candidate.chunk.sha256.clone(),
                offset,
            ));
            offset += candidate.chunk.bytes;
        }
        output.sync_all()?;
        let sha256 = format!("{:x}", pack_hash.finalize());
        let path = directory.join(format!("{sha256}.pack"));
        std::fs::rename(&temporary, &path)?;
        let total_bytes = offset;
        let refs = locations
            .into_iter()
            .zip(group)
            .map(|((key, original_sha, offset), candidate)| {
                (
                    key,
                    original_sha,
                    BackupPackLocation {
                        sha256: sha256.clone(),
                        offset,
                        length: candidate.chunk.bytes,
                        bytes: total_bytes,
                    },
                )
            })
            .collect();
        let mut synthetic = group[0].chunk.clone();
        synthetic.collection = "backup_pack".into();
        synthetic.chunk_uuid.clone_from(&sha256);
        synthetic.role = BackupManifestRole::Mutable;
        synthetic.sha256 = sha256;
        synthetic.bytes = offset;
        synthetic.pack = None;
        Ok((
            BackupChunkUploadCandidate {
                chunk: synthetic,
                path,
            },
            refs,
        ))
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

pub(super) fn pack_backup_publish_candidates(
    store: &crate::storage::laststore::LastStoreNamespacedStore,
    manifest: &mut BackupManifest,
    mut candidates: Vec<BackupChunkUploadCandidate>,
    previous: Option<&BackupManifest>,
    cloud_presence: Option<&CloudChunkPresence>,
    fresh_proof: Option<&FreshCloudProof>,
) -> SyncResult<Vec<BackupChunkUploadCandidate>> {
    if manifest.version != PACKED_MANIFEST_VERSION {
        return Ok(candidates);
    }
    repack_dirty_survivors(manifest, &mut candidates, previous, cloud_presence)?;
    let directory = store
        .backup_cut_freeze_dir(manifest.counter)
        .ok_or_else(|| SyncError::Storage("backup pack needs a local sidecar directory".into()))?;
    let mut ready = Vec::new();
    let mut pending = Vec::new();
    let mut pending_bytes = 0u64;
    let mut staged_bytes = 0u64;
    let mut pack_index = 0usize;
    let mut locations = BTreeMap::new();
    let prior_direct: BTreeMap<_, _> = previous
        .into_iter()
        .flat_map(|manifest| manifest.atom_chunks.iter().chain(&manifest.mutable_chunks))
        .filter(|chunk| chunk.pack.is_none())
        .map(|chunk| (file_key(chunk), (chunk.sha256.as_str(), chunk.bytes)))
        .collect();
    for candidate in candidates {
        let already_direct =
            prior_direct
                .get(&file_key(&candidate.chunk))
                .is_some_and(|(sha, bytes)| {
                    *sha == candidate.chunk.sha256 && *bytes == candidate.chunk.bytes
                });
        let can_pack = candidate.chunk.pack.is_none()
            && candidate.chunk.role == BackupManifestRole::Mutable
            && !matches!(candidate.chunk.collection.as_str(), "blobs" | "cas_blobs")
            && !already_direct
            && !fresh_proof.is_some_and(|proof| proof.contains_sha(&candidate.chunk.sha256))
            && candidate.chunk.bytes > 0
            && candidate.chunk.bytes <= MAX_PACK_BYTES
            && staged_bytes.saturating_add(candidate.chunk.bytes) <= MAX_STAGED_PACK_BYTES;
        if !can_pack
            || pending.len() >= MAX_PACK_FILES
            || pending_bytes + candidate.chunk.bytes > MAX_PACK_BYTES
        {
            flush_pack(
                &directory,
                manifest.counter,
                &mut pack_index,
                &mut pending,
                &mut pending_bytes,
                &mut ready,
                &mut locations,
            )?;
        }
        if can_pack {
            staged_bytes += candidate.chunk.bytes;
            pending_bytes += candidate.chunk.bytes;
            pending.push(candidate);
        } else if candidate.chunk.pack.is_none() {
            ready.push(candidate);
        }
    }
    flush_pack(
        &directory,
        manifest.counter,
        &mut pack_index,
        &mut pending,
        &mut pending_bytes,
        &mut ready,
        &mut locations,
    )?;
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
    Ok(ready)
}

fn flush_pack(
    directory: &Path,
    generation: u64,
    index: &mut usize,
    pending: &mut Vec<BackupChunkUploadCandidate>,
    pending_bytes: &mut u64,
    ready: &mut Vec<BackupChunkUploadCandidate>,
    locations: &mut BTreeMap<FileKey, (String, BackupPackLocation)>,
) -> SyncResult<()> {
    if !pending.is_empty() {
        let (pack, refs) = write_pack(directory, generation, *index, pending)?;
        for (key, sha, location) in refs {
            locations.insert(key, (sha, location));
        }
        ready.push(pack);
        *index += 1;
    } else {
        ready.append(pending);
    }
    pending.clear();
    *pending_bytes = 0;
    Ok(())
}
