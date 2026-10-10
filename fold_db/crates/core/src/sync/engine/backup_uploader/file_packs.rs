//! Byte-for-byte packs for completed LastStore files.

use super::*;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

const MAX_PACK_BYTES: u64 = 8 * 1024 * 1024;
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

/// Hash the exact file prefixes without a staged pack. The cut keeps the
/// source files under its packing lock until each pack is uploaded or retired.
fn plan_pack(
    directory: &Path,
    group: Vec<BackupChunkUploadCandidate>,
) -> SyncResult<(
    BackupChunkUploadCandidate,
    Vec<(FileKey, String, BackupPackLocation)>,
)> {
    let mut pack_hash = Sha256::new();
    let mut locations = Vec::with_capacity(group.len());
    let mut offset = 0u64;
    for candidate in &group {
        let mut input = std::fs::File::open(&candidate.path).map_err(|error| {
            SyncError::Storage(format!(
                "backup pack source {}/{} unavailable: {error}",
                candidate.chunk.collection, candidate.chunk.chunk_uuid
            ))
        })?;
        let mut member_hash = Sha256::new();
        let mut remaining = candidate.chunk.bytes;
        let mut buffer = [0u8; 64 * 1024];
        while remaining > 0 {
            let to_read = remaining.min(buffer.len() as u64) as usize;
            let count = input.read(&mut buffer[..to_read])?;
            if count == 0 {
                return Err(SyncError::Storage("backup pack source ended early".into()));
            }
            member_hash.update(&buffer[..count]);
            pack_hash.update(&buffer[..count]);
            remaining -= count as u64;
        }
        let actual = format!("{:x}", member_hash.finalize());
        if actual != candidate.chunk.sha256 {
            return Err(SyncError::Crypto(format!(
                "backup pack source sha256 mismatch: chunk_uuid={} expected={} got={actual}",
                candidate.chunk.chunk_uuid, candidate.chunk.sha256
            )));
        }
        locations.push((
            file_key(&candidate.chunk),
            candidate.chunk.sha256.clone(),
            offset,
            candidate.chunk.bytes,
        ));
        offset += candidate.chunk.bytes;
    }
    let sha256 = format!("{:x}", pack_hash.finalize());
    let refs = locations
        .into_iter()
        .map(|(key, original_sha, member_offset, length)| {
            (
                key,
                original_sha,
                BackupPackLocation {
                    sha256: sha256.clone(),
                    offset: member_offset,
                    length,
                    bytes: offset,
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
            path: directory.to_path_buf(),
            pack_members: Some(group),
        },
        refs,
    ))
}

/// Build one short-lived pack only after the cloud asks for its bytes. The
/// temporary file is unlinked, but its open handle stays valid through PUT.
pub(super) fn open_verified_backup_pack_candidate(
    candidate: &BackupChunkUploadCandidate,
) -> SyncResult<Option<std::fs::File>> {
    let members = candidate
        .pack_members
        .as_ref()
        .ok_or_else(|| SyncError::Storage("backup pack upload has no source members".into()))?;
    if members.is_empty()
        || members.len() > MAX_PACK_FILES
        || candidate.chunk.bytes > MAX_PACK_BYTES
    {
        return Err(SyncError::Storage(
            "backup pack exceeds its file or byte bound".into(),
        ));
    }
    std::fs::create_dir_all(&candidate.path)?;
    let mut output = tempfile::tempfile_in(&candidate.path)?;
    let mut pack_hash = Sha256::new();
    let mut bytes = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    for member in members {
        let Some(mut input) = open_verified_backup_candidate(member)? else {
            return Ok(None);
        };
        let mut remaining = member.chunk.bytes;
        while remaining > 0 {
            let to_read = remaining.min(buffer.len() as u64) as usize;
            let count = input.read(&mut buffer[..to_read])?;
            if count == 0 {
                return Err(SyncError::Storage("backup pack source ended early".into()));
            }
            output.write_all(&buffer[..count])?;
            pack_hash.update(&buffer[..count]);
            remaining -= count as u64;
            bytes += count as u64;
        }
    }
    let actual = format!("{:x}", pack_hash.finalize());
    if bytes != candidate.chunk.bytes || actual != candidate.chunk.sha256 {
        return Err(SyncError::Crypto(format!(
            "backup pack sha256 mismatch: expected={} got={actual} expected_bytes={} source_bytes={bytes}",
            candidate.chunk.sha256, candidate.chunk.bytes
        )));
    }
    output.seek(SeekFrom::Start(0))?;
    Ok(Some(output))
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
    // Keep each new pack's members close to one another in restore order.
    // The manifest still maps each member by its original file key.
    candidates.sort_by_key(|candidate| {
        super::super::backup_restore::backup_restore_chunk_order(&candidate.chunk)
    });
    let directory = store
        .backup_cut_freeze_dir(manifest.counter)
        .ok_or_else(|| SyncError::Storage("backup pack needs a local sidecar directory".into()))?;
    let mut ready = Vec::new();
    let mut pending = Vec::new();
    let mut pending_bytes = 0u64;
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
            && candidate.chunk.bytes <= MAX_PACK_BYTES;
        if !can_pack
            || pending.len() >= MAX_PACK_FILES
            || pending_bytes + candidate.chunk.bytes > MAX_PACK_BYTES
        {
            flush_pack(
                &directory,
                &mut pending,
                &mut pending_bytes,
                &mut ready,
                &mut locations,
            )?;
        }
        if can_pack {
            pending_bytes += candidate.chunk.bytes;
            pending.push(candidate);
        } else if candidate.chunk.pack.is_none() {
            ready.push(candidate);
        }
    }
    flush_pack(
        &directory,
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
    pending: &mut Vec<BackupChunkUploadCandidate>,
    pending_bytes: &mut u64,
    ready: &mut Vec<BackupChunkUploadCandidate>,
    locations: &mut BTreeMap<FileKey, (String, BackupPackLocation)>,
) -> SyncResult<()> {
    if !pending.is_empty() {
        let (pack, refs) = plan_pack(directory, std::mem::take(pending))?;
        for (key, sha, location) in refs {
            locations.insert(key, (sha, location));
        }
        ready.push(pack);
    }
    *pending_bytes = 0;
    Ok(())
}
