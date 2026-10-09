use super::*;

pub(super) fn make_verified_primary_resume_root(
    previous: &BackupManifest,
    manifest: &mut BackupManifest,
) -> SyncResult<()> {
    validate_manifest_chain(Some(previous), manifest).map_err(|error| {
        SyncError::Storage(format!(
            "primary resume predecessor step is invalid: {error}"
        ))
    })?;
    manifest.previous_manifest_sha256 = None;
    manifest.deletion_receipts.clear();
    validate_manifest_chain(None, manifest).map_err(|error| {
        SyncError::Storage(format!(
            "primary resume independent root is invalid: {error}"
        ))
    })
}

pub(super) fn require_primary_resume_manifest_identity(
    manifest: &BackupManifest,
    expected: &super::super::primary_resume::PrimaryResumeCutIdentity,
) -> SyncResult<()> {
    let sha = manifest_sha256_hex(manifest)
        .map_err(|error| SyncError::Storage(format!("hash primary resume cut: {error}")))?;
    if manifest.store_uuid != expected.store_uuid
        || manifest.epoch != expected.epoch
        || manifest.counter != expected.counter
        || !sha.eq_ignore_ascii_case(&expected.manifest_sha256)
        || manifest.previous_manifest_sha256.is_some()
    {
        return Err(SyncError::Storage(
            "primary resume cut differs from its durable root intent".into(),
        ));
    }
    Ok(())
}

/// Open and verify the exact byte version named by a manifest before any PUT.
///
/// Plain SegmentLog snapshots flush their active file without sealing it. A
/// later append can therefore enlarge `candidate.path` after the cut hashes
/// it. Uploading that live path would store the new bytes under the old SHA.
/// Hash only the manifest prefix and retain that same open file handle. The
/// packing lock prevents rewrite; later appends cannot change the prefix.
/// The fixed-size buffer bounds RSS even for a large chunk. A missing file
/// follows the existing SourceMissing policy. Present but wrong bytes remain
/// a named failure; verification cannot silently retire them from a held cut.
pub(super) fn open_verified_backup_candidate(
    candidate: &BackupChunkUploadCandidate,
) -> SyncResult<Option<std::fs::File>> {
    use sha2::{Digest, Sha256};
    use std::io::{Read, Seek, SeekFrom};

    let source = match std::fs::File::open(&candidate.path) {
        Ok(source) => source,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(SyncError::Io(error)),
    };
    let mut source = source.take(candidate.chunk.bytes);
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    let mut copied = 0u64;
    loop {
        let count = source.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
        copied = copied.saturating_add(count as u64);
    }
    let actual = format!("{:x}", hasher.finalize());
    if copied != candidate.chunk.bytes || actual != candidate.chunk.sha256 {
        return Err(SyncError::Crypto(format!(
            "backup source sha256 mismatch: chunk_uuid={} expected={} got={actual} manifest_bytes={} source_bytes={copied}",
            candidate.chunk.chunk_uuid, candidate.chunk.sha256, candidate.chunk.bytes
        )));
    }
    let mut source = source.into_inner();
    source.seek(SeekFrom::Start(0))?;
    Ok(Some(source))
}

/// The second local walk resolves paths, not a new content version. A plain
/// segment can gain appends between the manifest cut and this walk. Preserve
/// the manifest digest and length at the same chunk coordinates; ignore files
/// created after the cut. Upload verifies that the retained prefix still fits.
pub(super) fn bind_backup_candidates_to_manifest(
    manifest: &BackupManifest,
    candidates: Vec<BackupChunkUploadCandidate>,
) -> Vec<BackupChunkUploadCandidate> {
    let chunks: std::collections::BTreeMap<_, _> = manifest
        .atom_chunks
        .iter()
        .chain(&manifest.mutable_chunks)
        .map(|chunk| {
            (
                (
                    chunk.collection.as_str(),
                    chunk.shard,
                    chunk.group_id,
                    chunk.chunk_uuid.as_str(),
                ),
                chunk,
            )
        })
        .collect();
    candidates
        .into_iter()
        .filter_map(|mut candidate| {
            let key = (
                candidate.chunk.collection.as_str(),
                candidate.chunk.shard,
                candidate.chunk.group_id,
                candidate.chunk.chunk_uuid.as_str(),
            );
            candidate.chunk = (*chunks.get(&key)?).clone();
            Some(candidate)
        })
        .collect()
}
