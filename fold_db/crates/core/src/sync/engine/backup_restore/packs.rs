//! Read one original LastStore file from a byte-for-byte cloud pack.

use super::*;

pub(super) async fn download_packed_file(
    auth: &AuthClient,
    s3: &S3Client,
    chunk: &BackupChunkRef,
    progress: Option<&RestoreProgress>,
) -> SyncResult<Vec<u8>> {
    let pack = chunk
        .pack
        .as_ref()
        .ok_or_else(|| SyncError::Storage("backup pack location missing".into()))?;
    let packed = download_verified_pack(auth, s3, pack, progress).await?;
    extract_file(chunk, &packed)
}

async fn download_verified_pack(
    auth: &AuthClient,
    s3: &S3Client,
    pack: &crate::storage::laststore::BackupPackLocation,
    progress: Option<&RestoreProgress>,
) -> SyncResult<Vec<u8>> {
    let declared = usize::try_from(pack.bytes)
        .map_err(|_| SyncError::Storage(format!("backup pack too large: {}", pack.bytes)))?;
    if declared > BACKUP_CHUNK_DOWNLOAD_HARD_CAP {
        return Err(SyncError::Storage(format!(
            "backup pack exceeds restore limit: {}",
            pack.bytes
        )));
    }
    let presigned = progress::measure(
        progress,
        TransferOperation::Authorization,
        auth.presign_backup_chunk_download(&pack.sha256),
    )
    .await?;
    let packed = progress::measure(
        progress,
        TransferOperation::Download,
        s3.download_limited(&presigned, Some(declared)),
    )
    .await?
    .ok_or_else(|| SyncError::Storage(format!("backup pack {} missing", pack.sha256)))?;
    progress::update(progress, |p| {
        p.response_body_bytes = p.response_body_bytes.saturating_add(packed.len() as u64);
    });
    if packed.len() != declared || sha256_hex(&packed) != pack.sha256 {
        return Err(SyncError::Crypto(format!(
            "backup pack sha256 or length mismatch: {}",
            pack.sha256
        )));
    }
    Ok(packed)
}

fn extract_file(chunk: &BackupChunkRef, packed: &[u8]) -> SyncResult<Vec<u8>> {
    let pack = chunk
        .pack
        .as_ref()
        .ok_or_else(|| SyncError::Storage("backup pack location missing".into()))?;
    let start = usize::try_from(pack.offset)
        .map_err(|_| SyncError::Storage("backup pack offset exceeds local address space".into()))?;
    let length = usize::try_from(pack.length).map_err(|_| {
        SyncError::Storage("backup pack file length exceeds local address space".into())
    })?;
    let end = start
        .checked_add(length)
        .ok_or_else(|| SyncError::Storage("backup pack file range overflow".into()))?;
    let bytes = packed
        .get(start..end)
        .ok_or_else(|| SyncError::Storage("backup pack file range exceeds pack bytes".into()))?;
    if pack.length != chunk.bytes || sha256_hex(bytes) != chunk.sha256 {
        return Err(SyncError::Crypto(format!(
            "backup packed file sha256 or length mismatch: {}",
            chunk.sha256
        )));
    }
    Ok(bytes.to_vec())
}
