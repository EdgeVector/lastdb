//! Single-chunk download, size-cap retry and digest verification for S0 restore.

use super::*;
use crate::sync::s3::PresignedUrl;

/// Hard fence on a restore-time size-cap retry. Matches the manifest download
/// budget so a poisoned Content-Length cannot pin RSS.
pub(super) const BACKUP_CHUNK_DOWNLOAD_HARD_CAP: usize = 16 * 1024 * 1024;

const RESTORE_DOWNLOAD_CONCURRENCY: usize = 8;
const RESTORE_DOWNLOAD_BUFFER_BYTES: u64 = 128 * 1024 * 1024;

pub(super) fn download_reservation(chunk: &BackupChunkRef) -> u64 {
    let file_and_pack = chunk
        .pack
        .as_ref()
        .map_or(chunk.bytes, |pack| chunk.bytes.saturating_add(pack.bytes));
    file_and_pack.max(BACKUP_CHUNK_DOWNLOAD_HARD_CAP as u64)
}

pub(super) fn download_fits(count: usize, reserved: u64, chunk: &BackupChunkRef) -> bool {
    count == 0
        || (count < RESTORE_DOWNLOAD_CONCURRENCY
            && download_reservation(chunk)
                <= RESTORE_DOWNLOAD_BUFFER_BYTES.saturating_sub(reserved))
}

fn backup_chunk_id(chunk: &BackupChunkRef) -> String {
    format!("chunk_uuid={} sha256={}", chunk.chunk_uuid, chunk.sha256)
}

/// Parse the Content-Length named by a `download_limited` preflight refusal.
///
/// Only the header-preflight wording is retryable. The streamed "no
/// Content-Length" path has no observed size, so restore must not guess a cap.
fn oversize_content_length(err: &SyncError) -> Option<usize> {
    let SyncError::S3(msg) = err else {
        return None;
    };
    let rest = msg.strip_prefix("object content-length ")?;
    let n_str = rest.split_whitespace().next()?;
    n_str.parse().ok()
}

fn annotate_backup_chunk_error(err: SyncError, chunk: &BackupChunkRef) -> SyncError {
    let id = backup_chunk_id(chunk);
    match err {
        SyncError::S3(msg) if !msg.contains("chunk_uuid=") => {
            SyncError::S3(format!("{msg} ({id})"))
        }
        SyncError::Storage(msg) if !msg.contains("chunk_uuid=") => {
            SyncError::Storage(format!("{msg} ({id})"))
        }
        SyncError::Crypto(msg) if !msg.contains("chunk_uuid=") => {
            SyncError::Crypto(format!("{msg} ({id})"))
        }
        other => other,
    }
}

pub(in crate::sync::engine) async fn download_backup_chunk(
    auth: &AuthClient,
    s3: &S3Client,
    chunk: &BackupChunkRef,
) -> SyncResult<Vec<u8>> {
    download_backup_chunk_with_progress(auth, s3, chunk, None, None).await
}

pub(super) async fn download_backup_chunk_with_progress(
    auth: &AuthClient,
    s3: &S3Client,
    chunk: &BackupChunkRef,
    progress: Option<&RestoreProgress>,
    pack_cache: Option<&packs::VerifiedPackCache>,
) -> SyncResult<Vec<u8>> {
    if chunk.pack.is_some() {
        return packs::download_packed_file(auth, s3, chunk, progress, pack_cache).await;
    }
    let presigned = progress::measure(
        progress,
        TransferOperation::Authorization,
        auth.presign_backup_chunk_download(&chunk.sha256),
    )
    .await?;
    let declared = usize::try_from(chunk.bytes).map_err(|_| {
        SyncError::Storage(format!(
            "backup chunk too large: {} ({})",
            chunk.bytes,
            backup_chunk_id(chunk)
        ))
    })?;
    let bytes = fetch_chunk_bytes(s3, &presigned, chunk, declared, progress).await?;
    progress::update(progress, |p| {
        p.response_body_bytes = p.response_body_bytes.saturating_add(bytes.len() as u64);
    });
    verify_chunk_bytes(chunk, declared, bytes)
}

/// Download the chunk body limited to its declared size, with one bounded
/// retry when the stored object is larger than the manifest declares.
async fn fetch_chunk_bytes(
    s3: &S3Client,
    presigned: &PresignedUrl,
    chunk: &BackupChunkRef,
    declared: usize,
    progress: Option<&RestoreProgress>,
) -> SyncResult<Vec<u8>> {
    let first = progress::measure(
        progress,
        TransferOperation::Download,
        s3.download_limited(presigned, Some(declared)),
    )
    .await;
    match first {
        Ok(Some(bytes)) => Ok(bytes),
        Ok(None) => Err(SyncError::Storage(format!(
            "backup chunk {} missing",
            backup_chunk_id(chunk)
        ))),
        Err(err) => {
            // Restore-only retry: a content-addressed dedup hit can leave a
            // stored object whose Content-Length disagrees with this
            // manifest's `chunk.bytes` (observed 746 vs declared 701 on a
            // `sync_pin_log` chunk). ENC:/ENZ:/ENB: reseal changes the file
            // hash, so those format flips mint a new key and do not cause
            // this. Existence-only upload HEAD cannot repair the stored
            // object; sha256 of the downloaded bytes is the authority.
            let Some(observed) = oversize_content_length(&err) else {
                return Err(annotate_backup_chunk_error(err, chunk));
            };
            if observed > BACKUP_CHUNK_DOWNLOAD_HARD_CAP {
                return Err(SyncError::S3(format!(
                    "object content-length {observed} exceeds max_download_entry_bytes {declared} ({})",
                    backup_chunk_id(chunk)
                )));
            }
            progress::measure(
                progress,
                TransferOperation::Download,
                s3.download_limited(presigned, Some(observed)),
            )
            .await
            .map_err(|e| annotate_backup_chunk_error(e, chunk))?
            .ok_or_else(|| {
                SyncError::Storage(format!("backup chunk {} missing", backup_chunk_id(chunk)))
            })
        }
    }
}

/// Accept the bytes only when their sha256 equals the manifest digest.
fn verify_chunk_bytes(
    chunk: &BackupChunkRef,
    declared: usize,
    mut bytes: Vec<u8>,
) -> SyncResult<Vec<u8>> {
    let actual = sha256_hex(&bytes);
    if actual != chunk.sha256 {
        // Old uploaders read a live plain segment after the manifest cut.
        // The stored object can contain the exact cut plus later appends.
        // Recover only the declared prefix whose digest proves every byte.
        // No length-only fallback can authorize data from a wrong object.
        if bytes.len() > declared && sha256_hex(&bytes[..declared]) == chunk.sha256 {
            tracing::warn!(
                target: "fold_db::sync::restore",
                chunk_uuid = %chunk.chunk_uuid,
                sha256 = %chunk.sha256,
                manifest_bytes = chunk.bytes,
                downloaded_bytes = bytes.len(),
                "recovered exact manifest bytes from a legacy backup object with later appends"
            );
            bytes.truncate(declared);
            return Ok(bytes);
        }
        return Err(SyncError::Crypto(format!(
            "backup chunk sha256 mismatch: expected {}, got {actual} {} downloaded_bytes={}",
            chunk.sha256,
            backup_chunk_id(chunk),
            bytes.len()
        )));
    }
    if bytes.len() as u64 != chunk.bytes {
        tracing::warn!(
            target: "fold_db::sync::restore",
            chunk_uuid = %chunk.chunk_uuid,
            sha256 = %chunk.sha256,
            manifest_bytes = chunk.bytes,
            downloaded_bytes = bytes.len(),
            "backup chunk declared size disagreed with stored object; sha256 matched, accepting stored bytes"
        );
    }
    Ok(bytes)
}

pub(in crate::sync::engine) fn backup_restore_chunk_order(
    chunk: &BackupChunkRef,
) -> (u8, u64, String, u16, Option<u32>, String) {
    let role = match chunk.role {
        BackupManifestRole::Atom => 0,
        BackupManifestRole::Mutable if chunk.collection == "log" => 1,
        BackupManifestRole::Mutable => 2,
    };
    (
        role,
        chunk.end_csn,
        chunk.collection.clone(),
        chunk.shard,
        chunk.group_id,
        chunk.chunk_uuid.clone(),
    )
}
