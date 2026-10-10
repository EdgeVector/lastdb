//! Read original LastStore files from byte-for-byte cloud packs.

use super::*;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::{Mutex, OnceCell};

// The cache belongs to one restore. Active queue requests hold at most eight
// additional packs; completed packs retained for later files stay under both
// limits after each ordered install.
const VERIFIED_PACK_CACHE_BYTES: usize = 64 * 1024 * 1024;
const VERIFIED_PACK_CACHE_ENTRIES: usize = 32;

#[derive(Default)]
pub(super) struct VerifiedPackCache {
    state: Mutex<VerifiedPackCacheState>,
}

#[derive(Default)]
struct VerifiedPackCacheState {
    entries: BTreeMap<String, VerifiedPackEntry>,
    cached_bytes: usize,
    use_number: u64,
}

struct VerifiedPackEntry {
    bytes: Arc<OnceCell<Arc<Vec<u8>>>>,
    size: usize,
    last_used: u64,
}

impl VerifiedPackCache {
    async fn get_verified(
        &self,
        auth: &AuthClient,
        s3: &S3Client,
        pack: &crate::storage::laststore::BackupPackLocation,
        progress: Option<&RestoreProgress>,
    ) -> SyncResult<Arc<Vec<u8>>> {
        let declared = declared_pack_bytes(pack)?;
        let cell = {
            let mut state = self.state.lock().await;
            state.use_number = state.use_number.saturating_add(1);
            let use_number = state.use_number;
            let entry =
                state
                    .entries
                    .entry(pack.sha256.clone())
                    .or_insert_with(|| VerifiedPackEntry {
                        bytes: Arc::new(OnceCell::new()),
                        size: 0,
                        last_used: use_number,
                    });
            entry.last_used = use_number;
            Arc::clone(&entry.bytes)
        };
        // One initializer downloads and verifies the object. Other requests
        // for the same SHA wait for that result without a second cloud GET.
        let packed = Arc::clone(
            cell.get_or_try_init(|| async {
                download_verified_pack(auth, s3, pack, progress)
                    .await
                    .map(Arc::new)
            })
            .await?,
        );
        if packed.len() != declared {
            return Err(SyncError::Crypto(format!(
                "backup pack length mismatch: {}",
                pack.sha256
            )));
        }
        let mut state = self.state.lock().await;
        state.use_number = state.use_number.saturating_add(1);
        let use_number = state.use_number;
        let added = if let Some(entry) = state.entries.get_mut(&pack.sha256) {
            if Arc::ptr_eq(&entry.bytes, &cell) {
                entry.last_used = use_number;
                if entry.size == 0 {
                    entry.size = packed.len();
                    packed.len()
                } else {
                    0
                }
            } else {
                0
            }
        } else {
            0
        };
        state.cached_bytes = state.cached_bytes.saturating_add(added);
        state.trim();
        Ok(packed)
    }

    pub(super) async fn trim(&self) {
        self.state.lock().await.trim();
    }
}

impl VerifiedPackCacheState {
    fn trim(&mut self) {
        while self.cached_bytes > VERIFIED_PACK_CACHE_BYTES
            || self.entries.len() > VERIFIED_PACK_CACHE_ENTRIES
        {
            let oldest = self
                .entries
                .iter()
                .filter(|(_, entry)| {
                    entry.bytes.get().is_some() && Arc::strong_count(&entry.bytes) == 1
                })
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(sha, _)| sha.clone());
            let Some(oldest) = oldest else {
                // The restore queue bounds entries that current requests hold.
                break;
            };
            if let Some(entry) = self.entries.remove(&oldest) {
                self.cached_bytes = self.cached_bytes.saturating_sub(entry.size);
            }
        }
    }
}

pub(super) async fn download_packed_file(
    auth: &AuthClient,
    s3: &S3Client,
    chunk: &BackupChunkRef,
    progress: Option<&RestoreProgress>,
    cache: Option<&VerifiedPackCache>,
) -> SyncResult<Vec<u8>> {
    let pack = chunk
        .pack
        .as_ref()
        .ok_or_else(|| SyncError::Storage("backup pack location missing".into()))?;
    if let Some(cache) = cache {
        let packed = cache.get_verified(auth, s3, pack, progress).await?;
        extract_file(chunk, packed.as_slice())
    } else {
        let packed = download_verified_pack(auth, s3, pack, progress).await?;
        extract_file(chunk, &packed)
    }
}

fn declared_pack_bytes(pack: &crate::storage::laststore::BackupPackLocation) -> SyncResult<usize> {
    let declared = usize::try_from(pack.bytes)
        .map_err(|_| SyncError::Storage(format!("backup pack too large: {}", pack.bytes)))?;
    if declared > BACKUP_CHUNK_DOWNLOAD_HARD_CAP {
        return Err(SyncError::Storage(format!(
            "backup pack exceeds restore limit: {}",
            pack.bytes
        )));
    }
    Ok(declared)
}

async fn download_verified_pack(
    auth: &AuthClient,
    s3: &S3Client,
    pack: &crate::storage::laststore::BackupPackLocation,
    progress: Option<&RestoreProgress>,
) -> SyncResult<Vec<u8>> {
    let declared = declared_pack_bytes(pack)?;
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
