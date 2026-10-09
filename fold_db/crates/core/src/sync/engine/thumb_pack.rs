//! Snapshot-cadence thumbnail packs.
//!
//! A pack is a blind byte-shuffle over already-sealed loose thumbnail
//! ciphertexts. It never opens per-thumbnail DEKs; the record-level
//! `FileThumbnailRef` still carries the access metadata needed by readers.

use super::*;
use crate::sharing::delivery_wire::{file_thumbnail_ref_from_atom_value, FileThumbnailRef};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub(crate) const THUMB_CACHE_NAMESPACE: &str = "sync_thumb_cache";
const THUMB_PACK_INDEX_NAMESPACE: &str = "sync_thumb_pack_index";
const THUMB_PACK_MAGIC: &[u8] = b"LASTDB_THUMB_PACK_V1\n";
const THUMB_LOOSE_PREFIX: &str = "thumbs/loose/sha256/";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ThumbPackIndexEntry {
    pub blob_ref: String,
    pub thumb_hash: String,
    pub offset: u64,
    pub len: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ThumbPackIndex {
    pub version: u32,
    pub pack_id: String,
    pub entries: Vec<ThumbPackIndexEntry>,
}

#[derive(Debug, Clone)]
pub(crate) struct ThumbPack {
    pub index: ThumbPackIndex,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CachedThumbPackEntry {
    pub pack_id: String,
    pub offset: u64,
    pub len: u64,
}

impl SyncEngine {
    pub(crate) async fn cache_thumb_ciphertext(
        &self,
        thumb_hash: &str,
        ciphertext: &[u8],
    ) -> SyncResult<()> {
        let cache = self.store.open_namespace(THUMB_CACHE_NAMESPACE).await?;
        cache
            .put(thumb_hash.as_bytes(), ciphertext.to_vec())
            .await?;
        Ok(())
    }

    pub(crate) async fn load_thumb_ciphertext(
        &self,
        thumb_hash: &str,
    ) -> SyncResult<Option<Vec<u8>>> {
        let cache = self.store.open_namespace(THUMB_CACHE_NAMESPACE).await?;
        if let Some(bytes) = cache.get(thumb_hash.as_bytes()).await? {
            return Ok(Some(bytes));
        }

        if let Some(entry) = self.cached_thumb_pack_entry(thumb_hash).await? {
            let url = self
                .auth
                .presign_thumb_pack_download(&entry.pack_id)
                .await?;
            if let Some(bytes) = self
                .s3
                .download_range(&url, entry.offset, entry.len)
                .await?
            {
                cache.put(thumb_hash.as_bytes(), bytes.clone()).await?;
                return Ok(Some(bytes));
            }
        }

        let url = self.auth.presign_thumb_download(thumb_hash).await?;
        let Some(bytes) = self.s3.download(&url).await? else {
            return Ok(None);
        };
        cache.put(thumb_hash.as_bytes(), bytes.clone()).await?;
        Ok(Some(bytes))
    }

    async fn cached_thumb_pack_entry(
        &self,
        thumb_hash: &str,
    ) -> SyncResult<Option<CachedThumbPackEntry>> {
        let index = self
            .store
            .open_namespace(THUMB_PACK_INDEX_NAMESPACE)
            .await?;
        let Some(bytes) = index.get(thumb_hash.as_bytes()).await? else {
            return Ok(None);
        };
        serde_json::from_slice(&bytes).map(Some).map_err(|e| {
            SyncError::Serialization(format!("cached thumb pack index decode failed: {e}"))
        })
    }

    async fn cache_thumb_pack(&self, pack_id: &str, bytes: &[u8]) -> SyncResult<usize> {
        let (index, data) = decode_thumb_pack_index(bytes)?;
        if index.pack_id != pack_id {
            return Err(SyncError::Serialization(format!(
                "thumbnail pack id mismatch: expected {pack_id}, got {}",
                index.pack_id
            )));
        }

        let cache = self.store.open_namespace(THUMB_CACHE_NAMESPACE).await?;
        let pack_index = self
            .store
            .open_namespace(THUMB_PACK_INDEX_NAMESPACE)
            .await?;
        for entry in &index.entries {
            let start = entry.offset as usize;
            let end = start.saturating_add(entry.len as usize);
            if end > data.len() {
                return Err(SyncError::Serialization(format!(
                    "thumbnail pack entry {} range {}..{} exceeds data length {}",
                    entry.thumb_hash,
                    start,
                    end,
                    data.len()
                )));
            }
            cache
                .put(entry.thumb_hash.as_bytes(), data[start..end].to_vec())
                .await?;
            let absolute_offset = bytes.len().saturating_sub(data.len()) as u64 + entry.offset;
            let cached = CachedThumbPackEntry {
                pack_id: pack_id.to_string(),
                offset: absolute_offset,
                len: entry.len,
            };
            pack_index
                .put(
                    entry.thumb_hash.as_bytes(),
                    serde_json::to_vec(&cached)
                        .map_err(|e| SyncError::Serialization(e.to_string()))?,
                )
                .await?;
        }
        Ok(index.entries.len())
    }

    pub(crate) async fn warm_thumb_cache_for_snapshot(
        &self,
        snapshot: &Snapshot,
        pack_id: &str,
    ) -> SyncResult<usize> {
        let refs = collect_snapshot_thumbnail_refs(snapshot)?;
        if refs.is_empty() {
            return Ok(0);
        }

        let pack_url = self.auth.presign_thumb_pack_download(pack_id).await?;
        if let Some(bytes) = self.s3.download(&pack_url).await? {
            // `cache_thumb_pack` returns how many thumbs it unpacked into the
            // local cache — that is the warm count. Do not re-count hashes that
            // `load_thumb_ciphertext` then finds as cache hits (double-count).
            return self.cache_thumb_pack(pack_id, &bytes).await;
        }

        // Pack download missed; count only thumbs already resident locally.
        let mut warmed = 0usize;
        for thumb_hash in refs.keys() {
            if self.load_thumb_ciphertext(thumb_hash).await?.is_some() {
                warmed += 1;
            }
        }
        Ok(warmed)
    }

    pub(crate) async fn upload_thumb_pack_for_snapshot(
        &self,
        snapshot: &Snapshot,
        pack_id: &str,
    ) -> SyncResult<Option<usize>> {
        let refs = collect_snapshot_thumbnail_refs(snapshot)?;
        if refs.is_empty() {
            return Ok(None);
        }

        let mut ciphertexts = BTreeMap::new();
        for (thumb_hash, thumb_ref) in &refs {
            let Some(ciphertext) = self.load_thumb_ciphertext(thumb_hash).await? else {
                return Err(SyncError::S3(format!(
                    "live thumbnail {thumb_hash} is missing from local cache and loose storage"
                )));
            };
            if ciphertext.len() as u64 != thumb_ref.access.encrypted_size_bytes {
                return Err(SyncError::S3(format!(
                    "live thumbnail {thumb_hash} ciphertext size {} did not match declared {}",
                    ciphertext.len(),
                    thumb_ref.access.encrypted_size_bytes
                )));
            }
            ciphertexts.insert(thumb_hash.clone(), ciphertext);
        }

        let pack = build_thumb_pack(pack_id, &refs, &ciphertexts)?;
        let url = self
            .auth
            .presign_thumb_pack_upload(pack_id, pack.bytes.len() as u64)
            .await?;
        self.s3.upload_snapshot(&url, pack.bytes).await?;
        if let Err(e) = self.auth.confirm_thumb_pack_upload(pack_id).await {
            tracing::warn!(
                target: "fold_db::sync",
                pack_id,
                error = %e,
                "confirm_thumb_pack_upload metering failed (non-fatal)"
            );
        }

        // After a successful pack upload, delete only the loose objects we just
        // packed (ciphertexts.keys() == pack index / live refs). Do NOT walk
        // list_objects(thumbs/loose/) and delete every leaf: concurrent
        // upload_file_thumbnail (or another snapshot's loose objects) can appear
        // in the listing but not in this pack — deleting them races and loses
        // objects that are still the only copy of a live thumb.
        // Route through the same policy helper tests use (listed set = packed
        // bare hashes when we skip the dangerous full loose listing).
        let to_delete =
            loose_thumb_hashes_to_delete_after_pack(ciphertexts.keys(), ciphertexts.keys());
        for thumb_hash in &to_delete {
            match self.auth.presign_thumb_delete(thumb_hash).await {
                Ok(delete_url) => {
                    if let Err(e) = self.s3.delete(&delete_url).await {
                        tracing::warn!(
                            target: "fold_db::sync",
                            thumb_hash = %thumb_hash,
                            error = %e,
                            "failed to delete packed loose thumbnail (non-fatal)"
                        );
                        continue;
                    }
                    if let Err(e) = self.auth.confirm_thumb_delete(thumb_hash).await {
                        tracing::warn!(
                            target: "fold_db::sync",
                            thumb_hash = %thumb_hash,
                            error = %e,
                            "confirm_thumb_delete metering failed (non-fatal)"
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        target: "fold_db::sync",
                        thumb_hash = %thumb_hash,
                        error = %e,
                        "failed to presign loose thumbnail delete (non-fatal)"
                    );
                }
            }
        }

        Ok(Some(pack.index.entries.len()))
    }
}

/// Loose thumbnail hashes that may be deleted after a successful pack upload.
///
/// Only hashes that were included in this pack are safe to remove. A broader
/// `list_objects(thumbs/loose/…)` set can include concurrent uploads that are
/// not in the pack and must not be deleted.
pub(crate) fn loose_thumb_hashes_to_delete_after_pack(
    packed_hashes: impl IntoIterator<Item = impl AsRef<str>>,
    listed_loose_keys: impl IntoIterator<Item = impl AsRef<str>>,
) -> Vec<String> {
    let packed: std::collections::BTreeSet<String> = packed_hashes
        .into_iter()
        .map(|h| h.as_ref().to_string())
        .collect();
    let mut out = Vec::new();
    for key in listed_loose_keys {
        let key = key.as_ref();
        let hash = key.strip_prefix(THUMB_LOOSE_PREFIX).unwrap_or(key);
        if packed.contains(hash) {
            out.push(hash.to_string());
        }
    }
    out
}

pub(crate) fn collect_snapshot_thumbnail_refs(
    snapshot: &Snapshot,
) -> SyncResult<BTreeMap<String, FileThumbnailRef>> {
    let mut refs = BTreeMap::new();
    for namespace in &snapshot.namespaces {
        for entry in &namespace.entries {
            let value_bytes = BASE64.decode(&entry.value).map_err(|e| {
                SyncError::Serialization(format!("snapshot value base64 decode failed: {e}"))
            })?;
            let Ok(value) = serde_json::from_slice::<serde_json::Value>(&value_bytes) else {
                continue;
            };
            let thumb = match file_thumbnail_ref_from_atom_value(&value) {
                Ok(Some(thumb)) => thumb,
                Ok(None) => continue,
                Err(e) => {
                    return Err(SyncError::Serialization(format!(
                        "snapshot contains invalid thumbnail reference: {e}"
                    )));
                }
            };
            let thumb_hash = thumb.blob_ref.strip_prefix("sha256:").ok_or_else(|| {
                SyncError::Serialization(format!(
                    "thumbnail blob_ref must use sha256: prefix, got {}",
                    thumb.blob_ref
                ))
            })?;
            refs.entry(thumb_hash.to_string()).or_insert(thumb);
        }
    }
    Ok(refs)
}

pub(crate) fn build_thumb_pack(
    pack_id: &str,
    refs: &BTreeMap<String, FileThumbnailRef>,
    ciphertexts: &BTreeMap<String, Vec<u8>>,
) -> SyncResult<ThumbPack> {
    let mut data = Vec::new();
    let mut entries = Vec::with_capacity(refs.len());
    for (thumb_hash, thumb_ref) in refs {
        let ciphertext = ciphertexts.get(thumb_hash).ok_or_else(|| {
            SyncError::S3(format!(
                "missing ciphertext for live thumbnail {thumb_hash}"
            ))
        })?;
        let offset = data.len() as u64;
        data.extend_from_slice(ciphertext);
        entries.push(ThumbPackIndexEntry {
            blob_ref: thumb_ref.blob_ref.clone(),
            thumb_hash: thumb_hash.clone(),
            offset,
            len: ciphertext.len() as u64,
        });
    }

    let index = ThumbPackIndex {
        version: 1,
        pack_id: pack_id.to_string(),
        entries,
    };
    let index_json =
        serde_json::to_vec(&index).map_err(|e| SyncError::Serialization(e.to_string()))?;
    let mut bytes = Vec::with_capacity(THUMB_PACK_MAGIC.len() + 8 + index_json.len() + data.len());
    bytes.extend_from_slice(THUMB_PACK_MAGIC);
    bytes.extend_from_slice(&(index_json.len() as u64).to_be_bytes());
    bytes.extend_from_slice(&index_json);
    bytes.extend_from_slice(&data);
    Ok(ThumbPack { index, bytes })
}

pub(crate) fn decode_thumb_pack_index(bytes: &[u8]) -> SyncResult<(ThumbPackIndex, &[u8])> {
    if !bytes.starts_with(THUMB_PACK_MAGIC) {
        return Err(SyncError::Serialization(
            "thumbnail pack magic mismatch".to_string(),
        ));
    }
    let len_start = THUMB_PACK_MAGIC.len();
    let len_end = len_start + 8;
    if bytes.len() < len_end {
        return Err(SyncError::Serialization(
            "thumbnail pack missing index length".to_string(),
        ));
    }
    let mut len_bytes = [0u8; 8];
    len_bytes.copy_from_slice(&bytes[len_start..len_end]);
    let index_len = u64::from_be_bytes(len_bytes) as usize;
    let index_end = len_end + index_len;
    if bytes.len() < index_end {
        return Err(SyncError::Serialization(
            "thumbnail pack truncated index".to_string(),
        ));
    }
    let index: ThumbPackIndex = serde_json::from_slice(&bytes[len_end..index_end])
        .map_err(|e| SyncError::Serialization(e.to_string()))?;
    Ok((index, &bytes[index_end..]))
}
