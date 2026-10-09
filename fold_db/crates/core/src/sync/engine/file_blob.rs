//! Referenced-file blob upload/delete helpers.
//!
//! Cloud sync backs up personal CAS file blobs by uploading newly-created
//! ciphertext and deleting explicitly-removed blobs. It must not list or bulk
//! download `{scope}/cas/sha256/*` during ordinary sync/bootstrap/device-join;
//! blob reads are an explicit, on-demand recovery/query concern.

use super::super::*;
use crate::crypto::{CryptoProvider, LocalCryptoProvider};
use crate::hex::hex_lower;
use crate::sharing::delivery_wire::{
    file_blob_cipher_suite_supported, FileThumbnailAccess, FileThumbnailRef, FILE_THUMBNAIL_TIER,
};
// The pure convergent seal lives in `sharing` so a node without a sync engine
// derives the same `blob_ref`, DEK and pointer. `?` converts its error into
// `SyncError::Crypto` with the unchanged message.
pub use crate::sharing::file_blob_seal::FileBlobRef;
use crate::sharing::file_blob_seal::{decode_hex_32, seal_file_blob};
use crate::sync::engine::{FileBlobDurability, FILE_BLOB_ABSENT_IDENTITY_CAP};
use crate::sync::error::SyncResult;
use sha2::{Digest, Sha256};
use std::sync::atomic::Ordering;

const FILE_BLOB_KNOWN_NAMESPACE: &str = "sync_file_blob_known";

/// Caller-supplied generated thumbnail/poster bytes for one file blob.
///
/// Platform surfaces own decoding video frames or image-resizing policy. The
/// sync engine only seals the already-small derivative, writes it to the R2
/// thumbnail tier, and returns the reference stored in the file pointer.
#[derive(Debug, Clone)]
pub struct FileThumbnailUpload {
    pub bytes: Vec<u8>,
    pub media_type: String,
    pub width: u32,
    pub height: u32,
    pub kind: String,
}

fn hash_from_sha256_blob_ref(blob_ref: &str) -> SyncResult<&str> {
    blob_ref.strip_prefix("sha256:").ok_or_else(|| {
        SyncError::Crypto(format!(
            "thumbnail blob_ref must use sha256: prefix, got {blob_ref}"
        ))
    })
}

async fn open_file_blob(blob_ref: &FileBlobRef, ciphertext: &[u8]) -> SyncResult<Vec<u8>> {
    if !file_blob_cipher_suite_supported(&blob_ref.cipher_suite) {
        return Err(SyncError::Crypto(format!(
            "unsupported file blob cipher suite '{}'",
            blob_ref.cipher_suite
        )));
    }
    let dek = decode_hex_32(&blob_ref.dek)?;
    let plaintext = LocalCryptoProvider::from_key(dek)
        .decrypt(ciphertext)
        .await?;
    let digest = hex_lower(Sha256::digest(&plaintext));
    if digest != blob_ref.file_hash {
        return Err(SyncError::Crypto(format!(
            "file blob plaintext hash mismatch: expected {}, got {digest}",
            blob_ref.file_hash
        )));
    }
    let expected_ref = format!("sha256:{digest}");
    if blob_ref.blob_ref != expected_ref {
        return Err(SyncError::Crypto(format!(
            "file blob_ref mismatch: expected {expected_ref}, got {}",
            blob_ref.blob_ref
        )));
    }
    Ok(plaintext)
}

impl SyncEngine {
    async fn known_file_blob_ref(&self, file_hash: &str) -> SyncResult<Option<FileBlobRef>> {
        let known = self.store.open_namespace(FILE_BLOB_KNOWN_NAMESPACE).await?;
        let Some(bytes) = known.get(file_hash.as_bytes()).await? else {
            return Ok(None);
        };
        let blob_ref = serde_json::from_slice::<FileBlobRef>(&bytes).map_err(|e| {
            SyncError::Serialization(format!(
                "known file blob ref for {file_hash} is invalid: {e}"
            ))
        })?;
        if blob_ref.file_hash != file_hash {
            return Err(SyncError::Serialization(format!(
                "known file blob ref hash mismatch: expected {file_hash}, got {}",
                blob_ref.file_hash
            )));
        }
        Ok(Some(blob_ref))
    }

    async fn remember_file_blob_ref(&self, blob_ref: &FileBlobRef) -> SyncResult<()> {
        let known = self.store.open_namespace(FILE_BLOB_KNOWN_NAMESPACE).await?;
        let bytes = serde_json::to_vec(blob_ref).map_err(|e| {
            SyncError::Serialization(format!(
                "known file blob ref for {} could not serialize: {e}",
                blob_ref.file_hash
            ))
        })?;
        known.put(blob_ref.file_hash.as_bytes(), bytes).await?;
        Ok(())
    }

    async fn forget_file_blob_ref(&self, file_hash: &str) -> SyncResult<()> {
        let known = self.store.open_namespace(FILE_BLOB_KNOWN_NAMESPACE).await?;
        known.delete(file_hash.as_bytes()).await?;
        Ok(())
    }

    /// Upload a personal content-addressed file blob and best-effort confirm it
    /// for storage metering after the object PUT succeeds.
    ///
    /// The caller passes plaintext bytes. This method seals them under a
    /// convergent per-content DEK (same plaintext → same DEK and ciphertext on
    /// every device) and returns the metadata that must be stored with the DB
    /// pointer or shared slice so authorized readers can open the ciphertext.
    ///
    /// **Known-ref short-circuit is conditional on remote presence.** A prior
    /// successful upload is remembered under `sync_file_blob_known` so a second
    /// put of the same plaintext can reuse the DEK (and so row pointers stay
    /// valid). That memo is a claim about REMOTE state held locally, so it can
    /// outlive the object (bucket/backend migration, remote GC, quota reap).
    /// Callers that re-upload after a 404 (LastGit `--backfill-file-blobs`,
    /// repair) must not get the known DEK back with no PUT — that left the
    /// next fetch 404 forever. When the known object is missing (or the
    /// opened plaintext no longer matches), we re-seal with the *same* DEK and
    /// PUT again so every existing pointer that carries that DEK still opens.
    /// [`Self::download_file_blob`] may also drop the memo when it proves the
    /// object is gone; this path does not depend on that alone.
    pub async fn upload_file_blob(&self, plaintext: &[u8]) -> SyncResult<FileBlobRef> {
        self.ensure_file_blob_upload_allowed().await?;
        let file_hash = hex_lower(Sha256::digest(plaintext));
        if let Some(blob_ref) = self.known_file_blob_ref(&file_hash).await? {
            match self.download_file_blob(&blob_ref).await {
                Ok(Some(bytes)) if bytes.as_slice() == plaintext => {
                    return Ok(blob_ref);
                }
                Ok(Some(_)) => {
                    tracing::warn!(
                        target: "fold_db::sync",
                        file_hash = %file_hash,
                        "known file blob opened but plaintext hash mismatched stored ref; re-uploading with same DEK"
                    );
                }
                Ok(None) => {
                    tracing::warn!(
                        target: "fold_db::sync",
                        file_hash = %file_hash,
                        "known file blob missing from remote CAS; re-uploading with same DEK so existing pointers stay valid"
                    );
                }
                Err(e) => {
                    // Transport / auth failure: do not silently re-PUT (could thrash
                    // on a flaky link while the object is still present). Surface it.
                    return Err(e);
                }
            }
            return self.reupload_known_file_blob(plaintext, &blob_ref).await;
        }

        let (blob_ref, encrypted_blob) = seal_file_blob(plaintext)?;
        self.put_encrypted_file_blob(&blob_ref, encrypted_blob)
            .await?;
        Ok(blob_ref)
    }

    /// Re-encrypt `plaintext` under the known DEK and PUT the ciphertext.
    ///
    /// Preserves `blob_ref` / `dek` so pointers already written to the DB keep
    /// working. Used when `sync_file_blob_known` says we uploaded before but
    /// remote CAS no longer has the object.
    async fn reupload_known_file_blob(
        &self,
        plaintext: &[u8],
        known: &FileBlobRef,
    ) -> SyncResult<FileBlobRef> {
        let digest = hex_lower(Sha256::digest(plaintext));
        if digest != known.file_hash {
            return Err(SyncError::Crypto(format!(
                "reupload plaintext hash {digest} does not match known file_hash {}",
                known.file_hash
            )));
        }
        let dek = decode_hex_32(&known.dek)?;
        let encrypted = LocalCryptoProvider::from_key(dek)
            .encrypt(plaintext)
            .await?;
        let mut blob_ref = known.clone();
        blob_ref.encrypted_size_bytes = encrypted.len() as u64;
        self.put_encrypted_file_blob(&blob_ref, encrypted).await?;
        Ok(blob_ref)
    }

    async fn put_encrypted_file_blob(
        &self,
        blob_ref: &FileBlobRef,
        encrypted_blob: Vec<u8>,
    ) -> SyncResult<()> {
        self.ensure_file_blob_upload_allowed().await?;
        let file_hash = blob_ref.file_hash.as_str();
        let estimated_size_bytes = encrypted_blob.len() as u64;
        // Re-presign on every attempt: SlowDown / transport backoff can outlive
        // a single presigned URL, and a failed PUT must not remember the blob.
        self.retry_s3(&format!("file-blob upload {file_hash}"), || {
            let hash = file_hash.to_string();
            let bytes = encrypted_blob.clone();
            async move {
                self.ensure_file_blob_upload_allowed().await?;
                let url = self
                    .auth
                    .presign_file_upload(&hash, estimated_size_bytes)
                    .await?;
                self.s3.upload(&url, bytes).await
            }
        })
        .await?;

        if let Err(e) = self.auth.confirm_file_upload(file_hash, None).await {
            tracing::warn!(
                target: "fold_db::sync",
                error = %e,
                file_hash = %file_hash,
                "confirm_file_upload metering failed (non-fatal; reconcile will heal)"
            );
        }

        self.remember_file_blob_ref(blob_ref).await?;
        Ok(())
    }

    /// Upload one caller-supplied generated thumbnail/poster as a sealed loose
    /// R2 thumbnail object and return the pointer metadata to persist with the
    /// parent `$lastdb_file` reference.
    pub async fn upload_file_thumbnail(
        &self,
        source_hash: &str,
        thumbnail: &FileThumbnailUpload,
    ) -> SyncResult<FileThumbnailRef> {
        self.ensure_file_blob_upload_allowed().await?;
        if thumbnail.width == 0 || thumbnail.height == 0 {
            return Err(SyncError::Serialization(
                "thumbnail width and height must be non-zero".to_string(),
            ));
        }
        if thumbnail.kind != "image" && thumbnail.kind != "video-poster" {
            return Err(SyncError::Serialization(
                "thumbnail kind must be image or video-poster".to_string(),
            ));
        }
        if thumbnail.media_type.trim().is_empty() {
            return Err(SyncError::Serialization(
                "thumbnail media_type is required".to_string(),
            ));
        }

        let (thumb_ref, encrypted_thumb) = seal_file_blob(&thumbnail.bytes)?;
        let thumb_hash = thumb_ref.file_hash.as_str();
        let estimated_size_bytes = encrypted_thumb.len() as u64;
        self.cache_thumb_ciphertext(thumb_hash, &encrypted_thumb)
            .await?;
        self.retry_s3(&format!("file-thumb upload {thumb_hash}"), || {
            let hash = thumb_hash.to_string();
            let bytes = encrypted_thumb.clone();
            async move {
                self.ensure_file_blob_upload_allowed().await?;
                let url = self
                    .auth
                    .presign_thumb_upload(&hash, estimated_size_bytes)
                    .await?;
                self.s3.upload(&url, bytes).await
            }
        })
        .await?;

        Ok(FileThumbnailRef {
            blob_ref: thumb_ref.blob_ref,
            tier: FILE_THUMBNAIL_TIER.to_string(),
            access: FileThumbnailAccess {
                dek: thumb_ref.dek,
                cipher_suite: thumb_ref.cipher_suite,
                encrypted_size_bytes: thumb_ref.encrypted_size_bytes,
            },
            media_type: thumbnail.media_type.clone(),
            width: thumbnail.width,
            height: thumbnail.height,
            kind: thumbnail.kind.clone(),
            source_hash: source_hash.to_string(),
        })
    }

    async fn ensure_file_blob_upload_allowed(&self) -> SyncResult<()> {
        if self.backup_only_mode.load(Ordering::SeqCst) || !self.cloud_plane_allows_upload().await {
            return Err(SyncError::Storage(
                "cloud sync Off: file blob upload refused".into(),
            ));
        }
        Ok(())
    }

    /// Download and open a sealed loose thumbnail object.
    pub async fn download_file_thumbnail(
        &self,
        thumbnail: &FileThumbnailRef,
    ) -> SyncResult<Option<Vec<u8>>> {
        let thumb_hash = hash_from_sha256_blob_ref(&thumbnail.blob_ref)?;
        let Some(ciphertext) = self.load_thumb_ciphertext(thumb_hash).await? else {
            return Ok(None);
        };
        let blob_ref = FileBlobRef {
            blob_ref: thumbnail.blob_ref.clone(),
            file_hash: thumb_hash.to_string(),
            owner_scope: None,
            cipher_suite: thumbnail.access.cipher_suite.clone(),
            dek: thumbnail.access.dek.clone(),
            encrypted_size_bytes: thumbnail.access.encrypted_size_bytes,
        };
        open_file_blob(&blob_ref, &ciphertext).await.map(Some)
    }

    /// Download and open a personal content-addressed file blob using the DEK
    /// metadata returned by [`Self::upload_file_blob`].
    ///
    /// A `Ok(None)` here is a PROOF that the remote object is absent — the
    /// presign succeeded and the GET was answered with "not found", as opposed
    /// to a transport or auth failure, which surfaces as `Err`. That proof
    /// falsifies the `sync_file_blob_known` memo, so the memo is dropped: the
    /// next `upload_file_blob` of the same bytes re-seals and re-PUTs for real
    /// instead of short-circuiting on a claim we have just disproved.
    ///
    /// Dropping it costs at most one redundant upload if the object comes back
    /// (content-addressed objects do not), and never costs access to an
    /// existing one — the authoritative DEK lives on the persisted pointer,
    /// not in the memo.
    pub async fn download_file_blob(&self, blob_ref: &FileBlobRef) -> SyncResult<Option<Vec<u8>>> {
        let ciphertext = self
            .retry_s3(
                &format!("file-blob download {}", blob_ref.file_hash),
                || {
                    let hash = blob_ref.file_hash.clone();
                    let owner_scope = blob_ref.owner_scope.clone();
                    async move {
                        let url = self
                            .auth
                            .presign_file_download_owner_scope(&hash, owner_scope.as_deref())
                            .await?;
                        self.s3.download(&url).await
                    }
                },
            )
            .await?;
        let Some(ciphertext) = ciphertext else {
            self.forget_missing_file_blob(&blob_ref.file_hash).await;
            return Ok(None);
        };
        open_file_blob(blob_ref, &ciphertext).await.map(Some)
    }

    /// Drop the "already uploaded" memo for a blob the remote CAS has just
    /// answered `not found` for.
    ///
    /// Best-effort on purpose: failing a download because the memo could not
    /// be cleaned would turn a recoverable miss into an error, and the miss is
    /// the answer the caller asked for. A failed forget only means the next
    /// upload still short-circuits, which is the behaviour we already had.
    async fn forget_missing_file_blob(&self, file_hash: &str) {
        match self.known_file_blob_ref(file_hash).await {
            Ok(None) => {
                // No memo: bytes this node never claimed to have uploaded, so
                // the remote having none of them is an ordinary miss. Counted
                // anyway — without the benign class the durability class below
                // has nothing to be read against, and the `404` the caller saw
                // is indistinguishable between the two at the request layer.
                self.file_blob_absent_without_memo
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
            Ok(Some(_)) => self.record_file_blob_absent_with_memo(file_hash).await,
            Err(e) => {
                tracing::warn!(
                    target: "fold_db::sync",
                    error = %e,
                    file_hash = %file_hash,
                    "could not read known file blob memo while clearing a proven-missing blob"
                );
                return;
            }
        }
        match self.forget_file_blob_ref(file_hash).await {
            Ok(()) => tracing::warn!(
                target: "fold_db::sync",
                file_hash = %file_hash,
                "remote CAS has no object for a file blob this node recorded as uploaded; \
                 cleared the memo so the next upload of these bytes is a real one"
            ),
            Err(e) => tracing::warn!(
                target: "fold_db::sync",
                error = %e,
                file_hash = %file_hash,
                "could not clear the known file blob memo for a proven-missing blob; \
                 re-uploading these bytes will keep being a no-op"
            ),
        }
    }

    /// Book one proven-absent fetch for bytes this node had recorded as
    /// uploaded — the durability class of [`FileBlobDurability`].
    ///
    /// Off the healthy read path entirely: this runs only after a presigned GET
    /// has already answered "not found" for a blob we hold a memo for, so the
    /// lock is never contended by ordinary traffic. Once the identity cap is
    /// reached the atomic check short-circuits and the lock is not taken again,
    /// so a badly damaged CAS cannot serialize its own reads.
    async fn record_file_blob_absent_with_memo(&self, file_hash: &str) {
        self.file_blob_absent_with_memo
            .fetch_add(1, Ordering::Relaxed);
        if self
            .file_blob_absent_identities_capped
            .load(Ordering::Relaxed)
        {
            return;
        }
        let mut set = self.file_blob_absent_identities.lock().await;
        if set.len() < FILE_BLOB_ABSENT_IDENTITY_CAP {
            set.insert(file_hash.to_string());
        } else {
            self.file_blob_absent_identities_capped
                .store(true, Ordering::Relaxed);
        }
    }

    /// Snapshot of proven-absent file-blob accounting for `/api/status`.
    ///
    /// The event counter and the identity set are not read under one lock, and
    /// they cannot be: the events are atomics and only the set has a lock. What
    /// makes that safe is the ORDER inside
    /// [`Self::record_file_blob_absent_with_memo`] — the atomic is bumped
    /// before the set is touched, so a recorder racing this read can only make
    /// `absent_with_memo` run ahead of `distinct_with_memo`, never behind.
    ///
    /// That direction matters. `distinct > events` is arithmetically impossible
    /// and would read as a broken gauge; `events > distinct` is both possible
    /// and MEANINGFUL here — it is how a re-uploaded-and-lost-again blob shows
    /// up. A transient skew of one is indistinguishable from that signal, which
    /// is fine at the scale this fires (a durability event is rare, and one
    /// stale count for the width of a status read changes no decision).
    pub async fn file_blob_durability(&self) -> FileBlobDurability {
        let distinct_with_memo = self.file_blob_absent_identities.lock().await.len() as u64;
        FileBlobDurability {
            absent_with_memo: self.file_blob_absent_with_memo.load(Ordering::Relaxed),
            absent_without_memo: self.file_blob_absent_without_memo.load(Ordering::Relaxed),
            distinct_with_memo,
            distinct_capped: self
                .file_blob_absent_identities_capped
                .load(Ordering::Relaxed),
        }
    }

    /// Delete a personal content-addressed file blob and best-effort release
    /// its storage-metering credit after the object DELETE succeeds.
    pub async fn delete_file_blob(&self, file_hash: &str) -> SyncResult<()> {
        let url = self.auth.presign_file_delete(file_hash).await?;
        self.s3.delete(&url).await?;

        if let Err(e) = self.auth.confirm_file_delete(file_hash).await {
            tracing::warn!(
                target: "fold_db::sync",
                error = %e,
                file_hash = %file_hash,
                "confirm_file_delete metering failed (non-fatal; reconcile will heal)"
            );
        }

        self.forget_file_blob_ref(file_hash).await?;
        Ok(())
    }
}
