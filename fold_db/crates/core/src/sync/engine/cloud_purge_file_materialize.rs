//! One admitted file, with exact authenticated identity, size and plaintext SHA.

use super::cloud_purge_file_closure::RequiredBlob;
use super::*;
use crate::sharing::delivery_wire::{self, ContentAddressedBlob};
use crate::sync::cloud_purge::memory::{self, Admission};

impl SyncEngine {
    pub(super) async fn complete_one_purge_blob(
        &self,
        item: RequiredBlob,
        local: Option<Vec<u8>>,
        admission: &Admission,
    ) -> SyncResult<Option<(Vec<u8>, Vec<u8>)>> {
        if let Some(bytes) = local {
            let blob: ContentAddressedBlob = serde_json::from_slice(&bytes)
                .map_err(|_| fail("source local file metadata is invalid"))?;
            if blob.blob_ref != item.reference.blob_ref {
                return Err(fail("source local file reference differs"));
            }
            let plaintext = delivery_wire::decode_blob_bytes_with_dek(&blob, &item.reference.dek)
                .map_err(|_| fail("source local file authentication failed"))?;
            verify(&item.reference, &plaintext, &blob.content_sha256)?;
            // Keep the exact verified existing ciphertext, metadata and bytes.
            // A second seal would add memory and change this physical source.
            return Ok(None);
        }
        let ciphertext = self
            .purge_file_ciphertext(&item, admission)
            .await?
            .ok_or_else(|| fail("source file bytes absent from the local store and cloud"))?;
        if item.reference.encrypted_size_bytes != 0
            && ciphertext.len() as u64 != item.reference.encrypted_size_bytes
        {
            return Err(fail("source file ciphertext size differs"));
        }
        let key = crate::hex::hex_decode_array::<32>(&item.reference.dek)
            .ok_or_else(|| fail("source file encryption key is invalid"))?;
        let plaintext = crate::crypto::LocalCryptoProvider::from_key(key)
            .decrypt(&ciphertext)
            .await
            .map_err(|_| fail("source file encryption authentication failed"))?;
        verify(
            &item.reference,
            &plaintext,
            &crate::hex::sha256_hex(&ciphertext),
        )?;
        let mut access = item.reference.to_access();
        // Older pointers can omit this length; the authenticated download now
        // supplies the exact value for the local record constructor.
        access.encrypted_size_bytes = Some(ciphertext.len() as u64);
        let blob = delivery_wire::content_addressed_blob_from_envelope(
            plaintext.len() as u64,
            None,
            None,
            &access,
            &ciphertext,
        )
        .map_err(|_| fail("source file materialization encryption failed"))?;
        Ok(Some((
            blob.blob_ref.as_bytes().to_vec(),
            serde_json::to_vec(&blob)
                .map_err(|_| fail("source file materialization metadata is invalid"))?,
        )))
    }

    async fn purge_file_ciphertext(
        &self,
        item: &RequiredBlob,
        admission: &Admission,
    ) -> SyncResult<Option<Vec<u8>>> {
        let expected = (item.reference.encrypted_size_bytes != 0)
            .then_some(item.reference.encrypted_size_bytes);
        if let Some(hash) = &item.thumbnail {
            let sizes = memory::raw_lengths(
                admission,
                0,
                self.store.as_ref(),
                "sync_thumb_cache",
                std::slice::from_ref(hash),
            )?;
            if let Some(size) = sizes[0] {
                admission.check(memory::read_peak(size)?)?;
                let cache = self.store.open_namespace("sync_thumb_cache").await?;
                return cache.get(hash.as_bytes()).await.map_err(Into::into);
            }
            let sizes = memory::raw_lengths(
                admission,
                0,
                self.store.as_ref(),
                "sync_thumb_pack_index",
                std::slice::from_ref(hash),
            )?;
            if let Some(size) = sizes[0] {
                admission.check(memory::read_peak(size)?)?;
                if let Some(entry) = self.cached_thumb_pack_entry(hash).await? {
                    if expected.is_some_and(|bytes| bytes != entry.len) {
                        return Err(fail("thumbnail pack size differs"));
                    }
                    admission.check(memory::row_peak(entry.len)?)?;
                    let url = self
                        .auth
                        .presign_thumb_pack_download(&entry.pack_id)
                        .await?;
                    return self
                        .s3
                        .download_range_admitted(&url, entry.offset, entry.len, admission)
                        .await;
                }
            }
            let url = self.auth.presign_thumb_download(hash).await?;
            return self
                .s3
                .download_admitted(&url, admission, expected, 0)
                .await;
        }
        let url = self
            .auth
            .presign_file_download_owner_scope(
                crate::sharing::file_blob_seal::file_blob_storage_hash(&item.reference.blob_ref)?,
                item.reference.owner_scope.as_deref(),
            )
            .await?;
        self.s3
            .download_admitted(&url, admission, expected, 0)
            .await
    }
}

fn verify(reference: &FileBlobRef, plaintext: &[u8], ciphertext_hash: &str) -> SyncResult<()> {
    let size = (plaintext.len() as u64)
        .checked_add(crate::sharing::file_blob_seal::FILE_BLOB_ENVELOPE_OVERHEAD)
        .ok_or_else(|| fail("source file size overflows"))?;
    if reference.encrypted_size_bytes != 0 && size != reference.encrypted_size_bytes {
        return Err(fail("source file exact size or plaintext hash differs"));
    }
    crate::sharing::file_blob_seal::verify_file_blob_identity(
        &reference.to_access(),
        &crate::hex::sha256_hex(plaintext),
        ciphertext_hash,
    )?;
    Ok(())
}

fn fail(message: &str) -> SyncError {
    SyncError::Crypto(message.into())
}
