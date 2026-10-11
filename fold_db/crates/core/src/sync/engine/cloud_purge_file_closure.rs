//! Materialize and verify every source atom's exact file bytes before the hold.

use super::*;
use crate::db_operations::DbOperations;
use crate::sharing::delivery_wire::{self, ContentAddressedBlob};
use futures::{stream, StreamExt, TryStreamExt};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone)]
struct RequiredBlob {
    reference: FileBlobRef,
    thumbnail: Option<String>,
}

impl SyncEngine {
    pub async fn complete_cloud_purge_file_closure(
        &self,
        ops: &DbOperations,
        fence: &CloudPurgeWriteFence,
    ) -> SyncResult<()> {
        fence.require_source(&self.device_id)?;
        crate::sync::capture::with_capture_suppressed(self.complete_purge_files_inner(ops)).await
    }

    async fn complete_purge_files_inner(&self, ops: &DbOperations) -> SyncResult<()> {
        let source = ops
            .namespaced_store()
            .raw_last_store()
            .ok_or_else(|| SyncError::Storage("file closure requires LastStore".into()))?;
        ops.flush().await.map_err(error)?;
        let physical = crate::sync::cloud_purge::org_rows::main_rows(&source)?;
        let keys: Vec<_> = physical
            .keys()
            .filter(|key| {
                crate::atom::atom_key_codec::uuid_of(
                    crate::sync::cloud_purge::org_rows::bare(key).1,
                )
                .is_some()
            })
            .cloned()
            .collect();
        let mut required: BTreeMap<String, RequiredBlob> = BTreeMap::new();
        let mut declared = BTreeSet::new();
        for batch in keys.chunks(128) {
            let atoms = ops
                .atoms()
                .get_atoms_by_storage_keys(batch)
                .await
                .map_err(error)?;
            collect_required(atoms, &mut required, &mut declared)?;
        }
        let missing: Vec<_> = declared
            .iter()
            .filter(|id| !required.contains_key(*id))
            .cloned()
            .collect();
        if !missing.is_empty() {
            let memo = self.store.open_namespace("sync_file_blob_known").await?;
            let values = memo
                .get_many(
                    missing
                        .iter()
                        .map(|id| id.strip_prefix("sha256:").unwrap_or(id).as_bytes().to_vec())
                        .collect(),
                )
                .await?;
            for (id, value) in missing.into_iter().zip(values) {
                let reference: FileBlobRef = serde_json::from_slice(&value.ok_or_else(|| {
                    SyncError::Storage("source file reference lacks exact access metadata".into())
                })?)
                .map_err(error)?;
                if reference.blob_ref != id {
                    return Err(SyncError::Storage(
                        "source file memo identity differs".into(),
                    ));
                }
                insert_required(&mut required, reference, None)?;
            }
        }
        let cas = ops.open_namespace("cas_blobs").await.map_err(error)?;
        let refs: Vec<_> = required.into_values().collect();
        for batch in refs.chunks(4) {
            let values = cas
                .get_many(
                    batch
                        .iter()
                        .map(|item| item.reference.blob_ref.as_bytes().to_vec())
                        .collect(),
                )
                .await?;
            let puts: Vec<_> = stream::iter(
                batch
                    .iter()
                    .cloned()
                    .zip(values)
                    .map(|(item, value)| self.complete_one_purge_blob(item, value)),
            )
            .buffer_unordered(4)
            .try_collect()
            .await?;
            cas.batch_put(puts).await?;
        }
        ops.namespaced_store().restore_durability_barrier().await?;
        Ok(())
    }

    async fn complete_one_purge_blob(
        &self,
        item: RequiredBlob,
        local: Option<Vec<u8>>,
    ) -> SyncResult<(Vec<u8>, Vec<u8>)> {
        let plaintext = match local {
            Some(bytes) => {
                let blob: ContentAddressedBlob = serde_json::from_slice(&bytes).map_err(error)?;
                if blob.size > 16 * 1024 * 1024 {
                    return Err(SyncError::Storage("cloud purge source inline file exceeds the 16 MiB product limit; use file slabs before the cloud hold".into()));
                }
                if blob.blob_ref != item.reference.blob_ref {
                    return Err(SyncError::Crypto("local file reference differs".into()));
                }
                delivery_wire::decode_blob_bytes_with_dek(&blob, &item.reference.dek)
                    .map_err(error)?
            }
            None => {
                if let Some(hash) = item.thumbnail {
                    let ciphertext = self.load_thumb_ciphertext(&hash).await?.ok_or_else(|| {
                        SyncError::Storage("source thumbnail bytes absent".into())
                    })?;
                    if ciphertext.len() as u64 != item.reference.encrypted_size_bytes {
                        return Err(SyncError::Crypto(
                            "thumbnail ciphertext length differs".into(),
                        ));
                    }
                    let key = crate::hex::hex_decode_array::<32>(&item.reference.dek).ok_or_else(
                        || SyncError::Crypto("invalid thumbnail encryption key".into()),
                    )?;
                    crate::crypto::LocalCryptoProvider::from_key(key)
                        .decrypt(&ciphertext)
                        .await
                        .map_err(|_| {
                            SyncError::Crypto("thumbnail encryption authentication failed".into())
                        })?
                } else {
                    self.download_file_blob(&item.reference)
                        .await?
                        .ok_or_else(|| {
                            SyncError::Storage(
                                "source file bytes absent from the local store and cloud".into(),
                            )
                        })?
                }
            }
        };
        if crate::hex::sha256_hex(&plaintext) != item.reference.file_hash {
            return Err(SyncError::Crypto(
                "source file plaintext hash differs".into(),
            ));
        }
        let blob = delivery_wire::content_addressed_blob_sealed_under_dek(
            &plaintext,
            None,
            None,
            &item.reference.dek,
        )
        .map_err(error)?;
        Ok((
            blob.blob_ref.as_bytes().to_vec(),
            serde_json::to_vec(&blob).map_err(error)?,
        ))
    }
}

fn insert_required(
    required: &mut BTreeMap<String, RequiredBlob>,
    reference: FileBlobRef,
    thumbnail: Option<String>,
) -> SyncResult<()> {
    if reference.encrypted_size_bytes == 0
        || reference.encrypted_size_bytes > 16 * 1024 * 1024 * 4 / 3 + 4096
    {
        return Err(SyncError::Storage("cloud purge source file size is absent or exceeds the 16 MiB inline product limit; use file slabs before the cloud hold".into()));
    }
    if let Some(previous) = required.get(&reference.blob_ref) {
        if previous.reference.dek != reference.dek
            || previous.reference.file_hash != reference.file_hash
        {
            return Err(SyncError::Crypto(
                "conflicting access keys for one source file".into(),
            ));
        }
        return Ok(());
    }
    required.insert(
        reference.blob_ref.clone(),
        RequiredBlob {
            reference,
            thumbnail,
        },
    );
    Ok(())
}

fn error(error: impl std::fmt::Display) -> SyncError {
    SyncError::Storage(error.to_string())
}

fn collect_required(
    atoms: Vec<Option<crate::atom::Atom>>,
    required: &mut BTreeMap<String, RequiredBlob>,
    declared: &mut BTreeSet<String>,
) -> SyncResult<()> {
    for atom in atoms {
        let atom =
            atom.ok_or_else(|| SyncError::Storage("source file closure atom disappeared".into()))?;
        declared.extend(crate::atom::file_pointer::blob_refs_of_atom(
            atom.content(),
            atom.metadata(),
        ));
        if let Some(access) =
            delivery_wire::file_blob_access_from_atom_value(atom.content()).map_err(error)?
        {
            insert_required(
                required,
                FileBlobRef {
                    blob_ref: access.blob_ref,
                    file_hash: access.file_hash,
                    owner_scope: access.owner_scope,
                    cipher_suite: access.cipher_suite,
                    dek: access.dek,
                    encrypted_size_bytes: access.encrypted_size_bytes.unwrap_or(0),
                },
                None,
            )?;
        }
        if let Some(thumbnail) =
            delivery_wire::file_thumbnail_ref_from_atom_value(atom.content()).map_err(error)?
        {
            let access: delivery_wire::FileThumbnailAccess = serde_json::from_value(
                atom.content()
                    .pointer("/$lastdb_file/thumbnail/access")
                    .cloned()
                    .ok_or_else(|| SyncError::Storage("thumbnail encryption key absent".into()))?,
            )
            .map_err(error)?;
            let file_hash = thumbnail
                .blob_ref
                .strip_prefix("sha256:")
                .ok_or_else(|| SyncError::Storage("invalid thumbnail file reference".into()))?
                .to_string();
            insert_required(
                required,
                FileBlobRef {
                    blob_ref: thumbnail.blob_ref,
                    file_hash: file_hash.clone(),
                    owner_scope: None,
                    cipher_suite: access.cipher_suite,
                    dek: access.dek,
                    encrypted_size_bytes: access.encrypted_size_bytes,
                },
                Some(file_hash.clone()),
            )?;
        }
    }
    Ok(())
}
