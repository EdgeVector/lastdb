//! Materialize and verify every source atom's exact file bytes before the hold.

use super::*;
use crate::db_operations::DbOperations;
use crate::sharing::delivery_wire;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone)]
pub(super) struct RequiredBlob {
    pub(super) reference: FileBlobRef,
    pub(super) thumbnail: Option<String>,
}

impl SyncEngine {
    pub async fn complete_cloud_purge_file_closure(
        &self,
        ops: &DbOperations,
        fence: &CloudPurgeWriteFence,
    ) -> SyncResult<()> {
        fence.require_source(&self.device_id)?;
        crate::sync::capture::with_capture_suppressed(self.complete_purge_files_inner(ops, None))
            .await
    }

    pub(super) async fn complete_cloud_purge_org_file_closure(
        &self,
        ops: &DbOperations,
        target: &crate::sharing::org_sync_target::OrgSyncTarget,
        fence: &CloudPurgeWriteFence,
    ) -> SyncResult<()> {
        fence.require_source(&self.device_id)?;
        crate::sync::capture::with_capture_suppressed(
            self.complete_purge_files_inner(ops, Some(target)),
        )
        .await
    }

    async fn complete_purge_files_inner(
        &self,
        ops: &DbOperations,
        target: Option<&crate::sharing::org_sync_target::OrgSyncTarget>,
    ) -> SyncResult<()> {
        ops.flush().await.map_err(error)?;
        let keys = if let Some(target) = target {
            crate::sync::cloud_purge::org_export::source_atom_keys(ops, target).await?
        } else {
            personal_atom_keys(ops).await?
        };
        let required = super::cloud_purge_file_references::collect(self, ops, &keys).await?;
        let cas = ops.open_namespace("cas_blobs").await.map_err(error)?;
        let refs: Vec<_> = required.into_values().collect();
        let ids: Vec<_> = refs
            .iter()
            .map(|item| item.reference.blob_ref.clone())
            .collect();
        let metadata = crate::sync::cloud_purge::memory::Admission::acquire(0).await?;
        let sizes = crate::sync::cloud_purge::memory::raw_lengths(
            &metadata,
            0,
            ops.namespaced_store().as_ref(),
            "cas_blobs",
            &ids,
        )?;
        drop(metadata);
        // Metadata is batched. Bodies are admitted individually so four large
        // local files cannot create four simultaneous decode/seal peaks.
        for (item, raw_size) in refs.into_iter().zip(sizes) {
            let peak = match raw_size {
                Some(size) => crate::sync::cloud_purge::memory::read_peak(size)?,
                None => {
                    crate::sync::cloud_purge::memory::row_peak(item.reference.encrypted_size_bytes)?
                }
            };
            let admission = crate::sync::cloud_purge::memory::Admission::acquire(peak).await?;
            let local = cas.get(item.reference.blob_ref.as_bytes()).await?;
            if local.is_some() != raw_size.is_some() {
                return Err(error("source file changed after exact size preflight"));
            }
            if let Some((key, bytes)) = self
                .complete_one_purge_blob(item, local, &admission)
                .await?
            {
                cas.put(&key, bytes).await?;
            }
        }
        ops.namespaced_store().restore_durability_barrier().await?;
        Ok(())
    }
}

async fn personal_atom_keys(ops: &DbOperations) -> SyncResult<Vec<String>> {
    let source = ops
        .namespaced_store()
        .raw_last_store()
        .ok_or_else(|| SyncError::Storage("file closure requires LastStore".into()))?;
    let admission = crate::sync::cloud_purge::memory::Admission::acquire(0).await?;
    Ok(
        crate::sync::cloud_purge::org_rows::main_rows(&source, &admission)?
            .into_keys()
            .filter(|key| {
                crate::atom::atom_key_codec::body_uuid_of(
                    crate::sync::cloud_purge::org_rows::bare(key).1,
                )
                .is_some()
            })
            .collect(),
    )
}

pub(super) fn insert_required(
    required: &mut BTreeMap<String, RequiredBlob>,
    reference: FileBlobRef,
    thumbnail: Option<String>,
) -> SyncResult<()> {
    if reference.encrypted_size_bytes != 0
        && reference.encrypted_size_bytes
            < crate::sharing::file_blob_seal::FILE_BLOB_ENVELOPE_OVERHEAD
    {
        return Err(error(
            "cloud purge source file lacks an exact encrypted size",
        ));
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

pub(super) fn collect_required(
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
