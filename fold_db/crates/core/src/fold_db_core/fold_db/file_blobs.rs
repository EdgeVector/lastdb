use std::collections::HashMap;

use serde_json::Value;

use crate::access::AccessContext;
use crate::error::{fold_db_error_from_sync, FoldDbError};
use crate::schema::types::key_value::KeyValue;
use crate::schema::types::operations::MutationType;
use crate::schema::types::Mutation;
use crate::sharing::blob_cas;
use crate::sharing::delivery_wire::{
    lastdb_file_pointer_with_access_and_thumbnail, FileBlobAccess, FileThumbnailRef,
    LASTDB_FILE_KEY,
};
use crate::sync::engine::FileThumbnailUpload;

use super::FoldDB;

/// Inputs for writing one personal file field as a remote CAS-backed
/// `$lastdb_file` pointer.
#[derive(Debug, Clone)]
pub struct PersonalFileBlobWrite {
    pub schema_name: String,
    pub field_name: String,
    pub key_value: KeyValue,
    pub writer_pubkey: String,
    pub mutation_type: MutationType,
    pub name: Option<String>,
    pub media_type: Option<String>,
    pub thumbnail: Option<FileThumbnailUpload>,
    pub cache_local_plaintext: bool,
    pub additional_fields: HashMap<String, Value>,
}

/// Proof returned after the write path has uploaded and persisted a pointer.
#[derive(Debug, Clone)]
pub struct PersonalFileBlobWriteResult {
    pub pointer: Value,
    pub blob_ref: String,
    pub file_hash: String,
    pub thumbnail: Option<FileThumbnailRef>,
    pub mutation_ids: Vec<String>,
}

/// Inputs for forking an existing shared `$lastdb_file` pointer into the
/// caller's personal file-blob scope.
#[derive(Debug, Clone)]
pub struct PersonalFileBlobFork {
    pub source_pointer: Value,
    pub write: PersonalFileBlobWrite,
}

/// Proof returned after a shared pointer has been copied into the caller's
/// personal scope and rewritten locally.
#[derive(Debug, Clone)]
pub struct PersonalFileBlobForkResult {
    pub pointer: Value,
    pub source_blob_ref: String,
    pub blob_ref: String,
    pub file_hash: String,
    pub thumbnail: Option<FileThumbnailRef>,
    pub mutation_ids: Vec<String>,
    pub bytes: usize,
}

impl FoldDB {
    /// Upload plaintext to personal B2 CAS, persist a `$lastdb_file` pointer
    /// carrying [`FileBlobAccess`], and optionally seed the local CAS cache for
    /// immediate reopen on the creating device.
    pub async fn put_personal_file_blob(
        &self,
        sync_engine: &crate::sync::SyncEngine,
        request: PersonalFileBlobWrite,
        plaintext: &[u8],
    ) -> Result<PersonalFileBlobWriteResult, FoldDbError> {
        let blob_ref = sync_engine
            .upload_file_blob(plaintext)
            .await
            .map_err(|e| fold_db_error_from_sync("file blob upload failed", e))?;
        let access = FileBlobAccess {
            blob_ref: blob_ref.blob_ref.clone(),
            file_hash: blob_ref.file_hash.clone(),
            owner_scope: blob_ref.owner_scope.clone(),
            cipher_suite: blob_ref.cipher_suite.clone(),
            dek: blob_ref.dek.clone(),
            encrypted_size_bytes: Some(blob_ref.encrypted_size_bytes),
        };
        // Thumbnails are caller-supplied only. The platform surface that owns
        // the file owns decoding and resizing policy; the kernel just seals the
        // already-small derivative it is handed.
        let thumbnail_input = request.thumbnail.as_ref();
        let thumbnail = if let Some(thumbnail_input) = thumbnail_input {
            Some(
                sync_engine
                    .upload_file_thumbnail(&blob_ref.file_hash, thumbnail_input)
                    .await
                    .map_err(|e| fold_db_error_from_sync("thumbnail upload failed", e))?,
            )
        } else {
            None
        };
        let pointer = lastdb_file_pointer_with_access_and_thumbnail(
            &blob_ref.blob_ref,
            request.name.as_deref(),
            request.media_type.as_deref(),
            Some(&access),
            thumbnail.as_ref(),
        );

        // Operation Trinity: never leave plaintext file bytes in cas_blobs.
        // When the caller requests a local cache, seal under the file KDK.
        if request.cache_local_plaintext {
            blob_cas::put_blob_sealed_under_dek_in_ops(
                self.db_ops(),
                plaintext,
                request.media_type.clone(),
                request.name.clone(),
                &access.dek,
            )
            .await?;
        }

        let owner_id = self
            .get_node_id()
            .await
            .map_err(|e| FoldDbError::Database(format!("node id: {e}")))?;
        let ctx = AccessContext::owner(owner_id);
        let mut fields = request.additional_fields;
        fields.insert(request.field_name, pointer.clone());
        let mutation = Mutation::new(
            request.schema_name,
            fields,
            request.key_value,
            request.writer_pubkey,
            request.mutation_type,
        )
        .with_metadata(HashMap::from([
            ("file_hash".to_string(), blob_ref.file_hash.clone()),
            (
                "encrypted_size_bytes".to_string(),
                blob_ref.encrypted_size_bytes.to_string(),
            ),
        ]));
        let mutation_ids = self
            .mutation_manager()
            .write_mutations_with_access(vec![mutation], &ctx)
            .await?;

        Ok(PersonalFileBlobWriteResult {
            pointer,
            blob_ref: blob_ref.blob_ref,
            file_hash: blob_ref.file_hash,
            thumbnail,
            mutation_ids,
        })
    }

    /// Resolve an existing shared `$lastdb_file` pointer, upload its plaintext
    /// through this node's personal file-blob path, and persist the returned
    /// pointer back to the requested local record.
    #[cfg(feature = "cloud-sync")]
    pub async fn fork_personal_file_blob(
        &self,
        sync_engine: &crate::sync::SyncEngine,
        mut request: PersonalFileBlobFork,
    ) -> Result<PersonalFileBlobForkResult, FoldDbError> {
        use crate::sharing::delivery_wire::blob_ref_from_atom_value;
        use crate::sharing::query_slice::resolve_file_bytes_on_demand;

        let source_blob_ref = blob_ref_from_atom_value(&request.source_pointer)
            .ok_or_else(|| {
                FoldDbError::Other(
                    "source pointer must be a $lastdb_file value with blob_ref".to_string(),
                )
            })?
            .to_string();
        let plaintext = resolve_file_bytes_on_demand(self, sync_engine, &request.source_pointer)
            .await?
            .ok_or_else(|| {
                FoldDbError::Other(
                    "source file blob was not found locally or in remote CAS".to_string(),
                )
            })?;

        if request.write.name.is_none() {
            request.write.name = pointer_string(&request.source_pointer, "name");
        }
        if request.write.media_type.is_none() {
            request.write.media_type = pointer_string(&request.source_pointer, "media_type");
        }

        let bytes = plaintext.len();
        let result = self
            .put_personal_file_blob(sync_engine, request.write, &plaintext)
            .await?;

        Ok(PersonalFileBlobForkResult {
            pointer: result.pointer,
            source_blob_ref,
            blob_ref: result.blob_ref,
            file_hash: result.file_hash,
            thumbnail: result.thumbnail,
            mutation_ids: result.mutation_ids,
            bytes,
        })
    }
}

fn pointer_string(pointer: &Value, field: &str) -> Option<String> {
    pointer
        .pointer(&format!("/{LASTDB_FILE_KEY}/{field}"))
        .and_then(Value::as_str)
        .map(str::to_string)
}
