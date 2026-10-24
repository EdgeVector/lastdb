use super::*;

/// Resolve raw file bytes for a field value that is a `$lastdb_file` pointer.
///
/// Operation Trinity: local CAS rows sealed under the file KDK open with the
/// DEK from pointer access metadata (never stored in plain on the CAS row).
pub async fn resolve_file_bytes(
    db: &FoldDB,
    field_value: &Value,
) -> Result<Option<Vec<u8>>, FoldDbError> {
    let Some(blob_ref) = blob_ref_from_atom_value(field_value) else {
        // Try legacy embedded form.
        return Ok(extract_embedded_file(field_value)?.map(|f| f.bytes));
    };
    if let Some(access) = file_blob_access_from_atom_value(field_value)? {
        if !access.dek.is_empty() {
            let local = resolve_local_blob_bytes(db, &access).await?;
            if local.is_some() {
                return Ok(local);
            }
            return resolve_recovered_pointer(
                db,
                field_value,
                cloud_backup_protocol::FileDependencyKind::Main,
                &access,
            )
            .await;
        }
    }
    blob_cas::get_blob_bytes_in_ops(db.db_ops(), blob_ref).await
}

/// Resolve the exact original whole pointer and derivative kind without cloud access.
/// The installed resolver remains available after a normal daemon restart.
pub async fn resolve_file_bytes_kind(
    db: &FoldDB,
    pointer: &Value,
    kind: cloud_backup_protocol::FileDependencyKind,
) -> Result<Option<Vec<u8>>, FoldDbError> {
    use cloud_backup_protocol::FileDependencyKind;
    if kind == FileDependencyKind::Main {
        return resolve_file_bytes(db, pointer).await;
    }
    let Some(thumbnail) =
        crate::sharing::delivery_wire::file_thumbnail_ref_from_atom_value(pointer)?
    else {
        return Ok(None);
    };
    let expected = if thumbnail.kind == "image" {
        FileDependencyKind::Thumbnail
    } else {
        FileDependencyKind::Poster
    };
    if kind != expected {
        return Ok(None);
    }
    let access = thumbnail.file_access()?;
    let local = resolve_local_blob_bytes(db, &access).await?;
    if local.is_some() {
        return Ok(local);
    }
    resolve_recovered_pointer(db, pointer, kind, &access).await
}

/// A CAS row authenticates its own address; the atom also binds the plaintext hash.
pub(super) async fn resolve_local_blob_bytes(
    db: &FoldDB,
    access: &FileBlobAccess,
) -> Result<Option<Vec<u8>>, FoldDbError> {
    let Some(blob) = blob_cas::get_blob_in_ops(db.db_ops(), &access.blob_ref).await? else {
        return Ok(None);
    };
    if blob.blob_ref != access.blob_ref {
        return Err(FoldDbError::SecurityError(
            "local CAS record differs from the file pointer".into(),
        ));
    }
    let bytes = crate::sharing::delivery_wire::decode_blob_bytes_with_dek(&blob, &access.dek)?;
    crate::sharing::file_blob_seal::verify_file_blob_identity(
        access,
        &crate::hex::sha256_hex(&bytes),
        &blob.content_sha256,
    )?;
    if access.encrypted_size_bytes.is_some_and(|size| {
        size != bytes.len() as u64 + crate::sharing::file_blob_seal::FILE_BLOB_ENVELOPE_OVERHEAD
    }) {
        return Err(FoldDbError::SecurityError(
            "local CAS size differs from the file pointer".into(),
        ));
    }
    Ok(Some(bytes))
}

async fn resolve_recovered_pointer(
    db: &FoldDB,
    pointer: &Value,
    kind: cloud_backup_protocol::FileDependencyKind,
    access: &FileBlobAccess,
) -> Result<Option<Vec<u8>>, FoldDbError> {
    let Some(runtime) = db.db_ops().namespaced_store().disk_pack_runtime() else {
        return Ok(None);
    };
    let pointer = cloud_backup_protocol::logical_pointer_sha256(pointer)
        .map_err(|_| FoldDbError::SecurityError("file recovery pointer format".into()))?;
    let access = RecoveryAccess(access.clone());
    tokio::task::spawn_blocking(move || {
        runtime
            .resolve_recovered_file(&pointer, kind, &access.0)
            .map_err(FoldDbError::from)
    })
    .await
    .map_err(|_| FoldDbError::Other("file recovery read task failed".into()))?
}

struct RecoveryAccess(FileBlobAccess);
impl Drop for RecoveryAccess {
    fn drop(&mut self) {
        use zeroize::Zeroize;
        self.0.dek.zeroize();
    }
}
