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
            let local =
                blob_cas::get_blob_bytes_with_dek_in_ops(db.db_ops(), blob_ref, &access.dek)
                    .await?;
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
    let access = FileBlobAccess {
        file_hash: thumbnail
            .blob_ref
            .strip_prefix("sha256:")
            .unwrap_or_default()
            .into(),
        blob_ref: thumbnail.blob_ref,
        owner_scope: None,
        cipher_suite: thumbnail.access.cipher_suite,
        dek: thumbnail.access.dek,
        encrypted_size_bytes: Some(thumbnail.access.encrypted_size_bytes),
    };
    let local =
        blob_cas::get_blob_bytes_with_dek_in_ops(db.db_ops(), &access.blob_ref, &access.dek)
            .await?;
    if local.is_some() {
        return Ok(local);
    }
    resolve_recovered_pointer(db, pointer, kind, &access).await
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
