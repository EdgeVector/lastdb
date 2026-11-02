use super::*;
use crate::sharing::blob_cas;

/// Resolve raw file bytes for a field value that is a `$lastdb_file` pointer.
///
/// Local CAS rows are sealed under the file KDK and open with the DEK from the
/// pointer access metadata (never stored in plain on the CAS row). A value
/// without that metadata resolves to `None`.
pub async fn resolve_file_bytes(
    db: &FoldDB,
    field_value: &Value,
) -> Result<Option<Vec<u8>>, FoldDbError> {
    match file_blob_access_from_value(field_value)? {
        Some(access) => resolve_local_blob_bytes(db, &access).await,
        None => Ok(None),
    }
}

/// A CAS row authenticates its own address; the record also binds the plaintext hash.
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
        &lastdb_model::hex::sha256_hex(&bytes),
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
