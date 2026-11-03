//! Validation of file blob access metadata.

use super::file_blob_cipher_suite_supported;
use super::types::FileBlobAccess;
use crate::error::FoldDbError;

pub fn validate_file_blob_access_for_ref(
    access: &FileBlobAccess,
    blob_ref: &str,
) -> Result<(), FoldDbError> {
    if access.blob_ref != blob_ref {
        return Err(FoldDbError::SecurityError(format!(
            "file blob access ref mismatch: expected {blob_ref}, got {}",
            access.blob_ref
        )));
    }
    if access.file_hash.len() != 64 || !access.file_hash.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(FoldDbError::SecurityError(
            "file blob access file_hash must be 64 hex chars".to_string(),
        ));
    }
    crate::sharing::file_blob_seal::file_blob_storage_hash(blob_ref)?;
    if let Some(owner_scope) = &access.owner_scope {
        if owner_scope.is_empty()
            || owner_scope.contains('/')
            || owner_scope.contains("..")
            || owner_scope.bytes().any(|b| b.is_ascii_control())
        {
            return Err(FoldDbError::SecurityError(
                "file blob access owner_scope must be a safe storage scope component".to_string(),
            ));
        }
    }
    if !file_blob_cipher_suite_supported(&access.cipher_suite) {
        return Err(FoldDbError::SecurityError(format!(
            "unsupported file blob cipher suite '{}'",
            access.cipher_suite
        )));
    }
    if access.dek.len() != 64 || !access.dek.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(FoldDbError::SecurityError(
            "file blob access DEK must be 64 hex chars".to_string(),
        ));
    }
    Ok(())
}
