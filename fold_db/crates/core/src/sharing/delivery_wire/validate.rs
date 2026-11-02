//! Validation of blob references, file-blob access and thumbnail metadata.

use super::blob::decode_blob_bytes;
use super::pointer::{file_blob_access_from_value, file_thumbnail_ref_from_value};
use super::types::{ContentAddressedBlob, FileBlobAccess, FileThumbnailRef, LastDbSlicePayload};
use super::{
    blob_ref_from_value, file_blob_cipher_suite_supported, FILE_THUMBNAIL_MAX_ENCRYPTED_SIZE_BYTES,
    FILE_THUMBNAIL_TIER, LASTDB_FILE_KEY, LASTDB_FILE_THUMBNAIL_KEY,
};
use crate::error::FoldDbError;
use serde_json::Value;
use std::collections::BTreeMap;

/// Ensure every `$lastdb_file` record has a matching `blobs[]` entry.
pub fn validate_blob_refs(payload: &LastDbSlicePayload) -> Result<(), FoldDbError> {
    let known: BTreeMap<&str, &ContentAddressedBlob> = payload
        .blobs
        .iter()
        .map(|b| (b.blob_ref.as_str(), b))
        .collect();
    for record in &payload.records {
        if let Some(r) = blob_ref_from_value(&record.value) {
            let Some(blob) = known.get(r) else {
                return Err(FoldDbError::Other(format!(
                    "record {} references missing blob {r}",
                    record.value_ref
                )));
            };
            if let Some(pointer_access) = file_blob_access_from_value(&record.value)? {
                if blob.access.as_ref() != Some(&pointer_access) {
                    return Err(FoldDbError::Other(format!(
                        "record {} carries blob access metadata that does not match blob {r}",
                        record.value_ref
                    )));
                }
                file_thumbnail_ref_from_value(&record.value)?;
            } else if let Some(obj) = record
                .value
                .get(LASTDB_FILE_KEY)
                .and_then(serde_json::Value::as_object)
            {
                if obj.contains_key(LASTDB_FILE_THUMBNAIL_KEY) {
                    return Err(FoldDbError::SecurityError(format!(
                        "record {} thumbnail reference requires parent file blob access metadata",
                        record.value_ref
                    )));
                }
            }
        }
    }
    for blob in &payload.blobs {
        decode_blob_bytes(blob)?;
    }
    Ok(())
}

pub fn validate_file_thumbnail_ref_for_pointer(
    pointer: &serde_json::Map<String, Value>,
    parent_access: &FileBlobAccess,
    thumbnail: &FileThumbnailRef,
) -> Result<(), FoldDbError> {
    thumbnail.file_access()?;
    if let Some(inline_key) = thumbnail_inline_bytes_key(
        pointer
            .get(LASTDB_FILE_THUMBNAIL_KEY)
            .and_then(serde_json::Value::as_object),
    ) {
        return Err(FoldDbError::SecurityError(format!(
            "thumbnail reference must not embed inline bytes key '{inline_key}'",
        )));
    }
    let parent_media_type = pointer
        .get("media_type")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            FoldDbError::SecurityError("thumbnail reference requires parent media_type".to_string())
        })?;
    if !is_thumbnail_eligible_media_type(parent_media_type) {
        return Err(FoldDbError::SecurityError(format!(
            "thumbnail reference parent media_type must be image/* or video/*, got {parent_media_type}",
        )));
    }
    if thumbnail.tier != FILE_THUMBNAIL_TIER {
        return Err(FoldDbError::SecurityError(format!(
            "unsupported thumbnail tier '{}'",
            thumbnail.tier
        )));
    }
    if thumbnail.source_hash != parent_access.file_hash {
        return Err(FoldDbError::SecurityError(
            "thumbnail source_hash must match parent file_hash".to_string(),
        ));
    }
    if thumbnail.access.encrypted_size_bytes > FILE_THUMBNAIL_MAX_ENCRYPTED_SIZE_BYTES {
        return Err(FoldDbError::SecurityError(format!(
            "thumbnail encrypted_size_bytes must be <= {FILE_THUMBNAIL_MAX_ENCRYPTED_SIZE_BYTES}",
        )));
    }
    if !file_blob_cipher_suite_supported(&thumbnail.access.cipher_suite) {
        return Err(FoldDbError::SecurityError(format!(
            "unsupported thumbnail cipher suite '{}'",
            thumbnail.access.cipher_suite
        )));
    }
    if thumbnail.access.dek.len() != 64
        || !thumbnail.access.dek.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Err(FoldDbError::SecurityError(
            "thumbnail access DEK must be 64 hex chars".to_string(),
        ));
    }
    if thumbnail.width == 0 || thumbnail.height == 0 {
        return Err(FoldDbError::SecurityError(
            "thumbnail width and height must be non-zero".to_string(),
        ));
    }
    if thumbnail.kind != "image" && thumbnail.kind != "video-poster" {
        return Err(FoldDbError::SecurityError(
            "thumbnail kind must be image or video-poster".to_string(),
        ));
    }
    Ok(())
}

fn is_thumbnail_eligible_media_type(media_type: &str) -> bool {
    media_type.starts_with("image/") || media_type.starts_with("video/")
}

fn thumbnail_inline_bytes_key(
    obj: Option<&serde_json::Map<String, Value>>,
) -> Option<&'static str> {
    let obj = obj?;
    [
        "bytes",
        "bytes_b64",
        "content",
        "content_b64",
        "data",
        "data_b64",
        "inline_bytes",
    ]
    .into_iter()
    .find(|key| obj.contains_key(*key))
}

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
