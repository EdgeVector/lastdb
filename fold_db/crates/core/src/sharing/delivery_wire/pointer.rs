//! `$lastdb_file` pointer construction and extraction.

use super::types::{FileBlobAccess, FileThumbnailRef};
use super::validate::{validate_file_blob_access_for_ref, validate_file_thumbnail_ref_for_pointer};
use super::{LASTDB_FILE_KEY, LASTDB_FILE_THUMBNAIL_KEY};
use crate::error::FoldDbError;
use serde_json::Value;

/// Atom value that points at a first-class blob (no embedded file bytes).
pub fn lastdb_file_pointer(blob_ref: &str, name: Option<&str>, media_type: Option<&str>) -> Value {
    lastdb_file_pointer_with_access(blob_ref, name, media_type, None)
}

/// Atom value that points at a first-class blob and carries access metadata.
pub fn lastdb_file_pointer_with_access(
    blob_ref: &str,
    name: Option<&str>,
    media_type: Option<&str>,
    access: Option<&FileBlobAccess>,
) -> Value {
    lastdb_file_pointer_with_access_and_thumbnail(blob_ref, name, media_type, access, None)
}

/// Atom value that points at a first-class blob, carries access metadata, and
/// optionally references a generated thumbnail/poster object.
pub fn lastdb_file_pointer_with_access_and_thumbnail(
    blob_ref: &str,
    name: Option<&str>,
    media_type: Option<&str>,
    access: Option<&FileBlobAccess>,
    thumbnail: Option<&FileThumbnailRef>,
) -> Value {
    let mut inner = serde_json::Map::new();
    inner.insert("blob_ref".into(), Value::String(blob_ref.to_string()));
    if let Some(n) = name {
        inner.insert("name".into(), Value::String(n.to_string()));
    }
    if let Some(m) = media_type {
        inner.insert("media_type".into(), Value::String(m.to_string()));
    }
    if let Some(access) = access {
        inner.insert("file_hash".into(), Value::String(access.file_hash.clone()));
        if let Some(owner_scope) = &access.owner_scope {
            inner.insert("owner_scope".into(), Value::String(owner_scope.clone()));
        }
        inner.insert(
            "cipher_suite".into(),
            Value::String(access.cipher_suite.clone()),
        );
        inner.insert("dek".into(), Value::String(access.dek.clone()));
        if let Some(size) = access.encrypted_size_bytes {
            inner.insert(
                "encrypted_size_bytes".into(),
                Value::Number(serde_json::Number::from(size)),
            );
        }
    }
    if let Some(thumbnail) = thumbnail {
        inner.insert(
            LASTDB_FILE_THUMBNAIL_KEY.into(),
            serde_json::to_value(thumbnail).unwrap_or(Value::Null),
        );
    }
    let mut outer = serde_json::Map::new();
    outer.insert(LASTDB_FILE_KEY.into(), Value::Object(inner));
    Value::Object(outer)
}

/// Extract portable file-blob access metadata from a `$lastdb_file` pointer.
pub fn file_blob_access_from_atom_value(
    value: &Value,
) -> Result<Option<FileBlobAccess>, FoldDbError> {
    let Some(obj) = value.get(LASTDB_FILE_KEY).and_then(|v| v.as_object()) else {
        return Ok(None);
    };
    let Some(blob_ref) = obj.get("blob_ref").and_then(|v| v.as_str()) else {
        return Ok(None);
    };
    let Some(file_hash) = obj.get("file_hash").and_then(|v| v.as_str()) else {
        return Ok(None);
    };
    let Some(cipher_suite) = obj.get("cipher_suite").and_then(|v| v.as_str()) else {
        return Ok(None);
    };
    let Some(dek) = obj.get("dek").and_then(|v| v.as_str()) else {
        return Ok(None);
    };
    let access = FileBlobAccess {
        blob_ref: blob_ref.to_string(),
        file_hash: file_hash.to_string(),
        owner_scope: obj
            .get("owner_scope")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
        cipher_suite: cipher_suite.to_string(),
        dek: dek.to_string(),
        encrypted_size_bytes: obj
            .get("encrypted_size_bytes")
            .and_then(serde_json::Value::as_u64),
    };
    validate_file_blob_access_for_ref(&access, blob_ref)?;
    Ok(Some(access))
}

/// Extract and validate an optional thumbnail/poster reference from a `$lastdb_file` pointer.
pub fn file_thumbnail_ref_from_atom_value(
    value: &Value,
) -> Result<Option<FileThumbnailRef>, FoldDbError> {
    let Some(obj) = value.get(LASTDB_FILE_KEY).and_then(|v| v.as_object()) else {
        return Ok(None);
    };
    let Some(thumbnail_value) = obj.get(LASTDB_FILE_THUMBNAIL_KEY) else {
        return Ok(None);
    };
    let thumbnail: FileThumbnailRef =
        serde_json::from_value(thumbnail_value.clone()).map_err(FoldDbError::from)?;
    let parent_access = file_blob_access_from_atom_value(value)?.ok_or_else(|| {
        FoldDbError::SecurityError(
            "thumbnail reference requires parent file blob access metadata".to_string(),
        )
    })?;
    validate_file_thumbnail_ref_for_pointer(obj, &parent_access, &thumbnail)?;
    Ok(Some(thumbnail))
}
