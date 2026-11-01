use super::*;
use crate::sharing::delivery_wire::{file_blob_access_from_value, file_thumbnail_ref_from_value};
use lastdb_model::file_pointer;
use lastdb_model::record_value::StoredRecord;
use serde_json::Value;

/// Derive the dependencies of each field value of a format 7 record. A record
/// has no record, so the digest of a field's pointer names its dependencies. The record metadata belongs to the record (D7) and can
/// outlive the field that set it, so a reference that only the metadata names
/// is not refused: the byte backup copies every CAS body and the blob counter
/// keeps it.
pub fn expected_record(record: &StoredRecord) -> StorageResult<Vec<FileDependency>> {
    expected_values(record.fields.values())
}

/// The dependencies of field values without a record: the values of a record,
/// or the copied value of a record history entry.
pub fn expected_values<'a>(
    values: impl Iterator<Item = &'a Value>,
) -> StorageResult<Vec<FileDependency>> {
    let mut out = Vec::new();
    for value in values {
        if value.get(file_pointer::LASTDB_FILE_KEY).is_none()
            && file_pointer::blob_refs_of_value(value, None).is_empty()
        {
            continue;
        }
        let pointer = cloud_backup_protocol::logical_pointer_sha256(value)
            .map_err(|_| invalid("file dependency pointer digest"))?;
        out.extend(of_value(pointer.as_str(), value)?);
    }
    Ok(out)
}

/// Legacy, malformed and metadata-only references remain explicit refusals.
fn of_value(id: &str, content: &Value) -> StorageResult<Vec<FileDependency>> {
    let refs = file_pointer::blob_refs_of_value(content, None);
    let has_pointer = content.get(file_pointer::LASTDB_FILE_KEY).is_some();
    if !has_pointer && refs.is_empty() {
        return Ok(Vec::new());
    }
    let main = file_blob_access_from_value(content)
        .map_err(|_| invalid("invalid file dependency access"))?
        .ok_or_else(|| invalid("legacy or incomplete file dependency is uncertified"))?;
    let main = SecretAccess(main);
    let pointer = cloud_backup_protocol::logical_pointer_sha256(content)
        .map_err(|_| invalid("file dependency pointer digest"))?;
    let mut out = vec![make(id, &pointer, FileDependencyKind::Main, main)?];
    if let Some(thumbnail) = file_thumbnail_ref_from_value(content)
        .map_err(|_| invalid("invalid thumbnail dependency"))?
    {
        let kind = match thumbnail.kind.as_str() {
            "image" => FileDependencyKind::Thumbnail,
            "video-poster" => FileDependencyKind::Poster,
            _ => return Err(invalid("unsupported thumbnail dependency kind")),
        };
        let access = thumbnail
            .file_access()
            .map_err(|_| invalid("thumbnail dependency identity"))?;
        out.push(make(id, &pointer, kind, SecretAccess(access))?);
    }
    if refs.iter().any(|reference| {
        !out.iter()
            .any(|dependency| dependency.blob_ref() == reference)
    }) {
        return Err(invalid("metadata-only file dependency is uncertified"));
    }
    Ok(out)
}

fn make(
    record: &str,
    pointer: &Digest,
    kind: FileDependencyKind,
    access: SecretAccess,
) -> StorageResult<FileDependency> {
    // This record also enters a plaintext local capture spool. Files expose
    // only their ciphertext digest there; the plaintext digest stays in access.
    let public_hash = crate::sharing::file_blob_seal::file_blob_storage_hash(&access.0.blob_ref)
        .map_err(|_| invalid("file dependency storage digest"))?;
    let file_hash = Digest::try_from(public_hash.to_owned())
        .map_err(|_| invalid("file dependency requires canonical digest"))?;
    let stored = access
        .0
        .encrypted_size_bytes
        .ok_or_else(|| invalid("file dependency size is missing"))?;
    if !(29..=FILE_DEPENDENCY_MAX_STORED_BYTES).contains(&stored) {
        return Err(invalid("file dependency exceeds total backup policy"));
    }
    if record.len() != 64
        || !record
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid("file dependency record identity"));
    }
    let record = FileDependencyRecord {
        version: 1,
        record_id: record.to_owned(),
        logical_pointer_sha256: pointer.clone(),
        kind,
        blob_ref: access.0.blob_ref.clone(),
        file_hash,
        owner_scope: access.0.owner_scope.clone(),
        encryption_suite: access.0.cipher_suite.clone(),
        expected_stored_bytes: stored,
    };
    if serde_json::to_vec(&record)
        .map_err(|_| invalid("file dependency directory encode"))?
        .len() as u64
        > DIRECTORY_MAX_BYTES
    {
        return Err(invalid("file dependency directory exceeds its bound"));
    }
    Ok(FileDependency { record, access })
}
