use super::*;
use crate::atom::{file_pointer, Atom};
use crate::sharing::delivery_wire::{
    file_blob_access_from_atom_value, file_thumbnail_ref_from_atom_value,
};
use lastdb_model::record_value::StoredRecord;
use serde_json::Value;
use std::collections::HashMap;

/// Derive every supported dependency from the exact decrypted whole atom value.
/// Legacy, malformed and metadata-only references remain explicit refusals.
pub fn expected(atom: &Atom) -> StorageResult<Vec<FileDependency>> {
    of_value(atom.uuid(), atom.content(), atom.metadata())
}

/// Derive the dependencies of each field value of a format 7 record with the
/// atom rules. A record has no atom, so the digest of a field's pointer names
/// its dependencies. The record metadata belongs to the record (D7) and can
/// outlive the field that set it, so a reference that only the metadata names
/// is not refused: the byte backup copies every CAS body and the blob counter
/// keeps it.
pub fn expected_record(record: &StoredRecord) -> StorageResult<Vec<FileDependency>> {
    let mut out = Vec::new();
    for value in record.fields.values() {
        if value.get(file_pointer::LASTDB_FILE_KEY).is_none()
            && file_pointer::blob_refs_of_atom(value, None).is_empty()
        {
            continue;
        }
        let pointer = cloud_backup_protocol::logical_pointer_sha256(value)
            .map_err(|_| invalid("file dependency pointer digest"))?;
        out.extend(of_value(pointer.as_str(), value, None)?);
    }
    Ok(out)
}

fn of_value(
    id: &str,
    content: &Value,
    metadata: Option<&HashMap<String, String>>,
) -> StorageResult<Vec<FileDependency>> {
    let refs = file_pointer::blob_refs_of_atom(content, metadata);
    let has_pointer = content.get(file_pointer::LASTDB_FILE_KEY).is_some();
    if !has_pointer && refs.is_empty() {
        return Ok(Vec::new());
    }
    let main = file_blob_access_from_atom_value(content)
        .map_err(|_| invalid("invalid file dependency access"))?
        .ok_or_else(|| invalid("legacy or incomplete file dependency is uncertified"))?;
    let main = SecretAccess(main);
    let pointer = cloud_backup_protocol::logical_pointer_sha256(content)
        .map_err(|_| invalid("file dependency pointer digest"))?;
    let mut out = vec![make(id, &pointer, FileDependencyKind::Main, main)?];
    if let Some(thumbnail) = file_thumbnail_ref_from_atom_value(content)
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
    atom: &str,
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
    if atom.len() != 64
        || !atom
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid("file dependency atom identity"));
    }
    let record = FileDependencyRecord {
        version: 1,
        atom_id: atom.to_owned(),
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
