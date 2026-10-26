//! Verified backup-owned ciphertext outside the immutable native root.
//! Pointer keys remain in the original encrypted atoms. No native row is rewritten.
//!
//! Only the restore of a disk format v1 cloud backup wrote this directory. The
//! v2 restore has no external file dependencies, so this program only reads a
//! directory that an earlier program wrote.
use super::{PackRuntime, StorageError, StorageResult};
use crate::sharing::delivery_wire::{file_blob_cipher_suite_supported, FileBlobAccess};
use cloud_backup_protocol::{DecimalU64, Digest, FileDependencyKind, Id};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

mod files;

pub const DIRECTORY: &str = "native-file-recovery-v1";
const METADATA_MAX_BYTES: u64 = lastdb_storage_policy::backup_decode::DIRECTORY_MAX_BYTES;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryDirectory {
    pub version: u8,
    pub home_id: Id,
    pub source_current_sha256: Digest,
    pub commit_sha256: Digest,
    pub account_public_key: Digest,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryFile {
    pub logical_pointer_sha256: Digest,
    pub kind: FileDependencyKind,
    pub stored_source_sha256: Digest,
    pub stored_source_bytes: DecimalU64,
    pub encryption_suite: String,
}

impl RecoveryFile {
    pub fn validate(&self) -> StorageResult<()> {
        if !(cloud_backup_protocol::limits::ENCRYPTION_ENVELOPE_OVERHEAD_BYTES
            ..=cloud_backup_protocol::limits::BACKUP_STORED_BYTES_MAX)
            .contains(&self.stored_source_bytes.get())
            || !file_blob_cipher_suite_supported(&self.encryption_suite)
        {
            return Err(invalid("unsupported recovered file size or suite"));
        }
        Ok(())
    }
}

pub fn entry_key(pointer: &Digest, kind: FileDependencyKind) -> String {
    let kind = match kind {
        FileDependencyKind::Main => "main",
        FileDependencyKind::Thumbnail => "thumbnail",
        FileDependencyKind::Poster => "poster",
    };
    format!("{}-{kind}", pointer.as_str())
}

pub fn object_path(data_root: &Path, digest: &Digest) -> PathBuf {
    data_root
        .join(DIRECTORY)
        .join("objects")
        .join(digest.as_str())
}

/// Open the existing version-one remote file envelope under its original key.
/// Limits apply before AES-GCM allocates. The helper never accepts at-rest ENC.
pub fn open_remote_file(
    access: &FileBlobAccess,
    ciphertext: &[u8],
    maximum: u64,
) -> StorageResult<Vec<u8>> {
    if !file_blob_cipher_suite_supported(&access.cipher_suite)
        || ciphertext.first() != Some(&1)
        || (ciphertext.len() as u64)
            < cloud_backup_protocol::limits::ENCRYPTION_ENVELOPE_OVERHEAD_BYTES
        || ciphertext.len() as u64
            > maximum
                .saturating_add(cloud_backup_protocol::limits::ENCRYPTION_ENVELOPE_OVERHEAD_BYTES)
        || access.encrypted_size_bytes != Some(ciphertext.len() as u64)
    {
        return Err(invalid("recovered file envelope bounds"));
    }
    let key = zeroize::Zeroizing::new(
        crate::hex::hex_decode_array::<32>(&access.dek)
            .ok_or_else(|| invalid("recovered file key"))?,
    );
    let plain = crate::crypto::envelope::decrypt_envelope(&key, ciphertext)
        .map_err(|_| invalid("recovered file authentication"))?;
    if plain.len() as u64 > maximum {
        return Err(invalid("recovered file plaintext identity"));
    }
    crate::sharing::file_blob_seal::verify_file_blob_identity(
        access,
        cloud_backup_protocol::bytes_sha256(&plain).as_str(),
        cloud_backup_protocol::bytes_sha256(ciphertext).as_str(),
    )
    .map_err(|_| invalid("recovered file identity"))?;
    Ok(plain)
}

/// Read by the exact pointer digest and kind. An absent entry is a local miss.
/// Ordinary local writes can advance CURRENT; the immutable home identity stays.
/// Eager normal serving API. The local file API admission bounds its Vec result.
pub fn resolve_entry(
    data_root: &Path,
    home_id: &Id,
    pointer_id: &Digest,
    kind: FileDependencyKind,
    access: &FileBlobAccess,
    maximum: u64,
) -> StorageResult<Option<Vec<u8>>> {
    let Some(bytes) = read_ciphertext(data_root, home_id, pointer_id, kind, access, maximum)?
    else {
        return Ok(None);
    };
    open_remote_file(access, &bytes, maximum).map(Some)
}

fn read_ciphertext(
    data_root: &Path,
    home_id: &Id,
    pointer_id: &Digest,
    kind: FileDependencyKind,
    access: &FileBlobAccess,
    maximum: u64,
) -> StorageResult<Option<Vec<u8>>> {
    let Some(entry) = binding(data_root, home_id, pointer_id, kind, access)? else {
        return Ok(None);
    };
    if entry.stored_source_bytes.get()
        > maximum.saturating_add(cloud_backup_protocol::limits::ENCRYPTION_ENVELOPE_OVERHEAD_BYTES)
    {
        return Err(StorageError::ReadResourceLimit {
            detail: "recovered file input exceeds native pending memory allowance".into(),
        });
    }
    let bytes = files::read_bytes(
        &object_path(data_root, &entry.stored_source_sha256),
        entry.stored_source_bytes.get(),
    )?;
    if cloud_backup_protocol::bytes_sha256(&bytes) != entry.stored_source_sha256 {
        return Err(invalid("recovered file stored identity"));
    }
    Ok(Some(bytes))
}

fn binding(
    data_root: &Path,
    home_id: &Id,
    pointer_id: &Digest,
    kind: FileDependencyKind,
    access: &FileBlobAccess,
) -> StorageResult<Option<RecoveryFile>> {
    let root = data_root.join(DIRECTORY);
    if !files::directory_present(&root)? {
        return Ok(None);
    }
    for child in ["entries", "objects"] {
        if !files::directory_present(&root.join(child))? {
            return Err(invalid("recovery child directory"));
        }
    }
    let directory: RecoveryDirectory =
        files::read_json(&root.join("directory.json"), METADATA_MAX_BYTES)?;
    if directory.version != 1 || directory.home_id != *home_id {
        return Err(invalid("recovered file home identity"));
    }
    let path = root.join("entries").join(entry_key(pointer_id, kind));
    if !files::regular_present(&path)? {
        return Ok(None);
    }
    let entry: RecoveryFile = files::read_json(&path, METADATA_MAX_BYTES)?;
    entry.validate()?;
    if entry.logical_pointer_sha256 != *pointer_id
        || entry.kind != kind
        || entry.encryption_suite != access.cipher_suite
        || access.encrypted_size_bytes != Some(entry.stored_source_bytes.get())
    {
        return Err(invalid("recovered file pointer binding"));
    }
    Ok(Some(entry))
}

impl PackRuntime {
    pub(crate) fn resolve_recovered_file(
        &self,
        pointer: &Digest,
        kind: FileDependencyKind,
        access: &FileBlobAccess,
    ) -> StorageResult<Option<Vec<u8>>> {
        let root = self
            .physical
            .path()
            .parent()
            .ok_or_else(|| invalid("node root"))?;
        let home_id = Id::try_from(self.home_identity().to_string())
            .map_err(|_| invalid("native home identity"))?;
        resolve_entry(
            root,
            &home_id,
            pointer,
            kind,
            access,
            lastdb_storage_policy::native_write::LOCAL_FILE_INPUT_MEMORY_BYTES
                - cloud_backup_protocol::limits::ENCRYPTION_ENVELOPE_OVERHEAD_BYTES,
        )
    }
}

fn invalid(field: &'static str) -> StorageError {
    StorageError::InvalidOperation(field.into())
}
