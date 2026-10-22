//! Verified backup-owned ciphertext outside the immutable native root.
//! Pointer keys remain in the original encrypted atoms. No native row is rewritten.
use super::{PackRuntime, StorageError, StorageResult};
use crate::sharing::delivery_wire::{file_blob_cipher_suite_supported, FileBlobAccess};
use cloud_backup_protocol::{DecimalU64, Digest, ExternalFileDependency, FileDependencyKind, Id};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::{Path, PathBuf};

mod files;
pub use files::{prepare_directory, save_entry};

pub const DIRECTORY: &str = "native-file-recovery-v1";
/// The existing eager file API cannot authenticate arbitrary contract-sized files.
/// The owned native decoder releases outer layers before the next allocation.
pub const FILE_RECOVERY_MAX_PLAINTEXT_BYTES: u64 = 16 * 1024 * 1024;
pub const FILE_RECOVERY_MAX_STORED_BYTES: u64 = FILE_RECOVERY_MAX_PLAINTEXT_BYTES + 29;

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
    pub fn from_dependency(value: &ExternalFileDependency) -> StorageResult<Self> {
        value.validate().map_err(|_| invalid("file dependency"))?;
        let file = Self {
            logical_pointer_sha256: value.logical_pointer_sha256.clone(),
            kind: value.kind,
            stored_source_sha256: value.stored_source_sha256.clone(),
            stored_source_bytes: value.stored_source_bytes,
            encryption_suite: value.encryption_suite.as_str().into(),
        };
        file.validate()?;
        Ok(file)
    }

    pub fn key(&self) -> String {
        entry_key(&self.logical_pointer_sha256, self.kind)
    }

    pub fn validate(&self) -> StorageResult<()> {
        if !(29..=FILE_RECOVERY_MAX_STORED_BYTES).contains(&self.stored_source_bytes.get())
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
    let maximum = maximum.min(FILE_RECOVERY_MAX_PLAINTEXT_BYTES);
    if !file_blob_cipher_suite_supported(&access.cipher_suite)
        || ciphertext.first() != Some(&1)
        || ciphertext.len() < 29
        || ciphertext.len() as u64 > maximum.saturating_add(29)
        || access.encrypted_size_bytes != Some(ciphertext.len() as u64)
    {
        return Err(invalid("recovered file envelope bounds"));
    }
    let key = crate::hex::hex_decode_array::<32>(&access.dek)
        .ok_or_else(|| invalid("recovered file key"))?;
    let plain = crate::crypto::envelope::decrypt_envelope(&key, ciphertext)
        .map_err(|_| invalid("recovered file authentication"))?;
    let digest = cloud_backup_protocol::bytes_sha256(&plain);
    if plain.len() as u64 > maximum
        || access.file_hash != digest.as_str()
        || access.blob_ref != format!("sha256:{}", digest.as_str())
    {
        return Err(invalid("recovered file plaintext identity"));
    }
    Ok(plain)
}

/// Read by the exact pointer digest and kind. An absent resolver is a local miss.
/// Ordinary local writes can advance CURRENT; the immutable home identity stays.
pub fn resolve(
    data_root: &Path,
    home_id: &Id,
    pointer: &Value,
    kind: FileDependencyKind,
    access: &FileBlobAccess,
    maximum: u64,
) -> StorageResult<Option<Vec<u8>>> {
    let pointer_id = cloud_backup_protocol::logical_pointer_sha256(pointer)
        .map_err(|_| invalid("recovered file pointer"))?;
    resolve_entry(data_root, home_id, &pointer_id, kind, access, maximum)
}

/// Restore uses the already validated whole-pointer digest after it drops atom JSON.
pub fn resolve_entry(
    data_root: &Path,
    home_id: &Id,
    pointer_id: &Digest,
    kind: FileDependencyKind,
    access: &FileBlobAccess,
    maximum: u64,
) -> StorageResult<Option<Vec<u8>>> {
    let root = data_root.join(DIRECTORY);
    if !files::directory_present(&root)? {
        return Ok(None);
    }
    for child in ["entries", "objects"] {
        if !files::directory_present(&root.join(child))? {
            return Err(invalid("recovery child directory"));
        }
    }
    let directory: RecoveryDirectory = files::read_json(&root.join("directory.json"), 4096)?;
    if directory.version != 1 || directory.home_id != *home_id {
        return Err(invalid("recovered file home identity"));
    }
    let path = root.join("entries").join(entry_key(&pointer_id, kind));
    if !files::regular_present(&path)? {
        return Ok(None);
    }
    let entry: RecoveryFile = files::read_json(&path, 4096)?;
    entry.validate()?;
    if entry.logical_pointer_sha256 != *pointer_id
        || entry.kind != kind
        || entry.encryption_suite != access.cipher_suite
        || access.encrypted_size_bytes != Some(entry.stored_source_bytes.get())
    {
        return Err(invalid("recovered file pointer binding"));
    }
    if entry.stored_source_bytes.get()
        > maximum
            .min(FILE_RECOVERY_MAX_PLAINTEXT_BYTES)
            .saturating_add(29)
    {
        return Err(invalid("configured file recovery cap"));
    }
    let bytes = files::read_bytes(
        &object_path(data_root, &entry.stored_source_sha256),
        entry.stored_source_bytes.get(),
    )?;
    if cloud_backup_protocol::bytes_sha256(&bytes) != entry.stored_source_sha256 {
        return Err(invalid("recovered file stored identity"));
    }
    open_remote_file(access, &bytes, maximum).map(Some)
}

impl PackRuntime {
    pub(crate) fn resolve_recovered_file(
        &self,
        pointer: &Value,
        kind: FileDependencyKind,
        access: &FileBlobAccess,
    ) -> StorageResult<Option<Vec<u8>>> {
        let root = self
            .home
            .path()
            .parent()
            .ok_or_else(|| invalid("node root"))?;
        let home_id = Id::try_from(self.home.descriptor().identity.to_string())
            .map_err(|_| invalid("native home identity"))?;
        resolve(
            root,
            &home_id,
            pointer,
            kind,
            access,
            crate::fold_db_core::fold_db::local_file_blob_max_bytes() as u64,
        )
    }
}

fn invalid(field: &'static str) -> StorageError {
    StorageError::InvalidOperation(field.into())
}
