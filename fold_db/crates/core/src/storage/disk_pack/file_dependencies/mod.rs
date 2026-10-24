//! Atomic file-pointer directory and bounded, exact-root coverage proof.
//!
//! The directory contains no file keys. It is encrypted by the normal account
//! store and changes in the same native batch as the immutable atom body.

mod atom;
mod budget_json;
mod metadata;
mod native;
mod select;

pub use atom::open_atom;
pub use cloud_backup_protocol::FileDependencyKind;
pub use metadata::verify_metadata_coverage;
pub(crate) use native::scratch::Scratch as NativeCasOutput;
pub use native::{read_directories, read_native_cas, verify_native_cas, NativeCasProof};
pub use select::expected;

use super::{StorageError, StorageResult};
use crate::sharing::delivery_wire::FileBlobAccess;
use cloud_backup_protocol::Digest;
use serde::{Deserialize, Serialize};

/// Declared source bound is independent of buffers and external transport.
pub const FILE_DEPENDENCY_MAX_STORED_BYTES: u64 = lastdb_storage_policy::BACKUP_TOTAL_STORED_BYTES;
pub const NATIVE_CAS_ROW_MAX_BYTES: u64 = lastdb_storage_policy::native_v1::RECORD_BODY_MAX_BYTES;
pub const DIRECTORY_MAX_BYTES: u64 = lastdb_storage_policy::backup_decode::DIRECTORY_MAX_BYTES;
pub const ATOM_INVENTORY_READ_BYTES: u64 =
    lastdb_storage_policy::backup_decode::ATOM_INVENTORY_READ_BYTES;

/// The complete native atom coordinate range. Empty end means an empty range
/// in the physical reader, so both capture and restore use this exact boundary.
pub fn atom_inventory<'a>(
    read: &'a mut laststore::disk_pack::home::HomeRead,
    group: u16,
    start: &[u8],
) -> StorageResult<laststore::disk_pack::HashRangeCursor<'a>> {
    read.administrative_group_range("atoms", group, start, &[0xff])
        .map_err(super::map)
}

/// Nonsecret pointer identity. No ciphertext/body digest is guessed at mutation time.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileDependencyRecord {
    pub version: u16,
    pub atom_id: String,
    pub logical_pointer_sha256: Digest,
    pub kind: FileDependencyKind,
    pub blob_ref: String,
    /// Ciphertext digest for account-v2; plaintext digest for old file suites.
    /// This record is nonsecret and can enter the local capture spool.
    pub file_hash: Digest,
    pub owner_scope: Option<String>,
    pub encryption_suite: String,
    pub expected_stored_bytes: u64,
}

/// A selected pointer plus its original key. Never serialize this value.
pub struct FileDependency {
    pub record: FileDependencyRecord,
    access: SecretAccess,
}

pub(super) struct SecretAccess(pub FileBlobAccess);
impl Drop for SecretAccess {
    fn drop(&mut self) {
        use zeroize::Zeroize;
        self.0.dek.zeroize();
    }
}

impl FileDependency {
    pub fn access(&self) -> &FileBlobAccess {
        &self.access.0
    }

    pub fn logical_pointer_sha256(&self) -> &Digest {
        &self.record.logical_pointer_sha256
    }

    pub fn kind(&self) -> FileDependencyKind {
        self.record.kind
    }

    pub fn encryption_suite(&self) -> &str {
        &self.record.encryption_suite
    }

    pub fn blob_ref(&self) -> &str {
        &self.record.blob_ref
    }

    pub fn file_hash(&self) -> &Digest {
        &self.record.file_hash
    }

    pub fn storage_hash(&self) -> StorageResult<&str> {
        crate::sharing::file_blob_seal::file_blob_storage_hash(self.blob_ref())
            .map_err(|_| invalid("file dependency storage digest"))
    }

    pub fn expected_stored_bytes(&self) -> u64 {
        self.record.expected_stored_bytes
    }
}

/// Bare main-namespace key. Add the exact body scope with `build_storage_key`.
pub fn directory_key(atom_id: &str, kind: FileDependencyKind) -> String {
    format!("fdep:v1:{atom_id}\0{}", kind_name(kind))
}

pub fn is_directory_key(key: &str) -> bool {
    key.strip_prefix("fdep:v1:")
        .and_then(|tail| tail.split_once('\0'))
        .is_some_and(|(atom, kind)| {
            atom.len() == 64
                && atom
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                && matches!(kind, "main" | "thumbnail" | "poster")
        })
}

/// Decode the native storage coordinate before its atom identity or scope is used.
pub fn atom_coordinate(encoded: &str) -> StorageResult<Option<(Option<String>, String)>> {
    super::validate_restored_atom_key(encoded)?;
    let raw = super::super::laststore::native_routing::decode(encoded)?;
    let raw = std::str::from_utf8(&raw).map_err(|_| invalid("native atom key encoding"))?;
    let Some(id) = crate::atom::atom_key_codec::body_uuid_of(raw) else {
        return Ok(None);
    };
    let scope = super::super::current_keys::exact_scope(raw)?.map(str::to_owned);
    Ok(Some((scope, id.to_owned())))
}

/// Decode already account-decrypted directory bytes and compare every selector.
pub fn decode_directory(
    bytes: &[u8],
    expected: &FileDependency,
) -> StorageResult<FileDependencyRecord> {
    if bytes.len() as u64 > DIRECTORY_MAX_BYTES {
        return Err(invalid("file dependency directory exceeds its bound"));
    }
    let record: FileDependencyRecord =
        serde_json::from_slice(bytes).map_err(|_| invalid("file dependency directory format"))?;
    if record != expected.record {
        return Err(invalid(
            "file dependency directory differs from exact atom pointer",
        ));
    }
    Ok(record)
}

pub(crate) fn kind_name(kind: FileDependencyKind) -> &'static str {
    match kind {
        FileDependencyKind::Main => "main",
        FileDependencyKind::Thumbnail => "thumbnail",
        FileDependencyKind::Poster => "poster",
    }
}

pub(super) fn invalid(detail: &str) -> StorageError {
    StorageError::InvalidOperation(detail.into())
}
