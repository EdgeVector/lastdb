//! The file dependencies of record values and their bounded, exact-root
//! coverage proof.
//!
//! A format 7 record names its files in its field values. The restore proof
//! and the byte backup derive each dependency from the value and copy and open
//! its CAS body. The `fdep` directory rows of a format 6 atom are gone; the
//! key law refuses their family.

mod budget_json;
mod native;
mod select;

pub use cloud_backup_protocol::FileDependencyKind;
pub(crate) use native::scratch::Scratch as NativeCasOutput;
pub use native::{verify_native_cas, NativeCasProof};
pub use select::{expected_record, expected_values};

/// Reuse the bounded JSON decoder for another physical snapshot reader.
pub(crate) fn decode_bounded_json(bytes: &[u8]) -> StorageResult<serde_json::Value> {
    budget_json::decode(bytes)
}

use super::{StorageError, StorageResult};
use crate::sharing::delivery_wire::FileBlobAccess;
use cloud_backup_protocol::Digest;
use serde::{Deserialize, Serialize};

/// Declared source bound is independent of buffers and external transport.
pub const FILE_DEPENDENCY_MAX_STORED_BYTES: u64 = lastdb_storage_policy::BACKUP_TOTAL_STORED_BYTES;
pub const DIRECTORY_MAX_BYTES: u64 = lastdb_storage_policy::backup_decode::DIRECTORY_MAX_BYTES;

/// Nonsecret pointer identity. No ciphertext/body digest is guessed at mutation time.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileDependencyRecord {
    pub version: u16,
    pub atom_id: String,
    pub logical_pointer_sha256: Digest,
    pub kind: FileDependencyKind,
    pub blob_ref: String,
    /// Ciphertext digest of the stored file.
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

/// Check one offline atom-namespace coordinate with the normal current-key law.
/// Unknown or retired spellings cannot hide an unexamined file-pointer body.
pub fn validate_restored_atom_key(key: &str) -> StorageResult<()> {
    let raw = super::native_routing::decode(key)?;
    super::super::current_keys::validate("main", &raw)?;
    if super::native_routing::collection_for_key(&raw) != "atoms" {
        return Err(invalid("unsupported key in restored atom namespace"));
    }
    Ok(())
}

pub(super) fn invalid(detail: &str) -> StorageError {
    StorageError::InvalidOperation(detail.into())
}
