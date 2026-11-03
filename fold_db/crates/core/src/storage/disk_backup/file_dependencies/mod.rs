//! The file of each record and its bounded, exact-root proof (design
//! `design-lastdb-no-shared-file-blobs`).
//!
//! A record says that it has a file with its marker (`StoredRecord::file`).
//! The file lives in `cas_blobs` at a path made from the record key, so the
//! restore proof needs only the record: it copies the row at that path from
//! the authenticated CAS run, opens it with the key that the account key and
//! the marker's hash give, and checks the bytes against the marker. A record
//! has no separate file dependency row, and no record names another record's
//! file.

mod budget_json;
mod native;
mod select;

pub(crate) use native::scratch::Scratch as NativeCasOutput;
pub use native::{verify_native_cas, NativeCasProof};
pub use select::expected_record;

/// Reuse the bounded JSON decoder for another physical snapshot reader.
pub(crate) fn decode_bounded_json(bytes: &[u8]) -> StorageResult<serde_json::Value> {
    budget_json::decode(bytes)
}

use crate::storage::error::{StorageError, StorageResult};

/// Declared source bound is independent of buffers and external transport.
pub const FILE_DEPENDENCY_MAX_STORED_BYTES: u64 = lastdb_storage_policy::BACKUP_TOTAL_STORED_BYTES;
pub const DIRECTORY_MAX_BYTES: u64 = lastdb_storage_policy::backup_decode::DIRECTORY_MAX_BYTES;

/// The file of one record: its path and what its marker says. It holds the
/// file key, so it is never serialized.
pub struct FileDependency {
    path: String,
    sha256: String,
    size_bytes: u64,
    dek: zeroize::Zeroizing<[u8; 32]>,
}

impl FileDependency {
    /// The raw key of the file row in `cas_blobs`.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// The plaintext SHA-256 of the marker, lowercase hex.
    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    /// The plaintext size of the marker.
    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    fn dek(&self) -> &[u8; 32] {
        &self.dek
    }
}

pub(super) fn invalid(detail: &str) -> StorageError {
    StorageError::InvalidOperation(detail.into())
}
