//! Frozen version-one policy limits, below the source and provider ceilings.

use crate::{canonical_bytes, ProtocolError};
use serde::Serialize;

pub const CLOUD_OBJECT_MAX_BYTES: u64 = 64 * 1024 * 1024;
pub const REQUEST_JSON_MAX_BYTES: u64 = 256 * 1024;
pub const RECEIPT_JSON_MAX_BYTES: u64 = 16 * 1024;
pub const ENCRYPTION_ENVELOPE_OVERHEAD_BYTES: u64 = 29;
pub const MANIFEST_PAGE_ENTRIES_MAX: usize = 512;
pub const NATIVE_FILES_MAX: usize = lastdb_storage_policy::native_v1::MAX_BACKUP_FILES;
pub const BACKUP_STORED_BYTES_MAX: u64 = lastdb_storage_policy::BACKUP_TOTAL_STORED_BYTES;
pub const DECODED_VALUES_MAX_BYTES: u64 =
    lastdb_storage_policy::BACKUP_RESOURCES.decoded_values_bytes;
pub const STREAM_BUFFER_MAX_BYTES: u64 =
    lastdb_storage_policy::BACKUP_RESOURCES.stream_buffer_bytes;
pub const DURABLE_LOCAL_SPOOL_MAX_BYTES: u64 =
    lastdb_storage_policy::BACKUP_RESOURCES.durable_spool_bytes;

/// Post-decode bound only. The HTTP adapter must bound raw bytes before decode.
pub fn encoded_json_bytes<T: Serialize>(value: &T, maximum: u64) -> Result<u64, ProtocolError> {
    let length = canonical_bytes(value)?.len() as u64;
    if length > maximum {
        return Err(ProtocolError::limit("encoded_json.bytes"));
    }
    Ok(length)
}
