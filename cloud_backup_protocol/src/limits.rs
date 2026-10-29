//! Frozen version-one policy limits, below the source and provider ceilings.

use crate::{canonical_bytes, ProtocolError};
use serde::Serialize;

/// Maximum bytes of one cloud object in the version-one policy.
pub const CLOUD_OBJECT_MAX_BYTES: u64 = 64 * 1024 * 1024;
/// Maximum canonical JSON bytes of a version-one request.
pub const REQUEST_JSON_MAX_BYTES: u64 = 256 * 1024;
/// Maximum canonical JSON bytes of a version-one receipt.
pub const RECEIPT_JSON_MAX_BYTES: u64 = 16 * 1024;
/// Bytes added to a plaintext for the version-one encryption envelope.
pub const ENCRYPTION_ENVELOPE_OVERHEAD_BYTES: u64 = 29;
/// Maximum entries on one manifest page.
pub const MANIFEST_PAGE_ENTRIES_MAX: usize = 512;
/// Maximum native files, taken from the storage-policy backup file cap.
pub const NATIVE_FILES_MAX: usize = lastdb_storage_policy::native_v1::MAX_BACKUP_FILES;
/// Maximum stored backup bytes, taken from the storage-policy total.
pub const BACKUP_STORED_BYTES_MAX: u64 = lastdb_storage_policy::BACKUP_TOTAL_STORED_BYTES;
/// Maximum decoded-value bytes, taken from `BACKUP_RESOURCES`.
pub const DECODED_VALUES_MAX_BYTES: u64 =
    lastdb_storage_policy::BACKUP_RESOURCES.decoded_values_bytes;
/// Maximum stream-buffer bytes, taken from `BACKUP_RESOURCES`.
pub const STREAM_BUFFER_MAX_BYTES: u64 =
    lastdb_storage_policy::BACKUP_RESOURCES.stream_buffer_bytes;
/// Maximum durable local spool bytes, taken from `BACKUP_RESOURCES`.
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
