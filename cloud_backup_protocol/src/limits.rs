//! Frozen version-one policy limits, below the source and provider ceilings.

use crate::{canonical_bytes, ProtocolError};
use serde::Serialize;

pub const NATIVE_PART_MAX_BYTES: u64 = 8 * 1024 * 1024;
pub const CLOUD_OBJECT_MAX_BYTES: u64 = 64 * 1024 * 1024;
pub const UPLOAD_PART_BYTES: u64 = 8 * 1024 * 1024;
pub const UPLOAD_PARTS_MAX: usize = 8;
pub const REQUEST_JSON_MAX_BYTES: u64 = 256 * 1024;
pub const SESSION_JSON_MAX_BYTES: u64 = 64 * 1024;
pub const RECEIPT_JSON_MAX_BYTES: u64 = 16 * 1024;
pub const HEAD_JSON_MAX_BYTES: u64 = 16 * 1024;
pub const MANIFEST_PAGE_PLAINTEXT_MAX_BYTES: u64 = 256 * 1024;
pub const ENCRYPTION_ENVELOPE_OVERHEAD_BYTES: u64 = 29;
pub const MANIFEST_PAGE_ENVELOPE_MAX_BYTES: u64 =
    MANIFEST_PAGE_PLAINTEXT_MAX_BYTES + ENCRYPTION_ENVELOPE_OVERHEAD_BYTES;
pub const MANIFEST_PAGE_ENTRIES_MAX: usize = 512;
pub const MANIFEST_PAGES_MAX: usize = 1024;
pub const NATIVE_FILES_MAX: usize = 65_552;
pub const BACKUP_STORED_BYTES_MAX: u64 = 1024 * 1024 * 1024 * 1024;
pub const CLOUD_WORKING_ALLOCATION_MAX_BYTES: u64 = 96 * 1024 * 1024;
pub const DECODED_VALUES_MAX_BYTES: u64 = 64 * 1024 * 1024;
pub const STREAM_BUFFER_MAX_BYTES: u64 = 1024 * 1024;
pub const CONCURRENT_STREAMS_MAX: usize = 4;
pub const DURABLE_LOCAL_SPOOL_MAX_BYTES: u64 = 1024 * 1024 * 1024;
pub const LOGICAL_UNIT_MAX_BYTES: u64 = 16 * 1024 * 1024;
pub const LOGICAL_QUEUE_MAX_BYTES: u64 = 1024 * 1024 * 1024;
pub const LOGICAL_QUEUE_REQUESTS_MAX: usize = 1024;
pub const CONTROL_STORED_ITEM_MAX_BYTES: u64 = 256 * 1024;
pub const CONTROL_TRANSACTION_MAX_BYTES: u64 = 2 * 1024 * 1024;
pub const CONTROL_TRANSACTION_UNIQUE_ITEMS_MAX: usize = 16;
pub const CONTROL_ACCOUNT_KEY_MAX_BYTES: usize = 256;
pub const CONTROL_RECORD_KEY_MAX_BYTES: usize = 512;

// Shared wire validators use u64 counts, as do the exact decimal wire fields.
pub const MAX_OBJECT_BYTES: u64 = CLOUD_OBJECT_MAX_BYTES;
pub const MAX_NATIVE_PART_BYTES: u64 = NATIVE_PART_MAX_BYTES;
pub const MAX_MANIFEST_ENVELOPE_BYTES: u64 = MANIFEST_PAGE_ENVELOPE_MAX_BYTES;
pub const MAX_REFERENCE_PAGE_BYTES: u64 = MANIFEST_PAGE_PLAINTEXT_MAX_BYTES;
pub const MAX_LOGICAL_UNIT_BYTES: u64 = LOGICAL_UNIT_MAX_BYTES;
pub const MAX_REFERENCE_PAGES: u64 = 1024;
pub const MAX_PAGE_ENTRIES: u64 = 512;
pub const MAX_BACKUP_BYTES: u64 = BACKUP_STORED_BYTES_MAX;

/// Post-decode bound only. The HTTP adapter must bound raw bytes before decode.
pub fn encoded_json_bytes<T: Serialize>(value: &T, maximum: u64) -> Result<u64, ProtocolError> {
    let length = canonical_bytes(value)?.len() as u64;
    if length > maximum {
        return Err(ProtocolError::limit("encoded_json.bytes"));
    }
    Ok(length)
}

/// Count stored UTF-8 attribute-name and string-value bytes, not SDK JSON.
/// `other_attribute_bytes` must include each other stored name and value.
/// The caller supplies the complete final item, with unique attribute names.
pub fn control_item_stored_bytes(
    string_attributes: &[(&str, &str)],
    other_attribute_bytes: u64,
) -> Result<u64, ProtocolError> {
    if other_attribute_bytes > CONTROL_STORED_ITEM_MAX_BYTES {
        return Err(ProtocolError::invalid(
            "control item exceeds its byte limit",
        ));
    }
    let mut total = other_attribute_bytes;
    for (name, value) in string_attributes {
        if name.is_empty() {
            return Err(ProtocolError::invalid("control attribute name is empty"));
        }
        let bytes = name
            .len()
            .checked_add(value.len())
            .and_then(|length| u64::try_from(length).ok())
            .ok_or_else(|| ProtocolError::invalid("control item byte count overflow"))?;
        total = total
            .checked_add(bytes)
            .filter(|total| *total <= CONTROL_STORED_ITEM_MAX_BYTES)
            .ok_or_else(|| ProtocolError::invalid("control item exceeds its byte limit"))?;
    }
    Ok(total)
}
