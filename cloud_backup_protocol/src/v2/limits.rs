//! Transport metadata bounds, independent of client disk or object file size.
use crate::ProtocolError;
use serde::Serialize;

/// Maximum canonical JSON bytes of a V2 control message.
pub const CONTROL_JSON_MAX_BYTES: u64 = 256 * 1024;
/// Maximum canonical JSON bytes of one inventory page or index.
pub const INVENTORY_JSON_MAX_BYTES: u64 = 256 * 1024;
/// Maximum items in one V2 batch.
pub const MAX_BATCH_ITEMS: usize = 4;
/// Maximum receipts on one inventory page.
pub const MAX_PAGE_ENTRIES: usize = 512;
/// Maximum pages in one inventory index.
pub const MAX_PAGES: usize = 1024;

/// Reject canonical JSON of `value` larger than `maximum`.
pub fn encoded<T: Serialize>(value: &T, maximum: u64) -> Result<(), ProtocolError> {
    let bytes = super::hash::canonical_bytes(value)?;
    if bytes.len() as u64 > maximum {
        return Err(ProtocolError::limit("v2.encoded_bytes"));
    }
    Ok(())
}
