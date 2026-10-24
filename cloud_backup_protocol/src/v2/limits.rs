//! Transport metadata bounds, independent of client disk or object file size.
use crate::ProtocolError;
use serde::Serialize;

pub const CONTROL_JSON_MAX_BYTES: u64 = 256 * 1024;
pub const INVENTORY_JSON_MAX_BYTES: u64 = 256 * 1024;
pub const MAX_FORMAT_ID_BYTES: usize = 128;
pub const MAX_BOOTSTRAP_BYTES: usize = 16 * 1024;
pub const MAX_BATCH_ITEMS: usize = 4;
pub const MAX_PAGE_ENTRIES: usize = 512;
pub const MAX_PAGES: usize = 1024;

pub fn encoded<T: Serialize>(value: &T, maximum: u64) -> Result<(), ProtocolError> {
    let bytes = super::hash::canonical_bytes(value)?;
    if bytes.len() as u64 > maximum {
        return Err(ProtocolError::limit("v2.encoded_bytes"));
    }
    Ok(())
}
