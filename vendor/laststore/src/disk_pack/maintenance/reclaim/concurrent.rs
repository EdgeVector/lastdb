//! A newly allocated or incomplete part never belongs to concurrent cleanup.

use crate::disk_pack::bytes::{u64_at, verify_checksum};
use crate::disk_pack::PAGE_BYTES;
use crate::Result;
use std::io::Read;
use std::path::Path;

pub(super) fn old_part(path: &Path, floor: u64) -> Result<bool> {
    let mut file = std::fs::File::open(path)?;
    let mut magic = [0; 8];
    if file.read_exact(&mut magic).is_err() {
        return Ok(false);
    }
    let (length, offset) = match &magic {
        b"LDPAGE1\0" => (PAGE_BYTES, 16),
        b"LDPART1\0" => (128, 72),
        _ => return Ok(false),
    };
    let mut bytes = vec![0; length];
    bytes[..8].copy_from_slice(&magic);
    if file.read_exact(&mut bytes[8..]).is_err() || verify_checksum(&bytes, 80).is_err() {
        return Ok(false);
    }
    let generation = u64_at(&bytes, offset);
    Ok(generation > 0 && generation < floor)
}
