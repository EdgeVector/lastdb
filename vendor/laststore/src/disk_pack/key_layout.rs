//! Checked fixed and packed locator-key layouts.

use super::bytes::{u16_at, u32_at, u64_at, zero};
use super::{corrupt, KEY_DESCRIPTOR};
use crate::Result;

pub(super) const INLINE_KEY_BYTES: usize = 1024;
pub(super) const PACKED_INLINE: u16 = 2;
pub(super) const PACKED_EXTERNAL: u16 = 3;

pub(super) fn descriptor_bytes(entry: &[u8]) -> usize {
    match u16_at(entry, 6) {
        PACKED_INLINE => 40 + u32_at(entry, 0) as usize,
        PACKED_EXTERNAL => 184,
        _ => KEY_DESCRIPTOR,
    }
}

pub(super) fn inline(entry: &[u8]) -> &[u8] {
    let start = if u16_at(entry, 6) >= PACKED_INLINE {
        40
    } else {
        8
    };
    &entry[start..start + u16_at(entry, 4) as usize]
}

pub(super) fn overflow(entry: &[u8]) -> u64 {
    match u16_at(entry, 6) {
        PACKED_INLINE => 0,
        PACKED_EXTERNAL => u64_at(entry, 168),
        _ => u64_at(entry, 136),
    }
}

pub(super) fn key_digest(entry: &[u8]) -> &[u8] {
    if u16_at(entry, 6) >= PACKED_INLINE {
        &entry[8..40]
    } else {
        &entry[144..176]
    }
}

pub(super) fn value(entry: &[u8]) -> &[u8] {
    &entry[descriptor_bytes(entry)..]
}

pub(super) fn child(entry: &[u8]) -> u64 {
    u64_at(value(entry), 0)
}

pub(super) fn validate_packed(entry: &[u8], value_bytes: usize) -> Result<()> {
    if entry.len() < 40 + value_bytes {
        return Err(corrupt("short packed key entry"));
    }
    let length = u32_at(entry, 0) as usize;
    let inline = u16_at(entry, 4) as usize;
    let descriptor = match u16_at(entry, 6) {
        PACKED_INLINE if length <= INLINE_KEY_BYTES && inline == length => 40 + length,
        PACKED_EXTERNAL
            if length > INLINE_KEY_BYTES
                && length <= super::KEY_BYTES
                && inline == 128
                && entry.len() == 184 + value_bytes =>
        {
            if u64_at(entry, 168) == 0 || u64_at(entry, 176) != (length - 128) as u64 {
                return Err(corrupt("packed external key bounds"));
            }
            184
        }
        _ => return Err(corrupt("packed key descriptor")),
    };
    if entry.len() != descriptor + value_bytes {
        return Err(corrupt("packed key entry length"));
    }
    Ok(())
}

pub(super) fn validate_fixed(bytes: &[u8]) -> Result<()> {
    let length = u32_at(bytes, 0) as usize;
    let inline = u16_at(bytes, 4) as usize;
    let overflow = length > 128;
    if length > super::KEY_BYTES
        || inline != length.min(128)
        || u16_at(bytes, 6) != u16::from(overflow)
        || (u64_at(bytes, 136) != 0) != overflow
    {
        return Err(corrupt("key descriptor"));
    }
    zero(&bytes[8 + inline..136])
}
