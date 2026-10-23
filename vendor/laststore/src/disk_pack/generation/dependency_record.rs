use super::super::bytes::*;
use super::super::dependency::Coordinate;
use super::super::{corrupt, Current, PAGE_BYTES, PART_BYTES};
use crate::Result;

/// Validate every field independently of the immutable part's existence.
/// Receipt-only carry is permitted only by a verified exclusive writer lease.
pub(super) fn decode(current: &Current, entry: &[u8]) -> Result<Coordinate> {
    if entry.len() != 80 {
        return Err(corrupt("generation dependency record length"));
    }
    zero(&entry[1..4])?;
    zero(&entry[24..32])?;
    zero(&entry[40..72])?;
    let coordinate = Coordinate {
        kind: entry[0],
        object: nonnil(uuid_at(entry, 8))?,
        part: u32_at(entry, 4),
    };
    if !matches!(coordinate.kind, 1 | 2 | 3 | 5) {
        return Err(corrupt("generation dependency kind"));
    }
    let length = u64_at(entry, 32);
    let generation = u64_at(entry, 72);
    let data = coordinate.kind == 5;
    if length == 0
        || length > PART_BYTES
        || (data && length < 128)
        || (!data && length % PAGE_BYTES as u64 != 0)
        || generation == 0
        || generation > current.generation
    {
        return Err(corrupt("generation dependency length or creation boundary"));
    }
    Ok(coordinate)
}
