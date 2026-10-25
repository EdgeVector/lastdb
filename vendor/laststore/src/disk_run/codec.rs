//! Byte codec for run blocks: LEB128 varints and front-coded keys.
//!
//! A front-coded key is `varint shared, varint suffix_len, suffix`, where
//! `shared` counts the leading bytes it has in common with the key before it
//! in the same block. Each block starts again from an empty key, so a block
//! decodes without its neighbours.

use super::{Found, MAX_RUN_KEY_BYTES, MAX_RUN_VALUE_BYTES};
use crate::{Error, Result};

pub(super) fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

/// Bytes `put_varint` writes for `value`.
pub(super) fn varint_len(mut value: u64) -> usize {
    let mut n = 1;
    while value >= 0x80 {
        value >>= 7;
        n += 1;
    }
    n
}

/// Reads one varint at `*at` and moves `*at` past it.
pub(super) fn get_varint(bytes: &[u8], at: &mut usize) -> Result<u64> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *bytes
            .get(*at)
            .ok_or_else(|| corrupt("varint runs past the block"))?;
        *at += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(corrupt("varint longer than 64 bits"))
}

pub(super) fn get_len(bytes: &[u8], at: &mut usize, limit: usize) -> Result<usize> {
    let value = get_varint(bytes, at)?;
    usize::try_from(value)
        .ok()
        .filter(|n| *n <= limit)
        .ok_or_else(|| corrupt("length over its limit"))
}

pub(super) fn get_slice<'a>(bytes: &'a [u8], at: &mut usize, len: usize) -> Result<&'a [u8]> {
    let end = at
        .checked_add(len)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| corrupt("field runs past the block"))?;
    let slice = &bytes[*at..end];
    *at = end;
    Ok(slice)
}

pub(super) fn shared_prefix(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

/// Appends `key` front-coded against `prev`.
pub(super) fn put_key(out: &mut Vec<u8>, prev: &[u8], key: &[u8]) {
    let shared = shared_prefix(prev, key);
    put_varint(out, shared as u64);
    put_varint(out, (key.len() - shared) as u64);
    out.extend_from_slice(&key[shared..]);
}

/// Reads one front-coded key; `prev` holds the key before it and becomes it.
pub(super) fn get_key(bytes: &[u8], at: &mut usize, prev: &mut Vec<u8>) -> Result<()> {
    let shared = get_len(bytes, at, prev.len())?;
    let suffix_len = get_len(bytes, at, MAX_RUN_KEY_BYTES - shared)?;
    let suffix = get_slice(bytes, at, suffix_len)?;
    prev.truncate(shared);
    prev.extend_from_slice(suffix);
    Ok(())
}

/// Bytes one record adds to a block, before the record is written.
pub(super) fn record_len(prev: &[u8], key: &[u8], value: Option<&[u8]>) -> usize {
    let shared = shared_prefix(prev, key);
    let suffix = key.len() - shared;
    let tag = value.map_or(0, |v| v.len() as u64 + 1);
    varint_len(shared as u64)
        + varint_len(suffix as u64)
        + suffix
        + varint_len(tag)
        + value.map_or(0, <[u8]>::len)
}

/// Appends one record: front-coded key, then `0` for a tombstone or
/// `len + 1` and the value bytes.
pub(super) fn put_record(out: &mut Vec<u8>, prev: &[u8], key: &[u8], value: Option<&[u8]>) {
    put_key(out, prev, key);
    match value {
        None => put_varint(out, 0),
        Some(value) => {
            put_varint(out, value.len() as u64 + 1);
            out.extend_from_slice(value);
        }
    }
}

/// Decoded records of one data block, in key order.
pub(super) type BlockRecords = Vec<(Vec<u8>, Found)>;

/// Decodes every record of one data block, in key order.
pub(super) fn decode_block(payload: &[u8]) -> Result<BlockRecords> {
    let mut records = Vec::new();
    let mut at = 0;
    let mut key = Vec::new();
    while at < payload.len() {
        get_key(payload, &mut at, &mut key)?;
        let ascending = records
            .last()
            .map_or(!key.is_empty(), |(before, _): &(Vec<u8>, Found)| {
                *before < key
            });
        if !ascending {
            return Err(corrupt("block keys are not strictly ascending"));
        }
        let tag = get_len(payload, &mut at, MAX_RUN_VALUE_BYTES + 1)?;
        let found = if tag == 0 {
            Found::Tombstone
        } else {
            Found::Value(get_slice(payload, &mut at, tag - 1)?.to_vec())
        };
        records.push((key.clone(), found));
    }
    Ok(records)
}

pub(super) fn corrupt(what: &str) -> Error {
    Error::Corrupt(format!("disk run: {what}"))
}
