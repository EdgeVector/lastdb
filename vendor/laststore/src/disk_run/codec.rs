//! Varint, prefix-compressed key, and data-block record encoding for run files.

use crate::disk_run::{Found, MAX_RUN_KEY_BYTES, MAX_RUN_VALUE_BYTES};
use crate::{Error, Result};

pub(crate) fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn varint_len(value: u64) -> usize {
    ((64 - value.leading_zeros().min(63)) as usize).div_ceil(7)
}

pub(crate) fn get_varint(bytes: &[u8], at: &mut usize) -> Result<u64> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *bytes.get(*at).ok_or_else(|| corrupt("varint eof"))?;
        *at += 1;
        if shift == 63 && byte > 1 {
            return Err(corrupt("varint overflow"));
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(corrupt("varint overflow"))
}

pub(crate) fn get_len(bytes: &[u8], at: &mut usize, limit: usize) -> Result<usize> {
    let len = usize::try_from(get_varint(bytes, at)?).ok();
    len.filter(|&n| n <= limit)
        .ok_or_else(|| corrupt("length limit"))
}

pub(crate) fn get_slice<'a>(bytes: &'a [u8], at: &mut usize, len: usize) -> Result<&'a [u8]> {
    let end = at
        .checked_add(len)
        .filter(|&e| e <= bytes.len())
        .ok_or_else(|| corrupt("eof"))?;
    let slice = &bytes[*at..end];
    *at = end;
    Ok(slice)
}

pub(crate) fn get_bytes<'a>(bytes: &'a [u8], at: &mut usize, limit: usize) -> Result<&'a [u8]> {
    let len = get_len(bytes, at, limit)?;
    get_slice(bytes, at, len)
}

fn shared_prefix(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

pub(super) fn put_key(out: &mut Vec<u8>, prev: &[u8], key: &[u8]) {
    let shared = shared_prefix(prev, key);
    put_varint(out, shared as u64);
    put_varint(out, (key.len() - shared) as u64);
    out.extend_from_slice(&key[shared..]);
}

pub(crate) fn record_len(prev: &[u8], key: &[u8], value: Option<&[u8]>) -> usize {
    let (shared, vlen) = (shared_prefix(prev, key), value.map_or(0, <[u8]>::len));
    let (suffix, tag) = (key.len() - shared, value.map_or(0, |v| v.len() as u64 + 1));
    varint_len(shared as u64) + varint_len(suffix as u64) + suffix + varint_len(tag) + vlen
}

pub(crate) fn put_record(out: &mut Vec<u8>, prev: &[u8], key: &[u8], value: Option<&[u8]>) {
    put_key(out, prev, key);
    put_varint(out, value.map_or(0, |v| v.len() as u64 + 1));
    if let Some(v) = value {
        out.extend_from_slice(v);
    }
}

pub(crate) type BlockRecords = Vec<(Vec<u8>, Found)>;

pub(crate) fn decode_block(payload: &[u8]) -> Result<BlockRecords> {
    let (mut records, mut at, mut key): (BlockRecords, usize, Vec<u8>) =
        (Vec::new(), 0, Vec::new());
    while at < payload.len() {
        let shared = get_len(payload, &mut at, key.len())?;
        let suffix = get_bytes(payload, &mut at, MAX_RUN_KEY_BYTES - shared)?;
        key.truncate(shared);
        key.extend_from_slice(suffix);
        if key.is_empty() || records.last().is_some_and(|(b, _)| *b >= key) {
            return Err(corrupt("block keys not ascending"));
        }
        let found = match get_len(payload, &mut at, MAX_RUN_VALUE_BYTES + 1)?.checked_sub(1) {
            Some(len) => Found::Value(get_slice(payload, &mut at, len)?.to_vec()),
            None => Found::Tombstone,
        };
        records.push((key.clone(), found));
    }
    Ok(records)
}

pub(crate) fn corrupt(what: &str) -> Error {
    Error::Corrupt(format!("disk run: {what}"))
}

pub(super) mod bloom {
    use super::{corrupt, get_len, get_slice, get_varint, put_varint};
    use crate::Result;

    const PROBES: u64 = 7;
    const MAX_FILTER_BYTES: usize = 64 << 20;

    fn mix(mut z: u64) -> u64 {
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    pub(in crate::disk_run) fn key_hash(key: &[u8]) -> u64 {
        mix(key.iter().fold(0xcbf2_9ce4_8422_2325, |h, &b| {
            (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
        }))
    }

    fn probes(hash: u64, bits: u64) -> impl Iterator<Item = u64> {
        let step = mix(hash ^ 0x9e37_79b9_7f4a_7c15) | 1;
        (0..PROBES).map(move |i| hash.wrapping_add(i.wrapping_mul(step)) % bits)
    }

    pub(in crate::disk_run) fn put_filter(out: &mut Vec<u8>, hashes: &[u64], bits_per_key: u32) {
        let bytes = if bits_per_key == 0 || hashes.is_empty() {
            0
        } else {
            (hashes.len() * bits_per_key as usize)
                .div_ceil(8)
                .clamp(8, MAX_FILTER_BYTES)
        };
        put_varint(out, bytes as u64);
        put_varint(out, PROBES);
        let start = out.len();
        out.resize(start + bytes, 0);
        if bytes > 0 {
            let (bits, filter) = ((bytes * 8) as u64, &mut out[start..]);
            for &hash in hashes {
                for bit in probes(hash, bits) {
                    filter[(bit / 8) as usize] |= 1 << (bit % 8);
                }
            }
        }
    }

    pub(in crate::disk_run) fn read_filter<'a>(
        bytes: &'a [u8],
        at: &mut usize,
    ) -> Result<&'a [u8]> {
        let len = get_len(bytes, at, MAX_FILTER_BYTES)?;
        (get_varint(bytes, at)? == PROBES)
            .then(|| get_slice(bytes, at, len))
            .ok_or_else(|| corrupt("bloom probes"))?
    }

    pub(in crate::disk_run) fn may_hold(filter: &[u8], hash: u64) -> bool {
        filter.is_empty()
            || probes(hash, (filter.len() * 8) as u64)
                .all(|bit| filter[(bit / 8) as usize] & (1 << (bit % 8)) != 0)
    }
}
